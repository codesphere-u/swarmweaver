"""
Additional edge-case, concurrency, tamper-detection, and protocol tests for SwarmWeaver.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import pytest

from swarmweaver.blackboard import BlackboardContractError, TypedBlackboard, parse_contract_spec
from swarmweaver.checkpoint import CheckpointIntegrityError, CheckpointJournal
from swarmweaver.cli import main
from swarmweaver.dag import CycleDetectedError, DAGCompiler, scopes_overlap
from swarmweaver.engine import SwarmOrchestrator
from swarmweaver.leases import FileScopeLeaseManager, LeaseAcquisitionError
from swarmweaver.mcp_server import SwarmWeaverMCPServer
from swarmweaver.models import TaskSpec


@pytest.mark.parametrize(
    ("contract_str", "expected_key", "expected_type"),
    [
        ("ast_tree:dict", "ast_tree", "dict"),
        ("file_list:list", "file_list", "list"),
        ("retry_count:int", "retry_count", "int"),
        ("score:float", "score", "float"),
        ("passed:bool", "passed", "bool"),
        ("summary:str", "summary", "str"),
        ("plain_key", "plain_key", None),
        ("  spaced_key : object ", "spaced_key", "object"),
    ],
)
def test_parse_contract_spec_variations(
    contract_str: str, expected_key: str, expected_type: str | None
) -> None:
    assert parse_contract_spec(contract_str) == (expected_key, expected_type)


def test_blackboard_rejects_none_and_unknown_inline_types() -> None:
    bb = TypedBlackboard()
    with pytest.raises(BlackboardContractError, match="provided None"):
        bb.write("key_a", None, produced_by_task="t1")

    with pytest.raises(BlackboardContractError, match="Unknown inline contract type"):
        bb.write("key_b:unknown_type", "val", produced_by_task="t1")

    with pytest.raises(ValueError, match="non-empty string"):
        bb.write("   ", "val", produced_by_task="t1")

    with pytest.raises(KeyError, match="does not exist"):
        bb.read("missing_key")


def test_blackboard_tuple_type_schema_and_raising_validator() -> None:
    bb = TypedBlackboard()
    bb.register_schema("numeric_metric", (int, float))
    bb.write("numeric_metric", 42, produced_by_task="t1")
    bb.write("numeric_metric", 3.14, produced_by_task="t1")

    with pytest.raises(BlackboardContractError, match="got bool"):
        bb.write("numeric_metric", True, produced_by_task="t1")

    with pytest.raises(BlackboardContractError, match="failed type check"):
        bb.write("numeric_metric", "not-a-number", produced_by_task="t1")

    def exploding_validator(_v: object) -> bool:
        raise RuntimeError("validator internal failure")

    bb.register_schema("boom", exploding_validator)
    with pytest.raises(BlackboardContractError, match="validator internal failure"):
        bb.write("boom", "anything", produced_by_task="t1")


def test_multithreaded_lease_contention_allows_exactly_one_exclusive_writer() -> None:
    mgr = FileScopeLeaseManager(default_ttl_ms=30_000)
    writers = [f"writer_{i:02d}" for i in range(16)]

    def try_acquire(tid: str) -> str | None:
        try:
            grant = mgr.acquire(tid, write_scopes=["src/critical/kernel.py"])
            return grant.lease_token
        except LeaseAcquisitionError:
            return None

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(try_acquire, writers))

    acquired_tokens = [tok for tok in results if tok is not None]
    assert len(acquired_tokens) == 1
    assert len(mgr.conflicts_log) == 15


def test_lease_manager_edge_cases_and_check_conflict() -> None:
    with pytest.raises(ValueError, match="default_ttl_ms"):
        FileScopeLeaseManager(default_ttl_ms=0)

    mgr = FileScopeLeaseManager()
    with pytest.raises(ValueError, match="task_id"):
        mgr.acquire("", write_scopes=["a.py"])
    with pytest.raises(ValueError, match="ttl_ms"):
        mgr.acquire("t1", write_scopes=["a.py"], ttl_ms=0)

    mgr.acquire("t1", write_scopes=["src/api/routes.py"])
    conflict = mgr.check_conflict("t2", read_scopes=["src/api/*.py"])
    assert conflict is not None
    assert conflict.holding_task_id == "t1"

    # Re-acquiring for the same task replaces its old lease cleanly
    g_new = mgr.acquire("t1", write_scopes=["src/api/other.py"])
    assert mgr.check_conflict("t2", read_scopes=["src/api/routes.py"]) is None
    assert mgr.release(g_new.lease_token) is True
    assert mgr.release("non_existent_token") is False
    assert mgr.release_task("non_existent_task") is False

    with pytest.raises(KeyError, match="not active"):
        mgr.renew("non_existent_token")


def test_checkpoint_journal_sequence_and_prev_hash_tampering(tmp_path: Path) -> None:
    j_path = tmp_path / "chain.jsonl"
    j = CheckpointJournal(journal_path=j_path, workflow_id="wf-chain")
    j.append("t1", "d1", {"a": 1})
    j.append("t2", "d2", {"b": 2})

    # Tamper with prev_hash on record 1
    lines = j_path.read_text(encoding="utf-8").splitlines()
    rec1 = json.loads(lines[1])
    rec1["prev_hash"] = "f" * 64
    lines[1] = json.dumps(rec1)
    j_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    valid, err = j.verify_integrity()
    assert valid is False
    assert "Broken prev_hash link" in (err or "")

    # Tamper with sequence number on record 0
    rec0 = json.loads(lines[0])
    rec0["sequence"] = 99
    lines[0] = json.dumps(rec0)
    j_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    valid2, err2 = j.verify_integrity()
    assert valid2 is False
    assert "Sequence discontinuity" in (err2 or "")


def test_checkpoint_journal_malformed_jsonl_raises(tmp_path: Path) -> None:
    bad_file = tmp_path / "corrupt.jsonl"
    bad_file.write_text("{not_json}\n", encoding="utf-8")
    with pytest.raises(CheckpointIntegrityError, match="Malformed checkpoint record"):
        CheckpointJournal(journal_path=bad_file)


def test_orchestrator_crash_after_completed_count_and_raise_on_failure() -> None:
    tasks = [
        TaskSpec(task_id="s1", title="S1", output_contract=["o1:str"]),
        TaskSpec(task_id="s2", title="S2", depends_on=["s1"], output_contract=["o2:str"]),
        TaskSpec(task_id="s3", title="S3", depends_on=["s2"], output_contract=["o3:str"]),
    ]
    orch = SwarmOrchestrator(tasks)
    rep = orch.execute(crash_after_completed_count=2, resume=False)
    assert rep.status == "FAILED"
    assert rep.completed_tasks == ["s1", "s2"]
    assert rep.failed_task == "s3"

    with pytest.raises(RuntimeError, match="failed at 's3'"):
        orch.execute(fail_at_task="s3", resume=True, raise_on_failure=True)


def test_cli_human_readable_outputs_and_plan_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    tasks_file = tmp_path / "tasks.json"
    tasks_file.write_text(
        json.dumps(
            {
                "tasks": [
                    {
                        "task_id": "init",
                        "title": "Init",
                        "write_globs": ["src/main.py"],
                        "output_contract": ["init_done:bool"],
                    },
                    {
                        "task_id": "build",
                        "title": "Build",
                        "depends_on": ["init"],
                        "read_globs": ["src/main.py"],
                        "input_contract": ["init_done:bool"],
                        "output_contract": ["artifact:list"],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    assert main(["plan", "-f", str(tasks_file)]) == 0
    out_plan = capsys.readouterr().out
    assert "Conflict-Aware Wave Plan" in out_plan

    assert main(["demo"]) == 0
    out_demo = capsys.readouterr().out
    assert "10-Agent Full-Stack Refactoring Swarm Demo" in out_demo

    assert main(["benchmark"]) == 0
    out_bench = capsys.readouterr().out
    assert "Reproducible Benchmark Suite" in out_bench

    # Verify non-existent journal returns exit code 1
    assert main(["verify-journal", str(tmp_path / "missing.jsonl")]) == 1


def test_mcp_server_unknown_tool_returns_is_error() -> None:
    server = SwarmWeaverMCPServer()
    resp = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 404,
            "method": "tools/call",
            "params": {"name": "non_existent_tool", "arguments": {}},
        }
    )
    assert resp is not None
    assert resp["result"]["isError"] is True
    err_payload = json.loads(resp["result"]["content"][0]["text"])
    assert err_payload["error"] == "ValueError"


@pytest.mark.parametrize("cycle_size", [2, 4, 6])
def test_dag_compiler_various_cycle_sizes(cycle_size: int) -> None:
    tasks = [
        TaskSpec(
            task_id=f"c_{i}",
            title=f"Node {i}",
            depends_on=[f"c_{(i - 1) % cycle_size}"],
        )
        for i in range(cycle_size)
    ]
    with pytest.raises(CycleDetectedError) as exc_info:
        DAGCompiler(tasks).compile()
    assert len(exc_info.value.cycle_path) == cycle_size + 1


def test_double_wildcard_and_question_mark_scope_overlaps() -> None:
    assert scopes_overlap("src/**/test_*.py", "src/a/b/c/test_auth.py") is True
    assert scopes_overlap("src/**/test_*.py", "src/test_root.py") is True
    assert scopes_overlap("src/mod_?/handler.py", "src/mod_a/handler.py") is True
    assert scopes_overlap("src/mod_?/handler.py", "src/mod_ab/handler.py") is False

"""
Tests for SwarmWeaver CLI commands, JSON-RPC 2.0 MCP Server, and Benchmark Suite.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
import pytest

from swarmweaver.benchmark import run_benchmark_suite
from swarmweaver.checkpoint import CheckpointJournal
from swarmweaver.cli import main
from swarmweaver.mcp_server import SwarmWeaverMCPServer


def test_mcp_server_initialize_ping_and_tools_list() -> None:
    server = SwarmWeaverMCPServer()

    init_resp = server.handle_request({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    assert init_resp is not None
    assert init_resp["result"]["serverInfo"]["name"] == "swarmweaver-mcp"

    # Notification returns None
    assert server.handle_request({"jsonrpc": "2.0", "method": "notifications/initialized"}) is None

    ping_resp = server.handle_request({"jsonrpc": "2.0", "id": 2, "method": "ping"})
    assert ping_resp == {"jsonrpc": "2.0", "id": 2, "result": {}}

    list_resp = server.handle_request({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
    assert list_resp is not None
    tool_names = [t["name"] for t in list_resp["result"]["tools"]]
    assert tool_names == [
        "swarmweaver_plan_waves",
        "swarmweaver_check_file_conflicts",
        "swarmweaver_inspect_checkpoint",
        "swarmweaver_run_benchmark",
    ]


def test_mcp_server_tool_calls_and_stdio_loop(tmp_path: Path) -> None:
    server = SwarmWeaverMCPServer()

    # 1. swarmweaver_plan_waves
    plan_resp = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                "name": "swarmweaver_plan_waves",
                "arguments": {
                    "tasks": [
                        {"task_id": "a", "title": "A", "write_globs": ["src/x.py"]},
                        {"task_id": "b", "title": "B", "write_globs": ["src/x.py"]},
                    ]
                },
            },
        }
    )
    assert plan_resp is not None
    assert plan_resp["result"]["isError"] is False
    plan_data = json.loads(plan_resp["result"]["content"][0]["text"])
    assert plan_data["total_waves"] == 2
    assert len(plan_data["conflicts_resolved"]) == 1

    # 2. swarmweaver_check_file_conflicts
    conflict_resp = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "swarmweaver_check_file_conflicts",
                "arguments": {
                    "tasks": [
                        {"task_id": "t1", "write_globs": ["src/api/*.py"]},
                        {"task_id": "t2", "read_globs": ["src/api/routes.py"]},
                    ]
                },
            },
        }
    )
    assert conflict_resp is not None
    conflict_data = json.loads(conflict_resp["result"]["content"][0]["text"])
    assert conflict_data["safe_for_parallel_execution"] is False
    assert conflict_data["conflict_count"] == 1

    # 3. swarmweaver_inspect_checkpoint
    j_path = tmp_path / "mcp_journal.jsonl"
    j = CheckpointJournal(journal_path=j_path, workflow_id="mcp-wf")
    j.append("step_1", "dig_1", {"ok": True}, tokens_used=500)

    inspect_resp = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "swarmweaver_inspect_checkpoint",
                "arguments": {
                    "journal_path": str(j_path),
                    "expected_task_ids": ["step_1", "step_2"],
                },
            },
        }
    )
    assert inspect_resp is not None
    inspect_data = json.loads(inspect_resp["result"]["content"][0]["text"])
    assert inspect_data["integrity"]["valid"] is True
    assert inspect_data["completed_task_ids"] == ["step_1"]
    assert inspect_data["pending_task_ids"] == ["step_2"]
    assert inspect_data["tokens_recoverable_on_resume"] == 500

    # Test stdio loop with valid + malformed JSON lines
    stdin_buf = io.StringIO(
        '{"jsonrpc": "2.0", "id": 99, "method": "ping"}\n'
        'not-valid-json\n'
        '{"jsonrpc": "2.0", "id": 100, "method": "unknown_method"}\n'
    )
    stdout_buf = io.StringIO()
    server.serve_stdio(stdin=stdin_buf, stdout=stdout_buf)
    out_lines = [json.loads(line) for line in stdout_buf.getvalue().splitlines()]
    assert len(out_lines) == 3
    assert out_lines[0]["id"] == 99
    assert out_lines[1]["error"]["code"] == -32700
    assert out_lines[2]["error"]["code"] == -32601


def test_cli_commands_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", "--json"]) == 0
    plan_out = json.loads(capsys.readouterr().out)
    assert plan_out["total_tasks"] == 10

    assert main(["demo", "--json"]) == 0
    demo_out = json.loads(capsys.readouterr().out)
    assert demo_out["run_1_crash_simulation"]["status"] == "FAILED"
    assert demo_out["run_2_checkpoint_resume"]["status"] == "COMPLETED"
    assert demo_out["journal_integrity"]["valid"] is True

    j_file = tmp_path / "cli_journal.jsonl"
    journal = CheckpointJournal(journal_path=j_file)
    journal.append("t1", "d1", {"v": 1}, tokens_used=100)
    assert main(["verify-journal", str(j_file), "--json"]) == 0
    verify_out = json.loads(capsys.readouterr().out)
    assert verify_out["valid"] is True


def test_benchmark_suite_assertions() -> None:
    metrics = run_benchmark_suite()
    d1 = metrics["dag_compilation_200_tasks"]
    d2 = metrics["parallel_conflict_guard_50_tasks"]
    d3 = metrics["crash_recovery_20_workflows"]
    d4 = metrics["cycle_and_contract_detection_15_cases"]

    assert d1["task_count"] == 200
    assert d1["p50_ms"] < 50.0
    assert d2["race_conditions_observed"] == 0
    assert d2["collision_prevention_rate_pct"] == 100.0
    assert d3["token_savings_pct"] == 70.0
    assert d3["avg_replay_ms_per_node"] < 0.5
    assert d4["recall_pct"] == 100.0

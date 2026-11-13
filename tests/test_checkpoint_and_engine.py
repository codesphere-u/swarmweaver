"""
Tests for CheckpointJournal hash-chain integrity and SwarmOrchestrator crash-safe resume.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from swarmweaver.checkpoint import (
    GENESIS_HASH,
    CheckpointIntegrityError,
    CheckpointJournal,
)
from swarmweaver.engine import SwarmOrchestrator
from swarmweaver.models import TaskSpec, TaskStatus


def test_checkpoint_journal_hash_chaining_and_disk_persistence(tmp_path: Path) -> None:
    journal_file = tmp_path / "workflow.jsonl"
    journal = CheckpointJournal(journal_path=journal_file, workflow_id="wf-hash-test")

    d1 = journal.compute_input_digest("t1", {"a": 1})
    r1 = journal.append("t1", d1, {"out1": "ok"}, tokens_used=400, elapsed_ms=12.5)
    assert r1.sequence == 0
    assert r1.prev_hash == GENESIS_HASH

    d2 = journal.compute_input_digest("t2", {"out1": "ok"})
    r2 = journal.append("t2", d2, {"out2": [1, 2]}, tokens_used=600, elapsed_ms=18.2)
    assert r2.sequence == 1
    assert r2.prev_hash == r1.entry_hash

    # Reload from disk in a fresh CheckpointJournal instance
    reloaded = CheckpointJournal(journal_path=journal_file)
    verification = reloaded.verify_integrity()
    assert verification.valid is True
    assert verification.records_checked == 2
    assert verification.head_hash == r2.entry_hash
    assert reloaded.lookup("t1", d1) == r1
    assert reloaded.lookup("t2", d2) == r2


def test_checkpoint_journal_detects_payload_tampering(tmp_path: Path) -> None:
    journal_file = tmp_path / "tamper.jsonl"
    journal = CheckpointJournal(journal_path=journal_file, workflow_id="wf-tamper")
    journal.append("t1", "dig1", {"result": "authentic"}, tokens_used=250)
    journal.append("t2", "dig2", {"result": "step2"}, tokens_used=300)

    # Tamper with record 0's output_payload on disk without updating hashes
    lines = journal_file.read_text(encoding="utf-8").splitlines()
    rec0 = json.loads(lines[0])
    rec0["output_payload"]["result"] = "corrupted_by_attacker"
    lines[0] = json.dumps(rec0)
    journal_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    tampered_journal = CheckpointJournal(journal_path=journal_file)
    res = tampered_journal.verify_integrity()
    assert res.valid is False
    assert res.tampered_sequence == 0
    assert "Payload tampering detected" in (res.error or "")

    with pytest.raises(CheckpointIntegrityError):
        tampered_journal.verify_integrity(raise_on_error=True)


def test_orchestrator_crash_and_deterministic_zero_token_resume(tmp_path: Path) -> None:
    journal_path = tmp_path / "resume_test.jsonl"
    tasks = [
        TaskSpec(
            task_id=f"node_{i:02d}",
            title=f"Step {i}",
            depends_on=[f"node_{i - 1:02d}"] if i > 0 else [],
            read_globs=[f"src/m{i}.py"],
            write_globs=[f"src/m{i}.py"],
            estimated_ms=50,
            estimated_tokens=500,
            input_contract=[f"k_{i - 1}:dict"] if i > 0 else [],
            output_contract=[f"k_{i}:dict"],
            retry_budget=0,
        )
        for i in range(6)
    ]

    # Run 1: Inject crash at node_04 (nodes 00, 01, 02, 03 complete = 2,000 tokens)
    orch1 = SwarmOrchestrator(tasks, workflow_id="wf-resume", journal_path=journal_path)
    rep1 = orch1.execute(fail_at_task="node_04", resume=False)
    assert rep1.status == "FAILED"
    assert rep1.failed_task == "node_04"
    assert rep1.completed_tasks == ["node_00", "node_01", "node_02", "node_03"]
    assert rep1.tokens_spent == 2000
    assert rep1.task_statuses["node_05"] == TaskStatus.SKIPPED.value

    # Run 2: Resume workflow from journal; nodes 00..03 replay with 0 tokens spent
    orch2 = SwarmOrchestrator(tasks, workflow_id="wf-resume", journal_path=journal_path)
    rep2 = orch2.execute(resume=True)
    assert rep2.status == "COMPLETED"
    assert rep2.replayed_tasks == ["node_00", "node_01", "node_02", "node_03"]
    assert rep2.tokens_saved_by_replay == 2000
    assert rep2.tokens_spent == 1000  # Only node_04 and node_05 executed fresh!
    assert rep2.checkpoint_chain_valid is True


def test_orchestrator_retry_budget_recovers_transient_error() -> None:
    tasks = [
        TaskSpec(
            task_id="flaky_task",
            title="Flaky RPC",
            output_contract=["rpc_out:dict"],
            retry_budget=2,
        )
    ]
    attempts = {"count": 0}

    def flaky_executor(spec: TaskSpec, _inputs: dict) -> dict:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise TimeoutError("Transient network timeout")
        return {"rpc_out": {"attempt": attempts["count"]}}

    orch = SwarmOrchestrator(tasks)
    report = orch.execute(executor=flaky_executor)
    assert report.status == "COMPLETED"
    assert report.retries_used["flaky_task"] == 2
    assert report.blackboard_snapshot["rpc_out"] == {"attempt": 3}


def test_orchestrator_recomputes_when_input_digest_changes(tmp_path: Path) -> None:
    journal_path = tmp_path / "digest_change.jsonl"
    tasks = [
        TaskSpec(
            task_id="consumer",
            title="Consumer",
            input_contract=["config:dict"],
            output_contract=["built:dict"],
            estimated_tokens=750,
        )
    ]

    orch1 = SwarmOrchestrator(
        tasks,
        journal_path=journal_path,
        initial_blackboard={"config": {"version": 1}},
    )
    r1 = orch1.execute(resume=True)
    assert r1.tokens_spent == 750
    assert r1.replayed_tasks == []

    # Same input -> replayed with 0 tokens
    orch2 = SwarmOrchestrator(
        tasks,
        journal_path=journal_path,
        initial_blackboard={"config": {"version": 1}},
    )
    r2 = orch2.execute(resume=True)
    assert r2.tokens_spent == 0
    assert r2.replayed_tasks == ["consumer"]

    # Modified upstream input -> input_digest changes -> re-executes instead of stale replay
    orch3 = SwarmOrchestrator(
        tasks,
        journal_path=journal_path,
        initial_blackboard={"config": {"version": 2}},
    )
    r3 = orch3.execute(resume=True)
    assert r3.tokens_spent == 750
    assert r3.replayed_tasks == []

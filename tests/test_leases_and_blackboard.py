"""
Tests for FileScopeLeaseManager concurrency/TTL/revocation and TypedBlackboard contracts.
"""

from __future__ import annotations

import pytest

from swarmweaver.blackboard import BlackboardContractError, TypedBlackboard
from swarmweaver.leases import FileScopeLeaseManager, LeaseAcquisitionError
from swarmweaver.models import LeaseMode


def test_lease_manager_allows_shared_readers_and_blocks_writers() -> None:
    mgr = FileScopeLeaseManager(default_ttl_ms=10_000)

    g1 = mgr.acquire("reader_1", read_scopes=["src/auth/jwt.py"])
    g2 = mgr.acquire("reader_2", read_scopes=["src/auth/*.py"])
    assert mgr.validate_token(g1.lease_token, required_scope="src/auth/jwt.py")
    assert mgr.validate_token(g2.lease_token, required_scope="src/auth/jwt.py")

    # Writer attempting to modify src/auth/jwt.py must be blocked while readers hold leases
    with pytest.raises(LeaseAcquisitionError) as exc_info:
        mgr.acquire("writer_1", write_scopes=["src/auth/jwt.py"])

    conflict = exc_info.value.conflict
    assert conflict.requesting_task_id == "writer_1"
    assert conflict.requested_mode == LeaseMode.EXCLUSIVE_WRITE.value
    assert conflict.held_mode == LeaseMode.SHARED_READ.value
    assert len(mgr.conflicts_log) == 1

    # Once readers release, writer can acquire
    assert mgr.release(g1.lease_token) is True
    assert mgr.release_task("reader_2") is True

    gw = mgr.acquire("writer_1", write_scopes=["src/auth/jwt.py"])
    assert mgr.validate_token(gw.lease_token, required_scope="src/auth/jwt.py", require_write=True)


def test_lease_manager_blocks_read_during_exclusive_write() -> None:
    mgr = FileScopeLeaseManager()
    mgr.acquire("writer_a", write_scopes=["src/db/**"])

    with pytest.raises(LeaseAcquisitionError) as exc_info:
        mgr.acquire("reader_b", read_scopes=["src/db/models.py"])
    assert exc_info.value.conflict.held_mode == LeaseMode.EXCLUSIVE_WRITE.value
    assert exc_info.value.conflict.requested_mode == LeaseMode.SHARED_READ.value


def test_lease_manager_ttl_expiration_and_renewal() -> None:
    mgr = FileScopeLeaseManager(default_ttl_ms=100)
    t0 = 1_000_000_000

    grant = mgr.acquire("task_1", write_scopes=["src/app.py"], ttl_ms=50, now_ns=t0)
    assert not grant.is_expired(t0 + 20_000_000)

    # Renew lease before expiration
    renewed = mgr.renew(grant.lease_token, additional_ttl_ms=200, now_ns=t0 + 30_000_000)
    assert renewed.expires_at_ns == t0 + 30_000_000 + 200_000_000

    # Advance clock past renewed expiration; another task can now acquire the scope cleanly
    later_ns = renewed.expires_at_ns + 1
    grant2 = mgr.acquire("task_2", write_scopes=["src/app.py"], now_ns=later_ns)
    assert grant2.task_id == "task_2"
    assert not mgr.validate_token(grant.lease_token, now_ns=later_ns)


def test_lease_manager_symbol_level_isolation() -> None:
    mgr = FileScopeLeaseManager()
    g1 = mgr.acquire("agent_1", write_scopes=["src/service.py::compute_tax"])
    g2 = mgr.acquire("agent_2", write_scopes=["src/service.py::format_invoice"])
    assert len(mgr.active_grants) == 2

    # Whole-file write conflicts with symbol write
    with pytest.raises(LeaseAcquisitionError):
        mgr.acquire("agent_3", write_scopes=["src/service.py"])

    mgr.release(g1.lease_token)
    mgr.release(g2.lease_token)


def test_blackboard_write_read_version_and_digest_lineage() -> None:
    bb = TypedBlackboard({"seed_config": {"env": "prod"}})
    assert bb.read("seed_config") == {"env": "prod"}

    e1 = bb.write("auth_plan:dict", {"flows": ["oauth2"]}, produced_by_task="architect")
    assert e1.version == 1
    assert e1.produced_by_task == "architect"
    assert len(e1.sha256_digest) == 64

    e2 = bb.write("auth_plan:dict", {"flows": ["oauth2", "oidc"]}, produced_by_task="reviewer")
    assert e2.version == 2
    assert e2.sha256_digest != e1.sha256_digest
    assert len(bb.history) == 3


def test_blackboard_enforces_inline_and_registered_schemas() -> None:
    bb = TypedBlackboard()
    bb.register_schema("coverage_pct", lambda x: isinstance(x, (int, float)) and 0 <= x <= 100)

    bb.write("coverage_pct", 94.5, produced_by_task="qa")
    with pytest.raises(BlackboardContractError, match="custom schema validator"):
        bb.write("coverage_pct", 140.0, produced_by_task="qa")

    with pytest.raises(BlackboardContractError, match="expected dict, got list"):
        bb.write("schema:dict", [1, 2, 3], produced_by_task="coder")

    with pytest.raises(BlackboardContractError, match="expected int, got bool"):
        bb.write("count:int", False, produced_by_task="coder")


def test_blackboard_input_and_output_contracts() -> None:
    bb = TypedBlackboard()
    with pytest.raises(BlackboardContractError, match="missing from the blackboard"):
        bb.verify_input_contract("task_b", ["upstream_data:dict"])

    bb.write_many(
        {"upstream_data:dict": {"ok": True}, "items:list": ["a", "b"]},
        produced_by_task="task_a",
    )
    scoped = bb.verify_input_contract("task_b", ["upstream_data:dict", "items:list"])
    assert scoped == {"upstream_data": {"ok": True}, "items": ["a", "b"]}

    # Output contract requires that the task itself produced the key
    with pytest.raises(BlackboardContractError, match="did not produce required output key"):
        bb.verify_output_contract("task_b", ["upstream_data:dict"])

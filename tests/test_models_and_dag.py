"""
Tests for SwarmWeaver domain models, scope overlap matching, and DAGCompiler wave planning.
"""

from __future__ import annotations

import pytest

from swarmweaver.dag import (
    CycleDetectedError,
    DAGCompiler,
    DAGValidationError,
    find_task_pair_conflict,
    normalize_scope_pattern,
    scopes_overlap,
)
from swarmweaver.models import TaskSpec


def test_task_spec_validation_and_normalization() -> None:
    spec = TaskSpec(
        task_id="t1",
        title="Refactor auth",
        agent_role="coder",
        depends_on=["t0", "t0"],
        read_globs=["src/a.py", "src/a.py"],
        write_globs=["src/b.py", "src/b.py"],
        input_contract=["in_a", "in_a"],
        output_contract=["out_b", "out_b"],
    )
    assert spec.depends_on == ["t0"]
    assert spec.read_globs == ["src/a.py"]
    assert spec.write_globs == ["src/b.py"]
    assert spec.input_contract == ["in_a"]
    assert spec.output_contract == ["out_b"]

    roundtrip = TaskSpec.from_dict(spec.to_dict())
    assert roundtrip == spec


@pytest.mark.parametrize(
    ("kwargs", "match_msg"),
    [
        ({"task_id": "", "title": "Valid"}, "task_id"),
        ({"task_id": "   ", "title": "Valid"}, "task_id"),
        ({"task_id": "t1", "title": ""}, "title"),
        ({"task_id": "t1", "title": "Valid", "agent_role": " "}, "agent_role"),
        ({"task_id": "t1", "title": "Valid", "estimated_ms": -5}, "estimated_ms"),
        ({"task_id": "t1", "title": "Valid", "estimated_tokens": -1}, "estimated_tokens"),
        ({"task_id": "t1", "title": "Valid", "retry_budget": -2}, "retry_budget"),
    ],
)
def test_task_spec_rejects_invalid_fields(kwargs: dict, match_msg: str) -> None:
    with pytest.raises(ValueError, match=match_msg):
        TaskSpec(**kwargs)


@pytest.mark.parametrize(
    ("pat_a", "pat_b", "expected"),
    [
        ("src/auth/jwt.py", "src/auth/jwt.py", True),
        ("./src/auth/jwt.py", "src/auth/jwt.py", True),
        ("src/auth/*.py", "src/auth/jwt.py", True),
        ("src/**/*.py", "src/api/v1/routes.py", True),
        ("src/auth", "src/auth/jwt.py", True),
        ("src/auth/**", "src/auth/keys/rotate.py", True),
        ("src/auth/*.py", "src/*/jwt.py", True),
        ("src/auth/jwt.py::verify_token", "src/auth/jwt.py", True),
        ("src/auth/jwt.py::verify_*", "src/auth/jwt.py::verify_token", True),
        ("src/auth/jwt.py::verify_token", "src/auth/jwt.py::encode_token", False),
        ("src/auth/jwt.py", "src/billing/invoice.py", False),
        ("src/auth/*.py", "tests/auth/*.py", False),
        ("src/api/*.ts", "src/api/*.py", False),
    ],
)
def test_scopes_overlap_matrix(pat_a: str, pat_b: str, expected: bool) -> None:
    assert scopes_overlap(pat_a, pat_b) is expected
    assert scopes_overlap(pat_b, pat_a) is expected


def test_normalize_scope_pattern() -> None:
    assert normalize_scope_pattern("./src//auth/jwt.py::parse ") == ("src/auth/jwt.py", "parse")
    assert normalize_scope_pattern("src/auth/jwt.py") == ("src/auth/jwt.py", "*")
    assert normalize_scope_pattern("./") == (".", "*")


def test_find_task_pair_conflict_types() -> None:
    t_writer_1 = TaskSpec(task_id="w1", title="W1", write_globs=["src/auth/*.py"])
    t_writer_2 = TaskSpec(task_id="w2", title="W2", write_globs=["src/auth/jwt.py"])
    t_reader_1 = TaskSpec(task_id="r1", title="R1", read_globs=["src/auth/jwt.py"])
    t_reader_2 = TaskSpec(task_id="r2", title="R2", read_globs=["src/auth/*.py"])

    ww = find_task_pair_conflict(t_writer_1, t_writer_2)
    assert ww is not None
    assert ww.conflict_type == "WRITE_WRITE"

    rw = find_task_pair_conflict(t_writer_1, t_reader_1)
    assert rw is not None
    assert rw.conflict_type == "READ_WRITE"

    # Two shared readers never conflict
    assert find_task_pair_conflict(t_reader_1, t_reader_2) is None


def test_dag_compiler_empty_graph() -> None:
    plan = DAGCompiler([]).compile()
    assert plan.total_tasks == 0
    assert plan.waves == []
    assert plan.parallel_speedup_factor == 1.0


def test_dag_compiler_detects_duplicate_and_missing_deps() -> None:
    with pytest.raises(DAGValidationError, match="Duplicate task_id"):
        DAGCompiler([TaskSpec(task_id="a", title="1"), TaskSpec(task_id="a", title="2")])

    with pytest.raises(DAGValidationError, match="unknown task_id 'missing'"):
        DAGCompiler([TaskSpec(task_id="a", title="1", depends_on=["missing"])]).compile()


def test_dag_compiler_detects_self_dependency_and_exact_cycle_path() -> None:
    with pytest.raises(CycleDetectedError) as exc_self:
        DAGCompiler([TaskSpec(task_id="self_node", title="Self", depends_on=["self_node"])]).compile()
    assert exc_self.value.cycle_path == ["self_node", "self_node"]

    tasks = [
        TaskSpec(task_id="A", title="Task A", depends_on=["C"]),
        TaskSpec(task_id="B", title="Task B", depends_on=["A"]),
        TaskSpec(task_id="C", title="Task C", depends_on=["B"]),
    ]
    with pytest.raises(CycleDetectedError) as exc_cycle:
        DAGCompiler(tasks).compile()
    assert exc_cycle.value.cycle_path == ["A", "B", "C", "A"]
    assert "A -> B -> C -> A" in str(exc_cycle.value)


def test_dag_compiler_splits_conflicting_tasks_into_subwaves() -> None:
    tasks = [
        TaskSpec(task_id="root", title="Root", write_globs=["src/init.py"], estimated_ms=100),
        TaskSpec(
            task_id="branch_a",
            title="Branch A",
            depends_on=["root"],
            write_globs=["src/shared.py"],
            estimated_ms=200,
        ),
        TaskSpec(
            task_id="branch_b",
            title="Branch B (conflicts with A)",
            depends_on=["root"],
            write_globs=["src/shared.py"],
            estimated_ms=150,
        ),
        TaskSpec(
            task_id="branch_c",
            title="Branch C (independent file)",
            depends_on=["root"],
            write_globs=["src/other.py"],
            estimated_ms=120,
        ),
    ]
    plan = DAGCompiler(tasks).compile()
    assert len(plan.topological_levels) == 2
    assert len(plan.conflicts_resolved) == 1
    assert plan.conflicts_resolved[0].conflict_type == "WRITE_WRITE"
    # Level 1 was split into 2 sub-waves because branch_a and branch_b both write src/shared.py
    assert plan.total_waves == 3
    for wave in plan.waves:
        assert not ("branch_a" in wave and "branch_b" in wave)


def test_dag_compiler_validates_handoff_contracts() -> None:
    valid_tasks = [
        TaskSpec(task_id="t1", title="T1", output_contract=["plan:dict"]),
        TaskSpec(task_id="t2", title="T2", depends_on=["t1"], input_contract=["plan:dict"]),
    ]
    plan = DAGCompiler(valid_tasks, validate_contracts=True).compile()
    assert plan.total_tasks == 2

    broken_tasks = [
        TaskSpec(task_id="t1", title="T1", output_contract=["plan:dict"]),
        TaskSpec(
            task_id="t2",
            title="T2",
            depends_on=["t1"],
            input_contract=["non_existent_artifact:dict"],
        ),
    ]
    with pytest.raises(DAGValidationError, match="unproduced input_contract"):
        DAGCompiler(broken_tasks, validate_contracts=True).compile()

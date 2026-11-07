"""
Reproducible Benchmark Suite for SwarmWeaver Multi-Agent DAG Orchestration.
"""

from __future__ import annotations

import statistics
import time
from typing import Any

from swarmweaver.blackboard import BlackboardContractError, TypedBlackboard
from swarmweaver.checkpoint import CheckpointJournal
from swarmweaver.dag import CycleDetectedError, DAGCompiler, DAGValidationError
from swarmweaver.engine import SwarmOrchestrator
from swarmweaver.leases import FileScopeLeaseManager, LeaseAcquisitionError
from swarmweaver.models import TaskSpec


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


def _build_200_task_dag() -> list[TaskSpec]:
    """
    Constructs a realistic 200-task multi-agent software engineering DAG across 10 layers
    (20 tasks per layer) with controlled read/write file-glob overlaps.
    """
    tasks: list[TaskSpec] = []
    roles = ["architect", "backend", "frontend", "security", "qa"]
    modules = ["auth", "billing", "catalog", "orders", "telemetry", "search", "notifications", "admin"]

    for layer in range(10):
        for idx in range(20):
            tid = f"L{layer:02d}_T{idx:02d}"
            deps: list[str] = []
            if layer > 0:
                deps = [
                    f"L{layer - 1:02d}_T{idx:02d}",
                    f"L{layer - 1:02d}_T{(idx + 1) % 20:02d}",
                ]
            mod = modules[idx % len(modules)]
            # Create deliberate overlaps among tasks sharing the same module index % 8
            sub_group = idx // len(modules)
            if sub_group == 0:
                write_globs = [f"src/{mod}/service_{layer}.py"]
                read_globs = [f"src/{mod}/models.py"]
            elif sub_group == 1:
                # Overlaps with sub_group 0 on service_{layer}.py
                write_globs = [f"src/{mod}/service_{layer}.py", f"src/{mod}/routes_{layer}.py"]
                read_globs = [f"src/{mod}/schema.py"]
            else:
                # Reads service_{layer}.py (read-write conflict with writers in same level)
                write_globs = [f"tests/{mod}/test_layer_{layer}_{idx}.py"]
                read_globs = [f"src/{mod}/service_{layer}.py"]

            tasks.append(
                TaskSpec(
                    task_id=tid,
                    title=f"Layer {layer} Task {idx} ({mod})",
                    agent_role=roles[idx % len(roles)],
                    depends_on=deps,
                    read_globs=read_globs,
                    write_globs=write_globs,
                    estimated_ms=120 + (idx * 5),
                    estimated_tokens=600 + (idx * 25),
                    input_contract=[],
                    output_contract=[f"out_{tid}:dict"],
                )
            )
    return tasks


def benchmark_dag_compilation(iterations: int = 15) -> dict[str, Any]:
    """
    1. Evaluates 200-task multi-agent DAG compilation & conflict-free wave scheduling latency.
    """
    tasks = _build_200_task_dag()
    latencies_ms: list[float] = []
    last_plan = None

    for _ in range(max(1, iterations)):
        t0 = time.perf_counter_ns()
        compiler = DAGCompiler(tasks)
        last_plan = compiler.compile()
        elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000.0
        latencies_ms.append(elapsed_ms)

    latencies_ms.sort()
    assert last_plan is not None
    return {
        "task_count": len(tasks),
        "iterations": len(latencies_ms),
        "topological_levels": len(last_plan.topological_levels),
        "compiled_waves": last_plan.total_waves,
        "conflicts_auto_split": len(last_plan.conflicts_resolved),
        "p50_ms": round(_percentile(latencies_ms, 50.0), 3),
        "p95_ms": round(_percentile(latencies_ms, 95.0), 3),
        "mean_ms": round(statistics.mean(latencies_ms), 3),
        "parallel_speedup_factor": round(last_plan.parallel_speedup_factor, 2),
    }


def benchmark_parallel_conflict_guard() -> dict[str, Any]:
    """
    2. Evaluates 50 parallel conflicting file-edit tasks under both static wave splitting
    and runtime FileScopeLeaseManager lock contention.
    Verifies 0 race conditions and 100% write-write & read-write collision prevention.
    """
    shared_targets = [
        "src/core/router.py",
        "src/auth/jwt.py",
        "src/db/migrations/*.sql",
        "src/api/v1/handlers.py",
        "src/billing/ledger.py",
    ]

    tasks: list[TaskSpec] = []
    for i in range(50):
        target = shared_targets[i % len(shared_targets)]
        if i % 3 == 0:
            # Writer on exact or wildcard target
            w_globs = [target]
            r_globs = ["src/config/settings.py"]
        elif i % 3 == 1:
            # Second writer on overlapping glob/symbol scope
            base_dir = target.rsplit("/", 1)[0]
            w_globs = [f"{base_dir}/**"]
            r_globs = []
        else:
            # Reader on target being modified by writers
            w_globs = [f"tests/unit/test_case_{i}.py"]
            r_globs = [target]

        tasks.append(
            TaskSpec(
                task_id=f"edit_task_{i:02d}",
                title=f"Parallel file task {i}",
                agent_role="coder",
                depends_on=[],
                read_globs=r_globs,
                write_globs=w_globs,
                estimated_ms=80,
                estimated_tokens=400,
                output_contract=[f"patch_{i:02d}:dict"],
            )
        )

    compiler = DAGCompiler(tasks)
    plan = compiler.compile()

    # Verify that within EVERY compiled sub-wave, zero pairwise conflicts exist
    unprevented_wave_races = 0
    for wave in plan.waves:
        wave_specs = [compiler.tasks_by_id[tid] for tid in wave]
        wave_conflicts = DAGCompiler.check_parallel_conflicts(wave_specs)
        unprevented_wave_races += len(wave_conflicts)

    # Also test direct runtime FileScopeLeaseManager contention across all 50 tasks simultaneously
    lease_mgr = FileScopeLeaseManager(default_ttl_ms=60_000)
    granted_simultaneously: list[TaskSpec] = []
    blocked_by_lease_guard = 0

    for spec in tasks:
        try:
            lease_mgr.acquire(
                spec.task_id,
                read_scopes=spec.read_globs,
                write_scopes=spec.write_globs,
            )
            granted_simultaneously.append(spec)
        except LeaseAcquisitionError:
            blocked_by_lease_guard += 1

    # Verify that the simultaneously granted tasks have 0 conflicts among them
    runtime_races = len(DAGCompiler.check_parallel_conflicts(granted_simultaneously))
    total_races = unprevented_wave_races + runtime_races

    return {
        "parallel_tasks_tested": len(tasks),
        "static_conflicts_detected_and_split": len(plan.conflicts_resolved),
        "compiled_safe_subwaves": plan.total_waves,
        "runtime_lease_collisions_blocked": blocked_by_lease_guard,
        "race_conditions_observed": total_races,
        "collision_prevention_rate_pct": 100.0 if total_races == 0 else 0.0,
    }


def benchmark_crash_recovery_workflows(workflow_count: int = 20, tasks_per_workflow: int = 10) -> dict[str, Any]:
    """
    3. Simulates 20 multi-step workflows (10 tasks each = 200 task runs) where an injected
    failure occurs at 70% completion (after node 7 of 10), followed by `resume=True`.
    Measures exact token savings (~70%) and per-node checkpoint replay latency (<0.05 ms/node).
    """
    crash_index = int(tasks_per_workflow * 0.7)  # 7 of 10 completed (70%)
    total_baseline_tokens = 0
    total_resume_tokens_spent = 0
    total_tokens_saved = 0
    total_replayed_nodes = 0
    total_replay_elapsed_ms = 0.0
    integrity_verified_count = 0

    for wf_idx in range(workflow_count):
        tasks: list[TaskSpec] = []
        for t_idx in range(tasks_per_workflow):
            tid = f"wf{wf_idx:02d}_step_{t_idx:02d}"
            deps = [f"wf{wf_idx:02d}_step_{t_idx - 1:02d}"] if t_idx > 0 else []
            in_contract = [f"step_{t_idx - 1:02d}_out:dict"] if t_idx > 0 else []
            out_contract = [f"step_{t_idx:02d}_out:dict"]
            tasks.append(
                TaskSpec(
                    task_id=tid,
                    title=f"Workflow {wf_idx} Step {t_idx}",
                    agent_role="refactoring-agent",
                    depends_on=deps,
                    read_globs=[f"src/mod_{wf_idx}/part_{t_idx}.py"],
                    write_globs=[f"src/mod_{wf_idx}/part_{t_idx}.py"],
                    estimated_ms=100,
                    estimated_tokens=1000,
                    input_contract=in_contract,
                    output_contract=out_contract,
                    retry_budget=0,
                )
            )

        journal = CheckpointJournal(workflow_id=f"bench-wf-{wf_idx:02d}")
        fail_task_id = f"wf{wf_idx:02d}_step_{crash_index:02d}"

        # Run 1: Crashes at node 7 (after 70% completion)
        orch_crash = SwarmOrchestrator(
            tasks,
            workflow_id=f"bench-wf-{wf_idx:02d}",
            journal=journal,
        )
        crash_report = orch_crash.execute(fail_at_task=fail_task_id, resume=False)
        assert crash_report.status == "FAILED"
        assert len(crash_report.completed_tasks) == crash_index

        # Run 2: Resume from hash-chained CheckpointJournal
        orch_resume = SwarmOrchestrator(
            tasks,
            workflow_id=f"bench-wf-{wf_idx:02d}",
            journal=journal,
        )
        resume_report = orch_resume.execute(resume=True)
        assert resume_report.status == "COMPLETED"
        assert len(resume_report.replayed_tasks) == crash_index

        wf_baseline_tokens = sum(t.estimated_tokens for t in tasks)
        total_baseline_tokens += wf_baseline_tokens
        total_resume_tokens_spent += resume_report.tokens_spent
        total_tokens_saved += resume_report.tokens_saved_by_replay
        total_replayed_nodes += len(resume_report.replayed_tasks)
        total_replay_elapsed_ms += resume_report.replay_elapsed_ms
        if resume_report.checkpoint_chain_valid:
            integrity_verified_count += 1

    avg_replay_ms_per_node = (
        total_replay_elapsed_ms / total_replayed_nodes if total_replayed_nodes > 0 else 0.0
    )
    token_savings_pct = (
        (total_tokens_saved / total_baseline_tokens) * 100.0
        if total_baseline_tokens > 0
        else 0.0
    )

    return {
        "workflows_simulated": workflow_count,
        "tasks_per_workflow": tasks_per_workflow,
        "injected_failure_point_pct": 70.0,
        "replayed_nodes_total": total_replayed_nodes,
        "baseline_tokens_per_run": total_baseline_tokens,
        "resume_tokens_spent": total_resume_tokens_spent,
        "tokens_saved_on_resume": total_tokens_saved,
        "token_savings_pct": round(token_savings_pct, 2),
        "avg_replay_ms_per_node": round(avg_replay_ms_per_node, 5),
        "hash_chain_integrity_verified": f"{integrity_verified_count}/{workflow_count}",
    }


def benchmark_cycle_and_contract_detection() -> dict[str, Any]:
    """
    4. Evaluates cycle & contract violation detection accuracy across 15 malformed DAGs
    and broken handoffs (8 cyclic/invalid dependency graphs + 7 broken blackboard contracts).
    """
    detected_count = 0
    total_cases = 15

    # Cases 1-5: Cycles of lengths 1 (self-loop), 2, 3, 4, and 5
    for cycle_len in range(1, 6):
        nodes = [f"node_{i}" for i in range(cycle_len)]
        specs = [
            TaskSpec(
                task_id=nodes[i],
                title=f"Cycle node {i}",
                depends_on=[nodes[(i - 1) % cycle_len]],
            )
            for i in range(cycle_len)
        ]
        try:
            DAGCompiler(specs).compile()
        except CycleDetectedError:
            detected_count += 1

    # Case 6: Deep diamond graph with back-edge cycle (A -> B -> C -> D -> B)
    diamond_cycle = [
        TaskSpec(task_id="A", title="Root"),
        TaskSpec(task_id="B", title="Mid 1", depends_on=["A", "D"]),
        TaskSpec(task_id="C", title="Mid 2", depends_on=["B"]),
        TaskSpec(task_id="D", title="Leaf with back-edge", depends_on=["C"]),
    ]
    try:
        DAGCompiler(diamond_cycle).compile()
    except CycleDetectedError:
        detected_count += 1

    # Case 7: Missing dependency reference
    try:
        DAGCompiler([TaskSpec(task_id="orphan", title="Orphan", depends_on=["ghost_node"])]).compile()
    except DAGValidationError:
        detected_count += 1

    # Case 8: Duplicate task IDs
    try:
        DAGCompiler(
            [
                TaskSpec(task_id="dup", title="First"),
                TaskSpec(task_id="dup", title="Second"),
            ]
        ).compile()
    except DAGValidationError:
        detected_count += 1

    # Case 9: Compile-time unproduced input_contract key
    try:
        DAGCompiler(
            [
                TaskSpec(task_id="producer", title="Prod", output_contract=["schema_v1:dict"]),
                TaskSpec(
                    task_id="consumer",
                    title="Cons",
                    depends_on=["producer"],
                    input_contract=["missing_schema_v2:dict"],
                ),
            ],
            validate_contracts=True,
        ).compile()
    except DAGValidationError:
        detected_count += 1

    # Case 10: Runtime missing input_contract key on TypedBlackboard
    bb = TypedBlackboard()
    try:
        bb.verify_input_contract("downstream_agent", ["unwritten_key:dict"])
    except BlackboardContractError:
        detected_count += 1

    # Case 11: Runtime inline type mismatch (expected dict, wrote list)
    try:
        bb.write("api_spec:dict", ["not", "a", "dict"], produced_by_task="bad_agent")
    except BlackboardContractError:
        detected_count += 1

    # Case 12: Runtime bool masquerading as int rejected
    try:
        bb.write("worker_count:int", True, produced_by_task="bad_agent")
    except BlackboardContractError:
        detected_count += 1

    # Case 13: Runtime custom schema validator rejection
    bb.register_schema("confidence_score", lambda v: isinstance(v, (int, float)) and 0.0 <= v <= 1.0)
    try:
        bb.write("confidence_score", 1.75, produced_by_task="bad_agent")
    except BlackboardContractError:
        detected_count += 1

    # Case 14: Task completes without producing declared output_contract key
    bb_out = TypedBlackboard()
    try:
        bb_out.verify_output_contract("lazy_agent", ["required_artifact:str"])
    except BlackboardContractError:
        detected_count += 1

    # Case 15: Task tries to claim another task's output as its own output_contract
    bb_spoof = TypedBlackboard()
    bb_spoof.write("shared_report:dict", {"ok": True}, produced_by_task="agent_alpha")
    try:
        bb_spoof.verify_output_contract("agent_beta", ["shared_report:dict"])
    except BlackboardContractError:
        detected_count += 1

    return {
        "malformed_cases_tested": total_cases,
        "violations_detected": detected_count,
        "recall_pct": round((detected_count / total_cases) * 100.0, 2),
    }


def run_benchmark_suite() -> dict[str, Any]:
    """
    Runs the complete 4-part SwarmWeaver benchmark suite and returns structured metrics.
    """
    t0 = time.perf_counter_ns()
    dag_metrics = benchmark_dag_compilation()
    conflict_metrics = benchmark_parallel_conflict_guard()
    recovery_metrics = benchmark_crash_recovery_workflows()
    validation_metrics = benchmark_cycle_and_contract_detection()
    total_ms = (time.perf_counter_ns() - t0) / 1_000_000.0

    return {
        "suite": "swarmweaver-v1.0.0",
        "total_benchmark_elapsed_ms": round(total_ms, 2),
        "dag_compilation_200_tasks": dag_metrics,
        "parallel_conflict_guard_50_tasks": conflict_metrics,
        "crash_recovery_20_workflows": recovery_metrics,
        "cycle_and_contract_detection_15_cases": validation_metrics,
    }

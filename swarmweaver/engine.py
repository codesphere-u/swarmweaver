"""
SwarmOrchestrator — Deterministic Multi-Agent DAG Execution & Crash-Safe Resume Engine.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
from typing import Any

from swarmweaver.blackboard import BlackboardContractError, TypedBlackboard, parse_contract_spec
from swarmweaver.checkpoint import CheckpointJournal
from swarmweaver.dag import DAGCompiler
from swarmweaver.leases import FileScopeLeaseManager
from swarmweaver.models import DAGExecutionReport, TaskSpec, TaskStatus

TaskExecutorFn = Callable[[TaskSpec, dict[str, Any]], dict[str, Any]]


class InjectedCrashError(RuntimeError):
    """Raised when a deterministic crash injection triggers on a target task."""


def default_deterministic_executor(task: TaskSpec, scoped_inputs: dict[str, Any]) -> dict[str, Any]:
    """
    Synthesizes deterministic, contract-compliant outputs for a task when running
    in simulation or dry-run mode.
    """
    outputs: dict[str, Any] = {}
    for contract_spec in task.output_contract:
        clean_key, inline_type = parse_contract_spec(contract_spec)
        if inline_type in ("int", "integer"):
            outputs[clean_key] = len(scoped_inputs) + len(task.write_globs) + 1
        elif inline_type in ("float", "number"):
            outputs[clean_key] = round(float(len(scoped_inputs) + 1) * 0.95, 4)
        elif inline_type in ("bool", "boolean"):
            outputs[clean_key] = True
        elif inline_type in ("list", "array"):
            outputs[clean_key] = [
                f"{task.task_id}:{g}" for g in (task.write_globs or [clean_key])
            ]
        elif inline_type in ("str", "string"):
            outputs[clean_key] = f"verified:{task.task_id}:{clean_key}"
        else:
            outputs[clean_key] = {
                "producer": task.task_id,
                "role": task.agent_role,
                "key": clean_key,
                "inputs_seen": sorted(scoped_inputs.keys()),
                "write_targets": list(task.write_globs),
            }
    if not outputs:
        outputs[f"{task.task_id}_result"] = {
            "producer": task.task_id,
            "role": task.agent_role,
            "status": "ok",
        }
    return outputs


class SwarmOrchestrator:
    """
    End-to-end multi-agent execution engine integrating:
    - `DAGCompiler`: topological sorting, cycle detection & file-glob conflict wave splitting
    - `FileScopeLeaseManager`: runtime read/write lock enforcement & race-condition prevention
    - `CheckpointJournal`: hash-chained persistence & sub-millisecond zero-token replay
    - `TypedBlackboard`: strict input/output contract validation & lineage tracking
    """

    def __init__(
        self,
        tasks: Iterable[TaskSpec],
        *,
        workflow_id: str = "swarm-workflow",
        journal: CheckpointJournal | None = None,
        journal_path: str | Path | None = None,
        blackboard: TypedBlackboard | None = None,
        initial_blackboard: dict[str, Any] | None = None,
        lease_manager: FileScopeLeaseManager | None = None,
        validate_contracts_at_compile: bool = False,
        max_workers: int = 8,
    ) -> None:
        self._workflow_id = workflow_id
        self._task_list = list(tasks)
        self._blackboard = (
            blackboard if blackboard is not None else TypedBlackboard(initial_blackboard)
        )
        self._journal = (
            journal
            if journal is not None
            else CheckpointJournal(journal_path=journal_path, workflow_id=workflow_id)
        )
        self._lease_manager = (
            lease_manager if lease_manager is not None else FileScopeLeaseManager()
        )
        self._compiler = DAGCompiler(
            self._task_list,
            validate_contracts=validate_contracts_at_compile,
            initial_blackboard_keys=self._blackboard.snapshot().keys(),
        )
        self._max_workers = max(1, int(max_workers))

    @property
    def blackboard(self) -> TypedBlackboard:
        return self._blackboard

    @property
    def journal(self) -> CheckpointJournal:
        return self._journal

    @property
    def lease_manager(self) -> FileScopeLeaseManager:
        return self._lease_manager

    @property
    def compiler(self) -> DAGCompiler:
        return self._compiler

    @staticmethod
    def _task_config_fingerprint(task: TaskSpec) -> dict[str, Any]:
        return {
            "agent_role": task.agent_role,
            "read_globs": list(task.read_globs),
            "write_globs": list(task.write_globs),
            "input_contract": list(task.input_contract),
            "output_contract": list(task.output_contract),
        }

    def execute(
        self,
        *,
        executor: TaskExecutorFn | None = None,
        resume: bool = True,
        dry_run: bool = False,
        fail_at_task: str | set[str] | None = None,
        crash_after_completed_count: int | None = None,
        raise_on_failure: bool = False,
    ) -> DAGExecutionReport:
        """
        Compiles and executes the multi-agent DAG wave-by-wave.

        When `resume=True`, completed tasks whose `input_digest` matches an existing
        record in `CheckpointJournal` are replayed deterministically in <0.05 ms/node
        with 0 tokens spent.
        """
        start_ns = time.perf_counter_ns()
        wave_plan = self._compiler.compile()
        tasks_by_id = self._compiler.tasks_by_id

        if resume:
            self._journal.verify_integrity(raise_on_error=True)

        task_fn = default_deterministic_executor if (executor is None or dry_run) else executor

        fail_targets: set[str] = set()
        if isinstance(fail_at_task, str):
            fail_targets.add(fail_at_task)
        elif fail_at_task is not None:
            fail_targets.update(fail_at_task)

        task_statuses: dict[str, str] = {
            tid: TaskStatus.PENDING.value for tid in tasks_by_id
        }
        completed_tasks: list[str] = []
        replayed_tasks: list[str] = []
        retries_used: dict[str, int] = {tid: 0 for tid in tasks_by_id}
        tokens_spent = 0
        tokens_saved_by_replay = 0
        replay_elapsed_ns = 0
        failed_task: str | None = None
        error_message: str | None = None
        newly_executed_count = 0

        def run_single_task(tid: str) -> tuple[str, dict[str, Any], int, float, int]:
            """
            Executes a single non-replayed task with file-scope lease protection,
            blackboard contract validation, and retry budget handling.
            Returns (tid, verified_outputs, tokens_used, task_elapsed_ms, retries_count).
            """
            spec = tasks_by_id[tid]
            scoped_inputs = self._blackboard.verify_input_contract(tid, spec.input_contract)
            input_digest = self._journal.compute_input_digest(
                tid,
                scoped_inputs,
                self._task_config_fingerprint(spec),
            )

            grant = self._lease_manager.acquire(
                tid,
                read_scopes=spec.read_globs,
                write_scopes=spec.write_globs,
            )
            try:
                attempt = 0
                max_attempts = 1 + max(0, spec.retry_budget)
                last_exc: Exception | None = None

                while attempt < max_attempts:
                    t0_ns = time.perf_counter_ns()
                    try:
                        if tid in fail_targets:
                            raise InjectedCrashError(
                                f"Injected crash fault triggered at task {tid!r} (attempt {attempt + 1})."
                            )

                        raw_output = task_fn(spec, dict(scoped_inputs))
                        if not isinstance(raw_output, dict):
                            raise BlackboardContractError(
                                f"Task {tid!r} executor must return a dict, got {type(raw_output).__name__}."
                            )

                        self._blackboard.write_many(raw_output, produced_by_task=tid)
                        verified_out = self._blackboard.verify_output_contract(
                            tid,
                            spec.output_contract,
                        )
                        # Persist all returned keys (including non-contract auxiliary keys)
                        full_payload = dict(raw_output)
                        full_payload.update(verified_out)

                        elapsed_ms = (time.perf_counter_ns() - t0_ns) / 1_000_000.0
                        used_tokens = 0 if dry_run else spec.estimated_tokens
                        self._journal.append(
                            task_id=tid,
                            input_digest=input_digest,
                            output_payload=full_payload,
                            tokens_used=used_tokens,
                            elapsed_ms=elapsed_ms,
                        )
                        return tid, full_payload, used_tokens, elapsed_ms, attempt
                    except BlackboardContractError:
                        # Contract violations are structural errors; do not retry unless custom executor fixes them
                        raise
                    except Exception as exc:
                        last_exc = exc
                        attempt += 1
                        if attempt >= max_attempts:
                            raise
                assert last_exc is not None
                raise last_exc
            finally:
                self._lease_manager.release(grant.lease_token)

        # Execute wave-by-wave
        for wave in wave_plan.waves:
            tasks_to_run: list[str] = []

            # Phase 1: Check deterministic checkpoint replay for tasks in this wave
            for tid in wave:
                spec = tasks_by_id[tid]
                if resume:
                    try:
                        t_rep0 = time.perf_counter_ns()
                        scoped_inputs = self._blackboard.verify_input_contract(
                            tid, spec.input_contract
                        )
                        input_digest = self._journal.compute_input_digest(
                            tid,
                            scoped_inputs,
                            self._task_config_fingerprint(spec),
                        )
                        cached = self._journal.lookup(tid, input_digest)
                        if cached is not None:
                            self._blackboard.write_many(
                                cached.output_payload,
                                produced_by_task=tid,
                            )
                            self._blackboard.verify_output_contract(
                                tid,
                                spec.output_contract,
                            )
                            replay_elapsed_ns += time.perf_counter_ns() - t_rep0
                            task_statuses[tid] = TaskStatus.REPLAYED.value
                            replayed_tasks.append(tid)
                            completed_tasks.append(tid)
                            tokens_saved_by_replay += cached.tokens_used
                            continue
                    except Exception as exc:
                        failed_task = tid
                        error_message = str(exc)
                        task_statuses[tid] = TaskStatus.FAILED.value
                        break

                tasks_to_run.append(tid)

            if failed_task is not None:
                break

            if not tasks_to_run:
                continue

            # Phase 2: Execute remaining tasks in this wave (parallel when multiple tasks & no count crash hook)
            if len(tasks_to_run) == 1 or crash_after_completed_count is not None:
                for tid in tasks_to_run:
                    if (
                        crash_after_completed_count is not None
                        and newly_executed_count >= crash_after_completed_count
                    ):
                        failed_task = tid
                        error_message = (
                            f"Injected crash after {newly_executed_count} completed tasks "
                            f"(halted before {tid!r})."
                        )
                        task_statuses[tid] = TaskStatus.FAILED.value
                        break

                    task_statuses[tid] = TaskStatus.RUNNING.value
                    try:
                        _, _, used_tok, _, retries_cnt = run_single_task(tid)
                        retries_used[tid] = retries_cnt
                        tokens_spent += used_tok
                        task_statuses[tid] = TaskStatus.COMPLETED.value
                        completed_tasks.append(tid)
                        newly_executed_count += 1
                    except Exception as exc:
                        retries_used[tid] = tasks_by_id[tid].retry_budget
                        failed_task = tid
                        error_message = str(exc)
                        task_statuses[tid] = TaskStatus.FAILED.value
                        break
            else:
                for tid in tasks_to_run:
                    task_statuses[tid] = TaskStatus.RUNNING.value

                workers = min(self._max_workers, len(tasks_to_run))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = {tid: pool.submit(run_single_task, tid) for tid in tasks_to_run}
                    for tid in tasks_to_run:
                        fut = futures[tid]
                        try:
                            _, _, used_tok, _, retries_cnt = fut.result()
                            retries_used[tid] = retries_cnt
                            tokens_spent += used_tok
                            task_statuses[tid] = TaskStatus.COMPLETED.value
                            completed_tasks.append(tid)
                            newly_executed_count += 1
                        except Exception as exc:
                            retries_used[tid] = tasks_by_id[tid].retry_budget
                            if failed_task is None:
                                failed_task = tid
                                error_message = str(exc)
                            task_statuses[tid] = TaskStatus.FAILED.value

            if failed_task is not None:
                break

        # Mark remaining pending tasks as SKIPPED if workflow halted early
        if failed_task is not None:
            for tid, st in task_statuses.items():
                if st == TaskStatus.PENDING.value:
                    task_statuses[tid] = TaskStatus.SKIPPED.value

        total_elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
        replay_elapsed_ms = replay_elapsed_ns / 1_000_000.0
        chain_valid = bool(self._journal.verify_integrity())

        report = DAGExecutionReport(
            workflow_id=self._workflow_id,
            status="COMPLETED" if failed_task is None else "FAILED",
            wave_plan=wave_plan,
            task_statuses=task_statuses,
            completed_tasks=completed_tasks,
            replayed_tasks=replayed_tasks,
            failed_task=failed_task,
            error_message=error_message,
            tokens_spent=tokens_spent,
            tokens_saved_by_replay=tokens_saved_by_replay,
            total_elapsed_ms=total_elapsed_ms,
            replay_elapsed_ms=replay_elapsed_ms,
            retries_used=retries_used,
            lease_conflicts_prevented=(
                len(wave_plan.conflicts_resolved) + len(self._lease_manager.conflicts_log)
            ),
            checkpoint_chain_valid=chain_valid,
            blackboard_snapshot=self._blackboard.snapshot(),
        )

        if failed_task is not None and raise_on_failure:
            raise RuntimeError(f"Workflow {self._workflow_id!r} failed at {failed_task!r}: {error_message}")

        return report

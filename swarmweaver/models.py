"""
Immutable, validated domain models for SwarmWeaver multi-agent orchestration.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class LeaseMode(str, Enum):
    """Lock modes supported by the FileScopeLeaseManager."""

    SHARED_READ = "SHARED_READ"
    EXCLUSIVE_WRITE = "EXCLUSIVE_WRITE"


class TaskStatus(str, Enum):
    """Execution lifecycle state of a DAG task node."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    REPLAYED = "REPLAYED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class TaskSpec:
    """
    Immutable specification for an individual agent task in a SwarmWeaver DAG.
    """

    task_id: str
    title: str
    agent_role: str = "worker"
    depends_on: list[str] = field(default_factory=list)
    read_globs: list[str] = field(default_factory=list)
    write_globs: list[str] = field(default_factory=list)
    estimated_ms: int = 100
    estimated_tokens: int = 500
    input_contract: list[str] = field(default_factory=list)
    output_contract: list[str] = field(default_factory=list)
    retry_budget: int = 1

    def __post_init__(self) -> None:
        if not self.task_id or not str(self.task_id).strip():
            raise ValueError("TaskSpec.task_id must be a non-empty string.")
        if not self.title or not str(self.title).strip():
            raise ValueError(f"TaskSpec({self.task_id!r}).title must be a non-empty string.")
        if not self.agent_role or not str(self.agent_role).strip():
            raise ValueError(f"TaskSpec({self.task_id!r}).agent_role must be a non-empty string.")
        if self.estimated_ms < 0:
            raise ValueError(f"TaskSpec({self.task_id!r}).estimated_ms must be >= 0.")
        if self.estimated_tokens < 0:
            raise ValueError(f"TaskSpec({self.task_id!r}).estimated_tokens must be >= 0.")
        if self.retry_budget < 0:
            raise ValueError(f"TaskSpec({self.task_id!r}).retry_budget must be >= 0.")

        # Normalize list fields into deterministic lists of strings
        object.__setattr__(self, "depends_on", list(dict.fromkeys(self.depends_on)))
        object.__setattr__(self, "read_globs", list(dict.fromkeys(self.read_globs)))
        object.__setattr__(self, "write_globs", list(dict.fromkeys(self.write_globs)))
        object.__setattr__(self, "input_contract", list(dict.fromkeys(self.input_contract)))
        object.__setattr__(self, "output_contract", list(dict.fromkeys(self.output_contract)))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskSpec:
        return cls(
            task_id=str(data["task_id"]),
            title=str(data.get("title", data["task_id"])),
            agent_role=str(data.get("agent_role", "worker")),
            depends_on=list(data.get("depends_on", [])),
            read_globs=list(data.get("read_globs", [])),
            write_globs=list(data.get("write_globs", [])),
            estimated_ms=int(data.get("estimated_ms", 100)),
            estimated_tokens=int(data.get("estimated_tokens", 500)),
            input_contract=list(data.get("input_contract", [])),
            output_contract=list(data.get("output_contract", [])),
            retry_budget=int(data.get("retry_budget", 1)),
        )


@dataclass(frozen=True)
class FileConflictEdge:
    """
    Represents a detected read/write or write/write file-glob conflict between two tasks.
    """

    task_a: str
    task_b: str
    conflict_type: str  # "WRITE_WRITE" | "READ_WRITE"
    pattern_a: str
    pattern_b: str
    topological_level: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WavePlan:
    """
    Compiled conflict-free parallel execution schedule produced by DAGCompiler.
    """

    waves: list[list[str]]
    topological_levels: list[list[str]]
    conflicts_resolved: list[FileConflictEdge]
    total_tasks: int
    total_waves: int
    max_parallelism: int
    sequential_ms: int
    critical_path_ms: int
    critical_path_tasks: list[str]
    parallel_speedup_factor: float
    estimated_total_tokens: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "waves": [list(w) for w in self.waves],
            "topological_levels": [list(lvl) for lvl in self.topological_levels],
            "conflicts_resolved": [c.to_dict() for c in self.conflicts_resolved],
            "total_tasks": self.total_tasks,
            "total_waves": self.total_waves,
            "max_parallelism": self.max_parallelism,
            "sequential_ms": self.sequential_ms,
            "critical_path_ms": self.critical_path_ms,
            "critical_path_tasks": list(self.critical_path_tasks),
            "parallel_speedup_factor": round(self.parallel_speedup_factor, 3),
            "estimated_total_tokens": self.estimated_total_tokens,
        }


@dataclass(frozen=True)
class LeaseGrant:
    """
    Cryptographically signed lease token granting a task access to a set of scopes.
    """

    lease_token: str
    task_id: str
    read_scopes: tuple[str, ...]
    write_scopes: tuple[str, ...]
    granted_at_ns: int
    expires_at_ns: int

    def is_expired(self, now_ns: int) -> bool:
        return now_ns >= self.expires_at_ns

    def to_dict(self) -> dict[str, Any]:
        return {
            "lease_token": self.lease_token,
            "task_id": self.task_id,
            "read_scopes": list(self.read_scopes),
            "write_scopes": list(self.write_scopes),
            "granted_at_ns": self.granted_at_ns,
            "expires_at_ns": self.expires_at_ns,
        }


@dataclass(frozen=True)
class LeaseConflict:
    """
    Audit record for a prevented runtime race condition between two tasks.
    """

    requesting_task_id: str
    holding_task_id: str
    requested_scope: str
    requested_mode: str
    held_scope: str
    held_mode: str
    detected_at_ns: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CheckpointRecord:
    """
    Content-addressable, hash-chained journal record for a completed DAG task.
    """

    sequence: int
    workflow_id: str
    task_id: str
    input_digest: str
    output_payload: dict[str, Any]
    tokens_used: int
    elapsed_ms: float
    timestamp_ns: int
    prev_hash: str
    entry_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CheckpointRecord:
        return cls(
            sequence=int(data["sequence"]),
            workflow_id=str(data["workflow_id"]),
            task_id=str(data["task_id"]),
            input_digest=str(data["input_digest"]),
            output_payload=dict(data["output_payload"]),
            tokens_used=int(data["tokens_used"]),
            elapsed_ms=float(data["elapsed_ms"]),
            timestamp_ns=int(data["timestamp_ns"]),
            prev_hash=str(data["prev_hash"]),
            entry_hash=str(data["entry_hash"]),
        )


@dataclass(frozen=True)
class BlackboardEntry:
    """
    Versioned, lineage-tracked entry in the TypedBlackboard state store.
    """

    key: str
    value: Any
    produced_by_task: str
    version: int
    sha256_digest: str
    updated_at_ns: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DAGExecutionReport:
    """
    Complete execution report produced by SwarmOrchestrator.
    """

    workflow_id: str
    status: str  # "COMPLETED" | "FAILED"
    wave_plan: WavePlan
    task_statuses: dict[str, str]
    completed_tasks: list[str]
    replayed_tasks: list[str]
    failed_task: str | None
    error_message: str | None
    tokens_spent: int
    tokens_saved_by_replay: int
    total_elapsed_ms: float
    replay_elapsed_ms: float
    retries_used: dict[str, int]
    lease_conflicts_prevented: int
    checkpoint_chain_valid: bool
    blackboard_snapshot: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "status": self.status,
            "wave_plan": self.wave_plan.to_dict(),
            "task_statuses": dict(self.task_statuses),
            "completed_tasks": list(self.completed_tasks),
            "replayed_tasks": list(self.replayed_tasks),
            "failed_task": self.failed_task,
            "error_message": self.error_message,
            "tokens_spent": self.tokens_spent,
            "tokens_saved_by_replay": self.tokens_saved_by_replay,
            "total_elapsed_ms": round(self.total_elapsed_ms, 4),
            "replay_elapsed_ms": round(self.replay_elapsed_ms, 4),
            "retries_used": dict(self.retries_used),
            "lease_conflicts_prevented": self.lease_conflicts_prevented,
            "checkpoint_chain_valid": self.checkpoint_chain_valid,
            "blackboard_snapshot": dict(self.blackboard_snapshot),
        }

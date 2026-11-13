"""
SwarmWeaver — Deterministic Multi-Agent DAG Orchestrator, File-Scope Lease Conflict Guard
& Crash-Safe Checkpoint Resume Engine (with MCP Server).
"""

from swarmweaver.blackboard import BlackboardContractError, TypedBlackboard
from swarmweaver.checkpoint import CheckpointIntegrityError, CheckpointJournal
from swarmweaver.dag import CycleDetectedError, DAGCompiler, DAGValidationError
from swarmweaver.engine import SwarmOrchestrator
from swarmweaver.leases import FileScopeLeaseManager, LeaseAcquisitionError
from swarmweaver.models import (
    BlackboardEntry,
    CheckpointRecord,
    DAGExecutionReport,
    FileConflictEdge,
    LeaseConflict,
    LeaseGrant,
    LeaseMode,
    TaskSpec,
    TaskStatus,
    WavePlan,
)

__version__ = "1.0.0"

__all__ = [
    "BlackboardContractError",
    "BlackboardEntry",
    "CheckpointIntegrityError",
    "CheckpointJournal",
    "CheckpointRecord",
    "CycleDetectedError",
    "DAGCompiler",
    "DAGExecutionReport",
    "DAGValidationError",
    "FileConflictEdge",
    "FileScopeLeaseManager",
    "LeaseAcquisitionError",
    "LeaseConflict",
    "LeaseGrant",
    "LeaseMode",
    "SwarmOrchestrator",
    "TaskSpec",
    "TaskStatus",
    "TypedBlackboard",
    "WavePlan",
    "__version__",
]

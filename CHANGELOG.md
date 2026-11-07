# Changelog

All notable changes to `swarmweaver` will be documented in this file.

## [1.0.0] - 2026-11-07

### Added
- **`swarmweaver.models`**: Immutable, validated dataclasses (`TaskSpec`, `WavePlan`, `LeaseGrant`, `LeaseConflict`, `CheckpointRecord`, `BlackboardEntry`, `DAGExecutionReport`).
- **`swarmweaver.dag` (`DAGCompiler`)**: Topological wave compiler with exact cycle path detection (`A -> B -> C -> A`), compile-time contract validation, and automatic splitting of write-write and read-write file-glob overlaps into sequential sub-waves.
- **`swarmweaver.leases` (`FileScopeLeaseManager`)**: Thread-safe `SHARED_READ` and `EXCLUSIVE_WRITE` runtime lock manager with canonical deadlock-free ordering, TTL expiration, SHA-256 lease tokens, and `LeaseConflict` audit logging.
- **`swarmweaver.checkpoint` (`CheckpointJournal`)**: Content-addressable JSONL checkpoint engine with SHA-256 hash chaining (`prev_hash -> entry_hash`), tamper detection (`verify_integrity()`), and sub-millisecond zero-token replay.
- **`swarmweaver.blackboard` (`TypedBlackboard`)**: Shared multi-agent state store with inline and custom schema validation, input/output contract enforcement, and SHA-256 version lineage.
- **`swarmweaver.engine` (`SwarmOrchestrator`)**: End-to-end execution engine with retry budgets, crash injection testing, and deterministic resume.
- **`swarmweaver.mcp_server` (`SwarmWeaverMCPServer`)**: JSON-RPC 2.0 Stdio MCP server exposing `swarmweaver_plan_waves`, `swarmweaver_check_file_conflicts`, `swarmweaver_inspect_checkpoint`, and `swarmweaver_run_benchmark`.
- **`swarmweaver.cli` & `swarmweaver.benchmark`**: Full CLI (`plan`, `demo`, `benchmark`, `verify-journal`, `mcp`) and 4-part reproducible benchmark suite.

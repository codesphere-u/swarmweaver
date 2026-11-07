# SwarmWeaver Architecture Guide

## Overview

SwarmWeaver is structured around five composable, zero-dependency primitives that eliminate race conditions, crash amnesia, and unvalidated handoffs in parallel multi-agent workflows:

1. **`swarmweaver.models`**: Frozen, validated dataclasses (`TaskSpec`, `WavePlan`, `LeaseGrant`, `LeaseConflict`, `CheckpointRecord`, `BlackboardEntry`, `DAGExecutionReport`).
2. **`swarmweaver.dag` (`DAGCompiler`)**: Validates task dependency graphs (Kahn's topological sort + iterative DFS cycle path reconstruction) and splits each topological level into conflict-free parallel sub-waves.
3. **`swarmweaver.leases` (`FileScopeLeaseManager`)**: Thread-safe runtime lock manager enforcing `SHARED_READ` and `EXCLUSIVE_WRITE` leases over file paths, directory prefixes, globs, and AST symbols (`path.py::symbol`).
4. **`swarmweaver.checkpoint` (`CheckpointJournal`)**: Content-addressable JSONL journal chained with SHA-256 (`prev_hash -> entry_hash`) enabling `<0.01 ms/node` zero-token replay on crash recovery.
5. **`swarmweaver.blackboard` (`TypedBlackboard`)**: Versioned key-value state store enforcing `input_contract` and `output_contract` schemas and tracking producer lineage.

## Conflict-Aware Wave Splitting Algorithm

Given a DAG $G = (V, E)$:

1. **Topological Level Assignment:**
   - Every root task $u$ with $\text{in\_degree}(u) = 0$ is assigned $\text{level}(u) = 0$.
   - Every downstream task $v$ is assigned $\text{level}(v) = 1 + \max_{(u, v) \in E} \text{level}(u)$.
2. **Intra-Level Conflict Graph Construction:**
   - Within each topological level $L_k$, `DAGCompiler` constructs an undirected conflict graph $C_k = (L_k, E_{\text{conflict}})$ where an edge $(a, b) \in E_{\text{conflict}}$ exists iff `find_task_pair_conflict(a, b)` detects a `WRITE_WRITE` or `READ_WRITE` overlap between their normalized glob/symbol patterns.
3. **Criticality-Ordered Greedy Graph Coloring:**
   - Tasks in $L_k$ are ordered by descending downstream critical-path duration (`downstream_ms`) and ascending `task_id`.
   - Each task is placed into the earliest sub-wave $W_{k, m}$ that contains no conflicting neighbor in $C_k$.

This guarantees that tasks executing concurrently inside any sub-wave $W_{k, m}$ have **zero** overlapping write-write or read-write file scopes.

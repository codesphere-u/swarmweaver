"""
DAGCompiler & Topological Wave Planner with File-Glob Conflict Splitting.
"""

from __future__ import annotations

from collections import defaultdict, deque
from fnmatch import fnmatchcase
import posixpath
from typing import Iterable

from swarmweaver.models import FileConflictEdge, TaskSpec, WavePlan


class DAGValidationError(ValueError):
    """Raised when a task graph has missing dependencies, duplicate IDs, or invalid contracts."""


class CycleDetectedError(DAGValidationError):
    """Raised when a dependency cycle is detected in the task graph."""

    def __init__(self, cycle_path: list[str]) -> None:
        self.cycle_path = list(cycle_path)
        formatted = " -> ".join(self.cycle_path)
        super().__init__(f"Cyclic task dependency detected: {formatted}")


def normalize_scope_pattern(pattern: str) -> tuple[str, str]:
    """
    Splits a scope pattern into (normalized_file_glob, symbol_glob).
    If no `::symbol` suffix is present, `symbol_glob` defaults to `*` (entire file).
    """
    raw = pattern.strip().replace("\\", "/")
    if "::" in raw:
        file_part, symbol_part = raw.split("::", 1)
        symbol_part = symbol_part.strip() or "*"
    else:
        file_part, symbol_part = raw, "*"

    while file_part.startswith("./"):
        file_part = file_part[2:]
    file_part = file_part.rstrip("/")
    if not file_part:
        file_part = "."

    # Normalize redundant slashes/dots while preserving glob tokens
    parts = [p for p in file_part.split("/") if p and p != "."]
    norm_file = "/".join(parts) if parts else "."
    return norm_file, symbol_part


def _segment_matches(seg_a: str, seg_b: str) -> bool:
    """Returns True if two single path segments can match a common segment name."""
    if seg_a == seg_b or seg_a == "*" or seg_b == "*":
        return True
    if fnmatchcase(seg_a, seg_b) or fnmatchcase(seg_b, seg_a):
        return True
    # Check prefix/suffix compatibility for patterns like `test_*.py` vs `*_auth.py`
    if "*" in seg_a and "*" in seg_b:
        a_prefix = seg_a.split("*", 1)[0]
        b_prefix = seg_b.split("*", 1)[0]
        a_suffix = seg_a.rsplit("*", 1)[-1]
        b_suffix = seg_b.rsplit("*", 1)[-1]
        prefix_ok = a_prefix.startswith(b_prefix) or b_prefix.startswith(a_prefix)
        suffix_ok = a_suffix.endswith(b_suffix) or b_suffix.endswith(a_suffix)
        return prefix_ok and suffix_ok
    return False


def _glob_segments_overlap(segs_a: list[str], segs_b: list[str]) -> bool:
    """
    DP/memoized intersection check for two segmented glob paths (supporting `**`).
    """
    memo: dict[tuple[int, int], bool] = {}

    def match(i: int, j: int) -> bool:
        state = (i, j)
        if state in memo:
            return memo[state]

        if i == len(segs_a) and j == len(segs_b):
            return True

        # If one side is exhausted, it can only match if the remaining segments on the other side are '**'
        if i == len(segs_a):
            res = all(s == "**" for s in segs_b[j:])
            memo[state] = res
            return res
        if j == len(segs_b):
            res = all(s == "**" for s in segs_a[i:])
            memo[state] = res
            return res

        sa = segs_a[i]
        sb = segs_b[j]

        if sa == "**" and sb == "**":
            res = match(i + 1, j) or match(i, j + 1) or match(i + 1, j + 1)
        elif sa == "**":
            # '**' can match zero segments (i+1, j) or one/more segments (i, j+1)
            res = match(i + 1, j) or match(i, j + 1)
        elif sb == "**":
            res = match(i, j + 1) or match(i + 1, j)
        elif _segment_matches(sa, sb):
            res = match(i + 1, j + 1)
        else:
            res = False

        memo[state] = res
        return res

    return match(0, 0)


def _has_wildcard(text: str) -> bool:
    return any(ch in text for ch in ("*", "?", "["))


def scopes_overlap(pattern_a: str, pattern_b: str) -> bool:
    """
    Determines whether two file/glob/symbol scope specifications can refer to
    any overlapping file or AST symbol.
    """
    file_a, sym_a = normalize_scope_pattern(pattern_a)
    file_b, sym_b = normalize_scope_pattern(pattern_b)

    # First check symbol compatibility if both target the same file
    symbols_overlap = (
        sym_a == "*"
        or sym_b == "*"
        or sym_a == sym_b
        or fnmatchcase(sym_a, sym_b)
        or fnmatchcase(sym_b, sym_a)
    )
    if not symbols_overlap:
        return False

    if file_a == file_b or file_a in ("*", "**", ".") or file_b in ("*", "**", "."):
        return True

    if fnmatchcase(file_a, file_b) or fnmatchcase(file_b, file_a):
        return True

    # Directory prefix containment check for non-wildcard directory paths
    # e.g., "src/auth" vs "src/auth/jwt.py"
    if not _has_wildcard(file_a) and "." not in posixpath.basename(file_a):
        if file_b.startswith(file_a + "/"):
            return True
    if not _has_wildcard(file_b) and "." not in posixpath.basename(file_b):
        if file_a.startswith(file_b + "/"):
            return True

    segs_a = file_a.split("/")
    segs_b = file_b.split("/")
    return _glob_segments_overlap(segs_a, segs_b)


def find_task_pair_conflict(
    task_a: TaskSpec,
    task_b: TaskSpec,
    topological_level: int = 0,
) -> FileConflictEdge | None:
    """
    Checks whether `task_a` and `task_b` have any write-write or read-write file scope conflict.
    Returns the first `FileConflictEdge` found, or `None` if they are safe to run in parallel.
    """
    # 1. Check WRITE_WRITE conflicts (highest severity)
    for wa in task_a.write_globs:
        for wb in task_b.write_globs:
            if scopes_overlap(wa, wb):
                return FileConflictEdge(
                    task_a=task_a.task_id,
                    task_b=task_b.task_id,
                    conflict_type="WRITE_WRITE",
                    pattern_a=wa,
                    pattern_b=wb,
                    topological_level=topological_level,
                )

    # 2. Check READ_WRITE conflicts (task_a writes what task_b reads, or vice versa)
    for wa in task_a.write_globs:
        for rb in task_b.read_globs:
            if scopes_overlap(wa, rb):
                return FileConflictEdge(
                    task_a=task_a.task_id,
                    task_b=task_b.task_id,
                    conflict_type="READ_WRITE",
                    pattern_a=wa,
                    pattern_b=rb,
                    topological_level=topological_level,
                )

    for ra in task_a.read_globs:
        for wb in task_b.write_globs:
            if scopes_overlap(ra, wb):
                return FileConflictEdge(
                    task_a=task_a.task_id,
                    task_b=task_b.task_id,
                    conflict_type="READ_WRITE",
                    pattern_a=ra,
                    pattern_b=wb,
                    topological_level=topological_level,
                )

    return None


class DAGCompiler:
    """
    Compiles a set of TaskSpec nodes into validated, conflict-free parallel execution waves.
    """

    def __init__(
        self,
        tasks: Iterable[TaskSpec],
        *,
        validate_contracts: bool = False,
        initial_blackboard_keys: Iterable[str] = (),
    ) -> None:
        self._task_list = list(tasks)
        self._validate_contracts = validate_contracts
        self._initial_keys = set(initial_blackboard_keys)
        self._tasks_by_id: dict[str, TaskSpec] = {}

        for task in self._task_list:
            if task.task_id in self._tasks_by_id:
                raise DAGValidationError(f"Duplicate task_id detected: {task.task_id!r}")
            self._tasks_by_id[task.task_id] = task

    @property
    def tasks_by_id(self) -> dict[str, TaskSpec]:
        return dict(self._tasks_by_id)

    def validate(self) -> None:
        """
        Validates dependency existence, absence of self-dependencies or cycles,
        and (optionally) blackboard handoff contracts.
        """
        if not self._tasks_by_id:
            return

        # 1. Check missing dependencies and self-loops
        for task_id, task in self._tasks_by_id.items():
            for dep_id in task.depends_on:
                if dep_id == task_id:
                    raise CycleDetectedError([task_id, task_id])
                if dep_id not in self._tasks_by_id:
                    raise DAGValidationError(
                        f"Task {task_id!r} depends on unknown task_id {dep_id!r}."
                    )

        # 2. Detect cycles with exact cycle path reconstruction (A -> B -> C -> A)
        # In dependency graph, edge u -> v means v depends on u (or u must run before v).
        # Let's traverse dependency edges dep -> task so cycle reads in execution order: A -> B -> C -> A.
        adj: dict[str, list[str]] = defaultdict(list)
        for task_id, task in self._tasks_by_id.items():
            for dep_id in task.depends_on:
                adj[dep_id].append(task_id)
        for node_id in adj:
            adj[node_id].sort()

        color: dict[str, int] = {tid: 0 for tid in self._tasks_by_id}  # 0=unvisited, 1=visiting, 2=done
        parent: dict[str, str] = {}

        for start_node in sorted(self._tasks_by_id):
            if color[start_node] != 0:
                continue

            # Iterative DFS to avoid recursion limit on deep graphs
            stack: list[tuple[str, int]] = [(start_node, 0)]
            color[start_node] = 1

            while stack:
                u, idx = stack[-1]
                neighbors = adj.get(u, [])
                if idx < len(neighbors):
                    v = neighbors[idx]
                    stack[-1] = (u, idx + 1)
                    if color[v] == 1:
                        # Reconstruct exact cycle path v -> ... -> u -> v
                        cycle = [v]
                        cur = u
                        rev_nodes = []
                        while cur != v:
                            rev_nodes.append(cur)
                            cur = parent[cur]
                        cycle.extend(reversed(rev_nodes))
                        cycle.append(v)
                        raise CycleDetectedError(cycle)
                    elif color[v] == 0:
                        parent[v] = u
                        color[v] = 1
                        stack.append((v, 0))
                else:
                    color[u] = 2
                    stack.pop()

        # 3. Validate input/output contracts across ancestor chains if requested
        if self._validate_contracts:
            self.validate_handoff_contracts(self._initial_keys)

    def validate_handoff_contracts(self, initial_keys: Iterable[str] = ()) -> None:
        """
        Verifies that every key in `task.input_contract` is either present in `initial_keys`
        or produced by `output_contract` of at least one ancestor task in `depends_on`.
        """
        seed_keys = set(initial_keys) | self._initial_keys
        ancestors_cache: dict[str, set[str]] = {}

        def get_ancestors(tid: str) -> set[str]:
            if tid in ancestors_cache:
                return ancestors_cache[tid]
            spec = self._tasks_by_id[tid]
            anc: set[str] = set(spec.depends_on)
            for dep in spec.depends_on:
                anc |= get_ancestors(dep)
            ancestors_cache[tid] = anc
            return anc

        for tid, task in self._tasks_by_id.items():
            if not task.input_contract:
                continue
            available_outputs = set(seed_keys)
            for anc_id in get_ancestors(tid):
                available_outputs.update(self._tasks_by_id[anc_id].output_contract)
            missing = [k for k in task.input_contract if k not in available_outputs]
            if missing:
                raise DAGValidationError(
                    f"Task {tid!r} requires unproduced input_contract keys {missing!r} "
                    f"not provided by its dependency ancestors."
                )

    def compile(self) -> WavePlan:
        """
        Compiles the task graph into conflict-free parallel execution waves and computes
        critical path metrics.
        """
        self.validate()

        if not self._tasks_by_id:
            return WavePlan(
                waves=[],
                topological_levels=[],
                conflicts_resolved=[],
                total_tasks=0,
                total_waves=0,
                max_parallelism=0,
                sequential_ms=0,
                critical_path_ms=0,
                critical_path_tasks=[],
                parallel_speedup_factor=1.0,
                estimated_total_tokens=0,
            )

        # Kahn's algorithm + topological level calculation
        in_degree: dict[str, int] = {tid: len(t.depends_on) for tid, t in self._tasks_by_id.items()}
        dependents: dict[str, list[str]] = defaultdict(list)
        for tid, task in self._tasks_by_id.items():
            for dep in task.depends_on:
                dependents[dep].append(tid)
        for dep in dependents:
            dependents[dep].sort()

        topo_level: dict[str, int] = {}
        queue: deque[str] = deque(sorted(tid for tid, deg in in_degree.items() if deg == 0))
        for tid in queue:
            topo_level[tid] = 0

        topo_order: list[str] = []
        while queue:
            u = queue.popleft()
            topo_order.append(u)
            for v in dependents.get(u, []):
                in_degree[v] -= 1
                topo_level[v] = max(topo_level.get(v, 0), topo_level[u] + 1)
                if in_degree[v] == 0:
                    queue.append(v)

        max_level = max(topo_level.values(), default=0)
        level_groups: list[list[str]] = [[] for _ in range(max_level + 1)]
        for tid in sorted(self._tasks_by_id):
            level_groups[topo_level[tid]].append(tid)

        # Compute downstream weight (longest dependency chain from node) to prioritize critical tasks first
        downstream_ms: dict[str, int] = {}
        best_next: dict[str, str | None] = {}
        for tid in reversed(topo_order):
            spec = self._tasks_by_id[tid]
            next_candidates = dependents.get(tid, [])
            if not next_candidates:
                downstream_ms[tid] = spec.estimated_ms
                best_next[tid] = None
            else:
                best_child = max(next_candidates, key=lambda c: (downstream_ms[c], c))
                downstream_ms[tid] = spec.estimated_ms + downstream_ms[best_child]
                best_next[tid] = best_child

        # Split each topological level into conflict-free sequential sub-waves
        final_waves: list[list[str]] = []
        conflicts_resolved: list[FileConflictEdge] = []

        for lvl_idx, lvl_tasks in enumerate(level_groups):
            if len(lvl_tasks) <= 1:
                if lvl_tasks:
                    final_waves.append(list(lvl_tasks))
                continue

            # Detect all pairwise file-scope conflicts within this topological level
            conflict_adj: dict[str, set[str]] = defaultdict(set)
            for i in range(len(lvl_tasks)):
                ta = self._tasks_by_id[lvl_tasks[i]]
                for j in range(i + 1, len(lvl_tasks)):
                    tb = self._tasks_by_id[lvl_tasks[j]]
                    edge = find_task_pair_conflict(ta, tb, topological_level=lvl_idx)
                    if edge is not None:
                        conflicts_resolved.append(edge)
                        conflict_adj[ta.task_id].add(tb.task_id)
                        conflict_adj[tb.task_id].add(ta.task_id)

            # Deterministic greedy graph coloring ordered by (descending downstream_ms, ascending task_id)
            ordered_tasks = sorted(
                lvl_tasks,
                key=lambda tid: (-downstream_ms[tid], tid),
            )
            sub_waves: list[list[str]] = []
            for tid in ordered_tasks:
                placed = False
                for sub_wave in sub_waves:
                    if all(other not in conflict_adj[tid] for other in sub_wave):
                        sub_wave.append(tid)
                        placed = True
                        break
                if not placed:
                    sub_waves.append([tid])

            for sw in sub_waves:
                sw.sort()
                final_waves.append(sw)

        # Compute timing metrics
        sequential_ms = sum(t.estimated_ms for t in self._tasks_by_id.values())
        estimated_total_tokens = sum(t.estimated_tokens for t in self._tasks_by_id.values())

        # Wave-synchronous critical path duration: each wave takes max(estimated_ms of tasks in wave)
        wave_critical_ms = sum(
            max(self._tasks_by_id[tid].estimated_ms for tid in wave) for wave in final_waves
        )

        # Reconstruct primary dependency critical path
        start_best = max(topo_order, key=lambda tid: (downstream_ms[tid], tid))
        critical_path_tasks: list[str] = []
        curr: str | None = start_best
        while curr is not None:
            critical_path_tasks.append(curr)
            curr = best_next.get(curr)

        dep_critical_ms = downstream_ms[start_best]
        critical_path_ms = max(dep_critical_ms, wave_critical_ms)
        parallel_speedup = (
            float(sequential_ms) / float(critical_path_ms) if critical_path_ms > 0 else 1.0
        )
        max_parallelism = max((len(w) for w in final_waves), default=0)

        return WavePlan(
            waves=final_waves,
            topological_levels=level_groups,
            conflicts_resolved=conflicts_resolved,
            total_tasks=len(self._tasks_by_id),
            total_waves=len(final_waves),
            max_parallelism=max_parallelism,
            sequential_ms=sequential_ms,
            critical_path_ms=critical_path_ms,
            critical_path_tasks=critical_path_tasks,
            parallel_speedup_factor=parallel_speedup,
            estimated_total_tokens=estimated_total_tokens,
        )

    @staticmethod
    def check_parallel_conflicts(tasks: Iterable[TaskSpec]) -> list[FileConflictEdge]:
        """
        Checks a proposed batch of parallel tasks for any read/write or write/write overlaps.
        """
        task_list = list(tasks)
        conflicts: list[FileConflictEdge] = []
        for i in range(len(task_list)):
            for j in range(i + 1, len(task_list)):
                edge = find_task_pair_conflict(task_list[i], task_list[j], topological_level=0)
                if edge is not None:
                    conflicts.append(edge)
        return conflicts

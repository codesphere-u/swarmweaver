"""
Command-Line Interface for SwarmWeaver.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from swarmweaver import __version__
from swarmweaver.benchmark import run_benchmark_suite
from swarmweaver.checkpoint import CheckpointJournal
from swarmweaver.dag import DAGCompiler
from swarmweaver.demo import build_demo_10_agent_swarm, run_demo_workflow
from swarmweaver.http_server import DEFAULT_MAX_BODY_BYTES, serve_http
from swarmweaver.mcp_server import SwarmWeaverMCPServer
from swarmweaver.models import TaskSpec


def cmd_plan(args: argparse.Namespace) -> int:
    if args.file:
        raw = json.loads(Path(args.file).read_text(encoding="utf-8"))
        items = raw["tasks"] if isinstance(raw, dict) and "tasks" in raw else raw
        tasks = [TaskSpec.from_dict(t) for t in items]
    else:
        tasks = build_demo_10_agent_swarm()

    compiler = DAGCompiler(tasks, validate_contracts=True)
    plan = compiler.compile()

    if args.json:
        print(json.dumps(plan.to_dict(), indent=2))
        return 0

    print(f"SwarmWeaver v{__version__} — Conflict-Aware Wave Plan")
    print("=" * 68)
    print(f"Total Tasks          : {plan.total_tasks}")
    print(f"Topological Levels   : {len(plan.topological_levels)}")
    print(f"Conflict-Free Waves  : {plan.total_waves}")
    print(f"Conflicts Auto-Split : {len(plan.conflicts_resolved)}")
    print(f"Sequential Duration  : {plan.sequential_ms} ms")
    print(f"Critical Path        : {plan.critical_path_ms} ms ({' -> '.join(plan.critical_path_tasks)})")
    print(f"Parallel Speedup     : {plan.parallel_speedup_factor:.2f}x")
    print(f"Token Budget Forecast: {plan.estimated_total_tokens:,} tokens")
    print("-" * 68)
    for idx, wave in enumerate(plan.waves, start=1):
        print(f"  Wave {idx:02d}: {', '.join(wave)}")
    if plan.conflicts_resolved:
        print("-" * 68)
        print("Prevented File-Scope Collisions (Auto-Split into Sequential Sub-Waves):")
        for c in plan.conflicts_resolved:
            print(
                f"  - [{c.conflict_type}] {c.task_a} ({c.pattern_a}) <-> "
                f"{c.task_b} ({c.pattern_b}) at level {c.topological_level}"
            )
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    summary = run_demo_workflow()

    if args.json:
        print(json.dumps(summary, indent=2))
        return 0

    crash = summary["run_1_crash_simulation"]
    resume = summary["run_2_checkpoint_resume"]
    integrity = summary["journal_integrity"]
    print(f"SwarmWeaver v{__version__} — 10-Agent Full-Stack Refactoring Swarm Demo")
    print("=" * 72)
    print(
        f"[1] Compiled {summary['total_tasks']} tasks into {summary['total_waves']} "
        f"conflict-free waves ({len(summary['conflicts_auto_split'])} file collisions split)."
    )
    print(
        f"[2] Run #1 (Injected Crash at '08_integration_tests'): "
        f"status={crash['status']}, completed={len(crash['completed_before_crash'])}/10 nodes, "
        f"tokens_spent={crash['tokens_spent']:,}"
    )
    print(
        f"[3] Run #2 (Crash-Safe Checkpoint Resume): "
        f"status={resume['status']}, replayed={len(resume['replayed_tasks'])} nodes "
        f"in {resume['replay_elapsed_ms']:.3f} ms, "
        f"tokens_saved={resume['tokens_saved_by_replay']:,} tokens!"
    )
    print(
        f"[4] SHA-256 Checkpoint Hash Chain Verified: valid={integrity['valid']}, "
        f"records={integrity['records_checked']}, head={integrity['head_hash'][:16]}..."
    )
    return 0


def cmd_benchmark(args: argparse.Namespace) -> int:
    results = run_benchmark_suite()
    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    d1 = results["dag_compilation_200_tasks"]
    d2 = results["parallel_conflict_guard_50_tasks"]
    d3 = results["crash_recovery_20_workflows"]
    d4 = results["cycle_and_contract_detection_15_cases"]

    print(f"SwarmWeaver v{__version__} — Reproducible Benchmark Suite")
    print("=" * 72)
    print(
        f"1. 200-Task DAG Compilation & Wave Scheduling : "
        f"P50={d1['p50_ms']} ms | P95={d1['p95_ms']} ms | "
        f"{d1['compiled_waves']} waves ({d1['conflicts_auto_split']} conflicts split)"
    )
    print(
        f"2. 50 Parallel Conflicting File-Edit Tasks    : "
        f"{d2['race_conditions_observed']} race conditions | "
        f"{d2['collision_prevention_rate_pct']}% collision prevention"
    )
    print(
        f"3. 20 Crash-Recovery Workflows (70% Crash)    : "
        f"{d3['token_savings_pct']}% tokens saved ({d3['tokens_saved_on_resume']:,} tokens) | "
        f"{d3['avg_replay_ms_per_node']} ms/node replay"
    )
    print(
        f"4. Cycle & Contract Violation Detection (15)  : "
        f"{d4['violations_detected']}/{d4['malformed_cases_tested']} detected "
        f"({d4['recall_pct']}% recall)"
    )
    print("=" * 72)
    return 0


def cmd_verify_journal(args: argparse.Namespace) -> int:
    path = Path(args.journal_path)
    if not path.exists():
        print(
            json.dumps({"valid": False, "error": f"Journal file not found: {path}"}),
            file=sys.stderr,
        )
        return 1

    try:
        journal = CheckpointJournal(journal_path=path)
        res = journal.verify_integrity()
    except Exception as exc:  # noqa: BLE001 - report arbitrary journal parse errors cleanly
        print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
        return 1

    payload = {
        "journal_path": str(path),
        "workflow_id": journal.workflow_id,
        "completed_task_ids": journal.completed_task_ids(),
        **res.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        status_str = "VALID" if res.valid else f"TAMPERED ({res.error})"
        print(f"Journal Integrity : {status_str}")
        print(f"Records Checked   : {res.records_checked}")
        print(f"Head SHA-256      : {res.head_hash}")
    return 0 if res.valid else 1


def cmd_mcp(_args: argparse.Namespace) -> int:
    server = SwarmWeaverMCPServer()
    server.serve_stdio()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="swarmweaver",
        description=(
            "SwarmWeaver — Deterministic Multi-Agent DAG Orchestrator, "
            "File-Scope Lease Conflict Guard & Crash-Safe Checkpoint Resume Engine."
        ),
    )
    parser.add_argument("--version", action="version", version=f"swarmweaver {__version__}")
    sub = parser.add_subparsers(dest="command")

    p_plan = sub.add_parser("plan", help="Compile a task graph into conflict-free parallel waves.")
    p_plan.add_argument("-f", "--file", help="Path to JSON task specification file.")
    p_plan.add_argument("--json", action="store_true", help="Emit JSON output.")

    p_demo = sub.add_parser(
        "demo",
        help="Run the 10-agent full-stack refactoring swarm demo with crash injection & resume.",
    )
    p_demo.add_argument("--json", action="store_true", help="Emit JSON output.")

    p_bench = sub.add_parser("benchmark", help="Run the reproducible SwarmWeaver benchmark suite.")
    p_bench.add_argument("--json", action="store_true", help="Emit JSON output.")

    p_verify = sub.add_parser(
        "verify-journal",
        help="Verify the SHA-256 hash chain of a checkpoint journal file.",
    )
    p_verify.add_argument("journal_path", help="Path to the .jsonl checkpoint journal.")
    p_verify.add_argument("--json", action="store_true", help="Emit JSON output.")

    sub.add_parser("mcp", help="Start the JSON-RPC 2.0 Stdio MCP server.")

    p_http = sub.add_parser(
        "serve-http",
        help="Start the HTTP/SSE MCP portal and REST scheduler server.",
    )
    p_http.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1).")
    p_http.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000).")
    p_http.add_argument(
        "--max-body-bytes",
        type=int,
        default=DEFAULT_MAX_BODY_BYTES,
        help="Maximum allowed request body size in bytes.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "plan":
        return cmd_plan(args)
    if args.command == "demo":
        return cmd_demo(args)
    if args.command == "benchmark":
        return cmd_benchmark(args)
    if args.command == "verify-journal":
        return cmd_verify_journal(args)
    if args.command == "mcp":
        return cmd_mcp(args)
    if args.command == "serve-http":
        serve_http(host=args.host, port=args.port, max_body_bytes=args.max_body_bytes)
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

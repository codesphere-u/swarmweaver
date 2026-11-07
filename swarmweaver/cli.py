"""
Command-Line Interface for SwarmWeaver.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import tempfile
from typing import Any

from swarmweaver import __version__
from swarmweaver.benchmark import run_benchmark_suite
from swarmweaver.checkpoint import CheckpointJournal
from swarmweaver.dag import DAGCompiler
from swarmweaver.engine import SwarmOrchestrator
from swarmweaver.mcp_server import SwarmWeaverMCPServer
from swarmweaver.models import TaskSpec


def build_demo_10_agent_swarm() -> list[TaskSpec]:
    """
    Constructs a realistic 10-agent full-stack security & API refactoring swarm
    with deliberate file-glob conflicts that DAGCompiler automatically splits into
    safe sub-waves.
    """
    return [
        TaskSpec(
            task_id="01_ast_audit",
            title="Scan repository AST & security surface",
            agent_role="security-auditor",
            depends_on=[],
            read_globs=["src/**/*.py"],
            write_globs=[],
            estimated_ms=120,
            estimated_tokens=800,
            input_contract=[],
            output_contract=["ast_inventory:dict"],
        ),
        TaskSpec(
            task_id="02_schema_migration",
            title="Generate OAuth2 & session schema migration",
            agent_role="db-architect",
            depends_on=["01_ast_audit"],
            read_globs=["src/db/models.py"],
            write_globs=["src/db/migrations/004_oauth.sql", "src/db/models.py"],
            estimated_ms=180,
            estimated_tokens=1100,
            input_contract=["ast_inventory:dict"],
            output_contract=["db_schema_plan:dict"],
        ),
        TaskSpec(
            task_id="03_jwt_verifier",
            title="Refactor JWT token verifier & key rotation",
            agent_role="auth-engineer",
            depends_on=["01_ast_audit"],
            read_globs=["src/auth/config.py"],
            write_globs=["src/auth/jwt.py", "src/auth/keys.py"],
            estimated_ms=200,
            estimated_tokens=1250,
            input_contract=["ast_inventory:dict"],
            output_contract=["jwt_module_spec:dict"],
        ),
        TaskSpec(
            task_id="04_rbac_policy",
            title="Harden RBAC middleware & jwt.py claims parser",
            agent_role="policy-engineer",
            depends_on=["01_ast_audit"],
            # Deliberately overlaps on src/auth/jwt.py with 03_jwt_verifier in topological level 1!
            read_globs=["src/auth/jwt.py"],
            write_globs=["src/auth/jwt.py", "src/auth/rbac.py"],
            estimated_ms=160,
            estimated_tokens=950,
            input_contract=["ast_inventory:dict"],
            output_contract=["rbac_rules_spec:dict"],
        ),
        TaskSpec(
            task_id="05_api_gateway",
            title="Wire authenticated routes into FastAPI gateway",
            agent_role="backend-engineer",
            depends_on=["02_schema_migration", "03_jwt_verifier", "04_rbac_policy"],
            read_globs=["src/auth/*.py", "src/db/models.py"],
            write_globs=["src/api/routes.py", "src/api/deps.py"],
            estimated_ms=220,
            estimated_tokens=1400,
            input_contract=["db_schema_plan:dict", "jwt_module_spec:dict", "rbac_rules_spec:dict"],
            output_contract=["gateway_routes:dict"],
        ),
        TaskSpec(
            task_id="06_rate_limiter",
            title="Attach token-bucket rate limiter to API deps",
            agent_role="infra-engineer",
            depends_on=["03_jwt_verifier", "04_rbac_policy"],
            # Overlaps with 05_api_gateway on src/api/deps.py in topological level 2!
            read_globs=["src/api/deps.py"],
            write_globs=["src/api/deps.py", "src/api/ratelimit.py"],
            estimated_ms=150,
            estimated_tokens=900,
            input_contract=["jwt_module_spec:dict", "rbac_rules_spec:dict"],
            output_contract=["ratelimit_config:dict"],
        ),
        TaskSpec(
            task_id="07_openapi_client",
            title="Regenerate TypeScript SDK from updated routes",
            agent_role="frontend-engineer",
            depends_on=["05_api_gateway"],
            read_globs=["src/api/routes.py"],
            write_globs=["web/src/sdk/client.ts", "web/src/sdk/types.ts"],
            estimated_ms=170,
            estimated_tokens=1050,
            input_contract=["gateway_routes:dict"],
            output_contract=["ts_sdk_bundle:dict"],
        ),
        TaskSpec(
            task_id="08_integration_tests",
            title="Synthesize auth & rate-limit integration tests",
            agent_role="qa-engineer",
            depends_on=["05_api_gateway", "06_rate_limiter"],
            read_globs=["src/api/*.py", "src/auth/*.py"],
            write_globs=["tests/integration/test_auth_flow.py"],
            estimated_ms=210,
            estimated_tokens=1300,
            input_contract=["gateway_routes:dict", "ratelimit_config:dict"],
            output_contract=["integration_test_report:dict"],
            retry_budget=0,
        ),
        TaskSpec(
            task_id="09_fuzz_harness",
            title="Run property-based JWT & RBAC boundary fuzzing",
            agent_role="security-fuzzer",
            depends_on=["05_api_gateway", "06_rate_limiter"],
            read_globs=["src/auth/*.py"],
            write_globs=["tests/security/test_jwt_fuzz.py"],
            estimated_ms=190,
            estimated_tokens=1150,
            input_contract=["gateway_routes:dict", "ratelimit_config:dict"],
            output_contract=["fuzz_audit_report:dict"],
        ),
        TaskSpec(
            task_id="10_release_attestation",
            title="Compile final cryptographic release attestation",
            agent_role="release-governor",
            depends_on=["07_openapi_client", "08_integration_tests", "09_fuzz_harness"],
            read_globs=["src/**/*.py", "tests/**/*.py", "web/src/sdk/*.ts"],
            write_globs=["docs/RELEASE_ATTESTATION.json"],
            estimated_ms=110,
            estimated_tokens=700,
            input_contract=[
                "ts_sdk_bundle:dict",
                "integration_test_report:dict",
                "fuzz_audit_report:dict",
            ],
            output_contract=["release_attestation:dict"],
        ),
    ]


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
    tasks = build_demo_10_agent_swarm()
    with tempfile.TemporaryDirectory(prefix="swarmweaver_demo_") as tmp_dir:
        journal_path = Path(tmp_dir) / "checkpoint.jsonl"
        journal = CheckpointJournal(journal_path=journal_path, workflow_id="demo-10-agent-refactor")

        # Phase 1: Execute with injected crash at task '08_integration_tests'
        orch1 = SwarmOrchestrator(
            tasks,
            workflow_id="demo-10-agent-refactor",
            journal=journal,
            validate_contracts_at_compile=True,
        )
        crash_report = orch1.execute(fail_at_task="08_integration_tests", resume=False)

        # Phase 2: Resume from hash-chained CheckpointJournal
        orch2 = SwarmOrchestrator(
            tasks,
            workflow_id="demo-10-agent-refactor",
            journal=journal,
            validate_contracts_at_compile=True,
        )
        resume_report = orch2.execute(resume=True)

        integrity = journal.verify_integrity()

        summary: dict[str, Any] = {
            "workflow_id": "demo-10-agent-refactor",
            "total_tasks": len(tasks),
            "compiled_waves": resume_report.wave_plan.waves,
            "conflicts_auto_split": [c.to_dict() for c in resume_report.wave_plan.conflicts_resolved],
            "run_1_crash_simulation": {
                "status": crash_report.status,
                "failed_task": crash_report.failed_task,
                "completed_before_crash": crash_report.completed_tasks,
                "tokens_spent": crash_report.tokens_spent,
            },
            "run_2_checkpoint_resume": {
                "status": resume_report.status,
                "replayed_tasks": resume_report.replayed_tasks,
                "newly_executed_tasks": [
                    t for t in resume_report.completed_tasks if t not in resume_report.replayed_tasks
                ],
                "tokens_spent_on_resume": resume_report.tokens_spent,
                "tokens_saved_by_replay": resume_report.tokens_saved_by_replay,
                "replay_elapsed_ms": round(resume_report.replay_elapsed_ms, 4),
            },
            "journal_integrity": integrity.to_dict(),
        }

        if args.json:
            print(json.dumps(summary, indent=2))
            return 0

        print(f"SwarmWeaver v{__version__} — 10-Agent Full-Stack Refactoring Swarm Demo")
        print("=" * 72)
        print(
            f"[1] Compiled {len(tasks)} tasks into {resume_report.wave_plan.total_waves} "
            f"conflict-free waves ({len(resume_report.wave_plan.conflicts_resolved)} file collisions split)."
        )
        print(
            f"[2] Run #1 (Injected Crash at '08_integration_tests'): "
            f"status={crash_report.status}, completed={len(crash_report.completed_tasks)}/10 nodes, "
            f"tokens_spent={crash_report.tokens_spent:,}"
        )
        print(
            f"[3] Run #2 (Crash-Safe Checkpoint Resume): "
            f"status={resume_report.status}, replayed={len(resume_report.replayed_tasks)} nodes "
            f"in {resume_report.replay_elapsed_ms:.3f} ms, "
            f"tokens_saved={resume_report.tokens_saved_by_replay:,} tokens!"
        )
        print(
            f"[4] SHA-256 Checkpoint Hash Chain Verified: valid={integrity.valid}, "
            f"records={integrity.records_checked}, head={integrity.head_hash[:16]}..."
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
    except Exception as exc:
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

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

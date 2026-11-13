"""Deterministic 10-agent crash-resume refactoring swarm demo helpers for SwarmWeaver."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from swarmweaver.checkpoint import CheckpointJournal
from swarmweaver.engine import SwarmOrchestrator
from swarmweaver.models import TaskSpec


def build_demo_10_agent_swarm() -> list[TaskSpec]:
    """
    Construct a realistic 10-agent full-stack security & API refactoring swarm
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


def run_demo_workflow() -> dict[str, Any]:
    """Execute the 10-agent crash-injection and SHA-256 checkpoint resume workflow."""
    tasks = build_demo_10_agent_swarm()
    with tempfile.TemporaryDirectory(prefix="swarmweaver_demo_") as tmp_dir:
        journal_path = Path(tmp_dir) / "checkpoint.jsonl"
        journal = CheckpointJournal(journal_path=journal_path, workflow_id="demo-10-agent-refactor")

        orch1 = SwarmOrchestrator(
            tasks,
            workflow_id="demo-10-agent-refactor",
            journal=journal,
            validate_contracts_at_compile=True,
        )
        crash_report = orch1.execute(fail_at_task="08_integration_tests", resume=False)

        orch2 = SwarmOrchestrator(
            tasks,
            workflow_id="demo-10-agent-refactor",
            journal=journal,
            validate_contracts_at_compile=True,
        )
        resume_report = orch2.execute(resume=True)
        integrity = journal.verify_integrity()

        return {
            "workflow_id": "demo-10-agent-refactor",
            "total_tasks": len(tasks),
            "compiled_waves": resume_report.wave_plan.waves,
            "total_waves": resume_report.wave_plan.total_waves,
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

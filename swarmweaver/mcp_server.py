"""JSON-RPC 2.0 Model Context Protocol (MCP) Server for SwarmWeaver."""

from __future__ import annotations

import json
import sys
from typing import Any, TextIO

from swarmweaver import __version__
from swarmweaver.benchmark import run_benchmark_suite
from swarmweaver.checkpoint import CheckpointJournal
from swarmweaver.dag import DAGCompiler
from swarmweaver.demo import build_demo_10_agent_swarm
from swarmweaver.models import TaskSpec

MCP_PROTOCOL_VERSION = "2024-11-05"

MCP_TOOLS: list[dict[str, Any]] = [
    {
        "name": "swarmweaver_plan_waves",
        "description": (
            "Compiles a list of multi-agent tasks into conflict-free parallel execution waves, "
            "automatically splitting tasks with overlapping read/write or write/write file globs "
            "and computing critical-path duration and token forecasts."
        ),
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        "inputSchema": {
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "array",
                    "description": "List of TaskSpec objects to compile into execution waves.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "task_id": {"type": "string"},
                            "title": {"type": "string"},
                            "agent_role": {"type": "string"},
                            "depends_on": {"type": "array", "items": {"type": "string"}},
                            "read_globs": {"type": "array", "items": {"type": "string"}},
                            "write_globs": {"type": "array", "items": {"type": "string"}},
                            "estimated_ms": {"type": "integer"},
                            "estimated_tokens": {"type": "integer"},
                            "input_contract": {"type": "array", "items": {"type": "string"}},
                            "output_contract": {"type": "array", "items": {"type": "string"}},
                            "retry_budget": {"type": "integer"},
                        },
                        "required": ["task_id", "title"],
                    },
                },
                "validate_contracts": {
                    "type": "boolean",
                    "default": False,
                },
            },
            "required": ["tasks"],
        },
    },
    {
        "name": "swarmweaver_check_file_conflicts",
        "description": (
            "Tests whether a proposed set of parallel subagent tasks has write/write or "
            "read/write file-glob or symbol-scope collisions."
        ),
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        "inputSchema": {
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "array",
                    "description": "Proposed parallel tasks with read_globs and write_globs.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "task_id": {"type": "string"},
                            "title": {"type": "string"},
                            "read_globs": {"type": "array", "items": {"type": "string"}},
                            "write_globs": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["task_id"],
                    },
                },
            },
            "required": ["tasks"],
        },
    },
    {
        "name": "swarmweaver_inspect_checkpoint",
        "description": (
            "Verifies a checkpoint journal's SHA-256 hash chain (`prev_hash -> entry_hash`) "
            "and reports resumable completed tasks vs. pending tasks."
        ),
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        "inputSchema": {
            "type": "object",
            "properties": {
                "journal_path": {
                    "type": "string",
                    "description": "Path to the JSONL checkpoint journal file.",
                },
                "expected_task_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of all task IDs in the workflow to compute pending tasks.",
                },
            },
            "required": ["journal_path"],
        },
    },
    {
        "name": "swarmweaver_run_benchmark",
        "description": (
            "Runs the deterministic SwarmWeaver benchmark suite (200-task DAG compilation, "
            "50-task conflict guard, 20-workflow crash-recovery token savings, and 15-case "
            "cycle/contract violation detection)."
        ),
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
]

MCP_RESOURCES: list[dict[str, Any]] = [
    {
        "uri": "swarmweaver://workflow/demo-10-agent",
        "name": "10-Agent Full-Stack Refactoring Swarm Specification",
        "description": (
            "Canonical 10-task DAG with deliberate read/write glob overlaps resolved into "
            "conflict-free parallel execution waves."
        ),
        "mimeType": "application/json",
    },
    {
        "uri": "swarmweaver://benchmark/summary",
        "name": "SwarmWeaver Deterministic Benchmark Report",
        "description": (
            "200-task DAG scheduling P50/P95 latency, 50-task collision prevention rate, "
            "20-workflow crash-recovery token savings, and cycle/contract violation recall."
        ),
        "mimeType": "application/json",
    },
    {
        "uri": "swarmweaver://dag/lease-conflict-rules",
        "name": "File-Scope Lease & Wave Splitting Rules",
        "description": (
            "Formal specification of read/write, write/write, glob wildcard, and "
            "directory-prefix conflict edges enforced prior to parallel wave scheduling."
        ),
        "mimeType": "application/json",
    },
]

MCP_RESOURCE_TEMPLATES: list[dict[str, Any]] = [
    {
        "uriTemplate": "swarmweaver://workflow/{task_id}",
        "name": "Single Demo Swarm Task Specification",
        "description": "Inspect one task specification from the canonical 10-agent refactoring workflow.",
        "mimeType": "application/json",
    },
]

MCP_PROMPTS: list[dict[str, Any]] = [
    {
        "name": "plan-safe-swarm",
        "description": (
            "Decompose a multi-agent engineering objective into conflict-free parallel DAG waves "
            "with explicit file read/write globs and typed blackboard contracts."
        ),
        "arguments": [
            {
                "name": "objective",
                "description": "High-level engineering goal to split across parallel subagents.",
                "required": True,
            },
            {
                "name": "max_agents",
                "description": "Maximum concurrent workers per execution wave.",
                "required": False,
            },
        ],
    },
    {
        "name": "resolve-file-lease-conflict",
        "description": (
            "Diagnose write/write or read/write glob collisions across candidate tasks and "
            "propose wave-splitting or disjoint file ownership."
        ),
        "arguments": [
            {
                "name": "tasks_json",
                "description": "JSON list of TaskSpec payloads exhibiting file-scope collisions.",
                "required": True,
            },
        ],
    },
    {
        "name": "recover-crashed-workflow",
        "description": (
            "Verify a SwarmWeaver SHA-256 checkpoint journal and plan zero-waste replay for "
            "only the uncompleted downstream DAG nodes."
        ),
        "arguments": [
            {
                "name": "journal_path",
                "description": "Filesystem path to checkpoint.jsonl.",
                "required": True,
            },
        ],
    },
]


def _resource_payload(uri: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "contents": [
            {
                "uri": uri,
                "mimeType": "application/json",
                "text": json.dumps(data, indent=2, sort_keys=True),
            }
        ]
    }


class SwarmWeaverMCPServer:
    """
    Stateless/stateful JSON-RPC 2.0 MCP Server handler for SwarmWeaver.
    """

    def handle_tool_call(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        args = arguments or {}

        if name == "swarmweaver_plan_waves":
            raw_tasks = args.get("tasks", [])
            specs = [TaskSpec.from_dict(item) for item in raw_tasks]
            validate_contracts = bool(args.get("validate_contracts", False))
            compiler = DAGCompiler(specs, validate_contracts=validate_contracts)
            plan = compiler.compile()
            return plan.to_dict()

        if name == "swarmweaver_check_file_conflicts":
            raw_tasks = args.get("tasks", [])
            specs = [
                TaskSpec(
                    task_id=str(item["task_id"]),
                    title=str(item.get("title", item["task_id"])),
                    read_globs=list(item.get("read_globs", [])),
                    write_globs=list(item.get("write_globs", [])),
                )
                for item in raw_tasks
            ]
            conflicts = DAGCompiler.check_parallel_conflicts(specs)
            return {
                "safe_for_parallel_execution": len(conflicts) == 0,
                "conflict_count": len(conflicts),
                "conflicts": [c.to_dict() for c in conflicts],
            }

        if name == "swarmweaver_inspect_checkpoint":
            journal_path = str(args["journal_path"])
            expected_ids = [str(x) for x in args.get("expected_task_ids", [])]
            journal = CheckpointJournal(journal_path=journal_path)
            integrity = journal.verify_integrity()
            completed = journal.completed_task_ids()
            completed_set = set(completed)
            pending = [tid for tid in expected_ids if tid not in completed_set]
            total_tokens_saved = sum(r.tokens_used for r in journal.records)

            return {
                "journal_path": journal_path,
                "workflow_id": journal.workflow_id,
                "integrity": integrity.to_dict(),
                "completed_task_ids": completed,
                "pending_task_ids": pending,
                "total_checkpointed_tasks": len(completed),
                "tokens_recoverable_on_resume": total_tokens_saved,
            }

        if name == "swarmweaver_run_benchmark":
            return run_benchmark_suite()

        raise ValueError(f"Unknown MCP tool: {name!r}")

    def _read_resource(self, uri: str) -> dict[str, Any]:
        demo_tasks = build_demo_10_agent_swarm()
        if uri == "swarmweaver://workflow/demo-10-agent":
            return _resource_payload(
                uri,
                {
                    "workflow_id": "demo-10-agent-refactor",
                    "tasks": [t.to_dict() for t in demo_tasks],
                },
            )
        if uri == "swarmweaver://benchmark/summary":
            return _resource_payload(uri, run_benchmark_suite())
        if uri == "swarmweaver://dag/lease-conflict-rules":
            return _resource_payload(
                uri,
                {
                    "lease_modes": ["shared_read", "exclusive_write"],
                    "conflict_pairs": [
                        "write_vs_write",
                        "write_vs_read",
                        "directory_prefix_containment",
                        "wildcard_glob_intersection",
                    ],
                    "resolution": "Deterministic wave splitting into sequential sub-waves",
                },
            )
        if uri.startswith("swarmweaver://workflow/"):
            target_id = uri.removeprefix("swarmweaver://workflow/")
            for task in demo_tasks:
                if task.task_id == target_id:
                    return _resource_payload(uri, task.to_dict())
        raise KeyError(f"Unknown resource URI: {uri}")

    def _get_prompt(self, name: str, arguments: dict[str, Any] | None) -> dict[str, Any]:
        args = arguments or {}
        if name == "plan-safe-swarm":
            objective = str(args.get("objective", "")).strip()
            if not objective:
                raise ValueError("Prompt 'plan-safe-swarm' requires 'objective'.")
            max_agents = str(args.get("max_agents", "4"))
            return {
                "description": "Plan a conflict-free multi-agent execution DAG",
                "messages": [
                    {
                        "role": "user",
                        "content": {
                            "type": "text",
                            "text": (
                                f"Decompose the objective into conflict-free TaskSpec waves "
                                f"(max concurrency hint: {max_agents}):\n\n{objective}\n\n"
                                "Call `swarmweaver_plan_waves` with explicit `read_globs`, "
                                "`write_globs`, and typed `input_contract`/`output_contract`."
                            ),
                        },
                    }
                ],
            }
        if name == "resolve-file-lease-conflict":
            tasks_json = str(args.get("tasks_json", "")).strip()
            if not tasks_json:
                raise ValueError("Prompt 'resolve-file-lease-conflict' requires 'tasks_json'.")
            return {
                "description": "Resolve file-scope lease collisions between parallel tasks",
                "messages": [
                    {
                        "role": "user",
                        "content": {
                            "type": "text",
                            "text": (
                                "Analyze these proposed parallel tasks via "
                                "`swarmweaver_check_file_conflicts` and partition overlapping globs "
                                f"into safe sequential waves:\n\n{tasks_json}"
                            ),
                        },
                    }
                ],
            }
        if name == "recover-crashed-workflow":
            journal_path = str(args.get("journal_path", "")).strip()
            if not journal_path:
                raise ValueError("Prompt 'recover-crashed-workflow' requires 'journal_path'.")
            return {
                "description": "Verify hash-chained checkpoint journal and resume workflow",
                "messages": [
                    {
                        "role": "user",
                        "content": {
                            "type": "text",
                            "text": (
                                f"Inspect `{journal_path}` using `swarmweaver_inspect_checkpoint`, "
                                "confirm `integrity.valid == true`, and replay only pending tasks."
                            ),
                        },
                    }
                ],
            }
        raise KeyError(f"Unknown prompt: {name}")

    def handle_request(self, request: dict[str, Any]) -> dict[str, Any] | None:
        """
        Process a single JSON-RPC 2.0 request dictionary and return a JSON-RPC 2.0
        response dictionary (or `None` for notifications).
        """
        if not isinstance(request, dict) or (
            "jsonrpc" in request and request.get("jsonrpc") != "2.0"
        ):
            return {
                "jsonrpc": "2.0",
                "id": request.get("id") if isinstance(request, dict) else None,
                "error": {"code": -32600, "message": "Invalid Request."},
            }

        method = request.get("method")
        req_id = request.get("id")
        raw_params = request.get("params")
        if raw_params is not None and not isinstance(raw_params, dict):
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32602, "message": "Field 'params' must be a JSON object."},
            }
        params: dict[str, Any] = raw_params or {}

        # Notifications have no 'id' (or null id with notifications/* method)
        if req_id is None and (
            "id" not in request
            or (isinstance(method, str) and (method.startswith("notifications/") or method == "initialized"))
        ):
            return None

        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {
                        "tools": {"listChanged": False},
                        "resources": {"subscribe": False, "listChanged": False},
                        "prompts": {"listChanged": False},
                    },
                    "serverInfo": {
                        "name": "swarmweaver-mcp",
                        "version": __version__,
                    },
                },
            }

        if method in ("ping", "resources/subscribe", "resources/unsubscribe", "logging/setLevel"):
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {},
            }

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": MCP_TOOLS},
            }

        if method == "resources/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"resources": MCP_RESOURCES},
            }

        if method == "resources/templates/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"resourceTemplates": MCP_RESOURCE_TEMPLATES},
            }

        if method == "resources/read":
            uri = params.get("uri")
            if not isinstance(uri, str) or not uri:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32602, "message": "Missing 'uri' parameter."},
                }
            try:
                return {"jsonrpc": "2.0", "id": req_id, "result": self._read_resource(uri)}
            except KeyError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32602, "message": str(exc)},
                }

        if method == "prompts/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"prompts": MCP_PROMPTS},
            }

        if method == "prompts/get":
            prompt_name = params.get("name")
            prompt_args = params.get("arguments")
            if not isinstance(prompt_name, str) or not prompt_name:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32602, "message": "Missing prompt 'name'."},
                }
            if prompt_args is not None and not isinstance(prompt_args, dict):
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32602, "message": "Prompt 'arguments' must be an object."},
                }
            try:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": self._get_prompt(prompt_name, prompt_args),
                }
            except (KeyError, ValueError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32602, "message": str(exc)},
                }

        if method == "tools/call":
            tool_name = params.get("name", "")
            tool_args = params.get("arguments") or {}
            try:
                payload = self.handle_tool_call(tool_name, tool_args)
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(payload, indent=2),
                            }
                        ],
                        "structuredContent": payload,
                        "isError": False,
                    },
                }
            except Exception as exc:  # noqa: BLE001 - return MCP tool error envelope cleanly
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(
                                    {
                                        "error": type(exc).__name__,
                                        "message": str(exc),
                                    },
                                    indent=2,
                                ),
                            }
                        ],
                        "isError": True,
                    },
                }

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {method!r}",
            },
        }

    def serve_stdio(
        self,
        stdin: TextIO = sys.stdin,
        stdout: TextIO = sys.stdout,
    ) -> None:
        """Run the JSON-RPC 2.0 MCP server loop over standard input/output."""
        for raw_line in stdin:
            line = raw_line.strip()
            if not line:
                continue
            try:
                request = json.loads(line)
            except json.JSONDecodeError as exc:
                err_resp = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": f"Parse error: {exc}"},
                }
                stdout.write(json.dumps(err_resp) + "\n")
                stdout.flush()
                continue

            response = self.handle_request(request)
            if response is not None:
                stdout.write(json.dumps(response) + "\n")
                stdout.flush()

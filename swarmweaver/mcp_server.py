"""
JSON-RPC 2.0 Stdio Model Context Protocol (MCP) Server for SwarmWeaver.
"""

from __future__ import annotations

import json
import sys
from typing import Any, TextIO

from swarmweaver import __version__
from swarmweaver.benchmark import run_benchmark_suite
from swarmweaver.checkpoint import CheckpointJournal
from swarmweaver.dag import DAGCompiler
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
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
]


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

    def handle_request(self, request: dict[str, Any]) -> dict[str, Any] | None:
        """
        Processes a single JSON-RPC 2.0 request dictionary and returns a JSON-RPC 2.0
        response dictionary (or `None` for notifications).
        """
        method = request.get("method")
        req_id = request.get("id")
        params = request.get("params") or {}

        # Notifications have no 'id'
        if req_id is None and method in ("notifications/initialized", "initialized"):
            return None

        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {
                        "name": "swarmweaver-mcp",
                        "version": __version__,
                    },
                },
            }

        if method == "ping":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {},
            }

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": MCP_TOOLS,
                },
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
                        "isError": False,
                    },
                }
            except Exception as exc:
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
        """Runs the JSON-RPC 2.0 MCP server loop over standard input/output."""
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

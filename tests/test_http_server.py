"""Tests for SwarmWeaver HTTP, Streamable MCP, REST DAG portal, and MCP resources/prompts."""

from __future__ import annotations

import http.client
import io
import json
import threading
from typing import Any

from swarmweaver.demo import build_demo_10_agent_swarm
from swarmweaver.http_server import SwarmWeaverHTTPApp, create_http_server


def _decode(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))


def test_health_readyz_info_metrics_and_head() -> None:
    app = SwarmWeaverHTTPApp()

    health = app.handle_http("GET", "/health", {"X-Request-ID": "req-swarm-01"})
    assert health.status == 200
    assert health.headers["X-Request-ID"] == "req-swarm-01"
    assert health.headers["X-Content-Type-Options"] == "nosniff"
    assert _decode(health.body)["checks"]["dag_compiler"] == "ready"

    head = app.handle_http("HEAD", "/healthz", {})
    assert head.status == 200
    assert head.body == b""

    ready = app.handle_http("GET", "/readyz", {})
    assert ready.status == 200
    assert _decode(ready.body)["ready"] is True

    info = app.handle_http("GET", "/info", {})
    assert info.status == 200
    assert _decode(info.body)["service"] == "swarmweaver"

    root_html = app.handle_http("GET", "/", {"Accept": "text/html"})
    assert root_html.status == 200
    assert "SwarmWeaver" in root_html.body.decode("utf-8")

    root_json = app.handle_http("GET", "/", {"Accept": "application/json"})
    assert root_json.status == 200
    assert "endpoints" in _decode(root_json.body)

    prom = app.handle_http("GET", "/metrics", {})
    assert prom.status == 200
    assert "swarmweaver_http_requests_total" in prom.body.decode("utf-8")

    metrics_json = app.handle_http("GET", "/metrics?format=json", {})
    assert metrics_json.status == 200
    assert _decode(metrics_json.body)["requests_total"] >= 5


def test_rest_plan_conflicts_demo_and_benchmark() -> None:
    app = SwarmWeaverHTTPApp()
    tasks_payload = [t.to_dict() for t in build_demo_10_agent_swarm()]

    plan_resp = app.handle_http(
        "POST",
        "/api/plan",
        {"Content-Type": "application/json"},
        json.dumps({"tasks": tasks_payload, "validate_contracts": True}).encode("utf-8"),
    )
    assert plan_resp.status == 200
    plan_data = _decode(plan_resp.body)
    assert plan_data["total_waves"] >= 6
    assert len(plan_data["conflicts_resolved"]) >= 2

    conflicts_resp = app.handle_http(
        "POST",
        "/api/conflicts",
        {"Content-Type": "application/json"},
        json.dumps(
            {
                "tasks": [
                    {"task_id": "a", "write_globs": ["src/auth.py"]},
                    {"task_id": "b", "read_globs": ["src/auth.py"]},
                ]
            }
        ).encode("utf-8"),
    )
    assert conflicts_resp.status == 200
    conflicts_data = _decode(conflicts_resp.body)
    assert conflicts_data["safe_for_parallel_execution"] is False
    assert conflicts_data["conflict_count"] == 1

    demo_resp = app.handle_http("GET", "/api/demo", {})
    assert demo_resp.status == 200
    demo_data = _decode(demo_resp.body)
    assert demo_data["run_2_checkpoint_resume"]["status"] == "COMPLETED"
    assert demo_data["journal_integrity"]["valid"] is True

    bench_resp = app.handle_http("GET", "/api/benchmark", {})
    assert bench_resp.status == 200
    bench_data = _decode(bench_resp.body)
    assert bench_data["parallel_conflict_guard_50_tasks"]["race_conditions_observed"] == 0


def test_mcp_streamable_http_resources_and_prompts() -> None:
    app = SwarmWeaverHTTPApp()

    assert app.handle_http("GET", "/mcp", {}).status == 405
    sse_resp = app.handle_http("GET", "/mcp", {"Accept": "text/event-stream"})
    assert sse_resp.status == 200
    assert "Mcp-Session-Id" in sse_resp.headers
    assert "event: ready" in sse_resp.body.decode("utf-8")

    init_resp = app.handle_http(
        "POST",
        "/mcp",
        {"Content-Type": "application/json"},
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}).encode("utf-8"),
    )
    assert init_resp.status == 200
    sid = init_resp.headers["Mcp-Session-Id"]

    notif = app.handle_http(
        "POST",
        "/mcp",
        {"Content-Type": "application/json", "Mcp-Session-Id": sid},
        json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}).encode("utf-8"),
    )
    assert notif.status == 202
    assert notif.body == b""

    batch_calls = [
        {"jsonrpc": "2.0", "id": 2, "method": "resources/list"},
        {"jsonrpc": "2.0", "id": 3, "method": "resources/templates/list"},
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "resources/read",
            "params": {"uri": "swarmweaver://workflow/demo-10-agent"},
        },
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "resources/read",
            "params": {"uri": "swarmweaver://benchmark/summary"},
        },
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "resources/read",
            "params": {"uri": "swarmweaver://dag/lease-conflict-rules"},
        },
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "resources/read",
            "params": {"uri": "swarmweaver://workflow/01_ast_audit"},
        },
        {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "resources/read",
            "params": {"uri": "swarmweaver://workflow/UNKNOWN"},
        },
        {"jsonrpc": "2.0", "id": 9, "method": "prompts/list"},
        {
            "jsonrpc": "2.0",
            "id": 10,
            "method": "prompts/get",
            "params": {"name": "plan-safe-swarm", "arguments": {"objective": "Refactor auth"}},
        },
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "prompts/get",
            "params": {"name": "resolve-file-lease-conflict", "arguments": {"tasks_json": "[]"}},
        },
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "prompts/get",
            "params": {"name": "recover-crashed-workflow", "arguments": {"journal_path": "cp.jsonl"}},
        },
    ]
    batch_resp = app.handle_http(
        "POST",
        "/messages",
        {"Content-Type": "application/json", "Accept": "text/event-stream"},
        json.dumps(batch_calls).encode("utf-8"),
    )
    assert batch_resp.status == 200
    assert "event: message" in batch_resp.body.decode("utf-8")

    stdio_in = io.StringIO('{"jsonrpc":"2.0","id":1,"method":"ping"}\n')
    stdio_out = io.StringIO()
    app.mcp_server.serve_stdio(stdin=stdio_in, stdout=stdio_out)
    assert json.loads(stdio_out.getvalue())["result"] == {}


def test_hardening_guards_and_live_tcp_roundtrip() -> None:
    app = SwarmWeaverHTTPApp(max_body_bytes=512)

    assert app.handle_http("OPTIONS", "/api/plan", {}).status == 204
    assert app.handle_http("GET", "/missing", {}).status == 404
    assert app.handle_http("DELETE", "/health", {}).status == 405
    assert app.handle_http("POST", "/api/plan", {}, b"x" * 600).status == 413
    assert app.handle_http("POST", "/api/plan", {"Content-Length": "bad"}, b"{}").status == 400
    assert app.handle_http("POST", "/api/plan", {}, b"{not-json").status == 400
    assert app.handle_http("POST", "/api/plan", {}, json.dumps({"tasks": []}).encode("utf-8")).status == 400

    def _boom(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("sensitive-internal-state")

    app._dispatch_route = _boom  # type: ignore[method-assign]
    err = app.handle_http("GET", "/health", {"X-Request-ID": "trace-sw-500"})
    assert err.status == 500
    assert "sensitive-internal-state" not in err.body.decode("utf-8")
    assert _decode(err.body)["request_id"] == "trace-sw-500"

    server = create_http_server(host="127.0.0.1", port=0, max_body_bytes=2048)
    host, port = server.server_address[:2]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        data = _decode(resp.read())
        conn.close()
        assert resp.status == 200
        assert data["status"] == "ok"
    finally:
        server.shutdown()
        server.server_close()

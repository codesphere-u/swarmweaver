"""Zero-dependency HTTP, Streamable MCP, REST API, and DAG Wave Workbench for SwarmWeaver."""

from __future__ import annotations

import json
import re
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from swarmweaver import __version__
from swarmweaver.benchmark import run_benchmark_suite
from swarmweaver.dag import CycleDetectedError, DAGCompiler, DAGValidationError
from swarmweaver.demo import build_demo_10_agent_swarm, run_demo_workflow
from swarmweaver.mcp_server import (
    MCP_PROMPTS,
    MCP_PROTOCOL_VERSION,
    MCP_RESOURCE_TEMPLATES,
    MCP_RESOURCES,
    MCP_TOOLS,
    SwarmWeaverMCPServer,
)
from swarmweaver.models import TaskSpec

DEFAULT_MAX_BODY_BYTES = 1_048_576
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class PayloadTooLargeError(ValueError):
    """Raised when an incoming HTTP request exceeds the configured max body size."""


class BadRequestError(ValueError):
    """Raised when an incoming HTTP request has malformed headers or JSON payload."""


@dataclass(frozen=True)
class HTTPResponse:
    """Framework-agnostic HTTP response envelope."""

    status: int
    headers: dict[str, str]
    body: bytes


class MetricsCollector:
    """Thread-safe HTTP, MCP, DAG planning, and benchmark telemetry collector."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = time.monotonic()
        self._requests_total = 0
        self._errors_total = 0
        self._total_latency_ms = 0.0
        self._status_counts: dict[int, int] = {}
        self._method_counts: dict[str, int] = {}
        self._mcp_method_counts: dict[str, int] = {}
        self._plans_compiled = 0
        self._conflicts_detected = 0
        self._demos_run = 0
        self._benchmarks_run = 0

    def record_http(self, method: str, status: int, elapsed_ms: float) -> None:
        with self._lock:
            self._requests_total += 1
            if status >= 500:
                self._errors_total += 1
            self._total_latency_ms += elapsed_ms
            self._status_counts[status] = self._status_counts.get(status, 0) + 1
            self._method_counts[method] = self._method_counts.get(method, 0) + 1

    def record_mcp(self, method: str) -> None:
        if not method:
            return
        with self._lock:
            self._mcp_method_counts[method] = self._mcp_method_counts.get(method, 0) + 1

    def record_plan(self, conflicts_resolved: int) -> None:
        with self._lock:
            self._plans_compiled += 1
            self._conflicts_detected += max(0, conflicts_resolved)

    def record_demo(self) -> None:
        with self._lock:
            self._demos_run += 1

    def record_benchmark(self) -> None:
        with self._lock:
            self._benchmarks_run += 1

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            uptime = max(0.0, time.monotonic() - self._started_at)
            avg_ms = self._total_latency_ms / self._requests_total if self._requests_total else 0.0
            return {
                "service": "swarmweaver",
                "version": __version__,
                "uptime_seconds": round(uptime, 3),
                "requests_total": self._requests_total,
                "errors_total": self._errors_total,
                "avg_latency_ms": round(avg_ms, 4),
                "status_counts": {str(k): v for k, v in sorted(self._status_counts.items())},
                "method_counts": dict(sorted(self._method_counts.items())),
                "mcp_method_counts": dict(sorted(self._mcp_method_counts.items())),
                "plans_compiled": self._plans_compiled,
                "conflicts_detected": self._conflicts_detected,
                "demos_run": self._demos_run,
                "benchmarks_run": self._benchmarks_run,
            }

    def to_prometheus(self) -> str:
        snap = self.to_dict()
        lines = [
            "# HELP swarmweaver_uptime_seconds Seconds since SwarmWeaver HTTP server started.",
            "# TYPE swarmweaver_uptime_seconds gauge",
            f"swarmweaver_uptime_seconds {snap['uptime_seconds']}",
            "# HELP swarmweaver_http_requests_total Total HTTP requests processed.",
            "# TYPE swarmweaver_http_requests_total counter",
            f"swarmweaver_http_requests_total {snap['requests_total']}",
            "# HELP swarmweaver_http_errors_total Total HTTP 5xx errors.",
            "# TYPE swarmweaver_http_errors_total counter",
            f"swarmweaver_http_errors_total {snap['errors_total']}",
            "# HELP swarmweaver_plans_compiled_total Total DAG wave plans compiled via REST.",
            "# TYPE swarmweaver_plans_compiled_total counter",
            f"swarmweaver_plans_compiled_total {snap['plans_compiled']}",
            "# HELP swarmweaver_conflicts_detected_total Total file-scope lease conflicts resolved.",
            "# TYPE swarmweaver_conflicts_detected_total counter",
            f"swarmweaver_conflicts_detected_total {snap['conflicts_detected']}",
            "# HELP swarmweaver_benchmarks_run_total Total benchmark suite runs.",
            "# TYPE swarmweaver_benchmarks_run_total counter",
            f"swarmweaver_benchmarks_run_total {snap['benchmarks_run']}",
        ]
        for status_code, count in snap["status_counts"].items():
            lines.append(
                f'swarmweaver_http_responses_by_status{{status="{status_code}"}} {count}'
            )
        for mcp_method, count in snap["mcp_method_counts"].items():
            safe_method = mcp_method.replace('"', '\\"')
            lines.append(f'swarmweaver_mcp_calls_total{{method="{safe_method}"}} {count}')
        return "\n".join(lines) + "\n"


def sanitize_request_id(raw_value: str | None) -> str:
    """Validate and sanitize an incoming X-Request-ID or generate a fresh hex UUID."""
    if raw_value is not None:
        stripped = raw_value.strip()
        if _REQUEST_ID_RE.fullmatch(stripped):
            return stripped
    return uuid.uuid4().hex


def _get_header(headers: Mapping[str, str], name: str, default: str = "") -> str:
    target = name.lower()
    for key, val in headers.items():
        if key.lower() == target:
            return val
    return default


def parse_content_length(headers: Mapping[str, str]) -> int | None:
    """Parse Content-Length header safely, rejecting negative or malformed values."""
    raw = _get_header(headers, "content-length", "")
    if not raw:
        return None
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise BadRequestError("Invalid Content-Length header.") from exc
    if value < 0:
        raise BadRequestError("Negative Content-Length header.")
    return value


def build_security_headers(
    path: str,
    request_id: str,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build hardened response headers including CORS, security controls, and X-Request-ID."""
    headers: dict[str, str] = {
        "X-Request-ID": request_id,
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "SAMEORIGIN",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "X-Request-ID, Mcp-Session-Id",
    }
    if path.startswith("/api/") or path in ("/api", "/mcp", "/messages"):
        headers["Cache-Control"] = "no-store"
    if extra:
        headers.update(extra)
    return headers


def _render_portal_html() -> str:
    """Return the dark-mode interactive SwarmWeaver DAG Wave & Lease Workbench HTML."""
    sample_tasks = json.dumps([t.to_dict() for t in build_demo_10_agent_swarm()], indent=2)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>SwarmWeaver &mdash; Multi-Agent DAG &amp; Lease Workbench</title>
  <style>
    :root {{
      --bg: #070b12;
      --surface: #0f1724;
      --surface-2: #162033;
      --border: #25344d;
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --emerald: #10b981;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: var(--sans);
      background: radial-gradient(circle at top right, #16243d, var(--bg) 55%);
      color: var(--text);
      line-height: 1.5;
    }}
    header {{
      border-bottom: 1px solid var(--border);
      background: rgba(15, 23, 36, 0.92);
      padding: 1rem 1.5rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 0.75rem;
    }}
    .brand h1 {{ margin: 0; font-size: 1.25rem; }}
    .brand p {{ margin: 0.2rem 0 0; color: var(--muted); font-size: 0.85rem; }}
    .pill {{
      padding: 0.3rem 0.75rem;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--surface-2);
      font-size: 0.8rem;
      font-family: var(--mono);
    }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 1.5rem; display: grid; gap: 1.25rem; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1.25rem; }}
    .card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 1.15rem;
    }}
    .card h2 {{ margin: 0 0 0.75rem; font-size: 1.02rem; }}
    .controls {{ display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 0.75rem; }}
    button {{
      background: var(--accent);
      color: #06131f;
      border: none;
      border-radius: 8px;
      padding: 0.5rem 0.9rem;
      font-weight: 600;
      font-size: 0.85rem;
      cursor: pointer;
    }}
    button.secondary {{
      background: var(--surface-2);
      color: var(--text);
      border: 1px solid var(--border);
    }}
    textarea {{
      width: 100%;
      min-height: 170px;
      background: var(--bg);
      color: var(--text);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0.75rem;
      font-family: var(--mono);
      font-size: 0.78rem;
      resize: vertical;
    }}
    pre {{
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0.75rem;
      font-family: var(--mono);
      font-size: 0.78rem;
      overflow-x: auto;
      white-space: pre-wrap;
      word-break: break-word;
      margin: 0.5rem 0 0;
    }}
    .muted {{ color: var(--muted); font-size: 0.82rem; }}
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <h1>SwarmWeaver &mdash; DAG Wave Compiler &amp; Lease Workbench</h1>
      <p>Conflict-Free Parallel Execution Waves &bull; File-Scope Lease Guard &bull; Crash-Safe Checkpoint Resume</p>
    </div>
    <div id="health-pill" class="pill">Checking scheduler...</div>
  </header>
  <main>
    <div class="grid">
      <section class="card">
        <h2>1. Compile DAG Waves &amp; Check File-Lease Conflicts</h2>
        <div class="controls">
          <button type="button" onclick="compilePlan()">Compile Waves (/api/plan)</button>
          <button type="button" class="secondary" onclick="checkConflicts()">Check Collisions (/api/conflicts)</button>
        </div>
        <textarea id="tasks-input" spellcheck="false">{sample_tasks}</textarea>
        <pre id="plan-output">Click "Compile Waves" to schedule conflict-free parallel sub-waves.</pre>
      </section>
      <section class="card">
        <h2>2. Crash-Safe Resume Demo &amp; 200-Task Benchmark</h2>
        <div class="controls">
          <button type="button" onclick="runDemo()">Run 10-Agent Crash/Resume Demo</button>
          <button type="button" class="secondary" onclick="runBenchmark()">Run Benchmark Suite</button>
          <button type="button" class="secondary" onclick="loadMetrics()">Telemetry</button>
        </div>
        <pre id="demo-output">Execute the 10-agent crash-resume demo or 200-task DAG benchmark.</pre>
      </section>
    </div>
    <section class="card">
      <h2>3. Streamable HTTP MCP &amp; Endpoints</h2>
      <p class="muted">POST /mcp (JSON-RPC 2.0) &bull; GET /mcp (SSE) &bull; POST /api/plan &bull; POST /api/conflicts &bull; GET /api/demo &bull; GET /api/benchmark &bull; GET /metrics</p>
      <pre>swarmweaver serve-http --host 0.0.0.0 --port 8000
curl -s http://127.0.0.1:8000/health | python3 -m json.tool</pre>
    </section>
  </main>
  <script>
    async function refreshHealth() {{
      const h = await fetch("/health").then(r => r.json());
      document.getElementById("health-pill").textContent = "DAG Kernel: " + h.status + " | v" + h.version;
    }}
    async function compilePlan() {{
      const tasks = JSON.parse(document.getElementById("tasks-input").value);
      const data = await fetch("/api/plan", {{
        method: "POST",
        headers: {{"Content-Type": "application/json"}},
        body: JSON.stringify({{tasks, validate_contracts: true}})
      }}).then(r => r.json());
      document.getElementById("plan-output").textContent = JSON.stringify(data, null, 2);
    }}
    async function checkConflicts() {{
      const tasks = JSON.parse(document.getElementById("tasks-input").value);
      const data = await fetch("/api/conflicts", {{
        method: "POST",
        headers: {{"Content-Type": "application/json"}},
        body: JSON.stringify({{tasks}})
      }}).then(r => r.json());
      document.getElementById("plan-output").textContent = JSON.stringify(data, null, 2);
    }}
    async function runDemo() {{
      const data = await fetch("/api/demo").then(r => r.json());
      document.getElementById("demo-output").textContent = JSON.stringify(data, null, 2);
    }}
    async function runBenchmark() {{
      const data = await fetch("/api/benchmark").then(r => r.json());
      document.getElementById("demo-output").textContent = JSON.stringify(data, null, 2);
    }}
    async function loadMetrics() {{
      const data = await fetch("/metrics?format=json").then(r => r.json());
      document.getElementById("demo-output").textContent = JSON.stringify(data, null, 2);
    }}
    refreshHealth();
  </script>
</body>
</html>
"""


PORTAL_HTML = _render_portal_html()


class SwarmWeaverHTTPApp:
    """Framework-agnostic HTTP + Streamable MCP + REST application for SwarmWeaver."""

    def __init__(self, *, max_body_bytes: int = DEFAULT_MAX_BODY_BYTES) -> None:
        if max_body_bytes <= 0:
            raise ValueError("max_body_bytes must be positive")
        self.max_body_bytes = max_body_bytes
        self.mcp_server = SwarmWeaverMCPServer()
        self.metrics = MetricsCollector()
        self._sessions: dict[str, float] = {}
        self._route_table: dict[str, dict[str, Callable[..., HTTPResponse]]] = {
            "/": {"GET": self._handle_root},
            "/health": {"GET": self._handle_health},
            "/healthz": {"GET": self._handle_health},
            "/readyz": {"GET": self._handle_readyz},
            "/info": {"GET": self._handle_info},
            "/metrics": {"GET": self._handle_metrics},
            "/mcp": {"GET": self._handle_mcp_get, "POST": self._handle_mcp_post},
            "/messages": {"POST": self._handle_mcp_post},
            "/api/plan": {"POST": self._handle_api_plan},
            "/api/conflicts": {"POST": self._handle_api_conflicts},
            "/api/demo": {"GET": self._handle_api_demo, "POST": self._handle_api_demo},
            "/api/benchmark": {
                "GET": self._handle_api_benchmark,
                "POST": self._handle_api_benchmark,
            },
        }

    def service_info(self) -> dict[str, Any]:
        """Return machine-readable service metadata and endpoint inventory."""
        return {
            "service": "swarmweaver",
            "name": "SwarmWeaver DAG Orchestrator & MCP Server",
            "version": __version__,
            "description": (
                "Deterministic Multi-Agent DAG Orchestrator, File-Scope Lease Conflict Guard & "
                "Crash-Safe Checkpoint Resume Engine."
            ),
            "protocol": "mcp-streamable-http",
            "mcp_protocol_version": MCP_PROTOCOL_VERSION,
            "endpoints": {
                "portal": "GET / (Accept: text/html)",
                "health": ["GET /health", "GET /healthz", "GET /readyz"],
                "info": "GET /info",
                "metrics": "GET /metrics",
                "mcp_streamable": ["POST /mcp", "GET /mcp (SSE)", "POST /messages"],
                "rest_api": [
                    "POST /api/plan",
                    "POST /api/conflicts",
                    "GET|POST /api/demo",
                    "GET|POST /api/benchmark",
                ],
            },
            "mcp": {
                "tools": [t["name"] for t in MCP_TOOLS],
                "resources": [r["uri"] for r in MCP_RESOURCES],
                "resource_templates": [t["uriTemplate"] for t in MCP_RESOURCE_TEMPLATES],
                "prompts": [p["name"] for p in MCP_PROMPTS],
            },
        }

    def handle_http(
        self,
        method: str,
        target: str,
        headers: Mapping[str, str],
        body: bytes = b"",
    ) -> HTTPResponse:
        """Dispatch a single HTTP request with hardening, metrics, and zero traceback leaks."""
        t0 = time.perf_counter()
        parsed = urlsplit(target)
        path = parsed.path or "/"
        query = parse_qs(parsed.query)
        upper_method = method.upper()
        req_id = sanitize_request_id(_get_header(headers, "x-request-id"))

        try:
            content_len = parse_content_length(headers)
            if content_len is not None and content_len > self.max_body_bytes:
                raise PayloadTooLargeError(
                    f"Request body exceeds limit of {self.max_body_bytes} bytes."
                )
            if len(body) > self.max_body_bytes:
                raise PayloadTooLargeError(
                    f"Request body exceeds limit of {self.max_body_bytes} bytes."
                )

            if upper_method == "OPTIONS":
                resp = self._handle_options(path, req_id)
            else:
                effective_method = "GET" if upper_method == "HEAD" else upper_method
                resp = self._dispatch_route(effective_method, path, query, headers, body, req_id)
                if upper_method == "HEAD":
                    resp = HTTPResponse(status=resp.status, headers=resp.headers, body=b"")

        except PayloadTooLargeError as exc:
            resp = self._json_response(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                {"error": "Payload Too Large", "code": 413, "detail": str(exc)},
                path,
                req_id,
            )
        except BadRequestError as exc:
            resp = self._json_response(
                HTTPStatus.BAD_REQUEST,
                {"error": "Bad Request", "code": 400, "detail": str(exc)},
                path,
                req_id,
            )
        except Exception:  # noqa: BLE001 - top-level server boundary must never leak tracebacks
            resp = self._json_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "Internal server error", "code": 500, "request_id": req_id},
                path,
                req_id,
            )

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        self.metrics.record_http(upper_method, resp.status, elapsed_ms)
        return resp

    def _dispatch_route(
        self,
        method: str,
        path: str,
        query: dict[str, list[str]],
        headers: Mapping[str, str],
        body: bytes,
        req_id: str,
    ) -> HTTPResponse:
        handlers = self._route_table.get(path)
        if handlers is None:
            return self._json_response(
                HTTPStatus.NOT_FOUND,
                {"error": "Not Found", "path": path},
                path,
                req_id,
            )
        handler = handlers.get(method)
        if handler is None:
            allowed = sorted([*handlers.keys(), "OPTIONS"])
            if "GET" in handlers and "HEAD" not in allowed:
                allowed = sorted([*allowed, "HEAD"])
            return self._json_response(
                HTTPStatus.METHOD_NOT_ALLOWED,
                {"error": "Method Not Allowed", "allowed": allowed},
                path,
                req_id,
                extra_headers={"Allow": ", ".join(allowed)},
            )
        return handler(path=path, query=query, headers=headers, body=body, req_id=req_id)

    def _handle_options(self, path: str, req_id: str) -> HTTPResponse:
        headers = build_security_headers(
            path,
            req_id,
            extra={
                "Access-Control-Allow-Methods": "GET, POST, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": (
                    "Content-Type, Authorization, X-Request-ID, Accept, Mcp-Session-Id"
                ),
                "Access-Control-Max-Age": "86400",
                "Content-Length": "0",
            },
        )
        return HTTPResponse(status=HTTPStatus.NO_CONTENT, headers=headers, body=b"")

    def _handle_root(
        self,
        *,
        path: str,
        headers: Mapping[str, str],
        req_id: str,
        **_kwargs: Any,
    ) -> HTTPResponse:
        accept = _get_header(headers, "accept", "").lower()
        if "text/html" in accept:
            raw = PORTAL_HTML.encode("utf-8")
            resp_headers = build_security_headers(
                path,
                req_id,
                extra={
                    "Content-Type": "text/html; charset=utf-8",
                    "Content-Length": str(len(raw)),
                },
            )
            return HTTPResponse(status=HTTPStatus.OK, headers=resp_headers, body=raw)
        return self._json_response(HTTPStatus.OK, self.service_info(), path, req_id)

    def _handle_health(self, *, path: str, req_id: str, **_kwargs: Any) -> HTTPResponse:
        snap = self.metrics.to_dict()
        payload = {
            "status": "ok",
            "service": "swarmweaver",
            "version": __version__,
            "uptime_seconds": snap["uptime_seconds"],
            "checks": {
                "dag_compiler": "ready",
                "lease_manager": "ready",
                "checkpoint_engine": "sha256-chain",
            },
        }
        return self._json_response(HTTPStatus.OK, payload, path, req_id)

    def _handle_readyz(self, *, path: str, req_id: str, **_kwargs: Any) -> HTTPResponse:
        snap = self.metrics.to_dict()
        payload = {
            "status": "ready",
            "ready": True,
            "service": "swarmweaver",
            "version": __version__,
            "uptime_seconds": snap["uptime_seconds"],
        }
        return self._json_response(HTTPStatus.OK, payload, path, req_id)

    def _handle_info(self, *, path: str, req_id: str, **_kwargs: Any) -> HTTPResponse:
        return self._json_response(HTTPStatus.OK, self.service_info(), path, req_id)

    def _handle_metrics(
        self,
        *,
        path: str,
        query: dict[str, list[str]],
        headers: Mapping[str, str],
        req_id: str,
        **_kwargs: Any,
    ) -> HTTPResponse:
        accept = _get_header(headers, "accept", "").lower()
        fmt = (query.get("format") or [""])[0].lower()
        if fmt == "json" or ("application/json" in accept and "text/plain" not in accept):
            return self._json_response(HTTPStatus.OK, self.metrics.to_dict(), path, req_id)

        raw = self.metrics.to_prometheus().encode("utf-8")
        resp_headers = build_security_headers(
            path,
            req_id,
            extra={
                "Content-Type": "text/plain; version=0.0.4; charset=utf-8",
                "Content-Length": str(len(raw)),
            },
        )
        return HTTPResponse(status=HTTPStatus.OK, headers=resp_headers, body=raw)

    def _handle_mcp_get(
        self,
        *,
        path: str,
        headers: Mapping[str, str],
        req_id: str,
        **_kwargs: Any,
    ) -> HTTPResponse:
        accept = _get_header(headers, "accept", "").lower()
        if "text/event-stream" not in accept:
            return self._json_response(
                HTTPStatus.METHOD_NOT_ALLOWED,
                {
                    "error": "Method Not Allowed",
                    "detail": "GET /mcp requires Accept: text/event-stream; use POST /mcp.",
                    "allowed": ["POST", "OPTIONS"],
                },
                path,
                req_id,
                extra_headers={"Allow": "POST, OPTIONS"},
            )

        session_id = _get_header(headers, "mcp-session-id") or uuid.uuid4().hex
        self._sessions[session_id] = time.time()
        ready_meta = json.dumps(
            {
                "server": "swarmweaver-mcp",
                "version": __version__,
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "sessionId": session_id,
            }
        )
        sse_text = (
            f"event: endpoint\ndata: /messages?sessionId={session_id}\n\n"
            f"event: ready\ndata: {ready_meta}\n\n"
        )
        raw = sse_text.encode("utf-8")
        resp_headers = build_security_headers(
            path,
            req_id,
            extra={
                "Content-Type": "text/event-stream; charset=utf-8",
                "Cache-Control": "no-cache",
                "Mcp-Session-Id": session_id,
                "Content-Length": str(len(raw)),
            },
        )
        return HTTPResponse(status=HTTPStatus.OK, headers=resp_headers, body=raw)

    def _handle_mcp_post(
        self,
        *,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        req_id: str,
        **_kwargs: Any,
    ) -> HTTPResponse:
        try:
            decoded = body.decode("utf-8")
            payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return self._json_response(
                HTTPStatus.BAD_REQUEST,
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": f"Parse error: {exc}"},
                },
                path,
                req_id,
            )

        session_id = _get_header(headers, "mcp-session-id")
        if isinstance(payload, dict):
            method = str(payload.get("method", ""))
            self.metrics.record_mcp(method)
            if method == "initialize" and not session_id:
                session_id = uuid.uuid4().hex
                self._sessions[session_id] = time.time()
            rpc_resp = self.mcp_server.handle_request(payload)
            if rpc_resp is None:
                extra = {"Content-Length": "0"}
                if session_id:
                    extra["Mcp-Session-Id"] = session_id
                return HTTPResponse(
                    status=HTTPStatus.ACCEPTED,
                    headers=build_security_headers(path, req_id, extra=extra),
                    body=b"",
                )
            return self._mcp_payload_response(rpc_resp, path, headers, req_id, session_id)

        if isinstance(payload, list):
            if not payload:
                return self._json_response(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": -32600, "message": "Invalid Request: empty batch."},
                    },
                    path,
                    req_id,
                )
            batch_out: list[dict[str, Any]] = []
            for item in payload:
                if isinstance(item, dict):
                    self.metrics.record_mcp(str(item.get("method", "")))
                item_resp = self.mcp_server.handle_request(item)
                if item_resp is not None:
                    batch_out.append(item_resp)
            if not batch_out:
                return HTTPResponse(
                    status=HTTPStatus.ACCEPTED,
                    headers=build_security_headers(path, req_id, extra={"Content-Length": "0"}),
                    body=b"",
                )
            return self._mcp_payload_response(batch_out, path, headers, req_id, session_id)

        return self._json_response(
            HTTPStatus.BAD_REQUEST,
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32600, "message": "Invalid Request: expected JSON object."},
            },
            path,
            req_id,
        )

    def _mcp_payload_response(
        self,
        data: Any,
        path: str,
        headers: Mapping[str, str],
        req_id: str,
        session_id: str,
    ) -> HTTPResponse:
        extra: dict[str, str] = {}
        if session_id:
            extra["Mcp-Session-Id"] = session_id
        accept = _get_header(headers, "accept", "").lower()
        if "text/event-stream" in accept and "application/json" not in accept:
            sse_raw = f"event: message\ndata: {json.dumps(data)}\n\n".encode()
            extra["Content-Type"] = "text/event-stream; charset=utf-8"
            extra["Cache-Control"] = "no-cache"
            extra["Content-Length"] = str(len(sse_raw))
            return HTTPResponse(
                status=HTTPStatus.OK,
                headers=build_security_headers(path, req_id, extra=extra),
                body=sse_raw,
            )
        return self._json_response(HTTPStatus.OK, data, path, req_id, extra_headers=extra)

    def _handle_api_plan(
        self,
        *,
        path: str,
        body: bytes,
        req_id: str,
        **_kwargs: Any,
    ) -> HTTPResponse:
        payload = self._parse_json_object(body)
        raw_tasks = payload.get("tasks")
        if not isinstance(raw_tasks, list) or not raw_tasks:
            raise BadRequestError("Field 'tasks' must be a non-empty list of TaskSpec objects.")
        validate_contracts = bool(payload.get("validate_contracts", False))
        try:
            specs = [TaskSpec.from_dict(item) for item in raw_tasks]
            plan = DAGCompiler(specs, validate_contracts=validate_contracts).compile()
        except (CycleDetectedError, DAGValidationError, ValueError, KeyError, TypeError) as exc:
            raise BadRequestError(str(exc)) from exc

        self.metrics.record_plan(len(plan.conflicts_resolved))
        return self._json_response(HTTPStatus.OK, plan.to_dict(), path, req_id)

    def _handle_api_conflicts(
        self,
        *,
        path: str,
        body: bytes,
        req_id: str,
        **_kwargs: Any,
    ) -> HTTPResponse:
        payload = self._parse_json_object(body)
        raw_tasks = payload.get("tasks")
        if not isinstance(raw_tasks, list):
            raise BadRequestError("Field 'tasks' must be a list of task specs.")
        try:
            specs = [
                TaskSpec(
                    task_id=str(item["task_id"]),
                    title=str(item.get("title", item["task_id"])),
                    read_globs=list(item.get("read_globs", [])),
                    write_globs=list(item.get("write_globs", [])),
                )
                for item in raw_tasks
            ]
        except (KeyError, TypeError, ValueError) as exc:
            raise BadRequestError(str(exc)) from exc

        conflicts = DAGCompiler.check_parallel_conflicts(specs)
        self.metrics.record_plan(len(conflicts))
        return self._json_response(
            HTTPStatus.OK,
            {
                "safe_for_parallel_execution": len(conflicts) == 0,
                "conflict_count": len(conflicts),
                "conflicts": [c.to_dict() for c in conflicts],
            },
            path,
            req_id,
        )

    def _handle_api_demo(self, *, path: str, req_id: str, **_kwargs: Any) -> HTTPResponse:
        self.metrics.record_demo()
        return self._json_response(HTTPStatus.OK, run_demo_workflow(), path, req_id)

    def _handle_api_benchmark(self, *, path: str, req_id: str, **_kwargs: Any) -> HTTPResponse:
        self.metrics.record_benchmark()
        return self._json_response(HTTPStatus.OK, run_benchmark_suite(), path, req_id)

    @staticmethod
    def _parse_json_object(body: bytes) -> dict[str, Any]:
        if not body:
            raise BadRequestError("Request body must be a JSON object.")
        try:
            data = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadRequestError(f"Invalid JSON payload: {exc}") from exc
        if not isinstance(data, dict):
            raise BadRequestError("JSON payload must be an object.")
        return data

    def _json_response(
        self,
        status: int | HTTPStatus,
        payload: Any,
        path: str,
        req_id: str,
        *,
        extra_headers: Mapping[str, str] | None = None,
    ) -> HTTPResponse:
        raw = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        merged_extra: dict[str, str] = {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": str(len(raw)),
        }
        if extra_headers:
            merged_extra.update(extra_headers)
        headers = build_security_headers(path, req_id, extra=merged_extra)
        return HTTPResponse(status=int(status), headers=headers, body=raw)


class SwarmWeaverHTTPServer(ThreadingHTTPServer):
    """Threaded stdlib HTTP server hosting SwarmWeaverHTTPApp."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        request_handler_class: type[SwarmWeaverHTTPHandler],
        app: SwarmWeaverHTTPApp,
    ) -> None:
        self.app = app
        super().__init__(server_address, request_handler_class)


class SwarmWeaverHTTPHandler(BaseHTTPRequestHandler):
    """Stdlib HTTP request handler delegating directly to SwarmWeaverHTTPApp."""

    server: SwarmWeaverHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def _read_request_body(self) -> tuple[bytes, HTTPResponse | None]:
        header_map = {k: v for k, v in self.headers.items()}
        req_id = sanitize_request_id(_get_header(header_map, "x-request-id"))
        path = urlsplit(self.path).path or "/"
        try:
            content_len = parse_content_length(header_map)
        except BadRequestError as exc:
            return b"", self.server.app._json_response(
                HTTPStatus.BAD_REQUEST,
                {"error": "Bad Request", "code": 400, "detail": str(exc)},
                path,
                req_id,
            )
        if content_len is None:
            return b"", None
        if content_len > self.server.app.max_body_bytes:
            return b"", self.server.app._json_response(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                {
                    "error": "Payload Too Large",
                    "code": 413,
                    "detail": (
                        f"Request body exceeds limit of {self.server.app.max_body_bytes} bytes."
                    ),
                },
                path,
                req_id,
            )
        return self.rfile.read(content_len), None

    def _handle_any(self, method: str) -> None:
        header_map = {k: v for k, v in self.headers.items()}
        body, early_error = self._read_request_body()
        if early_error is not None:
            self._write_response(early_error, send_body=(method != "HEAD"))
            return
        resp = self.server.app.handle_http(method, self.path, header_map, body)
        self._write_response(resp, send_body=(method != "HEAD"))

    def _write_response(self, resp: HTTPResponse, *, send_body: bool) -> None:
        self.send_response(resp.status)
        for key, value in resp.headers.items():
            if key.lower() in ("server", "date", "connection"):
                continue
            self.send_header(key, value)
        self.send_header("Connection", "close")
        self.end_headers()
        if send_body and resp.body:
            self.wfile.write(resp.body)
        self.close_connection = True

    def do_GET(self) -> None:
        self._handle_any("GET")

    def do_HEAD(self) -> None:
        self._handle_any("HEAD")

    def do_POST(self) -> None:
        self._handle_any("POST")

    def do_OPTIONS(self) -> None:
        self._handle_any("OPTIONS")

    def do_PUT(self) -> None:
        self._handle_any("PUT")

    def do_DELETE(self) -> None:
        self._handle_any("DELETE")

    def do_PATCH(self) -> None:
        self._handle_any("PATCH")


def create_http_server(
    host: str = "127.0.0.1",
    port: int = 8000,
    *,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> SwarmWeaverHTTPServer:
    """Instantiate a ready-to-serve SwarmWeaverHTTPServer without blocking."""
    app = SwarmWeaverHTTPApp(max_body_bytes=max_body_bytes)
    return SwarmWeaverHTTPServer((host, port), SwarmWeaverHTTPHandler, app)


def serve_http(
    host: str = "127.0.0.1",
    port: int = 8000,
    *,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> None:
    """Start the SwarmWeaver HTTP/SSE MCP server and block until interrupted."""
    server = create_http_server(host=host, port=port, max_body_bytes=max_body_bytes)
    bound_host, bound_port = server.server_address[:2]
    host_display = (
        bound_host.decode("utf-8", errors="replace")
        if isinstance(bound_host, bytes)
        else bound_host
    )
    print(
        f"SwarmWeaver HTTP/MCP portal listening on http://{host_display}:{bound_port} "
        f"(MCP: /mcp, Health: /health, Metrics: /metrics)",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()

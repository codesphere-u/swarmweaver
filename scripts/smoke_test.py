"""End-to-end live HTTP & MCP smoke test script for SwarmWeaver."""

from __future__ import annotations

import http.client
import json
import sys
import threading

from swarmweaver.http_server import create_http_server


def _request(
    host: str,
    port: int,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    conn = http.client.HTTPConnection(host, port, timeout=5)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        raw = resp.read()
        resp_headers = {k.lower(): v for k, v in resp.getheaders()}
        return resp.status, resp_headers, raw
    finally:
        conn.close()


def run_smoke_test() -> None:
    """Boot SwarmWeaver HTTP server on an ephemeral port and verify core contracts."""
    server = create_http_server(host="127.0.0.1", port=0, max_body_bytes=8192)
    host, port = server.server_address[:2]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        status, headers, raw = _request(host, port, "GET", "/health")
        assert status == 200, f"/health returned {status}"
        assert json.loads(raw)["status"] == "ok"
        assert headers.get("x-content-type-options") == "nosniff"

        head_status, _, head_raw = _request(host, port, "HEAD", "/health")
        assert head_status == 200 and head_raw == b""

        html_status, html_headers, html_raw = _request(
            host, port, "GET", "/", headers={"Accept": "text/html"}
        )
        assert html_status == 200
        assert "SwarmWeaver" in html_raw.decode("utf-8")
        assert "text/html" in html_headers.get("content-type", "")

        mcp_get_status, _, _ = _request(host, port, "GET", "/mcp")
        assert mcp_get_status == 405

        init_body = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2024-11-05"},
            }
        ).encode("utf-8")
        init_status, init_headers, init_raw = _request(
            host,
            port,
            "POST",
            "/mcp",
            headers={"Content-Type": "application/json"},
            body=init_body,
        )
        assert init_status == 200
        assert "mcp-session-id" in init_headers
        assert json.loads(init_raw)["result"]["serverInfo"]["name"] == "swarmweaver-mcp"

        demo_status, _, demo_raw = _request(host, port, "GET", "/api/demo")
        assert demo_status == 200
        demo_data = json.loads(demo_raw)
        assert demo_data["run_2_checkpoint_resume"]["status"] == "COMPLETED"
        assert demo_data["journal_integrity"]["valid"] is True

        oversized_status, _, _ = _request(
            host,
            port,
            "POST",
            "/api/plan",
            headers={"Content-Type": "application/json"},
            body=b"x" * 9000,
        )
        assert oversized_status == 413

        print(f"SwarmWeaver smoke test PASSED against http://{host}:{port}")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    try:
        run_smoke_test()
    except Exception as exc:  # noqa: BLE001
        print(f"Smoke test FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

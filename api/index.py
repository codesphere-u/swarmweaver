"""FastAPI ASGI / Vercel entrypoint for SwarmWeaver."""

from __future__ import annotations

from fastapi import FastAPI, Request, Response

from swarmweaver import __version__
from swarmweaver.http_server import (
    DEFAULT_MAX_BODY_BYTES,
    BadRequestError,
    PayloadTooLargeError,
    SwarmWeaverHTTPApp,
    parse_content_length,
)


async def _read_bounded_body(request: Request, max_body_bytes: int) -> bytes:
    content_len = parse_content_length(request.headers)
    if content_len is not None and content_len > max_body_bytes:
        raise PayloadTooLargeError(f"Request body exceeds limit of {max_body_bytes} bytes.")

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_body_bytes:
            raise PayloadTooLargeError(f"Request body exceeds limit of {max_body_bytes} bytes.")
        chunks.append(chunk)
    return b"".join(chunks)


def create_fastapi_app(*, max_body_bytes: int = DEFAULT_MAX_BODY_BYTES) -> FastAPI:
    """Build a FastAPI ASGI app backed by SwarmWeaverHTTPApp."""
    core_app = SwarmWeaverHTTPApp(max_body_bytes=max_body_bytes)
    fastapi_app = FastAPI(
        title="SwarmWeaver DAG Orchestrator & MCP Server",
        version=__version__,
        description=(
            "Deterministic Multi-Agent DAG Orchestrator, File-Scope Lease Conflict Guard & "
            "Crash-Safe Checkpoint Resume Engine."
        ),
    )

    @fastapi_app.api_route(
        "/",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    )
    @fastapi_app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
        include_in_schema=False,
    )
    async def dispatch(request: Request, full_path: str = "") -> Response:
        _ = full_path
        target = f"{request.url.path}?{request.url.query}" if request.url.query else request.url.path
        try:
            body = await _read_bounded_body(request, core_app.max_body_bytes)
        except PayloadTooLargeError:
            http_resp = core_app.handle_http(
                request.method,
                target,
                request.headers,
                b"x" * (core_app.max_body_bytes + 1),
            )
            headers = {k: v for k, v in http_resp.headers.items() if k.lower() != "content-length"}
            return Response(content=http_resp.body, status_code=http_resp.status, headers=headers)
        except BadRequestError:
            http_resp = core_app.handle_http(
                request.method,
                target,
                {"content-length": "-1", **dict(request.headers)},
                b"",
            )
            headers = {k: v for k, v in http_resp.headers.items() if k.lower() != "content-length"}
            return Response(content=http_resp.body, status_code=http_resp.status, headers=headers)

        http_resp = core_app.handle_http(request.method, target, request.headers, body)
        headers = {k: v for k, v in http_resp.headers.items() if k.lower() != "content-length"}
        return Response(content=http_resp.body, status_code=http_resp.status, headers=headers)

    return fastapi_app


app = create_fastapi_app()

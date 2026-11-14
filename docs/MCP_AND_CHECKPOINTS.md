# MCP Server, HTTP Portal & Checkpoint Verification

## 1. Stdio MCP Server (`2024-11-05`)

Start the zero-dependency JSON-RPC 2.0 server:

```bash
swarmweaver mcp
```

Expose in Claude Desktop / Cursor:

```json
{
  "mcpServers": {
    "swarmweaver": {
      "command": "swarmweaver",
      "args": ["mcp"]
    }
  }
}
```

### Registered Tools
- `swarmweaver_plan_waves`: Compiles `TaskSpec` nodes into safe execution waves.
- `swarmweaver_check_file_conflicts`: Detects `WRITE_WRITE` and `READ_WRITE` overlaps across `read_globs` and `write_globs`.
- `swarmweaver_inspect_checkpoint`: Audits a SHA-256 `.jsonl` checkpoint journal and lists completed vs pending tasks.
- `swarmweaver_run_benchmark`: Runs the 4-part reproducible benchmark suite.

## 2. Verify SHA-256 Checkpoints from Disk

```bash
swarmweaver verify-journal .swarmweaver/checkpoint.jsonl --json
```

If any line is altered or reordered, `CheckpointJournal.verify_integrity()` reports `valid: false` with the exact `tampered_sequence` index.

## 3. Streamable HTTP/SSE Portal & REST API

```bash
swarmweaver serve-http --host 127.0.0.1 --port 8000
```

Routes include `GET /health`, `GET /readyz`, `GET /metrics`, `POST /mcp`, `GET /mcp`, `POST /api/plan`, `POST /api/conflicts`, `GET|POST /api/demo`, and `GET|POST /api/benchmark`.

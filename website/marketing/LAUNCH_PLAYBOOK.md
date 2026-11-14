# SwarmWeaver v1.1.0 — Multi-Agent Orchestrator & Launch Playbook

## Positioning
Zero-dependency deterministic DAG compiler, file-scope lease lock manager, typed blackboard, and SHA-256 crash-recovery engine for parallel AI coding agents.

## Launch Checklist
1. Verify package gates (`ruff check .`, `mypy swarmweaver api scripts`, `pytest -v --cov=swarmweaver --cov-fail-under=90`, `python3 scripts/smoke_test.py`).
2. Tag and release `v1.1.0` on GitHub (`codesphere-u/swarmweaver`) with live GitHub Pages (`https://codesphere-u.github.io/swarmweaver/`).
3. Publish Show HN / technical write-up centered on two concrete engineering failures solved in parallel agent teams: file overwrite races (`src/auth/jwt.py`) and lost progress after a late-stage step crash.
4. Distribute native MCP integration snippets (`swarmweaver mcp`) for Claude Desktop and Cursor users.

# SwarmWeaver v1.1.0 — Programmatic SEO & Developer Discovery Plan

## Target Intent Clusters
1. Multi-agent orchestration & parallel coding waves (`DAGCompiler`, topological wave splitting, critical path).
2. Agent file overwrite lock prevention (`FileScopeLeaseManager`, `WRITE_WRITE`, `READ_WRITE`, `::symbol` locks).
3. Crash-safe agent checkpoints & replay token reduction (`CheckpointJournal`, SHA-256 JSONL verification).
4. Python MCP orchestration server (`swarmweaver mcp`, `swarmweaver serve-http`).

## Structured Discovery Assets
- Canonical URLs & sitemap: `website/sitemap.xml`, `website/robots.txt` allowing AI crawler user-agents (`OAI-SearchBot`, `GPTBot`, `ClaudeBot`, `PerplexityBot`, `Google-Extended`).
- Machine-readable summaries: `llms.txt` and `llms-full.txt` mirrored at repository root and `website/`.
- Schema.org `@graph` JSON-LD (`Organization`, `Person`, `WebSite`, `SoftwareApplication`, `Offer`, `FAQPage`) on `website/index.html`.

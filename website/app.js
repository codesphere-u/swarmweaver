(() => {
  "use strict";

  const showToast = (msg) => {
    const el = document.getElementById("toast");
    if (!el) return;
    el.textContent = msg;
    el.classList.add("visible");
    window.clearTimeout(el._swTimer);
    el._swTimer = window.setTimeout(() => el.classList.remove("visible"), 2200);
  };

  const copyText = (text) => {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(
        () => showToast("Copied to clipboard"),
        () => showToast("Ready to copy")
      );
      return;
    }
    showToast("Selected command ready");
  };

  document.addEventListener("click", (ev) => {
    const copyBtn = ev.target.closest("[data-copy]");
    if (copyBtn) {
      copyText(copyBtn.getAttribute("data-copy") || "");
    }
  });

  /* Hero split button + popover */
  const heroCopyBtn = document.getElementById("heroCopyBtn");
  const heroToggleBtn = document.getElementById("heroToggleBtn");
  const heroPopover = document.getElementById("heroPopover");
  const heroCmdPreview = document.getElementById("heroCmdPreview");
  const popCmdCode = document.getElementById("popCmdCode");
  const popCopyBtn = document.getElementById("popCopyBtn");

  if (heroCopyBtn && heroCmdPreview) {
    heroCopyBtn.addEventListener("click", () => copyText(heroCmdPreview.textContent.trim()));
  }
  if (heroToggleBtn && heroPopover) {
    heroToggleBtn.addEventListener("click", () => {
      const open = heroPopover.classList.toggle("open");
      heroToggleBtn.setAttribute("aria-expanded", String(open));
    });
  }
  if (popCopyBtn && popCmdCode) {
    popCopyBtn.addEventListener("click", () => copyText(popCmdCode.textContent.trim()));
  }
  document.querySelectorAll("[data-pop-cmd]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const cmd = btn.getAttribute("data-pop-cmd") || "";
      document.querySelectorAll("[data-pop-cmd]").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      if (popCmdCode) popCmdCode.textContent = cmd;
      if (heroCmdPreview) heroCmdPreview.textContent = cmd;
    });
  });

  /* Home preview tabs */
  const homePreviewCode = document.getElementById("homePreviewCode");
  const previewPayloads = {
    plan: `$ swarmweaver plan
SwarmWeaver v1.1.0 — Conflict-Aware Wave Plan
Total Tasks          : 10   | Topological Levels : 5
Conflict-Free Waves  : 7    | Conflicts Auto-Split: 2
Critical Path        : 1170 ms (1.46x speedup vs 1710 ms)
--------------------------------------------------------------------
  Wave 01: 01_ast_audit
  Wave 02: 02_schema_migration, 03_jwt_verifier
  Wave 03: 04_rbac_policy   (split: WRITE_WRITE src/auth/jwt.py)
  Wave 04: 05_api_gateway
  Wave 05: 06_rate_limiter  (split: WRITE_WRITE src/api/deps.py)`,
    resume: `$ swarmweaver demo
[1] Compiled 10 tasks into 7 conflict-free waves (2 collisions split).
[2] Run #1 (Crash at '08_integration_tests'):
    status=FAILED | completed=8/10 | tokens_spent=8,600
[3] Run #2 (CheckpointJournal Resume):
    status=COMPLETED | replayed=8 nodes in 0.077 ms | saved=8,600 tokens
[4] SHA-256 Hash Chain Verified: valid=True | records=10`,
    mcp: `$ swarmweaver mcp   # JSON-RPC 2.0 (2024-11-05)
Tools:
  - swarmweaver_plan_waves
  - swarmweaver_check_file_conflicts
  - swarmweaver_inspect_checkpoint
  - swarmweaver_run_benchmark
Resources:
  - swarmweaver://workflow/demo-10-agent
  - swarmweaver://dag/lease-conflict-rules`
  };
  document.querySelectorAll("[data-preview-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = btn.getAttribute("data-preview-tab");
      document.querySelectorAll("[data-preview-tab]").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      if (homePreviewCode && previewPayloads[key]) {
        homePreviewCode.textContent = previewPayloads[key];
      }
    });
  });

  /* Studio: Top-level tabs */
  document.querySelectorAll("[data-studio-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = btn.getAttribute("data-studio-tab");
      document.querySelectorAll("[data-studio-tab]").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      document.querySelectorAll("[data-studio-panel]").forEach((panel) => {
        panel.classList.toggle("active", panel.getAttribute("data-studio-panel") === tab);
      });
    });
  });

  /* Studio: Browser DAG Wave & Conflict Compiler */
  const presetsEl = document.getElementById("sw-studio-presets");
  const dagInput = document.getElementById("dagJsonInput");
  if (presetsEl && dagInput) {
    let presets = {};
    try {
      presets = JSON.parse(presetsEl.textContent || "{}");
    } catch (_err) {
      presets = {};
    }
    let activePreset = "refactor";

    const splitScope = (raw) => {
      const cleaned = String(raw || "").trim().replace(/\\/g, "/");
      const parts = cleaned.split("::");
      return { file: parts[0].replace(/^\.\//, "").replace(/\/+$/, ""), sym: (parts[1] || "*").trim() };
    };

    const globRegex = (pattern) => {
      const esc = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&");
      return new RegExp("^" + esc.replace(/\*\*\//g, "(?:.+/)?").replace(/\*\*/g, ".*").replace(/\*/g, "[^/]*") + "$");
    };

    const filesOverlap = (a, b) => {
      if (!a || !b) return false;
      if (a === b || a === "**" || b === "**") return true;
      if (globRegex(a).test(b) || globRegex(b).test(a)) return true;
      const dirA = a.split("/").slice(0, -1).join("/");
      const dirB = b.split("/").slice(0, -1).join("/");
      return Boolean(a.includes("*") && b.includes("*") && dirA === dirB);
    };

    const scopesConflict = (patA, patB) => {
      const a = splitScope(patA);
      const b = splitScope(patB);
      if (!filesOverlap(a.file, b.file)) return false;
      return a.sym === "*" || b.sym === "*" || a.sym === b;
    };

    const pairConflict = (ta, tb) => {
      for (const wa of ta.write_globs || []) {
        for (const wb of tb.write_globs || []) {
          if (scopesConflict(wa, wb)) return { type: "WRITE_WRITE", a: wa, b: wb };
        }
        for (const rb of tb.read_globs || []) {
          if (scopesConflict(wa, rb)) return { type: "READ_WRITE", a: wa, b: rb };
        }
      }
      for (const ra of ta.read_globs || []) {
        for (const wb of tb.write_globs || []) {
          if (scopesConflict(ra, wb)) return { type: "READ_WRITE", a: ra, b: wb };
        }
      }
      return null;
    };

    const compileWaves = () => {
      const listEl = document.getElementById("dagResultsList");
      const diagEl = document.getElementById("dagDiagNote");
      const totEl = document.getElementById("dagTotalTasks");
      const waveEl = document.getElementById("dagWaveCount");
      const splitEl = document.getElementById("dagSplitCount");
      const speedEl = document.getElementById("dagSpeedup");

      let tasks = [];
      try {
        tasks = JSON.parse(dagInput.value);
        if (!Array.isArray(tasks) || tasks.length === 0) throw new Error("Provide a non-empty JSON array of TaskSpec objects.");
      } catch (err) {
        if (diagEl) diagEl.textContent = "Invalid JSON: " + err.message;
        return;
      }

      const byId = new Map();
      tasks.forEach((t) => byId.set(t.task_id, t));
      const indeg = new Map();
      const children = new Map();
      const level = new Map();
      tasks.forEach((t) => {
        indeg.set(t.task_id, (t.depends_on || []).length);
        children.set(t.task_id, []);
        level.set(t.task_id, 0);
      });
      for (const t of tasks) {
        for (const dep of t.depends_on || []) {
          if (!byId.has(dep)) {
            if (diagEl) diagEl.textContent = `Missing dependency "${dep}" required by "${t.task_id}".`;
            return;
          }
          children.get(dep).push(t.task_id);
        }
      }

      const queue = tasks.filter((t) => indeg.get(t.task_id) === 0).map((t) => t.task_id);
      const sorted = [];
      while (queue.length > 0) {
        const cur = queue.shift();
        sorted.push(cur);
        for (const nxt of children.get(cur)) {
          level.set(nxt, Math.max(level.get(nxt), level.get(cur) + 1));
          indeg.set(nxt, indeg.get(nxt) - 1);
          if (indeg.get(nxt) === 0) queue.push(nxt);
        }
      }

      if (sorted.length !== tasks.length) {
        const cycIds = tasks.filter((t) => indeg.get(t.task_id) > 0).map((t) => t.task_id);
        if (totEl) totEl.textContent = String(tasks.length);
        if (waveEl) waveEl.textContent = "BLOCKED";
        if (splitEl) splitEl.textContent = "cycle";
        if (speedEl) speedEl.textContent = "0.00x";
        if (diagEl) diagEl.textContent = `CycleDetectedError: cyclic loop involving [${cycIds.join(" -> ")} -> ${cycIds[0]}]`;
        if (listEl) {
          listEl.innerHTML = `<div class="rw-item danger-border"><span>CycleDetectedError</span><p>Topological compilation halted before execution to prevent infinite deadlock.</p></div>`;
        }
        return;
      }

      const maxLevel = Math.max(...sorted.map((id) => level.get(id)));
      const waves = [];
      const conflicts = [];
      let totalSeqMs = 0;
      let criticalWaveMs = 0;

      for (let lv = 0; lv <= maxLevel; lv += 1) {
        const levelTasks = tasks.filter((t) => level.get(t.task_id) === lv);
        const subWaves = [];
        for (const t of levelTasks) {
          totalSeqMs += Number(t.estimated_ms || 100);
          let placed = false;
          for (const sw of subWaves) {
            let hit = null;
            for (const other of sw) {
              const edge = pairConflict(t, other);
              if (edge) {
                hit = edge;
                conflicts.push({ lv, a: other.task_id, b: t.task_id, edge });
              }
            }
            if (!hit) {
              sw.push(t);
              placed = true;
              break;
            }
          }
          if (!placed) subWaves.push([t]);
        }
        for (const sw of subWaves) {
          waves.push(sw);
          criticalWaveMs += Math.max(...sw.map((t) => Number(t.estimated_ms || 100)));
        }
      }

      const speedup = criticalWaveMs > 0 ? (totalSeqMs / criticalWaveMs).toFixed(2) + "x" : "1.00x";
      if (totEl) totEl.textContent = String(tasks.length);
      if (waveEl) waveEl.textContent = String(waves.length);
      if (splitEl) splitEl.textContent = String(conflicts.length);
      if (speedEl) speedEl.textContent = speedup;
      if (diagEl) {
        diagEl.textContent = conflicts.length > 0
          ? `Compiled ${tasks.length} tasks into ${waves.length} conflict-free waves (${conflicts.length} glob overlap${conflicts.length === 1 ? "" : "s"} auto-split).`
          : `Compiled ${tasks.length} tasks into ${waves.length} conflict-free waves with zero overlapping file writes.`;
      }

      if (listEl) {
        const waveRows = waves.map((sw, idx) =>
          `<div class="rw-item"><span>Wave ${String(idx + 1).padStart(2, "0")}</span><p>${sw.map((t) => `<code>${t.task_id}</code>`).join(", ")}</p></div>`
        );
        const conflictRows = conflicts.map((c) =>
          `<div class="rw-item warn-border"><span>Auto-Split [${c.edge.type}] Level ${c.lv}</span><p>${c.a} (${c.edge.a}) &lt;-&gt; ${c.b} (${c.edge.b})</p></div>`
        );
        listEl.innerHTML = waveRows.concat(conflictRows).join("");
      }
    };

    const loadPreset = (name) => {
      activePreset = name;
      dagInput.value = JSON.stringify(presets[name] || [], null, 2);
      document.querySelectorAll("[data-dag-preset]").forEach((b) => {
        b.classList.toggle("active", b.getAttribute("data-dag-preset") === name);
      });
      compileWaves();
    };

    document.querySelectorAll("[data-dag-preset]").forEach((btn) => {
      btn.addEventListener("click", () => loadPreset(btn.getAttribute("data-dag-preset")));
    });
    const compileBtn = document.getElementById("dagCompileBtn");
    const resetBtn = document.getElementById("dagResetBtn");
    if (compileBtn) compileBtn.addEventListener("click", compileWaves);
    if (resetBtn) resetBtn.addEventListener("click", () => loadPreset(activePreset));
    loadPreset("refactor");
  }

  /* Studio: Crash Recovery Calculator */
  const simNodesInput = document.getElementById("simNodesInput");
  const simTokensInput = document.getElementById("simTokensInput");
  const simCrashPctInput = document.getElementById("simCrashPctInput");
  const simWorkflowInput = document.getElementById("simWorkflowInput");
  const updateCrashSim = () => {
    if (!simNodesInput || !simTokensInput || !simCrashPctInput) return;
    const total = Math.max(4, Math.min(500, Number(simNodesInput.value) || 10));
    const perNode = Math.max(100, Number(simTokensInput.value) || 1000);
    const pct = Math.max(10, Math.min(95, Number(simCrashPctInput.value) || 80));
    const wf = (simWorkflowInput && simWorkflowInput.value.trim()) || "refactor-auth-v11";

    const completed = Math.max(1, Math.min(total - 1, Math.round((total * pct) / 100)));
    const savedTokens = completed * perNode;
    const replayMs = (completed * 0.00752).toFixed(3);
    const savingsPct = ((completed / total) * 100).toFixed(1);

    document.getElementById("simReplayed").textContent = String(completed);
    document.getElementById("simTokensSaved").textContent = savedTokens.toLocaleString("en-US");
    document.getElementById("simReplayMs").textContent = `${replayMs} ms`;
    document.getElementById("simSavingsPct").textContent = `${savingsPct}%`;

    const out = document.getElementById("simJournalOutput");
    if (out) {
      out.textContent = JSON.stringify({
        workflow_id: wf,
        run_1_status: "FAILED",
        completed_nodes: completed,
        remaining_nodes: total - completed,
        run_2_replayed_nodes: completed,
        replay_elapsed_ms: Number(replayMs),
        tokens_saved_by_replay: savedTokens,
        hash_chain_verified: true,
        genesis_hash: "00000000000000000000000000000000"
      }, null, 2);
    }
  };
  [simNodesInput, simTokensInput, simCrashPctInput, simWorkflowInput].forEach((el) => {
    if (el) el.addEventListener("input", updateCrashSim);
  });
  updateCrashSim();

  /* Studio: MCP & CI Snippet Generator */
  const mcpSelect = document.getElementById("mcpClientSelect");
  const mcpOut = document.getElementById("mcpSnippetOutput");
  const mcpTitle = document.getElementById("mcpSnippetTitle");
  const copyMcpBtn = document.getElementById("copyMcpSnippetBtn");
  const mcpSnippets = {
    "claude-desktop": {
      title: "Stdio JSON-RPC 2.0 Config (~/.config/claude_desktop_config.json)",
      code: `{
  "mcpServers": {
    "swarmweaver": {
      "command": "swarmweaver",
      "args": ["mcp"]
    }
  }
}`
    },
    "github-actions": {
      title: "GitHub Actions Wave Plan & CI Verification Step",
      code: `- name: Compile Agent DAG & Run Quality Gate
  run: |
    pip install -e .
    swarmweaver plan --json
    ruff check .
    pytest -v --cov=swarmweaver --cov-fail-under=90`
    },
    "http-server": {
      title: "Streamable HTTP/SSE MCP Server & REST API",
      code: `swarmweaver serve-http --host 127.0.0.1 --port 8000
# Routes: POST /mcp | GET /mcp (SSE) | POST /api/plan | POST /api/conflicts | GET /metrics`
    },
    "docker": {
      title: "Hardened Non-Root Container Build",
      code: `docker build -t swarmweaver:1.1.0 .
docker run --rm -p 8000:8000 swarmweaver:1.1.0`
    }
  };
  const renderMcp = () => {
    if (!mcpSelect || !mcpOut) return;
    const item = mcpSnippets[mcpSelect.value] || mcpSnippets["claude-desktop"];
    if (mcpTitle) mcpTitle.textContent = item.title;
    mcpOut.textContent = item.code;
  };
  if (mcpSelect) {
    mcpSelect.addEventListener("change", renderMcp);
    renderMcp();
  }
  if (copyMcpBtn && mcpOut) {
    copyMcpBtn.addEventListener("click", () => copyText(mcpOut.textContent));
  }

  /* Rules Filter */
  const rulesSearch = document.getElementById("rulesSearchInput");
  const rulesCat = document.getElementById("rulesCategorySelect");
  const rulesTable = document.getElementById("rulesTable");
  const filterRules = () => {
    if (!rulesTable) return;
    const q = (rulesSearch ? rulesSearch.value : "").trim().toLowerCase();
    const cat = rulesCat ? rulesCat.value : "all";
    rulesTable.querySelectorAll("tbody tr").forEach((row) => {
      const rowCat = row.getAttribute("data-cat") || "";
      const txt = row.textContent.toLowerCase();
      const show = (cat === "all" || rowCat === cat) && (!q || txt.includes(q));
      row.hidden = !show;
    });
  };
  if (rulesSearch) rulesSearch.addEventListener("input", filterRules);
  if (rulesCat) rulesCat.addEventListener("change", filterRules);

  /* Docs Navigation & OS Tabs */
  const osCodeEl = document.getElementById("docOsCode");
  const osCopyBtn = document.getElementById("docOsCopyBtn");
  const osPayloads = {
    mac: `<span class="syn-cmd">git clone</span> https://github.com/codesphere-u/swarmweaver.git\n<span class="syn-cmd">cd</span> swarmweaver &amp;&amp; <span class="syn-cmd">pip install</span> <span class="syn-arg">-e .</span>\n<span class="syn-cmd">swarmweaver</span> demo &amp;&amp; <span class="syn-cmd">swarmweaver</span> benchmark`,
    win: `<span class="syn-cmd">git clone</span> https://github.com/codesphere-u/swarmweaver.git\n<span class="syn-cmd">cd</span> swarmweaver; <span class="syn-cmd">pip install</span> <span class="syn-arg">-e .</span>\n<span class="syn-cmd">swarmweaver</span> demo`
  };
  document.querySelectorAll("[data-doc-os]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const os = btn.getAttribute("data-doc-os");
      document.querySelectorAll("[data-doc-os]").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      if (osCodeEl && osPayloads[os]) osCodeEl.innerHTML = osPayloads[os];
    });
  });
  if (osCopyBtn && osCodeEl) {
    osCopyBtn.addEventListener("click", () => copyText(osCodeEl.textContent));
  }

  const switchDocSection = (targetId) => {
    document.querySelectorAll("[data-doc-section]").forEach((sec) => {
      sec.hidden = sec.getAttribute("data-doc-section") !== targetId;
    });
    document.querySelectorAll("[data-doc-target]").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-doc-target") === targetId);
    });
    document.querySelectorAll("[data-doc-subrail]").forEach((sub) => {
      sub.classList.toggle("open", sub.getAttribute("data-doc-subrail") === targetId);
    });
    window.scrollTo({ top: 0, behavior: "smooth" });
  };
  document.querySelectorAll("[data-doc-target], [data-doc-jump]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.getAttribute("data-doc-target") || btn.getAttribute("data-doc-jump");
      if (target) switchDocSection(target);
    });
  });
})();

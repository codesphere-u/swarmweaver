# Getting Started with SwarmWeaver v1.1.0

Interactive Website: https://codesphere-u.github.io/swarmweaver/
Docs & Studio: https://codesphere-u.github.io/swarmweaver/docs.html

## 1. Install (Python 3.11+, 0 Core Dependencies)

```bash
git clone https://github.com/codesphere-u/swarmweaver.git
cd swarmweaver
pip install -e .
```

Optional FastAPI/HTTP portal and dev tools:

```bash
pip install -e ".[server,dev]"
```

## 2. Run Core CLI Commands

```bash
swarmweaver plan         # Compile the 10-agent demo DAG into 7 conflict-free waves
swarmweaver demo         # Inject a crash at node 8 and replay 8 nodes from SHA-256 journal
swarmweaver benchmark    # Run the 4-axis latency, race-guard, resume, and cycle suite
```

## 3. Embed in Python

```python
from swarmweaver import SwarmOrchestrator, TaskSpec

tasks = [
    TaskSpec("audit", "Scan AST", read_globs=["src/**/*.py"], output_contract=["inv:dict"]),
    TaskSpec("jwt", "Fix JWT", depends_on=["audit"], write_globs=["src/auth/jwt.py"], input_contract=["inv:dict"]),
    TaskSpec("rbac", "Add RBAC", depends_on=["audit"], read_globs=["src/auth/jwt.py"], write_globs=["src/auth/jwt.py"]),
]

report = SwarmOrchestrator(tasks, journal_path=".swarmweaver/checkpoint.jsonl").run()
print(report.total_waves, report.conflicts_auto_split, report.tokens_saved_by_replay)
```

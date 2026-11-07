# Contributing to SwarmWeaver

Thank you for contributing to **SwarmWeaver**!

## Development Setup

```bash
git clone https://github.com/codesphere-u/swarmweaver.git
cd swarmweaver
python3 -m pip install -e ".[dev]"
```

## Verification Checklist Before Submitting a PR

1. **Run the full test suite (65+ tests):**
   ```bash
   pytest -v
   ```
2. **Run the 10-agent crash-resume swarm demo:**
   ```bash
   swarmweaver demo
   ```
3. **Run the reproducible benchmark suite:**
   ```bash
   swarmweaver benchmark --json
   ```
4. **Maintain zero external runtime dependencies** in `pyproject.toml`.

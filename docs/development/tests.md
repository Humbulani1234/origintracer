# Running Tests

## Setup

```bash
pip install -e ".[dev]"
```

## Run all tests

```bash
pytest origintracer/tests/ -x -q
```

## Run a specific file

```bash
pytest origintracer/tests/test_core_causal.py -x -q
```

## Run tests matching a pattern

```bash
pytest origintracer/tests/ -k "test_n_plus_one" -v
```

---

# CI

## Pre-commit hooks (local)

Install once after cloning:

```bash
pip install pre-commit
pre-commit install
```

Every `git commit` automatically runs black, ruff, and pytest. If any check fails the commit is aborted.

Run manually:

```bash
pre-commit run --all-files
```

## GitHub Actions

CI runs on every push and pull request to `main`. Two jobs run in parallel:

- `lint` - black check + ruff on Python 3.12
- `test` - pytest on Python 3.11 and 3.12

Workflow file: `.github/workflows/ci.yml`

---

# Contributing

## Code style

```bash
black origintracer/ # format
ruff check --fix origintracer/ # lint
```

Line length: 65.

## Adding a new probe

1. Create `origintracer/probes/myservice_probe.py`
2. Subclass `BaseProbe` and implement `start()` and `stop()`
3. Register probe types with `ProbeTypes.register_many({...})`
4. Use `emit()` for per-request events, `emit_direct()` for lifecycle events
5. Add probe name to `config/defaults.yaml` under `builtin_probes`
6. Add `_add_structural_edges` case to `RuntimeGraph` if the probe creates infrastructure topology
7. Write a test in `origintracer/tests/test_probe_myservice.py`

## Adding a causal rule

1. Write a predicate `(graph, temporal) → (bool, dict)`
2. Wrap in `CausalRule`
3. Register via `build_default_registry()` or as a user rule file
4. Write a test with a mock graph that triggers the rule
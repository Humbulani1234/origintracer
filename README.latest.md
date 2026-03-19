# StackTracer

**Live causal graph for Python async services.**

StackTracer instruments your stack automatically — no decorators, no SDK calls in
your views. It builds a continuously-updated graph of how your services relate to
each other, detects causal patterns as they emerge, and lets you query the live
runtime from a REPL or the React UI.

---

## What it produces

```
gunicorn::master  ──spawned──►  gunicorn::UvicornWorker-24861
gunicorn::UvicornWorker-24861  ──handled──►  uvicorn::/tasks/report/1/
uvicorn::/tasks/report/1/  ──calls──►  django::/tasks/report/1/
django::/tasks/report/1/  ──calls──►  redis::GET
django::/tasks/report/1/  ──dispatched──►  celery::myapp.tasks.process_report

celery::MainProcess  ──spawned──►  celery::ForkPoolWorker
celery::ForkPoolWorker  ──ran──►  celery::myapp.tasks.process_report
```

The graph converges — 24 nodes after 100 requests or 100,000. Call counts and
average durations accumulate on existing nodes. The N+1 pattern shows up as a
query node with twice the call count of the view that drives it. Event loop
starvation shows up as a high `avg_duration_ns` on asyncio tick nodes.

---

## Architecture

```
Probes  ──NormalizedEvent──►  emit()  ──►  Engine
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                         RuntimeGraph   TemporalStore   EventLog
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
              PatternRegistry  SemanticLayer  Compactor
                    │
              LocalQueryServer  (Unix socket — REPL connects here)
```

Probes never import `Engine`. The engine never imports any probe. The only
interface between them is `emit()` and `NormalizedEvent`.

---

## Installation

```bash
pip install stacktracer

# or editable from source:
pip install -e /path/to/stack-tracer
```

---

## Config merge order

Three layers, last wins:

```
1. stacktracer/config/defaults.yaml   — package defaults, never edited
2. stacktracer.yaml                   — your app config, found from cwd upward
3. init() kwargs                      — highest priority
```

`defaults.yaml` owns the builtin probe list, default sample rate, compactor
settings, semantic labels, and normalisation rules. Your `stacktracer.yaml`
overrides only what you need.

---

## Quick start — Django + gunicorn

**settings.py** — middleware must be first:

```python
MIDDLEWARE = [
    "stacktracer.probes.django_probe.TracerMiddleware",  # MUST be first
    "django.middleware.security.SecurityMiddleware",
    ...
]
```

**apps.py** — one init() per process, after Django is fully loaded:

```python
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import stacktracer
        stacktracer.init(debug=True)
```

**stacktracer.yaml** — place in your project root:

```yaml
probes:
  - django
  - asyncio
  - gunicorn
  - uvicorn
  - nginx
  - redis
```

**Run:**

```bash
gunicorn -c gunicorn.conf.py config.asgi:application \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 127.0.0.1:8000 --workers 1
```

---

## Quick start — Celery

Celery forks independently from gunicorn. Each process group gets its own engine.

**config/celery.py** — no `init()` here:

```python
import os
from celery import Celery
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
```

The Celery worker calls `init()` inside `worker_process_init` — handled
automatically by `CeleryProbe`. Add `celery` to your probes list and it works.

**Run:**

```bash
DJANGO_SETTINGS_MODULE=config.settings \
STACKTRACER_CONFIG=/path/to/your/stacktracer.yaml \
celery -A config worker --loglevel=info --concurrency=1
```

Set `STACKTRACER_CONFIG` explicitly for Celery — the cwd search is not reliable
from within the Celery process.

---

## REPL

```bash
python -m stacktracer.scripts.repl
```

```
SHOW nodes
SHOW edges
SHOW events LIMIT 20
SHOW latency WHERE service = "django"
HOTSPOT TOP 10
TRACE <trace_id>
\stitch <trace_id>       ← merges timeline across all live process sockets
STATUS
CAUSAL
```

`\stitch` takes a **trace_id** from `SHOW events`, not a node name. It queries
all live Unix sockets and merges the event timeline across process boundaries
into one chronological view with proportional duration bars.

---

## Probe reference

| Probe | What it observes |
|---|---|
| `django` | Full HTTP lifecycle, URL resolution, view dispatch via `View.dispatch` patch |
| `asyncio` | Task creation, `__step` patching (3.11) / `create_task` (3.12+), loop ticks |
| `gunicorn` | Master process, worker fork, pre-fork event preservation |
| `uvicorn` | Request receive, response start/body/complete |
| `nginx` | HTTP requests via log tail, Lua UDP, or kprobe+Lua combined |
| `celery` | Main process, worker fork, task lifecycle (prerun/postrun/retry/failure) |
| `redis` | Every command via `TracedRedis` subclass — GET, SET, HGET, LPUSH, etc. |
| `db` | PostgreSQL queries via kprobe on `send()` syscall |

### Adding your own probe

Drop a `*_probe.py` file in `<your_app>/stacktracer/probes/`. It is discovered
automatically at `init()` time. Subclass `BaseProbe`, implement `start()` and
`stop()`, and call `emit()` from your hooks.

```python
from stacktracer.sdk.base_probe import BaseProbe
from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.context.vars import get_trace_id

class PaymentProbe(BaseProbe):
    name = "payment"

    def start(self):
        # install your hooks here
        pass

    def stop(self):
        pass
```

Add `payment` to your `stacktracer.yaml` probes list.

---

## Causal rules

| Rule | Detects | Confidence |
|---|---|---|
| `new_sync_call_after_deployment` | New call edges appearing after a deployment marker | 85% |
| `asyncio_loop_starvation` | asyncio tick nodes averaging > 10ms | 80% |
| `retry_amplification` | Task nodes with retry count > 3 | 75% |
| `db_query_hotspot` | Single query node > 30% of all DB calls on the trace | 70% |
| `n_plus_one` | Query call count ≥ 2× the view call count | 80% |
| `worker_imbalance` | One worker handling > 80% of requests while others sit idle | 65% |

Drop a `*_rules.py` file in `<your_app>/stacktracer/rules/` to add your own.
Expose a `register(registry)` function — it receives the live registry.

---

## Graph model

Nodes are `service::name` — e.g. `django::/api/users/`, `redis::GET`.
Call count and total duration accumulate on each node. Edges are directional
with a type: `calls`, `spawned`, `ran`, `handled`, `dispatched`.

The graph normaliser collapses high-cardinality patterns automatically:
`/api/users/123/` → `/api/users/{id}/`, UUID paths, SQL literals. Add your own
normalisation rules in `stacktracer.yaml`:

```yaml
normalize:
  - service: django
    pattern: "/api/v\\d+/"
    replacement: "/api/{version}/"
```

The compactor evicts low-value nodes when the graph exceeds `max_nodes` (default
5,000). At typical application cardinality the graph stabilises at under 100
nodes regardless of traffic volume.

---

## Pre-fork event pattern

Gunicorn, Celery, and nginx probes all follow the same pattern for preserving
topology events across fork boundaries:

1. `probe.start()` runs in the master process before `fork()`
2. Topology events are parked in a module-level `_pre_fork_events` list
3. `_register_post_init_callback(_drain_pre_fork_events)` queues a drain
4. After `fork()`, the worker calls `stacktracer.init()`
5. At the end of `init()`, all callbacks run — events drain into the worker's
   fresh engine

This ensures the worker's graph contains the full topology (master → worker
nodes and edges) even though `probe.start()` ran before the engine existed.

---

## Storage backends

| Backend | Use case |
|---|---|
| `InMemoryRepository` | Default — tests and local dev |
| `EventRepository` | PostgreSQL — production event persistence |
| `ClickHouseRepository` | Analytics, long-term history |

```python
from stacktracer.storage.repository import EventRepository
import psycopg2

conn = psycopg2.connect("postgresql://user:pass@host/db")
stacktracer.init(repository=EventRepository(conn))
```

---

## Overhead

| Component | Cost |
|---|---|
| ContextVar lookup | ~5 ns |
| `emit()` | ~1–2 µs |
| Graph upsert | ~2–5 µs |
| Temporal snapshot (diff only) | ~50 µs per interval |

At 1% sample rate, total overhead is well under 1% on typical Django workloads.
The graph stabilises under 100 nodes for a typical service — under 500 KB
including the temporal store across a full day of snapshots at 15-second intervals.

---

## React UI

A minimal terminal-aesthetic dashboard that mirrors the REPL. Polls the HTTP
bridge every 5 seconds. Views: nodes, edges, trace timeline, event log.

```bash
cd stacktracer-ui
npm install
npm run dev      # http://localhost:5173
```

Add the bridge to your stacktracer package (`stacktracer/bridge.py`) — a
20-line FastAPI app that wraps the Unix socket queries as HTTP endpoints.
See `stacktracer-ui/src/api/client.js` for the full bridge spec.

---

## Development
 
### Running tests
 
```bash
# from the repo root
pip install -e ".[dev]"
pytest stacktracer/tests/ -x -q
```
 
`-x` stops on the first failure. Remove it to run the full suite.
 
Run a specific test file:
 
```bash
pytest stacktracer/tests/test_core_causal.py -x -q
```
 
Run tests matching a name pattern:
 
```bash
pytest stacktracer/tests/ -k "test_n_plus_one" -v
```
 
### Code formatting
 
```bash
black stacktracer/          # format in place
black --check stacktracer/  # check only, no changes (what CI runs)
ruff check stacktracer/     # lint
ruff check --fix stacktracer/  # lint + auto-fix
```
 
### Pre-commit hooks (local CI)
 
Install once after cloning:
 
```bash
pip install pre-commit
pre-commit install
```
 
After that, every `git commit` automatically runs black, ruff, and pytest.
If any check fails the commit is aborted. Fix the issues and commit again.
 
Run hooks manually without committing:
 
```bash
pre-commit run --all-files
```
 
### GitHub Actions (remote CI)
 
CI runs automatically on every push and pull request to `main`.
Two jobs run in parallel:
 
- `lint` — black check + ruff on Python 3.12
- `test` — pytest on Python 3.11 and 3.12
 
The workflow file is at `.github/workflows/ci.yml`.
No secrets or configuration needed — it installs from `pyproject.toml [dev]`
and runs the same pytest command as local.
 
To see CI status locally before pushing:
 
```bash
pre-commit run --all-files   # same checks, no network needed
```
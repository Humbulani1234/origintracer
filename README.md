# StackTracer

**Live causal graph for Python async services.**

StackTracer instruments your production stack — nginx, gunicorn, uvicorn, Django, asyncio, Celery — to capture *why* execution flowed the way it did, not just that it was slow. It builds a live graph of your service's execution structure, detects causal patterns, and answers questions like `BLAME WHERE system = "export"` or `DIFF SINCE deployment` against the running process.

The engine is open source. Deeper knowledge — traced book chapters and the rule libraries that implement what each chapter explains — is sold separately at [stacktracer.io](https://stacktracer.io).

---

## How it works

Every probe in the system observes one framework or layer. When something happens — a Django view executes, an asyncio task steps, the kernel returns I/O events from epoll — the probe emits a `NormalizedEvent` through `emit()`. The engine receives it, updates the runtime graph, appends it to the event log, and checks it against the temporal store. Nothing else. Probes never touch the engine. The engine never touches probes.

```
nginx → gunicorn → uvicorn → Django / FastAPI
  │         │          │          │
  │    gunicorn     uvicorn   django        ← built-in probes
  │    _probe.py    _probe.py  _probe.py
  │         │          │          │
  │         └──────────┴──────────┘
  │                    │
  │              emit(NormalizedEvent)
  │                    │
  ▼                    ▼
kernel_probe.py     Engine
asyncio_probe.py      ├── RuntimeGraph      ← live call graph
                      ├── GraphNormalizer   ← collapses high-cardinality names
                      ├── GraphCompactor    ← bounds memory via LRU + TTL
                      ├── TemporalStore     ← diff-based change log
                      ├── PatternRegistry   ← causal rules
                      └── SemanticLayer     ← label → node mapping

                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
          DSL Query     Storage      Backend
          Parser        (PG/CH)      (FastAPI)
```

**The invariant that makes this extensible:** probes only call `emit()`. The engine only receives `NormalizedEvent`. Neither side knows about the other's internals. Swap the engine, mock it for tests, or run it remotely — probes are unaffected.


---

## Installation

```bash
pip install stacktracer

# or editable from source:
pip install -e /path/to/stack-tracer
```

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

ll five observe real production stacks. Add them to `stacktracer.yaml`.

### nginx

Two modes, auto-selected at startup:

- **eBPF mode** (Linux + root): uprobes on six nginx C functions inside `/usr/sbin/nginx`. Fires on `ngx_http_wait_request_handler`, `ngx_http_init_request`, `ngx_http_handler`, `ngx_recv`, `ngx_add_event`, `ngx_epoll_process_events`. Sub-millisecond visibility into nginx's internal lifecycle before a request reaches gunicorn.
- **Log-tail mode** (all platforms): tails the nginx JSON access log. Less granular but zero privilege requirement.

```yaml
probes:
  - nginx   # auto-selects eBPF if root, log-tail otherwise
```

ProbeTypes: `nginx.connection.accept`, `nginx.request.parse`, `nginx.request.route`, `nginx.recv`, `nginx.upstream.dispatch`, `nginx.epoll.tick`

### gunicorn

Patches `Arbiter.spawn_worker`, `Arbiter._kill_worker`, `Worker.init_process`, `Worker.notify`, and `SyncWorker.handle_request`. Observes worker process lifecycle in the master process and per-request handling in sync workers. For `UvicornWorker`, request handling is covered by the uvicorn probe.

ProbeTypes: `gunicorn.worker.spawn`, `gunicorn.worker.init`, `gunicorn.worker.exit`, `gunicorn.request.handle`, `gunicorn.worker.heartbeat`

### uvicorn

Patches `run_asgi()` on both `H11Protocol` and `HttpToolsProtocol` — the two HTTP/1.1 backends uvicorn ships with. Captures the full ASGI lifecycle from parsed request to response sent. Reads `X-Request-ID` forwarded by nginx so nginx and uvicorn events share the same trace ID automatically.

Add to nginx config to enable cross-layer trace correlation:
```nginx
proxy_set_header X-Request-ID $request_id;
```

ProbeTypes: `uvicorn.request.receive`, `uvicorn.response.send`, `uvicorn.h11.cycle`, `uvicorn.httptools.cycle`

### django

`TracerMiddleware` wraps the full request lifecycle. Hooks into URL resolution and view dispatch via Django's internal signals. Works with both sync and async views.

ProbeTypes: `django.middleware.enter`, `django.middleware.exit`, `django.url.resolve`, `django.view.enter`, `django.view.exit`

### asyncio

Four layers applied in combination by Python version:

| Layer | Mechanism | Python | What it observes |
|---|---|---|---|
| 1 | `BaseEventLoop._run_once()` patch | All versions | `select()`/`epoll_wait()` duration, events returned by kernel, ready queue depth before and after |
| 2 | `Task.__step` patch | 3.11 only | Per-coroutine step with coro name, `fut_waiter` state |
| 3 | eBPF uprobe on `_asyncio.cpython-3XX.so` | 3.12+ Linux root | Per-step timing at C level |
| 4 | `asyncio.create_task()` wrap | All versions | Task creation with coro name |

Layer 1 is the most important. `_run_once()` is pure Python in all versions — only `Task` moved to C in 3.12. Layer 1 captures the exact line where the kernel returns I/O events:

```python
event_list = self._selector.select(timeout)  # ← we intercept here
```

The `asyncio.loop.select` event tells you how long `epoll_wait()` blocked and what it returned. The `asyncio.loop.run_once` event gives you the full tick breakdown: select time, process_events time, ready queue depth. These two events make event loop starvation visible without requiring any task-level patching.

ProbeTypes: `asyncio.loop.select`, `asyncio.loop.run_once`, `asyncio.loop.tick`, `asyncio.task.create`


### Adding your own probe

## Extending with custom probes and rules

StackTracer auto-discovers probes and rules from your project directory. Create this layout in your project:

```
myapp/
├── stacktracer.yaml
└── stacktracer/
    ├── probes/
    │   ├── celery_types.py     ← register ProbeType constants here
    │   └── celery_probe.py     ← auto-discovered (*_probe.py)
    └── rules/
        └── celery_rules.py     ← auto-discovered (*_rules.py)
```

No YAML entry required for files in these directories following the naming convention.

### Writing a probe

```python
# stacktracer/probes/celery_probe.py
from stacktracer.sdk.base_probe import BaseProbe
from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import NormalizedEvent, ProbeTypes
from stacktracer.context.vars import get_trace_id

# Register your probe types — happens at module import time
TASK_START = ProbeTypes.register("celery.task.start", "Celery task started")
TASK_END   = ProbeTypes.register("celery.task.end",   "Celery task completed")

class CeleryProbe(BaseProbe):
    name = "celery"

    def start(self) -> None:
        from celery.signals import task_prerun, task_postrun
        task_prerun.connect(self._on_start, weak=False)
        task_postrun.connect(self._on_end,  weak=False)

    def stop(self) -> None:
        from celery.signals import task_prerun, task_postrun
        task_prerun.disconnect(self._on_start)
        task_postrun.disconnect(self._on_end)

    def _on_start(self, task_id, task, **kw):
        emit(NormalizedEvent.now(
            probe=TASK_START,
            trace_id=get_trace_id() or task_id,
            service="celery",
            name=task.name,
            task_id=task_id,
        ))

    def _on_end(self, task_id, task, **kw):
        emit(NormalizedEvent.now(
            probe=TASK_END,
            trace_id=get_trace_id() or task_id,
            service="celery",
            name=task.name,
            task_id=task_id,
        ))
```

Three rules: always call the original (probes never prevent execution), only emit real observations (not in `start()`), keep `start()` fast.

### Writing a causal rule

```python
# stacktracer/rules/celery_rules.py
from stacktracer.core.causal import CausalRule, PatternRegistry

def register(registry: PatternRegistry) -> None:
    registry.register(CausalRule(
        name="celery_sync_db_call",
        description="Celery task making synchronous database calls",
        tags=["celery", "blocking"],
        predicate=_sync_db_in_celery,
        confidence=0.85,
    ))

def _sync_db_in_celery(graph, temporal):
    evidence = []
    for node in graph.all_nodes():
        if node.service != "celery":
            continue
        for edge in graph.neighbors(node.id):
            target = graph.get_node(edge.target)
            if target and target.service in ("postgres", "sqlite") \
               and (target.avg_duration_ns or 0) > 50_000_000:
                evidence.append({
                    "task":   node.id,
                    "db":     target.id,
                    "avg_ms": round((target.avg_duration_ns or 0) / 1e6, 1),
                })
    return bool(evidence), {"blocking_db_calls": evidence}
```

### Registering custom ProbeTypes

Add new probe type strings without touching the core:

```python
from stacktracer.core.event_schema import ProbeTypes

# In your probe file — registration at import time
MY_EVENT = ProbeTypes.register("myapp.thing.start", "Thing started")

# Or in bulk
ProbeTypes.register_many({
    "myapp.export.start": "Export job started",
    "myapp.export.end":   "Export job completed",
})

# Or in stacktracer.yaml
```

```yaml
probe_types:
  - name: myapp.export.start
    description: Export job started
```

Unknown probe type strings are warned in debug logs but never rejected. The registry is for tooling visibility (`\probes` in the REPL), not enforcement.

---


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

## Memory management

### GraphNormalizer

High-cardinality node names cause the graph to grow without bound. `/api/users/1234/profile` and `/api/users/5678/profile` are structurally the same endpoint — they should be one node, not ten thousand.

`GraphNormalizer` collapses names before graph insertion using built-in patterns (UUIDs, numeric URL segments, memory addresses, SQL literal values) plus user-defined rules:

```python
# Automatically applied — no config needed
/api/users/1234/profile     →   /api/users/{id}/profile
550e8400-e29b-41d4-a716-... →   {uuid}
SELECT * FROM t WHERE id=1  →   SELECT * FROM t WHERE id=?
coro at 0x7f3a2b4c1d0       →   coro
```

Add your own patterns in `stacktracer.yaml`:

```yaml
normalize:
  max_unique_names_per_service: 500
  rules:
    - service: django
      pattern: "/api/items/(\\d+)/reviews/(\\d+)/"
      replacement: "/api/items/{id}/reviews/{review_id}/"
```

### GraphCompactor

Evicts cold nodes when the graph exceeds configurable limits. Runs in the background snapshot loop — never on the hot path.

Two eviction passes per cycle:

1. **TTL eviction** — nodes not seen in the last hour are candidates for removal
2. **Cap eviction** — if node count still exceeds `max_nodes`, evict coldest (LRU) nodes until at `evict_to` count

Hot nodes (high `call_count`) are protected from both passes. Evicting a node removes all incident edges atomically.

Memory estimate: 5,000 nodes + 15,000 edges ≈ 5 MB. Without the compactor, a Django app with UUID-keyed endpoints can reach hundreds of thousands of nodes in a day.

### Graph serialization

Save and restore the full graph:

```python
from stacktracer.core.graph_serializer import MsgpackSerializer, ProtobufSerializer

# MessagePack — simple, no compile step, 3× smaller than JSON
s = MsgpackSerializer()
s.save(graph, "graph.msgpack")
graph = s.load("graph.msgpack")

# Protobuf — smallest, strongly typed, best for network transport
# Requires: pip install grpcio-tools && python -m grpc_tools.protoc ...
s = ProtobufSerializer()
data = s.serialize(graph)   # bytes, ~900KB for 5000-node graph
```

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

---

## Design decisions

**Why probes never import Engine.** The engine can be swapped, mocked, or run remotely without any probe changes. Probes are pure observation; the engine is pure reasoning. The boundary is `emit()`.

**Why the graph needs a normalizer.** Without normalization, every UUID-keyed URL produces a unique node. `/api/users/1234/` and `/api/users/5678/` are the same structural position in the call graph. The normalizer collapses them before graph insertion. This is not optional — without it the graph grows without bound on any REST API with resource IDs in URLs.

**Why diffs, not full snapshots.** Full graph deep copies at 15-second intervals OOM in production. Storing only what changed is both cheaper and more useful — you can reconstruct history from diffs, and `DIFF SINCE deployment` becomes a single set operation.

**Why the causal registry is rule-based, not ML.** Rules are honest about what they know and explainable at 3am. ML models trained on labelled incidents come later. Rules give you `new_sync_call_after_deployment` insight on day one without training data.

**Why ContextVar for trace propagation.** asyncio coroutines share a thread. ContextVar is the only mechanism that correctly isolates per-coroutine state across `await` boundaries. `threading.local()` would conflate every coroutine on the same thread into one trace.

**Why _run_once() gives full loop visibility on 3.12+.** CPython 3.12 moved `Task` to a C extension but left `BaseEventLoop._run_once()` in pure Python. The selector interaction — where `epoll_wait()` returns I/O events from the kernel — lives in `_run_once()`. Patching it gives complete loop-level visibility on all versions. Task-level visibility on 3.12+ requires the eBPF uprobe on `_asyncio.so`.

**Why user config lives outside the package.** Editing files inside `site-packages/stacktracer/` breaks on every `pip install --upgrade`. The user's `stacktracer.yaml` lives in their repo, survives upgrades, and is auto-discovered from the working directory. Package defaults merge underneath — user settings win.

**Why ProbeType is a registry, not a Literal.** A closed `Literal` requires editing the core to add a new probe type. An open registry accepts contributions from probe files, rules files, and YAML without touching core. Unknown strings are warned, never rejected. The registry is for tooling, not enforcement.

**Why nine daemon threads.** Each thread owns exactly one blocking concern. None of them touch the application thread after `init()` returns.

| Thread name | Owned by | What it does | Wakes every |
|---|---|---|---|
| `stacktracer-drain` | `sdk/emitter.py` `_DrainThread` | Drains `EventBuffer` into `Engine.process()` — builds the graph and event log off the application thread | 50 ms |
| `stacktracer-snapshot` | `core/engine.py` | Calls `engine.snapshot()` — diffs the graph against the previous snapshot and appends to `TemporalStore` | 15 s (configurable) |
| `stacktracer-uploader` | `buffer/uploader.py` | Batches raw events and POSTs to FastAPI backend. Only started when `api_key` is set | 10 s (configurable) |
| `stacktracer-active-req-evict` | `core/active_requests.py` | TTL-evicts in-flight request entries that exceed 30 s — safety valve against leaked trace IDs from crashed workers | 5 s |
| `stacktracer-nginx-kprobe` | `probes/nginx_probe.py` | Polls the BPF perf buffer for `accept4`, `epoll_wait`, `sendmsg`, `recvmsg` kprobe events from the nginx kernel path | Blocking poll |
| `stacktracer-nginx-lua-udp` | `probes/nginx_probe.py` | UDP server blocking on port 9119 — receives JSON events from `log_by_lua_block` enriched with HTTP semantics | Blocking recv |
| `stacktracer-nginx-correlator-evict` | `probes/nginx_probe.py` `_NginxCorrelator` | Evicts stale `(client_ip, client_port)` correlation entries for connections that closed without a Lua event | 30 s |
| `stacktracer-asyncio-epoll` | `probes/asyncio_probe.py` | Polls the BPF perf buffer for `sys_epoll_wait` events — classifies ready file descriptors by destination port (postgres/redis/tcp/pipe) | Blocking poll |
| `stacktracer-local-server` | `core/local_server.py` | Unix socket server at `/tmp/stacktracer.sock` — accepts DSL queries from the REPL or CLI connecting to a running agent | Blocking accept |

All nine are `daemon=True`. The OS reaps them with the process — no `join()` calls required at shutdown except the uploader, which calls `_flush()` then `join(timeout=5)` from `stacktracer.shutdown()` to guarantee the final batch is sent before the process exits. The application thread's only cost after `init()` is `EventBuffer.push()` — one lock acquisition and one `deque.append()`, which is the 0.5 µs figure in the overhead table above.

---

## OTEL compatibility

Every `NormalizedEvent` carries `span_id` and `parent_span_id` in standard 16-char hex format. Events are first-class citizens in any OpenTelemetry-compatible system. Correlate via `trace_id` (W3C `traceparent` compatible). StackTracer operates at the Python runtime layer — one zoom level below distributed tracing systems like Jaeger or Tempo, which operate at the service boundary layer.
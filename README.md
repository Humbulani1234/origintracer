# OriginTracer

**Live causal graph for async services.**

OriginTracer instruments your production stack - nginx, gunicorn, uvicorn, Django, asyncio, Celery - to capture *why* execution flowed the way it did, not just that it was slow. It builds a live graph of your service's execution structure, detects causal patterns, and answers questions like `BLAME WHERE system = "export"` or `DIFF SINCE deployment` against the running process.

**What you get:**

- Debugging real execution paths  
- Understanding frameworks deeply  
- Teaching how systems actually work  
- Visualizing control flow  
- Developer introspection  

The engine is open source. Deeper knowledge - traced book chapters and the rule libraries that implement what each chapter explains - is available on [origintracer.app](https://origintracer.app).

---

## How it works

Every probe in the system observes one framework or layer. When something happens - a Django view executes, an asyncio task steps, the kernel returns I/O events from epoll - the probe emits a `NormalizedEvent` through `emit()`. The engine receives it, updates the runtime graph, appends it to the event log, and checks it against the temporal store. Nothing else. Probes never touch the engine, and the engine never touches probes - that's the decoupling between the
user application and engine.

---

## Installation

```bash
git clone git@github.com:Humbulani1234/origintracer.git
cd origintracer
pip install -e .
```

---

## Quick start - Django application

**settings.py**

```python
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware", # MUST be first
    "django.middleware.security.SecurityMiddleware",
    ...
]
```

**apps.py**

```python
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import origintracer
        origintracer.init(debug=True)
```

**origintracer.yaml** - place in your project root:

```yaml
probes:
  - nginx
  - gunicorn
  - uvicorn
  - django
```

**Run:**

```bash
gunicorn -c gunicorn.conf.py config.asgi:application \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 127.0.0.1:8000 --workers 1
```

---

## Quick start - Celery application

Celery forks independently from gunicorn. Each process group gets its own engine.

**config/celery.py**

```python
import os
from celery import Celery
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
```

The Celery worker calls `init()` inside `worker_process_init` - handled
automatically by `CeleryProbe`. Add `celery` to your probes list and it works.

**Run:**

```bash
DJANGO_SETTINGS_MODULE=config.settings \
ORIGINTRACER_CONFIG=/path/to/your/origintracer.yaml \
celery -A config worker --loglevel=info --concurrency=1
```
---

## REPL

```bash
python -m origintracer.repl.repl
```

```
SHOW nodes
SHOW edges
SHOW events LIMIT 20
SHOW latency WHERE service = "django"
HOTSPOT TOP 10
TRACE <trace_id>
\stitch <trace_id>  <-- merges timeline across all live process sockets
STATUS
CAUSAL
```

`\stitch` takes a **trace_id** from `SHOW events`, not a node name. It queries
all live Unix sockets and merges the event timeline across process boundaries
into one chronological view with proportional duration bars.

---

## Windows Compatibility (WSL2)

For Windows users, **WSL2 (Windows Subsystem for Linux)** is a strict requirement for the following features:

* **Unix Domain Sockets:** The Local Query Server (`/tmp/origintracer-*.sock`) used by the REPL for live inspection.
* **Nginx kprobes:** Kernel-level tracing and log-tailing triggers.
* **Performance Stability:** High-concurrency benchmarking via Gunicorn/Uvicorn.

**Quick Start on WSL2**

1. Ensure you are running **WSL2**.
2. Install dependencies within the Linux terminal:
   ```bash
    git clone
    cd origintracer
    pip install -e .
---

## Probe reference

These probes observe real production stacks. Add them to `origintracer.yaml`.

### nginx - available on [origintracer.app](https://origintracer.app)

Two modes, auto-selected at startup:

- **eBPF mode** (Linux + root): uprobes on six nginx C functions inside `/usr/sbin/nginx`. Fires on `ngx_http_wait_request_handler`, `ngx_http_init_request`, `ngx_http_handler`, `ngx_recv`, `ngx_add_event`, `ngx_epoll_process_events`. Sub-millisecond visibility into nginx's internal lifecycle before a request reaches gunicorn.
- **Log-tail mode** (all platforms): tails the nginx JSON access log. Less granular but zero privilege requirement.

```yaml
probes:
  - nginx # auto-selects eBPF if root, log-tail otherwise
```

ProbeTypes: `nginx.connection.accept`, `nginx.request.parse`, `nginx.request.route`, `nginx.recv`, `nginx.upstream.dispatch`, `nginx.epoll.tick`

### gunicorn - available on [origintracer.app](https://origintracer.app)

Patches `Arbiter.spawn_worker`, `Arbiter._kill_worker`, `Worker.init_process`, `Worker.notify`, and `SyncWorker.handle_request`. Observes worker process lifecycle in the master process and per-request handling in sync workers. For `UvicornWorker`, request handling is covered by the uvicorn probe.

ProbeTypes: `gunicorn.worker.spawn`, `gunicorn.worker.init`, `gunicorn.worker.exit`, `gunicorn.request.handle`, `gunicorn.worker.heartbeat`

### uvicorn - available on [origintracer.app](https://origintracer.app)

Patches `run_asgi()` on both `H11Protocol` and `HttpToolsProtocol` - the two HTTP/1.1 backends uvicorn ships with. Captures the full ASGI lifecycle from parsed request to response sent. Reads `X-Request-ID` forwarded by nginx so nginx and uvicorn events share the same trace ID automatically.

Add to nginx config to enable cross-layer trace correlation:
```nginx
proxy_set_header X-Request-ID $request_id;
```

ProbeTypes: `uvicorn.request.receive`, `uvicorn.response.send`, `uvicorn.h11.cycle`, `uvicorn.httptools.cycle`

### django - builtin

`TracerMiddleware` wraps the full request lifecycle. Hooks into URL resolution and view dispatch via Django's internal signals. Works with both sync and async views.

ProbeTypes: `django.middleware.enter`, `django.middleware.exit`, `django.url.resolve`, `django.view.enter`, `django.view.exit`

### asyncio - builtin

Layers applied in combination by Python version:

Layer 1: `_run_once()` instrumentation.

```python
event_list = self._selector.select(timeout)  # we intercept here
```

The `asyncio.loop.select` event tells you how long `epoll_wait()` blocked and what it returned. The `asyncio.loop.run_once` event gives you the full tick breakdown: select time, process_events time, ready queue depth. These two events make event loop starvation visible without requiring any task-level patching.

ProbeTypes: `asyncio.loop.select`, `asyncio.loop.run_once`, `asyncio.loop.tick`, `asyncio.task.create`

---

## Extending with custom probes and rules

OriginTracer auto-discovers probes and rules from your project directory. Create this layout in your project:

```
myapp/
├── origintracer.yaml
└── origintracer/
    ├── probes/
    │   └── celery_probe.py  << auto-discovered (*_probe.py)
    └── rules/
        └── celery_rules.py  << auto-discovered (*_rules.py)
```

### Writing a probe

```python
# origintracer/probes/celery_probe.py
from origintracer.sdk.base_probe import BaseProbe
from origintracer.sdk.emitter import emit
from origintracer.core.event_schema import NormalizedEvent, ProbeTypes
from origintracer.context.vars import get_trace_id

# Register your probe types - happens at module import time
ProbeTypes.register("celery.task.start", "Celery task started")
ProbeTypes.register("celery.task.end", "Celery task completed")

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
# origintracer/rules/celery_rules.py
from origintracer.core.causal import CausalRule, PatternRegistry

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
from origintracer.core.event_schema import ProbeTypes

# In your probe file - registration at import time
ProbeTypes.register("myapp.thing.start", "Thing started")

# Or in bulk
ProbeTypes.register_many({
    "myapp.export.start": "Export job started",
    "myapp.export.end": "Export job completed",
})
```
---

## Causal rules

| Rule | Detects |
|---|---|
| `new_sync_call_after_deployment` | New call edges appearing after a deployment marker | 
| `asyncio_loop_starvation` on [origintracer.app](https://originracer.app) | asyncio tick nodes averaging > 10ms |
| `retry_amplification` | Task nodes with retry count > 3 |
| `db_query_hotspot` | Single query node > 30% of all DB calls on the trace |
| `n_plus_one` | Query call count ≥ 2× the view call count |
| `worker_imbalance` | One worker handling > 80% of requests while others sit idle |

Drop a `*_rules.py` file in `<your_app>/origintracer/rules/` to add your own.
Expose a `register(registry)` function - it receives the live registry.

---

## React UI

A minimal terminal-aesthetic dashboard that mirrors the REPL. Polls the HTTP
bridge. Views: nodes, edges, trace timeline, event log.

Ensure the `FastAPI` backend is running.

```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8001
```

Send heavy requests to OriginTracer - i.e. using a django application.

```bash
python /path/to/applications/django/burst_test_benchmarked.py
```

Run the `React` application.

```bash
cd frontend
npm install
npm run dev # http://localhost:5173
```
---

## Development
 
### Running tests
 
```bash
# from the repo root
pip install -e ".[dev]"
pytest origintracer/tests/ -x -q
```
 
Run a specific test file:
 
```bash
pytest origintracer/tests/test_core_causal.py -x -q
```
 
Run tests matching a name pattern:
 
```bash
pytest origintracer/tests/ -k "test_n_plus_one" -v
```
 
### Pre-commit hooks (local CI)
 
Install once after cloning:
 
```bash
pip install pre-commit
pre-commit install
```
 
---

## OTel Bridge Mode - still in development

OriginTracer can run in OpenTelemetry bridge mode instead of native probe mode.
In this mode OTel is the event source - OriginTracer's own probes are disabled
and the engine receives events translated from OTel spans instead.

**When to use OTel mode:**
- Your team already has OTel instrumentation deployed
- You want causal rules and the graph without adding a second instrumentation layer
- You need distributed tracing across polyglot services (Go, Java, Node)

**When to use native probe mode (default):**
- You want asyncio internals — `Task.__step`, loop tick, ready queue depth
- You want kernel-level timing via kprobe (accept4, epoll_wait)
- You want gunicorn worker lifecycle events and nginx correlation

Both modes feed the same engine, same REPL, same React UI. Only the event
source changes.

### Install OTel SDK

```bash
pip install opentelemetry-sdk \
            opentelemetry-instrumentation-django \
            opentelemetry-instrumentation-psycopg2 \
            opentelemetry-instrumentation-redis
```

### Enable OTel mode

Set the flag in `settings.py`:

```python
ORIGINTRACER_OTEL_MODE = True  # False = native probes (default)
```

`apps.py` reads this flag automatically and switches between native and OTel
initialisation. No other code changes needed.

### What `apps.py` does in each mode

```
ORIGINTRACER_OTEL_MODE = False (default)
    >> stacktracer.init() starts engine + native probes
    >> TracerMiddleware sets trace_id per request
    >> django/asyncio/gunicorn probes emit NormalizedEvents directly

ORIGINTRACER_OTEL_MODE = True
    >> stacktracer.init(otel_mode=True) starts engine only, no probes
    >> DjangoInstrumentor OTel instruments Django automatically
    >> Psycopg2Instrumentor OTel instruments DB queries
    >> BatchSpanProcessor batches completed spans
    >> StackTracerSpanExporter converts OTel spans --> NormalizedEvents
    >> engine.process(event) ame graph, same causal rules
```

---

## Performance Profile - script in django app

Recent architectural benchmarks on a 4-core environment demonstrate the efficiency of the OriginTracer kernel:

* **Ultra-Low Latency:** Mean overhead of **~22ms** per request during high-concurrency bursts (175+ req/s).
* **Asynchronous Draining:** Background threads ensure the tracing engine never blocks the Django request/response cycle.
* **Zero-Drop Reliability:** Proven "Fire and Forget" buffer design that protects application stability during traffic spikes.
* **Deduplication Engine:** Intelligent graph merging ensures that repeated execution paths do not bloat memory.

---
# StackTracer — Django Application

Traces the full request path: nginx → gunicorn → uvicorn → Django → Redis.
StackTracer instruments automatically via probes — no decorators, no SDK calls in your views.

---

## Directory layout

```
applications/django/
├── config/
│   ├── __init__.py
│   ├── settings.py
│   ├── asgi.py
│   └── urls.py
├── worker/                  ← Django app
│   ├── __init__.py          ← empty
│   ├── apps.py              ← AppConfig.ready() — single init() point
│   ├── views.py
│   ├── models.py
│   └── urls.py
├── probes/                  ← user extension probes
│   ├── __init__.py
│   └── redis_probe.py       ← TracedRedis (subclass, not composition)
├── stacktracer.yaml         ← user config — overrides defaults.yaml
├── gunicorn.conf.py
└── manage.py
```

---

## Prerequisites

```bash
pip install stacktracer gunicorn uvicorn django redis
```

StackTracer installed as editable from the repo root:

```bash
pip install -e /path/to/stack-tracer
```

---

## settings.py — critical requirement

`TracerMiddleware` **must be the first entry** in `MIDDLEWARE`. Without this,
`get_trace_id()` returns `None` in all probes — events are silently dropped.

```python
MIDDLEWARE = [
    "stacktracer.probes.django_probe.TracerMiddleware",  # MUST be first
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    ...
]
```

---

## apps.py — one init() per process

```python
# worker/apps.py
from django.apps import AppConfig

class WorkerConfig(AppConfig):
    name = "worker"

    def ready(self):
        import stacktracer
        stacktracer.init(debug=True)
```

`AppConfig.ready()` runs once per process after Django is fully loaded.
This is the only place `stacktracer.init()` is called for the gunicorn worker.
Never call `init()` at module level or in `settings.py`.

---

## stacktracer.yaml

```yaml
probes:
  - django
  - asyncio
  - gunicorn
  - uvicorn
  - redis

observe:
  modules:
    - worker.views
```

---

## TracedRedis — subclass pattern

`redis_probe.py` in the app's `probes/` directory uses subclassing, not composition.
Composition breaks because `__getattr__` returns bound methods of the inner object,
so `r.get()` never reaches the wrapper's `execute_command()`.

```python
# probes/redis_probe.py
import redis
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.context.vars import get_trace_id
from stacktracer.sdk.emitter import emit
import time

class TracedRedis(redis.Redis):
    def execute_command(self, *args, **options):
        trace_id = get_trace_id()
        command  = args[0] if args else "UNKNOWN"
        key      = str(args[1])[:100] if len(args) > 1 else ""
        t0 = time.perf_counter()
        try:
            result = super().execute_command(*args, **options)
        except Exception as exc:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe="redis.command.execute", trace_id=trace_id,
                    service="redis", name=command,
                    duration_ns=duration_ns, key=key,
                    error=str(exc)[:200], success=False,
                ))
            raise
        else:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe="redis.command.execute", trace_id=trace_id,
                    service="redis", name=command,
                    duration_ns=duration_ns, key=key,
                    success=True, result_type=type(result).__name__,
                ))
            return result

# use in views.py:
# from probes.redis_probe import TracedRedis   ← absolute import
# r = TracedRedis(host="localhost", port=6379, db=0)
```

Import as an **absolute** import in views — relative imports (`from ..probes`) fail
because gunicorn adds `applications/django/` to `sys.path`, making `worker` and
`probes` top-level packages with no shared parent.

---

## nginx setup

nginx sits in front of gunicorn and is observed via log tail (default), Lua UDP,
or kprobe — configured in `stacktracer.yaml` under the `nginx` key.

### Log tail (zero-privilege fallback)

The simplest mode. nginx writes a JSON access log. StackTracer tails it.

**nginx.conf** — add a JSON log format and point the access log to it:

```nginx
http {
    log_format stacktracer escape=json
        '{"remote_addr":"$remote_addr",'
        '"request_time":$request_time,'
        '"upstream_response_time":"$upstream_response_time",'
        '"request":"$request",'
        '"status":$status,'
        '"body_bytes_sent":$body_bytes_sent,'
        '"request_id":"$http_x_request_id"}';

    server {
        listen 80;

        access_log /var/log/nginx/access.log stacktracer;

        location / {
            proxy_pass http://127.0.0.1:8000;
            proxy_set_header X-Request-Id $request_id;
        }
    }
}
```

**stacktracer.yaml** — configure the log path and mode:

```yaml
nginx:
  mode: log
  log_path: /var/log/nginx/access.log
```

On Windows/WSL2 where nginx writes to a custom path:

```yaml
nginx:
  mode: log
  log_path: /mnt/c/Users/<you>/nginx-1.24.0/logs/access.log
```

### Lua UDP mode (full HTTP semantics)

Requires `ngx_lua` / OpenResty. nginx sends a UDP packet per request to
StackTracer's receiver on port 9119. Gives URI, method, status, and upstream
timing without log file parsing.

```nginx
log_by_lua_block {
    local cjson  = require "cjson"
    local socket = ngx.socket.udp()
    socket:setpeername("127.0.0.1", 9119)
    socket:send(cjson.encode({
        uri           = ngx.var.uri,
        method        = ngx.req.get_method(),
        status        = ngx.status,
        duration_ms   = ngx.now() * 1000 - ngx.req.start_time() * 1000,
        upstream_ms   = tonumber(ngx.var.upstream_response_time) or -1,
        remote_addr   = ngx.var.remote_addr,
        remote_port   = ngx.var.remote_port,
        request_id    = ngx.var.request_id,
    }))
    socket:close()
}
```

```yaml
nginx:
  mode: lua
  lua_host: "127.0.0.1"
  lua_port: 9119
```

### kprobe + Lua combined (full kernel + HTTP)

Requires root and Linux. kprobe captures `accept4`, `epoll_wait`, `sendmsg`,
`recvmsg` — kernel timing, fd numbers, client IP/port. When both kprobe and Lua
fire for the same connection, they are merged into a single
`nginx.request.enriched` event via `(client_ip, client_port)` correlation.

```yaml
nginx:
  mode: combined
  lua_host: "127.0.0.1"
  lua_port: 9119
```

### Graph nodes produced

```
nginx::master
nginx::worker-{pid}      ← one per worker process discovered at probe start
nginx::{uri}             ← one per unique URI pattern after normalisation
```

### Structural edges

```
nginx::master  ──spawned──►  nginx::worker-{pid}
nginx::worker-{pid}  ──handled──►  nginx::{uri}
```

These edges are drawn by `_add_structural_edges` in `runtime_graph.py` when
`nginx.worker.discovered` and `nginx.request.complete` / `nginx.request.enriched`
events arrive. The nginx probe parks `nginx.main.start` and
`nginx.worker.discovered` events in `_pre_fork_events` at `start()` time and
drains them into the worker's engine via `_register_post_init_callback` after
`init()` completes — same pattern as gunicorn_probe and celery_probe.

---

## Run

```bash
cd /path/to/stack-tracer/applications/django

export DJANGO_SETTINGS_MODULE=config.settings

gunicorn -c gunicorn.conf.py config.asgi:application \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 127.0.0.1:8000 \
  --timeout 4000000 \
  --workers 1
```

For multiple workers (graph shows separate `UvicornWorker-{pid}` nodes):

```bash
  --workers 3
```

With 3 workers but low request rate the OS routes to the same worker. Use
`--delay 0` in the load test scripts to saturate all workers.

---

## REPL

```bash
python -m stacktracer.scripts.repl
```

```
SHOW nodes
SHOW edges
SHOW events LIMIT 20
\stitch <trace_id>
```

---

## Load testing

```bash
# steady concurrent load
python load_test.py --requests 200 --workers 20 --delay 0

# burst waves — watch graph between waves
python burst_test.py --waves 6 --burst 100 --workers 15 --quiet 5
```

---

## OTel Bridge Mode (optional)

StackTracer can run in OpenTelemetry bridge mode instead of native probe mode.
In this mode OTel is the event source — StackTracer's own probes are disabled
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
STACKTRACER_OTEL_MODE = True   # False = native probes (default)
```

`apps.py` reads this flag automatically and switches between native and OTel
initialisation. No other code changes needed.

### What `apps.py` does in each mode

```
STACKTRACER_OTEL_MODE = False (default)
    → stacktracer.init()               starts engine + native probes
    → TracerMiddleware                 sets trace_id per request
    → django/asyncio/gunicorn probes   emit NormalizedEvents directly

STACKTRACER_OTEL_MODE = True
    → stacktracer.init(otel_mode=True) starts engine only, no probes
    → DjangoInstrumentor               OTel instruments Django automatically
    → Psycopg2Instrumentor             OTel instruments DB queries
    → BatchSpanProcessor               batches completed spans
    → StackTracerSpanExporter          converts OTel spans → NormalizedEvents
    → engine.process(event)            same graph, same causal rules
```

### traceparent propagation

In OTel mode, W3C `traceparent` headers are propagated automatically by the
OTel SDK. If you also have `TracerMiddleware` active, extract and share the
trace_id so native events and OTel spans are correlated:

```python
# in TracerMiddleware.__call__() entry path
from stacktracer.bridge.otel_bridge import extract_trace_id_from_traceparent

traceparent = request.META.get("HTTP_TRACEPARENT", "")
if traceparent:
    trace_id = extract_trace_id_from_traceparent(traceparent)
    if trace_id:
        set_trace_id(trace_id)   # StackTracer uses the same trace_id as OTel
```

This means nginx → uvicorn → Django events and OTel spans all share one
`trace_id` and the `\stitch` command reconstructs the full timeline.

### What is observed in each mode

| Signal | Native probes | OTel bridge |
|---|---|---|
| Django request lifecycle | ✓ | ✓ |
| DB query timing | ✓ | ✓ |
| Redis calls | ✓ | ✓ |
| Celery tasks | ✓ | ✓ |
| asyncio `Task.__step` | ✓ | ✗ |
| Event loop tick / ready queue | ✓ | ✗ |
| nginx kprobe | ✓ | ✗ |
| Gunicorn worker lifecycle | ✓ | ✗ |
| Cross-service distributed tracing | ✗ | ✓ |

### REPL usage in OTel mode

Identical to native probe mode. Once spans start flowing through
`StackTracerSpanExporter`, the graph builds normally:

```bash
python -m stacktracer.scripts.repl

› SHOW nodes
› CAUSAL
› DIFF SINCE deployment
```

The causal rules — loop starvation, N+1, retry amplification — evaluate
against the same `RuntimeGraph` regardless of how events arrived.
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

## nginx setup: this probe is available on the origintracer website.

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
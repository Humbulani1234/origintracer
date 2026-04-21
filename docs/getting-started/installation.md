# Installation

## Requirements

- Python 3.11, 3.12, or 3.13
- Django 4.2+ (for Django probe)
- No other required dependencies

## Install

```bash
pip install stacktracer
```

## Optional dependencies

| Package | Purpose |
|---|---|
| `msgpack` | Binary event serialisation — 3x smaller than JSON |
| `psycopg2-binary` | PostgreSQL storage backend |
| `httpx` | Uploader (sending snapshots to FastAPI backend) |
| `bcc` | nginx kprobe mode (Linux only, requires root) |

```bash
# recommended minimum
pip install stacktracer msgpack httpx
```

## Verify

```bash
python -c "import stacktracer; print(stacktracer.__version__)"
```

---

# Quick Start

The fastest path to a working graph.

## 1. Add middleware

`TracerMiddleware` must be the **first** entry in `MIDDLEWARE`. Without it `get_trace_id()` returns `None` everywhere and all probe events are silently dropped.

```python
# settings.py
MIDDLEWARE = [
    "stacktracer.probes.django_probe.TracerMiddleware",  # ← first
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    # ...
]
```

## 2. Initialise in AppConfig

```python
# apps.py
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import stacktracer
        stacktracer.init(debug=True)
```

`debug=True` enables verbose logging so you can confirm events are flowing.

## 3. Add config file

```yaml
# stacktracer.yaml — place in the project root
probes:
  - django
  - asyncio
  - gunicorn
  - uvicorn
```

## 4. Run the REPL

In a second terminal while your app is running:

```bash
python -m stacktracer.scripts.repl
```

Send a few requests to your app, then:

```
› SHOW nodes
› SHOW edges
› CAUSAL
```

You should see the graph building in real time.

---

# Django Integration

## Complete setup

### settings.py

```python
MIDDLEWARE = [
    "stacktracer.probes.django_probe.TracerMiddleware",
    # ... rest of middleware
]

# optional — switch to OTel bridge mode
STACKTRACER_OTEL_MODE = False
```

### apps.py

```python
import os
from django.apps import AppConfig
from django.conf import settings

class WorkerConfig(AppConfig):
    name = "worker"

    def ready(self):
        import stacktracer

        otel_mode = getattr(settings, "STACKTRACER_OTEL_MODE", False)

        if otel_mode:
            self._init_otel()
        else:
            self._init_native()

    def _init_native(self):
        import stacktracer
        stacktracer.init(debug=True)
        stacktracer.mark_deployment(
            os.getenv("GIT_SHA", "dev")
        )

    def _init_otel(self):
        import stacktracer
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.instrumentation.django import DjangoInstrumentor
        from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
        from stacktracer.bridge.otel_bridge import StackTracerSpanExporter

        stacktracer.init(otel_mode=True, debug=True)

        provider = TracerProvider()
        provider.add_span_processor(
            BatchSpanProcessor(StackTracerSpanExporter())
        )
        trace.set_tracer_provider(provider)

        DjangoInstrumentor().instrument()
        Psycopg2Instrumentor().instrument()
```

### stacktracer.yaml

```yaml
probes:
  - django
  - asyncio
  - gunicorn
  - uvicorn
```

### gunicorn.conf.py

```python
from stacktracer.probes.gunicorn_probe import install_hooks
install_hooks()

bind    = "127.0.0.1:8000"
workers = 1
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 120
```

### Run

```bash
export DJANGO_SETTINGS_MODULE=config.settings
gunicorn -c gunicorn.conf.py config.asgi:application \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 127.0.0.1:8000 \
    --workers 1
```

## What you get

| Probe event | Description |
|---|---|
| `request.entry` | Every HTTP request enters middleware |
| `request.exit` | Every HTTP response leaves middleware with duration |
| `django.view.enter` | CBV dispatch entered (class name) |
| `django.view.exit` | CBV dispatch returned with duration |
| `django.db.query` | Every ORM/raw SQL query with duration |
| `django.exception` | Unhandled exceptions |
| `gunicorn.worker.fork` | Worker process forked |
| `uvicorn.request.receive` | ASGI scope constructed |
| `uvicorn.request.complete` | Full request/response cycle complete |
| `asyncio.loop.tick` | Event loop tick (Python 3.11) |
| `asyncio.task.create` | Task created via `asyncio.create_task` |

---

# Celery Integration

## One init per process

| Process | Where to call `init()` |
|---|---|
| Gunicorn worker | `AppConfig.ready()` |
| Celery ForkPoolWorker | `celery_probe._on_worker_fork` (worker_process_init signal) |
| `config/celery.py` | **Never** — causes duplicate engine bug |

## celery config

```python
# config/celery.py
import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# DO NOT call stacktracer.init() here
# ForkPoolWorker init is handled by celery_probe._on_worker_fork
```

## stacktracer.yaml for Celery worker

```yaml
# stacktracer.yaml in the celery application directory
probes:
  - celery
  - django
  - asyncio
```

## Run

```bash
# Terminal 1 — Django
gunicorn -c gunicorn.conf.py config.asgi:application \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 127.0.0.1:8000 --workers 1

# Terminal 2 — Celery
DJANGO_SETTINGS_MODULE=config.settings \
STACKTRACER_CONFIG=/path/to/celery/stacktracer.yaml \
celery -A config worker --loglevel=info --concurrency=1
```

## Cross-process tracing

Use the `dispatch` helper from `celery_probe` instead of calling `.delay()` directly. It passes `trace_id` through task kwargs so the causal edge from the Django view to the Celery task is reconstructed:

```python
from stacktracer.probes.celery_probe import dispatch

# instead of: my_task.delay(arg1, arg2)
dispatch(my_task, get_trace_id(), arg1, arg2)
```

In the REPL, `\stitch <trace_id>` joins the Django and Celery graphs into one timeline.

---

# Configuration

## stacktracer.yaml

```yaml
# stacktracer.yaml — place in project root or set STACKTRACER_CONFIG env var

probes:
  - django
  - asyncio
  - gunicorn
  - uvicorn
  # - celery
  # - redis
  # - nginx

sample_rate: 1.0          # 1.0 = trace all requests, 0.1 = 10%

normalize:
  enabled: true           # normalise /api/users/123/ → /api/users/{id}/

compactor:
  enabled: true
  interval_s: 60          # compact graph every 60s

semantic:
  labels:                 # human-readable aliases for node ids
    auth: "django::/api/auth/"
    users: "django::/api/users/"
```

## Environment variables

| Variable | Description |
|---|---|
| `STACKTRACER_CONFIG` | Path to stacktracer.yaml |
| `STACKTRACER_API_KEY` | API key for sending snapshots to backend |
| `STACKTRACER_ENDPOINT` | Backend URL e.g. `http://localhost:7123` |

## init() kwargs

```python
stacktracer.init(
    config      = "stacktracer.yaml",   # path to config file
    api_key     = "sk_...",             # overrides STACKTRACER_API_KEY
    endpoint    = "http://...",         # overrides STACKTRACER_ENDPOINT
    debug       = False,                # verbose logging
    otel_mode   = False,                # use OTel bridge instead of probes
    sample_rate = 1.0,                  # fraction of requests to trace
)
```

kwargs override config file values which override defaults.yaml.
# Installation

```bash
git clone https://github.com/Humbulani1234/origintracer.git
cd origintracer 
pip install -e .
```

---

# Quick Start

The fastest path to a working graph. Place your django project in the `applications` directory, and follow the following steps.

## 1. Add middleware

`TracerMiddleware` must be the **first** entry in `MIDDLEWARE`. Without it `get_trace_id()` returns `None` everywhere and all probe events are silently dropped.

```python
# settings.py
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware", # first
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    # ...
]
```

## 3. Add config file

```yaml
# origintracer.yaml - place in the project root
probes:
  - django
  - asyncio
```

## 2. Initialise in AppConfig

```python
# apps.py
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import origintracer
        origintracer.init(debug=True, onfig=BASE_DIR / "origintracer.yaml")
```

`debug=True` enables verbose logging so you can confirm events are flowing.

## 4. Run the REPL

Send requests to your views using the provided `load_test` and `burst_test` scripts and explore the results with the REPL.

In a second terminal while your app is running:

```bash
python -m origintracer.repl.repl
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

Navigate to the `applications/django` directory for this specific application.
This is a fully OriginTracer already configured `Django` example application. The following steps detail how it was configured and the steps to follow for your application.

## Complete setup

### settings.py

```python
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware",
    # ... rest of middleware
]

# optional - switch to OTel bridge mode [experimental]
ORIGINTRACER_OTEL_MODE = False
```

### apps.py

```python
import os
from django.apps import AppConfig
from django.conf import settings

class WorkerConfig(AppConfig):
    name = "worker"

    def ready(self):
        import origintracer

        otel_mode = getattr(settings, "ORIGINTRACER_OTEL_MODE", False)

        if otel_mode:
            self._init_otel()
        else:
            self._init_native()

    def _init_native(self):
        import origintracer
        origintracer.init(debug=True)
        origintracer.mark_deployment(
            os.getenv("GIT_SHA", "dev")
        )

    def _init_otel(self):
        import origintracer
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.instrumentation.django import DjangoInstrumentor
        from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
        from stacktracer.bridge.otel_bridge import OriginTracerSpanExporter

        stacktracer.init(otel_mode=True, debug=True, onfig=BASE_DIR / "origintracer.yaml")

        provider = TracerProvider()
        provider.add_span_processor(
            BatchSpanProcessor(OriginTracerSpanExporter())
        )
        trace.set_tracer_provider(provider)

        DjangoInstrumentor().instrument()
        Psycopg2Instrumentor().instrument()
```

### origintracer.yaml

```yaml
probes:
  - django
  - asyncio
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

Navigate to the `applications/celery` directory for this specific application.
This is a fully OriginTracer already configured `Django/Celery` example application. The following steps detail how it was configured and the steps to follow for your application.

## One init per process

| Process | Where to call `init()` |
|---|---|
| Gunicorn worker | `AppConfig.ready()` |
| Celery ForkPoolWorker | `celery_probe._on_worker_fork` (worker_process_init signal) |
| `config/celery.py` | **Never** - causes duplicate engine bug |

## celery config

```python
# config/celery.py
import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# DO NOT call origintracer.init() here
# ForkPoolWorker init is handled by celery_probe._on_worker_fork
```

## origintracer.yaml for Celery worker

```yaml
# origintracer.yaml in the celery application directory
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
ORIGINTRACER_CONFIG=/path/to/celery/origintracer.yaml \
celery -A config worker --loglevel=info --concurrency=1
```

## Cross-process tracing

Use the `dispatch` helper from `celery_probe` instead of calling `.delay()` directly. It passes `trace_id` through task kwargs so the causal edge from the Django view to the Celery task is reconstructed:

```python
from origintracer.probes.celery_probe import dispatch

# instead of: my_task.delay(arg1, arg2)
dispatch(my_task, get_trace_id(), arg1, arg2)
```

In the REPL, `\stitch <trace_id>` joins the Django and Celery graphs into one timeline.

---

# Configuration

## origintracer.yaml

```yaml
# origintracer.yaml - place in project root or set ORIGINTRACER_CONFIG env var

probes:
  - django
  - asyncio
  - gunicorn
  - uvicorn
  # - celery
  # - redis
  # - nginx

sample_rate: 1.0 # 1.0 = trace all requests, 0.1 = 10%

normalize:
  enabled: true # normalise /api/users/123/ --> /api/users/{id}/

compactor:
  enabled: true
  interval_s: 60 # compact graph every 60s

semantic:
  labels: # human-readable aliases for node ids
    auth: "django::/api/auth/"
    users: "django::/api/users/"
```

## Environment variables

| Variable | Description |
|---|---|
| `ORIGINTRACER_CONFIG` | Path to origintracer.yaml |
| `ORIGINTRACER_API_KEY` | API key for sending snapshots to backend |
| `ORIGINTRACER_ENDPOINT` | Backend URL e.g. `http://localhost:8001` |

## init() kwargs

```python
origintracer.init(
    config = "stacktracer.yaml", # path to config file
    api_key = "sk_...", # overrides ORIGINTRACER_API_KEY
    endpoint = "http://...", # overrides ORIGINTRACER_ENDPOINT
    debug = False, # verbose logging
    otel_mode = False, # use OTel bridge instead of probes
    sample_rate = 1.0, # fraction of requests to trace
)
```

kwargs override config file values which override defaults.yaml.
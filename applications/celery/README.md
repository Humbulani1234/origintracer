# OriginTracer - Celery Application

Traces the cross-process path: Django view --> Redis (task queue) --> Celery worker. The trace_id travels inside task kwargs (`_trace_id`) across the
Redis boundary, allowing `\stitch` in the REPL to join both sides into one timeline.

---

## Directory layout

```
applications/celery/
├── config/
│   ├── __init__.py << exports celery_app
│   ├── settings.py << TracerMiddleware MUST be first in MIDDLEWARE
│   ├── celery.py << Celery app - no origintracer.init() here
│   ├── asgi.py
│   └── urls.py
├── worker/ << Django app directory (not a celery module)
│   ├── __init__.py
│   ├── apps.py << AppConfig.ready() — init() for gunicorn only
│   ├── views.py
│   ├── tasks.py
│   └── urls.py
├── probes/ << user extension probes
│   ├── __init__.py
│   ├── celery_probe.py << CeleryProbe - handles fork + task lifecycle
│   └── redis_probe.py << TracedRedis subclass
├── origintracer.yaml
└── gunicorn.conf.py
```
---

## Prerequisites

```bash
pip install origintracer gunicorn uvicorn django celery redis

```

---

## config/celery.py

```python

import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

```

```python

from .celery import app as celery_app
__all__ = ["celery_app"]
```
---

## TracedRedis - subclass pattern

`redis_probe.py` in the app's `probes/` directory uses subclassing, not composition.

```python
import redis
from origintracer.core.event_schema import NormalizedEvent
from origintracer.context.vars import get_trace_id
from origintracer.sdk.emitter import emit
import time

class TracedRedis(redis.Redis):
    def execute_command(self, *args, **options):
        trace_id = get_trace_id()
        command  = args[0] if args else "UNKNOWN"
        key = str(args[1])[:100] if len(args) > 1 else ""
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
# from probes.redis_probe import TracedRedis
# r = TracedRedis(host="localhost", port=6379, db=0)
```
---

## worker/apps.py 

```python
from django.apps import AppConfig

class WorkerConfig(AppConfig):
    name = "worker"

    def ready(self):
        import origintracer
        origintracer.init(debug=True)
        # Celery worker gets its own init() inside celery_probe._on_worker_fork
```

---

## origintracer.yaml

```yaml
probes:
  - django
  - asyncio
  - gunicorn
  - uvicorn
  - celery
  - redis
```

---

## Dispatch helper in views.py

The view emits a `celery.task.dispatch` event with `service="celery"` so the
node name is `celery::task_name`, not `django::task_name`. The edge then reads:


```python

from origintracer.context.vars import get_trace_id
from origintracer.sdk.emitter import emit
from origintracer.core.event_schema import NormalizedEvent

def _dispatch(task_fn, *args, **kwargs):
    trace_id = get_trace_id()
    task_fn.delay(*args, _trace_id=trace_id, **kwargs)
    if trace_id:
        emit(NormalizedEvent.now(
            probe="celery.task.dispatch",
            trace_id=trace_id,
            service="celery",          
            name=task_fn.name,
        ))
```

---

## Run

**Terminal 1 - gunicorn:**

```bash
cd /path/to/origintracer/applications/celery

export DJANGO_SETTINGS_MODULE=config.settings

gunicorn -c gunicorn.conf.py config.asgi:application \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 127.0.0.1:8000 \
  --timeout 4000000 \
  --workers 1
```

**Terminal 2 - celery worker:**

```bash
cd /path/to/origintracer/applications/celery

DJANGO_SETTINGS_MODULE=config.settings \
ORIGINTRACER_CONFIG=/path/to/origintracer/applications/celery/origintracer.yaml\
celery -A config worker --loglevel=info --concurrency=1
```
---

## REPL

Send heavy requests to OriginTracer.

```bash
python /path/to/applications/celery/burst_test.py
```

Start the REPL in its own terminal

```bash
python -m origintracer.repl.repl
```

Query it

```
# On gunicorn worker socket:
SHOW nodes << gunicorn/uvicorn/django/redis nodes
SHOW edges << gunicorn::master -- spawned --> gunicorn::UvicornWorker-{pid}

# On celery worker socket:
SHOW nodes << celery::MainProcess, ForkPoolWorker, task nodes
SHOW edges << MainProcess -- spawned --> ForkPoolWorker -- ran --> task

# Cross-process - find trace_id from SHOW events, then:
\stitch <trace_id>
```


```
SHOW events LIMIT 5
```

Copy a `trace_id` value from the output, then pass it to `\stitch`.

---

## Load testing

```bash
# steady concurrent load
python load_test.py --requests 200 --workers 20 --delay 0

# burst waves - watch graph between waves
python burst_test.py --waves 6 --burst 100 --workers 15 --quiet 5
```
---


# StackTracer — Celery Application

Traces the cross-process path: Django view → Redis (task queue) → Celery worker.
The trace_id travels inside task kwargs (`_trace_id`) across the Redis boundary,
allowing `\stitch` in the REPL to join both sides into one timeline.

---





## Directory layout

```
applications/celery/
├── config/
│   ├── __init__.py          ← exports celery_app
│   ├── settings.py          ← TracerMiddleware MUST be first in MIDDLEWARE
│   ├── celery.py            ← Celery app — NO stacktracer.init() here
│   ├── asgi.py
│   └── urls.py
├── worker/                  ← Django app directory (not a celery module)
│   ├── __init__.py          ← empty
│   ├── apps.py              ← AppConfig.ready() — init() for gunicorn only
│   ├── views.py
│   ├── tasks.py
│   └── urls.py
├── probes/                  ← user extension probes (absolute imports)
│   ├── __init__.py
│   ├── celery_probe.py      ← CeleryProbe — handles fork + task lifecycle
│   └── redis_probe.py       ← TracedRedis subclass
├── stacktracer.yaml
└── gunicorn.conf.py
```

---

## Prerequisites

```bash
pip install stacktracer gunicorn uvicorn django celery redis

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

## worker/apps.py 

```python
from django.apps import AppConfig

class WorkerConfig(AppConfig):
    name = "worker"

    def ready(self):
        import stacktracer
        stacktracer.init(debug=True)
        # Celery worker gets its own init() inside celery_probe._on_worker_fork
```

---

## stacktracer.yaml

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

from stacktracer.context.vars import get_trace_id
from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import NormalizedEvent

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

**Terminal 1 — gunicorn:**

```bash
cd /path/to/stack-tracer/applications/celery

export DJANGO_SETTINGS_MODULE=config.settings

gunicorn -c gunicorn.conf.py config.asgi:application \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 127.0.0.1:8000 \
  --timeout 4000000 \
  --workers 1
```

**Terminal 2 — celery worker:**

```bash
cd /path/to/stack-tracer/applications/celery

DJANGO_SETTINGS_MODULE=config.settings \
STACKTRACER_CONFIG=/path/to/stack-tracer/applications/celery/stacktracer.yaml \
celery -A config worker --loglevel=info --concurrency=1
```

`STACKTRACER_CONFIG` must be set explicitly for Celery — the cwd-upward search
does not always find the yaml from within the Celery process.

---

## REPL



```bash
python -m stacktracer.scripts.repl
```

```
# On gunicorn worker socket:
SHOW nodes        ← gunicorn/uvicorn/django/redis nodes
SHOW edges        ← gunicorn::master ──spawned──► gunicorn::UvicornWorker-{pid}

# On celery worker socket:
SHOW nodes        ← celery::MainProcess, ForkPoolWorker, task nodes
SHOW edges        ← MainProcess ──spawned──► ForkPoolWorker ──ran──► task

# Cross-process — find trace_id from SHOW events, then:
\stitch <trace_id>
```


```
SHOW events LIMIT 5
```

Copy a `trace_id` value from the output, then pass it to `\stitch`.

---


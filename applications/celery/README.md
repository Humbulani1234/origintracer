# StackTracer — Celery Application

Traces the cross-process path: Django view → Redis (task queue) → Celery worker.
The trace_id travels inside task kwargs (`_trace_id`) across the Redis boundary,
allowing `\stitch` in the REPL to join both sides into one timeline.

---

## Process model

```
Terminal 1: gunicorn → forks → UvicornWorker → runs Django
Terminal 2: celery main process → forks → ForkPoolWorker
Terminal 3: Redis server (standalone TCP — no StackTracer involvement)
```

Gunicorn and Celery fork independently. They have no parent-child relationship.
Django's `.delay()` writes a JSON message to Redis. Celery main polls Redis
and hands it to ForkPoolWorker. The two engines (one per process group) produce
separate graphs that `\stitch` merges at query time.

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
pip install -e /path/to/stack-tracer
```

---

## One init() per process — the critical rule

| Process | Where init() is called |
|---------|----------------------|
| Gunicorn worker | `worker/apps.py` — `AppConfig.ready()` |
| Celery ForkPoolWorker | `celery_probe._on_worker_fork` — after fork via `worker_process_init` signal |
| `config/celery.py` | **never** — causes duplicate engine bug |
| `worker/__init__.py` | **never** |

**Duplicate init() symptom:** `Local query server failed to start: [Errno 98] Address already in use`.
Two engines in the same process — REPL connects to engine 1 (empty), events land in engine 2.

---

## config/celery.py

```python
# config/celery.py
import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
# NO stacktracer.init() here
```

```python
# config/__init__.py
from .celery import app as celery_app
__all__ = ["celery_app"]
```

---

## worker/apps.py — init() for gunicorn only

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

## celery_probe.py — key design decisions

**Pre-fork event parking** — same pattern as gunicorn_probe. `_on_main_ready`
(connected to `celeryd_after_setup`) emits `celery.main.start` into
`_pre_fork_events` before fork. `_on_worker_fork` calls `stacktracer.init()`
then `_drain_pre_fork_events()` so the worker's engine gets the MainProcess node.

**`_task_state` dict** — single dict keyed by task request id. Not two separate
dicts, not stored on `task.request`.

**`_on_task_failure` uses `.get()`** — never owns token reset.
**`_on_task_end` is sole owner of `.pop()` and `reset_trace()`** with try/except guard.

**No duplicate signal connections** — `_on_worker_fork` defined once as class method.
Defining it twice causes the second to shadow the first silently.

**Signals connected:**
`celeryd_after_setup`, `task_prerun`, `task_postrun`,
`task_retry`, `task_failure`, `worker_process_init`, `worker_process_shutdown`

---

## Dispatch helper in views.py

The view emits a `celery.task.dispatch` event with `service="celery"` so the
node name is `celery::task_name`, not `django::task_name`. The edge then reads:

```
django::/tasks/report/1/  ──dispatched──►  celery::myapp.tasks.process_report
```

```python
# worker/views.py
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
            service="celery",          # ← not "django"
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

Two sockets are created — one per process group:

```bash
ls /tmp/stacktracer-*.sock
# /tmp/stacktracer-24861.sock   ← gunicorn worker
# /tmp/stacktracer-25103.sock   ← celery ForkPoolWorker
```

Open two REPL sessions or use `\stitch`:

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

**`\stitch` takes a trace_id, not a node name.** Get a real trace_id first:

```
SHOW events LIMIT 5
```

Copy a `trace_id` value from the output, then pass it to `\stitch`.

---

## Expected graph — celery worker

```
celery::MainProcess  ──spawned──►  celery::ForkPoolWorker
celery::ForkPoolWorker  ──ran──►   celery::myapp.tasks.process_report
```

## Expected graph — gunicorn worker (cross-process edge)

```
django::/tasks/report/1/  ──dispatched──►  celery::myapp.tasks.process_report
```

---

## Known issues encountered

**`node.name` AttributeError in `_add_structural_edges`** — `GraphNode` has no
`name` attribute. Use `nid.split("::", 1)[1]` to extract the name from the node id.
Exceptions in `_add_structural_edges` are swallowed by a broad `except` in the
engine — add `logger.warning` or `logger.exception` there to surface them.

**Duplicate `_on_worker_fork`** — defining the method twice in the class body
causes the second definition to shadow the first. Only one is registered as a
signal handler. Remove the duplicate.

**`worker/__init__.py` importing celery app** — causes `stacktracer.init()` to
run at module import time in the Celery process before `worker_process_init` fires.
Keep `worker/__init__.py` empty.

**`_task_state` KeyError in `_on_task_failure`** — use `.get()` not direct key
access in failure handler since `task_prerun` may not have fired for all failure paths.

**Stale `__pycache__`** — clear after editing probe files:

```bash
find applications/celery -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
```
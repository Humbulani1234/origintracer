# OriginTracer - Django Application

Traces the full request path: nginx >> gunicorn >> uvicorn >> Django >> Redis.
OriginTracer instruments automatically via probes - no decorators, no SDK calls in your views.

---

## Directory layout

```
applications/django/
├── config/
│   ├── __init__.py
│   ├── settings.py
│   ├── asgi.py
│   └── urls.py
├── worker/ << Django app
│   ├── __init__.py
│   ├── apps.py << AppConfig.ready() — single init() point
│   ├── views.py
│   ├── models.py
│   └── urls.py
│   
├── stacktracer.yaml << user config — overrides defaults.yaml
├── gunicorn.conf.py
└── manage.py
```

---

## Prerequisites

```bash
pip install stacktracer gunicorn uvicorn django redis
```
---

## settings.py

`TracerMiddleware` **must be the first entry** in `MIDDLEWARE`. Without this,
`get_trace_id()` returns `None` in all probes - events are silently dropped.

```python
MIDDLEWARE = [
    "stacktracer.probes.django_probe.TracerMiddleware", # MUST be first
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    ...
]
```

---

## apps.py

```python
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

## nginx setup: 

nginx sits in front of gunicorn and is observed via log tail (default), Lua UDP,
or kprobe — configured in `stacktracer.yaml` under the `nginx` key.

### Log tail

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

## Probes

The probes for: nginx, gunicorn and uvicorn are available on
[origintracer.app](https://origintracer.app)

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

---

## REPL

```bash
python -m origintracer.repl.repl
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

# burst waves - watch graph between waves
python burst_test.py --waves 6 --burst 100 --workers 15 --quiet 5
```
---
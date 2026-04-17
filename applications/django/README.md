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
│   ├── apps.py << AppConfig.ready() - single init() point
│   ├── views.py
│   ├── models.py
│   └── urls.py
│   
├── origintracer.yaml << user config - overrides defaults.yaml
├── gunicorn.conf.py
└── manage.py
```

---

## Prerequisites

```bash
pip install origintracer gunicorn uvicorn django redis
```
---

## settings.py

`TracerMiddleware` **must be the first entry** in `MIDDLEWARE`. Without this,
`get_trace_id()` returns `None` in all probes - events are silently dropped.

```python
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware", # MUST be first
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
        import origintracer
        origintracer.init(debug=True)
```

`AppConfig.ready()` runs once per process after Django is fully loaded.
This is the only place `origintracer.init()` is called for the gunicorn worker.
Never call `init()` at module level or in `settings.py`.

---

## origintracer.yaml

```yaml
probes:
  - nginx
  - gunicorn
  - uvicorn
  - django
```

---

## nginx setup: 

nginx sits in front of gunicorn and is observed via log tail (default), Lua UDP,
or kprobe - configured in `origintracer.yaml` under the `nginx` key.

### Log tail

The simplest mode. nginx writes a JSON access log. OriginTracer tails it.

**nginx.conf** - add a JSON log format and point the access log to it:

```nginx
http {
    log_format origintracer escape=json
        '{"remote_addr":"$remote_addr",'
        '"request_time":$request_time,'
        '"upstream_response_time":"$upstream_response_time",'
        '"request":"$request",'
        '"status":$status,'
        '"body_bytes_sent":$body_bytes_sent,'
        '"request_id":"$http_x_request_id"}';

    server {
        listen 80;

        access_log /var/log/nginx/access.log origintracer;

        location / {
            proxy_pass http://127.0.0.1:8000;
            proxy_set_header X-Request-Id $request_id;
        }
    }
}
```
---

## Probes

The probes for: nginx, gunicorn and uvicorn are available on
[origintracer.app](https://origintracer.app)

---

## Run

```bash
cd /path/to/origintracer/applications/django

export DJANGO_SETTINGS_MODULE=config.settings

gunicorn -c gunicorn.conf.py config.asgi:application \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 127.0.0.1:8000 \
  --timeout 4000000 \
  --workers 1
```

---

## REPL

Send heavy requests to OriginTracer.

```bash
python /path/to/applications/django/burst_test_benchmarked.py
```

Start the REPL in its own terminal

```bash
python -m origintracer.repl.repl
```

Query it

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
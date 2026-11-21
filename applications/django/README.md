# OriginTracer - Django Application

Traces the full request path: **nginx --> gunicorn --> uvicorn --> django --> redis**. OriginTracer instruments automatically via probes - no decorators, no SDK calls in your views.

---

## Project layout

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

The middleware assigns one span ID to each request entry and exit. Class-based
view entry and exit events share their own span ID and link to the request span;
database query events record the active view as their parent when available.
These links let stored trace reports derive request and view intervals and
subtract observed child intervals for a self-time estimate. The estimate can
include work from child operations that were not captured. Async view spans
remain active while the returned coroutine is awaited.

```python
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware", # MUST be first
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    ...
]
```
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

## apps.py

```python
from django.apps import AppConfig

class WorkerConfig(AppConfig):
    name = "worker"

    def ready(self):
        import origintracer
        origintracer.init(debug=True, BASE_DIR / "origintracer.yaml")
```

`AppConfig.ready()` runs once per process after Django is fully loaded.
This is the only place `origintracer.init()` is called for the gunicorn worker.
Never call `init()` at module level or in `settings.py`.

---

## nginx setup:

nginx sits in front of gunicorn and is observed via log tail (default), Lua UDP,
or kprobe - configured in `origintracer.yaml` under the `nginx` key.

### Log tail

The Nginx probe reads the access log by its absolute path (`/var/log/nginx/access.log`), so it does not depend on the directory from which Django or Gunicorn starts. The Gunicorn process must be able to read that file. The current probe uses this default path; it does not yet read a `log_path` value from `origintracer.yaml`. To use another log location, change the `log_path` default in the local `NginxProbe` constructor.

Add the `log_format` inside Nginx's `http {}` context (for example, in `/etc/nginx/conf.d/origintracer-log.conf`):

```nginx
log_format origintracer escape=json
    '{"remote_addr":"$remote_addr",'
    '"request_time":$request_time,'
    '"upstream_response_time":"$upstream_response_time",'
    '"request":"$request",'
    '"uri":"$uri",'
    '"status":$status,'
    '"body_bytes_sent":$body_bytes_sent,'
    '"request_id":"$request_id"}';
```

Put the `server` block in your site configuration (for example, `/etc/nginx/sites-available/origintracer-django`) and enable it using your distribution's normal Nginx setup:

```nginx
server {
    listen 80;
    server_name localhost;

    access_log /var/log/nginx/access.log origintracer;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Request-Id $request_id;
    }
}
```

The same `$request_id` is written to the Nginx log and forwarded to the ASGI application, allowing their events to share a trace ID. Validate and reload Nginx with `sudo nginx -t` and `sudo systemctl reload nginx`, then send a request through Nginx rather than directly to port 8000.

The probe tries kprobe mode first and falls back to reading the JSON access log when kprobe is unavailable. For log mode, ensure the Gunicorn user has read access to `/var/log/nginx/access.log`; the probe cannot read events from a log it cannot access.

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
# StackTracer — Django Demo Application

A minimal Django application demonstrating StackTracer's built-in probes
across the full production stack: nginx → gunicorn → uvicorn → Django.

---

## What this demonstrates

When you run this app under the full stack, StackTracer fires these
built-in probes for every request:

```
nginx.connection.accept       nginx accepted the TCP connection
nginx.request.parse           HTTP headers parsed by nginx
nginx.upstream.dispatch       nginx dispatched to gunicorn
gunicorn.worker.spawn         worker process created by master
gunicorn.worker.init          worker ready to serve
uvicorn.request.receive       ASGI scope constructed
uvicorn.h11.cycle             full HTTP/1.1 cycle
django.middleware.enter       request through middleware stack
django.url.resolve            URL matched to view
django.view.enter             view function called
asyncio.loop.tick             event loop Task.__step
asyncio.task.create           coroutine scheduled
django.view.exit              view returned
django.middleware.exit        response through middleware
uvicorn.response.send         status code and headers written
gunicorn.worker.heartbeat     worker alive signal
```

You can then query the live graph from the REPL:

```
SHOW latency WHERE system = "http"
BLAME WHERE system = "django"
HOTSPOT TOP 10
DIFF SINCE deployment
CAUSAL WHERE tags = "blocking"
```

---

## Prerequisites

```
Python 3.11+
nginx
pip install stacktracer django gunicorn uvicorn httptools
```

---

## Project layout

```
django_app/
├── README.md
├── stacktracer.yaml          ← StackTracer config
├── requirements.txt
├── manage.py
├── config/
│   ├── settings.py
│   ├── urls.py
│   ├── asgi.py               ← ASGI entry point for uvicorn
│   └── wsgi.py
└── mysite/
    ├── views.py              ← demo views that exercise the probes
    ├── urls.py
    └── templates/
        └── index.html
```

---

## Step 1 — Install dependencies

```bash
cd applications/django_app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Step 2 — Database setup

```bash
python manage.py migrate
python manage.py createsuperuser
```

---

## Step 3 — Run under the full stack

### Terminal 1 — gunicorn with uvicorn workers

```bash
gunicorn config.asgi:application \
    --bind 127.0.0.1:8000 \
    --workers 2 \
    --worker-class uvicorn.workers.UvicornWorker \
    --access-logfile - \
    --log-level info
```

`--workers 2` creates two worker processes. StackTracer's gunicorn probe
will emit `gunicorn.worker.spawn` and `gunicorn.worker.init` for each.

For development (single process, no gunicorn):

```bash
uvicorn config.asgi:application --host 127.0.0.1 --port 8000 --reload
```

### Terminal 2 — nginx

Copy the nginx config and reload:

```bash
sudo cp nginx.conf /etc/nginx/sites-available/stacktracer-django
sudo ln -s /etc/nginx/sites-available/stacktracer-django \
           /etc/nginx/sites-enabled/
sudo nginx -t
sudo nginx -s reload
```

The nginx config proxies port 80 → gunicorn at 127.0.0.1:8000 and adds
`X-Request-ID` so nginx and uvicorn traces share the same trace ID.

### Terminal 3 — StackTracer REPL

```bash
python -m stacktracer.repl --config stacktracer.yaml
```

Send a request in a fourth terminal:

```bash
curl http://localhost/
curl http://localhost/async/
curl http://localhost/slow/       # simulates a blocking call
curl http://localhost/db/         # hits the database
```

Watch events appear in the REPL as requests arrive.

---

## nginx configuration

Save as `nginx.conf` in this directory. The key directives:

```nginx
upstream django {
    server 127.0.0.1:8000;
    keepalive 32;              # reuse connections to gunicorn
}

server {
    listen 80;
    server_name localhost;

    # Generate a unique request ID and forward it.
    # StackTracer reads X-Request-ID to correlate nginx and uvicorn traces.
    add_header X-Request-ID $request_id always;
    proxy_set_header X-Request-ID $request_id;

    location /static/ {
        alias /path/to/django_app/staticfiles/;
        expires 1d;
    }

    location / {
        proxy_pass http://django;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header Connection "";   # keepalive to gunicorn

        # Timeouts
        proxy_connect_timeout 5s;
        proxy_send_timeout    60s;
        proxy_read_timeout    60s;
    }
}
```

If you have eBPF access (Linux, root), enable the nginx probe in
`stacktracer.yaml` to get sub-millisecond nginx lifecycle events.
Without it, the nginx probe falls back to log parsing automatically.

---

## gunicorn configuration

For production use a `gunicorn.conf.py` instead of CLI flags:

```python
# gunicorn.conf.py

bind        = "127.0.0.1:8000"
workers     = 2                              # start with 2 × CPU cores
worker_class = "uvicorn.workers.UvicornWorker"
timeout     = 30
keepalive   = 5
accesslog   = "-"
errorlog    = "-"
loglevel    = "info"

# Notify StackTracer on each new worker
def post_fork(server, worker):
    # Re-bind the StackTracer engine in the new worker process.
    # Each worker runs its own event loop — the engine must be
    # initialised after fork, not before.
    import stacktracer
    stacktracer.init(config="stacktracer.yaml")
```

Run with:

```bash
gunicorn -c gunicorn.conf.py config.asgi:application
```

---

## StackTracer REPL queries to try

After sending a few requests, open the REPL and run:

```
# What is the slowest node in the system?
HOTSPOT TOP 10

# Which function is responsible for the most time in Django?
BLAME WHERE system = "django"

# Show latency breakdown across all layers
SHOW latency WHERE system = "http"

# Did the last deployment introduce any new calls?
DIFF SINCE deployment

# Run all causal rules — detect blocking calls and loop starvation
CAUSAL

# Trace a specific request end to end
SHOW path WHERE trace_id = "<paste a trace ID from HOTSPOT output>"
```

---

## What to look for

**Normal behaviour:**
- `django.view.enter` → `django.view.exit` duration is your view time
- `uvicorn.h11.cycle` duration ≈ `django.middleware` duration + small overhead
- `asyncio.loop.tick` avg_duration_ns < 1ms

**Signs of trouble:**
- `asyncio.loop.tick` avg_duration_ns > 10ms → blocking call on the event loop
- `uvicorn.h11.cycle` >> `django.view` duration → middleware overhead
- `CAUSAL` fires `loop_starvation` rule → check the `BLAME` output for the caller
- `DIFF SINCE deployment` shows new edges → new call introduced by last deploy
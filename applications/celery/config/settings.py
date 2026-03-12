"""
config/settings.py

Django + Celery settings for the StackTracer celery demo app.

Run with gunicorn:
    gunicorn config.asgi:application -k uvicorn.workers.UvicornWorker \
             --workers 2 -c gunicorn.conf.py

Run celery worker separately:
    celery -A config worker --loglevel=info --concurrency=2
"""

import os

SECRET_KEY = "celery-demo-not-for-production"
DEBUG      = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "worker",
]

# ── StackTracer middleware must be first ───────────────────────────────────
MIDDLEWARE = [
    "stacktracer.probes.django_probe.TracerMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF  = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME":   os.path.join(os.path.dirname(__file__), "..", "demo.db"),
    }
}

# ── Celery ─────────────────────────────────────────────────────────────────
CELERY_BROKER_URL        = "redis://localhost:6379/0"
CELERY_RESULT_BACKEND    = "redis://localhost:6379/0"
CELERY_ACCEPT_CONTENT    = ["json"]
CELERY_TASK_SERIALIZER   = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE          = "UTC"
CELERY_RESULT_EXPIRES    = 3600

# ── StackTracer ────────────────────────────────────────────────────────────
# Config path is passed to stacktracer.init() in config/celery.py and
# gunicorn.conf.py. Expose here so both entry points can read it.
STACKTRACER_CONFIG = os.environ.get(
    "STACKTRACER_CONFIG",
    os.path.join(os.path.dirname(__file__), "..", "stacktracer.yaml"),
)
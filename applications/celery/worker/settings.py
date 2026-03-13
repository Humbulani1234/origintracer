"""
worker/settings.py

Minimal Django settings for the Celery demo worker.
No URL routing, no views — only what Celery needs to run.
"""

SECRET_KEY = "celery-demo-not-for-production"
DEBUG = True

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "worker",
]

MIDDLEWARE = [
    "stacktracer.probes.django_probe.TracerMiddleware",  # must be first
    "django.middleware.common.CommonMiddleware",
]

# ── Database ───────────────────────────────────────────────────────────────
# slow_task uses raw sqlite3 directly, but Django still needs
# a DATABASES entry for django.setup() to succeed.

ASGI_APPLICATION = "config.asgi.application"
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": "demo.db",
    }
}

# ── Celery ─────────────────────────────────────────────────────────────────

CELERY_BROKER_URL = "redis://localhost:6379/0"
CELERY_RESULT_BACKEND = "redis://localhost:6379/0"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = "UTC"

# Prefork pool — 2 workers so WORKER_IMBALANCE rule has something to detect
CELERY_WORKER_CONCURRENCY = 2

# Keep task results for 1 hour — enough for the demo
CELERY_RESULT_EXPIRES = 3600

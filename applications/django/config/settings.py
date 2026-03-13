"""
config/settings.py

Minimal Django settings for StackTracer demonstration.
"""

from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "SECRET_KEY", "demo-secret-key-change-in-production"
)
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_tracer",
]

MIDDLEWARE = [
    "stacktracer.probes.django_probe.TracerMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "config.urls"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "django" / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
            ],
        },
    },
]

STATIC_URL = "/static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ── StackTracer ──────────────────────────────────────────────────────
# Initialise StackTracer once at settings import time.
# In production with gunicorn, re-init in gunicorn's post_fork hook
# so each worker gets its own engine instance.

# import stacktracer
# stacktracer.init(config=str(BASE_DIR / "stacktracer.yaml"))

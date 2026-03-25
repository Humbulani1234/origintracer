"""
config/celery.py

Celery application instance for the combined Django + Celery app.

StackTracer is initialised via on_after_configure (main process) and
worker_process_init (each forked worker) — see celery_probe.py for
the fork re-init logic.
"""

import os

from celery import Celery

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE", "config.settings"
)

app = Celery("worker")
app.config_from_object(
    "django.conf:settings", namespace="CELERY"
)
app.autodiscover_tasks()

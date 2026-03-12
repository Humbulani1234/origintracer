"""
config/celery.py

Celery application instance for the combined Django + Celery app.

StackTracer is initialised via on_after_configure (main process) and
worker_process_init (each forked worker) — see celery_probe.py for
the fork re-init logic.
"""

import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("worker")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()   # finds myapp/tasks.py


@app.on_after_configure.connect
def setup_stacktracer(sender, **kwargs):
    """
    Init StackTracer in the main Celery process after app is configured.
    Each forked worker re-inits via worker_process_init in celery_probe.py.
    """
    from django.conf import settings
    import stacktracer
    stacktracer.init(config=settings.STACKTRACER_CONFIG)
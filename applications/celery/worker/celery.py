"""
myworker/celery.py

Celery application instance.
"""

import os
import django
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myworker.settings")

app = Celery("myworker")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()


@app.on_after_configure.connect
def setup_stacktracer(sender, **kwargs):
    """
    Initialise StackTracer once the Celery app is configured.
    This fires in the main process before workers are forked.
    """
    import stacktracer
    stacktracer.init(config="stacktracer.yaml")
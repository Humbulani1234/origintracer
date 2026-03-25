# django_tracer/apps.py
from pathlib import Path

from django.apps import AppConfig


class DjangoTracerConfig(AppConfig):
    name = "worker"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        print(">>> AppConfig.ready() fired", flush=True)
        import os

        _is_runserver_reloader = (
            os.environ.get("RUN_MAIN")
            is None  # not runserver at all — uvicorn, gunicorn, etc.
            or os.environ.get("RUN_MAIN")
            == "true"  # runserver worker process
        )
        if not _is_runserver_reloader:
            return
        import stacktracer

        BASE_DIR = Path(__file__).resolve().parent.parent
        stacktracer.init(
            config=str(BASE_DIR / "stacktracer.yaml")
        )

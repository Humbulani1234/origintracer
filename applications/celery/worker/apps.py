from pathlib import Path

from django.apps import AppConfig

import origintracer


class DjangoTracerConfig(AppConfig):
    name = "worker"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        import os

        _is_runserver_reloader = (
            os.environ.get("RUN_MAIN")
            is None  # not runserver at all — uvicorn, gunicorn, etc.
            or os.environ.get("RUN_MAIN")
            == "true"  # runserver worker process
        )
        if not _is_runserver_reloader:
            return

        BASE_DIR = Path(__file__).resolve().parent.parent
        origintracer.init(
            config=str(BASE_DIR / "origintracer.yaml")
        )

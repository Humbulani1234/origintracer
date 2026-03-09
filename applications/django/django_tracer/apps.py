# django_tracer/apps.py
from django.apps import AppConfig
from pathlib import Path

class DjangoTracerConfig(AppConfig):
    name = "django_tracer"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        import os
        if os.environ.get("RUN_MAIN") != "true":
            return
        import stacktracer
        BASE_DIR = Path(__file__).resolve().parent.parent
        stacktracer.init(config=str(BASE_DIR / "stacktracer.yaml"))
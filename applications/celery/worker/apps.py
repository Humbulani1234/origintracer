from pathlib import Path

from django.apps import AppConfig

import origintracer


class DjangoTracerConfig(AppConfig):
    name = "worker"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        import os

        _is_runserver_reloader = (
            os.environ.get("RUN_MAIN") is None
            or os.environ.get("RUN_MAIN")
            == "true"  # runserver worker process
        )
        if not _is_runserver_reloader:
            return

        config_path = os.environ.get(
            "ORIGINTRACER_CONFIG",
            str(
                Path(__file__).resolve().parent.parent
                / "origintracer.yaml"
            ),
        )
        origintracer.init(
            api_key=os.environ.get(
                "ORIGINTRACER_API_KEY", "test-key-123"
            ),
            endpoint=os.environ.get(
                "ORIGINTRACER_API_URL", "http://localhost:8001"
            ),
            config=config_path,
        )
        origintracer.mark_deployment("deployment")

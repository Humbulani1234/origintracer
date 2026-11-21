"""
ASGI entry point for uvicorn and gunicorn+UvicornWorker.
This is what uvicorn calls for every request.
"""

import os

import django
from django.core.asgi import get_asgi_application

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE", "config.settings"
)

django.setup()

# Import the app-local middleware after Django initialization; this ordering is
# deliberate even though it triggers Ruff E402.
from probes.uvicorn_probe import (  # noqa: E402
    OriginTracerASGIMiddleware,
)

application = OriginTracerASGIMiddleware(get_asgi_application())

"""
ASGI entry point for uvicorn and gunicorn+UvicornWorker.
This is what uvicorn calls for every request.
"""

import os

import django
from django.core.asgi import get_asgi_application

from origintracer.probes.uvicorn_probe import (
    OriginTracerASGIMiddleware,
)

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE", "config.settings"
)

django.setup()
django_app = get_asgi_application()
application = OriginTracerASGIMiddleware(django_app)

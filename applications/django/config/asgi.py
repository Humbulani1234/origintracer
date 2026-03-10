"""
config/asgi.py

ASGI entry point for uvicorn and gunicorn+UvicornWorker.
This is what uvicorn calls for every request.
"""

import os
import django
from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

from stacktracer.probes.uvicorn_probe import StackTracerASGIMiddleware

django.setup()
django_app = get_asgi_application()
application = StackTracerASGIMiddleware(django_app)
"""Gunicorn lifecycle hooks for the Django OriginTracer demo."""

from probes.gunicorn_probe import install_hooks

install_hooks()

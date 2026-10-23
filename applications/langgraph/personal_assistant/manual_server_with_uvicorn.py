"""
HTTP-layer entrypoint for running langgraph enabling tracing
form the Uvicorn server - the complete request flow tracing.

This module wraps the FastAPI application with OriginTracer's ASGI middleware
so the HTTP request becomes the parent of the LangGraph callback
events::

    uvicorn.request.receive
        -> langgraph.chain/llm/tool events
    uvicorn.request.complete

Use this only after this application's ``probes/uvicorn_probe.py`` is
available. The OriginTracer configuration must also activate both probes, for
example::

    probes:
      - uvicorn
      - langgraph

Start it from the ``applications/langgraph`` directory with::

    uvicorn personal_assistant.manual_server_with_uvicorn:app \
      --host 127.0.0.1 --port 8124

Application-local probes are auto-discovered from
``applications/langgraph/probes/*_probe.py``. Because those files are loaded
as standalone modules, use absolute imports such as
``from origintracer.context.vars import set_trace`` rather than package-
relative imports.

The middleware must delegate ASGI lifespan messages to the wrapped FastAPI
application.  That preserves ``manual_server``'s OriginTracer initialization
and shutdown lifecycle.  If the uvicorn probe is not installed yet, the
ordinary ``personal_assistant.manual_server:app`` entrypoint remains the
supported fallback.
"""

from __future__ import annotations

from personal_assistant.manual_server import app as manual_app

try:
    # Running Uvicorn from the application directory makes ``probes``
    # importable as a namespace package. This is intentionally a user probe,
    # not part of the OriginTracer core package.
    from probes.uvicorn_probe import OriginTracerASGIMiddleware
except (
    ImportError
) as exc:  # pragma: no cover - optional user probe
    raise RuntimeError(
        "The application uvicorn probe is not available. Add "
        "probes/uvicorn_probe.py, or run "
        "personal_assistant.manual_server:app instead."
    ) from exc


# The middleware wraps HTTP calls while passing lifespan through to the
# existing FastAPI app, so its OriginTracer lifespan still initializes the
# engine and callback probe exactly as it does in the normal manual server.
app = OriginTracerASGIMiddleware(manual_app)

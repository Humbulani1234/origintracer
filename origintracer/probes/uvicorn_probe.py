"""
Observes uvicorn using ASGI middleware.

Approach:
    ASGI middleware wraps the application callable.
    This is the documented, stable ASGI extension point.
    uvicorn calls app(scope, receive, send) - we wrap app with our middleware
    so OriginTracerASGIMiddleware(scope, receive, send) is called instead.

    The user wraps their application once:
        # asgi.py
        from stacktracer.probes.uvicorn_probe import OriginTracerASGIMiddleware
        application = OriginTracerASGIMiddleware(get_asgi_application())

    What we observe in ASGI middleware:
        scope["type"] == "http"          HTTP request lifecycle
        scope["method"], scope["path"]   Request details
        scope["headers"]                 For X-Request-ID propagation
        The receive() callable           Request body stream
        The send() callable              Response headers + body

    What we observe via kprobe on sys_epoll_wait (from asyncio probe):
        The actual I/O wait inside the uvicorn event loop.
        uvicorn uses asyncio's event loop — the asyncio epoll kprobe
        already covers this. No separate uvicorn epoll probe needed.

ASGI scope types:
    "http"      HTTP request (what we care about)
    "websocket" WebSocket connection (skip for now)
    "lifespan"  Startup/shutdown (skip)

send() message types:
    "http.response.start"   Response headers, status code
    "http.response.body"    Response body bytes

X-Request-ID propagation:
    If nginx forwards X-Request-ID, it arrives in scope["headers"] as
    b"x-request-id". We use it as the trace_id so nginx and uvicorn
    events share the same trace automatically.

ProbeTypes:
    uvicorn.request.receive     ASGI scope ready, app call starting
    uvicorn.response.start      Response headers and status code sent
    uvicorn.response.body       Response body sent (final chunk)
    uvicorn.request.complete    Full request/response cycle complete
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any, Callable

from ..context.vars import (
    get_span_id,
    get_trace_id,
    reset_trace,
    set_trace,
)
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("origintracer.probes.uvicorn")

ProbeTypes.register_many(
    {
        "uvicorn.request.receive": "ASGI scope constructed, app call starting",
        "uvicorn.response.start": "HTTP response status and headers sent",
        "uvicorn.response.body": "HTTP response body sent",
        "uvicorn.request.complete": "Full ASGI request/response cycle complete",
    }
)

# -------------------- ASGI middleware (the primary observation point) --------


class OriginTracerASGIMiddleware:
    """
    Pure ASGI middleware that wraps the application callable.

    This is not monkey patching. ASGI middleware is the documented
    standard for wrapping ASGI applications.

    Usage — Django:
        # config/asgi.py
        from django.core.asgi import get_asgi_application
        from stacktracer.probes.uvicorn_probe import OriginTracerASGIMiddleware

        django_app  = get_asgi_application()
        application = OriginTracerASGIMiddleware(django_app)

    Usage — FastAPI:
        # main.py
        from fastapi import FastAPI
        from origintracer.probes.uvicorn_probe import OriginTracerASGIMiddleware

        app = FastAPI()
        app = OriginTracerASGIMiddleware(app)

    Then run with uvicorn or gunicorn+UvicornWorker as normal:
        uvicorn config.asgi:application
        gunicorn config.asgi:application -k uvicorn.workers.UvicornWorker
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(
        self, scope: dict, receive: Callable, send: Callable
    ) -> None:
        if scope["type"] == "lifespan":
            await self._handle_lifespan(scope, receive, send)
            return
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        await self._handle_http(scope, receive, send)

    async def _handle_http(
        self,
        scope: dict,
        receive: Callable,
        send: Callable,
    ) -> None:
        # ── Extract request info from ASGI scope ──
        headers = dict(scope.get("headers", []))

        # Prefer X-Request-ID from nginx if present
        request_id = (
            headers.get(b"x-request-id", b"")
            .decode("ascii", errors="replace")
            .strip()
        )

        # Fall back to whatever Django middleware already set in the ContextVar
        # This ensures uvicorn and django events share the same trace_id
        trace_id = (
            request_id or get_trace_id() or str(uuid.uuid4())
        )
        token = set_trace(trace_id)

        method = scope.get("method", "")
        path = scope.get("path", "/")
        client = scope.get(
            "client"
        )  # (host, port) tuple or None
        http_v = scope.get("http_version", "1.1")
        emit(
            NormalizedEvent.now(
                probe="uvicorn.request.receive",
                trace_id=trace_id,
                service="uvicorn",
                name=path,
                method=method,
                http_version=http_v,
                client=(
                    f"{client[0]}:{client[1]}"
                    if client
                    else None
                ),
                worker_pid=os.getpid(),
            )
        )

        t0 = time.perf_counter()
        status_code = 0
        response_size = 0

        # ── Wrap send() to capture response events ──
        # Wrapping the send callable is clean — we are not replacing any
        # internal method, just wrapping the callable passed to us by uvicorn.
        async def _capturing_send(message: dict) -> None:
            nonlocal status_code, response_size

            mtype = message.get("type", "")

            if mtype == "http.response.start":
                status_code = message.get("status", 0)
                emit(
                    NormalizedEvent.now(
                        probe="uvicorn.response.start",
                        trace_id=trace_id,
                        service="uvicorn",
                        name=path,
                        status_code=status_code,
                    )
                )

            elif mtype == "http.response.body":
                body = message.get("body", b"")
                response_size += len(body)
                more_body = message.get("more_body", False)

                if not more_body:
                    emit(
                        NormalizedEvent.now(
                            probe="uvicorn.response.body",
                            trace_id=trace_id,
                            service="uvicorn",
                            name=path,
                            response_bytes=response_size,
                        )
                    )

            await send(message)

        # ── Call the wrapped application ──
        try:
            await self.app(scope, receive, _capturing_send)
        finally:
            duration_ns = int((time.perf_counter() - t0) * 1e9)

            emit(
                NormalizedEvent.now(
                    probe="uvicorn.request.complete",
                    trace_id=trace_id,
                    service="uvicorn",
                    name=path,
                    method=method,
                    status_code=status_code,
                    response_bytes=response_size,
                    duration_ns=duration_ns,
                    worker_pid=os.getpid(),
                )
            )
            reset_trace(token)

    async def _handle_lifespan(
        self, scope, receive, send
    ) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})

            elif message["type"] == "lifespan.shutdown":
                # flush uploader before process dies
                import origintracer

                origintracer.shutdown()
                await send(
                    {"type": "lifespan.shutdown.complete"}
                )
                return


# ---------------- UvicornProbe --------------------------------------


class UvicornProbe(BaseProbe):
    """
    uvicorn probe via ASGI middleware.

    Unlike probes that self-install via class patching, this probe
    works by having the user wrap their ASGI application with
    StackTracerASGIMiddleware. This is the standard ASGI pattern.

    The UvicornProbe.start() emits a reminder if the middleware
    is not detected in the application.

    I/O visibility:
        uvicorn's event loop IS asyncio's event loop.
        The asyncio epoll kprobe (from AsyncioProbe) observes epoll_wait
        calls from uvicorn workers with the same fidelity as from a pure
        asyncio application. No separate uvicorn epoll probe is needed.

    Per-request timing:
        StackTracerASGIMiddleware captures start→end with status code.

    Combined with AsyncioProbe, you get:
        uvicorn.request.receive    → ASGI scope ready
        asyncio.loop.epoll_wait    → actual I/O waits inside the handler
        asyncio.loop.coro_call     → coroutine execution (sys.monitoring)
        uvicorn.response.start     → headers sent
        uvicorn.request.complete   → full cycle with duration
    """

    name = "uvicorn"

    def start(self) -> None:

        import pdb

        pdb.set_trace()

        try:
            import uvicorn  # noqa: F401
        except ImportError:
            logger.info(
                "uvicorn not installed — uvicorn probe inactive"
            )
            return

        logger.info(
            "uvicorn probe: wrap your ASGI app with OriginTracerASGIMiddleware.\n"
            "Example (asgi.py):\n"
            "    from stacktracer.probes.uvicorn_probe import OriginTracerASGIMiddleware\n"
            "    application = OriginTracerASGIMiddleware(get_asgi_application())\n"
            "This is standard ASGI middleware - no internal patching."
        )

    def stop(self) -> None:
        # ASGI middleware is part of the application - it stops
        # when the application stops.
        pass

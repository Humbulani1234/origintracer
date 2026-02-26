"""
probes/django_probe.py

Django probe: instruments the HTTP request lifecycle via Django middleware.
Also patches URL resolver and view dispatch for finer-grained stage tracking.

Install in Django settings:
    MIDDLEWARE = [
        'stacktracer.probes.django_probe.TracerMiddleware',
        'django.middleware.security.SecurityMiddleware',
        ...
    ]

    import stacktracer
    stacktracer.init(api_key=..., sample_rate=0.05)

The middleware:
  1. Makes the sampling decision
  2. Generates a trace_id
  3. Sets it in the ContextVar
  4. Emits request.entry and request.exit probes
  5. Resets the ContextVar (even on exception)
"""

from __future__ import annotations

import random
import uuid
import logging
from typing import Any, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent
from ..context.vars import set_trace, reset_trace, get_trace_id

logger = logging.getLogger("stacktracer.probes.django")

# ------------------------------------------------------------------ #
# Middleware (framework-level probe)
# ------------------------------------------------------------------ #

def _make_middleware(get_response: Any, sample_rate: float) -> Any:
    """
    Factory that returns the right middleware function
    (sync or async) based on what Django is calling us with.
    """
    import asyncio
    from django.utils.decorators import sync_and_async_middleware

    @sync_and_async_middleware
    def middleware(get_response_inner: Any) -> Any:

        if asyncio.iscoroutinefunction(get_response_inner):

            async def async_middleware(request: Any) -> Any:
                if random.random() > sample_rate:
                    return await get_response_inner(request)

                trace_id = str(uuid.uuid4())
                token = set_trace(trace_id)

                emit(NormalizedEvent.now(
                    probe="request.entry",
                    trace_id=trace_id,
                    service="django",
                    name=request.path,
                    method=request.method,
                    path=request.path,
                    query=request.META.get("QUERY_STRING", "")[:200],
                    remote_addr=request.META.get("REMOTE_ADDR"),
                ))

                emit(NormalizedEvent.now(
                    probe="django.middleware.enter",
                    trace_id=trace_id,
                    service="django",
                    name="middleware_chain",
                ))

                response = None
                try:
                    response = await get_response_inner(request)
                    return response
                except Exception as exc:
                    emit(NormalizedEvent.now(
                        probe="function.exception",
                        trace_id=trace_id,
                        service="django",
                        name=request.path,
                        exception_type=type(exc).__name__,
                        exception_msg=str(exc)[:200],
                    ))
                    raise
                finally:
                    status = getattr(response, "status_code", None)
                    emit(NormalizedEvent.now(
                        probe="request.exit",
                        trace_id=trace_id,
                        service="django",
                        name=request.path,
                        status_code=status,
                    ))
                    reset_trace(token)

            return async_middleware

        else:
            def sync_middleware(request: Any) -> Any:
                if random.random() > sample_rate:
                    return get_response_inner(request)

                trace_id = str(uuid.uuid4())
                token = set_trace(trace_id)

                emit(NormalizedEvent.now(
                    probe="request.entry",
                    trace_id=trace_id,
                    service="django",
                    name=request.path,
                    method=request.method,
                    path=request.path,
                ))

                response = None
                try:
                    response = get_response_inner(request)
                    return response
                finally:
                    status = getattr(response, "status_code", None)
                    emit(NormalizedEvent.now(
                        probe="request.exit",
                        trace_id=trace_id,
                        service="django",
                        name=request.path,
                        status_code=status,
                    ))
                    reset_trace(token)

            return sync_middleware

    return middleware(get_response)


class TracerMiddlewareWrapper:
    """
    Django-style middleware class.
    Use this in settings.MIDDLEWARE:
        'stacktracer.probes.django_probe.TracerMiddlewareWrapper'

    Sample rate is read from stacktracer config if available.
    """

    def __init__(self, get_response: Any) -> None:
        try:
            from .._init import get_config
            sample_rate = get_config().sample_rate
        except Exception:
            sample_rate = 1.0

        self._mw = _make_middleware(get_response, sample_rate)

    def __call__(self, request: Any) -> Any:
        return self._mw(request)


# Alias for cleaner settings.py
TracerMiddleware = TracerMiddlewareWrapper


# ------------------------------------------------------------------ #
# Django probe (registers the middleware via monkey-patch if needed)
# ------------------------------------------------------------------ #

class DjangoProbe(BaseProbe):
    name = "django"

    def start(self) -> None:
        """
        Patch Django's URL resolver to emit django.url.resolve events,
        and patch BaseHandler.get_response to emit django.view.enter events.
        """
        try:
            self._patch_url_resolver()
            self._patch_view_dispatch()
            logger.info("Django probe installed")
        except Exception as exc:
            logger.warning("Django probe could not install all hooks: %s", exc)

    def stop(self) -> None:
        # Restore patched methods
        self._unpatch_url_resolver()
        self._unpatch_view_dispatch()
        logger.info("Django probe removed")

    def _patch_url_resolver(self) -> None:
        from django.urls import resolvers
        self._orig_resolve = resolvers.URLResolver.resolve

        def _traced_resolve(self_r: Any, path: str) -> Any:
            trace_id = get_trace_id()
            result = self._orig_resolve(self_r, path)
            if trace_id and result:
                emit(NormalizedEvent.now(
                    probe="django.url.resolve",
                    trace_id=trace_id,
                    service="django",
                    name=path,
                    view_name=str(result.url_name or result.func),
                ))
            return result

        resolvers.URLResolver.resolve = _traced_resolve

    def _unpatch_url_resolver(self) -> None:
        try:
            from django.urls import resolvers
            if hasattr(self, "_orig_resolve"):
                resolvers.URLResolver.resolve = self._orig_resolve
        except Exception:
            pass

    def _patch_view_dispatch(self) -> None:
        try:
            from django.core.handlers.base import BaseHandler
            self._orig_get_response = BaseHandler._get_response

            async def _traced_get_response(self_h: Any, request: Any) -> Any:
                trace_id = get_trace_id()
                if trace_id:
                    emit(NormalizedEvent.now(
                        probe="django.view.enter",
                        trace_id=trace_id,
                        service="django",
                        name=request.path,
                    ))
                return await self._orig_get_response(self_h, request)

            BaseHandler._get_response = _traced_get_response
        except Exception as exc:
            logger.debug("Could not patch view dispatch: %s", exc)

    def _unpatch_view_dispatch(self) -> None:
        try:
            from django.core.handlers.base import BaseHandler
            if hasattr(self, "_orig_get_response"):
                BaseHandler._get_response = self._orig_get_response
        except Exception:
            pass
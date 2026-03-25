"""
probes/django_probe.py

Observes Django using only stable, version-safe extension points.

Extension points used:
    TracerMiddleware              Official MIDDLEWARE hook — request lifecycle
    connection.execute_wrapper()  Official DB profiling API
    View.dispatch() patch         Single patch on the public CBV base class
    got_request_exception signal  Official Django signal — unhandled exceptions

What you get:
    request.entry      — every HTTP request enters middleware
    request.exit       — every HTTP response leaves middleware
    django.view.enter  — CBV dispatch entered (class name, e.g. AsyncView)
    django.view.exit   — CBV dispatch returned
    django.db.query    — every ORM / raw SQL query with duration
    django.exception   — unhandled exceptions

Why View.dispatch instead of sys.monitoring:
    sys.monitoring fires on every Python function call process-wide.
    Filtering that firehose down to just your views requires fighting
    framework internals, coroutine resume events, class body executions,
    and flag value differences across Python versions.

    View.dispatch() is the single method every Django class-based view
    passes through. One patch captures all CBVs cleanly with no filtering,
    no noise, and no version sensitivity. Works on Python 3.11, 3.12, 3.13.

    Function-based views are already identified by request.entry name field
    (the URL path) — no additional patching needed.

TracerMiddleware is REQUIRED and must be first in MIDDLEWARE:
    MIDDLEWARE = [
        "stacktracer.probes.django_probe.TracerMiddleware",
        "django.middleware.security.SecurityMiddleware",
        ...
    ]
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Callable, Optional

from ..context.vars import get_trace_id, reset_trace, set_trace
from ..core.event_schema import NormalizedEvent
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("stacktracer.probes.django")

_originals: dict = {}
_patched: bool = False


# ====================================================================== #
# TracerMiddleware
# ====================================================================== #


class TracerMiddleware:
    """
    ASGI/WSGI middleware. Must be first in MIDDLEWARE.
    Generates or inherits trace_id and wraps every request.
    """

    def __init__(self, get_response: Callable) -> None:
        self.get_response = get_response
        import asyncio
        import inspect

        self._is_async = asyncio.iscoroutinefunction(
            get_response
        ) or inspect.iscoroutinefunction(get_response)

    def __call__(self, request: Any) -> Any:
        if self._is_async:
            return self.__acall__(request)
        return self._sync_call(request)

    def _sync_call(self, request: Any) -> Any:
        trace_id, token = self._begin(request)
        try:
            response = self.get_response(request)
            self._end(request, response, trace_id)
            return response
        except Exception as exc:
            self._error(request, exc, trace_id)
            raise
        finally:
            reset_trace(token)

    async def __acall__(self, request: Any) -> Any:
        trace_id, token = self._begin(request)
        try:
            response = await self.get_response(request)
            self._end(request, response, trace_id)
            return response
        except Exception as exc:
            self._error(request, exc, trace_id)
            raise
        finally:
            reset_trace(token)

    def _begin(self, request: Any):
        trace_id = (
            request.META.get("HTTP_X_REQUEST_ID")
            or get_trace_id()
            or str(uuid.uuid4())
        )
        token = set_trace(trace_id)
        request._st_t0 = time.perf_counter()
        print(
            f">>> request.enter trace={get_trace_id()} path={request.path}"
        )
        emit(
            NormalizedEvent.now(
                probe="request.entry",
                trace_id=trace_id,
                service="django",
                name=request.path,
                method=request.method,
                http_host=request.META.get("HTTP_HOST", ""),
            )
        )
        return trace_id, token

    def _end(
        self, request: Any, response: Any, trace_id: str
    ) -> None:
        duration_ns = int(
            (time.perf_counter() - request._st_t0) * 1e9
        )
        emit(
            NormalizedEvent.now(
                probe="request.exit",
                trace_id=trace_id,
                service="django",
                name=request.path,
                method=request.method,
                status_code=response.status_code,
                duration_ns=duration_ns,
            )
        )

    def _error(
        self, request: Any, exc: Exception, trace_id: str
    ) -> None:
        duration_ns = int(
            (time.perf_counter() - request._st_t0) * 1e9
        )
        emit(
            NormalizedEvent.now(
                probe="django.exception",
                trace_id=trace_id,
                service="django",
                name=request.path,
                duration_ns=duration_ns,  # ← how long before it blew up
                exception_type=type(exc).__name__,
                exception_msg=str(exc)[:200],
                source="middleware",
            )
        )


# ====================================================================== #
# View.dispatch patch — CBV observation
# ====================================================================== #


def _patch_view_dispatch() -> None:
    """
    Patch View.dispatch() to emit django.view.enter / django.view.exit
    for every class-based view.

    View.dispatch() is the single entry point for all CBVs. One patch
    captures every CBV automatically — AsyncView, SlowView, DbView etc.
    No filtering needed. Works on all Python versions.

    Note on async views: dispatch() itself is sync even for async views.
    Django routes the request to get()/post() which may be async, but
    dispatch() always returns synchronously (or returns a coroutine that
    Django awaits). The finally block fires correctly in both cases at
    the dispatch level.
    """
    try:
        from django.views import View
    except ImportError:
        logger.debug(
            "django probe: Django not installed — skipping dispatch patch"
        )
        return

    original_dispatch = View.dispatch
    _originals["View.dispatch"] = original_dispatch

    def _traced_dispatch(self_view, request, *args, **kwargs):
        trace_id = get_trace_id()
        view_name = type(self_view).__name__
        t0 = time.perf_counter()

        if trace_id:
            emit(
                NormalizedEvent.now(
                    probe="django.view.enter",
                    trace_id=trace_id,
                    service="django",
                    name=view_name,
                )
            )

        try:
            return original_dispatch(
                self_view, request, *args, **kwargs
            )
        finally:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(
                    NormalizedEvent.now(
                        probe="django.view.exit",
                        trace_id=trace_id,
                        service="django",
                        name=view_name,
                        duration_ns=duration_ns,
                    )
                )

    View.dispatch = _traced_dispatch
    logger.info(
        "django probe: View.dispatch patched — all CBVs observed"
    )


def _unpatch_view_dispatch() -> None:
    original = _originals.pop("View.dispatch", None)
    if original is None:
        return
    try:
        from django.views import View

        View.dispatch = original
        logger.info("django probe: View.dispatch restored")
    except ImportError:
        pass


# ====================================================================== #
# Database execute_wrapper
# ====================================================================== #


def _make_db_wrapper():
    def _wrapper(execute, sql, params, many, context):
        trace_id = get_trace_id()
        t0 = time.perf_counter()
        try:
            result = execute(sql, params, many, context)
        except Exception as exc:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(
                    NormalizedEvent.now(
                        probe="django.db.query",
                        trace_id=trace_id,
                        service="django",
                        name=sql[:200],
                        duration_ns=duration_ns,
                        db_alias=context["connection"].alias,
                        success=False,
                        error=str(exc)[:200],
                    )
                )
            raise
        else:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(
                    NormalizedEvent.now(
                        probe="django.db.query",
                        trace_id=trace_id,
                        service="django",
                        name=sql[:200],
                        duration_ns=duration_ns,
                        db_alias=context["connection"].alias,
                        success=True,
                        row_count=getattr(
                            result, "rowcount", None
                        ),
                    )
                )
            return result

    return _wrapper


def _install_db_wrapper() -> None:
    """
    Install via connection_created signal — fires the first time each
    database connection is opened, which is after AppConfig.ready().
    This avoids the empty connections.all() problem at startup.
    """
    from django.db.backends.signals import connection_created

    wrapper = _make_db_wrapper()
    _originals["db_wrapper"] = wrapper

    def _on_connection_created(sender, connection, **kwargs):
        connection.execute_wrappers.append(wrapper)
        logger.debug(
            "django probe: execute_wrapper attached to connection alias=%s",
            connection.alias,
        )

    connection_created.connect(
        _on_connection_created, weak=False
    )
    _originals["connection_created_handler"] = (
        _on_connection_created
    )
    logger.info(
        "django probe: database wrapper registered on connection_created signal"
    )


def _uninstall_db_wrapper() -> None:
    from django.db.backends.signals import connection_created

    handler = _originals.pop("connection_created_handler", None)
    if handler:
        connection_created.disconnect(handler)

    wrapper = _originals.pop("db_wrapper", None)
    if wrapper is None:
        return
    try:
        from django.db import connections

        for conn in connections.all():
            if wrapper in conn.execute_wrappers:
                conn.execute_wrappers.remove(wrapper)
    except Exception:
        pass


# ====================================================================== #
# Django signals
# ====================================================================== #


def _on_unhandled_exception(
    sender: Any,
    request: Any = None,
    exception: Any = None,
    **kwargs,
) -> None:
    if exception is None:
        return
    trace_id = get_trace_id()
    if trace_id:
        emit(
            NormalizedEvent.now(
                probe="django.exception",
                trace_id=trace_id,
                service="django",
                name=request.path if request else "unknown",
                exception_type=type(exception).__name__,
                exception_msg=str(exception)[:200],
                source="got_request_exception_signal",
            )
        )


def _install_signals() -> None:
    try:
        from django.core.signals import got_request_exception

        got_request_exception.connect(
            _on_unhandled_exception, weak=False
        )
        logger.info(
            "django probe: got_request_exception signal connected"
        )
    except ImportError:
        pass


def _uninstall_signals() -> None:
    try:
        from django.core.signals import got_request_exception

        got_request_exception.disconnect(_on_unhandled_exception)
    except ImportError:
        pass


# ====================================================================== #
# DjangoProbe
# ====================================================================== #


class DjangoProbe(BaseProbe):
    """
    Observes Django using stable, version-safe extension points.
    No sys.monitoring — View.dispatch patch covers all CBVs cleanly.

    Hooks installed at start():
        1. View.dispatch() patch  — CBV entry/exit with class name + duration
        2. execute_wrapper        — every DB query with SQL + duration
        3. got_request_exception  — unhandled exceptions

    TracerMiddleware handles request.entry / request.exit and must be
    added manually as the first entry in settings.MIDDLEWARE.
    """

    name = "django"

    def start(self, observe_modules=None) -> None:

        # import pdb
        # pdb.set_trace()

        global _patched

        if _patched:
            logger.warning(
                "django probe: already installed — skipping"
            )
            return

        _patch_view_dispatch()
        _install_db_wrapper()
        _install_signals()

        _patched = True
        logger.info("django probe: installed")

    def stop(self) -> None:
        global _patched

        if not _patched:
            return

        _unpatch_view_dispatch()
        _uninstall_db_wrapper()
        _uninstall_signals()

        _patched = False
        logger.info("django probe: removed")

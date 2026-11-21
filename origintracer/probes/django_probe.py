"""
Observes Django using stable, version-safe extension points.

Extension points used:
    TracerMiddleware - MIDDLEWARE hook - request lifecycle
    connection.execute_wrapper() - DB profiling API
    View.dispatch() patch - Single patch on the public CBV base class
    got_request_exception signal - Django signal - unhandled exceptions

Probes:
    request.entry - every HTTP request enters middleware
    request.exit - every HTTP response leaves middleware
    django.view.enter - CBV dispatch entered (class name, e.g. AsyncView)
    django.view.exit - CBV dispatch returned
    django.db.query - every ORM/raw SQL query with duration
    django.exception - unhandled exceptions

TracerMiddleware is REQUIRED and must be first in MIDDLEWARE:
    MIDDLEWARE = [
        "origintracer.probes.django_probe.TracerMiddleware",
        "django.middleware.security.SecurityMiddleware",
        ...
    ]
"""

from __future__ import annotations

import asyncio
import ctypes
import inspect
import logging
import os
import time
import uuid
from typing import Any, Callable, Optional

import origintracer
from origintracer.core.kprobe_bridge import get_bridge

from ..context.vars import (
    get_span_id,
    get_trace_id,
    reset_span_id,
    reset_trace,
    set_span_id,
    set_trace,
)
from ..core.event_schema import NormalizedEvent
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("origintracer.probes.django")


class TracerMiddleware:
    """
    ASGI/WSGI middleware. Must be first in MIDDLEWARE.
    Generates or inherits trace_id and wraps every request.

    Request entry and exit events share a span ID. Each class-based view gets
    a child span whose context remains active through asynchronous dispatch;
    database query events use the active span as their parent when available.
    """

    def __init__(self, get_response: Callable) -> None:
        self.get_response = get_response
        self._is_async = asyncio.iscoroutinefunction(
            get_response
        ) or inspect.iscoroutinefunction(
            get_response
        )  # handles edge cases

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
            reset_span_id(request._ot_span_token)

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
            reset_span_id(request._ot_span_token)

    def _begin(self, request: Any):
        trace_id = (
            request.META.get("HTTP_X_REQUEST_ID")
            or get_trace_id()
            or str(uuid.uuid4())
        )
        token = set_trace(trace_id)
        request._ot_parent_span_id = get_span_id()
        request._ot_t0 = time.perf_counter()
        request._ot_span_id = uuid.uuid4().hex
        request._ot_span_token = set_span_id(request._ot_span_id)
        engine = origintracer.get_engine()
        pattern = engine.tracker._normalize_path(request.path)
        # start the ActiveRequest tracker
        if engine:
            engine.tracker.start(
                trace_id=trace_id,
                service="django",
                pattern=pattern,
            )
        # Bridge kernel events to this request: BPF looks up Linux native TID,
        # not Python's threading.get_ident() value. This libc syscall number is
        # Linux x86_64-specific; other architectures need their own gettid path.
        _libc = ctypes.CDLL("libc.so.6", use_errno=True)
        _SYS_gettid = 186  # x86_64 Linux

        def _get_os_tid() -> int:
            return _libc.syscall(_SYS_gettid)

        bridge = get_bridge()
        if bridge.available:
            handler_tid = _get_os_tid()
            # On Linux, the process ID is also the main thread's TID. Register
            # it for event-loop syscalls and the native TID of this middleware
            # thread. The bridge key is TID; PID is stored separately. Work later
            # offloaded to another thread is not covered by these two mappings.
            loop_tid = os.getpid()
            bridge.register_trace(
                tid=loop_tid,
                trace_id=trace_id,
                service="django",
                start_ns=int(time.time_ns()),
                pid=os.getpid(),
            )
            # also register handler thread
            bridge.register_trace(
                tid=handler_tid,
                trace_id=trace_id,
                service="django",
                start_ns=int(time.time_ns()),
                pid=os.getpid(),
            )
            # Keep the exact keys so _end can remove both mappings; stale map
            # entries could otherwise attribute later syscalls to this trace.
            request._ot_bridge_tids = [loop_tid, handler_tid]
        entry_event = NormalizedEvent.now(
            probe="request.entry",
            trace_id=trace_id,
            service="django",
            name=request.path,
            parent_span_id=request._ot_parent_span_id,
            method=request.method,
            http_host=request.META.get("HTTP_HOST", ""),
        )
        entry_event.span_id = request._ot_span_id
        emit(entry_event)
        return trace_id, token

    def _end(
        self, request: Any, response: Any, trace_id: str
    ) -> None:

        duration_ns = int(
            (time.perf_counter() - request._ot_t0) * 1e9
        )
        exit_event = NormalizedEvent.now(
            probe="request.exit",
            trace_id=trace_id,
            service="django",
            name=request.path,
            parent_span_id=request._ot_parent_span_id,
            method=request.method,
            status_code=response.status_code,
            duration_ns=duration_ns,
        )
        exit_event.span_id = request._ot_span_id
        emit(exit_event)
        engine = origintracer.get_engine()
        pattern = engine.tracker._normalize_path(request.path)
        if engine:
            span = engine.tracker.complete(trace_id)
            if span and span.duration_ms:
                logger.debug(
                    "request complete trace_id=%s pattern=%s duration_ms=%.1f",
                    trace_id,
                    pattern,
                    span.duration_ms,
                )
        # Remove the per-request BPF mappings registered in _begin. This bounds
        # the context to the request lifetime and prevents stale attribution.
        bridge = get_bridge()
        if bridge.available:
            for tid in getattr(request, "_ot_bridge_tids", []):
                bridge.unregister_trace(tid)

    def _error(
        self, request: Any, exc: Exception, trace_id: str
    ) -> None:
        duration_ns = int(
            (time.perf_counter() - request._ot_t0) * 1e9
        )  # how long before exception
        error_event = NormalizedEvent.now(
            probe="django.exception",
            trace_id=trace_id,
            service="django",
            name=request.path,
            duration_ns=duration_ns,
            exception_type=type(exc).__name__,
            exception_msg=str(exc)[:200],
            source="middleware",
        )
        error_event.span_id = request._ot_span_id
        emit(error_event)
        engine = origintracer.get_engine()
        if engine:
            engine.tracker.fail(
                trace_id, exception_type=type(exc).__name__
            )


# ------------- View.dispatch: CBV observation ----------

_originals: dict = {}
_patched: bool = False


def _instrument_view_dispatch() -> None:
    """
    View.dispatch() emits django.view.enter/django.view.exit
    for every class-based view.

    View.dispatch() is the single entry point for all CBVs. One patch
    captures every CBV automatically - AsyncView, SlowView, DbView etc.

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
            "django probe: Django not installed - skipping dispatch patch"
        )
        return

    original_dispatch = View.dispatch
    _originals["View.dispatch"] = original_dispatch

    def _traced_dispatch(self_view, request, *args, **kwargs):
        trace_id = get_trace_id()
        if not trace_id:
            return original_dispatch(
                self_view, request, *args, **kwargs
            )
        view_name = type(self_view).__name__
        parent_span_id = get_span_id()
        span_id = uuid.uuid4().hex
        span_token = set_span_id(span_id)
        started = time.perf_counter()
        enter = NormalizedEvent.now(
            probe="django.view.enter",
            trace_id=trace_id,
            service="django",
            name=view_name,
            parent_span_id=parent_span_id,
        )
        enter.span_id = span_id
        emit(enter)

        def finish() -> None:
            duration_ns = int(
                (time.perf_counter() - started) * 1e9
            )
            leave = NormalizedEvent.now(
                probe="django.view.exit",
                trace_id=trace_id,
                service="django",
                name=view_name,
                parent_span_id=parent_span_id,
                duration_ns=duration_ns,
            )
            leave.span_id = span_id
            emit(leave)
            reset_span_id(span_token)

        try:
            result = original_dispatch(
                self_view, request, *args, **kwargs
            )
        except BaseException:
            finish()
            raise
        if inspect.isawaitable(result):

            async def await_dispatch():
                try:
                    return await result
                finally:
                    finish()

            return await_dispatch()
        finish()
        return result

    View.dispatch = _traced_dispatch
    logger.info(
        "django probe: View.dispatch patched - all CBVs observed"
    )


def _uninstrument_view_dispatch() -> None:
    original = _originals.pop("View.dispatch", None)
    if original is None:
        return
    try:
        from django.views import View

        View.dispatch = original
        logger.info("django probe: View.dispatch restored")
    except ImportError:
        pass


# ----------------- Database execute_wrapper ------------------


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
                        parent_span_id=get_span_id(),
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
                        parent_span_id=get_span_id(),
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
    Install via connection_created signal - fires the first time each
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


# -------------------------- Django signals ----------------------


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


# ------------------------- DjangoProbe --------------------------------


class DjangoProbe(BaseProbe):
    """
    Observes Django using stable, version-safe extension points.

    Hooks installed at start():
        1. View.dispatch() patch - CBV entry/exit with class name + duration
        2. execute_wrapper - every DB query with SQL + duration
        3. got_request_exception - unhandled exceptions

    TracerMiddleware handles request.entry/request.exit and must be
    added manually as the first entry in settings.MIDDLEWARE.
    """

    name = "django"

    def start(self) -> None:

        global _patched

        if _patched:
            logger.warning(
                "django probe: already installed - skipping"
            )
            return

        _instrument_view_dispatch()
        _install_db_wrapper()
        _install_signals()

        _patched = True
        logger.info("django probe: installed")

    def stop(self) -> None:
        global _patched

        if not _patched:
            return

        _uninstrument_view_dispatch()
        _uninstall_db_wrapper()
        _uninstall_signals()

        _patched = False
        logger.info("django probe: removed")

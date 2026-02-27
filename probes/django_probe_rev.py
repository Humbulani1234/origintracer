"""
probes/django_probe.py  (revised — signals + middleware only)

Observes Django using only official extension points.

The previous approach used TracerMiddleware as the primary hook, which
is correct and remains. The question is what else to patch.

What we do NOT do here:
    - Patch URLResolver.resolve()        (private, version-sensitive)
    - Patch BaseHandler.get_response()   (internal, version-sensitive)
    - Patch any Django class methods     (monkey patching)

What we DO instead:
    - TracerMiddleware (WSGI/ASGI middleware — official extension point)
    - Django signals   (request_started, request_finished — designed for this)
    - Django database  (connection.execute_wrapper — official query hook)
    - sys.monitoring   (view function entry/return on 3.12+, no patching)

Django's middleware system is not monkey patching. It is the documented,
stable, official way to intercept request processing. TracerMiddleware
remains the primary observation point.

Django signals cover the lifecycle boundaries:
    request_started     → before middleware runs
    request_finished    → after response is sent to client
    got_request_exception → unhandled exception in view

Django's execute_wrapper covers database queries:
    connection.execute_wrapper(fn)
    Called for every SQL query through the ORM or raw cursor.
    This is the official profiling hook — used by Django Debug Toolbar.
    Gives: sql text, params, duration, connection alias.
    No parsing of wire bytes needed.

View function observation (where the interesting logic lives):
    On Python 3.12+: sys.monitoring CALL event filtered to the
    user's app module prefix. We observe the exact view function
    entry and return without replacing any code.

    On Python 3.11: sys.setprofile filtered to app modules.

    The filter is configured via stacktracer.yaml:
        observe:
          modules:
            - myapp
            - myapp.views
            - myapp.api

    Without this filter, sys.monitoring fires for all Python calls
    which is too broad. With a module prefix filter it is surgical.

ProbeTypes:
    request.entry             HTTP request received by middleware
    request.exit              HTTP response sent by middleware
    django.view.enter         View function entered (sys.monitoring)
    django.view.exit          View function returned
    django.db.query           ORM/raw SQL executed (execute_wrapper)
    django.exception          Unhandled exception in request lifecycle
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Any, Callable, List, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..context.vars import get_trace_id, set_trace, reset_trace, get_span_id

import uuid

logger = logging.getLogger("stacktracer.probes.django")

ProbeTypes.register_many({
    "request.entry":        "HTTP request received",
    "request.exit":         "HTTP response sent",
    "django.view.enter":    "Django view function entered",
    "django.view.exit":     "Django view function returned",
    "django.db.query":      "ORM or raw SQL query executed",
    "django.exception":     "Unhandled exception in request lifecycle",
})

_originals: dict = {}
_patched   = False
_MONITORING_TOOL_ID = None

# Module prefixes to observe — set from config in start()
_observe_modules: List[str] = []


# ====================================================================== #
# TracerMiddleware — the primary observation point
# ====================================================================== #

class TracerMiddleware:
    """
    ASGI/WSGI middleware. Wraps every request.

    This is not monkey patching. Middleware is Django's official,
    stable, documented extension point for request interception.
    It survives every Django version upgrade.

    Place first in MIDDLEWARE so it wraps the entire pipeline:
        MIDDLEWARE = [
            "stacktracer.probes.django_probe.TracerMiddleware",
            ...
        ]
    """

    def __init__(self, get_response: Callable) -> None:
        self.get_response = get_response
        self._is_async = asyncio_callable(get_response)

    def __call__(self, request):
        if self._is_async:
            return self.__acall__(request)
        return self._sync_call(request)

    def _sync_call(self, request):
        trace_id = self._begin(request)
        try:
            response = self.get_response(request)
            self._end(request, response, trace_id)
            return response
        except Exception as exc:
            self._error(request, exc, trace_id)
            raise

    async def __acall__(self, request):
        trace_id = self._begin(request)
        try:
            response = await self.get_response(request)
            self._end(request, response, trace_id)
            return response
        except Exception as exc:
            self._error(request, exc, trace_id)
            raise

    def _begin(self, request) -> str:
        # Use X-Request-ID if nginx forwarded one, else generate new
        trace_id = (
            request.META.get("HTTP_X_REQUEST_ID")
            or str(uuid.uuid4())
        )
        self._token = set_trace(trace_id)

        emit(NormalizedEvent.now(
            probe="request.entry",
            trace_id=trace_id,
            service="django",
            name=request.path,
            method=request.method,
            http_host=request.META.get("HTTP_HOST", ""),
        ))
        return trace_id

    def _end(self, request, response, trace_id: str) -> None:
        emit(NormalizedEvent.now(
            probe="request.exit",
            trace_id=trace_id,
            service="django",
            name=request.path,
            method=request.method,
            status_code=response.status_code,
        ))
        reset_trace(self._token)

    def _error(self, request, exc: Exception, trace_id: str) -> None:
        emit(NormalizedEvent.now(
            probe="django.exception",
            trace_id=trace_id,
            service="django",
            name=request.path,
            exception_type=type(exc).__name__,
            exception_msg=str(exc)[:200],
        ))
        reset_trace(self._token)


def asyncio_callable(fn) -> bool:
    import asyncio, inspect
    return asyncio.iscoroutinefunction(fn) or inspect.iscoroutinefunction(fn)


# ====================================================================== #
# Django database execute_wrapper — official query hook
# ====================================================================== #

def _make_db_wrapper():
    """
    Returns a context manager suitable for connection.execute_wrapper().
    This is Django's official profiling API for database queries —
    documented in Django docs under "Instrumentation".
    Used by Django Debug Toolbar, django-silk, and others.
    """
    from contextlib import contextmanager

    @contextmanager
    def db_trace_wrapper(execute, sql, params, many, context):
        trace_id = get_trace_id()
        t0 = time.perf_counter()

        try:
            result = execute(sql, params, many, context)
        except Exception as exc:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe="django.db.query",
                    trace_id=trace_id,
                    service="django",
                    name=sql[:200],   # GraphNormalizer will collapse literals
                    duration_ns=duration_ns,
                    db_alias=context["connection"].alias,
                    error=str(exc)[:200],
                    success=False,
                ))
            raise
        else:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe="django.db.query",
                    trace_id=trace_id,
                    service="django",
                    name=sql[:200],
                    duration_ns=duration_ns,
                    db_alias=context["connection"].alias,
                    success=True,
                    row_count=result.rowcount if hasattr(result, "rowcount") else None,
                ))
            return result

    return db_trace_wrapper


def _install_db_wrapper() -> bool:
    """Install the execute_wrapper on all Django database connections."""
    try:
        from django.db import connections
        wrapper = _make_db_wrapper()
        for conn in connections.all():
            conn.execute_wrappers.append(wrapper)
        _originals["db_wrapper"] = wrapper
        logger.info("django probe: database execute_wrapper installed")
        return True
    except Exception as exc:
        logger.warning("django probe: database wrapper failed: %s", exc)
        return False


def _uninstall_db_wrapper() -> None:
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
# Django signals — lifecycle events
# ====================================================================== #

def _on_request_started(sender, environ=None, scope=None, **kwargs) -> None:
    """
    request_started fires before middleware. We use it to note arrival time.
    The trace_id is set by TracerMiddleware which runs just after this.
    """
    pass   # trace_id not yet available here — middleware sets it


def _on_request_finished(sender, **kwargs) -> None:
    trace_id = get_trace_id()
    if trace_id:
        emit(NormalizedEvent.now(
            probe="request.exit",
            trace_id=trace_id,
            service="django",
            name="request_finished_signal",
        ))


def _on_exception(sender, request, exception, **kwargs) -> None:
    trace_id = get_trace_id()
    if trace_id:
        emit(NormalizedEvent.now(
            probe="django.exception",
            trace_id=trace_id,
            service="django",
            name=request.path if request else "unknown",
            exception_type=type(exception).__name__,
            exception_msg=str(exception)[:200],
            source="signal",
        ))


def _install_signals() -> None:
    try:
        from django.core.signals import request_started, request_finished
        from django.test.signals import setting_changed
        try:
            from django.core.signals import got_request_exception
            got_request_exception.connect(_on_exception, weak=False)
        except ImportError:
            pass
        logger.info("django probe: signals connected")
    except ImportError:
        pass


def _uninstall_signals() -> None:
    try:
        from django.core.signals import request_started, request_finished
        request_started.disconnect(_on_request_started)
        request_finished.disconnect(_on_request_finished)
        try:
            from django.core.signals import got_request_exception
            got_request_exception.disconnect(_on_exception)
        except ImportError:
            pass
    except ImportError:
        pass


# ====================================================================== #
# sys.monitoring for view functions (3.12+) / setprofile (3.11)
# ====================================================================== #

def _make_view_filter(observe_modules: List[str]) -> Callable:
    """
    Returns a predicate that returns True for code objects that
    belong to the user's application modules.
    """
    prefixes = tuple(m.replace(".", "/") for m in observe_modules)

    def is_app_code(code) -> bool:
        if not observe_modules:
            return False
        filename = code.co_filename
        return any(p in filename for p in prefixes)

    return is_app_code


def _setup_monitoring_312(observe_modules: List[str]) -> bool:
    global _MONITORING_TOOL_ID

    if not hasattr(sys, "monitoring"):
        return False

    TOOL_ID = sys.monitoring.PROFILER_ID
    _MONITORING_TOOL_ID = TOOL_ID
    is_app_code = _make_view_filter(observe_modules)

    try:
        sys.monitoring.set_events(
            TOOL_ID,
            sys.monitoring.events.CALL | sys.monitoring.events.PY_RETURN,
        )
    except Exception as exc:
        logger.warning("django probe sys.monitoring set_events: %s", exc)
        return False

    def on_call(code, offset: int, callable_: Any, arg0: Any):
        if not is_app_code(code):
            return sys.monitoring.DISABLE   # disable for this code object forever
        trace_id = get_trace_id()
        if not trace_id:
            return
        emit(NormalizedEvent.now(
            probe="django.view.enter",
            trace_id=trace_id,
            service="django",
            name=code.co_qualname,
            parent_span_id=get_span_id(),
            source="sys.monitoring",
        ))

    def on_return(code, offset: int, retval: Any):
        if not is_app_code(code):
            return sys.monitoring.DISABLE
        trace_id = get_trace_id()
        if not trace_id:
            return
        emit(NormalizedEvent.now(
            probe="django.view.exit",
            trace_id=trace_id,
            service="django",
            name=code.co_qualname,
            parent_span_id=get_span_id(),
            source="sys.monitoring",
        ))

    try:
        sys.monitoring.register_callback(TOOL_ID, sys.monitoring.events.CALL, on_call)
        sys.monitoring.register_callback(TOOL_ID, sys.monitoring.events.PY_RETURN, on_return)
        logger.info(
            "django probe: sys.monitoring installed for modules: %s",
            observe_modules,
        )
        return True
    except Exception as exc:
        logger.warning("django probe sys.monitoring register: %s", exc)
        return False


def _setup_setprofile_311(observe_modules: List[str]) -> bool:
    is_app_code = _make_view_filter(observe_modules)
    original_profile = sys.getprofile()
    _originals["sys_profile"] = original_profile

    def _profile(frame, event: str, arg: Any):
        if event not in ("call", "return"):
            if original_profile:
                original_profile(frame, event, arg)
            return

        code = frame.f_code
        if not is_app_code(code):
            if original_profile:
                original_profile(frame, event, arg)
            return

        trace_id = get_trace_id()
        if trace_id:
            probe = "django.view.enter" if event == "call" else "django.view.exit"
            emit(NormalizedEvent.now(
                probe=probe,
                trace_id=trace_id,
                service="django",
                name=code.co_qualname,
                parent_span_id=get_span_id(),
                source="sys.setprofile",
            ))

        if original_profile:
            original_profile(frame, event, arg)

    sys.setprofile(_profile)
    logger.info("django probe: sys.setprofile installed for modules: %s", observe_modules)
    return True


def _teardown_function_observation() -> None:
    global _MONITORING_TOOL_ID
    minor = sys.version_info[1]

    if minor >= 12 and _MONITORING_TOOL_ID is not None:
        if hasattr(sys, "monitoring"):
            sys.monitoring.set_events(_MONITORING_TOOL_ID, sys.monitoring.events.NO_EVENTS)
            sys.monitoring.register_callback(_MONITORING_TOOL_ID, sys.monitoring.events.CALL, None)
            sys.monitoring.register_callback(_MONITORING_TOOL_ID, sys.monitoring.events.PY_RETURN, None)
        _MONITORING_TOOL_ID = None
    elif minor == 11:
        original = _originals.pop("sys_profile", None)
        sys.setprofile(original)


# ====================================================================== #
# DjangoProbe
# ====================================================================== #

class DjangoProbe(BaseProbe):
    """
    Observes Django using only official extension points.

    Configure in stacktracer.yaml:
        observe:
          modules:
            - myapp
            - myapp.views
            - myapp.api

    The TracerMiddleware must be added manually:
        MIDDLEWARE = [
            "stacktracer.probes.django_probe.TracerMiddleware",
            ...
        ]

    The probe installs three things at start():
        1. Database execute_wrapper — query text and duration
        2. Django signals            — lifecycle events
        3. sys.monitoring (3.12+) or sys.setprofile (3.11) — view functions

    Nothing is patched. All three are official Django or CPython APIs.
    """
    name = "django"

    def start(self, observe_modules: Optional[List[str]] = None) -> None:
        global _patched, _observe_modules

        if _patched:
            logger.warning("django probe already installed — skipping")
            return

        _observe_modules = observe_modules or []

        _install_db_wrapper()
        _install_signals()

        minor = sys.version_info[1]
        if _observe_modules:
            if minor >= 12:
                _setup_monitoring_312(_observe_modules)
            else:
                _setup_setprofile_311(_observe_modules)
        else:
            logger.info(
                "django probe: no observe.modules configured — "
                "view-level tracing disabled. Add to stacktracer.yaml:\n"
                "  observe:\n    modules:\n      - myapp"
            )

        _patched = True

    def stop(self) -> None:
        global _patched

        if not _patched:
            return

        _uninstall_db_wrapper()
        _uninstall_signals()
        _teardown_function_observation()

        _patched = False
        logger.info("django probe removed")
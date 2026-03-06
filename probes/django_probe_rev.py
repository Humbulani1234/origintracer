"""
probes/django_probe.py

Observes Django using only official extension points.
No monkey patching of private Django internals.

Extension points used:
    TracerMiddleware              Official MIDDLEWARE hook — request lifecycle
    connection.execute_wrapper()  Official DB profiling API (Debug Toolbar uses this)
    got_request_exception signal  Official Django signal — unhandled exceptions
    sys.monitoring (3.12+)        CPython official profiling API — view functions
    sys.setprofile (3.11)         CPython profiling API — view functions (fallback)

What you get without observe.modules configured:
    request.entry          — every HTTP request enters middleware
    request.exit           — every HTTP response leaves middleware
    django.db.query        — every ORM / raw SQL query with duration
    django.exception       — unhandled exceptions that escape the view

What you additionally get WITH observe.modules configured:
    django.view.enter      — user app function entered
    django.view.exit       — user app function returned

Configure in stacktracer.yaml:
    observe:
      modules:
        - myapp
        - myapp.views
        - myapp.api

    The module list is matched as a filename substring — "myapp" matches
    any file whose path contains "myapp". Keep it narrow: observing only
    your application code keeps overhead low and graph noise minimal.

TracerMiddleware is REQUIRED and must be first in MIDDLEWARE:
    MIDDLEWARE = [
        "stacktracer.probes.django_probe.TracerMiddleware",
        "django.middleware.security.SecurityMiddleware",
        ...
    ]

    Without TracerMiddleware, get_trace_id() returns None everywhere and
    all other hooks silently drop their events — nothing is traced.
"""

from __future__ import annotations

import logging
import sys
import time
import uuid
from typing import Any, Callable, List, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent
from ..context.vars import get_trace_id, set_trace, reset_trace

logger = logging.getLogger("stacktracer.probes.django")

# Module-level state — one probe instance per process
_originals:         dict  = {}
_patched:           bool  = False
_MONITORING_TOOL_ID: Any  = None
_observe_modules:   List[str] = []


# ====================================================================== #
# TracerMiddleware — primary observation point
# ====================================================================== #

class TracerMiddleware:
    """
    ASGI/WSGI middleware. First in MIDDLEWARE. Wraps every request.

    This is not monkey patching. Django middleware is the documented,
    stable, version-safe way to intercept request processing.

    Handles both sync and async views correctly — detects which mode
    Django is using and returns the appropriate callable.
    """

    def __init__(self, get_response: Callable) -> None:
        self.get_response = get_response
        import asyncio, inspect
        self._is_async = (
            asyncio.iscoroutinefunction(get_response)
            or inspect.iscoroutinefunction(get_response)
        )

    def __call__(self, request: Any) -> Any:
        if self._is_async:
            return self.__acall__(request)
        return self._sync_call(request)

    # ── sync path ─────────────────────────────────────────────────────

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

    # ── async path ────────────────────────────────────────────────────

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

    # ── helpers ───────────────────────────────────────────────────────

    def _begin(self, request: Any):
        # Prefer trace_id forwarded by nginx via X-Request-ID header.
        # Fall back to generating a new UUID if this is the entry point.
        trace_id = (
            request.META.get("HTTP_X_REQUEST_ID")
            or str(uuid.uuid4())
        )
        token = set_trace(trace_id)
        emit(NormalizedEvent.now(
            probe    = "request.entry",
            trace_id = trace_id,
            service  = "django",
            name     = request.path,
            method   = request.method,
            http_host= request.META.get("HTTP_HOST", ""),
        ))
        return trace_id, token

    def _end(self, request: Any, response: Any, trace_id: str) -> None:
        emit(NormalizedEvent.now(
            probe       = "request.exit",
            trace_id    = trace_id,
            service     = "django",
            name        = request.path,
            method      = request.method,
            status_code = response.status_code,
        ))

    def _error(self, request: Any, exc: Exception, trace_id: str) -> None:
        emit(NormalizedEvent.now(
            probe          = "django.exception",
            trace_id       = trace_id,
            service        = "django",
            name           = request.path,
            exception_type = type(exc).__name__,
            exception_msg  = str(exc)[:200],
            source         = "middleware",
        ))


# ====================================================================== #
# Database execute_wrapper — official query profiling hook
# ====================================================================== #

def _make_db_wrapper():
    """
    context manager suitable for connection.execute_wrapper().

    This is Django's official database profiling API, documented at:
    https://docs.djangoproject.com/en/stable/topics/db/instrumentation/

    It is used by Django Debug Toolbar and django-silk for the same purpose.
    Gives SQL text, params, duration, and connection alias without any
    wire-level parsing.
    """
    from contextlib import contextmanager

    @contextmanager
    def _wrapper(execute, sql, params, many, context):
        trace_id = get_trace_id()
        t0 = time.perf_counter()

        try:
            result = execute(sql, params, many, context)
        except Exception as exc:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe      = "django.db.query",
                    trace_id   = trace_id,
                    service    = "django",
                    name       = sql[:200],   # GraphNormalizer collapses literals
                    duration_ns= duration_ns,
                    db_alias   = context["connection"].alias,
                    success    = False,
                    error      = str(exc)[:200],
                ))
            raise
        else:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe      = "django.db.query",
                    trace_id   = trace_id,
                    service    = "django",
                    name       = sql[:200],
                    duration_ns= duration_ns,
                    db_alias   = context["connection"].alias,
                    success    = True,
                    row_count  = getattr(result, "rowcount", None),
                ))
            return result

    return _wrapper


def _install_db_wrapper() -> None:
    try:
        from django.db import connections
        wrapper = _make_db_wrapper()
        for conn in connections.all():
            conn.execute_wrappers.append(wrapper)
        _originals["db_wrapper"] = wrapper
        logger.info("django probe: database execute_wrapper installed")
    except Exception as exc:
        logger.warning("django probe: database wrapper failed: %s", exc)


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
# Django signals — unhandled exceptions only
# ====================================================================== #
#
# We do NOT connect request_started or request_finished signals.
# TracerMiddleware already emits request.entry and request.exit for every
# request — connecting those signals would emit duplicate events.
#
# got_request_exception is the only signal we use. It fires for exceptions
# that escape the view and are not caught by TracerMiddleware's try/except
# (e.g. exceptions raised inside Django's own request handling machinery
# before the middleware's finally block runs in certain edge cases).

def _on_unhandled_exception(sender: Any, request: Any, exception: Any, **kwargs) -> None:
    trace_id = get_trace_id()
    if trace_id:
        emit(NormalizedEvent.now(
            probe          = "django.exception",
            trace_id       = trace_id,
            service        = "django",
            name           = request.path if request else "unknown",
            exception_type = type(exception).__name__,
            exception_msg  = str(exception)[:200],
            source         = "got_request_exception_signal",
        ))


def _install_signals() -> None:
    try:
        from django.core.signals import got_request_exception
        got_request_exception.connect(_on_unhandled_exception, weak=False)
        logger.info("django probe: got_request_exception signal connected")
    except ImportError:
        pass


def _uninstall_signals() -> None:
    try:
        from django.core.signals import got_request_exception
        got_request_exception.disconnect(_on_unhandled_exception)
    except ImportError:
        pass


# ====================================================================== #
# View function observation via sys.monitoring (3.12+) / setprofile (3.11)
# ====================================================================== #
#
# Both paths are gated on observe_modules being non-empty.
# An empty list means "observe nothing" — no callbacks are registered,
# no overhead is added, and a clear INFO log explains what is missing.

def _make_module_filter(observe_modules: List[str]) -> Callable:
    """
    Returns a predicate that returns True for code objects whose filename
    contains any of the configured module path prefixes.

    "myapp" matches:
        /home/user/myapp/views.py
        /home/user/myapp/api/orders.py
        /home/user/myapp/tasks.py

    It does NOT match:
        /usr/lib/python3.12/asyncio/events.py
        /home/user/.venv/lib/django/views/generic/base.py
    """
    # Convert Python module names to path fragments for substring matching
    path_fragments = tuple(m.replace(".", "/") for m in observe_modules)

    def is_app_code(code: Any) -> bool:
        return any(frag in code.co_filename for frag in path_fragments)

    return is_app_code


def _setup_monitoring_312(observe_modules: List[str]) -> None:
    """Install sys.monitoring callbacks for Python 3.12+."""
    global _MONITORING_TOOL_ID

    if not hasattr(sys, "monitoring"):
        logger.warning("django probe: sys.monitoring not available (Python < 3.12)")
        return

    TOOL_ID = sys.monitoring.PROFILER_ID
    _MONITORING_TOOL_ID = TOOL_ID
    is_app_code = _make_module_filter(observe_modules)

    try:
        sys.monitoring.set_events(
            TOOL_ID,
            sys.monitoring.events.CALL | sys.monitoring.events.PY_RETURN,
        )
    except Exception as exc:
        logger.warning("django probe: sys.monitoring set_events failed: %s", exc)
        return

    def on_call(code: Any, offset: int, callable_: Any, arg0: Any) -> Any:
        if not is_app_code(code):
            # Returning DISABLE tells sys.monitoring to stop calling this
            # callback for this specific code object — zero recurring overhead
            return sys.monitoring.DISABLE
        trace_id = get_trace_id()
        if trace_id:
            emit(NormalizedEvent.now(
                probe    = "django.view.enter",
                trace_id = trace_id,
                service  = "django",
                name     = code.co_qualname,
                source   = "sys.monitoring",
            ))

    def on_return(code: Any, offset: int, retval: Any) -> Any:
        if not is_app_code(code):
            return sys.monitoring.DISABLE
        trace_id = get_trace_id()
        if trace_id:
            emit(NormalizedEvent.now(
                probe    = "django.view.exit",
                trace_id = trace_id,
                service  = "django",
                name     = code.co_qualname,
                source   = "sys.monitoring",
            ))

    try:
        sys.monitoring.register_callback(TOOL_ID, sys.monitoring.events.CALL,      on_call)
        sys.monitoring.register_callback(TOOL_ID, sys.monitoring.events.PY_RETURN, on_return)
        logger.info("django probe: sys.monitoring installed for modules: %s", observe_modules)
    except Exception as exc:
        logger.warning("django probe: sys.monitoring register_callback failed: %s", exc)


def _setup_setprofile_311(observe_modules: List[str]) -> None:
    """Install sys.setprofile for Python 3.11 (fallback from sys.monitoring)."""
    is_app_code = _make_module_filter(observe_modules)
    original_profile = sys.getprofile()
    _originals["sys_profile"] = original_profile

    def _profile(frame: Any, event: str, arg: Any) -> None:
        if event in ("call", "return") and is_app_code(frame.f_code):
            trace_id = get_trace_id()
            if trace_id:
                probe = "django.view.enter" if event == "call" else "django.view.exit"
                emit(NormalizedEvent.now(
                    probe    = probe,
                    trace_id = trace_id,
                    service  = "django",
                    name     = frame.f_code.co_qualname,
                    source   = "sys.setprofile",
                ))
        # Always chain to the previous profiler if one existed
        if original_profile:
            original_profile(frame, event, arg)

    sys.setprofile(_profile)
    logger.info("django probe: sys.setprofile installed for modules: %s", observe_modules)


def _teardown_function_observation() -> None:
    global _MONITORING_TOOL_ID
    minor = sys.version_info.minor

    if minor >= 12 and _MONITORING_TOOL_ID is not None:
        if hasattr(sys, "monitoring"):
            sys.monitoring.set_events(
                _MONITORING_TOOL_ID,
                sys.monitoring.events.NO_EVENTS,
            )
            sys.monitoring.register_callback(
                _MONITORING_TOOL_ID, sys.monitoring.events.CALL, None,
            )
            sys.monitoring.register_callback(
                _MONITORING_TOOL_ID, sys.monitoring.events.PY_RETURN, None,
            )
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
    No monkey patching. No private API access.

    Installed automatically by stacktracer.init() when "django" is in probes.
    TracerMiddleware must be added manually to settings.MIDDLEWARE.

    See module docstring for full details.
    """
    name = "django"

    def start(self, observe_modules: Optional[List[str]] = None) -> None:
        global _patched, _observe_modules

        if _patched:
            logger.warning("django probe: already installed — skipping")
            return

        _observe_modules = observe_modules or []

        # Always install — these work with or without observe.modules
        _install_db_wrapper()
        _install_signals()

        # View-function observation requires the user to name their modules.
        # Without that list we cannot filter the firehose of Python calls
        # down to just application code.
        if _observe_modules:
            minor = sys.version_info.minor
            if minor >= 12:
                _setup_monitoring_312(_observe_modules)
            else:
                _setup_setprofile_311(_observe_modules)
        else:
            logger.info(
                "django probe: observe.modules not configured — "
                "django.view.enter / django.view.exit disabled. "
                "To enable view tracing, add to stacktracer.yaml:\n"
                "  observe:\n    modules:\n      - myapp"
            )

        _patched = True
        logger.info("django probe: installed (view_tracing=%s)", bool(_observe_modules))

    def stop(self) -> None:
        global _patched

        if not _patched:
            return

        _uninstall_db_wrapper()
        _uninstall_signals()
        _teardown_function_observation()

        _patched = False
        logger.info("django probe: removed")
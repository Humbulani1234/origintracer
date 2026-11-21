"""Capture outbound HTTP dependency calls from supported Python clients.

This is a core, library-level probe rather than a Django/FastAPI framework
probe: applications opt into it in ``origintracer.yaml`` when they want
outbound dependency timing. It observes calls through ``httpx`` and, when the
optional ``requests`` package is installed, ``requests``. It records the HTTP
method, destination host, status/error type, and elapsed call duration. URL
paths, query strings, headers, and bodies are intentionally excluded because
they can contain secrets or user data.

Do not enable this alongside OpenTelemetry HTTP client instrumentation for the
same library, or the same outbound call may appear twice.

The recorded duration runs until the client method returns; for streaming
responses that can mean response headers arrive before the body is consumed.
The probe wraps public client methods and restores them on ``stop()``. It is
opt-in because patching a process-wide client method is an explicit behavior
change, even though the wrapper preserves the original call and result.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Callable
from urllib.parse import urlsplit

import httpx

from origintracer.context.vars import get_span_id, get_trace_id
from origintracer.core.event_schema import (
    NormalizedEvent,
    ProbeTypes,
)
from origintracer.sdk.base_probe import BaseProbe
from origintracer.sdk.emitter import emit

logger = logging.getLogger("origintracer.probes.external_http")
ProbeTypes.register(
    "external.http.client",
    "Completed outbound HTTP request observed by a client-library probe",
)

_originals: list[tuple[Any, str, Callable, Callable]] = []


def _host(url: Any) -> str:
    """Return a host and optional port without credentials or URL paths."""
    try:
        parsed = urlsplit(str(url))
        host = parsed.hostname
        if not host:
            return "unknown"
        # Avoid netloc: it may contain username/password userinfo.
        rendered_host = f"[{host}]" if ":" in host else host
        return (
            f"{rendered_host}:{parsed.port}"
            if parsed.port is not None
            else rendered_host
        )
    except (TypeError, ValueError):
        return "unknown"


def _emit_observation(
    *,
    method: str,
    host: str,
    started_ns: int,
    status_code: int | None,
    error_type: str | None,
    trace_id: str,
    parent_span_id: str | None,
) -> None:
    """Emit one completed client-call observation without affecting the call."""
    try:
        emit(
            NormalizedEvent.now(
                probe="external.http.client",
                trace_id=trace_id,
                parent_span_id=parent_span_id,
                service="external",
                name=f"{method.upper()} {host}",
                duration_ns=max(
                    0, time.perf_counter_ns() - started_ns
                ),
                external=True,
                dependency_host=host,
                http_method=method.upper(),
                status_code=status_code,
                error_type=error_type,
                duration_scope="client_call",
            )
        )
    except Exception:
        # Tracing is best effort and must never change application behavior.
        logger.exception(
            "Could not emit outbound HTTP observation"
        )


def _context() -> tuple[str, str | None]:
    return (
        get_trace_id() or f"external-{uuid.uuid4().hex}",
        get_span_id(),
    )


def _is_origintracer_upload(headers: Any) -> bool:
    """Exclude OriginTracer's own telemetry uploads from dependency graphs."""
    if not headers:
        return False
    try:
        normalized = {str(key).lower() for key in headers}
    except TypeError:
        return False
    return "x-origintracer-worker-instance-id" in normalized


def _install_requests() -> None:
    try:
        import requests
    except ImportError:
        return

    target = requests.sessions.Session
    original = target.request

    def traced_request(session, method, url, *args, **kwargs):
        # The SDK uploader uses this identity header for its own
        # snapshot/heartbeat traffic. Excluding it prevents telemetry
        # about telemetry from appearing as an external dependency.
        if _is_origintracer_upload(kwargs.get("headers")):
            return original(
                session, method, url, *args, **kwargs
            )
        started_ns = time.perf_counter_ns()
        trace_id, parent_span_id = _context()
        response = None
        error_type = None
        try:
            response = original(
                session, method, url, *args, **kwargs
            )
            return response
        except Exception as exc:
            error_type = type(exc).__name__
            raise
        finally:
            _emit_observation(
                method=str(method),
                host=_host(url),
                started_ns=started_ns,
                status_code=(
                    response.status_code
                    if response is not None
                    else None
                ),
                error_type=error_type,
                trace_id=trace_id,
                parent_span_id=parent_span_id,
            )

    # requests declares request as a typed method; setattr is needed here
    # because this opt-in probe intentionally replaces it at runtime.
    setattr(target, "request", traced_request)
    _originals.append(
        (target, "request", original, traced_request)
    )


def _install_httpx_sync() -> None:
    target = httpx.Client
    original = target.send

    def traced_send(client, request, *args, **kwargs):
        if _is_origintracer_upload(
            getattr(request, "headers", None)
        ):
            return original(client, request, *args, **kwargs)
        started_ns = time.perf_counter_ns()
        trace_id, parent_span_id = _context()
        response = None
        error_type = None
        try:
            response = original(client, request, *args, **kwargs)
            return response
        except Exception as exc:
            error_type = type(exc).__name__
            raise
        finally:
            _emit_observation(
                method=getattr(request, "method", "GET"),
                host=_host(getattr(request, "url", "")),
                started_ns=started_ns,
                status_code=(
                    response.status_code
                    if response is not None
                    else None
                ),
                error_type=error_type,
                trace_id=trace_id,
                parent_span_id=parent_span_id,
            )

    # The probe intentionally replaces client methods at runtime.
    setattr(target, "send", traced_send)
    _originals.append((target, "send", original, traced_send))


def _install_httpx_async() -> None:
    target = httpx.AsyncClient
    original = target.send

    async def traced_send(client, request, *args, **kwargs):
        if _is_origintracer_upload(
            getattr(request, "headers", None)
        ):
            return await original(
                client, request, *args, **kwargs
            )
        started_ns = time.perf_counter_ns()
        trace_id, parent_span_id = _context()
        response = None
        error_type = None
        try:
            response = await original(
                client, request, *args, **kwargs
            )
            return response
        except Exception as exc:
            error_type = type(exc).__name__
            raise
        finally:
            _emit_observation(
                method=getattr(request, "method", "GET"),
                host=_host(getattr(request, "url", "")),
                started_ns=started_ns,
                status_code=(
                    response.status_code
                    if response is not None
                    else None
                ),
                error_type=error_type,
                trace_id=trace_id,
                parent_span_id=parent_span_id,
            )

    # The probe intentionally replaces client methods at runtime.
    setattr(target, "send", traced_send)
    _originals.append((target, "send", original, traced_send))


class ExternalHTTPProbe(BaseProbe):
    """Observe outbound HTTP calls made by requests and httpx clients."""

    name = "external_http"

    def start(self) -> None:
        if _originals:
            return
        try:
            _install_httpx_sync()
            _install_httpx_async()
            _install_requests()
        except Exception:
            self.stop()
            raise
        logger.info(
            "external HTTP probe installed for httpx and available requests"
        )

    def stop(self) -> None:
        while _originals:
            target, method_name, original, wrapper = (
                _originals.pop()
            )
            # Preserve a later third-party patch rather than overwriting it.
            if getattr(target, method_name) is wrapper:
                setattr(target, method_name, original)
        logger.info(
            "external HTTP probe instrumentation removed"
        )

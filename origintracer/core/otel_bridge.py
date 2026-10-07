"""
**NOTE**: In development - experimental.

Overview
--------
Makes OriginTracer an optional OpenTelemetry `SpanExporter`.

When this bridge is active:
- OpenTelemetry becomes the source of truth for spans
- OriginTracer probes are disabled
- The engine receives `NormalizedEvent` objects translated from OTel spans

---

What is lost in OTel mode
------------------------
- asyncio internals (`Task.__step`, loop tick, ready queue depth)
- kernel-level timing (kprobe on `accept4`, `epoll_wait`)
- gunicorn worker lifecycle events
- nginx correlation

- **OTel mode** --> graph + causal rules
- **Native probe mode** --> graph + kernel internals + asyncio internals

---

Architecture
------------
```text
OTel SDK (Django)
    --> OriginTracerSpanExporter
    --> span_to_event()
    --> engine.process(event)
```
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional, Sequence

import origintracer

logger = logging.getLogger("origintracer.bridge.otel")

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import ReadableSpan  # type: ignore[arg-type]

    from .core.event_schema import NormalizedEvent  # type: ignore[arg-type]


# Probe type mapping from OTel SpanKind
_SPAN_KIND_TO_PROBE = {
    0: "otel.internal",  # INTERNAL
    1: "otel.server",  # SERVER - incoming HTTP request
    2: "otel.client",  # CLIENT - outgoing HTTP/DB/Redis call
    3: "otel.producer",  # PRODUCER - message queue publish
    4: "otel.consumer",  # CONSUMER - message queue consume
}

# OTel semantic convention attributes >> OriginTracer service names
_SERVICE_HINTS = {
    "db.system": lambda v: v,  # "postgresql", "redis", etc.
    "http.scheme": lambda v: "http",
    "rpc.system": lambda v: f"rpc.{v}",
    "messaging.system": lambda v: v,  # "redis", "kafka", etc.
    "peer.service": lambda v: v,
}


def _extract_service(span: "ReadableSpan") -> str:
    """
    Derive a service name from OTel semantic convention attributes.
    Falls back to the instrumentation scope name (e.g. 'opentelemetry.instrumentation.django').
    """
    attrs = span.attributes or {}
    for attr, fn in _SERVICE_HINTS.items():
        if attr in attrs:
            return fn(attrs[attr])
    scope = getattr(span.instrumentation_scope, "name", "") or ""
    if "django" in scope:
        return "django"
    if "psycopg" in scope or "sqlite" in scope:
        return "db"
    if "redis" in scope:
        return "redis"
    if "celery" in scope:
        return "celery"
    if (
        "requests" in scope
        or "httpx" in scope
        or "urllib" in scope
    ):
        return "http_client"
    return scope.split(".")[-1] or "unknown"


def _extract_name(span: "ReadableSpan") -> str:
    """
    Extract a meaningful name from the span.
    Prefer semantic convention attributes over the raw span name.
    """
    attrs = span.attributes or {}
    # HTTP server spans - use the route, not the raw URL
    if "http.route" in attrs:
        method = attrs.get("http.method", "")
        return f"{method} {attrs['http.route']}".strip()
    # DB spans - use the statement (truncated)
    if "db.statement" in attrs:
        stmt = str(attrs["db.statement"])
        return stmt[:120]
    if "db.operation" in attrs and "db.sql.table" in attrs:
        return f"{attrs['db.operation']} {attrs['db.sql.table']}"
    # Messaging
    if "messaging.destination.name" in attrs:
        return attrs["messaging.destination.name"]
    # Fall back to span name
    return span.name or "unknown"


def _trace_id_hex(trace_id: int) -> str:
    """Convert OTel 128-bit integer trace_id to hex string."""
    return format(trace_id, "032x")


def _span_id_hex(span_id: int) -> str:
    """Convert OTel 64-bit integer span_id to hex string."""
    return format(span_id, "016x")


def span_to_event(
    span: "ReadableSpan",
) -> Optional[NormalizedEvent]:
    """
    Convert one OTel ReadableSpan to a NormalizedEvent.
    Returns None if the span should be skipped (e.g. internal OTel noise).
    """
    try:
        from origintracer.core.event_schema import (
            NormalizedEvent,
        )
    except ImportError:
        logger.error(
            "origintracer not installed - cannot convert OTel span"
        )
        return None

    ctx = span.context
    if ctx is None:
        return None

    trace_id = _trace_id_hex(ctx.trace_id)
    span_id = _span_id_hex(ctx.span_id)
    parent_sid = (
        _span_id_hex(span.parent.span_id)
        if span.parent and span.parent.span_id
        else None
    )

    probe = _SPAN_KIND_TO_PROBE.get(
        (
            span.kind.value
            if hasattr(span.kind, "value")
            else span.kind
        ),
        "otel.internal",
    )
    service = _extract_service(span)
    name = _extract_name(span)

    # duration in nanoseconds
    duration_ns = None
    wall_time = None
    if span.start_time and span.end_time:
        duration_ns = (
            span.end_time - span.start_time
        )  # OTel times are ns integers
        wall_time = (
            span.start_time / 1e9
        )  # convert to seconds for wall_time

    # collect useful attributes into metadata
    attrs = dict(span.attributes or {})
    status_code = None
    if span.status:
        status_code = (
            span.status.status_code.value
            if hasattr(span.status.status_code, "value")
            else str(span.status.status_code)
        )

    return NormalizedEvent(
        probe=probe,
        service=service,
        name=name,
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_sid,
        duration_ns=duration_ns,
        wall_time=wall_time or 0.0,
        metadata={
            "otel_span_name": span.name,
            "status_code": status_code,
            "attrs": attrs,
        },
    )


class OriginTracerSpanExporter:
    """
    OriginTracerSpanExporter
    =======================

    OpenTelemetry SpanExporter that feeds spans into OriginTracer's engine.

    Overview
    --------
    This exporter converts OpenTelemetry spans into NormalizedEvents
    and forwards them to the OriginTracer engine.

    Installation
    ------------
    Install as a span processor in your OTel setup:

    ```python

        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from origintracer.bridge.otel_bridge import OriginTracerSpanExporter

        provider.add_span_processor(
            BatchSpanProcessor(OriginTracerSpanExporter())
        )
    ```

    Notes
    -----
    - Use ``BatchSpanProcessor`` instead of ``SimpleSpanProcessor``.
    - Batch export runs in a background thread and does not block request handling.
    """

    def export(self, spans: Sequence["ReadableSpan"]) -> object:
        try:
            from opentelemetry.sdk.trace.export import (  # type: ignore[arg-type]
                SpanExportResult,
            )
        except ImportError:
            return 0  # SUCCESS

        engine = origintracer.get_engine()

        if engine is None:
            logger.debug(
                "OTel bridge: engine not ready - dropping %d spans",
                len(spans),
            )
            return SpanExportResult.SUCCESS

        converted = 0
        for span in spans:
            event = span_to_event(span)
            if event is None:
                continue
            try:
                engine.process(event)
                converted += 1
            except Exception as exc:
                logger.debug(
                    "OTel bridge: engine.process failed: %s", exc
                )

        logger.debug(
            "OTel bridge: exported %d/%d spans to OriginTracer engine",
            converted,
            len(spans),
        )
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        """
        Called by OTel SDK on process exit. Nothing to do - engine shutdown is handled by origintracer.shutdown().
        """
        pass

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        """
        Called by BatchSpanProcessor on shutdown. Nothing to flush - engine handles its own buffer.
        """
        return True


def extract_trace_id_from_traceparent(
    traceparent: str,
) -> Optional[str]:
    """
    Extract the trace_id from a W3C traceparent header.

        traceparent format: ``00-{trace_id}-{span_id}-{flags}``
        Example: ``00-4a3ce96-00f067aa902b7-01``

    Use this in TracerMiddleware to share trace_id with OTel spans
    ::

        traceparent = request.META.get("HTTP_TRACEPARENT", "")
        if traceparent:
            trace_id = extract_trace_id_from_traceparent(traceparent)
            if trace_id:
                set_trace_id(trace_id)
    """
    try:
        parts = traceparent.split("-")
        if len(parts) == 4:
            return parts[1]  # the 32-char hex trace_id
    except Exception:
        pass
    return None

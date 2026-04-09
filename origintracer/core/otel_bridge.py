"""
NOTE: Must be thoroughly tested.

Makes OriginTracer an optional OpenTelemetry SpanExporter.

When this bridge is active, OTel is the source of truth for spans.
StackTracer's own probes are disabled and the engine receives
NormalizedEvents translated from OTel spans instead.

Why OTel as source of truth:
    Some teams already have OTel instrumentation across their stack —
    Django OTel SDK, SQLAlchemy instrumentation, Redis instrumentation.
    Re-instrumenting with StackTracer probes alongside OTel would be
    redundant and risky. The bridge lets StackTracer consume the OTel
    signal the team already has and apply causal rules against it.

What is lost when using OTel mode:
    - asyncio internals (Task.__step, loop tick, ready queue depth)
    - kernel-level timing (kprobe on accept4, epoll_wait)
    - gunicorn worker lifecycle events
    - nginx correlation
    OTel does not go that deep. These are StackTracer-only observations.
    OTel mode gives you the graph and causal rules. Native probe mode
    gives you the graph + kernel internals + asyncio internals.

What is gained when using OTel mode:
    - Zero additional instrumentation if OTel is already deployed
    - Distributed tracing across polyglot services (Go, Java, Node)
    - OTel Collector as the transport (batching, retry, TLS)
    - W3C traceparent header propagation across services automatically

Architecture:
    OTel SDK (in Django)
        >> StackTracerSpanExporter (this file)
        >> span_to_event() converts OTel Span → NormalizedEvent
        >> engine.process(event) feeds the StackTracer graph

    The engine, graph, causal rules, REPL, and React UI are unchanged.
    Only the event source changes from native probes to OTel spans.

Usage - Django:

    # settings.py
    ORIGINTRACER_OTEL_MODE = True # disables native probes

    # apps.py
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.django import DjangoInstrumentor
    from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from stacktracer.bridge.otel_bridge import StackTracerSpanExporter

    class MyAppConfig(AppConfig):
        name = "myapp"

        def ready(self):
            import stacktracer

            # init StackTracer in OTel mode — skips probe startup
            stacktracer.init(otel_mode=True)

            # set up OTel with StackTracer as the exporter
            provider = TracerProvider()
            provider.add_span_processor(
                BatchSpanProcessor(StackTracerSpanExporter())
            )
            trace.set_tracer_provider(provider)

            # instrument Django, DB, Redis
            DjangoInstrumentor().instrument()
            Psycopg2Instrumentor().instrument()
            RedisInstrumentor().instrument()

    This is the complete integration. OTel instruments your code,
    spans flow to StackTracerSpanExporter, StackTracer builds the
    causal graph, REPL and React UI work as normal.

OTel span >> NormalizedEvent mapping:
    span.name >> name
    span.kind >> probe (server=request, client=call, internal=function)
    span.context.trace_id >> trace_id (hex string)
    span.context.span_id >> span_id
    span.parent.span_id >> parent_span_id
    span.attributes["http.route"] >> name (for HTTP spans)
    span.attributes["db.statement"] >> name (for DB spans)
    span.attributes["peer.service"] >> service
    span.start_time/end_time >> wall_time, duration_ns
    span.status.status_code >> metadata["status_code"]
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional, Sequence

logger = logging.getLogger("origintracer.bridge.otel")

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import ReadableSpan


# ── Probe type mapping from OTel SpanKind ────────────────────────────────────

_SPAN_KIND_TO_PROBE = {
    0: "otel.internal",  # INTERNAL
    1: "otel.server",  # SERVER - incoming HTTP request
    2: "otel.client",  # CLIENT - outgoing HTTP/DB/Redis call
    3: "otel.producer",  # PRODUCER - message queue publish
    4: "otel.consumer",  # CONSUMER - message queue consume
}

# OTel semantic convention attributes >> StackTracer service names
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
    # fall back to instrumentation scope name — strip the long prefix
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


def span_to_event(span: "ReadableSpan") -> Optional[object]:
    """
    Convert one OTel ReadableSpan to a NormalizedEvent.
    Returns None if the span should be skipped (e.g. internal OTel noise).
    """
    try:
        from stacktracer.core.event_schema import NormalizedEvent
    except ImportError:
        logger.error(
            "stacktracer not installed — cannot convert OTel span"
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


# StackTracerSpanExporter


class OriginTracerSpanExporter:
    """
    OpenTelemetry SpanExporter that feeds spans into StackTracer's engine.

    Install as a span processor in your OTel setup:

        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from stacktracer.bridge.otel_bridge import OriginTracerSpanExporter

        provider.add_span_processor(
            BatchSpanProcessor(OriginTracerSpanExporter())
        )

    Use BatchSpanProcessor not SimpleSpanProcessor — batch export runs
    in a background thread and does not block request handling.
    """

    def export(self, spans: Sequence["ReadableSpan"]) -> object:
        try:
            from opentelemetry.sdk.trace.export import (
                SpanExportResult,
            )
        except ImportError:
            return 0  # SUCCESS

        import origintracer

        engine = origintracer.get_engine()

        if engine is None:
            logger.debug(
                "OTel bridge: engine not ready — dropping %d spans",
                len(spans),
            )
            return (
                SpanExportResult.SUCCESS
            )  # drop silently, not an error

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
            "OTel bridge: exported %d/%d spans to StackTracer engine",
            converted,
            len(spans),
        )
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        """Called by OTel SDK on process exit. Nothing to do — engine shutdown is handled by stacktracer.shutdown()."""
        pass

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        """Called by BatchSpanProcessor on shutdown. Nothing to flush — engine handles its own buffer."""
        return True


# W3C traceparent extraction helper


def extract_trace_id_from_traceparent(
    traceparent: str,
) -> Optional[str]:
    """
    Extract the trace_id from a W3C traceparent header.

    traceparent format: 00-{trace_id}-{span_id}-{flags}
    Example: 00-4a3ce96-00f067aa902b7-01

    Use this in TracerMiddleware to share trace_id with OTel spans:

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

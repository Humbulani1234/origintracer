# otel_bridge.py
from opentelemetry.sdk.trace import ReadableSpan

from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.sdk.emitter import emit


def ingest_otel_span(span: ReadableSpan) -> None:
    emit(
        NormalizedEvent(
            probe=f"otel.{span.instrumentation_scope.name}",
            trace_id=format(span.context.trace_id, "032x"),
            service=span.resource.attributes.get("service.name", "unknown"),
            name=span.name,
            duration_ns=span.end_time - span.start_time,
            timestamp=span.start_time / 1e9,
            metadata=dict(span.attributes or {}),
        )
    )

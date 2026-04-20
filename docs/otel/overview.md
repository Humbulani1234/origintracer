# OpenTelemetry Bridge

!!! warning "In Development"
    This bridge must be thoroughly tested before use in production.
    Native probe mode is more capable and recommended where possible.

Makes OriginTracer an optional OpenTelemetry `SpanExporter`. When active,
OTel is the source of truth for spans — OriginTracer's native probes are
disabled and the engine receives `NormalizedEvent` objects translated from
OTel spans instead.

## Native Probes vs OTel Mode

OTel mode gives you the graph and causal rules. Native probe mode gives you
the graph plus kernel internals and asyncio internals.

The following observations are **only available in native probe mode**:

| Observation | Why OTel cannot provide it |
|---|---|
| asyncio internals — `Task.__step`, loop tick, ready queue depth | OTel does not instrument the event loop |
| Kernel-level timing — `accept4`, `epoll_wait` kprobes | OTel operates above the syscall boundary |
| gunicorn worker lifecycle events | Not part of the OTel spec |
| nginx correlation | No OTel instrumentation at the nginx layer |

Use OTel mode when you already have OTel instrumentation and want to add
causal graph analysis without deploying native probes. Use native probe mode
when you need the full picture.

## Architecture

```
OTel SDK (in Django)
    └── OriginTracerSpanExporter          ← this file
            └── span_to_event()           converts OTel Span → NormalizedEvent
                    └── engine.process()  feeds the OriginTracer graph
```

The engine, graph, causal rules, REPL, and React UI are unchanged.
Only the event source changes from native probes to OTel spans.

## Setup — Django

```python
# settings.py
ORIGINTRACER_OTEL_MODE = True   # disables native probes

# apps.py
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from origintracer.bridge.otel_bridge import OriginTracerSpanExporter

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import origintracer

        # init OriginTracer in OTel mode — skips native probe startup
        origintracer.init(otel_mode=True)

        # set up OTel with OriginTracer as the exporter
        provider = TracerProvider()
        provider.add_span_processor(
            BatchSpanProcessor(OriginTracerSpanExporter())
        )
        trace.set_tracer_provider(provider)

        # instrument Django, DB, Redis
        DjangoInstrumentor().instrument()
        Psycopg2Instrumentor().instrument()
        RedisInstrumentor().instrument()
```

This is the complete integration. OTel instruments your code, spans flow to
`OriginTracerSpanExporter`, OriginTracer builds the causal graph, and the
REPL and React UI work as normal.

!!! note
    Use `BatchSpanProcessor`, not `SimpleSpanProcessor`. Batch export runs
    in a background thread and does not block request handling.

## Span → NormalizedEvent Mapping

| OTel field | NormalizedEvent field | Notes |
|---|---|---|
| `span.name` | `name` | Overridden by `http.route` or `db.statement` if present |
| `span.kind` | `probe` | `server` → `request`, `client` → `call`, `internal` → `function` |
| `span.context.trace_id` | `trace_id` | Hex string |
| `span.context.span_id` | `span_id` | |
| `span.parent.span_id` | `parent_span_id` | |
| `span.attributes["http.route"]` | `name` | HTTP spans — preferred over `span.name` |
| `span.attributes["db.statement"]` | `name` | DB spans — preferred over `span.name` |
| `span.attributes["peer.service"]` | `service` | |
| `span.start_time` / `end_time` | `wall_time`, `duration_ns` | |
| `span.status.status_code` | `metadata["status_code"]` | |

## W3C Traceparent Integration

If your upstream proxy injects a `traceparent` header, use
`extract_trace_id_from_traceparent()` in `TracerMiddleware` to share the
same `trace_id` between OTel spans and OriginTracer context:

```python
from origintracer.bridge.otel_bridge import extract_trace_id_from_traceparent

traceparent = request.META.get("HTTP_TRACEPARENT", "")
if traceparent:
    trace_id = extract_trace_id_from_traceparent(traceparent)
    if trace_id:
        set_trace_id(trace_id)
```

The `traceparent` format is the W3C standard:

```
00-{trace_id}-{span_id}-{flags}

e.g. 00-4a3ce96-00f067aa902b7-01
```

This ensures kernel-level events attributed via `KprobeBridge` and OTel spans
share the same `trace_id` for correct critical path reconstruction.
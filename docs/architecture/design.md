# Architecture: Design Principles

## Emit observations, infer meaning later

Probes emit raw `NormalizedEvent` objects. The engine derives meaning — graph topology, causal rules, pattern matches — from the event stream. Probes never import the engine. The engine never imports probes. The `NormalizedEvent` is the only data contract between them.

## The graph converges

The `RuntimeGraph` converges to a stable topology after warmup. At 100 req/s hitting 5 routes, the graph reaches ~24 nodes after the first few requests and stays there. `upsert_node` and `upsert_edge` update call counts and durations on existing nodes — the graph does not grow without bound.

## Decouple emission from processing

Probes call `emit()` which appends to `EventBuffer` — a thread-safe bounded deque. The drain thread processes the buffer asynchronously. Request handlers never wait for graph processing. This is the same design that makes Kafka fast — write to the log immediately, process later.

## One init per process

After `os.fork()`, threads do not survive. The drain thread is dead in the child. `_restart_drain_thread()` revives it. `AppConfig.ready()` is the correct and only place to call `stacktracer.init()` in a gunicorn worker — never in gunicorn hooks.

## Lifecycle events bypass the buffer

Per-request events go through `emit()` → buffer → drain thread. Lifecycle events — `gunicorn.worker.fork`, `celery.worker.fork`, `nginx.worker.discovered` — go through `emit_direct()` → engine directly. They must appear in the graph immediately at startup before any requests arrive.

---

# Architecture: Event Schema

## NormalizedEvent

The sole data contract between probes and the engine.

```python
@dataclass
class NormalizedEvent:
    probe:          str           # ProbeType — e.g. "request.entry"
    service:        str           # "django", "nginx", "celery"
    name:           str           # route, function, query text
    trace_id:       str           # ties all events in one request
    timestamp:      float         # perf_counter() at emission
    wall_time:      float         # time.time() for display
    duration_ns:    Optional[int] # measured duration
    span_id:        str           # OTel-compatible span id
    parent_span_id: Optional[str] # parent span for distributed tracing
    pid:            Optional[int] # OS process id
    metadata:       Dict          # probe-specific payload
```

## Factory

```python
event = NormalizedEvent.now(
    probe    = "django.view.exit",
    trace_id = get_trace_id(),
    service  = "django",
    name     = "NPlusOneView",
    duration_ns = 4_300_000,
)
```

`NormalizedEvent.now()` captures `timestamp` and `wall_time` at the call site.
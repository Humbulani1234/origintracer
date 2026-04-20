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

---
# Architecture: Normalisation

Before a `NormalizedEvent` enters the `RuntimeGraph`, the `(service, name)`
pair is normalised to its structural form.

## The problem

`RuntimeGraph` keys nodes by `"service::name"`. High-cardinality names cause
unbounded graph growth:

```
django::/api/users/1234/profile  →  one node per user
django::/api/users/5678/profile  →  another node
...10,000 users = 10,000 nodes for one structural endpoint
```

## The solution

`GraphNormalizer` rewrites names before node insertion:

```
django::/api/users/1234/profile  →  django::/api/users/{id}/profile
django::/api/users/5678/profile  →  django::/api/users/{id}/profile  (same node)
```

The graph has one node per structural pattern. `call_count` accumulates across
all individual user requests. This is almost always what you want for causal
reasoning — the endpoint is slow, not user 1234 specifically.

## Normalisation order

Applied in this order, last match wins:

1. Built-in patterns — UUIDs, numeric IDs, memory addresses, SQL literals
2. Per-service rules from `origintracer.yaml`
3. Global rules (`service: "*"`) from `origintracer.yaml`
4. Max-length truncation

## Extending

In `origintracer.yaml`:

```yaml
normalize:
  - service: django
    pattern: "/api/items/(\\d+)/"
    replacement: "/api/items/{id}/"
```

Or at `init()` time:

```python
origintracer.init(
    normalize=[
        {"service": "django", "pattern": "/api/items/(\\d+)/", "replacement": "/api/items/{id}/"}
    ]
)
```

---
# Architecture: Compaction

The `RuntimeGraph` is bounded by two mechanisms running on the background
snapshot loop.

## Mechanism 1 — Node TTL

Nodes not seen for longer than `node_ttl_s` seconds are evicted. This handles
services that appear temporarily — a feature flag endpoint that was removed,
a task type no longer dispatched.

Default TTL: `3600` seconds.

## Mechanism 2 — Node cap with LRU eviction

When node count exceeds `max_nodes`, the least recently seen nodes are evicted
until the count reaches `max_nodes * evict_to_ratio`.

Default: cap `5000` nodes, evict to 80% (`4000` nodes) when exceeded.

## Edge handling

When a node is evicted, all edges incident to it are also removed. Dangling
edges pointing to evicted nodes are invalid for causal reasoning and must not
persist.

## Compaction stats

`compactor.compact(graph)` returns:

```python
{"evicted_nodes": 12, "evicted_edges": 34, "reason": "ttl+cap"}
```

## Tuning

```python
origintracer.init(
    compactor={
        "max_nodes":      5000,
        "evict_to_ratio": 0.80,
        "node_ttl_s":     3600,
        "min_call_count": 1,     # never evict nodes with fewer calls than this
    }
)
```

---
# Architecture: Active Request Tracking

`ActiveRequestTracker` maintains a live registry of in-flight requests —
requests that have started but not yet completed.

## What it tracks

Each in-flight request is a `RequestSpan`:

```python
@dataclass
class RequestSpan:
    trace_id:   str
    service:    str
    pattern:    str    # normalised path e.g. /api/users/{id}/profile
    started_at: float  # perf_counter()
    duration_ms: Optional[float]  # set on complete()
```

## Lifecycle

```python
# TracerMiddleware._begin()
engine.tracker.start(trace_id=trace_id, service="django", pattern=pattern)

# TracerMiddleware._end()
span = engine.tracker.complete(trace_id)
```

`complete()` records `duration_ms` and removes the span from the active set.
Spans that never complete are evicted after `ttl_s` seconds.

## What causal rules get from it

Rules receive the tracker as an optional third argument:

```python
def _my_rule(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict]:
    ...
```

The `request_duration_anomaly` rule uses it to compare recent P99 against
historical average — it needs live span durations, not just graph node averages.

## Tuning

```python
origintracer.init(
    active_requests={
        "ttl_s":    30,     # evict incomplete spans after 30s
        "max_size": 10_000, # cap the active set
    }
)
```

`\active` in the REPL shows the current in-flight request set live.


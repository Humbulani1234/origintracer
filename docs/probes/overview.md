# Probes Overview

Probes are the observation layer. Each probe instruments one framework or library using its official, documented extension points — no monkey patching of internal classes, no fragile bytecode manipulation.

## How probes work

```mermaid
graph LR
    A[Framework hook fires] --> B[Probe callback]
    B --> C[NormalizedEvent.now()]
    C --> D[emit() / emit_direct()]
    D --> E[EventBuffer]
    E -->|drain thread| F[Engine.process()]
    F --> G[RuntimeGraph]
```

Probes call `emit()` for per-request events (goes through the buffer) and `emit_direct()` for lifecycle events (bypasses the buffer, processes immediately).

## Extension points used

| Probe | Extension point | API stability |
|---|---|---|
| Django | `MIDDLEWARE`, `View.dispatch()`, `execute_wrapper` | Stable, documented |
| asyncio | `Task.__step` patch (3.11), `sys.monitoring` (3.12+) | Internal (3.11), public (3.12+) |
| Gunicorn | Server hooks (`post_fork`, `worker_exit`, etc.) | Stable, documented |
| Uvicorn | ASGI middleware | Stable, documented |
| Celery | Signals (`task_prerun`, `worker_process_init`, etc.) | Stable, documented |
| nginx | kprobe + Lua UDP | Kernel-level |
| Redis | `TracedRedis` subclass | Stable |

## Built-in probes (free)

Four probes ship with the open-source package:

- `django` — full probe
- `asyncio` — `create_task` wrapper + loop tick counter
- `gunicorn` — structural fork events only
- `uvicorn` — full ASGI middleware

## Paid probe libraries

The full probe set — nginx, Celery, Redis, database kprobe — ships with the corresponding book chapters at [stacktracer.dev](https://stacktracer.dev).

---

# Causal Rules Overview

Rules are plain Python predicates evaluated against the `RuntimeGraph`. Each rule returns `(matched: bool, evidence: dict)`.

## Built-in starter rules

### Loop starvation

Fires when asyncio loop ticks average over 10ms. A blocking operation — CPU work, synchronous I/O, missing await — is starving other coroutines.

```python
# fires when:
n.avg_duration_ns > 10_000_000  # >10ms per tick
```

### N+1 query

Fires when a database query node is called more than 2× per view call. Classic ORM N+1 pattern.

```python
# fires when:
node.call_count >= caller.call_count * 2
```

### Retry amplification

Fires when downstream edges have high retry counts. A slow dependency is being amplified by retry loops.

```python
# fires when:
e.metadata.get("retries", 0) > 3
```

### New sync call after deployment

Fires when new call edges appeared after the most recent deployment marker. A newly introduced synchronous dependency is the probable root cause of latency.

```python
# fires when:
new_edges_since_deployment and ":calls" in edge_key
```

## Running rules

```
› CAUSAL
```

Or filter by tag:

```
› CAUSAL latency
› CAUSAL db
› CAUSAL asyncio
```

---

# Writing Custom Rules

Drop a `*_rules.py` file anywhere in your project. StackTracer discovers it automatically at startup.

## Minimal example

```python
# myapp/stacktracer/rules/payment_rules.py
from stacktracer.core.causal import CausalRule


def _slow_payment(graph, temporal):
    """Fire if payment.initialize averages over 2 seconds."""
    nodes = [
        n for n in graph.all_nodes()
        if n.service == "payment"
        and n.avg_duration_ns
        and n.avg_duration_ns > 2_000_000_000
    ]
    if not nodes:
        return False, {}
    return True, {
        "slow_nodes": [
            {"node": n.id, "avg_ms": round(n.avg_duration_ns / 1e6, 1)}
            for n in nodes
        ]
    }


def register(registry):
    registry.register(CausalRule(
        name        = "slow_payment_gateway",
        description = "Payment gateway averaging over 2s — check Paystack status.",
        predicate   = _slow_payment,
        confidence  = 0.85,
        tags        = ["payment", "latency"],
    ))
```

## Rule anatomy

```python
CausalRule(
    name        = "rule_name",          # unique identifier
    description = "Human explanation",  # shown in REPL output
    predicate   = my_fn,                # (graph, temporal) → (bool, dict)
    confidence  = 0.85,                 # 0.0–1.0
    tags        = ["latency"],          # for CAUSAL <tag> filtering
)
```

The predicate receives the live `RuntimeGraph` and `TemporalStore`. It must never raise — exceptions are caught and logged with `confidence=0.0`.

---

# REPL Overview

The REPL connects to the engine via a Unix socket (`/tmp/stacktracer-*.sock`) and queries it in real time while your app runs.

## Start

```bash
python -m stacktracer.scripts.repl
```

## Command reference

| Command | Description |
|---|---|
| `SHOW nodes` | All graph nodes with call count and avg duration |
| `SHOW edges` | All graph edges with call count |
| `SHOW events LIMIT 20` | Recent probe events |
| `SHOW latency` | Nodes sorted by avg duration |
| `SHOW active` | Currently in-flight requests |
| `SHOW status` | Engine health, buffer depth, dropped events |
| `SHOW probes` | Active probes |
| `CAUSAL` | Run all causal rules |
| `CAUSAL <tag>` | Run rules filtered by tag |
| `DIFF SINCE deployment` | Graph changes since last deployment marker |
| `MARK DEPLOYMENT <label>` | Mark a deployment boundary |
| `TRACE <trace_id>` | Events for one trace |
| `\stitch <trace_id>` | Merge multi-process graphs into one timeline |
| `HOTSPOT TOP 10` | Top 10 nodes by call count |
| `BLAME <node>` | Upstream callers of a node |

## \stitch command

`\stitch` is the centrepiece. It takes a `trace_id` visible in `SHOW events` and reconstructs the full causal timeline across process boundaries — gunicorn worker + celery worker + redis — into one ordered sequence of stages with durations.

```
› \stitch fbaa0af4-2305-42df-97db-caec54bd65a1

  nginx::accept          0.2ms
  uvicorn::receive       0.1ms
  django::/api/orders/   4.3ms
    django::OrderView    4.1ms
    django::SELECT...    3.8ms  ← 90 calls
  celery::send_email     12.4ms
  ─────────────────────────────
  total                  17.3ms
```

---

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
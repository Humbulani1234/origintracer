# OriginTracer

**Live causal graph for complex async services.**

OriginTracer instruments your full production stack - **nginx --> gunicorn/uvicorn --> Django/FastAPI --> asyncio --> Celery** - to reveal *why* execution flowed the way it did, not just that it was slow. It builds a real-time causal graph, automatically detects anti-patterns, and lets you query the live system with powerful REPL commands like `BLAME`, `DIFF SINCE deployment`, or `CAUSAL`.

It combines:
- Deep, source-grounded "traced book" chapters that teach framework and kernel internals via real pivot points.
- Ready-to-run **rule libraries** that turn that knowledge into automatic detectors.
- A clean, open-source engine that stays non-blocking and production-safe.

The result: actionable insight into opaque async behavior, cross-process flows, and hidden latency sources that traditional tracing often misses.

[origintracer.app](https://origintracer.app)

## Why OriginTracer

Most observability tools show you *what* happened (spans, metrics, logs). OriginTracer shows *why* - the causal relationships across layers, including kernel-level events.

- **Causal graph** with intelligent deduplication and compaction (stable even under high load)
- **Automatic causal rules** (N+1 queries, asyncio loop starvation, worker imbalance, retry amplification, etc.)
- **Cross-process stitching** — merge timelines across gunicorn workers, Celery, and nginx
- **Native deep probes** (including eBPF/kprobes for nginx) *or* OpenTelemetry bridge mode
- **Zero blocking** of the request/response cycle via async draining and fire-and-forget buffers
- **Extensible by design** - anyone can add custom probes and rules

Benchmarks on a 4-core setup show ~22 ms mean overhead per request at 175+ req/s bursts, with zero dropped events thanks to background processing and deduplication.

## Multi-Language Potential

OriginTracer's core engine is **language-agnostic**. It only understands a clean, normalized event protocol (`NormalizedEvent`). 

This means:
- The engine, causal graph, deduplication, temporal snapshots, REPL, causal rules, and React UI work independently of the source language.
- Any language can feed events into the engine as long as they follow the protocol.

**Node.js support is coming soon**, but you can start working on it now.

### For Node.js Developers

If you're working with Node.js (Express, Fastify, NestJS, etc.), you can already start building **custom probes** that emit events to the OriginTracer engine. 

Because the engine only cares about the normalized protocol, a well-written Node.js probe can:
- Capture request lifecycle, middleware execution, async operations, database calls, or event loop behavior
- Participate in the same causal graph as Python probes
- Trigger the same powerful causal rules (N+1 style patterns, hotspot detection, retry amplification, etc.)
- Use cross-language stitching once multiple services are instrumented

**Example flow for Node.js**:
1. Write a lightweight probe (e.g., using Async Hooks, diagnostics_channel, or library-specific hooks)
2. Emit events matching the `NormalizedEvent` schema (or via a thin bridge)
3. Drop your probe into the shared engine — the causal graph, REPL, and rules work immediately

This makes OriginTracer a powerful **polyglot observability backend** for teams running mixed Python + Node.js services.

**We're actively working on official Node.js probes** (Express/Fastify middleware, async context, event loop delays, etc.). In the meantime, the open engine and clear event protocol make it easy for the community to experiment and contribute.

Want to build the first Node.js probe? Open an issue or PR - we'd love to collaborate and give early feedback.

## Quick Start – Django

```bash
pip install origintracer
```

**1. Add middleware** (must be first in `settings.py`):

```python
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware",  # required for trace_id propagation
    "django.middleware.security.SecurityMiddleware",
    # ...
]
```

**2. Initialize in `apps.py`**:

```python
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import origintracer
        origintracer.init(debug=True)  # set debug=False in production
```

**3. Create `origintracer.yaml`** in your project root:

```yaml
probes:
  - django
  - asyncio
  - gunicorn
  # - nginx          # deeper version available on origintracer.app
```

**4. Run your app** and explore:

```bash
gunicorn -c gunicorn.conf.py config.asgi:application \
  --worker-class uvicorn.workers.UvicornWorker
```

Open the REPL:

```bash
python -m origintracer.repl
```

Try:
- `SHOW nodes`
- `SHOW edges`
- `CAUSAL`
- `\stitch <trace_id>` (merges across processes)

## Quick Start – Celery

Celery workers automatically receive their own engine instance. Just add `celery` to your `probes` list in `origintracer.yaml`. The Celery probe handles `worker_process_init` for you.

## Core Strengths

- **Decoupled architecture**: Probes emit `NormalizedEvent` objects. The engine never touches probes, and probes never block application code.
- **Temporal & causal intelligence**: Graph snapshots, deployment markers (`mark_deployment()`), and rules that compare against historical behavior.
- **Extensibility**: Auto-discovers custom probes and rules from `yourapp/origintracer/probes/*.py` and `rules/*.py`. Write once, register via a simple `register(registry)` function.
- **Dual modes**: Native deep probes (best for asyncio internals, nginx kprobes, gunicorn lifecycle) or lightweight OpenTelemetry bridge mode.

## Probe Overview

**Built-in (free)**:
- **django** — Full request lifecycle, URL resolution, view dispatch (sync + async)
- **asyncio** — Loop ticks, `_run_once`, selector events — makes event loop starvation visible

**Advanced (available on origintracer.app)**:
- **nginx** — Dual mode: eBPF/kprobes (accept4, epoll_wait, send/recv) + JSON log tail fallback. Includes master/worker topology discovery and request enrichment.
- **gunicorn** — Worker spawn, init, heartbeat, and request handling
- **uvicorn** — ASGI lifecycle with automatic X-Request-ID correlation from nginx

Custom probes follow the same `BaseProbe` pattern and are auto-discovered.

## Extending OriginTracer

### Writing a Custom Probe

Place files in `yourapp/origintracer/probes/myprobe.py`:

```python
from origintracer.sdk.base_probe import BaseProbe
from origintracer.sdk.emitter import emit
from origintracer.core.event_schema import NormalizedEvent, ProbeTypes

ProbeTypes.register("celery.task.start", "Celery task started")

class CeleryProbe(BaseProbe):
    name = "celery"

    def start(self):
        from celery.signals import task_prerun, task_postrun
        task_prerun.connect(self._on_start, weak=False)
        # ...

    def _on_start(self, task_id, task, **kwargs):
        emit(NormalizedEvent.now(
            probe="celery.task.start",
            trace_id=...,
            service="celery",
            name=task.name,
            # ...
        ))
```

### Writing a Causal Rule

Place in `yourapp/origintracer/rules/myrules.py`:

```python
from origintracer.core.causal import CausalRule, PatternRegistry

def register(registry: PatternRegistry):
    registry.register(CausalRule(
        name="sync_db_in_celery",
        description="Celery task making blocking DB calls",
        tags=["celery", "blocking"],
        predicate=_detect_blocking_db,
        confidence=0.85,
    ))
```

Rules receive the live graph and can emit evidence with confidence scores.

## Additional Features

- **REPL + React UI** — Terminal-style live dashboard (nodes, edges, timelines, event log)
- **Cross-process stitching** via Unix sockets for multi-worker and Celery visibility
- **OpenTelemetry bridge mode** — Use existing OTel instrumentation while still getting OriginTracer’s causal rules and graph. **[still experimental]**
- **Windows support** via WSL2 (required for Unix sockets and full kprobe features)

## Performance & Production Notes

- Mean overhead ~22 ms per request in high-concurrency tests (async draining keeps the request path unblocked)
- Deduplication engine prevents graph bloat even after thousands of requests
- Designed for production: fire-and-forget buffers, background tasks, graceful shutdown

## Development

```bash
pip install -e ".[dev]"
pytest origintracer/tests/ -q
pre-commit install
```

See `docs/` for full architecture, probe internals, and contribution guidelines.

## License

- **Core engine** (graph, engine, emitter, auto-discovery, REPL, etc.): MIT
- Advanced probes, rule libraries, and traced book chapters: Commercial (available on [origintracer.app](https://origintracer.app))

The open core is designed for wide use and community contributions. High-value, battle-tested extensions (especially nginx eBPF depth and causal rules derived from the traced books) remain commercial to sustain development.

---

**Made for engineers who want to move beyond "it's slow" to "here's exactly why - and how to fix it."**

Questions or ideas? Open an issue or reach out via the site.

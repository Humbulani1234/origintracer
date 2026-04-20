<div align="center">
<h1>OriginTracer</h1>

<h3>

[Website](https://origintracer.app/)

</h3>

![Backend](https://img.shields.io/badge/Backend-FastAPI-green?style=flat-square)
![Frontend](https://img.shields.io/badge/Frontend-React_UI_%26_REPL-blue?style=flat-square)
![Website](https://img.shields.io/badge/Website-origintracer.app-orange?style=flat-square&link=https://origintracer.app)
![License](https://img.shields.io/badge/license-MIT-red?style=flat-square)
![eBPF](https://img.shields.io/badge/Kernel-eBPF_Powered-blueviolet?style=flat-square)


![WSL2](https://img.shields.io/badge/WSL2-0078D4?style=for-the-badge&logo=windows&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black)

</div>

**Live causal graph for async services.**

OriginTracer instruments your full production stack - **nginx --> gunicorn --> asynico --> uvicorn --> django/fastapi --> celery** - to reveal *why* execution flowed the way it did, not just that it was slow. It builds a real-time causal graph, automatically detects anti-patterns, and lets you query the live system with REPL commands like `BLAME`, `DIFF SINCE deployment`, or `CAUSAL`.

It combines:
- Deep, source-grounded "traced book" chapters that teach framework and kernel internals via real pivot points.
- Ready-to-run **rule libraries** that turn that knowledge into automatic detectors.
 Ready-to-run **probe libraries** that emit events of interest from your application.
- A clean, open-source engine that stays non-blocking and efficient.

The result: actionable insight into async behavior, cross-process flows, and hidden latency sources that traditional tracing often misses.

Vist the website for more details and traced chapters - [origintracer.app](https://origintracer.app)

## Why OriginTracer

Most observability tools show you *what* happened (spans, metrics, logs). OriginTracer shows *why* - the causal relationships across layers, including kernel-level events.

- **Causal graph** with deduplication and compaction (stable even under high load)
- **Automatic causal rules** (N+1 queries, asyncio loop starvation, worker imbalance, retry amplification, etc.)
- **Cross-process stitching** — merge timelines across gunicorn workers, Celery, and nginx
- **Native deep probes** (including eBPF/kprobes for nginx and asyncio) *or* OpenTelemetry bridge mode (experimental)
- **Zero blocking** of the request/response cycle via async draining and fire-and-forget buffers
- **Extensible by design** - anyone can add custom probes and rules

## Multi-Language Support Potential

OriginTracer’s core engine is **completely language-agnostic**. It only understands a clean, normalized event protocol (`NormalizedEvent`). The graph, deduplication, causal rules, REPL, and UI have no idea whether events came from Python, Node.js, or another language.

This design makes OriginTracer uniquely positioned as a **unified causal observability backend**.

### For Node.js Developers

You can already start building probes for Node.js services (Express, Fastify, NestJS, etc.). A Node.js probe can:

- Hook into request/response lifecycle, middleware, async operations, or event loop behavior
- Emit events that seamlessly join the same causal graph as your Python services
- Trigger the same powerful rules (N+1-style patterns, hotspots, retry amplification, blocking calls, etc.)
- Enable cross-language stitching between Node.js and Python microservices

**Official Node.js probes** (covering Express/Fastify middleware, async context tracking, event loop delays, etc.) are in the roadmap.

If you want to help accelerate this, the open engine and clear event protocol make experimentation straightforward.

## Installation

```bash
git clone https://github.com/Humbulani1234/origintracer.git
cd origintracer 
pip install -e .
```

## Quick Start - Django application

Navigate to the `applications/django` directory for this specific application.

**1. Add middleware** (must be first in `settings.py`):

```python
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware", # required for trace_id propagation
    "django.middleware.security.SecurityMiddleware",
    # ...
]
```

**2. Create `origintracer.yaml`** in your project root:

```yaml
probes:
  - django
  - asyncio
  - gunicorn # available on origintracer.app
  - uvicorn # available on origintracer.app
  - nginx  # available on origintracer.app
```

**3. Initialize in `apps.py`**:

```python
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import origintracer
        origintracer.init(debug=True, config=BASE_DIR / "origintracer.yaml")
```

**4. Run your app**:

```bash
gunicorn -c gunicorn.conf.py config.asgi:application \
  --worker-class uvicorn.workers.UvicornWorker
```

Send requests to your views using the provided `load_test` and `burst_test` scripts and explore the results with the REPL.

Open the REPL:

```bash
python -m origintracer.repl.repl
```

Try:
- `SHOW nodes`
- `SHOW edges`
- `CAUSAL`
- `\stitch <trace_id>` (merges across processes)

## Quick Start - Celery application

Similar to the `Django` application above. Navigate to the `applications/celery` directory for this specific application, and follow the steps.

## Core Strengths

- **Decoupled architecture**: Probes emit `NormalizedEvent` objects. The engine never touches probes, and probes never block application code.
- **Temporal & causal intelligence**: Graph snapshots, deployment markers (`mark_deployment()`), and rules that compare against historical behavior.
- **Extensibility**: Auto-discovers custom probes and rules from `yourapp/origintracer/probes/*.py` and `rules/*.py`. Write once, register via a simple `register(registry)` function.
- **Dual modes**: Native deep probes (best for asyncio internals, nginx kprobes, gunicorn lifecycle) or lightweight OpenTelemetry bridge mode.

## Probe Overview

**Built-in**:
- **django** - Full request lifecycle, URL resolution, view dispatch (sync + async)
- **asyncio** - Loop ticks, `_run_once`, selector events - makes event loop starvation visible

**Advanced** - available on [origintracer.app](https://origintracer.app):
- **nginx** - Dual mode: eBPF/kprobes (accept4, epoll_wait, send/recv) + JSON log tail fallback. Includes master/worker topology discovery and request enrichment.
- **gunicorn** - Worker spawn, init, heartbeat, and request handling
- **uvicorn** - ASGI lifecycle with automatic X-Request-ID correlation from nginx

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

## Benchmarks

OriginTracer is engineered for early production workloads, not just local debugging.

### Stress Test Highlights: 4-core machine

- **4,000 requests** processed with **zero errors** and **zero dropped events** (`ok=4000  err=0  dropped=0`)
- **48,000 events** captured, decoded, correlated, and drained (~12k events per wave × 4 waves).  
  Buffer depth stayed at `buf_depth=0` and `in_flight=0` after every wave - no backpressure observed.
- **Latency impact remained low and stable**:
  - 50 req burst:  mean ≈ 23 ms, p95 = 39 ms, p99 = 44 ms
  - 1000 req burst: mean ≈ 21 ms → 20 ms, p95 ≈ 33 --> 31 ms,
    p99 ≈ 44--> 35 ms
  - One isolated p99 spike to 70 ms recovered quickly to ~37 ms.
- **Sustained throughput**: **393 requests/second** through a single worker with no special tuning.

### Takeaways

- eBPF/kprobe overhead is effectively unmeasurable at this traffic level.
- The Python-side event pipeline (decoding, correlation, graph updates) introduces no meaningful backpressure.
- Graph deduplication and normalization stay solid - the causal graph converged quickly and remained stable even after 48,000 events.
- The design keeps the request/response path completely non-blocking.

These results demonstrate that OriginTracer delivers deep cross-layer visibility (kernel timings, nginx internals, asyncio behavior, causal relationships) while staying production-viable. The only notable deployment consideration is elevated privileges for full eBPF mode.

This level of stability gives strong confidence for running OriginTracer alongside real traffic.

## Development

```bash
pip install -e ".[dev]"
pytest origintracer/tests/ -q
pre-commit install
```

See `docs/` for full architecture, probe internals, and contribution guidelines.

---

**Made for engineers who want to move beyond "it's slow" to "here's exactly why - and how to fix it."**

Questions or ideas? reach out via the [site](https://origintracer.app)

# OriginTracer

**Probe-based Python runtime observability.**

StackTracer instruments the framework hooks - Django middleware, asyncio `Task.__step`, Gunicorn fork callbacks, Uvicorn ASGI middleware - and builds a live causal graph of your running stack from the signals they emit. No code changes in your views.

```
gunicorn::master  ── spawned ──>  gunicorn::UvicornWorker-24861
                  ── handled ──>  uvicorn::/api/users/
                  ── calls ──>    django::/api/users/
                  ── calls ──>    django::NPlusOneView  ×180
                  ── calls ──>    django::SELECT "auth_user"...  ×90
```

The N+1 pattern surfaces structurally - `NPlusOneView` has 180 calls, the query has 90. No query analysis needed.

---

## What it does

=== "Graph"
    Builds a live `RuntimeGraph` of your stack topology - nodes are services and routes, edges are causal relationships. The graph converges after warmup: 24 nodes after 100 requests or 100,000.

=== "Rules"
    Evaluates causal rules against the graph. Rules detect loop starvation, N+1 queries, retry amplification, worker imbalance, and more. Rules are plain Python predicates - write your own in a `.py` file.

=== "REPL"
    A terminal REPL that queries the live engine via Unix socket. `SHOW nodes`, `SHOW edges`, `CAUSAL`, `DIFF SINCE deployment`, `\stitch <trace_id>` - all from a second terminal while your app runs.

=== "React UI"
    A terminal-aesthetic dashboard - nodes, edges, trace timeline, event log, deployment diffs. Connects to the FastAPI backend.

---

## Quick start

```bash
git clone https://github.com/Humbulani1234/origintracer.git
cd origintracer 
pip install -e .
```

Place your django project in the `applications` directory.

```python
# settings.py - TracerMiddleware must be first
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware",
    "django.middleware.security.SecurityMiddleware",
    # ...
]
```

```yaml
# origintracer.yaml - project root
probes:
  - django
  - asyncio
```

```python
# apps.py
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import origintracer
        origintracer.init(debug=True, onfig=BASE_DIR / "origintracer.yaml")
```

Send requests to your views, and open the REPL in a second terminal:

```bash
python -m origintracer.repl.repl

› SHOW nodes
› CAUSAL
```

---

## Architecture

```mermaid
graph LR
    A[Probes] -->|NormalizedEvent| B[EventBuffer]
    B -->|drain| C[Engine]
    C --> D[RuntimeGraph]
    C --> E[TemporalStore]
    C --> F[ActiveRequestTracker]
    D --> G[PatternRegistry]
    D --> H[REPL/React UI]
    C -->|Uploader| I[FastAPI Backend]
```

---

## Open source model

OriginTracer is MIT licensed.

The **rules and probes libraries** that ship with the book chapters are available at [origintracer.dev](https://origintracer.app). Each chapter covers one layer of the stack - asyncio, Django, nginx, Gunicorn, Celery, database internals - with PDB and GDB traces, and a companion `.py` rule and probe files that detects the patterns described in the chapter automatically in your running system.

Four starter rules and two probes ship with the package.

---

See [Getting Started](getting-started/installation.md) for full instructions.
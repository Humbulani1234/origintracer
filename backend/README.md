# OriginTracer Backend - Experimental

FastAPI backend that receives graph snapshots and events from OriginTracer, serves graph queries, and exposes the runtime graph to the React UI.

---

## What it does

The backend is stateless except for graph snapshots. OriginTracer (Django/Celery
process) owns the live `RuntimeGraph` - it builds it, updates it, and snapshots
it. The backend receives those snapshots, stores them in memory (or PostgreSQL),
and serves read queries against them.

```
OriginTracer process                    Backend
─────────────────────────────────────────────────────
RuntimeGraph (live)
  >> Uploader flushes every 10s
  >> POST /api/v1/events >> persists raw events
  >> POST /api/v1/graph/snapshot >> deserialises + stores graph
                                 >> GET /api/v1/graph
                                 >> GET /api/v1/status
                                 >> GET /api/v1/hotspots
                                 >> POST /api/v1/query
```

---

## Prerequisites

```bash
pip install fastapi uvicorn httpx msgpack
```

---

## Quick start - in-memory (dev)

```bash
cd backend

ORIGINTRACER_API_KEYS=test-key-123:local-dev \
uvicorn main:app --host 0.0.0.0 --port 8000 --log-level info
```

`test-key-123` is the API key. `local-dev` is the customer_id used as the
storage key. Use the same key in `stacktracer.init()` on the agent side.

---

## Quick start - PostgreSQL

```bash
STACKTRACER_API_KEYS=test-key-123:local-dev \
STACKTRACER_DB_DSN=postgresql://user:password@localhost/stacktracer \
uvicorn main:app --host 0.0.0.0 --port 8001 --log-level info
```

Create the database first:

```sql
CREATE DATABASE origintracer;
```

The backend creates tables on startup automatically.

---

## OriginTracer configuration

In the Django app's `apps.py`:

```python
origintracer.init(
    api_key  = "test-key-123",
    endpoint = "http://localhost:8001",
    debug = True,
)
```

The Uploader starts automatically when `api_key` is set. It batches events
and flushes every `flush_interval` seconds (default 10). Graph snapshots
are sent every `snapshot_interval` seconds (default 15).

---

## API reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/events` | Receive probe events from agent (msgpack or JSON) |
| `POST` | `/api/v1/graph/snapshot` | Receive serialised RuntimeGraph from agent |
| `GET`  | `/api/v1/status` | Snapshot metadata and storage info |
| `GET`  | `/api/v1/graph` | Full graph, optionally filtered by `?service=` or `?system=` |
| `POST` | `/api/v1/query` | Execute DSL query — `{"query": "SHOW nodes"}` |
| `GET`  | `/api/v1/hotspots` | Top N nodes by call count — `?top=10` |
| `GET`  | `/api/v1/causal` | Run causal rules — `?tags=latency,async` |
| `GET`  | `/api/v1/traces/{id}` | Critical path for a trace (requires event storage) |
| `POST` | `/api/v1/deployment` | Mark a deployment `{"label": "deployment"}` |
| `GET`  | `/health` | Liveness probe |

---
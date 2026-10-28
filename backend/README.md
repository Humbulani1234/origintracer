# OriginTracer Backend - Experimental

FastAPI backend that receives graph snapshots and events from OriginTracer, serves graph queries, and exposes the runtime graph to the React UI.


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

## Prerequisites

```bash
pip install fastapi uvicorn httpx msgpack
```


## Quick start - in-memory (dev)

```bash
cd backend

ORIGINTRACER_API_KEYS=test-key-123:local-dev \
uvicorn main:app --host 0.0.0.0 --port 8000 --log-level info
```

`test-key-123` is the API key. `local-dev` is the customer_id used as the
storage key. Use the same key in `origintracer.init()` on the agent side.


## Quick start - PostgreSQL

```bash
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
ORIGINTRACER_DB_DSN=postgresql://user:password@localhost/origintracer \
uvicorn backend.main:app --host 0.0.0.0 --port 8001 --log-level info
```

**Note**: when running with ReactUI start the server first, then `OriginTracer` tool for it to send the `deployment marker`

Create the database first:

```sql
CREATE DATABASE origintracer;
```

The backend creates tables on startup automatically.


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


## API reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/events` | Receive probe events from agent (msgpack or JSON) |
| `POST` | `/api/v1/graph/snapshot` | Receive serialised RuntimeGraph from agent |
| `GET`  | `/api/v1/snapshots` | Latest stored snapshot metadata for each worker instance |
| `GET`  | `/api/v1/snapshots/{worker_instance_id}` | Graph and metadata for one process instance |
| `GET`  | `/api/v1/status?worker_instance_id=...` | Snapshot metadata for one process instance |
| `GET`  | `/api/v1/graph?worker_instance_id=...` | Full graph for one process instance; optional `service` / `system` filters |
| `POST` | `/api/v1/query` | Execute DSL query — `{"query": "SHOW nodes"}` |
| `GET`  | `/api/v1/hotspots?worker_instance_id=...` | Top N nodes by call count for one process instance |
| `GET`  | `/api/v1/causal?worker_instance_id=...` | Run causal rules for one process instance |
| `GET`  | `/api/v1/traces/{id}` | Critical path for a trace (requires event storage) |
| `POST` | `/api/v1/deployment` | Mark a deployment `{"label": "deployment"}` |
| `GET`  | `/health` | Liveness probe |


# Live worker selection

OriginTracer keeps one runtime graph inside each instrumented process. The React
dashboard therefore selects a process before requesting live graph data; it does
not merge or relocate those graphs.

## Current workflow

```text
Instrumented process
  -> /tmp/origintracer-{pid}.sock
  -> LocalQueryServer evaluates the existing DSL against that process's Engine

React dashboard
  -> GET /api/v1/workers
  -> selects a discovered PID
  -> POST /api/v1/workers/{pid}/query {"query": "SHOW GRAPH"}

FastAPI
  -> resolves the PID against the current socket inventory
  -> sends newline-delimited JSON through the Unix socket
  -> returns that worker's response to React
```

The browser never receives authority to choose an arbitrary filesystem path.
FastAPI constructs the path from the integer PID and accepts it only while that
path appears in `discover_sockets()`.

React automatically selects the first live process, preserves the selection
while that PID remains available, and falls back to another live process if it
exits. When no local socket is available, it clears live-process data and
displays an explicit `No live OriginTracer processes` state. A saved snapshot
can be selected separately; the dashboard labels it as a snapshot rather than
a live process.

Persistence-backed features remain separate: causal history can still be
browsed, and an explicit `\\stitch <trace_id>` lookup can still retrieve a
historical trace when no live worker exists.

This workflow requires FastAPI to share the instrumented processes' `/tmp` and
PID namespace. It is intended for the local/live inspection surface represented
by the existing REPL.

## Process-scoped snapshots

Each OriginTracer process generates a stable `worker_instance_id` and reports it
with its OS PID. Snapshot, diff, deployment-marker, and causal-history records
are scoped to that instance. The PID is display metadata; the instance ID is
the storage key because PIDs can be reused. Snapshot uploads must include both
`X-OriginTracer-Worker-Instance-ID` and `X-OriginTracer-Worker-PID` headers.

The React dashboard lists live processes separately from saved snapshots. A
saved snapshot is selected by instance ID and can be read after its process has
exited. Snapshot-backed graph endpoints require `worker_instance_id`; they do
not choose a customer's newest process implicitly. The live worker query API
continues to select by PID and query the process socket directly.

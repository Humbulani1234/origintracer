# OriginTracer Backend - Experimental

FastAPI backend that receives graph snapshots and events from OriginTracer and
serves stored-data queries to the React UI and investigation agent.


## What it does

OriginTracer (Django/Celery process) owns the live `RuntimeGraph`; it builds,
updates, and uploads snapshots. FastAPI serves dashboard graph queries from
those uploaded snapshots, and serves events/traces from the event store. With
PostgreSQL configured, snapshots and events persist across backend restarts and
hosts. The in-memory repository is intended for local development. The React UI
does not inspect a worker's in-memory graph or query its REPL socket.

```
OriginTracer process                    Backend
─────────────────────────────────────────────────────
RuntimeGraph (live)
  >> Uploader flushes every 10s
  >> POST /api/v1/events >> persists raw events
  >> POST /api/v1/graph/snapshot >> deserialises + stores graph
  >> POST /api/v1/worker/runtime >> stores uptime, activity, probes, heartbeat
                                 >> GET /api/v1/graph
                                 >> GET /api/v1/events/content
                                 >> GET /api/v1/status
                                 >> GET /api/v1/hotspots
                                 >> GET /api/v1/events
                                 >> GET /api/v1/traces/{id}
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
| `GET`  | `/api/v1/events/content?worker_instance_id=...&kind=...` | Captured LangGraph LLM or tool previews from stored events; `kind` is `llm` or `tool` |
| `POST` | `/api/v1/graph/snapshot` | Receive serialised RuntimeGraph from agent |
| `POST` | `/api/v1/worker/runtime` | Store uploader heartbeat and runtime-only status for a process instance |
| `GET`  | `/api/v1/snapshots` | Latest stored snapshot metadata for each worker instance |
| `GET`  | `/api/v1/snapshots/{worker_instance_id}` | Graph and metadata for one process instance |
| `GET`  | `/api/v1/status?worker_instance_id=...` | Snapshot metadata, latest runtime heartbeat, and freshness for one process instance |
| `GET`  | `/api/v1/graph?worker_instance_id=...` | Full graph for one process instance; optional `service` / `system` filters |
| `POST` | `/api/v1/query` | Execute DSL query — `{"query": "SHOW nodes"}` |
| `GET`  | `/api/v1/hotspots?worker_instance_id=...` | Top N nodes by call count for one process instance |
| `GET`  | `/api/v1/causal?worker_instance_id=...` | Evaluate causal rules for one process instance without saving |
| `POST` | `/api/v1/causal/report` | Receive current matches evaluated by the instrumented process |
| `POST` | `/api/v1/causal/evaluate` | Evaluate causal rules and save matches for one process instance |
| `GET`  | `/api/v1/causal/history?worker_instance_id=...` | Read saved causal match history |
| `GET`  | `/api/v1/traces/{id}` | Critical path for a trace (requires event storage) |
| `POST` | `/api/v1/deployment` | Mark a deployment `{"label": "deployment"}` |
| `GET`  | `/health` | Liveness probe |

Captured-content responses expose only the bounded prompt, response, tool
input, or tool output/error preview fields, plus trace and investigation IDs.
They are read from uploaded event storage and scoped to one worker instance;
they do not query a live worker socket. LLM and tool capture are opt-in in the
instrumented process with `ORIGINTRACER_CAPTURE_LLM_CONTENT=1` and
`ORIGINTRACER_CAPTURE_TOOL_CONTENT=1`, respectively. Events uploaded before
the backend began attaching worker-instance IDs cannot be selected by this
endpoint.


# Worker selection and freshness

OriginTracer keeps its live runtime graph inside each instrumented process.
The uploader sends graph snapshots, events, and a runtime heartbeat to FastAPI.
React and the investigation agent read the stored graph and event APIs using
the stable `worker_instance_id`; neither needs the worker's Unix socket or a
shared PID namespace.

The snapshot inventory retains process instances after they stop. A fresh
heartbeat marks a process as reporting, while the dashboard can still inspect
the last stored graph for a stale process. Heartbeats include uptime, active
request count, active probes and rules, graph counts, semantic-label count,
and event-log size. FastAPI reports freshness based on the configured snapshot interval,
with a 30-second minimum and five-minute maximum. This is a remote heartbeat,
not an operating-system process check.

FastAPI serves uploaded data only and does not discover local worker sockets or
proxy queries to an instrumented process. Live in-memory inspection remains in
the OriginTracer REPL and local query server.

## Process-scoped snapshots

Each OriginTracer process generates a stable `worker_instance_id` and reports it
with its OS PID. Snapshot, diff, deployment-marker, and causal-history records
are scoped to that instance. The PID is display metadata; the instance ID is
the storage key because PIDs can be reused. Snapshot uploads must include both
`X-OriginTracer-Worker-Instance-ID` and `X-OriginTracer-Worker-PID` headers.

The React dashboard lists stored snapshots by instance ID and can read them
after their process has exited. Snapshot-backed graph endpoints require
`worker_instance_id`; they do not choose a customer's newest process
implicitly. The separate live worker query API continues to select by PID and
query the process socket directly for REPL/development clients.

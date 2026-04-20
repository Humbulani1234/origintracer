# FastAPI Backend — Overview

The FastAPI backend is the persistence, query, and UI surface for OriginTracer.

## Architecture

OriginTracer owns the graph and builds it locally inside the agent process.
The backend receives serialised graph snapshots at every configured interval,
deserialises them, and serves all queries from the deserialised graph.

```
Agent process                        FastAPI backend
─────────────────────────────────    ──────────────────────────────
RuntimeGraph (live, in-memory)  ──►  POST /api/v1/graph/snapshot
                                          │
                                          ▼
                                     deserialise + store
                                          │
                                          ▼
                                     serve GET queries
```

The backend does not rebuild a graph from raw events — that is the agent's
responsibility. On startup, the latest snapshot is loaded from the storage
backend so queries work immediately after a FastAPI restart without waiting
for the next snapshot interval.

## Running

```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8001
```

## Environment Variables

| Variable | Description |
|---|---|
| `ORIGINTRACER_API_KEYS` | Comma-separated `key:customer` pairs e.g. `sk_dev_yyy:dev_customer` |
| `ORIGINTRACER_DB_DSN` | PostgreSQL DSN for event and snapshot storage |

## Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/graph/snapshot` | Receive serialised graph from agent |
| `POST` | `/api/v1/events` | Receive raw events for persistence |
| `POST` | `/api/v1/deployment` | Store a deployment marker |
| `GET` | `/api/v1/graph` | Current graph from latest snapshot |
| `GET` | `/api/v1/traces/{id}` | Critical path from event store |
| `GET` | `/api/v1/causal` | Causal rules evaluated on latest snapshot |
| `GET` | `/api/v1/causal/history` | Historical causal match snapshots |
| `GET` | `/api/v1/hotspots` | Top N busiest nodes |
| `GET` | `/api/v1/diff` | Graph diff since a marker |
| `GET` | `/api/v1/status` | Snapshot metadata and system info |
| `GET` | `/health` | Liveness probe |


# API Reference

All endpoints except `/health` require an `Authorization: Bearer <key>` header.
Keys are configured via `ORIGINTRACER_API_KEYS`.

---

## Graph

### `POST /api/v1/graph/snapshot`

Receive a serialised `RuntimeGraph` from the agent. Called automatically
by the agent at every `snapshot_interval`. Not intended for manual use.

**Body:** `application/msgpack` or `application/json` — serialised graph bytes.

---

### `GET /api/v1/graph`

Return the current graph from the latest snapshot.

**Response:**
```json
{
  "nodes": [...],
  "edges": [...],
  "snapshot_at": 1718023400.0
}
```

---

### `GET /api/v1/diff`

Graph diff since a named marker (e.g. `deployment`).

**Query params:**

| Param | Type | Description |
|---|---|---|
| `since` | `str` | Marker label e.g. `deployment` |

---

### `GET /api/v1/hotspots`

Top N busiest nodes by call count.

**Query params:**

| Param | Type | Default | Description |
|---|---|---|---|
| `top` | `int` | `10` | Number of nodes to return |

---

## Traces

### `GET /api/v1/traces/{id}`

Reconstruct the critical path for a trace from the event store.

**Path params:**

| Param | Description |
|---|---|
| `id` | `trace_id` string |

---

## Causal

### `GET /api/v1/causal`

Evaluate all registered causal rules against the latest snapshot.

**Response:**
```json
{
  "matches": [
    {
      "rule": "n_plus_one_queries",
      "confidence": 0.90,
      "tags": ["db", "performance"],
      "payload": {...}
    }
  ]
}
```

---

### `GET /api/v1/causal/history`

Return historical causal match snapshots from the storage backend.

**Query params:**

| Param | Type | Default | Description |
|---|---|---|---|
| `limit` | `int` | `50` | Number of snapshots to return |

---

## Events

### `POST /api/v1/events`

Receive raw `NormalizedEvent` objects for persistence.

**Body:** JSON array of event dicts.

---

## Deployment

### `POST /api/v1/deployment`

Store a deployment marker. Used by `DIFF SINCE deployment` in the REPL
and the causal rule `new_sync_call_after_deployment`.

**Body:**
```json
{"label": "deployment"}
```

---

## System

### `GET /api/v1/status`

Snapshot metadata and system info — node count, edge count, uptime,
last snapshot timestamp.

---

### `GET /health`

Liveness probe. Returns `{"status": "ok"}`. No auth required.
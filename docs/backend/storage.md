# Storage Backends

The storage backend is the persistence layer for events, graph snapshots,
deployment markers, graph diffs, and causal history. All backends implement
`BaseRepository`.

## Choosing a Backend

| Backend | Use case |
|---|---|
| `InMemoryRepository` | Local development, tests, zero setup |
| `PGEventRepository` | Production — persistent storage, multi-process |

## InMemoryRepository

No setup required. Data is lost on process restart. Use this for local
development and integration tests.

```python
from origintracer.backend.storage import InMemoryRepository

repo = InMemoryRepository()
```

## PGEventRepository

PostgreSQL-backed. Requires `ORIGINTRACER_DB_DSN` to be set.

```python
from origintracer.backend.storage import PGEventRepository

repo = PGEventRepository(dsn="postgresql://user:pass@localhost/origintracer")
```

Snapshots are stored per `customer_id`. Only the most recent snapshot per
customer needs to be immediately queryable — older snapshots can be retained
for audit or overwritten depending on your retention policy.

On FastAPI startup, `get_latest_snapshot()` is called automatically to
restore the graph without waiting for the next agent snapshot interval.

## BaseRepository Interface

All backends implement the following interface:

### `insert_event(event)`

Store one `NormalizedEvent` from the agent.

---

### `query_events(...)`

Retrieve events matching filters, newest first.

| Parameter | Type | Description |
|---|---|---|
| `trace_id` | `str`, optional | Filter by trace |
| `probe` | `str`, optional | Filter by probe type |
| `service` | `str`, optional | Filter by service |
| `since` | `float`, optional | Unix timestamp lower bound |
| `limit` | `int` | Max results, default `100` |

---

### `insert_snapshot(customer_id, data, ...)`

Persist a serialised `RuntimeGraph` snapshot. Called by FastAPI on every
`POST /api/v1/graph/snapshot`.

| Parameter | Type | Description |
|---|---|---|
| `customer_id` | `str` | Customer identifier |
| `data` | `bytes` | Serialised graph |
| `content_type` | `str` | `application/msgpack` (default) or `application/json` |
| `node_count` | `int` | Recorded for the status endpoint |
| `edge_count` | `int` | Recorded for the status endpoint |

---

### `get_latest_snapshot(customer_id)`

Return the latest snapshot for a customer, or `None`.

Returns a dict with keys: `data` (bytes), `content_type` (str),
`received_at` (float).

---

### `insert_deployment_marker(customer_id, label)`

Store a deployment marker with the current timestamp. Used by
`DIFF SINCE deployment` and the `new_sync_call_after_deployment` causal rule.

---

### `insert_graph_diff(customer_id, diff)`

Store one graph diff snapshot received from the agent.

---

### `save_causal_matches(customer_id, matches, timestamp)`

Persist causal rule matches for historical review. Retrieved by
`GET /api/v1/causal/history`.

---

### `get_causal_history(customer_id, limit)`

Return the most recent causal match snapshots, newest first.
Default `limit` is `50`.

---

### `close()`

Release open connections. Called on FastAPI shutdown.
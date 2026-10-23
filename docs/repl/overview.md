# REPL Overview

The OriginTracer REPL connects directly to a running agent via Unix domain
socket. All queries run against the live engine in the worker process - 
no HTTP, no serialisation round-trip, no restart required.

## Starting the REPL

```bash
python -m origintracer.repl.repl
```

The REPL auto-discovers `/tmp/origintracer-{pid}.sock` and connects. If
multiple workers are running, it presents a socket picker.

## Protocol

JSON over Unix domain socket:

```json
// Send
{"id": "1", "query": "SHOW NODES"}

// Receive
{"id": "1", "ok": true, "data": {...}}
```

## Relationship to the Backend

The REPL and the FastAPI backend serve the same data from different surfaces.
The REPL connects directly to the agent's Unix socket — useful for live
debugging in a terminal. The backend serves the same graph over HTTP — useful
for dashboards and remote access.

Use the REPL when you need to inspect a live worker interactively. Use the
backend when you need persistent storage, historical queries, or a UI.


# REPL Command Reference

## DSL Queries

DSL queries interrogate the live RuntimeGraph and event store.

| Command | Description |
|---|---|
| `SHOW latency WHERE service = "django"` | Latency breakdown for a service |
| `SHOW latency WHERE system = "export"` | Latency for a semantic system alias |
| `SHOW graph` | Full RuntimeGraph as adjacency list |
| `SHOW graph WHERE system = "worker"` | Graph filtered to a semantic system |
| `SHOW events WHERE probe = "db.query.start" LIMIT 20` | Raw events by probe type |
| `HOTSPOT TOP 10` | Top 10 busiest nodes by call count |
| `CAUSAL` | Run all causal rules against the live graph |
| `CAUSAL WHERE tags = "blocking, worker"` | Causal rules filtered by tag |
| `BLAME WHERE system = "export"` | Root cause candidates for a system |
| `DIFF SINCE deployment` | Graph diff since the last deployment marker |
| `TRACE <trace_id>` | Critical path for a specific trace |

## Meta-commands

Meta-commands inspect engine internals and control the REPL session.

| Command | Description |
|---|---|
| `\status` | Engine stats — graph size, uptime, event counts |
| `\probes` | Registered probe adapters |
| `\rules` | Registered causal rules with confidence and tags |
| `\semantic` | Semantic aliases and their resolved node sets |
| `\emit <probe> <svc> <name>` | Inject a synthetic event for testing |
| `\snapshot [label]` | Capture a temporal diff snapshot manually |
| `\active` | In-flight requests tracked by `ActiveRequestTracker` |
| `\reconnect` | Pick a different worker socket |
| `\help` | Print command reference |
| `\quit` | Exit — also `Ctrl+C` |

## Tips

- `CAUSAL` with no filter runs every registered rule. On a large graph this
  is fast (<5ms) but if you only care about one category use
  `CAUSAL WHERE tags = "db"` to narrow it.
- `DIFF SINCE deployment` requires at least one deployment marker — post one
  with `\snapshot deployment` or via `POST /api/v1/deployment` on the backend.
- `TRACE <id>` works best when `ActiveRequestTracker` is enabled — it
  reconstructs the critical path from in-memory span data. For historical
  traces use `GET /api/v1/traces/{id}` on the backend instead.
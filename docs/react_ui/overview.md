# React Dashboard

The React dashboard displays graph snapshots, events, and captured content
stored by the FastAPI backend. It is a browser interface for uploaded data;
it does not connect to a worker's Unix socket or issue arbitrary live REPL
commands. Use the [REPL](../repl/overview.md) for direct live inspection.

## Start the dashboard

The dashboard requires Node.js 18 or newer and a reachable FastAPI backend.

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173`. For production, `npm run build` writes static
assets to `frontend/dist/`; serve them from a static host or behind your
application server.

Configure the backend URL and API key in `frontend/.env`:

```dotenv
VITE_API_BASE=http://localhost:8001
VITE_API_KEY=your-development-key
```

The key must be configured in the backend's `ORIGINTRACER_API_KEYS` setting.
See the [backend overview](../backend/overview.md) and
[storage guide](../backend/storage.md).

## Worker selection and freshness

The sidebar lists worker process instances with fresh uploader heartbeats.
The dashboard selects the newest reporting instance by default and remembers
the selected `worker_instance_id` across reloads. Selecting another worker
loads that process's stored graph and events. A PID alone is not a durable
identity because a restarted process can reuse it; the dashboard uses the
stable process-instance ID returned by the backend.

Stale snapshots remain stored in the backend but are omitted from the current
reporting-worker list. If no worker is reporting, the dashboard displays a
no-worker state. The worker status badge reflects uploader heartbeat freshness,
not an operating-system process check.

## Views

- **Nodes and edges:** tabular views of the selected worker's stored graph.
- **Graph:** a compact node and edge listing for the selected snapshot.
- **Events:** recent uploaded events for the selected worker.
- **Trace:** timeline for a trace found in the selected worker's stored events.
- **Stitch:** combines uploaded event rows for a trace across workers. It does
  not contact live worker sockets.
- **Diff:** changes in the selected worker graph since a deployment marker or
  timestamp.
- **Status:** snapshot and heartbeat details, including uptime, active requests,
  active probes, graph counts, and reporting freshness when available.
- **Causal and history:** current rule matches and previously recorded causal
  evaluation history.
- **LLM content and tool content:** captured LangGraph previews grouped by
  trace and, when available, investigation ID. Selecting a trace shows its
  captured prompt/response or tool input/output previews.

## Supported queries

The query bar accepts the stored-data commands listed below. These commands
select the corresponding dashboard view and query the backend for the selected
worker instance.

| Command | Result |
|---|---|
| `SHOW nodes` | Stored nodes for the selected worker. |
| `SHOW edges` | Stored edges for the selected worker. |
| `SHOW graph` | Combined graph view. |
| `SHOW events` | Recent uploaded events. |
| `SHOW status` | Worker heartbeat and graph status. |
| `SHOW llm_content` | Captured model content previews. |
| `SHOW tool_content` | Captured tool content previews. |
| `TRACE <id>` | Stored trace stages for the selected worker. |
| `STITCH <id>` | Stored trace stages assembled across workers. |
| `CAUSAL` | Causal rule matches for the selected snapshot. |
| `DIFF SINCE <label>` | Stored graph changes since a marker or timestamp. |

Arbitrary DSL queries and live socket operations belong in the REPL. See its
[command reference](../repl/overview.md).

## Capturing LangGraph content

The LLM and tool content views display previews only when capture was enabled
in the instrumented process at event emission time. Set either or both
environment variables before starting that process:

```bash
export ORIGINTRACER_CAPTURE_LLM_CONTENT=1
export ORIGINTRACER_CAPTURE_TOOL_CONTENT=1
```

The LangGraph probe bounds and redacts captured previews before upload. These
settings cannot recover content from events emitted before capture was
enabled. See the [LangGraph probe guide](../probes/langgraph.md) for event and
capture behavior.

The dashboard polls the backend for snapshots and the currently selected
view's data. For durable storage across backend restarts and hosts, configure
PostgreSQL; the in-memory repository is intended for development. See the
[storage guide](../backend/storage.md).

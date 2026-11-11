# OriginTracer UI

Browser dashboard for inspecting OriginTracer data uploaded to FastAPI.
The dashboard reads stored snapshots and events; it does not query worker REPL
sockets. Use the REPL for direct live inspection and arbitrary DSL commands.

---
## Prerequisites

Node.js 18+ — https://nodejs.org (LTS version)

---
## Quick start

```bash
cd frontend
npm install
npm run dev # http://localhost:5173
```
---
## Query bar

Captured LLM and tool content is selected by trace ID. When multiple traces
share an investigation ID, the selector groups those runs under that
investigation; a single-trace investigation keeps the regular trace option.
Trace IDs remain separate so each run can still be inspected independently.

Graph, status, diff, causal, and causal-history views use FastAPI snapshot and
storage endpoints. Events and traces use uploaded event rows. Trace stitching
combines uploaded events; it does not contact worker sockets. The **LLM
content** and **tool content** views read captured LangGraph event previews
from FastAPI's `/api/v1/events/content` endpoint, scoped by worker instance
ID. Enable
capture in the instrumented process with `ORIGINTRACER_CAPTURE_LLM_CONTENT=1`
and/or `ORIGINTRACER_CAPTURE_TOOL_CONTENT=1`; previews are bounded and
redacted by the LangGraph probe before upload. Capture settings must be enabled
when the events are emitted.

Type a supported stored-data command and press Enter or click run:
```
SHOW nodes
SHOW edges
SHOW graph
SHOW events
SHOW status
SHOW llm_content
SHOW tool_content
TRACE <id>  << reads this trace from the selected snapshot's stored events
STITCH <id> << combines uploaded events across stored workers
```

React lists process instances with fresh uploader heartbeats, selects the
newest reporting worker by default, and preserves the selected process instance
across reloads. Stale snapshots remain in backend storage but are excluded from
the reporting-worker list. The status view includes heartbeat age, uptime,
active requests, and active probes. If no worker is reporting, it shows `No
currently reporting workers`. The dashboard can run separately from
instrumented applications as long as it can reach FastAPI. For durable storage
across backend restarts and hosts, configure PostgreSQL; the in-memory
repository is intended for development. Live in-memory DSL queries remain
available through the OriginTracer REPL and local query server.

## Production build under a URL path

The static dashboard can be hosted below a domain path such as `/chat-react/`.
Copy `.env.production.example` to `.env.production`, then set `VITE_BASE_PATH`
before building:

```bash
cp .env.production.example .env.production
npm ci
npm run build
```

Vite writes the static site to `dist/`; serve that directory from Nginx at the
configured path. `VITE_API_URL` should point to a same-origin Nginx proxy for
the FastAPI backend. The dashboard is a browser app, so any `VITE_*` value is
public in the built files. Leave `VITE_API_KEY` empty in production and have
Nginx inject the backend Authorization header on the proxy route instead.

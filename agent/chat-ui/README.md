# OriginTracer Chat

A lightweight chat interface for asking the OriginTracer investigation agent
about live workers. This app lives in `agent/chat-ui/` and is separate from the
existing `frontend/` observability dashboard.

The interface is based on LangChain's Agent Chat UI and keeps its upstream MIT
license and attribution in [`LICENSE`](LICENSE).

## Run locally

Start the OriginTracer backend, an instrumented worker, and the Agent Server as
described in [`agent/OPERATIONS.md`](../OPERATIONS.md). Then, from this
directory:

```bash
corepack pnpm install --frozen-lockfile
cp .env.local.example .env.local
corepack pnpm dev
```

Open <http://localhost:3000>, select a worker, and ask a question in plain
language. You can paste the prompt from an eval case to exercise the same agent
through chat. The UI does not execute eval files or grading logic.

The local environment expects:

- Agent Server at `http://127.0.0.1:2024`
- OriginTracer backend at `http://127.0.0.1:8001`
- Development backend key `test-key-123`
- Sidebar label `127.0.0.1:2024` for the Agent Server

If a service prints a different address or uses a different key, update
`.env.local` and restart the UI. `LANGGRAPH_API_URL` is the server-side target
for the chat proxy; `NEXT_PUBLIC_AGENT_SERVER_LABEL` controls the address shown
in the sidebar. The browser calls the same-origin `/api` route.

Static demo build and Nginx deployment steps are documented separately in
[`STATIC_DEMO.md`](STATIC_DEMO.md).

The startup agent connectivity warning is disabled by default. To enable the
`/info` reachability check and show a warning when the Agent Server is
unavailable, set `NEXT_PUBLIC_CHECK_AGENT_CONNECTION=true` in the environment
used to build or run the UI. For static deployments, set it before building;
`NEXT_PUBLIC_*` values are included in the client bundle.

## Interface behavior

- The selected worker PID is sent with each user message.
- Thread history includes conversations started from this browser UI, not
  unrelated eval or `curl` threads on the shared Agent Server.
- The app shows investigation output in the conversation and supports the
  agent's worker-scope and action-approval prompts.
- The backend API key stays on the Next.js server for worker discovery.

## Inline investigation results

Completed investigation messages may include an `origintracer_report` with
structured observations. The report renderer uses each observation's `tool`
name to select an inline component; it does not try to infer the display type
from the model's prose. The mapping lives in
`src/components/thread/messages/origintracer-evidence.tsx`:

- `get_worker_status` renders status cards.
- `get_worker_nodes` and `get_worker_edges` render operation and relationship
  cards.
- `get_worker_graph` combines graph counts, operation cards, and observed
  relationships. It is a structured list view, not an interactive graph
  canvas.
- `get_worker_events` and `get_worker_database_events` render event tables.
- `get_worker_trace` and `get_worker_stitched_trace` render trace stages.
- `get_worker_latency_diagnostics` renders separate database, request, and
  asyncio measurement tables plus detected rule cards.
- `evaluate_worker_rules` and `get_worker_causal_history` render rule cards.

Ordinary findings and explanation checks remain conversational text. Detailed
raw tool responses are available in the collapsed evidence section. When
adding a visual tool, update the allowlist and tool-to-component mapping in
`origintracer-evidence.tsx`; keep the structured result as the component input
so the UI does not have to parse prose.

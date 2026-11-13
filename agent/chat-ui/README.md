# OriginTracer Chat

A lightweight chat interface for asking the OriginTracer investigation agent
about live workers. This app lives in `agent/chat-ui/` and is separate from the
existing `frontend/` observability dashboard.

The interface is based on LangChain's Agent Chat UI and keeps its upstream MIT
license and attribution in [`LICENSE`](LICENSE).

## Run locally

Start Keycloak, FastAPI, an instrumented worker, and the Agent Server as
described in the [local run guide](../../../docs/getting-started/run-locally.md).
Then, from this directory:

```bash
corepack pnpm install --frozen-lockfile
cp .env.local.example .env.local
corepack pnpm dev
```

!!! note "Agent Server startup"
    Start the Agent Server before opening Chat UI. If the UI starts while the
    Agent Server is unavailable or still starting, thread requests can log a
    temporary upstream connection error such as `POST /api/threads/search`.
    Retry once the server is ready. If the error continues, check the configured
    Agent Server URL and its logs; other failures should remain visible.

Open <http://localhost:3000>, select a worker, and ask a question in plain
language. You can paste the prompt from an eval case to exercise the same agent
through chat. The UI does not execute eval files or grading logic.

The local environment expects:

- Agent Server at `http://127.0.0.1:8123`
- OriginTracer backend at `http://127.0.0.1:8001`
- Development backend key `test-key-123`
- Sidebar label `127.0.0.1:8123` for the Agent Server

If a service prints a different address or uses a different key, update
`.env.local` and restart the UI. `LANGGRAPH_API_URL` is the server-side target
for the chat proxy; `NEXT_PUBLIC_AGENT_SERVER_LABEL` controls the address shown
in the sidebar. The browser calls the same-origin `/api` route.

For an Nginx deployment mounted under `/chat-agent`, set `NEXT_BASE_PATH`
before building and include the prefix in `NEXT_PUBLIC_API_URL` (for example,
`https://example.com/chat-agent/api`). For the complete production layout, see
the [production deployment guide](../../../docs/deployment/production.md).

The startup agent connectivity warning is disabled by default. To enable the
`/info` reachability check and show a warning when the Agent Server is
unavailable, set `NEXT_PUBLIC_CHECK_AGENT_CONNECTION=true` in the environment
used to build or run the UI. For static deployments, set it before building;
`NEXT_PUBLIC_*` values are included in the client bundle.

## Interface behavior

- The selected worker PID is sent with each user message.
- Thread history includes conversations for the selected assistant found by
  the Agent Server, including threads started through other clients.
- The app shows investigation output in the conversation and supports the
  agent's worker-scope and action-approval prompts.
- When Keycloak OIDC is configured, users sign in before chat or worker
  discovery. The Next.js server forwards the user's short-lived access token to
  LangGraph Server and the OriginTracer backend; it does not put the backend
  API key in the browser bundle. Without OIDC, local development can use the
  configured development key.
- The main composer sends a new message. Editing a prior prompt or rerunning an
  answer starts a branch from the checkpoint before that prompt; it does not
  mutate the saved conversation history. Rerun replays the original user
  request, while Edit opens an inline editor for that request.

## Keycloak sign-in and worker keys

Copy `.env.local.example` to `.env.local`, then set `AUTH_SECRET`,
`AUTH_KEYCLOAK_ISSUER`, `AUTH_KEYCLOAK_ID`, and `AUTH_KEYCLOAK_SECRET`. Register
the NextAuth callback URL with the Keycloak confidential client:
`https://your-domain/chat-agent/api/auth/callback/keycloak` (omit `/chat-agent`
when the app is hosted at `/`). Enable account registration in the realm when
users should be able to create email accounts. To offer Google sign-in, configure
Google as a Keycloak identity provider; the sign-in page selects it through
Keycloak's `kc_idp_hint`.

The account page lets an authenticated user create, list, and revoke worker API
keys. A new key is displayed once and should be stored as
`ORIGINTRACER_API_KEY` in the instrumented process environment. Configure the
OriginTracer backend to trust the same Keycloak issuer and the API audience
assigned to its access tokens. The Agent Server's `auth.py` validates the
incoming token through the backend and scopes thread operations to that user.

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
- `get_worker_component_diagnostics` renders per-service and per-system
  measurement tables, sampled events, shared trace IDs, and detected rules.
- `evaluate_worker_rules` and `get_worker_causal_history` render rule cards.

Ordinary findings and explanation checks remain conversational text. Detailed
raw tool responses are available in the collapsed evidence section. When
adding a visual tool, update the allowlist and tool-to-component mapping in
`origintracer-evidence.tsx`; keep the structured result as the component input
so the UI does not have to parse prose.

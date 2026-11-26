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

Trace responses preserve span IDs, parent span IDs, timestamps, and recorded
durations. The `timing` object reports trace wall duration only when complete
request boundaries or one complete root span support it. It also includes
inclusive span intervals, self-time estimates, same-trace interval overlaps,
and cumulative inclusive duration.

Self-time subtracts returned direct-child intervals and may include unobserved
child work. Parentless overlaps show temporal co-occurrence, not known lineage
or CPU parallelism. Cumulative inclusive duration includes nested work and may
count overlapping intervals more than once. Traces reaching the 1,000-event
retrieval limit are marked as potentially truncated and do not receive a trace
wall-duration value.

The legacy event `duration_ms` is an inter-event gap; legacy `total_ms` is the
sum of recorded event durations. Neither is trace wall time.

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


## Sign-in and API keys

OriginTracer separates people signing in from instrumented processes sending
data. A person signs in through an OpenID Connect provider such as Keycloak.
FastAPI verifies the provider's signed access token and uses its stable `sub`
claim as the initial OriginTracer customer namespace. This first version treats
one OIDC subject as one account; organization membership and shared accounts
can be added later without changing worker API-key semantics.

Instrumented processes continue to use OriginTracer API keys. A signed-in user
can create, list, and revoke keys through `/api/v1/api-keys`. The plaintext
secret is returned only when created; PostgreSQL stores only its SHA-256 hash.
Use a dedicated key for each instrumented process and revoke it if that process
is retired or the key is exposed. The in-memory backend can issue keys for local
testing, but those keys disappear when the backend restarts. Production startup requires PostgreSQL so issued keys survive restarts.

### Configure OIDC

Create a Keycloak realm and an API audience for OriginTracer. Configure the
clients used by the web applications to request access tokens whose `aud`
contains that API audience. The backend validates each token's signature using
Keycloak's JWKS endpoint and checks its issuer, audience, and expiry. It does not
accept an ID token as an API credential.

#### Local Keycloak and FastAPI setup

For local development, start Keycloak in development mode, create the
`origintracer` realm, and configure the chat and React OIDC clients as described
in `agent/chat-ui/README.md` and `frontend/README.md`. Add an audience mapper so
their access tokens include `origintracer-api` in `aud`.

Start FastAPI with the OIDC settings in the same process environment as Uvicorn:

```bash
cd backend
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
ORIGINTRACER_OIDC_ISSUER=http://localhost:8080/realms/origintracer \
ORIGINTRACER_OIDC_AUDIENCE=origintracer-api \
uvicorn main:app --host 0.0.0.0 --port 8001 --log-level info
```

`ORIGINTRACER_OIDC_ISSUER` must match the token's `iss` claim, including the
realm path. `ORIGINTRACER_OIDC_AUDIENCE` must match the API audience mapper and
the token's `aud` claim. `ORIGINTRACER_API_KEYS` keeps the development worker
key available; `local-dev` is its customer namespace. OIDC users are scoped by
their Keycloak subject, so workers still using this development key are stored
under `local-dev`, not the signed-in user's account. Create a worker key from
the UI and configure instrumented workers to use it to see their data under
that account.

These variables are not read from a dotenv file by `backend/main.py`. For
Supervisor, put them in that program's `environment=` setting and restart it.
Use the production configuration below for a deployed service.

Set these values in the backend service environment:

```dotenv
ORIGINTRACER_ENV=production
ORIGINTRACER_DB_DSN=postgresql://user:password@localhost/origintracer
ORIGINTRACER_OIDC_ISSUER=https://identity.example.com/realms/origintracer
ORIGINTRACER_OIDC_AUDIENCE=origintracer-api
# Optional; defaults to <issuer>/protocol/openid-connect/certs
ORIGINTRACER_OIDC_JWKS_URL=https://identity.example.com/realms/origintracer/protocol/openid-connect/certs
# Keep existing worker keys during migration, then rotate them.
ORIGINTRACER_API_KEYS=existing-worker-key:customer-id
```

When `ORIGINTRACER_ENV=production`, the backend does not silently accept
`test-key-123`. In development, the current test-key behavior remains available.
Environment-configured keys remain supported so current uploaders keep working
while they are migrated to generated keys.

### Create and use a worker key

After signing in and obtaining an **access token** for the `origintracer-api`
audience, create a key:

```bash
curl -X POST https://origintracer.app/api/v1/api-keys \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"books-api production"}'
```

Copy the returned `api_key` into the instrumented process configuration. It is
shown once and cannot be retrieved later. List safe metadata with
`GET /api/v1/api-keys`, or revoke a key with
`DELETE /api/v1/api-keys/{key_id}`. Only a verified OIDC access token can manage
keys; API keys themselves cannot create or revoke other keys.

The same access token can call `GET /api/v1/auth/me` to confirm which
OriginTracer customer namespace it maps to. A generated worker key maps to that
same namespace, so its uploaded snapshots and events are visible to the account
that created it. Existing application uploaders still send their normal
`Authorization: Bearer <api-key>` header.

The chat UI uses a server-side Keycloak client and forwards the user's access
token through the LangGraph Agent Server's custom-auth hook. Agent tools pass
that token to FastAPI, which validates it and scopes their reads to the signed-in
user. The React dashboard uses a public OIDC client with PKCE and sends its
access token to FastAPI through the configured API URL. See the frontend READMEs
for each app's environment variables, redirect URIs, and local startup steps.

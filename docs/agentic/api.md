# Agent Server API

The Agent Server is a LangGraph deployment. It uses the LangGraph Server HTTP
API for assistants, threads, runs, streaming, and checkpointed conversations;
OriginTracer does not define a separate chat REST API. Use the registered graph
ID `origintracer_investigator` as the `assistant_id`.

## Request flow

1. The client signs in through Keycloak and sends its short-lived access token
   as `Authorization: Bearer <access-token>` to the Agent Server.
2. The Agent Server validates the token through FastAPI's
   `/api/v1/auth/me` endpoint. Its auth policy assigns new threads to the
   verified subject and scopes thread reads, searches, updates, deletes, and
   runs to that owner.
3. The client creates a thread, then starts a run with the graph ID and input.
   A worker investigation supplies `worker_pid` in the input.
4. The graph resolves that PID to a worker instance and reads stored evidence
   from FastAPI. A run that reaches an approved action returns an interrupt;
   the client resumes the same thread with a decision.

The graph reads uploaded snapshots and events. It does not open a socket to the
worker. The worker's OriginTracer API key is used for upload and is not the
user's Keycloak token.

## Create a thread

Use the Agent Server address printed at startup. The local development command
in this project uses port `8123`; the API documentation is available at
`/docs` on that address.

```bash
AGENT_SERVER_URL=http://127.0.0.1:8123
ACCESS_TOKEN=your-short-lived-access-token

curl -sS -X POST "$AGENT_SERVER_URL/threads" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}'
```

Use the returned `thread_id` for runs and later follow-up messages. Thread
ownership is assigned by the server auth policy; callers do not choose an owner
in the request body.

## Start a worker investigation

The chat client uses the streaming run endpoint. The same input can be sent to
`/runs/wait` when a client wants the complete response in one HTTP request.

```bash
THREAD_ID=thread-id-from-create

curl -N -X POST \
  "$AGENT_SERVER_URL/threads/$THREAD_ID/runs/stream" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "assistant_id": "origintracer_investigator",
    "input": {
      "worker_pid": "12345",
      "messages": [{
        "role": "user",
        "content": "Show this worker’s graph and explain its busiest operations."
      }]
    }
  }'
```

Replace `12345` with a PID returned by the live worker inventory. The agent
resolves it to a stable process instance before reading worker data. Do not
reuse evidence from another worker when changing the selected PID.

For a follow-up, send another run to the same thread. To start a fresh
investigation on that worker, include `new_investigation: true` in the graph
input. Selecting a different worker starts a fresh investigation automatically.

## Resume an approval interrupt

A supported write action pauses and returns an interrupt instead of running
immediately. After the caller approves or rejects it, resume the same thread
with the decision in the LangGraph `command.resume` field:

```json
{
  "assistant_id": "origintracer_investigator",
  "command": {
    "resume": {
      "approved": true
    }
  }
}
```

Send that payload to the thread's `/runs/wait` or `/runs/stream` endpoint. Use
`approved: false` to reject the action. The exact interrupt payload identifies
the action and its arguments.

## Credentials and service boundary

- **User access token:** authenticates UI users and scopes their Agent Server
  threads and backend reads.
- **Worker API key:** authenticates an instrumented process when it uploads
  snapshots, events, and runtime heartbeats to FastAPI.
- **Agent Server address:** must be reachable by the UI server. In the hosted
  Chat UI, browser requests use the same-origin Next.js API proxy rather than
  connecting directly to the Agent Server.

For the full LangGraph Server endpoint schema and streaming formats, use the
Agent Server's `/docs` page. The generated Python graph and tool reference is
documented separately from this HTTP API.

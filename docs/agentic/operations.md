# Setup and Operations

The Agent Server, OriginTracer backend, and instrumented target application run as separate processes. The target and Agent Server each report their own worker snapshots to the backend.

## Configure

!!! warning "Protect credentials"
    Keep backend and model-provider credentials in environment variables or a secret store. Requests sent to cloud model providers include prompts and
    worker evidence.

Create an isolated environment and install the Agent Server CLI and service dependencies from the repository's `agent/` directory:

```bash
cd agent
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Configure the environment variables in the table below in the Agent Server process environment. The service also loads `agent/.env.example` as defaults;
shell variables take precedence. For secrets and machine-specific settings, prefer shell exports or your process manager's secret store.

| Variable | Purpose |
|---|---|
| `ORIGINTRACER_API_URL` | Base URL for the FastAPI backend. |
| `ORIGINTRACER_API_KEY` | Backend bearer key used for worker reads and approved writes. |
| `ORIGINTRACER_AGENT_MODEL` | Provider-prefixed chat model, such as `ollama:qwen3:4b`, `groq:openai/gpt-oss-20b`, or `mistral:mistral-small-latest`. Defaults to Ollama. |
| `OLLAMA_BASE_URL` | Ollama endpoint; defaults to the local server. |
| `GROQ_API_KEY` | Credential when using a `groq:` model. |
| `MISTRAL_API_KEY` | Credential when using a `mistral:` model. |
| `ORIGINTRACER_EVAL_JUDGE_MODEL` | Optional model override for the agent evaluation runner. |
| `ORIGINTRACER_DOCS_URL` | Optional base URL for the published MkDocs search index used by product chat. |
| `ORIGINTRACER_CAPTURE_LLM_CONTENT` | Enable bounded LLM preview capture in the instrumented process. |
| `ORIGINTRACER_CAPTURE_TOOL_CONTENT` | Enable bounded tool preview capture in the instrumented process. |
| `ORIGINTRACER_LLM_CONTENT_MAX_CHARS` | Maximum captured LLM preview length. |
| `ORIGINTRACER_TOOL_CONTENT_MAX_CHARS` | Maximum captured tool preview length. |

With Ollama, pull a tool-calling model before starting the service:

```bash
ollama pull qwen3:4b
```

Cloud model providers receive prompts and evidence sent to them.

Enable the `langgraph` probe in the target application's OriginTracer config. Set the capture variables in the **target process environment** before it starts if you want the React UI to display stored content previews. Setting them only on the Agent Server does not enable capture in a separate target process. The Agent Server's own calls use its own process settings.

## Start the services

For local development, start the in-memory backend from `backend/` in one terminal:

```bash
cd backend
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
  uvicorn main:app --host 0.0.0.0 --port 8001
```

The backend API key and customer name are configured as a `key:customer` pair. See the [backend overview](../backend/overview.md) and [storage guide](../backend/storage.md) for PostgreSQL and deployment options.
Start the instrumented target app in another terminal using its regular application command. It must initialize OriginTracer in the process being
observed and enable the `langgraph` probe for LangGraph workloads.

## Run

Start the backend using its normal deployment command, start the instrumented target application, then start the Agent Server from `agent/`:

```bash
langgraph dev --n-jobs-per-worker 1
```

The local server prints its address; the LangGraph development default is `http://127.0.0.1:2024`, with API docs at `/docs`. Use a reporting worker's PID as `worker_pid` in a run.

The sections below show how to discover a worker, send requests, approve actions, and troubleshoot common setup problems.

## Select a worker

The backend, target application, and Agent Server are separate processes. The target must initialize OriginTracer with the `langgraph` probe enabled in the
same process that runs its LangGraph model and tools, while the Agent Server also starts OriginTracer in its own FastAPI lifespan, so it reports a distinct
worker PID, and both processes must be able to reach the backend; when local socket discovery is used, they must share the relevant host/container PID and Unix socket namespace.

To store LLM and tool previews, set these before starting the instrumented target process:

```bash
export ORIGINTRACER_CAPTURE_LLM_CONTENT=1
export ORIGINTRACER_CAPTURE_TOOL_CONTENT=1
```

Check the backend inventory and select a worker whose heartbeat is fresh:

```bash
curl -H "Authorization: Bearer test-key-123" \
  http://127.0.0.1:8001/api/v1/snapshots
```

Use the selected row's `pid` as `worker_pid`. The agent resolves it to a stable `worker_instance_id` and uses that ID for all stored-data reads.

## Send an investigation request

Create a thread and copy its returned `thread_id`:

```bash
curl -sS -X POST http://127.0.0.1:2024/threads \
  -H "Content-Type: application/json" -d '{}'
```

Replace `12345` with a reporting worker PID:

```bash
THREAD_ID="paste-thread-id-here"
curl -sS -X POST \
  "http://127.0.0.1:2024/threads/${THREAD_ID}/runs/wait" \
  -H "Content-Type: application/json" \
  -d '{
    "assistant_id": "origintracer_investigator",
    "input": {
      "worker_pid": "12345",
      "messages": [{
        "role": "user",
        "content": "Show the graph and explain the busiest nodes."
      }]
    }
  }'
```

Send a later run to the same thread to continue. For a fresh conversation set `new_investigation` to `true`, and supplying a different `worker_pid` starts a fresh investigation automatically. The thread checkpoint stores conversation and investigation state; the agent does not maintain a separate investigation database.

## Approve or reject a write action

Ask the agent to record a deployment marker or evaluate and record causal rules. The run will then pauses with an approval request, and reject or approve for the thread to resume.

```bash
curl -sS -X POST \
  "http://127.0.0.1:2024/threads/${THREAD_ID}/runs/wait" \
  -H "Content-Type: application/json" \
  -d '{
    "assistant_id": "origintracer_investigator",
    "command": {"resume": {"approved": true}}
  }'
```

## Troubleshooting

- **No workers listed:** confirm that the backend is reachable and the target and Agent Server have the correct backend URL and API key. Wait for the next graph snapshot and runtime heartbeat.
- **Worker is stale:** the UI and agent overview only include fresh heartbeats. Check that the instrumented process is running and its uploader can reach the backend.
- **Model cannot call tools or return structured output:** choose a model with tool-calling and structured-output support and confirm its provider package and credentials are configured.
- **Content previews are empty:** enable the matching capture environment variable before starting the instrumented process. Capture settings cannot recover events uploaded before capture was enabled.
- **Ollama is remote:** set `OLLAMA_BASE_URL` to a reachable LAN endpoint and configure Ollama to listen on that interface; restrict access to a trusted network.

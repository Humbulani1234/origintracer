# Agent Setup and Operations

This guide takes you from installing the Agent Server through configuring a
model, starting the services it depends on, and sending a test investigation.
It also explains how to approve a supported write action. For a closer look at
the graph's state and routing, see [Agent Architecture](ARCHITECTURE.md); the
[evaluation guide](evals/README.md) covers the end-to-end evaluation cases.
For the production service layout and configuration, see the
[production deployment guide](../docs/deployment/production.md).

## Install

From the `agent/` directory, install the Agent Server CLI and this application:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

The Agent Server reads the environment file named by `agent/langgraph.json`
(currently `agent/.env.example`). This file is ignored by Git and must be
created locally; keep real credentials there or in the service manager, never
in a committed example. Shell environment values take precedence. The
[local run guide](../docs/getting-started/run-locally.md) gives the complete
service startup order, including Keycloak.

To install development tools and run source checks:

```bash
python -m pip install -e ".[dev]"
pytest
black --line-length 65 src tests
ruff check src tests
mypy
```

The agent's pre-commit configuration is separate from the core repository
configuration. From the repository root, run its hooks explicitly with:

```bash
pre-commit run --config agent/.pre-commit-config.yaml --all-files
```

The repository's pre-commit hooks and GitHub Actions also run these checks on
the agent source and tests.

## Configure a model

The agent can use local Ollama or the GroqCloud and Mistral services through
their LangChain provider integrations. Ollama is the default provider, and its
default model, `qwen3:4b`, supports the tool calls and structured responses
required by the investigation graph.

### Ollama

If you are using Ollama, first pull the default model on the computer that
runs the Ollama service:

```bash
ollama pull qwen3:4b
```

In `agent/.env.example`, set `ORIGINTRACER_AGENT_MODEL` to a provider-prefixed
model such as `ollama:qwen3:4b`. The evaluation runner uses
`ORIGINTRACER_EVAL_JUDGE_MODEL`, which accepts the same format. Set
`OLLAMA_BASE_URL` to the address where Ollama is listening; when it runs on a
different computer, use that computer's LAN address, such as
`http://192.168.1.50:11434`. The Ollama host must accept connections from the
agent computer, and its firewall must allow port `11434`. You can check
connectivity from the agent computer with:

```bash
curl http://192.168.1.50:11434/api/tags
```

By default, Ollama listens only on localhost. To allow connections over your
LAN, configure the Ollama computer to listen on `0.0.0.0:11434` and restart
the service. On Linux systems that run Ollama as a service, open its override
file with `sudo systemctl edit ollama.service` and add:

```ini
[Service]
Environment="OLLAMA_HOST=0.0.0.0:11434"
```

After saving the override, reload systemd and restart Ollama with
`sudo systemctl daemon-reload` and `sudo systemctl restart ollama.service`.
Point `OLLAMA_BASE_URL` at the Ollama computer's LAN IP, and keep port `11434`
accessible only on a trusted network.

### GroqCloud

For a cloud-backed demo, create a Groq API key and add it to
`agent/.env.example` alongside the model settings:

```dotenv
GROQ_API_KEY=your-groq-api-key
ORIGINTRACER_AGENT_MODEL=groq:openai/gpt-oss-20b
ORIGINTRACER_EVAL_JUDGE_MODEL=groq:openai/gpt-oss-20b
```

The Groq integration is installed with the agent dependencies, so after
changing these settings, restart the Agent Server to apply them. Because the
evaluation runner reads the same file, it will also use Groq for quality
judging. The runtime environment file is ignored by Git. Keep the key private
and do not commit it. When you use Groq,
prompts and investigation evidence are sent to the provider. Free-tier limits
vary by model and account, so check the
[Groq rate-limit page](https://console.groq.com/docs/rate-limits) before a
demo. If requests return HTTP 429, wait for the limit window to reset or reduce
the number of demo requests.

### Mistral

To use Mistral, add `MISTRAL_API_KEY` to `agent/.env.example` and set both
`ORIGINTRACER_AGENT_MODEL` and `ORIGINTRACER_EVAL_JUDGE_MODEL` to a model in
the `mistral:<model-id>` format, such as `mistral:mistral-small-latest`.
LangChain's Mistral integration supports the tool calls the agent needs. Restart
the Agent Server after changing the provider or model settings.

## Start and observe the services

The backend, the instrumented target application, and the Agent Server run as
separate processes. Start Keycloak and configure its realm first when testing
signed-in UIs. Then start the backend; for local development with the in-memory
store, run
this command from the repository's `backend/` directory:

```bash
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
  ORIGINTRACER_OIDC_ISSUER=http://localhost:8080/realms/origintracer \
  ORIGINTRACER_OIDC_AUDIENCE=origintracer-api \
  uvicorn main:app --host 0.0.0.0 --port 8001
```

Next, set `ORIGINTRACER_API_URL` and `ORIGINTRACER_API_KEY` in
`agent/.env.example` to match the backend you started. The Agent Server uses
that key to query worker data.

Now start the target application with its usual command. In the process that
runs the LangGraph model and tools, initialize OriginTracer with the
`langgraph` probe enabled. The [Django example](../applications/django/README.md)
shows one way to configure this. If you also want the React UI to display LLM
prompt/response and tool previews, set these variables **before starting the
target process**:

```bash
export ORIGINTRACER_CAPTURE_LLM_CONTENT=1
export ORIGINTRACER_CAPTURE_TOOL_CONTENT=1
```

With the backend and target running, start the Agent Server as described below.
Its FastAPI lifespan starts its own OriginTracer worker, so the investigator
and target appear under different PIDs and can be inspected independently. For
the backend to discover both workers through local socket discovery, all three
processes must share the relevant host or container PID and Unix socket
namespace.

## Run the Agent Server

From the `agent/` directory, start the local LangGraph development server:

```bash
langgraph dev --n-jobs-per-worker 1
```

The `--n-jobs-per-worker 1` setting allows one graph run at a time in each
server worker, which is useful when stepping through the graph in a debugger.
You can raise it, for example to `4`, when you want that worker to handle more
graph jobs concurrently; this setting controls the agent's graph runs rather
than requests served by the instrumented OriginTracer application.

The graph is registered as `origintracer_investigator`, which the LangChain
Chat UI fork can select when sending thread and run requests. The development
server exposes API documentation at `http://127.0.0.1:2024/docs` by default;
if you configured another port, use the address printed by `langgraph dev`.

The Agent Server's `.env.example` also enables content capture for calls made
by the investigator itself. Since exported shell values take precedence, make
sure the capture settings are not set to `0` in the server's environment. Once
the processes have had time to upload their snapshots, check the backend
inventory:

```bash
curl -H "Authorization: Bearer test-key-123" \
    http://127.0.0.1:8001/api/v1/snapshots
```

Choose a row whose `online` value is `true`, then pass its PID as `worker_pid`
when you start an investigation. The agent resolves that PID to the associated
`worker_instance_id` and scopes its reads to that process instance. The React
interface uses the same uploaded inventory, so to inspect the investigator's
own LLM or tool calls, select the Agent Server worker and open LLM Content or
Tool Content after a run.

## Send a test request

The API expects each run to belong to a thread. Create one first:

```bash
curl -sS -X POST http://127.0.0.1:2024/threads \
    -H "Content-Type: application/json" \
    -d '{}'
```

Copy the returned `thread_id` into the variable below, and replace `12345` with
the PID of an online worker from `GET /api/v1/snapshots`:

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
          "content": "Compare a database hotspot with event-loop starvation as possible causes of high latency. Gather evidence for and against each, and say what remains unclear."
        }]
      }
    }'
```

The run response includes the assistant's message and the final investigation
state. To ask a follow-up, send another run to the same thread; the thread
retains its worker selection. If you provide a different `worker_pid`, the
agent starts a fresh investigation so evidence from separate processes is not
mixed. You can also set `new_investigation` to `true` to clear the current
investigation and start over with the same worker.

LangGraph manages the conversation, runs, and thread checkpoints. It saves the
investigation fields as part of each thread's graph state, so the Agent Server
does not maintain a separate conversation or evidence database.

## Approve or reject an action

To record a deployment marker, ask the agent something like “Record a
deployment marker named demo-release.” The run pauses and returns an approval
request that identifies the proposed action and label. Resume the same thread
with approval to execute it:

```bash
curl -sS -X POST \
    "http://127.0.0.1:2024/threads/${THREAD_ID}/runs/wait" \
    -H "Content-Type: application/json" \
    -d '{
      "assistant_id": "origintracer_investigator",
      "command": {"resume": {"approved": true}}
    }'
```

To reject the proposal instead, resume the thread with
`{"approved": false}`. Evaluating and saving causal-rule matches is the other
supported write action, and it follows the same approval step.

## Evaluate the graph

The [evaluation guide](evals/README.md) describes end-to-end cases for evidence
gathering, follow-up questions, missing workers, and approval handling. Before
deploying the service, account for its current authorization model: every run
uses the same backend API key, so keep the Agent Server on a trusted network
until caller authentication and per-user authorization are configured.

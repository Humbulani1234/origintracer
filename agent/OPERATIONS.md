# Agent Setup and Operations

This guide covers installation, model configuration, local services, and
example requests. For graph behavior and state flow, see
[Agent Architecture](ARCHITECTURE.md). For full evaluation instructions, see
[Agent graph evaluations](evals/README.md).

## Install

From the `agent/` directory, install the Agent Server CLI and this application:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

The Agent Server, Python configuration, and evaluation runner read
`agent/.env.example` directly. Edit it for machine-specific service addresses,
model settings, and API keys. Already-exported shell variables take
precedence; Python settings have localhost fallbacks when a value is absent.

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

The agent supports local Ollama, GroqCloud, and Mistral through LangChain
provider integrations. Ollama remains the default. The default model is `qwen3:4b`,
which supports tool calling and structured responses used by the investigation
graph.

### Ollama

Pull the default model on the computer running Ollama:

```bash
ollama pull qwen3:4b
```

Set `ORIGINTRACER_AGENT_MODEL` to a provider-prefixed model such as
`ollama:qwen3:4b`. Set `ORIGINTRACER_EVAL_JUDGE_MODEL` to the model used for
evaluation judging; it accepts the same provider-prefixed format. Set
`OLLAMA_BASE_URL` to the Ollama server address. If Ollama is on another
computer, use its LAN address, for example `http://192.168.1.50:11434`. The
Ollama server must accept connections from the agent computer, and the network
firewall must allow port `11434`. Check the connection from the agent computer:

```bash
curl http://192.168.1.50:11434/api/tags
```

Ollama listens only on localhost by default. On the computer running Ollama,
set `OLLAMA_HOST=0.0.0.0:11434` and restart Ollama so it accepts LAN requests.
For a Linux system service, add this under `[Service]` with
`sudo systemctl edit ollama.service`:

```ini
[Service]
Environment="OLLAMA_HOST=0.0.0.0:11434"
```

Then run `sudo systemctl daemon-reload` and
`sudo systemctl restart ollama.service`. Use the Ollama computer's LAN IP in
`OLLAMA_BASE_URL` and keep this port limited to your trusted LAN.

### GroqCloud

For a cloud-backed demo, create a Groq API key and edit `agent/.env.example`:

```dotenv
GROQ_API_KEY=your-groq-api-key
ORIGINTRACER_AGENT_MODEL=groq:openai/gpt-oss-20b
ORIGINTRACER_EVAL_JUDGE_MODEL=groq:openai/gpt-oss-20b
```

The Groq integration package is installed with the agent dependencies. Restart
the Agent Server after changing its environment. The eval runner also reads
`agent/.env.example`, so it will use Groq for its quality judge with this
configuration. `.env.example` is tracked in the repository; keep your API key
private and do not commit your local key change. Prompts and investigation
evidence sent to the model provider leave your machine when using Groq.
Free-tier limits depend on the selected model and account; check the
[Groq rate-limit page](https://console.groq.com/docs/rate-limits) before a
demo. If requests return HTTP 429, wait for the limit window to reset or reduce
the number of demo requests.

### Mistral

Set `MISTRAL_API_KEY` in `agent/.env.example`, then set both
`ORIGINTRACER_AGENT_MODEL` and `ORIGINTRACER_EVAL_JUDGE_MODEL` to a Mistral
model using the `mistral:<model-id>` form, for example
`mistral:mistral-small-latest`. The Mistral integration supports tool calling
through LangChain. Restart the Agent Server after changing the model settings.

## Start and observe the services

Run the backend, the target app, and this Agent Server as separate processes.
For the in-memory backend, start it from the repository's `backend/` directory:

```bash
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
  uvicorn main:app --host 0.0.0.0 --port 8001
```

Set `ORIGINTRACER_API_URL` and `ORIGINTRACER_API_KEY` in
`agent/.env.example` to match the running OriginTracer backend. The backend
key is used by this server when it queries workers.

Start the target app using its normal command. It must call `origintracer.init`
with a config that enables the `langgraph` probe in the same process that runs
the LangGraph model and tools. For the Django example, see
[`applications/django/README.md`](../applications/django/README.md). To capture
prompt/response and tool previews, set these variables **before starting the
target process**:

```bash
export ORIGINTRACER_CAPTURE_LLM_CONTENT=1
export ORIGINTRACER_CAPTURE_TOOL_CONTENT=1
```

Then start the Agent Server with `langgraph dev` as described below. Its
FastAPI lifespan starts a separate OriginTracer worker to observe the
investigator itself. The target app and investigator have different PIDs and
can be inspected independently. The backend must share their host/container
PID and Unix socket namespace to discover both.

## Run the Agent Server

Start the local LangGraph server from the `agent/` directory:

```bash
langgraph dev --n-jobs-per-worker 1
```

`--n-jobs-per-worker 1` limits the development server to one active graph run
per worker process, which helps when stepping through the graph in a debugger.
Use a larger number, such as `4`, to run more graph jobs at once. This limits
graph jobs; it does not control how many requests the OriginTracer application
handles.

The graph ID is `origintracer_investigator`. The LangChain Chat UI fork can
connect to this Agent Server and send normal thread and run requests. Local API
documentation is available at `http://127.0.0.1:2024/docs` with the current
CLI default port. Use the address printed by `langgraph dev` if you set a
different port.

The Agent Server's `.env.example` enables content capture for its own calls.
Shell values take precedence, so make sure they are not set to `0` when you
start the server. Check which worker snapshots the backend has received:

```bash
curl -H "Authorization: Bearer test-key-123" \
    http://127.0.0.1:8001/api/v1/snapshots
```

Choose a row with `online: true` and use its PID as `worker_pid` in an
investigation. The response also includes `worker_instance_id`; the agent
resolves and scopes reads to that process instance. The React interface uses
the same uploaded snapshot inventory. To inspect the investigator's own LLM
and tool calls, choose the Agent Server worker; after a run, inspect LLM Content
or Tool Content in React.

## Send a test request

Create a thread:

```bash
curl -sS -X POST http://127.0.0.1:2024/threads \
    -H "Content-Type: application/json" \
    -d '{}'
```

Copy the returned `thread_id` and set it below. Replace `12345` with a reporting
worker PID from `GET /api/v1/snapshots`:

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

The response includes the assistant message and final investigation state.
To continue the investigation, send another run to the same thread. Send a
different `worker_pid` to switch workers; this starts a new investigation so
evidence from two processes is never mixed. Set `new_investigation` to `true`
in a later run to clear the current investigation and start another on the same
worker. The thread keeps its worker selection for follow-up questions.

The Agent Server manages conversations, runs, and thread checkpoints. The
investigation fields live in graph state saved with each thread; this service
does not add a separate conversation or evidence database.

## Approve or reject an action

For a deployment marker, ask: “Record a deployment marker named
demo-release.” The run pauses with an approval request containing the action
and label. Resume that thread to approve:

```bash
curl -sS -X POST \
    "http://127.0.0.1:2024/threads/${THREAD_ID}/runs/wait" \
    -H "Content-Type: application/json" \
    -d '{
      "assistant_id": "origintracer_investigator",
      "command": {"resume": {"approved": true}}
    }'
```

To reject the action, resume with `{"approved": false}`. For the other
supported action, ask: “Run causal rules on the selected worker snapshot and
save any matches.” It uses the same approval step.

## Evaluate the graph

For end-to-end evidence-gathering, follow-up, missing-worker, and action
approval cases, see [Agent graph evaluations](evals/README.md).

This first version uses one backend API key for every agent run. Keep the
Agent Server on a trusted network until caller authentication and per-user
authorization are configured for the deployment.

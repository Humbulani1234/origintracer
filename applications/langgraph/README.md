# OriginTracer - LangGraph Agent Server Application

Runs a supervisor and two specialist agents through LangGraph's production Agent Server and other alternatives. `langgraph.json` exposes the compiled supervisor, while a FastAPI lifespan initializes OriginTracer inside the server process for tracing.
LangSmith tracing is disabled - agent callbacks are observed by OriginTracer's LangGraph probe instead.

This example has been adapted from [LangGraph](https://docs.langchain.com/oss/python/langchain/multi-agent/subagents-personal-assistant)

## Application layout

```text
applications/langgraph/
├── personal_assistant/
│   ├── __init__.py
│   ├── graph.py # supervisor, specialist agents, and tools
│   ├── manual_server.py # optional host-native experiment server
│   ├── manual_server_with_uvicorn.py # optional ASGI HTTP-layer experiment
│   ├── scenarios.py # bounded graphs that demonstrate causal rules
│   └── webapp.py # FastAPI lifespan for OriginTracer
├── .env.example
├── invoke.py # sends one request to Agent Server
├── invoke_latency_hotspot.py
├── invoke_loop_runaway.py
├── langgraph.json # Agent Server application definition
├── docker-compose.host.yml # Docker-to-host mapping for local services
├── origintracer.yaml
├── requirements.txt
└── README.md
```

`create_agent()` returns the compiled LangGraph graph referenced by `langgraph.json`, and Agent Server supplies its standard assistants, threads, runs, streaming, and persistence APIs. The custom FastAPI application is mounted into the same server; extending the Agent Server APIs.

## Prerequisites

- Python 3.11 or newer
- Docker with Docker Compose support
- Ollama
- The LangGraph CLI
- The Agent Server credentials or license required for your deployment mode

`langgraph up` runs the production-shaped stack locally: Agent Server, PostgreSQL, and Redis. It requires a LangSmith API key for local use and the
appropriate license for production use, and the requirement belongs to the Agent Server runtime.

## Install the application

From this directory, create a virtual environment and install the CLI and the
application dependencies:

```bash
cd /path/to/origintracer/applications/langgraph

python -m pip install -r requirements.txt
python -m pip install -e ../..
```

The relative `../..` dependency in `langgraph.json` tells Agent Server that this example uses the OriginTracer package from the same repository.

## Start Ollama

Install Ollama using the instructions for your operating system, start its service, and pull a model that supports tool calling:

```bash
ollama serve
ollama pull qwen2.5:7b or any model of your choice
```

## Configure the server

Create the environment file and add the credentials required by Agent Server:

```bash
touch env.example
```

At minimum, review:

```dotenv
LANGSMITH_API_KEY=your-key-for-agent-server
LANGSMITH_TRACING=false
OLLAMA_MODEL=qwen2.5:7b or any model of your choice
OLLAMA_BASE_URL=http://host.docker.internal:11434
ORIGINTRACER_ENDPOINT=http://host.docker.internal:8001
```

`LANGSMITH_API_KEY` is required by Agent Server's startup license check even when `LANGSMITH_TRACING=false`; the latter only disables LangSmith trace delivery. `OLLAMA_BASE_URL` and `ORIGINTRACER_ENDPOINT` are addresses viewed from inside the container. The included `docker-compose.host.yml` maps `host.docker.internal` to the Docker host on Linux.

Start the OriginTracer backend on the host in a separate terminal:

```bash
cd /path/to/origintracer
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
uvicorn backend.main:app --host 0.0.0.0 --port 8001 --log-level info
```

If the host firewall blocks Docker bridge traffic, allow the Agent Server's Docker subnet to reach the backend port, and for the default stack network used by this example:

```bash
sudo ufw allow from 172.19.0.0/16 to any port 8001 proto tcp
```

Ollama must likewise accept connections from the container. If it is running on another machine, set `OLLAMA_BASE_URL` to that machine's reachable address.

## Run the production-shaped Agent Server

Build the production image using the Agent Server release compatible with this example's LangGraph 1.x dependencies:

```bash
langgraph build \
    --api-version 0.9.1 \
    -t origintracer-langgraph:local
```

Start the complete local stack from that image:

```bash
langgraph up \
    --docker-compose docker-compose.host.yml \
    --image origintracer-langgraph:local \
    --no-pull \
    --wait
```

The API is exposed at `http://localhost:8123`. The `--docker-compose` override is required so the container can resolve `host.docker.internal`; keep the
backend Uvicorn process running while the stack is in use. Re-run the build command after changing application or OriginTracer source, then run the
`langgraph up` command again.

This Docker-based Agent Server performs a LangGraph Deployment license check at startup. It requires a valid `LANGSMITH_API_KEY` with Deployment access (or a
`LANGGRAPH_CLOUD_LICENSE_KEY`) even when `LANGSMITH_TRACING=false`. For OriginTracer-only local debugging without a LangSmith key, use the manual
Uvicorn server below.

Agent Server normally listens at `http://localhost:8123`. Its OpenAPI page is available at:

```text
http://localhost:8123/docs
```

The small FastAPI route supplied by this example confirms that the custom application was mounted and its lifespan ran:

```bash
curl http://localhost:8123/origintracer-example
```

## Run directly with Uvicorn

For host-native experimentation without Docker or the complete Agent Server stack, this example includes a small FastAPI adapter. It implements only the
stateless `/runs/wait` request used by the invocation scripts; it does not provide Agent Server threads, persistence, queues, streaming, or deployment
features.

Start it from the `applications/langgraph` directory:

```bash
export N_JOBS_PER_WORKER=1

uvicorn personal_assistant.manual_server:app \
    --env-file .env \
    --host 127.0.0.1 \
    --port 8124 \
    --workers 1
```

These settings control different layers of concurrency. `--workers 1` starts one Uvicorn process, while `N_JOBS_PER_WORKER=1` permits only one active graph run in that process. If another request arrives, it waits for the current run to finish. This is the recommended configuration when stepping through probe callbacks with `pdb`.

The `--env-file env.example` option loads `OLLAMA_MODEL`, `OLLAMA_BASE_URL`, and `ORIGINTRACER_ENDPOINT` before Uvicorn imports the graph and constructs its
`ChatOllama` model. Without it, the manual server does not automatically read `env.example` and defaults to Ollama at `http://localhost:11434` and the optional OriginTracer backend at `http://localhost:8001`.

To override an address for one server session, export it before starting Uvicorn; an existing server must be restarted after changing these values:

```bash
export OLLAMA_BASE_URL=http://localhost:11434
export ORIGINTRACER_ENDPOINT=http://localhost:8001
```

Since Uvicorn runs directly on the host, OriginTracer creates its socket in the host `/tmp`, and the existing backend and React worker selector can therefore discover the Uvicorn process normally.

Use the normal, without trigger rules, client against this server:

```bash
python invoke.py --url http://localhost:8124
```

### Optional Uvicorn HTTP-layer tracing

For observing the relationship between an HTTP request and its LangGraph callbacks closely, the optional `manual_server_with_uvicorn.py` entrypoint wraps the same FastAPI application with `OriginTracerASGIMiddleware`. This adds
`uvicorn.request.receive` and `uvicorn.request.complete` events around the
LangGraph events, while leaving the normal `manual_server:app` path unchanged.

This requires an application-local `probes/uvicorn_probe.py` module and both probes to be enabled in `origintracer.yaml`:

```yaml
probes:
  - uvicorn
  - langgraph
```

Start the optional entrypoint with:

```bash
uvicorn personal_assistant.manual_server_with_uvicorn:app \
    --env-file .env \
    --host 127.0.0.1 \
    --port 8124 \
    --workers 1
```

This experimental wrapper covers the manual FastAPI application's routes. It does not wrap the parent `/runs/*` routes owned by the Docker Agent Server.

The dedicated scenario scripts read their server address from the environment:

```bash
export LANGGRAPH_API_URL=http://localhost:8124
python invoke_loop_runaway.py
python invoke_latency_hotspot.py
```

## Invoke the assistant

With Agent Server running, use the included client to send the example request and wait for the final response:

```bash
python invoke.py
```

Pass another prompt as a positional argument when needed:

```bash
python invoke.py "Email alice@example.com a reminder about tomorrow's review."
```

The client defaults to `http://localhost:8123`. A different deployment and an API key can be supplied without editing it:

```bash
python invoke.py \
  --url https://your-agent-server.example.com \
  --api-key your-agent-server-key
```

Each execution produces a trace in the long-running Agent Server process.
The script uses the standard stateless `POST /runs/wait` API and has no package dependencies beyond Python itself.

To inspect streaming updates directly, call Agent Server's streaming API:

```bash
curl --no-buffer --request POST \
  --url http://localhost:8123/runs/stream \
  --header 'Content-Type: application/json' \
  --data '{
    "assistant_id": "personal_assistant",
    "input": {
      "messages": [{
        "role": "user",
        "content": "Schedule a one-hour design meeting next Tuesday at 2pm with design@example.com, and email design@example.com a reminder to review the new mockups."
      }]
    },
    "stream_mode": "updates"
  }'
```

The supervisor delegates to the calendar and email agents. Their low-level tools return simulated success messages so the example remains safe to run.

## Trigger the causal rules

The normal `invoke.py` entry point does not deliberately create a failure. Therefore two additional entry points target bounded graphs containing specific unhealthy patterns so the LangGraph rules can be demonstrated reliably.

Run the model-loop scenario:

```bash
python invoke_loop_runaway.py
```

It makes nine short calls to the same Ollama model within one graph invocation, and crosses the `loop_runaway` threshold of eight calls and then terminates. Local inference can make this scenario take noticeably longer than a normal request.

Run the latency-hotspot scenario:

```bash
python invoke_latency_hotspot.py
```

It starts two tool branches concurrently, one takes approximately 10ms and the other takes approximately one second, causing `slow_dependency` to dominate leaf-work time and trigger `agent_latency_hotspot`.

OriginTracer's runtime graph aggregates observations for the lifetime of the Agent Server process. Start with a fresh server before each scenario when you
want the rule ratios to reflect only that demonstration. After invoking a
scenario, run `CAUSAL` in the OriginTracer REPL to inspect the finding.


## Debug Agent Server locally with `langgraph dev`

For host-native debugging of the Agent Server Python code, use `langgraph dev`, which unlike `langgraph up`does not launch the production Docker stack. It starts the local development Agent Server in the current Python environment and uses the in-memory runtime instead of PostgreSQL and Redis.

Install the CLI with its in-memory development dependencies:

```bash
python -m pip install --upgrade "langgraph-cli[inmem]"
```

Run the server without the automatic reloader so that terminal `pdb` owns the
request-handling process directly:

```bash
langgraph dev --no-reload --no-browser
```

The development API normally listens at:

```text
http://localhost:2024
```

Use another terminal to invoke it, for example:

```bash
python invoke.py --url http://localhost:2024

curl -X POST \
  "http://localhost:2024/runs/01a0a7d1-8803-7341-a911-38849cf00fdd/cancel?wait=true&action=interrupt"

curl -X POST \
  "http://localhost:2024/threads/01a0a7d1-8803-7341-a911-389ec631987b/runs/01a0a7d1-8803-7341-a911-38849cf00fdd/cancel?wait=true&action=interrupt"
```

## Inspect OriginTracer

OriginTracer creates one runtime graph and Unix socket per Agent Server process.
Inside a container, that socket belongs to the container's `/tmp` and its PID
namespace. It is intentionally not presented as a host process by the current
React worker selector.

To use the local REPL against the production-shaped container, identify the
Agent Server container and open a shell in it:

```bash
docker compose ps
docker exec -it <agent-server-container> python -m origintracer.repl.repl
```

Useful queries are:

```text
SHOW nodes
SHOW edges
SHOW events LIMIT 20
CAUSAL
```

For a real deployment, OriginTracer can upload events to the configured backend.
Direct host-side socket discovery would additionally require a deliberate
container-to-host transport; sharing `/tmp` alone is insufficient because host
and container PIDs are different namespaces.

## Stop the stack

Stop the containers using the Compose project created by `langgraph up`, or use
the shutdown command printed by the CLI. Agent Server then runs the FastAPI
shutdown lifespan, which calls `origintracer.shutdown()` and removes the local
socket.

# OriginTracer Agent Service

This is the server side of the OriginTracer investigation agent. It runs as a
LangGraph Agent Server application, investigates one live OriginTracer process
through the existing backend API, and can run approved backend actions.

Investigation runs require the selected process ID as `worker_pid` in the run
input. The agent resolves the matching process-instance ID through the backend
worker inventory. It does not use the React dashboard or connect directly to
the OriginTracer engine.

## Tools

The graph keeps structured state for the question, scope, plan, hypotheses,
evidence, findings, unresolved questions, and investigation status. It plans
which evidence to gather, calls read-only tools, assesses whether results
support or weaken each hypothesis, and gathers more evidence when needed. It
then writes a response with evidence IDs and limits.

The planner handles direct questions, multi-step investigations, and checks of
competing explanations. Evidence gathering is bounded to four rounds and four
tool calls in each round. One thread keeps one active investigation; set
`new_investigation` to `true` when a new question should start with fresh
evidence.

The first tools can list live workers and inspect a selected worker's status,
graph nodes and edges, hotspots, recent events, trace path, causal rules, and
graph changes. Every process-specific tool sends a query to
`POST /api/v1/workers/{pid}/query`. The backend checks that the process is still
available before it sends the query to that process. `get_worker_snapshot`
reads the saved graph by the selected process's stable `worker_instance_id`.
New read-only tools can be added to the graph without changing the
investigation state model.

Read-only investigation tools do not write to the backend or change the
instrumented process. Event queries read the worker's bounded in-memory event log.
Historical event storage and PID filtering are not part of this service yet.
The graph can only assess causes that the available OriginTracer evidence can
distinguish. Separate actions can write backend markers or causal history after
the user approves them; see [Actions and approval](#actions-and-approval).

## How the investigation works

Each new request first goes through a simple intent check. Normal questions
continue through the investigation flow below. A clear request to perform one
of the supported actions goes to the separate approval flow instead.

For an investigation, the graph focuses on one selected OriginTracer worker.
The caller supplies the worker PID. The agent resolves its stable
`worker_instance_id` from the live-worker inventory and keeps both identities
with the investigation, so evidence and snapshots from different process
instances are not combined.

The investigation follows the path:

1. **Set the investigation scope.** The agent uses the question and selected worker to set the scope. A missing or unavailable worker must be resolved before the investigation can continue.
2. **Choose an investigation approach.** The agent decides whether the request
   needs a direct query, several related checks, or a comparison of possible
   explanations. It makes a short plan for the evidence to gather.
3. **Gather evidence.** The agent uses read-only tools to query the selected
   worker through the OriginTracer backend. Tool results are kept with their
   source and worker details so the investigation can refer back to them.
4. **Review what the evidence shows.** The agent checks whether the results
   address the plan and support, weaken, or leave each possible explanation
   unresolved. If more useful checks are available, it gathers more evidence.
5. **Report the findings.** The agent returns conclusions linked to evidence,
   along with unresolved questions and limits in the available data.

Assessment and report construction are separate responsibilities. The LLM
assessor proposes interpretations and follow-up checks; deterministic graph
logic validates evidence and hypothesis references before they enter state.
The final report builder uses evidence IDs already recorded by the graph. For
structured inventories, it computes exact summaries from tool rows instead of
asking the model to count or compare them. For example, a single-step query
whose plan selects only `get_worker_graph` and has no hypotheses gets node and
edge counts and call-count summaries computed from the complete graph result.
This behavior is selected from the plan and tool result, not keywords in the
user's wording. Multi-step and verification plans continue through evidence
assessment so their competing explanations are reviewed normally.

Evidence gathering and review can repeat, up to four rounds, with no more than
four tool calls in each round. This gives the agent room to check its initial
observations while keeping each investigation bounded. If the available tools
cannot distinguish between explanations, the report should say so rather than
presenting a guess as a cause.

## Tool discovery and evidence integrity

Read tools are registered with capability metadata. The planner sees a compact
catalog relevant to the request, and each research call exposes only the
relevant tools plus those named by pending plan steps. The graph retains the
full read-only registry for execution; a broad or unmatched request falls back
to the full catalog. Adding a read tool means adding its capability profile,
not expanding every prompt with every tool schema.

A plan step that names tools stays pending until evidence from those tools is
recorded. If research returns prose instead of making a required read, the
agent retries that tool selection once and continues only within its round
limit. This keeps a partially followed plan from being reported as complete.

Event evidence is grouped by trace ID and labeled with observed lifecycle
signals so the agent can choose a request, worker/process, or internal trace
that matches the question. Trace reports preserve all returned stages and
their `duration_ms` values. The agent does not derive stage durations by
subtracting event timestamps. Assessment can cite only recorded evidence IDs
and supplied hypothesis IDs; unknown hypothesis reviews are discarded rather
than added to investigation state.

An Agent Server thread holds the conversation and the active investigation.
Follow-up requests can use the existing plan and evidence when they continue the
same investigation. A request for a different worker starts fresh so evidence
from separate processes stays apart. Set `new_investigation` to `true` to start
fresh on the same worker. The Agent Server checkpoints the thread state; this
service does not keep a separate investigation database.

## Actions and approval

Actions use a separate path from evidence gathering. The graph only prepares
an action when the user clearly asks it to do so. It then pauses the thread and
shows the action name and arguments. The action runs only after an approval is
resumed on that same thread. Rejecting it ends the action without making a
change. Action results are reported separately from investigation evidence.

Two actions are available:

* **Record a deployment marker.** After approval, this checks the selected
  worker with `DIFF SINCE <label>`. If the marker already exists in that
  worker's live temporal history, it skips the backend write. Otherwise it
  calls `POST /api/v1/deployment` to record the marker in the backend
  repository. A live worker PID is required for this check.
* **Evaluate and record causal rules.** This calls
  `POST /api/v1/causal/evaluate`, evaluates rules against the selected
  process-instance snapshot, and stores matches in that instance's causal
  history.

For this action, ask: “Run causal rules on the selected worker snapshot and
save any matches.” It uses the same approval step as the deployment marker.

For example, send a request such as “Record a deployment marker named
demo-release” using the normal thread run API. For example:

```bash
curl -sS -X POST \
    "http://127.0.0.1:8123/threads/${THREAD_ID}/runs/wait" \
    -H "Content-Type: application/json" \
    -d '{
      "assistant_id": "origintracer_investigator",
      "input": {
        "messages": [{
          "role": "user",
          "content": "Record a deployment marker named demo-release."
        }]
      }
    }'
```

The run pauses with an approval request containing the action and label.
Resume that thread to approve:

```bash
curl -sS -X POST \
    "http://127.0.0.1:8123/threads/${THREAD_ID}/runs/wait" \
    -H "Content-Type: application/json" \
    -d '{
      "assistant_id": "origintracer_investigator",
      "command": {"resume": {"approved": true}}
    }'
```

To reject the action, resume with `{"approved": false}`. The approval pause
prevents accidental execution, but it does not replace caller authorization.
This first version uses one backend API key for all runs, so keep the Agent
Server on a trusted network.

## Evaluate the graph

The `evals/` directory contains cases that run against this Agent Server graph,
including evidence gathering, follow-ups, missing worker selection, and action
approval. You can run the same cases locally or record an experiment in
LangSmith. See [Agent graph evaluations](evals/README.md) for setup and run
instructions.

## Install

From this directory, install the Agent Server CLI and this application:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

The Agent Server loads `.env.example` as configured in `langgraph.json`; edit
that file for machine-specific service addresses and other settings. The
Python configuration has localhost fallbacks when a value is absent, and
already-exported shell variables take precedence. The evaluation runner loads
`.env` separately when present.

To install the development tools for this application, use:

```bash
python -m pip install -e ".[dev]"
```

The agent's pre-commit configuration is separate from the core repository
configuration. From the repository root, run its hooks explicitly with:

```bash
pre-commit run --config agent/.pre-commit-config.yaml --all-files
```

Install the agent development extras from this directory before running its
checks:

```bash
python -m pip install -e ".[dev]"
```

Run the agent checks from this directory:

```bash
pytest
black --line-length 65 src tests
ruff check src tests
mypy
```

The repository's pre-commit hooks and GitHub Actions also run these checks on
the agent source and tests. The agent jobs use the same Black line length, Ruff
rules, mypy options, and pytest settings as the root project.

The agent uses Ollama through LangChain's `langchain-ollama` integration. Pull
the model you want on the computer running Ollama. The default is
`qwen3:4b`, which supports tool calling and structured responses used by the
investigation graph.

For the default model, run this on the Ollama computer:

```bash
ollama pull qwen3:4b
```

Set `ORIGINTRACER_AGENT_MODEL` to an Ollama model identifier such as
`ollama:qwen3:4b`. Set `ORIGINTRACER_EVAL_JUDGE_MODEL` to the Ollama model tag,
such as `qwen3:4b` (without the `ollama:` provider prefix). Set
`OLLAMA_BASE_URL` to the Ollama server address. If
Ollama is on another computer, use that computer's LAN address, for example
`http://192.168.1.50:11434`. The Ollama server must accept connections from the
agent computer, and the network firewall must allow port `11434`. From the
agent computer, check the connection with
`curl http://192.168.1.50:11434/api/tags`, using the Ollama computer's LAN IP.

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
the agent's `OLLAMA_BASE_URL`. Keep this port limited to your trusted LAN.

Set `ORIGINTRACER_API_URL` and `ORIGINTRACER_API_KEY` to match the running
OriginTracer backend. `.env.example` is loaded by `langgraph.json`; shell
environment values take precedence. The backend key is used by this server
when it queries workers.

This first version uses one backend API key for every agent run. Keep the
Agent Server on a trusted network until caller authentication and per-user
authorization are configured for the deployment.

## Start and observe the processes

Run the backend, the target app, and this Agent Server as separate processes.
For the in-memory backend, start it from the repository's `backend/` directory:

```bash
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
  uvicorn main:app --host 0.0.0.0 --port 8001
```

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

Then start this Agent Server with `langgraph dev` as described below. Its
FastAPI lifespan starts a separate OriginTracer worker to observe the
investigator itself. The target app and investigator have different PIDs and
can be inspected independently. The backend must share their host/container
PID and Unix socket namespace to discover both.

## Run the Agent Server

Start the local LangGraph server from this directory:

```bash
langgraph dev --n-jobs-per-worker 1
```

`--n-jobs-per-worker 1` limits the development server to one active graph run
per worker process. This is useful when stepping through the graph in a
debugger. Set it to a larger number, such as `4`, when you want more graph runs
at once. This limits graph jobs; it does not control how many requests the
OriginTracer application itself handles.

The server exposes the graph under the ID `origintracer_investigator`. The
LangChain Chat UI fork can connect to this Agent Server and send normal thread
and run requests. The local API documentation is available at
`http://127.0.0.1:8123/docs` with the current CLI default port. Use the address
printed by `langgraph dev` if you set a different port.

The Agent Server's `.env.example` enables content capture for its own calls.
Shell values take precedence, so make sure they are not set to `0` when you
start the server. Check which workers the backend can see:

```bash
curl -H "Authorization: Bearer test-key-123" \
    http://127.0.0.1:8001/api/v1/workers
```

Select the target app PID when investigating that app, or select the Agent Server PID to inspect the investigator's own LLM and tool calls. In the React
interface, probes and events are shown for the selected PID, and one can inspect the LLM Content or Tool Content view after a run.

## Send a test request

Create a thread:

```bash
curl -sS -X POST http://127.0.0.1:8123/threads \
    -H "Content-Type: application/json" \
    -d '{}'
```

Copy the returned `thread_id` and set it below. Replace `12345` with a live
worker PID from `GET /api/v1/workers`:

```bash
THREAD_ID="paste-thread-id-here"

curl -sS -X POST \
    "http://127.0.0.1:8123/threads/${THREAD_ID}/runs/wait" \
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

The response includes the assistant message and the final investigation state.
To continue the investigation, send another run to the same thread with the
next user message. Send `new_investigation: true` in the input to start a fresh
investigation on the same worker.

On the first run, include `worker_pid` with the message input. The thread keeps
that worker selection for follow-up questions. Send a different `worker_pid` to
switch workers; this starts a new investigation so evidence from two processes
is never mixed. Set `new_investigation` to `true` to start a new investigation
on the same worker. For example:

```json
{
  "worker_pid": "12345",
  "messages": [
    {"role": "user", "content": "What is currently slow in this process?"}
  ]
}
```

Replace `12345` with a PID returned by the `list_live_workers` tool or the
backend's `GET /api/v1/workers` endpoint. The graph checks that the PID is still
available before planning the investigation. Set `new_investigation` to
`true` in a later run to clear the current investigation and start another on
the same worker.

The Agent Server manages conversations, runs, and thread checkpoints. The
investigation fields live in the graph state saved with each thread; this
service does not add a separate conversation or evidence database.

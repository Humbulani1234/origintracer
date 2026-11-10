# Agent graph evaluations

These evaluations run against the `origintracer_investigator` graph served by
LangGraph Agent Server. They use the same graph, tools, FastAPI stored-data
routes and Ollama model as an ordinary agent run. They do not use a mock graph
or query worker Unix sockets.

The cases cover heartbeat-backed status and worker freshness, stored snapshot
inventory, hotspots and events, database duration versus
call counts, caller relationships, graph changes since deployment, configured
causal rules, and a Django comparison of database latency with asyncio
event-loop starvation. Docs-backed chat cases cover native Django probe setup
and the built-in Django database rules; these require the published docs search
index to be reachable when the agent cache is cold.

Multi-turn cases reuse prior evidence for related follow-ups based on the
planner's structured continuation decision. Their final reports stay scoped
to the latest turn and omit unrelated prior observations. Another case starts a
fresh investigation for an unrelated question on the same thread. Continuity
does not depend on particular follow-up words; stable worker identity remains
the scope boundary. The planner also interprets overview scope; the graph asks
for a structured all-versus-selected choice only when the planner marks it
ambiguous, then lets the planner complete the plan. The cases cover missing
worker selection and both action approval paths. The deployment-marker action case
also checks that a live worker PID is included and that the duplicate check is
explained before approval. Longer request and grading text uses
`{"lines": [...]}` in `cases.json` so it can wrap cleanly; the runner joins
those lines before sending the text to the graph or judge. Action cases stop at
the approval interrupt. They do not approve or run the write actions, so they
check the duplicate-check proposal but not its live execution result.

## Choose the application under test

These are live integration evaluations, not isolated model tests. Each
worker-required case inspects the process selected by
`ORIGINTRACER_EVAL_WORKER_PID`; the runner uses that same PID for the whole
suite. Pick the application based on the capabilities the case needs, and
generate relevant traffic before running so its in-memory graph and events
contain useful observations.

The `django_database_vs_event_loop` case targets the Django example in
[`applications/django`](../../applications/django/README.md). It expects the
selected process to have Django database and asyncio instrumentation, plus the
`database` semantic group. The planner uses the generic
`get_worker_component_diagnostics` tool to query the relevant service labels
and semantic alias; there is no Django-versus-asyncio-specific comparison tool.
Run the Django app with its `origintracer.yaml`, send database-backed and async
traffic, wait for a graph snapshot upload, then set its process PID. A fresh
heartbeat is needed for cases that ask about current reporting status; stale
snapshots remain selectable for stored graph and event questions. Other
applications can use the same diagnostics tool with their own exact service
labels. The runner executes every case against one selected worker; run
separate eval sessions when comparing different apps.

Results depend on the workload present in that worker and on the configured
Ollama model. The pathway grader checks stable behavior such as tool selection
and evidence links; the model judge scores the answer against the case's
criteria. Neither substitutes for a controlled benchmark with a known latency
baseline.

Each case has two graders:

* A pathway grader checks the graph state and tool path. It checks things such
  as selected tools, evidence count, the final report structure, observations
  linked to evidence, findings linked to evidence, thread follow-up state, and
  whether actions are still waiting for approval. These checks use structured
  fields and tool identities rather than matching exact answer wording.
* An OpenEvals judge checks the answer against the case's criteria. It uses the
  configured Ollama model, so answer evaluation can also stay on your network.

## Install

From the `agent/` directory, install the evaluation dependencies:

```bash
python -m pip install -e ".[evals]"
```

## Run locally

Start the OriginTracer backend, an instrumented process, and the LangGraph
Agent Server. The server must use the same `.env.example` settings as the agent and
can be limited to one active job while debugging:

```bash
langgraph dev --n-jobs-per-worker 1
```

In another terminal, set the PID for a process with an uploaded snapshot and
run the local evaluation:

```bash
export ORIGINTRACER_EVAL_WORKER_PID=12345
python -m evals.run --mode local
```

The runner loads `agent/.env.example`. Set `ORIGINTRACER_AGENT_SERVER_URL` if the Agent Server is not at
`http://127.0.0.1:8123`. For a fully local run, keep
`LANGSMITH_TRACING=false` in `agent/.env.example` before starting the server. Local mode
does not create a LangSmith experiment. It prints each case's graph output and
scores. The run exits with a nonzero status if a case fails.

Evaluation threads are deleted after their results are collected. Add
`--keep-threads` to inspect them in the Agent Server:

```bash
python -m evals.run --mode local --keep-threads
```

## Run selected cases

Use `--case` to focus on one example while learning or debugging it. Repeat
the option to run a small group:

```bash
python -m evals.run --mode local --case django_database_vs_event_loop
python -m evals.run --mode local \
  --case direct_worker_status \
  --case django_database_vs_event_loop
```

The filter also applies in `--mode langsmith`. If `--case` is omitted, the
runner executes the full suite. An unknown case ID is reported with the list
of available IDs.

## Record results in LangSmith

Use the same cases and actual Agent Server target, then ask the LangSmith SDK
to record the experiment:

```bash
export LANGSMITH_API_KEY=your-langsmith-api-key
export ORIGINTRACER_EVAL_WORKER_PID=12345
python -m evals.run --mode langsmith
```

This mode requires network access to LangSmith and records the evaluation
experiment there. The graph and OpenEvals judge still use the configured
Agent Server and Ollama model. `LANGSMITH_TRACING` controls whether graph and
model traces are sent separately; it is not needed to record the evaluation
experiment itself.

## Add a case

Add an entry to `cases.json` with `inputs.turns`, optional worker requirements,
and expected graph checks under `outputs`. Keep the user request realistic.
The pathway grader checks behavior that should be stable; the OpenEvals judge
checks the answer using `quality_criteria`. This lets the same case run locally
or in LangSmith.

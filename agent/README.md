# OriginTracer Agent Service

The Agent Server investigates one selected live OriginTracer worker. It uses
the OriginTracer backend for worker data, maintains investigation state in
LangGraph thread checkpoints, and can run approved backend actions.

Investigation runs require the selected process ID as `worker_pid` in the run
input. The agent resolves the matching `worker_instance_id` through the
backend and does not connect directly to the OriginTracer engine or use the
React dashboard as a data source.

## Guides

- [Architecture and investigation flow](ARCHITECTURE.md): request
  routing, worker scope, evidence gathering, assessment, reports, and actions.
- [Setup and operations](OPERATIONS.md): installation, `.env.example`,
  Ollama and Groq configuration, running the services, and sending requests.
- [Local authentication setup](AUTHENTICATION_SETUP.md): Keycloak clients,
  issuer and audience settings, UI sign-in, and worker API keys.
- [Agent graph evaluations](evals/README.md): run and extend end-to-end graph
  evaluation cases.

## Capabilities

The agent can answer OriginTracer product and observability questions, inspect
one selected worker's status, graph, hotspots, events, traces, causal rules,
and graph changes, and compare evidence about possible causes. Read-only
investigation tools do not change the worker or backend. Marker and causal-rule
history writes use a separate approval flow.

The investigation graph keeps structured state for the question, scope, plan,
requested deliverables, hypotheses, evidence, findings, unresolved questions,
and status. Checkpoints retain conversation history, while each model stage
receives context selected for its task; the full thread is not automatically
sent to every model call. The assessor classifies each requested deliverable
as answered or unanswered; the graph derives completion from those
classifications and any pending planned reads. Unresolved questions and
follow-up reads link to a specific deliverable. Evidence gathering is bounded
to four rounds and four tool calls per round. A thread retains a continuing
investigation; set `new_investigation` to `true` to start fresh on the same
worker. Changing workers starts a fresh investigation.

The product chat route retrieves relevant sections from the live MkDocs search
index, cites their source links, and falls back to a concise product overview if
the docs site is unavailable. The search index is cached in memory for ten
minutes; set `ORIGINTRACER_DOCS_URL` to use another MkDocs documentation base.
Mixed requests
that combine general chat with a worker investigation are not split into two
answers yet. These planned follow-ups are documented in the
[architecture guide](ARCHITECTURE.md#planned-follow-ups).

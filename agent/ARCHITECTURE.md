# Agent Architecture

This guide describes how the OriginTracer investigation graph routes requests, gathers evidence, assesses it, and builds reports. For installation and operating instructions, see [Setup and operations](OPERATIONS.md).

## Request routing

Each new request first goes through an intent check. Requests such as: greetings, date/time, OriginTracer product questions, and observability concepts take a separate `chat` route without worker tools. For product questions, the route searches the website `MkDocs` search index at <https://origintracer.app/docs/>, selects a few relevant excerpts, and hands them to the model with source links, where a concise product overview provides fallback context when retrieval is unavailable. Subjects matter unrelated to OriginTracer receive a redirect.

For requests related to OriginTracer runtime
questions continue through the investigation flow below, and a request to perform one of the supported actions goes to the separate approval flow.

## Worker scope and investigation flow

For an investigation, the graph focuses on one selected OriginTracer process supplied by the caller as a worker PID. The agent resolves its stable
`worker_instance_id` from the FastAPI snapshot inventory and keeps both identities with the investigation, so evidence and snapshots from different process instances are not combined. Live-worker reports include only process instances with fresh uploader heartbeats; stale records remain in backend storage but are not included in those reports.

The investigation follows this path:

1. **Set the scope.** The agent uses the question and selected worker. A
   missing or unavailable worker must be resolved before investigation.
2. **Choose an approach.** The planner chooses a direct query, related factual
   checks, or a comparison of possible explanations, then makes a short plan.
3. **Gather evidence.** Read-only tools query the selected worker through the
   OriginTracer backend. Results retain their source and worker details.
4. **Assess the evidence.** The assessor checks whether results address the
   plan and support, weaken, or leave each explanation unresolved. The graph
   can gather more evidence when useful planned reads remain.
5. **Build the report.** Findings cite recorded evidence and state material
   uncertainty and data limits.

Evidence gathering is constrained to four rounds and four tool calls per round.
If available tools cannot distinguish explanations, the report should say so
rather than present a guess as a cause.

Read tools inspect the same stored snapshot and event data used by React:
status and inventory, graph nodes and edges, hotspots, latency, recent events,
trace paths, causal rules, and graph changes. The agent resolves the selected
PID to a stable `worker_instance_id`, then calls the FastAPI stored-data routes
using that instance ID. It never connects to a worker Unix socket. FastAPI
exposes the uploaded snapshot, event, and runtime APIs; live socket inspection
stays in the OriginTracer REPL and local query server. `get_worker_snapshot`
reads the stored graph by exact process-instance ID. Event reads come from
the backend event repository, and trace stitching combines uploaded event
rows.

Runtime fields that cannot be reconstructed from a graph snapshot are sent by
the uploader to `POST /api/v1/worker/runtime`. The heartbeat contains uptime,
active-request count, active probes and rules, graph counts, semantic-label
count, and event-log size. FastAPI stores it with the process-instance ID and reports its
age and freshness from `/api/v1/status` and `/api/v1/snapshots`. A heartbeat is
considered fresh for up to three snapshot intervals, with a 30-second minimum
and a five-minute cap. This is a remote reporting signal, not an OS-level
process check. FastAPI does not proxy reads to local worker sockets.

Read-only tools do not write to the backend or change the instrumented
process. New stored-data reads can be added without changing the investigation
state model.

## Planning and tool discovery

Read tools are registered with capability metadata. The planner sees a compact
catalog of all read-only tool names, capabilities, and short tool descriptions,
along with the trusted selected-worker identity. Capability terms only rank
catalog entries; they never hide a tool because a request used different
wording. It declares
requested outputs as deliverables and links each to tools documented to supply
it. The plan should not promise a format or analysis that those tools do not
return. The planner's suggestions determine which full tool schemas are exposed during evidence
gathering; those names are constrained to the catalog. If the plan contains no
valid tool selection, the graph falls back to the full read-only set. Tool
execution still enforces selected-worker identity and read/write boundaries.

Each read tool's `ToolProfile` can define research guidance for choosing its
arguments and assessment guidance for interpreting returned evidence. For
example, event guidance distinguishes event probe names from graph node IDs;
ranking guidance explains whether a result ranks by call count or duration.
These instructions are attached to the tool metadata and added when relevant.

A plan step that names tools stays pending until evidence from those tools is
recorded. If research returns prose instead of making a required read, the
agent retries the tool selection once and continues only within its round
limit. Empty object arguments for the optional `tags` list on
`evaluate_worker_rules` are normalized to an omitted filter, which evaluates
all configured rules. This handles local models that encode an omitted list
as `{}`. Other malformed filter values remain tool errors. This keeps a
partially followed plan from being reported as complete.

## Assessment and report construction

The assessor proposes interpretations and follow-up checks; deterministic
graph logic validates references and structural requirements before results
enter state. Every requested deliverable is classified as answered or
unanswered. The graph derives assessment completion from that classification
and pending planned reads; completing reads alone does not mark a request
answered. An output marked answered without evidence from one of its linked
tools is conservatively moved to unanswered. The report exposes every
unanswered deliverable. Assessor-written unresolved questions and next steps
must reference a valid deliverable ID, and questions linked to answered outputs
are omitted. For each assessment, the schema limits citation fields to
top-level evidence IDs and completed-step fields to plan IDs supplied to that
run, with graph validation as a second check.

The assessor receives hypothesis claims in order, without internal IDs, and
returns reviews in that same order. The graph maps each review to its stored
hypothesis. A review-count mismatch stops assessment. A supported or weakened
review without evidence citations, or a causal finding in a database-versus-
event-loop comparison without request-correlated component durations, also
stops assessment instead of being rewritten into a generic report statement.
The graph records `status: assessment_failed`, stores the failure reason and
gathered evidence IDs, returns an explicit message, and ends before report
synthesis. Evidence remains available in thread state, but no completed
`final_report` is produced. Unexpected runtime errors still raise normally.

When component durations are not correlated to individual requests, causal
hypothesis reviews must remain `inconclusive`; the graph keeps the same check
as a backstop. Hypotheses are reserved for verification plans that diagnose a
problem or compare possible causes. Direct facts and metric comparisons can
use several reads without creating causal hypotheses.

The final report is scoped to the latest user turn. Continuation keeps prior
evidence available to planning and assessment, while current-turn deliverables,
findings, unresolved questions, and limitations form the response contract.
The report includes newly gathered observations and older observations cited
by current findings or hypothesis reviews; it does not replay unrelated prior
status, graph, or event sections. Prior conclusions and deliverables remain
in investigation history so later follow-ups or an explicit recap can use them.

Follow-up reports are scoped to the latest user turn. The graph keeps prior
evidence and conclusions available to planning and assessment, but reports only
current-turn deliverables and findings, newly gathered observations, and older
evidence cited by the current findings or verification hypotheses. Prior
conclusions and deliverables remain in investigation history for later context
or an explicit recap; unrelated status, graph, and event sections are not
replayed in every answer.

The final report builder uses evidence IDs already recorded by the graph. For
structured inventories, it computes exact summaries from tool rows instead
of asking the model to count or compare them. For example, a single-step query
whose plan selects only `get_worker_graph` and has no hypotheses gets node and
edge counts and call-count summaries computed from the complete graph result.
This behavior is selected from the plan and tool result, not keywords in the
user's wording. Multi-step and verification plans continue through evidence
assessment so their explanations are reviewed normally.

Event evidence is grouped by trace ID and labeled with lifecycle signals so
the agent can choose a request, worker/process, or internal trace that matches
the question. Trace reports preserve returned `duration_ms` values. The agent
does not derive stage durations by subtracting event timestamps. Assessment
can cite only recorded evidence IDs. The assessor does not produce hypothesis
IDs; its ordered review list must have exactly one entry per supplied
hypothesis or assessment ends with a structured failure.

## Conversation continuity

An Agent Server thread checkpoints the conversation and active investigation,
but checkpointed history is not automatically sent wholesale to every model
call. Each graph stage builds a task-specific prompt:

- The classifier sees the latest user request and chooses only its broad route.
- The planner sees the latest request, recent conversation, the latest
  request recorded in the active investigation, that investigation's original
  question, and relevant saved findings and evidence. It decides semantically
  whether the request continues the investigation; the graph does not require a
  follow-up phrase or matching topic words. Recent chat messages can inform
  interpretation, but unrelated chat or out-of-scope messages are not promoted
  to investigation questions.
- The evidence gatherer sees the current request, plan, hypotheses, relevant
  findings, and bounded tool evidence. It does not receive the full transcript.
- The assessor sees the current request, the preceding request within the
  active investigation (empty on its first turn), that investigation's original
  question, requested outputs, plan, hypotheses, and evidence needed to assess
  the current turn. It does not receive the full transcript. A fresh
  investigation resets this question history, even when it starts in a thread
  that contains unrelated chat.
- General chat receives a bounded set of recent user and assistant messages.

The graph retains prior evidence only when the planner selects continuation
and the selected PID and worker instance remain consistent. The active
investigation's `questions` list supplies its latest and preceding requests;
`question` remains the original request for that investigation. These fields
are not reconstructed from the most recent human message in the whole thread,
which may be unrelated chat. A new investigation starts a fresh question list
with the current request, even on a thread that previously discussed another
topic. Prior hypotheses remain active only for a continued verification plan.
A different worker starts fresh so evidence from separate processes stays
apart. Set `new_investigation` to `true` to start fresh on the same worker. The
Agent Server checkpoints thread state; this service does not keep a separate
investigation database.

The intent classifier chooses only the broad route. The planner interprets
operational overview scope from the request and conversation context, deciding
whether it means all recently reporting workers or the selected worker. If the planner finds
the scope ambiguous, the graph asks for a structured choice and sends that
choice back to the planner to complete the plan. Scope is not inferred from a
required phrase or keyword list. The graph validates worker identity and
limits tools to the planned read-only capability set.

## Actions and approval

Actions use a separate path from evidence gathering. The graph only prepares
an action when the user clearly asks it to do so. It pauses the thread and
shows the action name and arguments. The action runs only after approval is
resumed on that same thread. Rejecting it ends the action without making a
change. Action results are reported separately from investigation evidence.

Two actions are available:

- **Record a deployment marker.** After approval, the agent checks the selected
  process instance through stored graph-diff data. If the marker already exists
  in backend history, it skips the write. Otherwise it calls
  `POST /api/v1/deployment` to record the marker in the backend repository.
- **Evaluate and record causal rules.** This calls
  `POST /api/v1/causal/evaluate`, evaluates rules against the selected
  process-instance snapshot, and stores matches in that instance's causal
  history.

The approval pause prevents accidental execution, but it does not replace
caller authorization. This first version uses one backend API key for all
runs, so keep the Agent Server on a trusted network.

## Planned follow-ups

### Mixed requests

Mixed-intent handling is not implemented. The classifier currently chooses
one route for the whole message, so a message such as “How are you, and show
my worker's graph” enters investigation without separately answering the
greeting. A future change should identify and handle both intents, then
present their answers together. Keep single-intent routing and the existing
approval flow for write actions intact. Add eval coverage for a greeting plus
investigation request and a product question plus investigation request.

### Documentation retrieval evolution

The first retrieval step is implemented: product chat fetches the published
MkDocs search index, ranks its sections locally, and passes a small set of
excerpts and canonical page links to the model. The index is cached in process
for ten minutes, so retrieval stays simple and sees published updates without
a separate ingestion job. Worker-investigation tools remain separate.

This is a lightweight search prototype, not a durable RAG pipeline. If the
documentation grows or live retrieval becomes unreliable, a follow-up can
sync the docs into a versioned local corpus and index it for richer retrieval
(e.g. hybrid or vector search). Keep the source URLs and excerpt provenance
with retrieved content so answers can cite the published pages. Add evaluation
coverage for product questions and retrieval outages before expanding the
retrieval system.

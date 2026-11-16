# Agent Architecture

This guide describes how the OriginTracer investigation graph routes requests, gathers and assesses evidence, and produces investigation reports. It also explains how the agent discovers and selects tools, maintains investigation context across conversations, and handles actions that require user approval. For installation and operating instructions, see *Setup and operations*.

---

## Request routing

Every new request first passes through an intent check, which determines how it should be handled. Requests involving greetings, date and time queries, OriginTracer product questions, or observability concepts are routed to a separate chat path that does not use worker tools.

For OriginTracer product questions, the chat path retrieves relevant documentation from the published MkDocs search index at `https://origintracer.app/docs/`. It ranks the available sections, selects a small number of relevant excerpts, and provides them to the model together with their source links. If documentation retrieval is unavailable, the model can fall back to a concise overview of the product. Requests about subjects unrelated to OriginTracer are redirected.

Requests concerning the OriginTracer runtime proceed through the investigation flow described below. Requests to perform one of the supported actions follow a separate approval flow, ensuring that operations which modify backend state are handled independently of read-only investigations.

---

## Worker scope and investigation flow

Each investigation is scoped to a single OriginTracer process, identified by a worker PID supplied by the caller. The agent resolves this PID to its stable `worker_instance_id` using the FastAPI snapshot inventory and retains both identifiers throughout the investigation, and this ensures that evidence and snapshots belonging to different process instances are never inadvertently combined.

The dedicated live-worker overview is the exception: it can inspect several
recently reporting workers at once. It does not broaden the scope of later
reads. A follow-up inspection uses the selected worker, and the agent does not
combine evidence from different process instances in one diagnosis.

Once the worker has been identified, the investigation proceeds through five stages:

1. **Set the scope.** The agent establishes the investigation scope using the user's question and the selected worker. If the worker is missing or unavailable, that must be resolved before the investigation can proceed.
2. **Choose an approach.** The planner determines whether the request requires a direct query, a set of related factual checks, or a comparison of possible explanations. It then produces a short investigation plan.
3. **Gather evidence.** Read-only tools query the selected worker through the OriginTracer backend. Each result retains its source information and worker identity so that it can be traced back to the correct process instance.
4. **Assess the evidence.** The assessor evaluates whether the gathered results address the investigation plan and whether they support, weaken, or leave each proposed explanation unresolved. When additional planned reads would be useful, the graph can gather more evidence before proceeding.
5. **Build the report.** The report presents findings supported by recorded evidence, together with any material uncertainties or limitations in the available data.

Evidence gathering is bounded to **four rounds**, with a maximum of **four tool calls per round**. If the available tools cannot distinguish between competing explanations, the report explicitly state that the cause remains unresolved rather than presenting an unsupported guess.

The read-only tools operate on the same stored snapshot and event data used by the React application. They cover worker status and inventory, graph nodes and edges, hotspots, latency, recent events, trace paths, causal rules, and graph changes. To retrieve this information, the agent first resolves the selected PID to its stable `worker_instance_id` and then calls the corresponding FastAPI stored-data routes using that instance ID.

Trace reads preserve each event's span ID, parent span ID, and timing fields.
When the trace contains complete request boundaries or a complete root span,
the backend reports its wall-clock duration. It also reports inclusive span
intervals, temporal overlaps between observed intervals, and self-time
estimates after subtracting returned direct-child intervals. Self-time may
include work from child operations that were not captured. Parentless overlaps
show temporal co-occurrence in the same trace; they do not establish
parent-child relationships or CPU parallelism. Cumulative inclusive time sums
observed span intervals, so nested and overlapping work can be counted more
than once. A trace that reaches the event retrieval cap is marked as possibly
truncated, and its wall-clock duration is withheld.

> **Important:** The agent never connects directly to a worker's Unix socket. FastAPI exposes the uploaded snapshot, event, and runtime APIs, while live socket inspection remains the responsibility of the OriginTracer REPL and local query server. For example, `get_worker_snapshot` retrieves the stored graph using the exact process-instance ID, event reads come from the backend event repository, and trace stitching combines uploaded event rows.

Some runtime information cannot be reconstructed from a graph snapshot alone. To make these fields available to the backend, the uploader sends them to `POST /api/v1/worker/runtime`. The heartbeat includes:
* Uptime
* Active-request count
* Active probes and rules
* Graph counts
* Semantic-label count
* Event-log size

FastAPI stores each heartbeat against its corresponding process-instance ID. Its age and freshness are then exposed through `/api/v1/status` and `/api/v1/snapshots`. A heartbeat is considered fresh for up to three snapshot intervals, subject to a minimum of 30 seconds and a maximum of five minutes. This provides a remote reporting signal about worker activity, not an OS-level process check. FastAPI does not proxy reads to local worker sockets.

Throughout the investigation, read-only tools are prevented from writing to the backend or modifying the instrumented process. This separation also allows new stored-data reads to be introduced without changing the investigation state model.

---

## Planning and tool discovery

The investigation planner works from a restricted collection of tools, where read tools are registered with capability metadata that allows the planner to identify which tools are relevant to the requested investigation.

It receives a compact catalog containing the names, capabilities, and short descriptions of all available read-only tools, together with the trusted identity of the selected worker. Capability terms are used to rank potentially relevant catalog entries, but they do not exclude tools simply because the user's wording differs from the terminology used in the catalog.

Based on this information, the planner defines the requested outputs as **deliverables** and links each deliverable to the tools documented as capable of supplying it. This also constrains what the plan can promise: it should not request a format or analysis that the selected tools cannot actually produce.

The planner's tool suggestions determine which full tool schemas are exposed during evidence gathering. Tool names must come from the registered catalog, preventing the planner from selecting arbitrary tools. If a plan contains no valid tool selections, the graph falls back to the complete read-only tool set. Regardless of how tools are selected, execution continues to enforce the selected-worker identity and the established read/write boundaries.

Individual read tools can also provide more specific guidance through their `ToolProfile` definitions. These profiles support two purposes:
* Helping the planner choose appropriate arguments.
* Helping the assessor interpret the returned evidence.

For example, event-related guidance distinguishes event probe names from graph node IDs, while ranking guidance explains whether a result is ordered by call count or execution duration. The relevant instructions are attached to the tool metadata and supplied to the model when needed.

Latency comparisons use the generic `get_worker_component_diagnostics` tool.
The planner supplies exact service labels from the selected worker, such as
`nginx`, `gunicorn`, or `asyncio`, and may include configured semantic system
aliases such as `database`. One call can compare up to five services and
aliases; adding a framework does not require a new pair-specific tool. The
result keeps aggregate node measurements, sampled events, and rule matches
distinct. Shared trace IDs help select traces to inspect but do not establish
causation.

The graph also tracks whether the planned reads have actually been performed. A plan step that specifies required tools remains pending until evidence from those tools has been recorded. If the research stage returns prose instead of executing a required read, the agent retries the tool selection once. It then continues only within the established round limit.

There is also a compatibility adjustment for `evaluate_worker_rules`. When an optional tags list is intended to be omitted but a local model supplies an empty object (`{}`), the graph normalizes that value to an omitted filter, allowing all configured rules to be evaluated. Other malformed filter values remain tool errors. This prevents a partially followed investigation plan from being incorrectly reported as complete.

---

## Assessment and report construction

Once evidence has been gathered, the assessor evaluates the results against the requested deliverables and proposed explanations. Although the assessor can propose interpretations and additional checks, deterministic graph logic validates references and structural requirements before any results are accepted into investigation state.

Each requested deliverable is explicitly classified as either **answered** or **unanswered**. Assessment completion is derived from this classification and the state of any pending planned reads. Consequently, completing the required tool calls does not, by itself, mean that a request has been answered.

The graph also applies a conservative validation rule: if an output is marked as answered without evidence from at least one of its linked tools, it is moved back to the unanswered category. Every unanswered deliverable is exposed in the final report.

Assessor-generated unresolved questions and suggested next steps must reference valid deliverable IDs. Questions associated with deliverables already classified as answered are omitted. Citation fields are similarly constrained: each assessment can reference only top-level evidence IDs, while completed-step fields can contain only plan IDs supplied to that particular run. Graph validation provides a second layer of enforcement for these requirements.

Hypothesis assessment follows an additional validation process:
* The assessor receives hypothesis claims in their original order, without internal IDs, and must return its reviews in that same order.
* The graph maps each returned review to its corresponding stored hypothesis. If the number of reviews does not match the number of supplied hypotheses, assessment stops.
* The graph also rejects reviews that claim a hypothesis is supported or weakened without providing evidence citations.
* For comparisons between components and affected outcomes, causal reviews remain inconclusive unless the evidence correlates component durations to individual outcomes. This does not prevent the report from stating measured values, rule matches, or other supported observations.

When one of these validation failures occurs, the graph records `status: assessment_failed`, stores the failure reason and gathered evidence IDs, returns an explicit message, and terminates before report synthesis. The gathered evidence remains available in thread state, but no completed `final_report` is produced. Unexpected runtime errors continue to raise normally.

The requirement for request-correlated component durations also applies when interpreting causal hypotheses more generally. If component durations cannot be associated with individual requests, causal hypothesis reviews must remain inconclusive. The graph enforces this condition as a backstop, independently of the assessor's interpretation.

Not every investigation requires hypotheses. They are reserved for verification plans that attempt to diagnose a problem or compare possible causes. Direct factual questions and metric comparisons can involve multiple reads without introducing causal hypotheses unnecessarily.

---

## Report scope and evidence selection

The final report is always scoped to the latest user turn, even when the investigation spans multiple turns of conversation. Previous evidence remains available to planning and assessment when the request continues an existing investigation, but the report itself focuses on the current request.

It includes the deliverables and findings relevant to the latest turn, together with newly gathered observations and older observations cited by current findings or hypothesis reviews. Unrelated sections from earlier status, graph, or event reports are not automatically repeated. Previous conclusions and deliverables remain in investigation history so that later follow-ups or an explicit recap can refer to them.

This approach allows an investigation to build on earlier work without making every response a repetition of the entire investigation history. It also ensures that a follow-up report reflects what the user is currently asking rather than simply replaying previously generated content.

The final report builder uses evidence IDs already recorded by the graph. Where a request concerns structured inventories, the builder can calculate exact summaries directly from tool results instead of relying on the model to count or compare records.

For example, when a single-step query selects only `get_worker_graph` and contains no hypotheses, the report builder calculates node counts, edge counts, and call-count summaries from the complete graph result. This behaviour is determined by the plan and tool result, not by keywords in the user's question. Multi-step investigations and verification plans continue through the normal evidence-assessment process so that their explanations receive the appropriate review.

Event evidence is organised by trace ID and labelled with lifecycle signals. These labels help the agent select the appropriate request, worker/process, or internal trace for the question being investigated.

Trace reports preserve the returned `duration_ms` values rather than deriving stage durations by subtracting event timestamps. This distinction ensures that the report reflects the timing information actually provided by the backend. Assessment can cite only recorded evidence IDs, and the assessor cannot introduce its own hypothesis IDs. Its ordered review list must contain exactly one entry for every supplied hypothesis; otherwise, assessment terminates with a structured failure.

---

## Conversation continuity

An Agent Server thread maintains checkpoints of the conversation and active investigation, but checkpointed history is not automatically passed in full to every model call. Instead, each stage of the investigation graph constructs a task-specific prompt containing only the context required for that stage.

This keeps the model focused on the current task while preserving the information needed to continue an investigation across multiple turns. The amount and type of context provided depend on the stage:

* **Classifier:** Receives the latest user request and determines only its broad route through the graph.
* **Planner:** Receives the latest request, recent conversation, the latest request recorded in the active investigation, the investigation's original question, and relevant saved findings and evidence. It determines semantically whether the request continues the existing investigation. This decision does not depend on a specific follow-up phrase or matching topic words. Recent chat messages can inform the interpretation, but unrelated conversation and out-of-scope messages are not promoted to investigation questions.
* **Evidence gatherer:** Receives the current request, investigation plan, hypotheses, relevant findings, and bounded tool evidence. It does not receive the full conversation transcript.
* **Assessor:** Receives the current request, the preceding request within the active investigation (which is empty on the first turn), the investigation's original question, requested outputs, plan, hypotheses, and the evidence needed to assess the current turn. It also does not receive the full transcript. Starting a fresh investigation resets this question history, even when the thread contains unrelated earlier conversation.
* **General chat:** Receives a bounded set of recent user and assistant messages.

The graph retains previous evidence only when the planner determines that the new request continues the existing investigation and the selected PID and worker instance remain consistent. This prevents evidence from being carried across unrelated investigations or different process instances.

Within the active investigation, the `questions` list tracks the latest and preceding requests, while `question` retains the original request that started the investigation. These fields are maintained as investigation state rather than reconstructed from the most recent human message in the entire thread, which may belong to an unrelated conversation.

A new investigation always starts with a fresh question list containing the current request, even if the thread has previously discussed another topic. Similarly, previous hypotheses remain active only when the investigation continues an existing verification plan. Selecting a different worker starts a fresh investigation so that evidence from separate processes cannot become mixed. To explicitly restart an investigation on the same worker, set `new_investigation` to `true`.

The Agent Server is responsible for checkpointing thread state. The OriginTracer service does not maintain a separate investigation database.

The distinction between routing and investigation planning is also important when determining the scope of operational overview requests. The intent classifier chooses only the broad route, while the planner interprets the requested operational scope using the request and conversation context. Depending on that context, an overview may refer to all recently reporting workers or only the selected worker.

If the planner cannot determine the intended scope, the graph asks the user for a structured choice and passes the response back to the planner to complete the plan. As with investigation continuation, scope is determined semantically rather than through a required phrase or keyword list. Once the plan is complete, the graph validates worker identity and restricts execution to the planned read-only capability set.

---

## Actions and approval

Operations that modify backend state follow a separate execution path from read-only investigations. The graph prepares an action only when the user has clearly requested it. Before execution, the thread is paused and the proposed action name and arguments are presented for approval.

The action runs only after the same thread is resumed with approval. If the user rejects the action, execution ends without making any changes. Results from these operations are reported separately from investigation evidence, preserving the distinction between observing system state and modifying it.

The current implementation supports two actions:

* **Record a deployment marker.** After approval, the agent checks the selected process instance against stored graph-diff data to determine whether the marker already exists in backend history. If it does, the write is skipped. Otherwise, the agent calls `POST /api/v1/deployment` to record the marker in the backend repository.
* **Evaluate and record causal rules.** After approval, the agent calls `POST /api/v1/causal/evaluate`, evaluates the configured rules against the selected process-instance snapshot, and stores any matches in that instance's causal history.

The approval mechanism prevents accidental execution, but it does not replace caller authorization. The current implementation uses a single backend API key for all runs, so the Agent Server must remain on a trusted network.

---

### Documentation retrieval evolution
The first stage of documentation retrieval is already implemented. Product-related chat requests retrieve the published MkDocs search index, rank its sections locally, and pass a small selection of relevant excerpts and canonical page links to the model.

The index is cached in process for ten minutes. This keeps the retrieval implementation lightweight while allowing it to reflect published documentation updates without requiring a separate ingestion job. Worker-investigation tools remain independent of this documentation retrieval path.

The current implementation is intentionally a lightweight search prototype rather than a durable retrieval-augmented generation (RAG) pipeline. If the documentation grows substantially or live retrieval becomes unreliable, a future improvement could synchronise the documentation into a versioned local corpus and index it for more advanced retrieval techniques, such as hybrid or vector search.

Any such implementation should preserve source URLs and excerpt provenance alongside retrieved content so that generated answers can continue to cite the published documentation accurately. Evaluation coverage for product questions and retrieval outages should also be added before expanding the retrieval system.

# Investigation Flow

An investigation is a bounded, evidence-linked workflow scoped to one worker
process instance.

1. **Classify the request.** Product questions and general chat use a separate route. Worker questions proceed to investigation; supported write requests enter an approval flow.
2. **Resolve scope.** The caller supplies `worker_pid`. The agent resolves it through the backend inventory and records both PID and stable `worker_instance_id`. A missing or stale worker is surfaced rather than silently replaced.
3. **Plan the reads.** The planner identifies requested deliverables, chooses tools from a capability catalog, and creates a short plan. The plan is bounded to four evidence rounds and four tool calls per round.
4. **Gather evidence.** Read-only tools query stored status, graph, event, trace, causal-rule, and diff data for the selected worker instance.
5. **Assess and continue.** The assessor links claims to recorded evidence and classifies each requested deliverable as answered or unanswered. The graph can gather more planned evidence within the round limit.
6. **Report.** Findings cite evidence and include unresolved questions and data limitations. The response does not claim causality when the evidence cannot distinguish possible explanations.

!!! note "Investigation limits"
    A run is scoped to one worker instance and bounded evidence rounds, and findings should retain uncertainty when the available evidence cannot prove a cause.

## Thread continuity

LangGraph checkpoints conversation and investigation state by thread. Related
turns can reuse evidence when the request continues the same investigation and
the worker PID and instance still match. Set `new_investigation: true` to start
fresh on the same worker. Changing the worker starts a fresh investigation.

Worker evidence is retrieved from the backend, while investigation state is carried in the Agent Server thread checkpoint.

## Actions

!!! warning "Writes require approval"
    The graph pauses before a deployment marker or causal-rule evaluation is recorded. The caller must resume the same thread with approval.

The graph prepares the requested action and pauses. The caller resumes the
same thread with an approval decision before execution. Supported actions are
recording a deployment marker and evaluating and recording causal rules.

The HTTP request and resume shapes are described in the [Agent Server API](api.md).
The Python graph, tools, and data models are documented in the API reference.

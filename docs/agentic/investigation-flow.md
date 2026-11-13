# Investigation Flow

An investigation is a bounded, evidence-linked workflow scoped to one worker process instance.

1. **Classify the request.** Product questions and general chat use a separate route. Worker questions proceed to investigation; supported write requests enter an approval flow.
2. **Resolve scope.** The caller supplies `worker_pid`. The agent resolves it through the backend inventory and records both PID and stable `worker_instance_id`. A missing or stale worker is surfaced rather than silently replaced.
3. **Plan the reads.** The planner identifies requested deliverables, chooses tools from a capability catalog, and creates a short plan. The plan is bounded to four evidence rounds and four tool calls per round.
4. **Gather evidence.** Read-only tools query stored status, graph, event, trace, causal-rule, and diff data for the selected worker instance.
5. **Assess and continue.** The assessor links claims to recorded evidence and classifies each requested deliverable as answered or unanswered. The graph can gather more planned evidence within the round limit.
6. **Report.** Findings cite evidence and include unresolved questions and data limitations. The response does not claim causality when the evidence cannot distinguish possible explanations.

!!! note "Investigation limits"
    A run is scoped to one worker instance and bounded evidence rounds, and findings should retain uncertainty when the available evidence cannot prove a cause.

## Thread continuity

LangGraph checkpoints conversation and investigation state by thread, and related turns can reuse evidence when the planner determines that the new request
continues the same investigation and the worker PID and instance still match. Set `new_investigation: true`to start fresh on the same worker, and also changing the worker starts fresh automatically.

Worker evidence is retrieved from the backend, while investigation state is carried in the Agent Server thread checkpoint.

## Actions

!!! warning "Writes require approval"
    The graph pauses before a deployment marker or causal-rule evaluation is recorded. The caller must resume the same thread with approval.

Write actions use a separate route, where the agent prepares the action and pauses; the caller must explicitly approve it on the same thread before execution. Supported actions are: recording a deployment marker and evaluating/recording
causal rules.

For the corresponding Python interfaces, see the [workflow graph API](../api/agent_graph.md), [tool API](../api/agent_tools.md), and [investigation data models](../api/agent_models.md).

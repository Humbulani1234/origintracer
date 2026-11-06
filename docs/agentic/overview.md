# Agentic Investigation

The optional OriginTracer Agent Server answers product questions and investigates uploaded worker evidence through a LangGraph workflow. It complements the React
dashboard and REPL: the agent queries the FastAPI backend's stored snapshots and events, while the REPL remains the interface for direct live socket queries.

## What it can do

!!! info "Where evidence comes from"
    The Agent Server investigates snapshots and events uploaded to the FastAPI backend. Direct live socket queries remain the REPL's role.

- Inspect reporting workers, status, graph nodes and edges, hotspots, events, traces, causal rules, and graph changes.
- Plan bounded evidence gathering, assess findings against the request, cite evidence, and state what remains uncertain.
- Continue an investigation on a LangGraph thread, or start a fresh one for the same worker. Switching to another worker starts a separate investigation.
- Prepare supported write actions, such as recording a deployment marker or causal-rule evaluation, and wait for explicit approval before executing them.
- Answer OriginTracer product questions using the published documentation search index when it is available.

Each request follows one route. If evidence cannot distinguish possible explanations, the report preserve that uncertainty instead of guessing at a cause.

The caller supplies the worker PID. It is the service responsibility to resolve that PID to a stable `worker_instance_id` and scopes every backend read to that process instance. The agent does not connect to the worker's local socket, and read-only investigations do not modify OriginTracer state.

## Guides

- [Investigation flow](investigation-flow.md) explains routing, worker identity, evidence collection, assessment, report construction, and approval.
- [Setup and operations](operations.md) covers local setup, configuration, request examples, approval handling, and troubleshooting.
- [Agent service API](../api/agent.md) is the entry point for the Python API reference, generated from documented classes and functions.

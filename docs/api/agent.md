# Agent Service API

The Agent Server is an optional Python application in `agent/`, separate from
the core `origintracer` distribution. Its API documentation is generated from
the source docstrings by `mkdocstrings`; signatures and docstrings update when
the static site is rebuilt. Only the symbols listed on these pages are
published, so add a directive and navigation entry when introducing a new
important public interface.

## Reference pages

- [Workflow graph](agent_graph.md): compiled LangGraph application and its
  major nodes and routing functions.
- [Tools](agent_tools.md): read-only worker tools, planning metadata, and
  approval-gated actions.
- [Backend client](agent_backend.md): stored-data client operations and
  query builders.
- [Data models](agent_models.md): investigation state and typed model outputs.

## Trust and scope contract

The caller supplies `worker_pid`; the agent resolves it to a stable
`worker_instance_id` and scopes reads to that process instance. Read tools use
stored backend routes and do not connect to worker sockets. Write tools are
separate actions and the graph pauses for explicit approval before running
them. The Agent Server's backend key is shared by runs in this deployment, so
protect the service and key at the network boundary.

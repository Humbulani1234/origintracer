# Python Agent API Reference

This page documents Python symbols in the optional Agent Server application
under `agent/`; it is not the HTTP API used by the chat client. The Agent
Server's LangGraph HTTP contract, authentication, thread, run, and resume flows
are described in the [Agent Server API guide](../agentic/api.md).

The reference below is generated from source docstrings by `mkdocstrings`.
Signatures and docstrings update when the static site is rebuilt. Only listed
symbols are published, so add a directive when introducing an important
interface.

## Reference pages

| Page | What it covers |
|---|---|
| [Workflow graph](agent_graph.md) | Request routing, investigation stages, assessment, and approval flow. |
| [Tools](agent_tools.md) | Read-only worker tools, planning metadata, and approved actions. |
| [Backend client](agent_backend.md) | Stored-data reads, approved writes, and query construction. |
| [Data models](agent_models.md) | Investigation state and typed plans, evidence, and report outputs. |

## Trust and scope contract

!!! info "Worker scope and actions"
    Reads are scoped to one resolved process instance. Writes are isolated
    behind an explicit approval pause.

The caller supplies `worker_pid`; the agent resolves it to a stable
`worker_instance_id` and scopes reads to that process instance. Read tools use
stored backend routes and do not connect to worker sockets. Write tools are
separate actions and the graph pauses for explicit approval before running
them. The Agent Server's backend key is shared by runs in this deployment, so
protect the service and key at the network boundary.

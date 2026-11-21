from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Annotated, Any

from langchain.tools import ToolRuntime, tool
from langchain_core.tools import ToolException
from pydantic import Field

from origintracer_agent.backend import (
    authenticated_backend_context,
    build_causal_query,
    build_diff_query,
    build_event_query,
    build_latency_query,
    build_trace_query,
    evaluate_and_record_rules,
    get_causal_history,
    list_workers,
    query_worker,
    record_deployment_marker,
    stitch_stored_trace,
    user_access_token_from_runtime,
    validate_pid,
)
from origintracer_agent.backend import (
    get_worker_snapshot as fetch_worker_snapshot,
)


def origintracer_tool(function: Any) -> Any:
    """Bind each tool's backend reads to its authenticated LangGraph user."""

    @wraps(function)
    async def call_with_user_scope(
        *args: Any, **kwargs: Any
    ) -> Any:
        runtime = kwargs.get("runtime")
        if runtime is None:
            runtime = next(
                (
                    value
                    for value in args
                    if hasattr(value, "config")
                    and hasattr(value, "state")
                ),
                None,
            )
        with authenticated_backend_context(
            user_access_token_from_runtime(runtime)
        ):
            return await function(*args, **kwargs)

    return tool(call_with_user_scope)


def _pid(runtime: ToolRuntime[Any]) -> int:
    """Read the PID label for the selected stored process snapshot."""
    return validate_pid(runtime.state.get("worker_pid"))


def _instance_id(runtime: ToolRuntime[Any]) -> str | None:
    """Return the selected stable process-instance key, when resolved."""
    value = runtime.state.get("worker_instance_id")
    return str(value) if value else None


@origintracer_tool
async def list_live_workers(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """List worker instances with fresh runtime heartbeats.

    Use this when no worker is selected or when checking which workers are reporting. It
    reads the authenticated backend snapshot inventory, excludes stale snapshots, and does
    not access local worker sockets.

    Returns
    -------
    dict[str, Any]
        Inventory source, observation time, count, and reporting worker rows. Each row
        includes a PID and stable worker-instance ID.

    Parameters
    ----------
    runtime : ToolRuntime
        Authenticated request context supplied by LangGraph Server.
    """
    snapshots = await list_workers()
    reporting = [
        worker for worker in snapshots if worker["online"]
    ]
    return {
        "source": "OriginTracer stored snapshot inventory",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "total_live_workers": len(reporting),
        "result": reporting,
    }


def _overview_graph_payload(result: Any) -> Any:
    """Bound graph detail while retaining counts and an explicit preview flag."""
    graph = (
        result.get("data", result)
        if isinstance(result, dict)
        else result
    )
    if not isinstance(graph, dict):
        return graph
    nodes = graph.get("nodes")
    edges = graph.get("edges")
    if not isinstance(nodes, list) or not isinstance(
        edges, list
    ):
        return graph
    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes[:20],
        "edges": edges[:20],
        "nodes_truncated": len(nodes) > 20,
        "edges_truncated": len(edges) > 20,
    }


@origintracer_tool
async def get_live_workers_overview(
    runtime: ToolRuntime[Any],
    max_workers: int = 10,
) -> dict[str, Any]:
    """
    Read a bounded overview of recently reporting workers.

    This read-only overview needs no caller-selected PID. It inspects
    the authenticated snapshot inventory and reports partial failures
    by worker or section. Each worker's graph preview is limited
    to 20 nodes and 20 edges.

    Parameters
    ----------
    max_workers : int, default=10
        Maximum number of reporting workers to inspect. The value
        is clamped to the range 1–20.
    runtime : ToolRuntime
        Authenticated request context supplied by LangGraph Server.

    Returns
    -------
    dict[str, Any]
        Worker status, graph preview, causal-rule matches, registered
        rules, and interpretation limits. Truncation flags identify workers
        or graph rows omitted from the preview.
    """
    bounded_workers = max(1, min(int(max_workers), 20))
    snapshots = await list_workers()
    workers = [
        worker for worker in snapshots if worker["online"]
    ]
    shown = workers[:bounded_workers]
    semaphore = asyncio.Semaphore(4)

    async def read_section(
        worker: dict[str, str], query: str
    ) -> Any:
        async with semaphore:
            try:
                response = await query_worker(
                    int(worker["pid"]),
                    query,
                    worker_instance_id=worker[
                        "worker_instance_id"
                    ],
                )
                return response.get("result", response)
            except Exception as exc:
                return {"error": str(exc)}

    async def inspect_worker(
        worker: dict[str, str],
    ) -> dict[str, Any]:
        status, graph, matches, registered = (
            await asyncio.gather(
                read_section(worker, "SHOW status"),
                read_section(worker, "SHOW graph"),
                read_section(worker, build_causal_query(None)),
                read_section(worker, "SHOW rules"),
            )
        )
        return {
            "pid": worker["pid"],
            "worker_instance_id": worker["worker_instance_id"],
            "status": status,
            "graph": _overview_graph_payload(graph),
            "causal_rule_matches": matches,
            "registered_causal_rules": registered,
        }

    inspected = await asyncio.gather(
        *(inspect_worker(worker) for worker in shown)
    )
    return {
        "source": "OriginTracer stored multi-worker overview",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "total_live_workers": len(workers),
        "workers_shown": len(inspected),
        "workers_truncated": len(workers) > len(shown),
        "worker_limit": bounded_workers,
        "workers": inspected,
        "interpretation_limits": [
            "Each worker's status, graph, and rule results are scoped to that worker instance.",
            "Only process instances with fresh uploaded runtime heartbeats are included.",
            "Heartbeat freshness is inferred from the uploader interval and does not replace an operating-system process check.",
            "Active probes and rules come from that process instance's uploaded runtime heartbeat.",
            "Rule matches and graph structure are detected patterns and relationships, not proof of root cause or impact.",
            "Graph nodes and edges are capped at 20 each per worker; truncation flags identify omitted rows.",
        ],
    }


@origintracer_tool
async def get_worker_status(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Read uploaded heartbeat and graph status for the selected worker.

    A stale heartbeat is reported as offline. This reads stored backend data and does not
    probe the process directly.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker PID and, when resolved, its
        stable instance ID.

    Returns
    -------
    dict[str, Any]
        Uploaded uptime, active requests, active probes, graph counts, and heartbeat
        freshness.
    """
    return await query_worker(
        _pid(runtime), "SHOW status", _instance_id(runtime)
    )


@origintracer_tool
async def get_worker_snapshot(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Read the saved graph for the selected worker process instance.

    The tool requires the instance ID resolved from the selected stored snapshot and never
    chooses the newest customer snapshot implicitly.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker PID and stable instance ID.

    Returns
    -------
    dict[str, Any]
        Saved graph snapshot for exactly the selected process instance.

    Raises
    ------
    ToolException
        If the selected worker has no registered snapshot identity.
    """
    instance_id = runtime.state.get("worker_instance_id")
    if not instance_id:
        raise ToolException(
            "The selected worker has no registered snapshot identity."
        )
    return await fetch_worker_snapshot(str(instance_id))


@origintracer_tool
async def get_worker_nodes(
    runtime: ToolRuntime[Any], limit: int = 50
) -> dict[str, Any]:
    """List operation nodes from the selected worker's stored snapshot.

    Rows include operation name, service, call count, aggregate duration, and safe
    operational metadata when the probe recorded it. Results are not ranked by latency.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    limit : int, default=50
        Maximum number of rows to return, clamped to 1–100.

    Returns
    -------
    dict[str, Any]
        Operation nodes observed in the saved graph.
    """
    return await query_worker(
        _pid(runtime),
        f"SHOW nodes LIMIT {max(1, min(limit, 100))}",
        _instance_id(runtime),
    )


@origintracer_tool
async def get_worker_edges(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Show observed caller-to-callee relationships in the worker graph.

    Edges describe direction and call count, but do not include per-edge latency. Use them
    to understand how work flows between operations or to identify the caller of a hotspot.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.

    Returns
    -------
    dict[str, Any]
        Caller-to-callee edges from the selected worker's saved graph.
    """
    return await query_worker(
        _pid(runtime), "SHOW edges", _instance_id(runtime)
    )


@origintracer_tool
async def get_worker_graph(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Read the selected worker's combined node and edge graph.

    Nodes describe observed operations and edges describe observed relationships. Node
    rows may include safe operational metadata; this structural view does not provide
    per-edge latency.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.

    Returns
    -------
    dict[str, Any]
        The selected worker's saved graph.
    """
    return await query_worker(
        _pid(runtime), "SHOW graph", _instance_id(runtime)
    )


@origintracer_tool
async def get_worker_hotspots(
    runtime: ToolRuntime[Any], top: int = 10
) -> dict[str, Any]:
    """Rank graph nodes by call count.

    This is a frequency ranking, not a latency ranking: a frequently called node may be
    fast, while a slow database query may rank lower. Use ``get_worker_database_hotspots``
    to inspect database durations.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    top : int, default=10
        Number of nodes to request, clamped to 1–100.

    Returns
    -------
    dict[str, Any]
        Graph nodes ranked by call count.
    """
    return await query_worker(
        _pid(runtime),
        f"HOTSPOT TOP {max(1, min(top, 100))}",
        _instance_id(runtime),
    )


@origintracer_tool
async def get_worker_database_hotspots(
    runtime: ToolRuntime[Any], limit: int = 20
) -> dict[str, Any]:
    """Rank observed database query nodes by average duration.

    Results include query name, call count, average duration, and total duration for nodes
    in the configured ``database`` semantic group. Empty results may mean that the group is
    unconfigured or no database query nodes were observed; check probes and workload before
    drawing a conclusion.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    limit : int, default=20
        Maximum number of rows to return, clamped to 1–100.

    Returns
    -------
    dict[str, Any]
        Database query nodes ranked by average duration.
    """
    bounded_limit = max(1, min(limit, 100))
    return await query_worker(
        _pid(runtime),
        f'SHOW latency WHERE system = "database" LIMIT {bounded_limit}',
        _instance_id(runtime),
    )


@origintracer_tool
async def get_worker_database_events(
    runtime: ToolRuntime[Any],
    limit: int = 50,
    trace_id: str | None = None,
) -> dict[str, Any]:
    """List captured Django SQL query events, optionally narrowed to one trace.

    Event rows identify SQL operations and trace IDs, but do not include query duration. Use
    ``get_worker_database_hotspots`` for aggregate timings and ``get_worker_trace`` to
    inspect a request. Results are limited to the latest 100 matching events.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    limit : int, default=50
        Requested result count, capped at 100.
    trace_id : str, optional
        Trace ID used to narrow the event query.

    Returns
    -------
    dict[str, Any]
        Captured Django SQL query events and their trace identifiers.
    """
    query = build_event_query(
        limit,
        trace_id=trace_id,
        service="django",
        probe="django.db.query",
    )
    return await query_worker(
        _pid(runtime), query, _instance_id(runtime)
    )


@origintracer_tool
async def get_worker_component_diagnostics(
    runtime: ToolRuntime[Any],
    services: Annotated[
        list[str] | None,
        Field(
            description=(
                "Exact graph service labels to inspect, such as nginx or "
                "asyncio. Use labels present in the worker graph."
            ),
            max_length=5,
        ),
    ] = None,
    systems: Annotated[
        list[str] | None,
        Field(
            description=(
                "Configured semantic system labels to inspect, such as "
                "database. Use for graph nodes grouped by a configured alias."
            ),
            max_length=5,
        ),
    ] = None,
    outcome_services: Annotated[
        list[str] | None,
        Field(
            description=(
                "Optional subset of services whose measurements represent "
                "the affected request or outcome."
            ),
            max_length=5,
        ),
    ] = None,
    limit: Annotated[
        int,
        Field(description="Rows per section, clamped to 1–10."),
    ] = 5,
) -> dict[str, Any]:
    """Read latency and event evidence for caller-selected worker services.

    This generic diagnostic supports arbitrary graph service labels and
    configured semantic system labels. It returns aggregate node measurements
    separately from sampled events, and reports shared trace IDs without
    inferring request-level causation.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    services : list[str], optional
        Exact graph service labels, such as ``nginx`` or ``asyncio``.
    systems : list[str], optional
        Configured semantic system labels, such as ``database``.
    outcome_services : list[str], optional
        Services whose graph nodes represent the affected request or outcome.
    limit : int, default=5
        Maximum latency rows per selector and event rows per service, clamped
        to 1–10.

    Returns
    -------
    dict[str, Any]
        Scoped latency rows, per-service events, shared trace IDs, and current
        causal-rule observations.

    Raises
    ------
    ToolException
        If no selectors are supplied, a selector is invalid, or a selected
        worker identity is unavailable.
    """
    selected_services = list(dict.fromkeys(services or []))
    selected_systems = list(dict.fromkeys(systems or []))
    selected_outcomes = set(outcome_services or [])
    if not selected_services and not selected_systems:
        raise ToolException(
            "Provide at least one service or configured system to inspect."
        )
    if len(selected_services) + len(selected_systems) > 5:
        raise ToolException(
            "Inspect no more than five services and systems in one call."
        )
    if not selected_outcomes.issubset(selected_services):
        raise ToolException(
            "outcome_services must be included in services."
        )

    pid = _pid(runtime)
    instance_id = _instance_id(runtime)
    bounded_limit = max(1, min(limit, 10))
    requests: list[tuple[str, str, Any]] = []
    for service in selected_services:
        latency_query = build_latency_query(
            bounded_limit, service=service
        )
        events_query = build_event_query(
            min(max(bounded_limit * 10, 20), 100),
            service=service,
        )
        requests.append(
            ("service_latency", service, latency_query)
        )
        requests.append(
            ("service_events", service, events_query)
        )
    for system in selected_systems:
        requests.append(
            (
                "system_latency",
                system,
                build_latency_query(
                    bounded_limit, system=system
                ),
            )
        )

    query_results = await asyncio.gather(
        *(
            query_worker(pid, query, instance_id)
            for _, _, query in requests
        ),
        query_worker(pid, build_causal_query(None), instance_id),
        query_worker(pid, "SHOW rules", instance_id),
    )
    service_measurements: dict[str, Any] = {}
    system_measurements: dict[str, Any] = {}
    service_events: dict[str, Any] = {}
    for (kind, selector, _), response in zip(
        requests, query_results[: len(requests)]
    ):
        payload = response.get("result", response)
        if not isinstance(payload, dict):
            payload = (
                {"data": payload}
                if isinstance(payload, list)
                else {}
            )
        rows = payload.get("data")
        if not isinstance(rows, list):
            rows = []
        if kind == "service_events":
            events = [
                row for row in rows if isinstance(row, dict)
            ][-bounded_limit:]
            service_events[selector] = {
                "service": selector,
                "data": events,
                "trace_ids": list(
                    dict.fromkeys(
                        str(row["trace_id"])
                        for row in events
                        if row.get("trace_id")
                    )
                ),
            }
            continue

        scope = "component" if kind == "system_latency" else None
        annotated_rows = []
        for row in rows[:bounded_limit]:
            if not isinstance(row, dict):
                continue
            node_name = str(row.get("node", "")).split("::", 1)[
                -1
            ]
            row_scope = scope or (
                "outcome"
                if selector in selected_outcomes
                or node_name.startswith("/")
                else "component"
            )
            annotated_rows.append(
                {
                    **row,
                    "component": selector,
                    "measurement_scope": row_scope,
                }
            )
        section = {
            **payload,
            "selector": selector,
            "measurement_scope": scope
            or (
                "outcome"
                if selector in selected_outcomes
                else "component"
            ),
            "data": annotated_rows,
        }
        if kind == "service_latency":
            service_measurements[selector] = section
        else:
            system_measurements[selector] = section

    trace_sets = [
        set(section["trace_ids"])
        for section in service_events.values()
        if section.get("trace_ids")
    ]
    shared_trace_ids = (
        sorted(set.intersection(*trace_sets))
        if len(trace_sets) > 1
        else []
    )
    causal_response = query_results[-2].get(
        "result", query_results[-2]
    )
    active_rules_response = query_results[-1].get(
        "result", query_results[-1]
    )
    return {
        "source": "OriginTracer stored snapshot latency diagnostics",
        "worker_pid": str(pid),
        "query": "service/system latency and service events",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "result": {
            "service_measurements": service_measurements,
            "system_measurements": system_measurements,
            "service_events": service_events,
            "shared_trace_ids": shared_trace_ids,
            "causal_rule_matches": causal_response,
            "worker_active_rules": active_rules_response,
            "interpretation_limits": [
                (
                    "Latency values are graph-node averages or aggregates "
                    "across observed calls, not per-request durations."
                ),
                (
                    "A shared trace ID groups recorded events; inspect its "
                    "trace before drawing timing or causal conclusions."
                ),
                (
                    "Event timestamps do not provide event durations, and "
                    "empty service data does not prove the service is absent."
                ),
            ],
        },
    }


@origintracer_tool
async def get_worker_events(
    runtime: ToolRuntime[Any],
    limit: Annotated[
        int,
        Field(
            description="Maximum event rows to return; capped at 100."
        ),
    ] = 20,
    trace_id: Annotated[
        str,
        Field(
            description=(
                "Optional exact trace ID filter. Leave empty to include "
                "events from all traces."
            )
        ),
    ] = "",
    service_name: Annotated[
        str,
        Field(
            description=(
                "Optional event service label, such as django or asyncio. "
                "This is not a worker PID; worker selection is supplied "
                "separately. Leave empty for all services."
            )
        ),
    ] = "",
    probe: Annotated[
        str,
        Field(
            description=(
                "Optional exact event probe filter, such as "
                "django.view.enter, django.db.query, or request.exit. "
                "This matches the event's probe field, not a graph node ID "
                "or operation name. Leave empty to include all probes; to "
                "find events related to an operation, filter by service and "
                "match returned event names and trace IDs."
            )
        ),
    ] = "",
) -> dict[str, Any]:
    """Read recent events captured by the selected worker.

    The selected PID comes from trusted graph state and is never a query filter. Events are
    retained in memory, so an empty result does not prove that an event never occurred. A
    graph node ID or operation name is not an event probe; query by service and inspect
    returned event names and trace IDs instead.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    limit : int
        Requested event count, capped at 100.
    trace_id : str, optional
        Exact trace ID used to narrow results.
    service_name : str, optional
        Service name used to narrow results.
    probe : str, optional
        Exact event probe name, such as ``django.view.enter`` or ``request.exit``. This
        matches the event's probe field.

    Returns
    -------
    dict[str, Any]
        Recent matching events from the selected worker, including recorded duration,
        process/thread and span identifiers, and safe operational metadata when present.
    """
    query = build_event_query(
        limit,
        trace_id or None,
        service_name or None,
        probe or None,
    )
    return await query_worker(
        _pid(runtime), query, _instance_id(runtime)
    )


@origintracer_tool
async def get_worker_trace(
    runtime: ToolRuntime[Any], trace_id: str
) -> dict[str, Any]:
    """Inspect the critical path for one trace ID.

    Use a trace ID returned by ``get_worker_events`` or ``get_worker_database_events`` to
    connect query and event observations to one request. Only retained trace data is
    available.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    trace_id : str
        Exact trace ID to inspect.

    Returns
    -------
    dict[str, Any]
        Ordered event rows with span identity, parent lineage, process/thread IDs,
        and safe operational metadata when present. The timing
        section includes trace wall duration when a request boundary pair or
        complete root span exists, span intervals, inclusive durations,
        observed-child self-time estimates, sibling overlap, and cumulative
        inclusive duration. The response also reports observed lifecycle
        boundaries, any matching tracker record, and recorded capture-loss
        counters when those are available. Self-time may include unobserved
        child work, and overlap does not establish CPU parallelism. Event gaps
        and duration sums are labeled separately.
    """
    query = build_trace_query(trace_id)
    return await query_worker(
        _pid(runtime), query, _instance_id(runtime)
    )


@origintracer_tool
async def get_worker_stitched_trace(
    runtime: ToolRuntime[Any], trace_id: str
) -> dict[str, Any]:
    """Join one explicitly identified trace across stored process instances.

    Use this only when asked to follow a trace across processes. The tool searches uploaded
    event rows for the exact trace ID and does not guess IDs or inspect arbitrary queries.
    Each stage retains its worker PID and recorded duration; no cross-process end-to-end
    duration is computed.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    trace_id : str
        Exact trace ID to search across stored process instances.

    Returns
    -------
    dict[str, Any]
        Matching trace stages, workers searched, errors, observation time, and
        interpretation limits.
    """
    selected_pid = _pid(runtime)
    payload = await stitch_stored_trace(trace_id)
    stages = payload.get("stages", [])
    workers_found = payload.get("workers_found", [])
    errors = payload.get("errors", [])
    return {
        "source": "OriginTracer stitched stored event trace",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "trace_id": trace_id,
        "selected_worker_pid": str(selected_pid),
        "workers_searched": payload.get("workers_searched", []),
        "workers_found": workers_found,
        "stages": stages,
        "errors": errors,
        "interpretation_limits": [
            (
                "duration_ms is each event's recorded duration when "
                "available; no cross-process wall-clock duration is computed."
            ),
            (
                "A worker with no matching retained events is not evidence "
                "that the process was absent from the original request."
            ),
        ],
    }


@origintracer_tool
async def get_worker_causal_history(
    runtime: ToolRuntime[Any], limit: int = 50
) -> dict[str, Any]:
    """Read saved causal-rule match history for the selected worker instance.

    This reads backend history; it does not evaluate rules or save new matches. Use
    ``get_worker_events`` for recent raw event history.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker PID and stable instance ID.
    limit : int, default=50
        Maximum number of saved history entries to return.

    Returns
    -------
    dict[str, Any]
        Saved causal-rule history and selected worker identity.

    Raises
    ------
    ToolException
        If the selected worker has no registered snapshot identity.
    """
    instance_id = runtime.state.get("worker_instance_id")
    if not instance_id:
        raise ToolException(
            "The selected worker has no registered snapshot identity."
        )
    result = await get_causal_history(str(instance_id), limit)
    return {
        "source": "OriginTracer saved causal history",
        "worker_pid": str(_pid(runtime)),
        "worker_instance_id": str(instance_id),
        "query": "saved causal-rule history",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "result": result.get("data", result),
    }


@origintracer_tool
async def evaluate_worker_rules(
    runtime: ToolRuntime[Any], tags: list[str] | None = None
) -> dict[str, Any]:
    """Evaluate backend-registered causal rules on the selected stored graph.

    The backend evaluates its registered rules against the uploaded snapshot. Per-process
    rule configuration is not included in that snapshot, so this tool cannot establish which
    rules the originating process loaded. A match shows that a rule pattern fired; it does
    not prove root cause. This read-only query does not save matches to causal history.

    Optional tags filter rule categories: ``db``, ``n+1``, ``performance``, ``asyncio``,
    ``async``, ``latency``, ``blocking``, ``deployment``, ``langgraph``, ``agent``, and
    ``loop``. Prefer this tool when asked which rules matched without a broader latency
    comparison.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    tags : list[str], optional
        Rule categories to include. Omit this argument to evaluate every configured
        backend rule.

    Returns
    -------
    dict[str, Any]
        Matches and registered rules returned by the backend for the selected snapshot.
    """
    pid = _pid(runtime)
    instance_id = _instance_id(runtime)
    matches, registered = await asyncio.gather(
        query_worker(pid, build_causal_query(tags), instance_id),
        query_worker(pid, "SHOW rules", instance_id),
    )
    return {
        "source": "OriginTracer stored graph causal-rule review",
        "worker_pid": str(pid),
        "query": "configured rule matches and registered rules",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "result": {
            "matches": matches.get("result", matches),
            "registered_rules": registered.get(
                "result", registered
            ),
            "interpretation_limit": (
                "A rule match indicates a detected graph pattern, not proof "
                "that it caused the reported issue."
            ),
        },
    }


@origintracer_tool
async def get_worker_changes(
    runtime: ToolRuntime[Any], since: str = "deployment"
) -> dict[str, Any]:
    """Compare the current worker graph with a prior marker or time.

    Structural changes show which nodes and edges were added or removed, but do not by
    themselves explain a latency change.

    Parameters
    ----------
    runtime : ToolRuntime
        Trusted graph state containing the selected worker identity.
    since : str, optional
        Deployment marker or timestamp to compare against. The backend default marker is
        used when omitted.

    Returns
    -------
    dict[str, Any]
        Graph differences since the selected marker or time.
    """
    return await query_worker(
        _pid(runtime),
        build_diff_query(since),
        _instance_id(runtime),
    )


@dataclass(frozen=True)
class ToolProfile:
    """Catalog and interpretation metadata for one read-only tool.

    Attributes
    ----------
    tool : Any
        LangChain tool whose schema may be bound to the research model.
    capability : str
        Short capability name exposed in the planner's catalog.
    terms : tuple[str, ...]
        Request phrases used to rank this tool in the compact catalog.
    assessment_guidance : str
        Interpretation guidance added to the assessor prompt when this tool
        contributed evidence.
    research_guidance : str
        Argument-selection guidance added when this tool is planned.
    requires_selected_worker : bool
        Whether the tool is available only after a worker has been selected.
    """

    tool: Any
    capability: str
    terms: tuple[str, ...]
    assessment_guidance: str = ""
    research_guidance: str = ""
    requires_selected_worker: bool = True


READ_ONLY_TOOL_PROFILES = (
    ToolProfile(
        get_live_workers_overview,
        "multi_worker_overview",
        (
            "overview of workers",
            "workers overview",
            "overview of processes",
            "running processes",
            "all currently reporting workers",
            "all workers",
            "all processes",
            "worker fleet overview",
        ),
        assessment_guidance=(
            "get_live_workers_overview reports independently scoped stored "
            "status, snapshot, graph, and causal-rule data for each process "
            "with a fresh uploader heartbeat. "
            "Report worker IDs accurately, identify graph truncation, and "
            "treat rules and topology as patterns rather than proof of risk "
            "impact or root cause. Distinguish an empty result from an error. "
            "Keep every status, graph, and rule value attached to its PID; "
            "never copy fields between workers. A node label alone does not "
            "establish a semantic pattern such as N+1. Do not multiply an "
            "average duration by a call count or infer per-user impact."
        ),
        research_guidance=(
            "Use get_live_workers_overview when the plan requests all "
            "recently reporting workers. Its trusted backend inventory "
            "selects process instances with fresh runtime heartbeats, then "
            "the tool inspects each returned worker. Do not pass a PID or "
            "make separate selected-worker reads for this all-worker scope."
        ),
        requires_selected_worker=False,
    ),
    ToolProfile(
        list_live_workers,
        "worker_inventory",
        (
            "currently reporting workers",
            "live worker inventory",
            "which workers are live",
            "reported workers",
            "worker list",
            "processes",
            "pids",
            "available worker",
        ),
        requires_selected_worker=False,
    ),
    ToolProfile(
        get_worker_status,
        "worker_status",
        (
            "worker health and status",
            "uptime and active requests",
            "active probes",
            "snapshot time",
            "stored worker status",
        ),
    ),
    ToolProfile(
        get_worker_snapshot,
        "saved_snapshot",
        ("snapshot", "saved graph", "uploaded", "snapshot data"),
    ),
    ToolProfile(
        get_worker_nodes,
        "graph_nodes",
        (
            "nodes",
            "node inventory",
            "operations",
            "graph inventory",
        ),
    ),
    ToolProfile(
        get_worker_edges,
        "graph_edges",
        (
            "edges",
            "callers",
            "call graph",
            "relationships",
            "call flow",
        ),
    ),
    ToolProfile(
        get_worker_graph,
        "graph_overview",
        (
            "graph",
            "worker graph",
            "dependency graph",
            "graph overview",
        ),
    ),
    ToolProfile(
        get_worker_hotspots,
        "call_count_hotspots",
        (
            "hotspots",
            "hot spot",
            "most called",
            "call count",
            "frequent",
        ),
        assessment_guidance=(
            "get_worker_hotspots ranks graph nodes by call_count. Its "
            "avg_duration_ms values are context only and do not determine "
            "the ranking. High call frequency alone does not establish "
            "request-level latency impact."
        ),
    ),
    ToolProfile(
        get_worker_database_hotspots,
        "database_latency",
        (
            "database latency",
            "sql latency",
            "slow query",
            "slowest database query",
            "slowest database queries",
            "slow database query",
            "slow database queries",
            "database query duration",
            "database query durations",
            "rank database queries",
            "query duration",
            "database hotspot",
        ),
        assessment_guidance=(
            "get_worker_database_hotspots ranks database nodes by "
            "avg_duration_ms. total_duration_ms is aggregated across "
            "call_count observations; it is not the ranking key or one "
            "request's latency. Without a supplied baseline, do not label "
            "a query absolutely slow. This ranking alone does not establish "
            "request-level impact."
        ),
    ),
    ToolProfile(
        get_worker_database_events,
        "database_events",
        (
            "sql events",
            "database events",
            "query events",
            "sql trace",
        ),
    ),
    ToolProfile(
        get_worker_component_diagnostics,
        "component_latency_diagnostics",
        (
            "compare",
            " versus ",
            " vs ",
            "latency",
            "duration",
            "slow request",
            "compare bottlenecks",
            "compare services",
            "compare components",
            "service latency",
            "component latency",
        ),
        assessment_guidance=(
            "get_worker_component_diagnostics reads aggregate latency rows "
            "for caller-selected exact service labels and configured system "
            "aliases, plus sampled events for selected services. It marks "
            "outcome_services as outcome scope; other rows are component "
            "scope. Shared trace IDs identify events to inspect, not proof "
            "of causal contribution. Use get_worker_trace for a relevant "
            "trace. Empty results do not prove a service is absent."
        ),
        research_guidance=(
            "For service comparisons, pass exact service labels from the "
            "selected worker's graph. Use `systems` only for configured "
            "semantic aliases such as database. Mark a service as an outcome "
            "only when the request identifies it as the affected operation."
        ),
    ),
    ToolProfile(
        get_worker_events,
        "events",
        (
            "events",
            "recent event",
            "trace id",
            "event history",
            "loop ticks",
        ),
        research_guidance=(
            "For get_worker_events, `probe` is an exact event-probe filter "
            "such as django.view.enter or request.exit. Do not pass a graph "
            "node ID or operation name as `probe`. To check which recent "
            "events relate to an operation, filter by its service only, leave "
            "`probe` empty, and inspect returned event `name`, `probe`, and "
            "trace ID fields. An empty filtered result only means that exact "
            "filter matched nothing in the retained event set."
        ),
    ),
    ToolProfile(
        get_worker_trace,
        "trace",
        (
            "trace",
            "critical path",
            "request path",
            "trace stages",
            "trace duration",
        ),
    ),
    ToolProfile(
        get_worker_stitched_trace,
        "cross_process_trace",
        (
            "stitch",
            "stitched trace",
            "cross process",
            "across processes",
            "across workers",
        ),
    ),
    ToolProfile(
        get_worker_causal_history,
        "causal_history",
        (
            "causal history",
            "saved rule history",
            "saved matches",
            "rule history",
        ),
    ),
    ToolProfile(
        evaluate_worker_rules,
        "causal_rules",
        (
            "causal rule",
            "rule match",
            "matching rules",
            "configured rules",
            "evaluate rules",
            "pattern rules",
            "rule patterns",
            "n+1 query",
            "database query hotspot",
            "query hotspot pattern",
        ),
    ),
    ToolProfile(
        get_worker_changes,
        "graph_changes",
        (
            "changes",
            "diff",
            "deployment",
            "since marker",
            "added or removed",
        ),
    ),
)

READ_ONLY_TOOLS = [
    profile.tool for profile in READ_ONLY_TOOL_PROFILES
]


def assessment_guidance_for_tools(
    tool_names: set[str],
) -> list[str]:
    """Return evidence-interpretation guidance for observed tools.

    Guidance lives beside each tool profile, so it follows the evidence
    actually gathered without request-keyword or tool-pair checks in the graph.

    Parameters
    ----------
    tool_names : set[str]
        Names of tools that returned evidence during the investigation.

    Returns
    -------
    list[str]
        Non-empty interpretation guidance for the observed tools.
    """
    return [
        profile.assessment_guidance
        for profile in READ_ONLY_TOOL_PROFILES
        if profile.tool.name in tool_names
        and profile.assessment_guidance
    ]


def research_guidance_for_tools(
    tool_names: set[str],
) -> list[str]:
    """Return tool-specific read semantics for planned observations.

    Parameters
    ----------
    tool_names : set[str]
        Names of tools selected for the next evidence-gathering round.

    Returns
    -------
    list[str]
        Non-empty research guidance for the planned tools.
    """
    return [
        profile.research_guidance
        for profile in READ_ONLY_TOOL_PROFILES
        if profile.tool.name in tool_names
        and profile.research_guidance
    ]


def _read_only_profiles_for_scope(
    *, selected_worker: bool
) -> list[ToolProfile]:
    """Expose only tools whose worker scope is available to this run."""
    return [
        profile
        for profile in READ_ONLY_TOOL_PROFILES
        if selected_worker
        or not profile.requires_selected_worker
    ]


def select_planned_read_only_tools(
    suggested_names: list[str],
    *,
    selected_worker: bool = True,
    request: str = "",
) -> list[Any]:
    """Bind schemas for planner-selected tools from the read-only allowlist.

    Unknown or out-of-scope names are ignored. If the plan contains no usable
    name, make the complete set eligible for this worker scope available so
    an invalid or empty plan cannot expose tools requiring an absent PID.

    Parameters
    ----------
    suggested_names : list[str]
        Tool names proposed by the planner.
    selected_worker : bool, default=True
        Whether the graph has resolved a worker for this run.
    request : str, optional
        Current request, retained for call-site compatibility.

    Returns
    -------
    list[Any]
        Selected read-only tool schemas, or the eligible scoped fallback set.
    """
    eligible_profiles = _read_only_profiles_for_scope(
        selected_worker=selected_worker
    )
    selected_names = set(suggested_names)
    selected = [
        profile.tool
        for profile in eligible_profiles
        if profile.tool.name in selected_names
    ]
    return selected or [
        profile.tool for profile in eligible_profiles
    ]


def read_only_tool_catalog(
    request: str, *, selected_worker: bool = True
) -> list[dict[str, str]]:
    """Return compact tool capabilities and complete summaries, relevant first.

    Each entry contains the complete opening summary paragraph from the tool
    description, avoiding arbitrary mid-sentence truncation while keeping
    parameter details and full schemas for evidence gathering. Request wording
    ranks capabilities within the current worker scope. Tools requiring a
    selected PID are hidden when the run has no selected worker; the planner's
    tool names later determine which eligible full schemas are bound for
    evidence gathering.

    Parameters
    ----------
    request : str
        Current user request used to rank capability terms.
    selected_worker : bool, default=True
        Whether tools requiring a selected worker may be included.

    Returns
    -------
    list[dict[str, str]]
        Ordered catalog entries with tool name, capability, and description.
    """
    normalized = " ".join(request.casefold().split())
    eligible_profiles = _read_only_profiles_for_scope(
        selected_worker=selected_worker
    )
    relevant = [
        profile
        for profile in eligible_profiles
        if any(term in normalized for term in profile.terms)
    ]
    relevant_names = {profile.tool.name for profile in relevant}
    ordered_profiles = relevant + [
        profile
        for profile in eligible_profiles
        if profile.tool.name not in relevant_names
    ]
    return [
        {
            "name": profile.tool.name,
            "capability": profile.capability,
            "description": (
                (profile.tool.description or "")
                .partition("\n\n")[0]
                .strip()
            ),
        }
        for profile in ordered_profiles
    ]


@origintracer_tool
async def record_deployment_marker_action(
    label: str = "deployment",
    worker_pid: str = "",
    worker_instance_id: str = "",
) -> dict[str, Any]:
    """Record a deployment marker after caller approval.

    Before writing, check the selected process instance's stored graph-diff history for the
    same marker. If it already exists, return a successful no-op result; otherwise write
    through FastAPI, scoped to the exact stored instance rather than a live worker socket.

    Parameters
    ----------
    label : str, default="deployment"
        Deployment label to check for duplicates and record.
    worker_pid : str
        Selected PID label from trusted graph state.
    worker_instance_id : str
        Stable stored identity of the selected process.

    Returns
    -------
    dict[str, Any]
        Backend write result, or a successful no-op result if the label already exists.

    Raises
    ------
    ToolException
        If the marker cannot be checked safely or the selected process instance is
        unavailable.
    """
    pid = validate_pid(worker_pid)
    try:
        existing = await query_worker(
            pid, build_diff_query(label), worker_instance_id
        )
    except ToolException as exc:
        missing_marker_detail = (
            "HTTP 404: No deployment marker or graph diff found for "
            f"'{label}'"
        )
        if not str(exc).endswith(missing_marker_detail):
            raise ToolException(
                "Could not verify whether this deployment marker already "
                "exists; no backend marker was written."
            ) from exc
        if not worker_instance_id:
            raise ToolException(
                "The selected worker instance is unavailable for marker recording."
            ) from exc
        return await record_deployment_marker(
            label, worker_instance_id, pid
        )
    query_result = existing.get("result")
    if not isinstance(query_result, dict):
        raise ToolException(
            "Could not verify whether this deployment marker already exists; "
            "no backend marker was written."
        )

    error = query_result.get("error")
    if error:
        error_text = str(error).casefold()
        if not any(
            phrase in error_text
            for phrase in (
                "no marker found",
                "no deployment marker or graph diff found",
            )
        ):
            raise ToolException(
                "Could not verify whether this deployment marker already "
                "exists; no backend marker was written."
            )
        if not worker_instance_id:
            raise ToolException(
                "The selected worker instance is unavailable for marker recording."
            )
        return await record_deployment_marker(
            label, worker_instance_id, pid
        )

    diff = query_result.get("data")
    if (
        isinstance(diff, dict)
        and diff.get("since_label") == label
    ):
        return {
            "ok": True,
            "recorded": False,
            "already_exists": True,
            "label": label,
            "worker_pid": str(pid),
            "message": "The selected worker already has this deployment marker; no duplicate was recorded.",
        }

    raise ToolException(
        "Could not verify whether this deployment marker already exists; "
        "no backend marker was written."
    )


@origintracer_tool
async def evaluate_and_record_rules_action(
    tags: list[str] | None = None,
    worker_instance_id: str = "",
) -> dict[str, Any]:
    """Evaluate rules on the selected worker's saved snapshot and record matches after
    approval.

    The selected process instance is explicit, so another worker's snapshot is never
    substituted. This action writes its matches to backend causal history.

    Parameters
    ----------
    tags : list[str], optional
        Rule categories to evaluate. Omit this argument to evaluate all registered rules.
    worker_instance_id : str
        Stable identity of the selected process instance.

    Returns
    -------
    dict[str, Any]
        Rule matches recorded in backend causal history.

    Raises
    ------
    ToolException
        If the selected worker instance is unavailable.
    """
    if not worker_instance_id:
        raise ToolException(
            "The selected worker instance is unavailable for rule evaluation."
        )
    return await evaluate_and_record_rules(
        tags, worker_instance_id
    )


ACTION_TOOLS = {
    "record_deployment_marker": record_deployment_marker_action,
    "evaluate_and_record_rules": evaluate_and_record_rules_action,
}

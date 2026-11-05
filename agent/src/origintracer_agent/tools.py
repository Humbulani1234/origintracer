from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Annotated, Any

from langchain.tools import ToolRuntime, tool
from langchain_core.tools import ToolException
from pydantic import Field

from origintracer_agent.backend import (
    build_causal_query,
    build_diff_query,
    build_event_query,
    build_trace_query,
    evaluate_and_record_rules,
    get_causal_history,
    list_workers,
    query_worker,
    record_deployment_marker,
    stitch_stored_trace,
    validate_pid,
)
from origintracer_agent.backend import (
    get_worker_snapshot as fetch_worker_snapshot,
)


def _pid(runtime: ToolRuntime[Any]) -> int:
    """Read the PID label for the selected stored process snapshot."""
    return validate_pid(runtime.state.get("worker_pid"))


def _instance_id(runtime: ToolRuntime[Any]) -> str | None:
    """Return the selected stable process-instance key, when resolved."""
    value = runtime.state.get("worker_instance_id")
    return str(value) if value else None


@tool
async def list_live_workers() -> dict[str, Any]:
    """List process instances with fresh runtime heartbeats from FastAPI.

    Use this when no worker is selected or when checking which workers are
    currently reporting. Returns only reporting process PIDs and instance IDs;
    stale stored snapshots are excluded and no local worker sockets are read.
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


@tool
async def get_live_workers_overview(
    max_workers: int = 10,
) -> dict[str, Any]:
    """Read the stored overview for each recently reporting worker.

    This read-only overview needs no caller-selected PID. It inspects only
    process instances with fresh uploader heartbeats in the authenticated
    snapshot inventory, up to max_workers (1–20), and reports partial failures
    per worker/section.
    Graphs include at most 20 nodes and 20 edges per worker.
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


@tool
async def get_worker_status(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Read stored heartbeat and graph status for the selected process.

    Returns uploaded uptime, active requests, active probes, graph counts, and
    heartbeat freshness. A stale heartbeat is reported as offline; this tool
    does not probe the process directly.
    """
    return await query_worker(
        _pid(runtime), "SHOW status", _instance_id(runtime)
    )


@tool
async def get_worker_snapshot(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Read the saved graph for exactly the selected worker process instance.

    Use this for questions about uploaded snapshot data. The tool requires the
    instance ID resolved from the selected stored snapshot and never chooses the
    customer's newest snapshot implicitly.
    """
    instance_id = runtime.state.get("worker_instance_id")
    if not instance_id:
        raise ToolException(
            "The selected worker has no registered snapshot identity."
        )
    return await fetch_worker_snapshot(str(instance_id))


@tool
async def get_worker_nodes(
    runtime: ToolRuntime[Any], limit: int = 50
) -> dict[str, Any]:
    """List observed operation nodes from the selected stored snapshot.

    Rows include operation name, service, call count, and aggregate duration.
    Use to check whether an operation was observed or inspect the graph
    inventory. Results are capped at 100 and are not latency-ranked.
    """
    return await query_worker(
        _pid(runtime),
        f"SHOW nodes LIMIT {max(1, min(limit, 100))}",
        _instance_id(runtime),
    )


@tool
async def get_worker_edges(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Show observed caller-to-callee relationships in the worker graph.

    Edges include direction and call count. Use to see how work flows between
    operations or relate a suspected hotspot to its caller; edges have no
    per-edge latency measurement.
    """
    return await query_worker(
        _pid(runtime), "SHOW edges", _instance_id(runtime)
    )


@tool
async def get_worker_graph(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Read the selected worker's combined node and edge graph.

    Use for a direct request to show or summarize the graph. Nodes describe
    observed operations and edges describe observed relationships; this
    structural view does not provide per-edge latency.
    """
    return await query_worker(
        _pid(runtime), "SHOW graph", _instance_id(runtime)
    )


@tool
async def get_worker_hotspots(
    runtime: ToolRuntime[Any], top: int = 10
) -> dict[str, Any]:
    """Rank the selected worker's graph nodes by call count.

    Use to find frequently executed operations. This is not a latency ranking:
    a frequent node may be fast, and a slow database query may not appear near
    the top. Use get_worker_database_hotspots for database durations.
    """
    return await query_worker(
        _pid(runtime),
        f"HOTSPOT TOP {max(1, min(top, 100))}",
        _instance_id(runtime),
    )


@tool
async def get_worker_database_hotspots(
    runtime: ToolRuntime[Any], limit: int = 20
) -> dict[str, Any]:
    """Rank observed database query nodes by average duration.

    Returns query name, call count, average duration, and total duration for
    nodes in the worker's configured `database` semantic group. Use to check
    whether database work is slow; unlike get_worker_hotspots, this is
    duration-ranked. Empty results can mean the group is unconfigured or no
    database query nodes were observed, so check probes and workload too.
    """
    bounded_limit = max(1, min(limit, 100))
    return await query_worker(
        _pid(runtime),
        f'SHOW latency WHERE system = "database" LIMIT {bounded_limit}',
        _instance_id(runtime),
    )


@tool
async def get_worker_database_events(
    runtime: ToolRuntime[Any],
    limit: int = 50,
    trace_id: str | None = None,
) -> dict[str, Any]:
    """List captured Django SQL query events, optionally narrowed to one trace.

    Each event identifies the SQL operation and trace ID. Use to confirm which
    queries ran and find traces for get_worker_trace. Event rows do not include
    query duration; use get_worker_database_hotspots for aggregate timings.
    Results are limited to the latest 100 matching events.
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


@tool
async def get_worker_latency_diagnostics(
    runtime: ToolRuntime[Any], limit: int = 5
) -> dict[str, Any]:
    """Gather the minimum evidence for a database-vs-event-loop comparison.

    Includes duration-ranked database nodes, Django request-duration nodes,
    SQL query events, asyncio node durations and events, and live causal-rule
    matches. Use this to compare database work with event-loop starvation.
    Event rows have timestamps, not per-event durations; only correlate rows
    with the same trace ID. Tick activity alone cannot prove or rule out
    starvation. Missing request nodes or database rows are limitations, not
    evidence that the work was fast or absent.
    """
    pid = _pid(runtime)
    bounded_limit = max(1, min(limit, 10))
    database_latency_query = (
        'SHOW latency WHERE system = "database" LIMIT '
        f"{bounded_limit}"
    )
    database_events_query = build_event_query(
        bounded_limit,
        service="django",
        probe="django.db.query",
    )
    # The Django semantic group includes its asyncio nodes. Query that group,
    # then keep only asyncio rows because the DSL's service filter resolves
    # semantic labels rather than arbitrary service names.
    asyncio_latency_query = (
        f'SHOW latency WHERE system = "django" LIMIT '
        f"{max(bounded_limit, 50)}"
    )
    # The query DSL resolves service filters as semantic labels. Fetch a
    # bounded sample without that filter, then select asyncio rows locally.
    asyncio_events_query = build_event_query(100)
    (
        database_latency,
        database_events,
        asyncio_latency,
        asyncio_events,
        causal_rules,
        registered_rules,
    ) = await asyncio.gather(
        query_worker(
            pid, database_latency_query, _instance_id(runtime)
        ),
        query_worker(
            pid, database_events_query, _instance_id(runtime)
        ),
        query_worker(
            pid, asyncio_latency_query, _instance_id(runtime)
        ),
        query_worker(
            pid, asyncio_events_query, _instance_id(runtime)
        ),
        query_worker(
            pid, build_causal_query(None), _instance_id(runtime)
        ),
        query_worker(pid, "SHOW rules", _instance_id(runtime)),
    )
    asyncio_latency_result = asyncio_latency.get(
        "result", asyncio_latency
    )
    if isinstance(asyncio_latency_result, dict) and isinstance(
        asyncio_latency_result.get("data"), list
    ):
        asyncio_latency_result = {
            **asyncio_latency_result,
            "data": [
                row
                for row in asyncio_latency_result["data"]
                if isinstance(row, dict)
                and row.get("service") == "asyncio"
            ][:bounded_limit],
        }
    asyncio_events_result = asyncio_events.get(
        "result", asyncio_events
    )
    request_latency_result = asyncio_latency.get(
        "result", asyncio_latency
    )
    if isinstance(request_latency_result, dict) and isinstance(
        request_latency_result.get("data"), list
    ):
        request_latency_result = {
            **request_latency_result,
            "data": [
                row
                for row in request_latency_result["data"]
                if isinstance(row, dict)
                and row.get("service") == "django"
                and str(row.get("node", ""))
                .split("::", 1)[-1]
                .startswith("/")
            ],
        }
    request_events_result = asyncio_events.get(
        "result", asyncio_events
    )
    if isinstance(request_events_result, dict) and isinstance(
        request_events_result.get("data"), list
    ):
        request_events_result = {
            **request_events_result,
            "data": [
                row
                for row in request_events_result["data"]
                if isinstance(row, dict)
                and row.get("service") == "django"
                and row.get("probe")
                in {
                    "request.entry",
                    "request.exit",
                }
            ][-bounded_limit:],
        }
    if isinstance(asyncio_events_result, dict) and isinstance(
        asyncio_events_result.get("data"), list
    ):
        asyncio_events_result = {
            **asyncio_events_result,
            "data": [
                row
                for row in asyncio_events_result["data"]
                if isinstance(row, dict)
                and row.get("service") == "asyncio"
            ][-bounded_limit:],
        }
    return {
        "source": "OriginTracer stored snapshot latency diagnostics",
        "worker_pid": str(pid),
        "query": "database and asyncio latency/events",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "result": {
            "database_latency": database_latency.get(
                "result", database_latency
            ),
            "database_events": database_events.get(
                "result", database_events
            ),
            "request_latency": request_latency_result,
            "request_events": request_events_result,
            "asyncio_latency": asyncio_latency_result,
            "asyncio_events": asyncio_events_result,
            "causal_rule_matches": causal_rules.get(
                "result", causal_rules
            ),
            "registered_causal_rules": registered_rules.get(
                "result", registered_rules
            ),
            "interpretation_limits": [
                (
                    "Latency rows report aggregate node durations; event rows "
                    "do not report per-event duration."
                ),
                (
                    "Only compare events with the same trace_id; timestamps "
                    "from different traces are unrelated."
                ),
                (
                    "Asyncio tick frequency alone does not prove or rule out "
                    "event-loop starvation."
                ),
            ],
        },
    }


@tool
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

    Optional filters narrow results by trace ID, service name, or exact event
    probe; limit is capped at 100. A graph node ID or operation name is not a
    probe. To look for an operation, query its service without a probe filter
    and inspect returned event names and trace IDs. The selected worker PID
    comes from trusted graph state and is never a filter argument. Events are
    retained in memory, so an empty result does not prove an event never
    occurred.
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


@tool
async def get_worker_trace(
    runtime: ToolRuntime[Any], trace_id: str
) -> dict[str, Any]:
    """Inspect the selected worker's critical path for one trace ID.

    Returns ordered stages and timing data for that request. Use a trace ID
    from get_worker_events or get_worker_database_events to connect query and
    event observations to one request. Only retained trace data is available.
    """
    query = build_trace_query(trace_id)
    return await query_worker(
        _pid(runtime), query, _instance_id(runtime)
    )


@tool
async def get_worker_stitched_trace(
    runtime: ToolRuntime[Any], trace_id: str
) -> dict[str, Any]:
    """Join one explicitly identified trace across stored process instances.

    Use only when the user asks to stitch or follow a trace across processes.
    The tool searches uploaded event rows for this exact trace ID; it does not
    guess trace IDs or inspect arbitrary queries. Each stage keeps its worker
    PID and recorded duration. No cross-process end-to-end time is computed.
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
                "duration_ms values are returned per-worker inter-event "
                "intervals; no cross-process duration is computed."
            ),
            (
                "A worker with no matching retained events is not evidence "
                "that the process was absent from the original request."
            ),
        ],
    }


@tool
async def get_worker_causal_history(
    runtime: ToolRuntime[Any], limit: int = 50
) -> dict[str, Any]:
    """Read saved causal-rule match history for the selected worker instance.

    This reads backend history associated with the selected process instance;
    it does not evaluate rules or save new history. Use get_worker_events for
    recent raw event history instead.
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


@tool
async def evaluate_worker_rules(
    runtime: ToolRuntime[Any], tags: list[str] | None = None
) -> dict[str, Any]:
    """Evaluate backend-registered causal rules on the selected stored graph.

    Use this read-only tool for rule-based evidence about Django database
    patterns (`n_plus_one_queries`, `db_query_hotspot`), asyncio problems
    (`asyncio_event_loop_starvation`, `new_sync_call_after_deployment`), or
    LangGraph behavior (`loop_runaway`, `agent_latency_hotspot`). The backend
    evaluates its registered rules against this uploaded snapshot. Per-process
    rule configuration is not part of the snapshot, so this cannot prove which
    rules the originating process loaded.

    Optional tags filter rule categories: `db`, `n+1`, `performance`,
    `asyncio`, `async`, `latency`, `blocking`, `deployment`, `langgraph`,
    `agent`, and `loop`. Omit tags to check every configured rule. A match is
    evidence that a rule's pattern fired, not proof of root cause. This query
    does not save matches to backend causal history. Prefer it over the broad
    latency-diagnostics tool when the user asks which rules are configured or
    matched without asking for a latency comparison.
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


@tool
async def get_worker_changes(
    runtime: ToolRuntime[Any], since: str = "deployment"
) -> dict[str, Any]:
    """Compare the current worker graph with a prior marker or time.

    Use to see nodes and edges added or removed since `since`, which defaults
    to the deployment marker. Structural changes alone do not explain latency.
    """
    return await query_worker(
        _pid(runtime),
        build_diff_query(since),
        _instance_id(runtime),
    )


@dataclass(frozen=True)
class ToolProfile:
    """Catalog and interpretation metadata for one read-only tool.

    ``terms`` ranks this tool in the planner's compact catalog. Worker scope
    controls availability: tools marked ``requires_selected_worker`` are only
    exposed when the graph has a selected worker PID.
    ``assessment_guidance`` records how to interpret its returned evidence;
    the graph adds it to the assessor prompt only when this tool was observed.
    ``research_guidance`` records argument-selection semantics for evidence
    gathering; the graph adds it when this tool is planned.
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
            "get_live_workers_overview lists recently reporting process "
            "instances from FastAPI snapshot storage and inspects each one. Do not "
            "pass a PID or make separate selected-worker reads for this scope."
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
        get_worker_latency_diagnostics,
        "latency_diagnostics",
        (
            "latency",
            "duration",
            "slow request",
            "compare bottlenecks",
            "event loop starvation",
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

    Keeping these semantics beside each tool profile makes the assessor
    guidance follow the evidence actually gathered without request keywords
    or tool-pair checks in the graph.
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
    """Return tool-specific read semantics for planned observations."""
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
    """Return compact tool capabilities and descriptions, relevant tools first.

    Each entry contains a bounded description so planning can match requested
    outputs to what a tool actually returns. Request wording ranks capabilities
    within the current worker scope. Tools requiring a selected PID are hidden
    when the run has no selected worker; the planner's tool names later
    determine which eligible full schemas are bound for evidence gathering.
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
            "description": (profile.tool.description or "")[
                :220
            ],
        }
        for profile in ordered_profiles
    ]


@tool
async def record_deployment_marker_action(
    label: str = "deployment",
    worker_pid: str = "",
    worker_instance_id: str = "",
) -> dict[str, Any]:
    """Record a deployment marker after the caller approves the action.

    First check the selected process instance's stored graph-diff history for
    the same marker. If it already exists, skip the backend write to avoid a
    duplicate. Otherwise write the marker through FastAPI, scoped to the exact
    stored instance rather than a live worker socket.

    Args:
        label: Deployment label to record and check for duplicates.
        worker_pid: Selected PID label from trusted graph state.
        worker_instance_id: Stable stored identity for the selected process.

    Returns:
        Backend write result, or a successful no-op result when the label
        already exists.

    Raises:
        ToolException: If the existing marker cannot be checked safely or the
            selected stored process instance is unavailable.
    """
    pid = validate_pid(worker_pid)
    existing = await query_worker(
        pid, build_diff_query(label), worker_instance_id
    )
    query_result = existing.get("result")
    if not isinstance(query_result, dict):
        raise ToolException(
            "Could not verify whether this deployment marker already exists; "
            "no backend marker was written."
        )

    error = query_result.get("error")
    if error:
        if "no marker found" not in str(error).casefold():
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


@tool
async def evaluate_and_record_rules_action(
    tags: list[str] | None = None,
    worker_instance_id: str = "",
) -> dict[str, Any]:
    """Evaluate rules on the selected worker's saved snapshot and save matches.

    Optional tags select rule categories. This writes matches to backend
    causal history after the caller approves the action. The selected process
    instance is explicit, so another worker's snapshot is never substituted.
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

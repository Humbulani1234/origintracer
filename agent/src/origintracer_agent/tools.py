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
    validate_pid,
)
from origintracer_agent.backend import (
    get_worker_snapshot as fetch_worker_snapshot,
)


def _pid(runtime: ToolRuntime[Any]) -> int:
    """Read the caller-selected PID from trusted graph input state."""
    return validate_pid(runtime.state.get("worker_pid"))


@tool
async def list_live_workers() -> dict[str, Any]:
    """Find live OriginTracer processes that can be inspected.

    Use this when no worker is selected or when checking whether a selected
    PID is available. Returns worker PIDs; it does not inspect their graphs.
    The caller must select a PID before worker queries can run.
    """
    return {
        "source": "OriginTracer backend worker inventory",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "result": await list_workers(),
    }


@tool
async def get_worker_status(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Check the selected worker's health and current activity.

    Returns process uptime, probe status, graph counts, and active-request
    count. Use for a direct health question or to confirm probes before
    interpreting missing telemetry; it does not find slow operations.
    """
    return await query_worker(_pid(runtime), "SHOW status")


@tool
async def get_worker_snapshot(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Read the saved graph for exactly the selected worker process instance.

    Use this for questions about uploaded snapshot data. The tool requires the
    instance ID resolved from the selected live worker and never chooses the
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
    """List observed operation nodes from the selected worker's live graph.

    Rows include operation name, service, call count, and aggregate duration.
    Use to check whether an operation was observed or inspect the graph
    inventory. Results are capped at 100 and are not latency-ranked.
    """
    return await query_worker(
        _pid(runtime),
        f"SHOW nodes LIMIT {max(1, min(limit, 100))}",
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
    return await query_worker(_pid(runtime), "SHOW edges")


@tool
async def get_worker_graph(
    runtime: ToolRuntime[Any],
) -> dict[str, Any]:
    """Read the selected worker's combined node and edge graph.

    Use for a direct request to show or summarize the graph. Nodes describe
    observed operations and edges describe observed relationships; this
    structural view does not provide per-edge latency.
    """
    return await query_worker(_pid(runtime), "SHOW graph")


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
        _pid(runtime), f"HOTSPOT TOP {max(1, min(top, 100))}"
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
    return await query_worker(_pid(runtime), query)


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
        query_worker(pid, database_latency_query),
        query_worker(pid, database_events_query),
        query_worker(pid, asyncio_latency_query),
        query_worker(pid, asyncio_events_query),
        query_worker(pid, build_causal_query(None)),
        query_worker(pid, "SHOW rules"),
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
        "source": "OriginTracer live worker latency diagnostics",
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
                "Optional exact event probe filter. Leave empty to include "
                "all probes."
            )
        ),
    ] = "",
) -> dict[str, Any]:
    """Read recent events captured by the selected worker.

    Optional filters narrow results by trace ID, service name, or probe;
    limit is capped at 100. The selected worker PID comes from trusted graph
    state and is never a filter argument. Use for event-level evidence such as
    loop ticks or request boundaries. Events are retained in memory, so an
    empty result does not prove an event never occurred.
    """
    query = build_event_query(
        limit,
        trace_id or None,
        service_name or None,
        probe or None,
    )
    return await query_worker(_pid(runtime), query)


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
    return await query_worker(_pid(runtime), query)


@tool
async def get_worker_stitched_trace(
    runtime: ToolRuntime[Any], trace_id: str
) -> dict[str, Any]:
    """Join one explicitly identified trace across live worker processes.

    Use only when the user asks to stitch or follow a trace across processes.
    The tool searches the backend's current live-worker inventory for this
    exact trace ID; it does not guess trace IDs or inspect arbitrary queries.
    Each stage keeps its worker PID and TRACE duration_ms. Those durations are
    per-worker inter-event intervals, not cross-process span durations.
    """
    query = build_trace_query(trace_id)
    selected_pid = _pid(runtime)
    workers = await list_workers()
    worker_by_pid: dict[str, dict[str, Any]] = {
        str(worker["pid"]): worker
        for worker in workers
        if str(worker.get("pid", "")).isdigit()
    }
    worker_by_pid.setdefault(
        str(selected_pid),
        {
            "pid": str(selected_pid),
            "worker_instance_id": runtime.state.get(
                "worker_instance_id"
            ),
        },
    )
    ordered_workers = sorted(
        worker_by_pid.values(),
        key=lambda worker: int(worker["pid"]),
    )
    responses = await asyncio.gather(
        *(
            query_worker(int(worker["pid"]), query)
            for worker in ordered_workers
        ),
        return_exceptions=True,
    )

    stages: list[dict[str, Any]] = []
    workers_found: list[str] = []
    errors: list[dict[str, str]] = []
    for worker, response in zip(ordered_workers, responses):
        pid = str(worker["pid"])
        if isinstance(response, BaseException):
            errors.append(
                {"worker_pid": pid, "error": str(response)}
            )
            continue
        result = response.get("result", response)
        if isinstance(result, dict) and result.get("error"):
            errors.append(
                {
                    "worker_pid": pid,
                    "error": str(result["error"]),
                }
            )
            continue
        if isinstance(result, dict) and result.get("truncated"):
            errors.append(
                {
                    "worker_pid": pid,
                    "error": "The worker's TRACE response was truncated.",
                }
            )
            continue
        worker_stages = (
            result.get("data", [])
            if isinstance(result, dict)
            else []
        )
        if (
            not isinstance(worker_stages, list)
            or not worker_stages
        ):
            continue
        workers_found.append(pid)
        for stage in worker_stages:
            if not isinstance(stage, dict):
                continue
            stages.append(
                {
                    **stage,
                    "worker_pid": pid,
                    "worker_instance_id": worker.get(
                        "worker_instance_id"
                    ),
                }
            )

    def stage_sort_key(
        stage: dict[str, Any],
    ) -> tuple[bool, float, str]:
        timestamp = stage.get("timestamp")
        if isinstance(timestamp, (int, float)):
            sort_timestamp = float(timestamp)
            missing_timestamp = False
        elif isinstance(timestamp, str):
            try:
                sort_timestamp = float(timestamp)
                missing_timestamp = False
            except ValueError:
                sort_timestamp = 0.0
                missing_timestamp = True
        else:
            sort_timestamp = 0.0
            missing_timestamp = True
        return (
            missing_timestamp,
            sort_timestamp,
            stage["worker_pid"],
        )

    stages.sort(key=stage_sort_key)
    return {
        "source": "OriginTracer stitched live worker trace",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "trace_id": trace_id,
        "selected_worker_pid": str(selected_pid),
        "workers_searched": [
            str(worker["pid"]) for worker in ordered_workers
        ],
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
    """Evaluate configured causal rules on the selected worker's live graph.

    Use this read-only tool for rule-based evidence about Django database
    patterns (`n_plus_one_queries`, `db_query_hotspot`), asyncio problems
    (`asyncio_event_loop_starvation`, `new_sync_call_after_deployment`), or
    LangGraph behavior (`loop_runaway`, `agent_latency_hotspot`). The worker
    must have loaded the corresponding rules in its OriginTracer config.

    Optional tags filter rule categories: `db`, `n+1`, `performance`,
    `asyncio`, `async`, `latency`, `blocking`, `deployment`, `langgraph`,
    `agent`, and `loop`. Omit tags to check every configured rule. A match is
    evidence that a rule's pattern fired, not proof of root cause. This query
    does not save matches to backend causal history. Prefer it over the broad
    latency-diagnostics tool when the user asks which rules are configured or
    matched without asking for a latency comparison.
    """
    pid = _pid(runtime)
    matches, registered = await asyncio.gather(
        query_worker(pid, build_causal_query(tags)),
        query_worker(pid, "SHOW rules"),
    )
    return {
        "source": "OriginTracer live worker causal-rule review",
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
        _pid(runtime), build_diff_query(since)
    )


@dataclass(frozen=True)
class ToolProfile:
    """Discovery metadata used to expose only relevant tools to the LLM."""

    tool: Any
    capability: str
    terms: tuple[str, ...]


READ_ONLY_TOOL_PROFILES = (
    ToolProfile(
        list_live_workers,
        "worker_inventory",
        (
            "live workers",
            "worker list",
            "processes",
            "pids",
            "available worker",
        ),
    ),
    ToolProfile(
        get_worker_status,
        "worker_status",
        (
            "status",
            "health",
            "uptime",
            "active requests",
            "probe status",
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
    ),
    ToolProfile(
        get_worker_database_hotspots,
        "database_latency",
        (
            "database latency",
            "sql latency",
            "slow query",
            "query duration",
            "database hotspot",
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
            "configured rules",
            "evaluate rules",
            "pattern rules",
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


def select_read_only_tools(
    request: str,
    suggested_names: list[str] | None = None,
) -> list[Any]:
    """Select tools using request relevance and plan suggestions.

    Tool metadata is the single place to extend discovery as capabilities are
    added. If a request is too broad to match safely, expose the full catalog
    rather than risk making a needed read unavailable.
    """
    normalized = " ".join(request.casefold().split())
    selected_names = set(suggested_names or [])
    selected_names.update(
        profile.tool.name
        for profile in READ_ONLY_TOOL_PROFILES
        if any(term in normalized for term in profile.terms)
    )
    if not selected_names:
        return READ_ONLY_TOOLS
    selected = [
        profile.tool
        for profile in READ_ONLY_TOOL_PROFILES
        if profile.tool.name in selected_names
    ]
    return selected or READ_ONLY_TOOLS


def read_only_tool_catalog(request: str) -> list[dict[str, str]]:
    """Return a compact, request-relevant catalog for the planning model."""
    selected_names = {
        tool.name for tool in select_read_only_tools(request)
    }
    return [
        {
            "name": profile.tool.name,
            "capability": profile.capability,
        }
        for profile in READ_ONLY_TOOL_PROFILES
        if profile.tool.name in selected_names
    ]


@tool
async def record_deployment_marker_action(
    label: str = "deployment",
    worker_pid: str = "",
    worker_instance_id: str = "",
) -> dict[str, Any]:
    """Record a deployment marker after the caller approves the action.

    First check the selected worker's live temporal history for the same
    marker. If it already exists, skip the backend write to avoid recording
    the startup marker twice. Otherwise write the marker to backend history.
    """
    pid = validate_pid(worker_pid)
    existing = await query_worker(pid, build_diff_query(label))
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

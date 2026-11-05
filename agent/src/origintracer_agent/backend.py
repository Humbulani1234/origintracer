"""HTTP client for stored OriginTracer data and approved backend actions."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

import httpx
from langchain_core.tools import ToolException

from origintracer_agent.config import (
    backend_api_key,
    backend_url,
)

# Bound each agent-to-backend request so a stalled API cannot block a graph run.
_REQUEST_TIMEOUT_S = 12.0
# Keep serialized evidence within the model tool-result budget.
_MAX_TOOL_RESULT_CHARS = 24_000
_SAFE_FILTER = re.compile(r"^[A-Za-z0-9_.:/-]{1,200}$")
_SAFE_TAG = re.compile(r"^[A-Za-z0-9_.:/+-]{1,200}$")


def _headers() -> dict[str, str]:
    """Build backend authentication headers without exposing the key."""
    key = backend_api_key()
    if not key:
        raise ToolException(
            "ORIGINTRACER_API_KEY is not configured for the agent service."
        )
    return {"Authorization": f"Bearer {key}"}


async def _request(
    method: str, path: str, **kwargs: Any
) -> dict[str, Any]:
    """Call one fixed backend endpoint and return its JSON object."""
    try:
        async with httpx.AsyncClient(
            base_url=backend_url(),
            headers=_headers(),
            timeout=_REQUEST_TIMEOUT_S,
        ) as client:
            response = await client.request(
                method, path, **kwargs
            )
    except ToolException:
        raise
    except httpx.TimeoutException as exc:
        raise ToolException(
            "The OriginTracer backend request timed out."
        ) from exc
    except httpx.HTTPError as exc:
        raise ToolException(
            f"Could not reach the OriginTracer backend: {exc}."
        ) from exc

    if response.is_error:
        try:
            detail = response.json().get(
                "detail", response.reason_phrase
            )
        except (ValueError, AttributeError):
            detail = response.reason_phrase
        raise ToolException(
            f"OriginTracer backend returned HTTP {response.status_code}: {detail}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise ToolException(
            "The OriginTracer backend returned invalid JSON."
        ) from exc
    if not isinstance(payload, dict):
        raise ToolException(
            "The OriginTracer backend returned an unexpected response."
        )
    return payload


async def list_workers() -> list[dict[str, Any]]:
    """List stored process snapshots and their heartbeat freshness.

    This inventory uses FastAPI's snapshot route rather than discovering
    worker sockets. Each valid row is normalized to a PID label, stable
    process-instance ID, snapshot receipt time, heartbeat last-seen time, and
    the backend's freshness decision. Stale snapshots are retained in the
    returned inventory so callers can still inspect their stored data.

    Returns:
        Snapshot rows for the authenticated customer, newest first.

    Raises:
        ToolException: If the backend request fails or returns an invalid list.
    """
    payload = await _request("GET", "/api/v1/snapshots")
    workers = payload.get("data", [])
    if not isinstance(workers, list):
        raise ToolException(
            "The backend returned an invalid worker list."
        )
    return [
        {
            "pid": str(
                worker.get("worker_pid", worker.get("pid", ""))
            ),
            "worker_instance_id": str(
                worker["worker_instance_id"]
            ),
            "received_at": str(worker.get("received_at", "")),
            "last_seen": str(worker.get("last_seen", "")),
            "online": worker.get("online") is True,
        }
        for worker in workers
        if isinstance(worker, dict)
        and str(
            worker.get("worker_pid", worker.get("pid", ""))
        ).isdigit()
        and worker.get("worker_instance_id")
    ]


async def record_deployment_marker(
    label: str, worker_instance_id: str, worker_pid: int
) -> dict[str, Any]:
    """Record an approved deployment marker in the backend repository."""
    if not label or len(label) > 200:
        raise ToolException(
            "The deployment marker label must be 1 to 200 characters."
        )
    return await _request(
        "POST",
        "/api/v1/deployment",
        json={"label": label},
        headers={
            "X-OriginTracer-Worker-Instance-ID": worker_instance_id,
            "X-OriginTracer-Worker-PID": str(worker_pid),
        },
    )


async def evaluate_and_record_rules(
    tags: list[str] | None = None,
    worker_instance_id: str = "",
) -> dict[str, Any]:
    """Run rules on the selected worker instance snapshot and store its matches."""
    if tags is not None and (
        len(tags) > 20 or any(not tag for tag in tags)
    ):
        raise ToolException(
            "Provide up to 20 non-empty rule tags."
        )
    return await _request(
        "POST",
        "/api/v1/causal/evaluate",
        json={
            "tags": tags or [],
            "worker_instance_id": worker_instance_id,
        },
    )


async def get_worker_snapshot(
    worker_instance_id: str,
) -> dict[str, Any]:
    """Read the saved graph snapshot for one selected worker process instance."""
    return await _request(
        "GET", f"/api/v1/snapshots/{worker_instance_id}"
    )


async def get_causal_history(
    worker_instance_id: str, limit: int = 50
) -> dict[str, Any]:
    """Read saved causal-rule history for one worker process instance."""
    if not worker_instance_id or len(worker_instance_id) > 200:
        raise ToolException(
            "A valid worker instance ID is required."
        )
    return await _request(
        "GET",
        "/api/v1/causal/history",
        params={
            "worker_instance_id": worker_instance_id,
            "limit": max(1, min(limit, 100)),
        },
    )


async def stitch_stored_trace(trace_id: str) -> dict[str, Any]:
    """Retrieve one trace assembled from uploaded events across instances.

    The backend searches persisted event rows for this exact trace ID and
    returns stages with their originating worker identities. This read does
    not contact live workers and does not calculate a cross-process end-to-end
    duration.

    Args:
        trace_id: Trace identifier observed in previously captured events.

    Returns:
        Backend trace details, including matching stages and worker IDs.

    Raises:
        ToolException: If the backend request fails or the trace ID is rejected.
    """
    return await _request(
        "GET", f"/api/v1/traces/{trace_id}/stitch"
    )


def validate_pid(value: Any) -> int:
    """Check the selected PID before constructing a worker API request."""
    if isinstance(value, bool) or not str(value or "").isdigit():
        raise ToolException(
            "No valid worker is selected. Ask the caller to choose a live PID "
            "and provide it as worker_pid in the run input."
        )
    pid = int(value)
    if pid < 1:
        raise ToolException(
            "worker_pid must be a positive process ID."
        )
    return pid


def _safe_filter(value: str, field: str) -> str:
    """Validate a simple DSL filter value before quoting it."""
    pattern = _SAFE_TAG if field == "tag" else _SAFE_FILTER
    if not pattern.fullmatch(value):
        raise ToolException(
            f"{field} must contain only letters, numbers, dots, colons, "
            "slashes, underscores, or hyphens."
        )
    return f'"{value}"'


async def _snapshot_for_pid(pid: int) -> dict[str, Any]:
    """Resolve a PID label to its newest uploaded process instance.

    PID is not a durable identity because the operating system can reuse it.
    Agent reads therefore resolve it through the stored snapshot inventory and
    use the returned instance ID for the actual request. The inventory is
    ordered newest first by the backend.

    Args:
        pid: Selected positive process ID supplied in trusted run state.

    Returns:
        The matching snapshot inventory row, including its instance ID.

    Raises:
        ToolException: If no uploaded snapshot currently matches the PID.
    """
    for worker in await list_workers():
        if str(worker.get("pid")) == str(pid):
            return worker
    raise ToolException(
        f"No uploaded snapshot is available for worker PID {pid}. "
        "The process may not have uploaded its first snapshot yet."
    )


async def query_worker(
    pid: int,
    query: str,
    worker_instance_id: str | None = None,
) -> dict[str, Any]:
    """Run a supported read against FastAPI's uploaded snapshot/event APIs.

    The historical name is retained for tool call compatibility. Unless an
    instance ID is supplied, the PID is resolved through the stored snapshot
    inventory; all subsequent reads are scoped by that stable instance ID.
    Supported DSL metrics map to FastAPI graph, event, trace, status, latency,
    causal-rule, and change routes. This function never calls the live-worker
    socket proxy and never falls back to it when a stored route is unsupported.
    Tool results are bounded before being returned to the model.

    Args:
        pid: Selected process ID used to resolve the stored instance.
        query: Supported read-only OriginTracer DSL query.
        worker_instance_id: Optional already-resolved stable instance ID.

    Returns:
        Query source, worker identity, observation time, and API result.

    Raises:
        ToolException: If the PID has no snapshot, the DSL metric is
            unsupported, or the backend request fails.
    """
    from origintracer.query.parser import parse

    worker = (
        {
            "pid": str(pid),
            "worker_instance_id": worker_instance_id,
        }
        if worker_instance_id
        else await _snapshot_for_pid(pid)
    )
    instance_id = str(worker["worker_instance_id"])
    parsed = parse(query)
    metric = parsed.metric
    filters = parsed.filters
    limit = max(1, min(int(parsed.limit or 100), 100))

    if parsed.verb == "TRACE" or metric == "path":
        trace_id = filters.get("trace_id")
        payload = await _request(
            "GET",
            f"/api/v1/traces/{trace_id}",
            params={"worker_instance_id": instance_id},
        )
        result = {
            "metric": "trace",
            "data": payload.get("data", []),
        }
    elif parsed.verb == "CAUSAL":
        payload = await _request(
            "GET",
            "/api/v1/causal",
            params={
                "worker_instance_id": instance_id,
                **(
                    {"tags": filters["tags"]}
                    if filters.get("tags")
                    else {}
                ),
            },
        )
        result = payload
    elif parsed.verb == "HOTSPOT":
        payload = await _request(
            "GET",
            "/api/v1/hotspots",
            params={
                "worker_instance_id": instance_id,
                "top": limit,
            },
        )
        result = {
            "verb": "HOTSPOT",
            "data": payload.get("data", []),
        }
    elif parsed.verb == "DIFF" or metric == "changes":
        payload = await _request(
            "GET",
            "/api/v1/graph/diff",
            params={
                "worker_instance_id": instance_id,
                **(
                    {"since": filters["since"]}
                    if filters.get("since")
                    else {}
                ),
            },
        )
        result = payload
    elif metric in {"nodes", "graph", "edges"}:
        if metric == "graph":
            payload = await _request(
                "GET",
                "/api/v1/graph",
                params={"worker_instance_id": instance_id},
            )
            result = payload.get("data", payload)
        elif metric == "nodes":
            params = {
                "worker_instance_id": instance_id,
                "limit": limit,
            }
            if filters.get("service"):
                params["service"] = filters["service"]
            payload = await _request(
                "GET", "/api/v1/nodes", params=params
            )
            result = payload.get("data", {}).get("data", [])
            result = {"metric": "nodes", "data": result}
        else:
            payload = await _request(
                "GET",
                "/api/v1/edges",
                params={"worker_instance_id": instance_id},
            )
            result = payload.get("data", {}).get("data", [])
            result = {"metric": "edges", "data": result}
    elif metric == "events":
        params = {
            "worker_instance_id": instance_id,
            "limit": limit,
        }
        for field in ("trace_id", "service", "probe"):
            if filters.get(field):
                params[field] = filters[field]
        payload = await _request(
            "GET", "/api/v1/events", params=params
        )
        result = payload
    elif metric in {"llm_content", "tool_content"}:
        kind = "llm" if metric == "llm_content" else "tool"
        payload = await _request(
            "GET",
            "/api/v1/events/content",
            params={
                "worker_instance_id": instance_id,
                "kind": kind,
                "limit": limit,
            },
        )
        result = payload
    elif metric == "status":
        payload = await _request(
            "GET",
            "/api/v1/status",
            params={"worker_instance_id": instance_id},
        )
        snapshot = payload.get("snapshot", {})
        runtime = snapshot.get("runtime_status") or {}
        result = {
            "metric": "status",
            "source": "uploaded runtime heartbeat and graph snapshot",
            "pid": worker.get("pid"),
            "worker_instance_id": instance_id,
            "online": snapshot.get("online", False),
            "last_seen": snapshot.get("last_seen"),
            "heartbeat_age_s": snapshot.get("heartbeat_age_s"),
            "snapshot": {
                "available": snapshot.get("available", False),
                "nodes": snapshot.get("nodes"),
                "edges": snapshot.get("edges"),
                "received_at": snapshot.get("received_at"),
            },
            "data": {
                "pid": worker.get("pid"),
                "worker_instance_id": instance_id,
                "online": snapshot.get("online", False),
                "uptime_s": runtime.get("uptime_s"),
                "uptime": (
                    f"{int(runtime['uptime_s'] // 3600)}h "
                    f"{int((runtime['uptime_s'] % 3600) // 60)}m"
                    if isinstance(
                        runtime.get("uptime_s"), (int, float)
                    )
                    else None
                ),
                "active_requests": runtime.get(
                    "active_requests"
                ),
                "graph_nodes": runtime.get(
                    "graph_nodes", snapshot.get("nodes")
                ),
                "graph_edges": runtime.get(
                    "graph_edges", snapshot.get("edges")
                ),
                "probes_active": runtime.get("probes_active"),
                "probes_total": runtime.get(
                    "probes_total",
                    len(runtime.get("probes_active", [])),
                ),
                "rules_active": runtime.get("rules_active"),
                "semantic_labels": runtime.get(
                    "semantic_labels"
                ),
                "event_log_size": runtime.get("event_log_size"),
                "last_seen": snapshot.get("last_seen"),
                "heartbeat_age_s": snapshot.get(
                    "heartbeat_age_s"
                ),
            },
            "storage": payload.get("storage"),
            "timestamp": payload.get("timestamp"),
        }
    elif metric == "latency":
        params = {
            "worker_instance_id": instance_id,
            "limit": limit,
        }
        if filters.get("service"):
            params["service"] = filters["service"]
        if filters.get("system"):
            params["system"] = filters["system"]
        payload = await _request(
            "GET", "/api/v1/latency", params=params
        )
        result = payload
    elif metric == "rules":
        payload = await _request(
            "GET",
            "/api/v1/status",
            params={"worker_instance_id": instance_id},
        )
        runtime = (
            payload.get("snapshot", {}).get("runtime_status")
            or {}
        )
        result = {
            "metric": "rules",
            "scope": "uploaded process runtime heartbeat",
            "data": runtime.get("rules_active", []),
            "online": payload.get("snapshot", {}).get(
                "online", False
            ),
        }
    else:
        raise ToolException(
            f"The stored-data API does not support '{query}' yet. "
            "This agent read path does not fall back to worker sockets."
        )

    if isinstance(result, dict) and result.get("ok") is False:
        raise ToolException(
            str(result.get("error", "Stored query failed."))
        )
    encoded = json.dumps(result, ensure_ascii=False, default=str)
    if len(encoded) > _MAX_TOOL_RESULT_CHARS:
        result = {
            "truncated": True,
            "notice": (
                "The worker result was shortened to fit the tool response. "
                "Use a narrower query or lower limit for more detail."
            ),
            "preview": encoded[:_MAX_TOOL_RESULT_CHARS],
        }
    return {
        "source": "OriginTracer stored snapshot/event query",
        "worker_pid": str(pid),
        "query": query,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "result": result,
    }


def build_event_query(
    limit: int,
    trace_id: str | None = None,
    service: str | None = None,
    probe: str | None = None,
) -> str:
    """Build a bounded event query from validated filter values."""
    limit = max(1, min(limit, 100))
    filters = []
    for field, value in (
        ("trace_id", trace_id),
        ("service", service),
        ("probe", probe),
    ):
        if value:
            filters.append(
                f"{field} = {_safe_filter(value, field)}"
            )
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    return f"SHOW events{where} LIMIT {limit}"


def build_causal_query(tags: list[str] | None) -> str:
    """
    Build a causal rules query with optional tag filtering.
    """
    if not tags:
        return "CAUSAL"
    safe_tags = [
        _safe_filter(tag, "tag")[1:-1] for tag in tags[:20]
    ]
    return f'CAUSAL WHERE tags = "{",".join(safe_tags)}"'


def build_trace_query(trace_id: str) -> str:
    """Build a trace query from a validated trace identifier."""
    return f"TRACE {_safe_filter(trace_id, 'trace_id')[1:-1]}"


def build_diff_query(since: str) -> str:
    """Build a temporal diff query for a marker or timestamp."""
    return f"DIFF SINCE {_safe_filter(since, 'since')}"

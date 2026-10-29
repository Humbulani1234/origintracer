"""Small HTTP client for OriginTracer worker queries and approved actions."""

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

_REQUEST_TIMEOUT_S = 12.0
_MAX_TOOL_RESULT_CHARS = 24_000
_SAFE_FILTER = re.compile(r"^[A-Za-z0-9_.:/-]{1,200}$")


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


async def list_workers() -> list[dict[str, str]]:
    """List live workers with both their PID and stable process-instance ID."""
    payload = await _request("GET", "/api/v1/workers")
    workers = payload.get("data", [])
    if not isinstance(workers, list):
        raise ToolException(
            "The backend returned an invalid worker list."
        )
    return [
        {
            "pid": str(worker["pid"]),
            "worker_instance_id": str(
                worker["worker_instance_id"]
            ),
        }
        for worker in workers
        if isinstance(worker, dict)
        and str(worker.get("pid", "")).isdigit()
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
    if not _SAFE_FILTER.fullmatch(value):
        raise ToolException(
            f"{field} must contain only letters, numbers, dots, colons, "
            "slashes, underscores, or hyphens."
        )
    return f'"{value}"'


async def query_worker(pid: int, query: str) -> dict[str, Any]:
    """Send one read-only OriginTracer DSL query to a selected worker."""
    payload = await _request(
        "POST",
        f"/api/v1/workers/{pid}/query",
        json={"query": query},
    )
    if payload.get("ok") is False:
        raise ToolException(
            str(payload.get("error", "Worker query failed."))
        )
    result = payload.get("data", payload)
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
        "source": "OriginTracer live worker query",
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

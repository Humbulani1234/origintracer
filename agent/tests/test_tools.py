"""Tests for read-tool catalog ordering and schema selection."""

import pytest
from langchain_core.tools import ToolException

from origintracer_agent.tools import (
    READ_ONLY_TOOLS,
    read_only_tool_catalog,
    record_deployment_marker_action,
    select_planned_read_only_tools,
)


def test_catalog_exposes_all_capabilities_and_ranks_relevant_tools():
    request = (
        "Compare the most frequently called operations with the slowest "
        "database queries and request latency."
    )

    catalog = read_only_tool_catalog(request)
    names = [item["name"] for item in catalog]

    assert set(names) == {tool.name for tool in READ_ONLY_TOOLS}
    assert names.index("get_worker_hotspots") < 3
    assert names.index("get_worker_database_hotspots") < 3


def test_planned_selection_binds_only_allowlisted_tools():
    selected = select_planned_read_only_tools(
        ["get_worker_trace", "unknown_tool"]
    )

    assert [tool.name for tool in selected] == [
        "get_worker_trace"
    ]
    assert (
        select_planned_read_only_tools(["unknown_tool"])
        == READ_ONLY_TOOLS
    )


@pytest.mark.anyio
async def test_marker_action_records_when_precheck_confirms_marker_absent(
    monkeypatch,
):
    import origintracer_agent.tools as tools

    async def query_worker(_pid, _query, _worker_instance_id):
        raise ToolException(
            "OriginTracer backend returned HTTP 404: "
            "No deployment marker or graph diff found for 'eval-demo-release'"
        )

    recorded = {}

    async def record(label, worker_instance_id, worker_pid):
        recorded.update(
            label=label,
            worker_instance_id=worker_instance_id,
            worker_pid=worker_pid,
        )
        return {"ok": True, "recorded": True, "label": label}

    monkeypatch.setattr(tools, "query_worker", query_worker)
    monkeypatch.setattr(
        tools, "record_deployment_marker", record
    )

    result = await record_deployment_marker_action.coroutine(
        label="eval-demo-release",
        worker_pid="12345",
        worker_instance_id="worker-instance",
    )

    assert result == {
        "ok": True,
        "recorded": True,
        "label": "eval-demo-release",
    }
    assert recorded == {
        "label": "eval-demo-release",
        "worker_instance_id": "worker-instance",
        "worker_pid": 12345,
    }


@pytest.mark.anyio
async def test_marker_action_fails_closed_on_unexpected_precheck_error(
    monkeypatch,
):
    import origintracer_agent.tools as tools

    async def query_worker(_pid, _query, _worker_instance_id):
        raise ToolException(
            "OriginTracer backend returned HTTP 503: unavailable"
        )

    async def record(*_args):
        pytest.fail(
            "must not record a marker when its existence is unknown"
        )

    monkeypatch.setattr(tools, "query_worker", query_worker)
    monkeypatch.setattr(
        tools, "record_deployment_marker", record
    )

    with pytest.raises(ToolException, match="Could not verify"):
        await record_deployment_marker_action.coroutine(
            label="eval-demo-release",
            worker_pid="12345",
            worker_instance_id="worker-instance",
        )

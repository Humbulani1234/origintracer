"""Tests for read-tool catalog ordering and schema selection."""

from origintracer_agent.tools import (
    READ_ONLY_TOOLS,
    read_only_tool_catalog,
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

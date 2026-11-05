"""Tests for the agent's HTTP boundary and query validation."""

import httpx
import pytest
from langchain_core.tools import ToolException

from origintracer_agent import backend


def _mock_backend(monkeypatch, handler):
    transport = httpx.MockTransport(handler)
    async_client = httpx.AsyncClient

    def client_factory(**kwargs):
        return async_client(transport=transport, **kwargs)

    monkeypatch.setattr(
        backend.httpx, "AsyncClient", client_factory
    )
    monkeypatch.setattr(
        backend, "backend_url", lambda: "http://backend.test"
    )
    monkeypatch.setattr(
        backend, "backend_api_key", lambda: "test-api-key"
    )


@pytest.mark.anyio
async def test_worker_inventory_filters_invalid_rows_and_authenticates(
    monkeypatch,
):
    def handler(request):
        assert request.method == "GET"
        assert request.url.path == "/api/v1/snapshots"
        assert (
            request.headers["Authorization"]
            == "Bearer test-api-key"
        )
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "worker_pid": 321,
                        "worker_instance_id": "instance-a",
                        "online": True,
                        "received_at": 100.0,
                        "last_seen": 101.0,
                    },
                    {
                        "worker_pid": "not-a-pid",
                        "worker_instance_id": "bad",
                        "online": True,
                    },
                    {
                        "worker_pid": 654,
                        "worker_instance_id": "instance-stale",
                        "online": False,
                    },
                ]
            },
        )

    _mock_backend(monkeypatch, handler)

    assert await backend.list_workers() == [
        {
            "pid": "321",
            "worker_instance_id": "instance-a",
            "received_at": "100.0",
            "last_seen": "101.0",
            "online": True,
        },
        {
            "pid": "654",
            "worker_instance_id": "instance-stale",
            "received_at": "",
            "last_seen": "",
            "online": False,
        },
    ]


@pytest.mark.anyio
async def test_worker_query_reads_selected_stored_instance_status(
    monkeypatch,
):
    def handler(request):
        assert request.method == "GET"
        assert request.url.path == "/api/v1/status"
        assert (
            request.url.params["worker_instance_id"]
            == "instance-a"
        )
        return httpx.Response(
            200,
            json={
                "snapshot": {
                    "available": True,
                    "online": True,
                    "heartbeat_age_s": 2.0,
                    "last_seen": 101.0,
                    "nodes": 3,
                    "edges": 2,
                    "runtime_status": {
                        "uptime_s": 3600,
                        "active_requests": 1,
                        "probes_active": ["django"],
                        "probes_total": 1,
                        "rules_active": ["db_query_hotspot"],
                    },
                },
                "storage": "InMemoryRepository",
                "timestamp": 102.0,
            },
        )

    _mock_backend(monkeypatch, handler)
    result = await backend.query_worker(
        321, "SHOW status", worker_instance_id="instance-a"
    )

    assert result["worker_pid"] == "321"
    assert result["query"] == "SHOW status"
    assert (
        result["source"]
        == "OriginTracer stored snapshot/event query"
    )
    assert result["result"]["online"] is True
    assert result["result"]["data"]["uptime"] == "1h 0m"
    assert result["result"]["data"]["probes_active"] == [
        "django"
    ]
    assert result["result"]["data"]["rules_active"] == [
        "db_query_hotspot"
    ]


def test_query_builders_reject_unsafe_filters_and_allow_n_plus_one_tag():
    assert backend.build_event_query(
        500,
        trace_id="trace-1",
        service="django",
        probe="django.db.query",
    ) == (
        'SHOW events WHERE trace_id = "trace-1" AND service = "django" '
        'AND probe = "django.db.query" LIMIT 100'
    )
    assert backend.build_causal_query(["n+1", "db"]) == (
        'CAUSAL WHERE tags = "n+1,db"'
    )

    with pytest.raises(
        ToolException, match="trace_id must contain"
    ):
        backend.build_trace_query('trace" OR true')


@pytest.mark.parametrize("value", [None, "abc", 0, -1, True])
def test_pid_validation_rejects_values_that_cannot_select_a_worker(
    value,
):
    with pytest.raises(ToolException):
        backend.validate_pid(value)


def test_pid_validation_accepts_positive_integer_and_numeric_string():
    assert backend.validate_pid(321) == 321
    assert backend.validate_pid("654") == 654

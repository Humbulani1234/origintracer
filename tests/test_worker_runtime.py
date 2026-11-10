from __future__ import annotations

import time

import pytest

import backend.main as backend
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.storage.base import InMemoryRepository


@pytest.mark.anyio
async def test_runtime_heartbeat_is_stored_and_returned_with_freshness(
    monkeypatch,
):
    customer_id = "runtime-test-customer"
    instance_id = "runtime-test-instance"
    pid = 45678
    repository = InMemoryRepository()
    graph = RuntimeGraph()
    graph.upsert_node("django::view", "function", "django")

    monkeypatch.setitem(
        backend._valid_api_keys, "runtime-test-key", customer_id
    )
    monkeypatch.setattr(backend, "_repository", repository)
    monkeypatch.setitem(
        backend._graphs, (customer_id, instance_id), graph
    )
    monkeypatch.setitem(
        backend._snapshot_metadata,
        (customer_id, instance_id),
        {
            "worker_pid": pid,
            "received_at": time.time(),
            "node_count": 1,
            "edge_count": 0,
        },
    )

    status = {
        "uptime_s": 125.0,
        "active_requests": 2,
        "probes_active": ["django", "asyncio"],
        "probes_total": 2,
        "rules_active": ["db_query_hotspot"],
        "snapshot_interval_s": 15.0,
    }
    try:
        uploaded = await backend.receive_worker_runtime(
            backend.WorkerRuntimeRequest(
                status=status, observed_at=time.time()
            ),
            authorization="Bearer runtime-test-key",
            worker_instance_id_header=instance_id,
            worker_pid_header=pid,
            repository=repository,
        )
        assert uploaded["ok"] is True
        response = await backend.status(
            worker_instance_id=instance_id,
            authorization="Bearer runtime-test-key",
            repository=repository,
        )
    finally:
        backend._graphs.pop((customer_id, instance_id), None)
        backend._snapshot_metadata.pop(
            (customer_id, instance_id), None
        )
        backend._valid_api_keys.pop("runtime-test-key", None)

    snapshot = response["snapshot"]
    assert snapshot["online"] is True
    assert snapshot["runtime_status"] == status
    assert snapshot["runtime_status"]["uptime_s"] == 125.0
    assert snapshot["heartbeat_age_s"] <= 1.0

"""Focused tests for shared evidence paths used by application-local rules."""

from __future__ import annotations

import time

import httpx

from backend import main as backend
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.probes import external_http_probe
from origintracer.probes.external_http_probe import (
    ExternalHTTPProbe,
)


def test_retry_count_metadata_keeps_highest_observed_value():
    graph = RuntimeGraph()
    graph.upsert_node(
        "nginx::/checkout/",
        "nginx",
        "nginx",
        metadata={"retry_count": 5},
    )

    node = graph.upsert_node(
        "nginx::/checkout/",
        "nginx",
        "nginx",
        metadata={"retry_count": 0},
    )

    assert node.call_count == 2
    assert node.metadata["retry_count"] == 0
    assert node.metadata["max_retry_count"] == 5


def test_core_http_probe_emits_sanitized_outbound_call(
    monkeypatch,
):
    emitted = []
    monkeypatch.setattr(
        external_http_probe, "emit", emitted.append
    )
    probe = ExternalHTTPProbe()
    probe.start()
    try:
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, request=request)
        )
        with httpx.Client(transport=transport) as client:
            response = client.get(
                "https://user:secret@example.test:9443/private?token=hidden"
            )
    finally:
        probe.stop()

    assert response.status_code == 200
    assert len(emitted) == 1
    event = emitted[0]
    assert event.probe == "external.http.client"
    assert event.service == "external"
    assert event.name == "GET example.test:9443"
    assert event.metadata["external"] is True
    assert (
        event.metadata["dependency_host"] == "example.test:9443"
    )
    assert "secret" not in repr(event.metadata)
    assert "token=hidden" not in repr(event.metadata)
    assert (
        event.duration_ns is not None and event.duration_ns >= 0
    )


def test_core_http_probe_ignores_origintracer_upload(
    monkeypatch,
):
    emitted = []
    monkeypatch.setattr(
        external_http_probe, "emit", emitted.append
    )
    probe = ExternalHTTPProbe()
    probe.start()
    try:
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, request=request)
        )
        with httpx.Client(transport=transport) as client:
            request = client.build_request(
                "POST",
                "http://telemetry.example/snapshot",
                headers={
                    "X-OriginTracer-Worker-Instance-ID": "worker-a"
                },
            )
            client.send(request)
    finally:
        probe.stop()

    assert emitted == []


def _worker_graph(
    worker_pid: int, master_pid: int
) -> RuntimeGraph:
    graph = RuntimeGraph()
    graph.upsert_node(
        "gunicorn::master",
        "gunicorn",
        "gunicorn",
        metadata={
            "probe": "gunicorn.master.start",
            "master_pid": master_pid,
        },
    )
    graph.upsert_node(
        f"gunicorn::UvicornWorker-{worker_pid}",
        "gunicorn",
        "gunicorn",
        metadata={
            "probe": "gunicorn.worker.fork",
            "worker_pid": worker_pid,
            "master_pid": master_pid,
        },
    )
    return graph


def _add_fresh_worker(
    monkeypatch,
    instance_id: str,
    worker_pid: int,
    master_pid: int,
    start_count: int,
    end_count: int,
    *,
    observed_at: float | None = None,
) -> None:
    now = time.time()
    key = ("test-customer", instance_id)
    backend._graphs[key] = _worker_graph(worker_pid, master_pid)
    backend._snapshot_metadata[key] = {
        "worker_pid": worker_pid,
        "runtime_status": {"snapshot_interval_s": 15},
        "runtime_observed_at": observed_at or now,
    }
    backend._worker_request_samples[key] = [
        (now - 30, start_count),
        (now, end_count),
    ]


def test_fleet_worker_imbalance_compares_recent_sibling_rates(
    monkeypatch,
):
    monkeypatch.setattr(backend, "_graphs", {})
    monkeypatch.setattr(backend, "_snapshot_metadata", {})
    monkeypatch.setattr(backend, "_worker_request_samples", {})
    _add_fresh_worker(monkeypatch, "worker-a", 101, 900, 0, 40)
    _add_fresh_worker(monkeypatch, "worker-b", 102, 900, 0, 10)

    match = backend._fleet_worker_imbalance(
        "test-customer", "worker-a", None
    )

    assert match is not None
    assert match["rule"] == "worker_imbalance"
    assert match["evidence"]["rate_ratio"] == 4.0
    assert match["evidence"]["gunicorn_master_pid"] == 900
    assert len(match["evidence"]["workers"]) == 2


def test_fleet_worker_imbalance_ignores_different_masters(
    monkeypatch,
):
    monkeypatch.setattr(backend, "_graphs", {})
    monkeypatch.setattr(backend, "_snapshot_metadata", {})
    monkeypatch.setattr(backend, "_worker_request_samples", {})
    _add_fresh_worker(monkeypatch, "worker-a", 101, 900, 0, 40)
    _add_fresh_worker(monkeypatch, "worker-b", 102, 901, 0, 10)

    assert (
        backend._fleet_worker_imbalance(
            "test-customer", "worker-a", None
        )
        is None
    )


def test_fleet_worker_imbalance_requires_fresh_heartbeats(
    monkeypatch,
):
    monkeypatch.setattr(backend, "_graphs", {})
    monkeypatch.setattr(backend, "_snapshot_metadata", {})
    monkeypatch.setattr(backend, "_worker_request_samples", {})
    now = time.time()
    _add_fresh_worker(monkeypatch, "worker-a", 101, 900, 0, 40)
    _add_fresh_worker(
        monkeypatch,
        "worker-b",
        102,
        900,
        0,
        10,
        observed_at=now - 60,
    )

    assert (
        backend._fleet_worker_imbalance(
            "test-customer", "worker-a", None
        )
        is None
    )


def test_fleet_worker_imbalance_reports_online_zero_request_sibling(
    monkeypatch,
):
    monkeypatch.setattr(backend, "_graphs", {})
    monkeypatch.setattr(backend, "_snapshot_metadata", {})
    monkeypatch.setattr(backend, "_worker_request_samples", {})
    _add_fresh_worker(monkeypatch, "worker-a", 101, 900, 0, 40)
    _add_fresh_worker(monkeypatch, "worker-b", 102, 900, 0, 0)

    match = backend._fleet_worker_imbalance(
        "test-customer", "worker-b", None
    )

    assert match is not None
    assert (
        match["evidence"]["imbalance_kind"]
        == "zero_requests_on_online_worker"
    )
    assert match["evidence"]["zero_request_worker_pids"] == [102]


def test_causal_evaluation_includes_backend_fleet_match(
    monkeypatch,
):
    from origintracer.core.causal import PatternRegistry
    from origintracer.storage.base import InMemoryRepository

    graph = RuntimeGraph()
    monkeypatch.setattr(
        backend,
        "_graphs",
        {("test-customer", "worker-a"): graph},
    )
    monkeypatch.setattr(backend, "_snapshot_metadata", {})
    monkeypatch.setattr(backend, "_reported_causal_matches", {})
    fleet_match = {"rule": "worker_imbalance", "confidence": 0.8}
    monkeypatch.setattr(
        PatternRegistry,
        "evaluate",
        classmethod(
            lambda cls, graph, temporal, tracker=None, tags=None: []
        ),
    )
    monkeypatch.setattr(
        backend,
        "_fleet_worker_imbalance",
        lambda customer, instance, tags: fleet_match,
    )

    result = backend._evaluate_causal_rules(
        "test-customer",
        "worker-a",
        InMemoryRepository(),
        None,
        persist=False,
    )

    assert result["match_count"] == 1
    assert result["data"] == [fleet_match]

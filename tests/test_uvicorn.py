import pytest

from applications.django.probes import uvicorn_probe
from applications.django.rules.uvicorn_rules import (
    POST_DEPLOYMENT_REGRESSION,
    REQUEST_DURATION_ANOMALY,
    TRAFFIC_SPIKE,
    _post_deployment_regression,
    _request_duration_anomaly,
    _traffic_spike,
)
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import GraphDiff, TemporalStore


@pytest.mark.asyncio
async def test_asgi_middleware_propagates_request_id_and_emits_completion(
    monkeypatch,
):
    events = []
    monkeypatch.setattr(uvicorn_probe, "emit", events.append)

    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 201,
                "headers": [],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"ok",
                "more_body": False,
            }
        )

    async def receive():
        return {
            "type": "http.request",
            "body": b"",
            "more_body": False,
        }

    async def send(_message):
        return None

    middleware = uvicorn_probe.OriginTracerASGIMiddleware(app)
    await middleware(
        {
            "type": "http",
            "method": "GET",
            "path": "/health",
            "headers": [(b"x-request-id", b"nginx-request-7")],
            "client": ("127.0.0.1", 1234),
            "http_version": "1.1",
        },
        receive,
        send,
    )

    assert [event.probe for event in events] == [
        "uvicorn.request.receive",
        "uvicorn.response.start",
        "uvicorn.response.body",
        "uvicorn.request.complete",
    ]
    assert {event.trace_id for event in events} == {
        "nginx-request-7"
    }
    assert events[-1].duration_ns >= 0
    assert events[-1].metadata["status_code"] == 201


def test_uvicorn_rules_register_as_one_integration():
    names = set()

    class Registry:
        def register(self, rule):
            names.add(rule.name)

    from applications.django.rules.uvicorn_rules import register

    register(Registry())

    assert names == {
        REQUEST_DURATION_ANOMALY.name,
        TRAFFIC_SPIKE.name,
        POST_DEPLOYMENT_REGRESSION.name,
    }


def test_request_anomaly_compares_recent_percentile_to_baseline():
    graph = RuntimeGraph()
    for _ in range(50):
        graph.upsert_node(
            "django::/slow/", "django", "django", 1_000_000
        )

    class Tracker:
        def all_patterns_summary(self):
            return {
                "/slow/": {
                    "count": 10,
                    "p99_ms": 4.0,
                    "avg_ms": 3.5,
                }
            }

        def slow_in_flight(self, threshold_ms):
            return []

    matched, evidence = _request_duration_anomaly(
        graph, TemporalStore(), Tracker()
    )
    assert matched is True
    assert evidence["anomalous_endpoints"][0]["ratio"] == 4.0


def test_traffic_spike_uses_new_node_diff():
    graph = RuntimeGraph()
    graph.upsert_node("django::/", "django", "django")
    temporal = TemporalStore()
    temporal._diffs.append(
        GraphDiff(
            0.0,
            None,
            None,
            added_node_ids={
                f"django::new-{i}" for i in range(10)
            },
        )
    )
    matched, evidence = _traffic_spike(graph, temporal)
    assert matched is True
    assert evidence["added_node_count"] == 10


def test_deployment_rule_reports_slow_node_on_added_edge():
    graph = RuntimeGraph()
    graph.upsert_node(
        "django::/slow/",
        "django",
        "django",
        metadata={"avg_duration_ns": 250_000_000},
    )
    temporal = TemporalStore()
    temporal._diffs.append(
        GraphDiff(
            0.0,
            None,
            None,
            added_edge_keys={
                "django::caller→django::/slow/:calls"
            },
        )
    )
    matched, evidence = _post_deployment_regression(
        graph, temporal
    )
    assert matched is True
    assert (
        evidence["regressed_nodes"][0]["node"]
        == "django::/slow/"
    )

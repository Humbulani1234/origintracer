import json

from applications.django.probes import nginx_probe
from applications.django.rules.nginx_rules import (
    _retry_amplification,
)
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore


def test_nginx_access_log_probe_records_upstream_attempts(
    monkeypatch,
):
    observed = []
    monkeypatch.setattr(nginx_probe, "emit", observed.append)
    probe = nginx_probe._NginxLogMode("unused")

    probe._line(
        json.dumps(
            {
                "request_id": "request-1",
                "uri": "/checkout/",
                "status": "502",
                "request_time": "1.2",
                "upstream_response_time": "0.1:0.2:0.3:0.4,0.5:0.6",
            }
        )
    )

    assert len(observed) == 1
    event = observed[0]
    assert event.probe == "nginx.request.complete"
    assert event.trace_id == "request-1"
    assert event.metadata["retry_count"] == 4
    assert event.metadata["upstream_attempt_count"] == 6
    assert (
        event.metadata["upstream_duration_ns"] == 2_100_000_000
    )


def test_nginx_retry_rule_uses_maximum_observed_retry_count():
    graph = RuntimeGraph()
    graph.upsert_node(
        "nginx::/checkout/",
        "nginx",
        "nginx",
        metadata={"retry_count": 4, "max_retry_count": 4},
    )

    matched, evidence = _retry_amplification(
        graph, TemporalStore()
    )

    assert matched is True
    assert evidence["requests"][0]["attempt_count"] == 5

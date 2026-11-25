from types import SimpleNamespace

from origintracer.core.otel_bridge import span_to_event


def _span(kind_value, attributes):
    return SimpleNamespace(
        context=SimpleNamespace(trace_id=0x1234, span_id=0x5678),
        parent=None,
        kind=SimpleNamespace(value=kind_value),
        attributes=attributes,
        instrumentation_scope=SimpleNamespace(
            name="httpx.client"
        ),
        name="HTTP GET",
        start_time=1_000_000_000,
        end_time=1_250_000_000,
        status=SimpleNamespace(
            status_code=SimpleNamespace(value=1),
        ),
    )


def test_client_span_is_marked_external_for_dependency_rules():
    event = span_to_event(
        _span(
            2,
            {
                "http.method": "GET",
                "http.route": "/charge",
                "peer.service": "payments",
            },
        )
    )

    assert event is not None
    assert event.probe == "otel.client"
    assert event.service == "payments"
    assert event.name == "GET /charge"
    assert event.duration_ns == 250_000_000
    assert event.metadata["external"] is True


def test_non_client_span_is_not_marked_external():
    event = span_to_event(
        _span(1, {"http.method": "GET", "http.route": "/health"})
    )

    assert event is not None
    assert event.probe == "otel.server"
    assert event.metadata["external"] is False

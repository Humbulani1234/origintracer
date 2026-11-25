from applications.django.probes import gunicorn_probe
from applications.django.rules.gunicorn_rules import (
    _worker_imbalance,
    cascade_failure,
)
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore


def test_gunicorn_hook_installer_chains_existing_hooks():
    calls = []
    config = {
        "on_starting": lambda server: calls.append(
            ("existing", server)
        )
    }
    gunicorn_probe._chain(
        config,
        "on_starting",
        lambda server: calls.append(("ot", server)),
    )

    config["on_starting"]("server")

    assert calls == [("existing", "server"), ("ot", "server")]


def test_gunicorn_abort_probe_emits_explicit_crash_reason(
    monkeypatch,
):
    events = []
    monkeypatch.setattr(
        gunicorn_probe, "emit_direct", events.append
    )

    gunicorn_probe.ot_worker_abort(type("Worker", (), {})())

    assert len(events) == 1
    assert events[0].probe == "gunicorn.worker.crash"
    assert events[0].metadata["reason"] == "SIGABRT_timeout"


def test_cascade_rule_ignores_graceful_sigint_but_reports_abort():
    graph = RuntimeGraph()
    graph.upsert_node(
        "gunicorn::worker-int",
        "gunicorn",
        "gunicorn",
        metadata={
            "probe": "gunicorn.worker.crash",
            "reason": "SIGINT",
        },
    )
    assert cascade_failure(graph, TemporalStore())[0] is False
    graph.upsert_node(
        "gunicorn::worker-abort",
        "gunicorn",
        "gunicorn",
        metadata={
            "probe": "gunicorn.worker.crash",
            "reason": "SIGABRT_timeout",
        },
    )
    matched, evidence = cascade_failure(graph, TemporalStore())
    assert matched is True
    assert (
        evidence["observed_failures"][0]["reason"]
        == "SIGABRT_timeout"
    )


def test_worker_imbalance_requires_multiple_observed_workers():
    graph = RuntimeGraph()
    for worker, n in (("worker-1", 4), ("worker-2", 1)):
        worker_id = f"gunicorn::{worker}"
        graph.upsert_node(
            worker_id,
            "gunicorn",
            "gunicorn",
            metadata={
                "probe": "gunicorn.worker.fork",
                "worker_pid": worker,
            },
        )
        for index in range(n):
            graph.upsert_edge(
                worker_id,
                f"django::request-{worker}-{index}",
                "handled",
            )

    matched, evidence = _worker_imbalance(graph, TemporalStore())
    assert matched is True
    assert evidence["ratio"] == 4.0

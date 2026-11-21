from types import SimpleNamespace

from probes import celery_probe
from rules.celery_rules import (
    CELERY_TASK_DURATION_SPIKE,
    SYNC_DB_IN_CELERY,
)

from origintracer.context.vars import (
    get_trace_id,
    reset_trace,
    set_trace,
)
from origintracer.core.causal import PatternRegistry
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore


def fresh():
    return RuntimeGraph(), TemporalStore(), None


def test_task_trace_id_crosses_publish_and_task_lifecycle(
    monkeypatch,
):
    events = []
    monkeypatch.setattr(celery_probe, "emit", events.append)
    probe = celery_probe.CeleryProbe()
    ambient = set_trace("request-trace")
    celery_probe._task_state.clear()
    try:
        headers = {}
        probe._on_before_task_publish(headers=headers)
        assert (
            headers[celery_probe._TRACE_HEADER]
            == "request-trace"
        )

        task = SimpleNamespace(
            name="reports.generate",
            max_retries=3,
            request=SimpleNamespace(
                headers=headers, retries=0, max_retries=3
            ),
        )
        probe._on_task_start(
            task_id="task-1", task=task, args=(), kwargs={}
        )
        assert get_trace_id() == "request-trace"
        probe._on_task_end(
            task_id="task-1",
            task=task,
            args=(),
            kwargs={},
            retval=None,
            state="SUCCESS",
        )

        assert [event.probe for event in events] == [
            "celery.task.start",
            "celery.task.end",
        ]
        assert all(
            event.trace_id == "request-trace" for event in events
        )
        assert get_trace_id() == "request-trace"
        assert "task-1" not in celery_probe._task_state
    finally:
        celery_probe._task_state.clear()
        reset_trace(ambient)


def test_retry_probe_uses_propagated_trace_header(monkeypatch):
    events = []
    monkeypatch.setattr(celery_probe, "emit", events.append)
    request = SimpleNamespace(
        headers={celery_probe._TRACE_HEADER: "parent-trace"},
        id="task-only-id",
        task="reports.generate",
        retries=1,
    )

    celery_probe.CeleryProbe()._on_task_retry(
        request=request,
        reason="retry",
        einfo=None,
    )

    assert events[0].trace_id == "parent-trace"
    assert events[0].metadata["retries"] == 1


def _add_celery_task(
    g,
    task_name,
    call_count=1,
    state="SUCCESS",
    retries=0,
    duration_ns=0,
):
    node_id = f"celery::{task_name}"
    # Simulate multiple calls to the same task
    for i in range(call_count):
        # In real life, 'retries' increments with each call.
        # For the test, we can just ensure the final call has the max retries.
        g.upsert_node(
            node_id,
            node_type="celery",
            service="celery",
            duration_ns=duration_ns,
            metadata={
                "probe": "celery.task.end",
                "state": state,
                "retries": retries,  # This will be the value stored in the node
            },
        )
    return g


def _add_db_node(
    g,
    node_id,
    service="postgres",
    call_count=10,
    total_duration_ns=500_000_000,
):
    for _ in range(call_count):
        g.upsert_node(
            node_id,
            node_type=service,
            service=service,
            duration_ns=total_duration_ns,
            metadata={"probe": f"{service}.query"},
        )
    return g


class TestSyncDbInCelery:
    def teardown_method(self):
        PatternRegistry._reset()

    def test_fires_when_celery_task_has_slow_db_edge(self):
        g, t, a = fresh()
        _add_celery_task(
            g, "myapp.tasks.process_report", call_count=10
        )
        _add_db_node(
            g,
            "postgres::SELECT report WHERE id=%s",
            service="postgres",
            call_count=10,
            total_duration_ns=500_000_000,
        )  # 50ms avg
        g.upsert_edge(
            "celery::myapp.tasks.process_report",
            "postgres::SELECT report WHERE id=%s",
            "calls",
        )
        matched, evidence = SYNC_DB_IN_CELERY.predicate(g, t, a)
        assert matched
        assert evidence["blocking_db_calls"]
        assert evidence["blocking_db_calls"][0]["avg_ms"] > 50

    def test_silent_when_db_is_fast(self):
        g, t, a = fresh()
        _add_celery_task(
            g, "myapp.tasks.fast_task", call_count=10
        )
        # fast db — 1ms avg
        _add_db_node(
            g,
            "postgres::SELECT id FROM users",
            service="postgres",
            call_count=10,
            total_duration_ns=10_000_000,
        )
        g.upsert_edge(
            "celery::myapp.tasks.fast_task",
            "postgres::SELECT id FROM users",
            "calls",
        )
        matched, _ = SYNC_DB_IN_CELERY.predicate(g, t, a)
        assert not matched

    def test_silent_when_no_db_edge(self):
        g, t, a = fresh()
        _add_celery_task(g, "myapp.tasks.no_db", call_count=10)
        matched, _ = SYNC_DB_IN_CELERY.predicate(g, t, a)
        assert not matched

    def test_silent_when_target_is_not_db(self):
        g, t, a = fresh()
        _add_celery_task(
            g, "myapp.tasks.redis_task", call_count=10
        )
        for _ in range(10):
            g.upsert_node(
                "redis::cache.get",
                node_type="redis",
                service="redis",
                metadata={"probe": "redis.command"},
            )
        g.upsert_edge(
            "celery::myapp.tasks.redis_task",
            "redis::cache.get",
            "calls",
        )
        matched, _ = SYNC_DB_IN_CELERY.predicate(g, t, a)
        assert not matched

    def test_fires_for_mysql_too(self):
        g, t, a = fresh()
        _add_celery_task(
            g, "myapp.tasks.mysql_task", call_count=10
        )
        _add_db_node(
            g,
            "mysql::SELECT * FROM orders",
            service="mysql",
            call_count=10,
            total_duration_ns=500_000_000,
        )
        g.upsert_edge(
            "celery::myapp.tasks.mysql_task",
            "mysql::SELECT * FROM orders",
            "calls",
        )
        matched, _ = SYNC_DB_IN_CELERY.predicate(g, t, a)
        assert matched

    def test_evidence_contains_remediation(self):
        g, t, a = fresh()
        _add_celery_task(g, "myapp.tasks.slow_db", call_count=10)
        _add_db_node(
            g,
            "postgres::slow_query",
            service="postgres",
            call_count=10,
            total_duration_ns=500_000_000,
        )
        g.upsert_edge(
            "celery::myapp.tasks.slow_db",
            "postgres::slow_query",
            "calls",
        )
        matched, evidence = SYNC_DB_IN_CELERY.predicate(g, t, a)
        assert matched
        assert "remediation" in evidence
        assert "sync_to_async" in evidence["remediation"]

    def test_confidence_is_high(self):
        assert SYNC_DB_IN_CELERY.confidence >= 0.80


class TestCeleryTaskDurationSpike:
    def teardown_method(self):
        PatternRegistry._reset()

    def _build_duration_graph(
        self,
        slow_duration_ns=10_000_000_000,
        normal_duration_ns=500_000_000,
        slow_count=10,
        normal_count=10,
    ):
        g = RuntimeGraph()

        # 1. Normal Tasks
        for _ in range(normal_count):
            g.upsert_node(
                "celery::myapp.tasks.process_report",
                node_type="celery",
                service="celery",
                duration_ns=normal_duration_ns,  # CORRECT: Passed here
                metadata={
                    "probe": "celery.task.end",
                    "state": "SUCCESS",
                },
            )

        for _ in range(normal_count):
            g.upsert_node(
                "celery::myapp.tasks.generate_report",
                node_type="celery",
                service="celery",
                duration_ns=normal_duration_ns,  # CORRECT: Passed here
                metadata={
                    "probe": "celery.task.end",
                    "state": "SUCCESS",
                },
            )

        # 2. Slow Outlier Task
        for _ in range(slow_count):
            g.upsert_node(
                "celery::myapp.tasks.export_data",
                node_type="celery",
                service="celery",
                duration_ns=slow_duration_ns,  # CORRECT: Passed here
                metadata={
                    "probe": "celery.task.end",
                    "state": "SUCCESS",
                },
            )

        return g

    def test_fires_when_one_task_is_outlier(self):
        # export_data takes 10s avg vs 500ms for others — 20x spike
        g = self._build_duration_graph(
            slow_duration_ns=10_000_000_000,
            normal_duration_ns=500_000_000,
        )
        matched, evidence = CELERY_TASK_DURATION_SPIKE.predicate(
            g, TemporalStore(), None
        )
        assert matched
        assert evidence["slow_tasks"]
        assert evidence["slow_tasks"][0]["ratio"] > 2

    def test_silent_when_all_tasks_similar_duration(self):
        g = RuntimeGraph()
        for task in ["task_a", "task_b", "task_c"]:
            for _ in range(10):
                g.upsert_node(
                    f"celery::myapp.tasks.{task}",
                    node_type="celery",
                    service="celery",
                    metadata={
                        "probe": "celery.task.end",
                        "state": "SUCCESS",
                        "total_duration_ns": 500_000_000,
                    },
                )
        matched, _ = CELERY_TASK_DURATION_SPIKE.predicate(
            g, TemporalStore(), None
        )
        assert not matched

    def test_silent_when_only_one_celery_node(self):
        g = RuntimeGraph()
        for _ in range(10):
            g.upsert_node(
                "celery::myapp.tasks.only_task",
                node_type="celery",
                service="celery",
                metadata={
                    "probe": "celery.task.end",
                    "state": "SUCCESS",
                    "total_duration_ns": 500_000_000,
                },
            )
        matched, _ = CELERY_TASK_DURATION_SPIKE.predicate(
            g, TemporalStore(), None
        )
        assert not matched

    def test_silent_when_below_call_count_threshold(self):
        # slow task but only 2 calls — below min threshold of 5
        g = self._build_duration_graph(
            slow_duration_ns=10_000_000_000,
            normal_duration_ns=500_000_000,
            slow_count=2,  # below threshold
            normal_count=10,
        )
        matched, _ = CELERY_TASK_DURATION_SPIKE.predicate(
            g, TemporalStore(), None
        )
        assert not matched

    def test_silent_when_dispatch_nodes_only(self):
        # dispatch nodes have total_duration_ns=0 — should be excluded
        g = RuntimeGraph()
        for task in ["task_a", "task_b", "task_c"]:
            for _ in range(10):
                g.upsert_node(
                    f"celery::myapp.tasks.{task}",
                    node_type="celery",
                    service="celery",
                    metadata={
                        "probe": "celery.task.dispatch",
                        "task_name": f"myapp.tasks.{task}",
                    },
                )
        matched, _ = CELERY_TASK_DURATION_SPIKE.predicate(
            g, TemporalStore(), None
        )
        assert not matched

    def test_evidence_contains_median_and_ratio(self):
        g = self._build_duration_graph(
            slow_duration_ns=10_000_000_000,
            normal_duration_ns=500_000_000,
        )
        matched, evidence = CELERY_TASK_DURATION_SPIKE.predicate(
            g, TemporalStore(), None
        )
        assert matched
        assert "median_ms" in evidence
        assert evidence["median_ms"] > 0
        task = evidence["slow_tasks"][0]
        assert "avg_ms" in task
        assert "ratio" in task
        assert "task" in task

    def test_evidence_contains_remediation(self):
        g = self._build_duration_graph(
            slow_duration_ns=10_000_000_000,
            normal_duration_ns=500_000_000,
        )
        matched, evidence = CELERY_TASK_DURATION_SPIKE.predicate(
            g, TemporalStore(), None
        )
        assert matched
        assert "remediation" in evidence
        assert "DIFF" in evidence["remediation"]

    def test_confidence(self):
        assert CELERY_TASK_DURATION_SPIKE.confidence >= 0.70

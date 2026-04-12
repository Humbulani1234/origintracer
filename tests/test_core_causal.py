"""
tests/test_core_causal.py

Tests for every built-in causal rule.

Each rule has two test cases:
    1. FIRES    — build a graph/store that matches the rule's condition
    2. NO FIRE  — verify the rule stays silent on a clean graph

This is the most important correctness test in the suite. A rule that
fires on clean data creates alert fatigue. A rule that never fires even
when the condition is met is useless.

Also tests:
    - PatternRegistry.evaluate() tag filtering
    - Registry builds correctly with and without tracker
    - User rules register correctly via the register(registry) convention
"""

from __future__ import annotations

import time
import uuid

import pytest

from applications.django.rules.gunicorn_rules import (
    WORKER_IMBALANCE,
)
from applications.django.rules.nginx_rules import (
    RETRY_AMPLIFICATION,
)
from applications.django.rules.uvicorn_rules import (
    REQUEST_DURATION_ANOMALY,
)
from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import (
    CausalRule,
    PatternRegistry,
)
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore
from origintracer.rules.asyncio_rules import (
    LOOP_STARVATION,
    NEW_SYNC_CALL,
)
from origintracer.rules.django_rules import (
    DB_HOTSPOT,
    N_PLUS_ONE,
)


def fresh(tracker):
    """Return a clean (graph, temporal) pair."""
    return (
        RuntimeGraph(),
        TemporalStore(),
        tracker,
    )


class TestPatternRegistry:

    def teardown_method(self):
        PatternRegistry._reset()

    def test_register_and_list(self):
        r = PatternRegistry
        r.register(
            CausalRule(
                name="test_rule",
                description="test",
                predicate=lambda g, t: (False, {}),
            )
        )
        assert "test_rule" in r.rule_names()

    def test_evaluate_calls_predicate(self, tracker):
        fired = []

        def pred(g, t, a):
            fired.append(True)
            return True, {"key": "value"}

        r = PatternRegistry
        r.register(
            CausalRule(name="r", description="d", predicate=pred)
        )
        g, t, a = fresh(tracker)
        matches = r.evaluate(g, t, a)
        assert len(matches) == 1
        assert matches[0].evidence == {"key": "value"}
        assert fired

    def test_tag_filtering_excludes_unmatched(self, tracker):
        r = PatternRegistry
        r.register(
            CausalRule(
                name="a",
                description="",
                predicate=lambda g, t: (True, {}),
                tags=["latency"],
            )
        )
        r.register(
            CausalRule(
                name="b",
                description="",
                predicate=lambda g, t: (True, {}),
                tags=["db"],
            )
        )
        g, t, a = fresh(tracker)
        matches = r.evaluate(g, t, a, tags=["latency"])
        assert all(m.rule_name == "a" for m in matches)

    def test_broken_rule_does_not_crash_evaluate(self, tracker):
        def exploding(g, t):
            raise RuntimeError("rule is broken")

        r = PatternRegistry
        r.register(
            CausalRule(
                name="broken",
                description="",
                predicate=exploding,
            )
        )
        g, t, a = fresh(tracker)
        matches = r.evaluate(g, t, a)
        # Returns a zero-confidence match explaining the error rather than raising
        assert len(matches) == 1
        assert matches[0].confidence == 0.0

    def test_results_sorted_by_confidence_descending(
        self, tracker
    ):
        r = PatternRegistry
        r.register(
            CausalRule(
                name="low",
                description="",
                predicate=lambda g, t: (True, {}),
                confidence=0.3,
            )
        )
        r.register(
            CausalRule(
                name="high",
                description="",
                predicate=lambda g, t: (True, {}),
                confidence=0.9,
            )
        )
        g, t, a = fresh(tracker)
        matches = r.evaluate(g, t, a)
        assert matches[0].confidence >= matches[1].confidence

    def test_build_default_registry_includes_all_rules(self):
        r = PatternRegistry
        names = r.rule_names()
        assert "n_plus_one_queries" in names
        assert "new_sync_call_after_deployment" in names
        assert "asyncio_event_loop_starvation" in names
        assert "worker_imbalance" in names
        assert "retry_amplification" in names
        assert "db_query_hotspot" in names
        assert "request_duration_anomaly" in names

    def test_build_default_registry_order_confidence_descending(
        self,
    ):
        """
        Rules must be registered highest-confidence first so the REPL
        shows the most actionable match at the top.
        N_PLUS_ONE(0.90) > NEW_SYNC_CALL(0.85) > LOOP_STARVATION(0.80)
        > WORKER_IMBALANCE(0.80) > RETRY_AMPLIFICATION(0.75) > DB_HOTSPOT(0.70)
        """
        r = PatternRegistry
        names = r.rule_names()
        assert names.index("n_plus_one_queries") < names.index(
            "db_query_hotspot"
        )
        assert names.index(
            "new_sync_call_after_deployment"
        ) < names.index("retry_amplification")

    def test_build_default_registry_without_tracker_is_safe(
        self, tracker
    ):
        """tracker=None means anomaly rule registers but never fires — no crash."""
        r = PatternRegistry
        g, t, a = fresh(tracker)
        matches = r.evaluate(g, t, a)
        anomalies = [
            m
            for m in matches
            if m.rule_name == "request_duration_anomaly"
        ]
        assert anomalies == []


class TestRetryAmplification:

    def teardown_method(self):
        PatternRegistry._reset()

    def test_fires_when_retry_count_high(self, tracker):
        g, t, a = fresh(tracker)
        g.upsert_node("django::view", "fn", "django")
        g.upsert_node("redis::get", "fn", "redis")
        g.upsert_edge("django::view", "redis::get", "calls")
        # Inject retry metadata onto the edge
        edge_key = list(g._edge_index.keys())[0]
        g._edge_index[edge_key].metadata["retries"] = 10
        matched, evidence = RETRY_AMPLIFICATION.predicate(
            g, t, a
        )
        assert matched
        assert any(e["retries"] >= 10 for e in evidence["edges"])

    def test_silent_when_no_retries(self, tracker):
        g, t, a = fresh(tracker)
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")
        g.upsert_edge("A", "B", "calls")
        matched, _ = RETRY_AMPLIFICATION.predicate(g, t, a)
        assert not matched


class TestNewSyncCallAfterDeployment:

    def test_silent_without_deployment_marker(self, tracker):
        g, t, a = fresh(tracker)
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")
        g.upsert_edge("A", "B", "calls")
        t.capture(g.snapshot())

        matched, _ = NEW_SYNC_CALL.predicate(g, t, a)
        assert not matched

    def test_silent_when_edge_existed_before_deployment(
        self, tracker
    ):
        g, t, a = fresh(tracker)
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")
        g.upsert_edge("A", "B", "calls")

        # edge exists before deployment
        t.capture(g.snapshot())
        t.mark_event("deployment")
        time.sleep(0.01)
        t.capture(g.snapshot())  # no new edges

        matched, _ = NEW_SYNC_CALL.predicate(g, t, a)
        assert not matched

    def test_silent_when_no_slow_nodes(self, tracker):
        """New edges after deployment but no latency — should not fire."""
        g, t, a = fresh(tracker)
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")

        t.capture(g.snapshot())
        t.mark_event("deployment")
        time.sleep(0.01)

        g.upsert_edge("A", "B", "calls")
        t.capture(g.snapshot())
        # no slow nodes — avg_duration_ns not set

        matched, _ = NEW_SYNC_CALL.predicate(g, t, a)
        assert not matched

    def test_fires_when_new_edge_and_slow_node(self, tracker):
        """New edges after deployment AND slow node — should fire."""
        g, t, a = fresh(tracker)
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")

        t.capture(g.snapshot())
        t.mark_event("deployment")
        for diff in t._diffs:
            if diff.label == "deployment":
                diff.timestamp -= 121

        g.upsert_edge("A", "B", "calls")
        # simulate slow node — >200ms
        g.upsert_node(
            "A", "fn", "svc", duration_ns=500 * 1_000_000
        )
        t.capture(g.snapshot())
        matched, evidence = NEW_SYNC_CALL.predicate(g, t, a)
        assert matched
        assert "slow_nodes" in evidence

    def test_silent_when_slow_node_unrelated_to_new_edges(
        self, tracker
    ):
        """Slow node exists but is not part of any new sync edge — should not fire."""
        g, t, a = fresh(tracker)
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")
        g.upsert_node(
            "C", "fn", "svc", duration_ns=250 * 1_000_000
        )

        t.capture(g.snapshot())
        t.mark_event("deployment")
        time.sleep(0.01)

        # new edge between A and B — C is slow but unrelated
        g.upsert_edge("A", "B", "calls")
        t.capture(g.snapshot())

        matched, _ = NEW_SYNC_CALL.predicate(g, t, a)
        assert not matched


class TestLoopStarvation:

    def teardown_method(self):
        PatternRegistry._reset()

    def test_fires_when_loop_tick_avg_high(self, tracker):
        g, t, a = fresh(tracker)
        g.upsert_node("asyncio::loop.tick", "asyncio", "asyncio")
        node = g._nodes["asyncio::loop.tick"]
        # Simulate 20ms average tick duration (threshold is 10ms)
        node.total_duration_ns = 20_000_000
        node.call_count = 1
        node.node_type = "asyncio"
        matched, evidence = LOOP_STARVATION.predicate(g, t, a)
        assert matched
        assert any(
            e["avg_ms"] >= 20 for e in evidence["stalled_ticks"]
        )

    def test_silent_when_loop_tick_fast(self, tracker):
        g, t, a = fresh(tracker)
        g.upsert_node("asyncio::loop.tick", "asyncio", "asyncio")
        node = g._nodes["asyncio::loop.tick"]
        node.total_duration_ns = (
            1_000_000  # 1ms — well below 10ms threshold
        )
        node.call_count = 1
        node.node_type = "asyncio"
        matched, _ = LOOP_STARVATION.predicate(g, t, a)
        assert not matched


class TestDbQueryHotspot:
    """
    _is_db_node() checks metadata["probe"].endswith(".db.query"),
    NOT node_type == "db". Tests must set the probe key in metadata.
    Threshold: a single query node call_count > 30% of total and > 5.
    """

    def teardown_method(self):
        PatternRegistry._reset()

    def _db_node(
        self, g: RuntimeGraph, node_id: str, call_count: int
    ) -> None:
        """Add a DB query node with correct probe metadata."""
        for _ in range(call_count):
            g.upsert_node(
                node_id,
                node_type="django",
                service="django",
                metadata={"probe": "django.db.query"},
            )

    def test_fires_when_single_query_dominates(self, tracker):
        g, t, a = fresh(tracker)
        # 10 book queries + 1 author query + 1 view node
        # book query = 10 / (10+1+1) = 83% — well above 30% threshold
        self._db_node(
            g,
            "django::SELECT book WHERE author_id=%s",
            call_count=10,
        )
        self._db_node(g, "django::SELECT author", call_count=1)
        g.upsert_node(
            "django::NPlusOneView", "django", "django"
        )  # no probe metadata

        matched, evidence = DB_HOTSPOT.predicate(g, t, a)
        assert matched
        assert any(
            e["call_count"] >= 10
            for e in evidence["hotspot_queries"]
        )
        # Evidence must name the hotspot query
        assert any(
            "SELECT book" in q["node"]
            for q in evidence.get("hotspot_queries", [])
        )

    def test_silent_when_calls_evenly_distributed(self, tracker):
        g, t, a = fresh(tracker)
        # Ten DB queries with equal counts — none exceeds 30%
        for i in range(10):
            self._db_node(
                g, f"django::SELECT table_{i}", call_count=2
            )

        matched, _ = DB_HOTSPOT.predicate(g, t, a)
        assert not matched

    def test_silent_when_call_count_below_minimum(self, tracker):
        """A query that dominates percentage-wise but only fires 2 times is noise."""
        g, t, a = fresh(tracker)
        # Only 2 calls total — below the minimum absolute count
        self._db_node(g, "django::SELECT tiny", call_count=2)
        g.upsert_node("django::view", "django", "django")

        matched, _ = DB_HOTSPOT.predicate(g, t, a)
        # Should not fire — call_count <= 5
        assert not matched


class TestNPlusOne:
    """
    N_PLUS_ONE fires when a DB query node is called N times per view call
    where N >= 5 — the classic ORM loop pattern.

    The rule walks reverse edges (callers) from DB nodes to find their
    view parents. Ratio = db.call_count / view.call_count.
    """

    def teardown_method(self):
        PatternRegistry._reset()

    def _build_nplusone_graph(
        self, db_calls: int = 10, view_calls: int = 1
    ) -> RuntimeGraph:
        g = RuntimeGraph()
        # View node
        g.upsert_node(
            "django::NPlusOneView",
            node_type="django",
            service="django",
            metadata={"probe": "django.view.enter"},
        )
        # Force call_count
        for _ in range(view_calls - 1):
            g.upsert_node(
                "django::NPlusOneView",
                "django",
                "django",
                metadata={"probe": "django.view.enter"},
            )

        # DB query node called N times
        for _ in range(db_calls):
            g.upsert_node(
                "django::SELECT book WHERE author_id=%s",
                node_type="django",
                service="django",
                metadata={"probe": "django.db.query"},
            )

        # Edge: view → db (structural) and reverse (callers)
        g.upsert_edge(
            source="django::NPlusOneView",
            target="django::SELECT book WHERE author_id=%s",
            edge_type="calls",
        )
        return g

    def test_fires_on_classic_n_plus_one(self, tracker):
        g, t, a = fresh(tracker)
        g = self._build_nplusone_graph(db_calls=10, view_calls=1)

        matched, evidence = N_PLUS_ONE.predicate(g, t, a)
        assert matched
        assert any(
            e["query_count"] >= 10
            for e in evidence["n_plus_one_patterns"]
        )

    def test_evidence_contains_ratio(self, tracker):
        g, t, a = fresh(tracker)
        g = self._build_nplusone_graph(db_calls=10, view_calls=1)

        matched, evidence = N_PLUS_ONE.predicate(g, t, a)
        assert matched

        patterns = evidence.get("n_plus_one_patterns", [])
        assert len(patterns) > 0

        target = next(
            (
                p
                for p in patterns
                if p["query"]
                == "django::SELECT book WHERE author_id=%s"
            ),
            None,
        )
        assert (
            target is not None
        ), "Expected N+1 query not found in evidence"
        assert target["ratio"] >= 5

    def test_silent_when_ratio_below_threshold(self, tracker):
        g = self._build_nplusone_graph(db_calls=3, view_calls=1)
        matched, _ = N_PLUS_ONE.predicate(
            g, fresh(tracker)[1], fresh(tracker)[2]
        )
        assert not matched

    def test_silent_when_no_view_caller(self, tracker):
        g, t, a = fresh(tracker)
        for _ in range(10):
            g.upsert_node(
                "django::SELECT orphan",
                node_type="django",
                service="django",
                metadata={"probe": "django.db.query"},
            )
        matched, _ = N_PLUS_ONE.predicate(g, t, a)
        assert not matched

    def test_fires_on_two_hop_n_plus_one(self, tracker):
        """N+1 where view → author_query → book_query (two hops)."""
        g, t, a = fresh(tracker)

        # view
        g.upsert_node(
            "django::NPlusOneView",
            node_type="django",
            service="django",
            metadata={"probe": "django.view.enter"},
        )

        # intermediate db query (144 calls)
        for _ in range(144):
            g.upsert_node(
                "django::SELECT author",
                node_type="django",
                service="django",
                metadata={"probe": "django.db.query"},
            )

        # leaf db query (1440 calls) — the N+1
        for _ in range(1440):
            g.upsert_node(
                "django::SELECT book WHERE author_id=%s",
                node_type="django",
                service="django",
                metadata={"probe": "django.db.query"},
            )

        # view → author (direct)
        g.upsert_edge(
            "django::NPlusOneView",
            "django::SELECT author",
            "calls",
        )
        # author → book (second hop)
        g.upsert_edge(
            "django::SELECT author",
            "django::SELECT book WHERE author_id=%s",
            "calls",
        )

        matched, evidence = N_PLUS_ONE.predicate(g, t, a)
        assert matched
        patterns = evidence["n_plus_one_patterns"]
        target = next(
            (p for p in patterns if "book" in p["query"]), None
        )
        assert target is not None
        assert target["ratio"] >= 5  # 1440/1 = 1440x

    def test_confidence_is_high(self):
        """N_PLUS_ONE confidence must be >= 0.85 — it's a near-certain bug."""
        assert N_PLUS_ONE.confidence >= 0.85


class TestWorkerImbalance:
    """
    WORKER_IMBALANCE fires when one gunicorn worker handles significantly
    more requests than others — busiest / least_busy >= 2.0.

    Worker nodes: node_type="gunicorn", metadata["probe"]="gunicorn.worker.fork"
    Edge type "handled" from worker to request node carries the load count.
    """

    def teardown_method(self):
        PatternRegistry._reset()

    def _build_imbalanced_workers(
        self,
        worker_loads: list,  # e.g. [10, 2] means worker0 handled 10, worker1 handled 2
    ) -> RuntimeGraph:
        g = RuntimeGraph()

        for i, load in enumerate(worker_loads):
            worker_id = f"gunicorn::worker-{i}"
            g.upsert_node(
                worker_id,
                node_type="gunicorn",
                service="gunicorn",
                metadata={
                    "probe": "gunicorn.worker.fork",
                    "worker_pid": 1000 + i,
                },
            )
            # Each "handled" edge represents requests routed to this worker
            request_id = "uvicorn::/api/"
            if request_id not in [n.id for n in g.all_nodes()]:
                g.upsert_node(request_id, "uvicorn", "uvicorn")
            for _ in range(load):
                g.upsert_edge(worker_id, request_id, "handled")

        return g

    # @pytest.mark.requires_rule("worker_imbalance")
    def test_fires_when_one_worker_handles_much_more(
        self, tracker
    ):
        g, t, a = fresh(tracker)
        g = self._build_imbalanced_workers([10, 2])  # ratio = 5x

        matched, evidence = WORKER_IMBALANCE.predicate(g, t, a)
        assert matched
        assert "busiest_worker" in evidence

    def test_silent_when_workers_balanced(self, tracker):
        g, t, a = fresh(tracker)
        g = self._build_imbalanced_workers(
            [5, 5, 4]
        )  # ratio ≈ 1.25 — balanced

        matched, _ = WORKER_IMBALANCE.predicate(g, t, a)
        assert not matched

    def test_silent_with_single_worker(self, tracker):
        """One worker cannot be imbalanced — need at least two."""
        g, t, a = fresh(tracker)
        g = self._build_imbalanced_workers([10])

        matched, _ = WORKER_IMBALANCE.predicate(g, t, a)
        assert not matched

    def test_evidence_names_busy_and_idle_workers(self, tracker):
        g, t, a = fresh(tracker)
        g = self._build_imbalanced_workers([20, 2])

        matched, evidence = WORKER_IMBALANCE.predicate(g, t, a)
        assert matched
        assert "busiest_worker" in evidence
        assert "ratio" in evidence
        assert evidence["ratio"] >= 2.0


class TestRequestDurationAnomaly1:

    def _make_tracker_with_history(
        self,
        tracker,
        pattern: str,
        fast_avg_ms: float,
        slow_p99_ms: float,
    ):
        """
        Build a tracker with:
          - 50+ historical completions at fast_avg_ms (stored in graph node)
          - Recent completions at slow_p99_ms (stored in tracker)
        """
        # Simulate 50 historical completions
        for i in range(50):
            tid = f"hist-{i}"
            tracker.start(
                trace_id=tid, service="django", pattern=pattern
            )
            span = tracker._active[tid]
            span.start_time -= (
                fast_avg_ms / 1000
            )  # fast completion
            tracker.complete(trace_id=tid)

        # Simulate 10 recent slow completions
        for i in range(10):
            tid = f"slow-{i}"
            tracker.start(
                trace_id=tid, service="django", pattern=pattern
            )
            span = tracker._active[tid]
            span.start_time -= slow_p99_ms / 1000
            tracker.complete(trace_id=tid)

        return tracker

    def test_fires_when_p99_exceeds_3x_avg(self, tracker):

        pattern = "django::/api/users/{id}/"
        tracker = self._make_tracker_with_history(
            tracker,
            pattern=pattern,
            fast_avg_ms=50,  # historical average
            slow_p99_ms=200,  # 4x — above threshold
        )

        g, t, a = fresh(tracker)
        # Add the graph node with historical average duration
        g.upsert_node(pattern, "fn", "django")
        node = g._nodes[pattern]
        for _ in range(50):
            node.total_duration_ns += 50_000_000  # 50ms
            node.call_count += 1
        matched, evidence = REQUEST_DURATION_ANOMALY.predicate(
            g, t, a
        )
        assert matched
        assert any(
            e["historical_n"] >= 50
            for e in evidence["anomalous_endpoints"]
        )

    def test_silent_when_latency_within_normal_range(
        self, tracker
    ):
        pattern = "django::/api/users/{id}/"
        self._make_tracker_with_history(
            tracker,
            pattern=pattern,
            fast_avg_ms=50,
            slow_p99_ms=60,  # only 1.2x — well within threshold
        )

        g, t, a = fresh(tracker)
        g.upsert_node(pattern, "fn", "django")
        node = g._nodes[pattern]
        for _ in range(50):
            node.total_duration_ns += 50_000_000
            node.call_count += 1
        matched, _ = REQUEST_DURATION_ANOMALY.predicate(g, t, a)
        assert not matched

    def test_silent_when_tracker_is_none(self, tracker):
        """Anomaly rule with tracker=None must never crash."""
        rule = REQUEST_DURATION_ANOMALY
        g, t, a = fresh(tracker)
        matched, evidence = rule.predicate(g, t, None)
        assert not matched
        assert evidence == {}


class TestUserRuleConvention:

    def teardown_method(self):
        PatternRegistry._reset()

    def test_register_function_wires_into_registry(self):
        """
        Simulate a user's *_rules.py file that exposes register(registry).
        The discovery function calls register() — verify it works.
        """
        r = PatternRegistry()
        called_with = []

        def register(registry):
            called_with.append(registry)
            registry.register(
                CausalRule(
                    name="user_custom_rule",
                    description="user defined",
                    predicate=lambda g, t: (False, {}),
                )
            )

        register(r)

        assert "user_custom_rule" in r.rule_names()
        assert called_with[0] is r


class TestRequestDurationAnomaly2:

    def _make_node(
        self,
        g: RuntimeGraph,
        pattern: str,
        service: str,
        call_count: int,
        avg_duration_ns: int,
    ) -> None:
        """Upsert a node call_count times with avg_duration_ns each call."""
        for _ in range(call_count):
            g.upsert_node(
                f"{service}::{pattern}",
                node_type=service,
                service=service,
                duration_ns=avg_duration_ns,
            )

    def _fill_tracker(
        self,
        tracker: ActiveRequestTracker,
        pattern: str,
        service: str,
        durations,
    ) -> None:
        """Directly populate completions ring buffer."""
        with tracker._lock:
            for ms in durations:
                tracker._completions[pattern].append(ms)

    def teardown_method(self):
        pass  # fresh() creates a new tracker per test, stop if you hold a ref

    def test_fires_when_p99_exceeds_threshold(self, tracker):
        g, t, a = fresh(tracker)
        self._make_node(
            g,
            "/api/slow",
            "django",
            call_count=100,
            avg_duration_ns=50 * 1_000_000,
        )
        self._fill_tracker(
            a, "/api/slow", "django", [200.0] * 15
        )

        matched, evidence = REQUEST_DURATION_ANOMALY.predicate(
            g, t, a
        )
        assert matched
        ep = evidence["anomalous_endpoints"][0]
        assert ep["pattern"] == "/api/slow"
        assert ep["ratio"] >= 3.0

    def test_silent_when_no_tracker(self, tracker):
        """No tracker — rule must not crash and must return False."""
        g, t, _ = fresh(tracker)
        matched, evidence = REQUEST_DURATION_ANOMALY.predicate(
            g, t, None
        )
        assert not matched
        assert evidence == {}

    def test_silent_when_recent_count_below_threshold(
        self, tracker
    ):
        """Fewer than 10 recent samples — not enough to trust P99."""
        g, t, a = fresh(tracker)
        self._make_node(
            g,
            "/api/slow",
            "django",
            call_count=100,
            avg_duration_ns=50 * 1_000_000,
        )
        # only 5 completions — below threshold of 10
        self._fill_tracker(a, "/api/slow", "django", [500.0] * 5)

        matched, _ = REQUEST_DURATION_ANOMALY.predicate(g, t, a)
        assert not matched

    def test_silent_when_historical_count_below_threshold(
        self, tracker
    ):
        """Fewer than 50 historical samples — baseline not trusted."""
        g, t, a = fresh(tracker)
        # only 20 historical calls on the node
        self._make_node(
            g,
            "/api/slow",
            "django",
            call_count=20,
            avg_duration_ns=50 * 1_000_000,
        )
        self._fill_tracker(
            a, "/api/slow", "django", [500.0] * 15
        )

        matched, _ = REQUEST_DURATION_ANOMALY.predicate(g, t, a)
        assert not matched

    def test_silent_when_ratio_below_threshold(self, tracker):
        """P99 only 1.8x historical — below 3x threshold."""
        g, t, a = fresh(tracker)
        self._make_node(
            g,
            "/api/view",
            "django",
            call_count=100,
            avg_duration_ns=50 * 1_000_000,
        )
        # 50ms historical, p99 ~90ms = 1.8x — below threshold
        self._fill_tracker(a, "/api/view", "django", [90.0] * 15)

        matched, _ = REQUEST_DURATION_ANOMALY.predicate(g, t, a)
        assert not matched

    def test_silent_when_historical_avg_below_1ms(self, tracker):
        """Historical avg < 1ms — too fast to be meaningful, skip."""
        g, t, a = fresh(tracker)
        # 0.5ms historical avg
        self._make_node(
            g,
            "/health",
            "django",
            call_count=100,
            avg_duration_ns=500_000,
        )
        self._fill_tracker(a, "/health", "django", [50.0] * 15)

        matched, _ = REQUEST_DURATION_ANOMALY.predicate(g, t, a)
        assert not matched

    def test_slow_in_flight_attached_when_present(self, tracker):
        g, t, a = fresh(tracker)
        self._make_node(
            g,
            "/api/slow",
            "django",
            call_count=100,
            avg_duration_ns=50 * 1_000_000,
        )
        self._fill_tracker(
            a, "/api/slow", "django", [300.0] * 15
        )

        # inject a slow in-flight span
        trace_id = str(uuid.uuid4())
        a.start(trace_id, "django", "/api/slow")
        with a._lock:
            a._active[trace_id].start_time = (
                time.monotonic() - 5.0
            )
            a._active[trace_id].probe_sequence = [
                "request.entry",
                "django.db.query",
                "django.db.query",
            ]

        matched, evidence = REQUEST_DURATION_ANOMALY.predicate(
            g, t, a
        )
        assert matched
        ep = evidence["anomalous_endpoints"][0]
        assert "slow_in_flight" in ep
        assert ep["slow_in_flight"]["trace_id"] == trace_id

    def test_multiple_anomalies_sorted_by_ratio(self, tracker):
        """Multiple anomalous endpoints are sorted by ratio descending."""
        g, t, a = fresh(tracker)
        self._make_node(
            g,
            "/api/a",
            "django",
            call_count=100,
            avg_duration_ns=50 * 1_000_000,
        )
        self._make_node(
            g,
            "/api/b",
            "django",
            call_count=100,
            avg_duration_ns=50 * 1_000_000,
        )
        # /api/a: p99=200ms = 4x historical 50ms
        self._fill_tracker(a, "/api/a", "django", [200.0] * 15)
        # /api/b: p99=500ms = 10x historical 50ms
        self._fill_tracker(a, "/api/b", "django", [500.0] * 15)

        matched, evidence = REQUEST_DURATION_ANOMALY.predicate(
            g, t, a
        )
        assert matched
        ratios = [
            ep["ratio"] for ep in evidence["anomalous_endpoints"]
        ]
        assert ratios == sorted(ratios, reverse=True)

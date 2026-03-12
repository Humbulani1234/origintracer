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
import pytest

from stacktracer.core.causal import (
    PatternRegistry, CausalRule,
    RETRY_AMPLIFICATION, NEW_SYNC_CALL, LOOP_STARVATION,
    DB_HOTSPOT, N_PLUS_ONE, WORKER_IMBALANCE, build_default_registry,
)
from stacktracer.core.runtime_graph import RuntimeGraph
from stacktracer.core.temporal import TemporalStore
from stacktracer.core.active_requests import ActiveRequestTracker


# ── Helpers ────────────────────────────────────────────────────────────────

def fresh():
    """Return a clean (graph, temporal) pair."""
    return RuntimeGraph(), TemporalStore()


# ====================================================================== #
# PatternRegistry
# ====================================================================== #

class TestPatternRegistry:

    def test_register_and_list(self):
        r = PatternRegistry()
        r.register(CausalRule(
            name="test_rule",
            description="test",
            predicate=lambda g, t: (False, {}),
        ))
        assert "test_rule" in r.rule_names()

    def test_evaluate_calls_predicate(self):
        fired = []
        def pred(g, t):
            fired.append(True)
            return True, {"key": "value"}

        r = PatternRegistry()
        r.register(CausalRule(name="r", description="d", predicate=pred))
        g, t = fresh()
        matches = r.evaluate(g, t)
        assert len(matches) == 1
        assert matches[0].evidence == {"key": "value"}
        assert fired

    def test_tag_filtering_excludes_unmatched(self):
        r = PatternRegistry()
        r.register(CausalRule(name="a", description="", predicate=lambda g,t: (True, {}), tags=["latency"]))
        r.register(CausalRule(name="b", description="", predicate=lambda g,t: (True, {}), tags=["db"]))
        g, t = fresh()
        matches = r.evaluate(g, t, tags=["latency"])
        assert all(m.rule_name == "a" for m in matches)

    def test_broken_rule_does_not_crash_evaluate(self):
        def exploding(g, t):
            raise RuntimeError("rule is broken")

        r = PatternRegistry()
        r.register(CausalRule(name="broken", description="", predicate=exploding))
        g, t = fresh()
        matches = r.evaluate(g, t)
        # Returns a zero-confidence match explaining the error rather than raising
        assert len(matches) == 1
        assert matches[0].confidence == 0.0

    def test_results_sorted_by_confidence_descending(self):
        r = PatternRegistry()
        r.register(CausalRule(name="low",  description="", predicate=lambda g,t: (True, {}), confidence=0.3))
        r.register(CausalRule(name="high", description="", predicate=lambda g,t: (True, {}), confidence=0.9))
        g, t = fresh()
        matches = r.evaluate(g, t)
        assert matches[0].confidence >= matches[1].confidence

    def test_build_default_registry_includes_all_rules(self):
        r = build_default_registry()
        names = r.rule_names()
        assert "n_plus_one"                    in names
        assert "new_sync_call_after_deployment" in names
        assert "asyncio_event_loop_starvation"  in names
        assert "worker_imbalance"              in names
        assert "retry_amplification"           in names
        assert "db_query_hotspot"              in names
        assert "request_duration_anomaly"      in names

    def test_build_default_registry_order_confidence_descending(self):
        """
        Rules must be registered highest-confidence first so the REPL
        shows the most actionable match at the top.
        N_PLUS_ONE(0.90) > NEW_SYNC_CALL(0.85) > LOOP_STARVATION(0.80)
        > WORKER_IMBALANCE(0.80) > RETRY_AMPLIFICATION(0.75) > DB_HOTSPOT(0.70)
        """
        r = build_default_registry()
        names = r.rule_names()
        assert names.index("n_plus_one") < names.index("db_query_hotspot")
        assert names.index("new_sync_call_after_deployment") < names.index("retry_amplification")

    def test_build_default_registry_without_tracker_is_safe(self):
        """tracker=None means anomaly rule registers but never fires — no crash."""
        r = build_default_registry(tracker=None)
        g, t = fresh()
        matches = r.evaluate(g, t)
        anomalies = [m for m in matches if m.rule_name == "request_duration_anomaly"]
        assert anomalies == []


# ====================================================================== #
# retry_amplification rule
# ====================================================================== #

class TestRetryAmplification:

    def test_fires_when_retry_count_high(self):
        g, t = fresh()
        g.upsert_node("django::view",   "fn", "django")
        g.upsert_node("redis::get",     "fn", "redis")
        g.upsert_edge("django::view", "redis::get", "calls")
        # Inject retry metadata onto the edge
        edge_key = list(g._edge_index.keys())[0]
        g._edge_index[edge_key].metadata["retries"] = 10

        r = PatternRegistry()
        r.register(RETRY_AMPLIFICATION)
        matches = r.evaluate(g, t)
        assert any(m.rule_name == "retry_amplification" for m in matches)

    def test_silent_when_no_retries(self):
        g, t = fresh()
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")
        g.upsert_edge("A", "B", "calls")
        r = PatternRegistry()
        r.register(RETRY_AMPLIFICATION)
        assert r.evaluate(g, t) == []


# ====================================================================== #
# new_sync_call_after_deployment rule
# ====================================================================== #

class TestNewSyncCallAfterDeployment:

    def test_fires_when_new_edge_after_deployment(self):
        g, t = fresh()
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")

        # Capture baseline before deployment
        t.capture(g.snapshot())
        t.mark_event("deployment")
        time.sleep(0.01)

        # New edge appears after deployment
        g.upsert_edge("A", "B", "calls")
        t.capture(g.snapshot())

        r = PatternRegistry()
        r.register(NEW_SYNC_CALL)
        matches = r.evaluate(g, t)
        assert any(m.rule_name == "new_sync_call_after_deployment" for m in matches)

    def test_silent_without_deployment_marker(self):
        g, t = fresh()
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")
        g.upsert_edge("A", "B", "calls")

        r = PatternRegistry()
        r.register(NEW_SYNC_CALL)
        # No deployment marker in temporal store
        assert r.evaluate(g, t) == []


# ====================================================================== #
# asyncio_event_loop_starvation rule
# ====================================================================== #

class TestLoopStarvation:

    def test_fires_when_loop_tick_avg_high(self):
        g, t = fresh()
        g.upsert_node("asyncio::loop.tick", "asyncio", "asyncio")
        node = g._nodes["asyncio::loop.tick"]
        # Simulate 20ms average tick duration (threshold is 10ms)
        node.total_duration_ns = 20_000_000
        node.call_count = 1
        node.node_type = "asyncio"

        r = PatternRegistry()
        r.register(LOOP_STARVATION)
        matches = r.evaluate(g, t)
        assert any(m.rule_name == "asyncio_event_loop_starvation" for m in matches)

    def test_silent_when_loop_tick_fast(self):
        g, t = fresh()
        g.upsert_node("asyncio::loop.tick", "asyncio", "asyncio")
        node = g._nodes["asyncio::loop.tick"]
        node.total_duration_ns = 1_000_000   # 1ms — well below 10ms threshold
        node.call_count = 1
        node.node_type = "asyncio"

        r = PatternRegistry()
        r.register(LOOP_STARVATION)
        assert r.evaluate(g, t) == []


# ====================================================================== #
# db_query_hotspot rule
# ====================================================================== #

class TestDbQueryHotspot:
    """
    _is_db_node() checks metadata["probe"].endswith(".db.query"),
    NOT node_type == "db". Tests must set the probe key in metadata.
    Threshold: a single query node call_count > 30% of total and > 5.
    """

    def _db_node(self, g: RuntimeGraph, node_id: str, call_count: int) -> None:
        """Add a DB query node with correct probe metadata."""
        for _ in range(call_count):
            g.upsert_node(
                node_id,
                node_type = "django",
                service   = "django",
                metadata  = {"probe": "django.db.query"},
            )

    def test_fires_when_single_query_dominates(self):
        g, t = fresh()
        # 10 book queries + 1 author query + 1 view node
        # book query = 10 / (10+1+1) = 83% — well above 30% threshold
        self._db_node(g, "django::SELECT book WHERE author_id=%s", call_count=10)
        self._db_node(g, "django::SELECT author",                  call_count=1)
        g.upsert_node("django::NPlusOneView", "django", "django")  # no probe metadata

        r = PatternRegistry()
        r.register(DB_HOTSPOT)
        matches = r.evaluate(g, t)
        assert any(m.rule_name == "db_query_hotspot" for m in matches)
        # Evidence must name the hotspot query
        evidence = next(m for m in matches if m.rule_name == "db_query_hotspot").evidence
        assert any(
            "SELECT book" in q["node"]
            for q in evidence.get("hotspot_queries", [])
        )

    def test_silent_when_calls_evenly_distributed(self):
        g, t = fresh()
        # Ten DB queries with equal counts — none exceeds 30%
        for i in range(10):
            self._db_node(g, f"django::SELECT table_{i}", call_count=2)

        r = PatternRegistry()
        r.register(DB_HOTSPOT)
        assert r.evaluate(g, t) == []

    def test_silent_when_call_count_below_minimum(self):
        """A query that dominates percentage-wise but only fires 2 times is noise."""
        g, t = fresh()
        # Only 2 calls total — below the minimum absolute count
        self._db_node(g, "django::SELECT tiny", call_count=2)
        g.upsert_node("django::view", "django", "django")

        r = PatternRegistry()
        r.register(DB_HOTSPOT)
        # Should not fire — call_count <= 5
        assert r.evaluate(g, t) == []


# ====================================================================== #
# n_plus_one rule
# ====================================================================== #

class TestNPlusOne:
    """
    N_PLUS_ONE fires when a DB query node is called N times per view call
    where N >= 5 — the classic ORM loop pattern.

    The rule walks reverse edges (callers) from DB nodes to find their
    view parents. Ratio = db.call_count / view.call_count.
    """

    def _build_nplusone_graph(self, db_calls: int = 10, view_calls: int = 1) -> RuntimeGraph:
        g = RuntimeGraph()
        # View node
        g.upsert_node(
            "django::NPlusOneView",
            node_type = "django",
            service   = "django",
            metadata  = {"probe": "django.view.enter"},
        )
        # Force call_count
        for _ in range(view_calls - 1):
            g.upsert_node("django::NPlusOneView", "django", "django",
                          metadata={"probe": "django.view.enter"})

        # DB query node called N times
        for _ in range(db_calls):
            g.upsert_node(
                "django::SELECT book WHERE author_id=%s",
                node_type = "django",
                service   = "django",
                metadata  = {"probe": "django.db.query"},
            )

        # Edge: view → db (structural) and reverse (callers)
        g.upsert_edge(
            source    = "django::NPlusOneView",
            target    = "django::SELECT book WHERE author_id=%s",
            edge_type = "calls",
        )
        return g

    def test_fires_on_classic_n_plus_one(self):
        g, t = fresh()
        g = self._build_nplusone_graph(db_calls=10, view_calls=1)

        r = PatternRegistry()
        r.register(N_PLUS_ONE)
        matches = r.evaluate(g, t)
        assert any(m.rule_name == "n_plus_one" for m in matches)

    def test_evidence_contains_ratio(self):
        g, t = fresh()
        g = self._build_nplusone_graph(db_calls=10, view_calls=1)

        r = PatternRegistry()
        r.register(N_PLUS_ONE)
        matches = r.evaluate(g, t)
        m = next(m for m in matches if m.rule_name == "n_plus_one")
        # Ratio should be reported
        assert m.evidence.get("ratio", 0) >= 5

    def test_silent_when_ratio_below_threshold(self):
        """3 DB calls per view is noisy but not N+1 — threshold is 5."""
        g, t = fresh()
        g = self._build_nplusone_graph(db_calls=3, view_calls=1)

        r = PatternRegistry()
        r.register(N_PLUS_ONE)
        assert r.evaluate(g, t) == []

    def test_silent_when_no_view_caller(self):
        """DB query with no view parent edge should not fire."""
        g, t = fresh()
        for _ in range(10):
            g.upsert_node(
                "django::SELECT orphan",
                node_type = "django",
                service   = "django",
                metadata  = {"probe": "django.db.query"},
            )
        r = PatternRegistry()
        r.register(N_PLUS_ONE)
        assert r.evaluate(g, t) == []

    def test_confidence_is_high(self):
        """N_PLUS_ONE confidence must be >= 0.85 — it's a near-certain bug."""
        assert N_PLUS_ONE.confidence >= 0.85


# ====================================================================== #
# worker_imbalance rule
# ====================================================================== #

class TestWorkerImbalance:
    """
    WORKER_IMBALANCE fires when one gunicorn worker handles significantly
    more requests than others — busiest / least_busy >= 2.0.

    Worker nodes: node_type="gunicorn", metadata["probe"]="gunicorn.worker.fork"
    Edge type "handled" from worker to request node carries the load count.
    """

    def _build_imbalanced_workers(
        self,
        worker_loads: list,   # e.g. [10, 2] means worker0 handled 10, worker1 handled 2
    ) -> RuntimeGraph:
        g = RuntimeGraph()

        for i, load in enumerate(worker_loads):
            worker_id = f"gunicorn::worker-{i}"
            g.upsert_node(
                worker_id,
                node_type = "gunicorn",
                service   = "gunicorn",
                metadata  = {"probe": "gunicorn.worker.fork", "worker_pid": 1000 + i},
            )
            # Each "handled" edge represents requests routed to this worker
            request_id = f"uvicorn::/api/"
            if request_id not in [n.id for n in g.all_nodes()]:
                g.upsert_node(request_id, "uvicorn", "uvicorn")
            for _ in range(load):
                g.upsert_edge(worker_id, request_id, "handled")

        return g

    def test_fires_when_one_worker_handles_much_more(self):
        g, t = fresh()
        g = self._build_imbalanced_workers([10, 2])   # ratio = 5x

        r = PatternRegistry()
        r.register(WORKER_IMBALANCE)
        matches = r.evaluate(g, t)
        assert any(m.rule_name == "worker_imbalance" for m in matches)

    def test_silent_when_workers_balanced(self):
        g, t = fresh()
        g = self._build_imbalanced_workers([5, 5, 4])   # ratio ≈ 1.25 — balanced

        r = PatternRegistry()
        r.register(WORKER_IMBALANCE)
        assert r.evaluate(g, t) == []

    def test_silent_with_single_worker(self):
        """One worker cannot be imbalanced — need at least two."""
        g, t = fresh()
        g = self._build_imbalanced_workers([10])

        r = PatternRegistry()
        r.register(WORKER_IMBALANCE)
        assert r.evaluate(g, t) == []

    def test_evidence_names_busy_and_idle_workers(self):
        g, t = fresh()
        g = self._build_imbalanced_workers([20, 2])

        r = PatternRegistry()
        r.register(WORKER_IMBALANCE)
        matches = r.evaluate(g, t)
        m = next(m for m in matches if m.rule_name == "worker_imbalance")
        assert "busiest_worker" in m.evidence
        assert "ratio"          in m.evidence
        assert m.evidence["ratio"] >= 2.0


# ====================================================================== #
# request_duration_anomaly rule
# ====================================================================== #

class TestRequestDurationAnomaly:

    def _make_tracker_with_history(self, pattern: str, fast_avg_ms: float, slow_p99_ms: float):
        """
        Build a tracker with:
          - 50+ historical completions at fast_avg_ms (stored in graph node)
          - Recent completions at slow_p99_ms (stored in tracker)
        """
        from stacktracer.core.active_requests import ActiveRequestTracker
        tracker = ActiveRequestTracker()

        # Simulate 50 historical completions
        for i in range(50):
            tid = f"hist-{i}"
            tracker.start(trace_id=tid, service="django", pattern=pattern)
            span = tracker._active[tid]
            span.start_time -= fast_avg_ms / 1000   # fast completion
            tracker.complete(trace_id=tid)

        # Simulate 10 recent slow completions
        for i in range(10):
            tid = f"slow-{i}"
            tracker.start(trace_id=tid, service="django", pattern=pattern)
            span = tracker._active[tid]
            span.start_time -= slow_p99_ms / 1000
            tracker.complete(trace_id=tid)

        return tracker

    def test_fires_when_p99_exceeds_3x_avg(self):
        from stacktracer.core.causal import _make_request_duration_anomaly

        pattern = "django::/api/users/{id}/"
        tracker = self._make_tracker_with_history(
            pattern      = pattern,
            fast_avg_ms  = 50,    # historical average
            slow_p99_ms  = 200,   # 4x — above threshold
        )

        g, t = fresh()
        # Add the graph node with historical average duration
        g.upsert_node(pattern, "fn", "django")
        node = g._nodes[pattern]
        for _ in range(50):
            node.total_duration_ns += 50_000_000   # 50ms
            node.call_count += 1

        rule = _make_request_duration_anomaly(tracker)
        r = PatternRegistry()
        r.register(rule)
        matches = r.evaluate(g, t)
        assert any(m.rule_name == "request_duration_anomaly" for m in matches)

    def test_silent_when_latency_within_normal_range(self):
        from stacktracer.core.causal import _make_request_duration_anomaly

        pattern = "django::/api/users/{id}/"
        tracker = self._make_tracker_with_history(
            pattern     = pattern,
            fast_avg_ms = 50,
            slow_p99_ms = 60,   # only 1.2x — well within threshold
        )

        g, t = fresh()
        g.upsert_node(pattern, "fn", "django")
        node = g._nodes[pattern]
        for _ in range(50):
            node.total_duration_ns += 50_000_000
            node.call_count += 1

        rule = _make_request_duration_anomaly(tracker)
        r = PatternRegistry()
        r.register(rule)
        assert r.evaluate(g, t) == []

    def test_silent_when_tracker_is_none(self):
        """Anomaly rule with tracker=None must never crash."""
        from stacktracer.core.causal import _make_request_duration_anomaly
        rule = _make_request_duration_anomaly(None)
        g, t = fresh()
        matched, evidence = rule.predicate(g, t)
        assert matched is False
        assert evidence == {}


# ====================================================================== #
# User rule discovery convention
# ====================================================================== #

class TestUserRuleConvention:

    def test_register_function_wires_into_registry(self):
        """
        Simulate a user's *_rules.py file that exposes register(registry).
        The discovery function calls register() — verify it works.
        """
        r = PatternRegistry()
        called_with = []

        def register(registry):
            called_with.append(registry)
            registry.register(CausalRule(
                name="user_custom_rule",
                description="user defined",
                predicate=lambda g, t: (False, {}),
            ))

        register(r)

        assert "user_custom_rule" in r.rule_names()
        assert called_with[0] is r
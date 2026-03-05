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
    DB_HOTSPOT, build_default_registry,
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
        assert "retry_amplification"           in names
        assert "new_sync_call_after_deployment" in names
        assert "asyncio_event_loop_starvation"  in names
        assert "db_query_hotspot"               in names
        assert "request_duration_anomaly"       in names

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

    def test_fires_when_single_query_dominates(self):
        g, t = fresh()
        # One DB query accounts for 40% of all calls
        for _ in range(40):
            g.upsert_node("postgres::SELECT users", "db", "postgres")
        for _ in range(60):
            g.upsert_node("django::view", "fn", "django")

        r = PatternRegistry()
        r.register(DB_HOTSPOT)
        matches = r.evaluate(g, t)
        assert any(m.rule_name == "db_query_hotspot" for m in matches)

    def test_silent_when_calls_evenly_distributed(self):
        g, t = fresh()
        # Ten queries with equal call counts — none exceeds 30%
        for i in range(10):
            for _ in range(5):
                g.upsert_node(f"postgres::query_{i}", "db", "postgres")

        r = PatternRegistry()
        r.register(DB_HOTSPOT)
        assert r.evaluate(g, t) == []


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
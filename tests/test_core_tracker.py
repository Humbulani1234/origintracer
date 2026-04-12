"""
tests/test_core_tracker.py

Tests for:
    ActiveRequestTracker  — in-flight request tracking and latency comparison
    SemanticLayer         — human label → graph node resolution
"""

from __future__ import annotations

import time

import pytest

from origintracer.core.active_requests import (
    ActiveRequestTracker,
    RequestSpan,
)
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.semantic import (
    SemanticAlias,
    SemanticLayer,
    load_from_dict,
)


class TestActiveRequestTracker:

    def setup_method(self):
        self.t = ActiveRequestTracker(ttl_s=5.0, max_size=100)

    def test_start_creates_span(self):
        self.t.start(
            trace_id="abc",
            service="django",
            pattern="/api/users/",
        )
        assert "abc" in self.t._active

    def test_complete_removes_from_active(self):
        self.t.start(
            trace_id="abc",
            service="django",
            pattern="/api/users/",
        )
        span = self.t.complete(trace_id="abc")
        assert "abc" not in self.t._active
        assert span is not None
        assert span.is_complete

    def test_complete_returns_duration(self):
        self.t.start(
            trace_id="abc",
            service="django",
            pattern="/api/users/",
        )
        # In test_core_tracker.py
        time.sleep(0.02)
        span = self.t.complete(trace_id="abc")
        # Use a lower bound to account for OS scheduling jitter
        assert span.duration_ms >= 14.5

    def test_complete_unknown_trace_returns_none(self):
        assert self.t.complete("nonexistent") is None

    def test_event_appends_probe_to_sequence(self):
        self.t.start(
            trace_id="abc", service="django", pattern="/api/"
        )
        self.t.event(trace_id="abc", probe="db.query.start")
        self.t.event(trace_id="abc", probe="db.query.end")
        span = self.t._active["abc"]
        assert span.probe_sequence == [
            "db.query.start",
            "db.query.end",
        ]

    def test_event_ignores_unknown_trace(self):
        """event() on unknown trace_id must not raise."""
        self.t.event(
            trace_id="nonexistent", probe="db.query.start"
        )

    def test_active_count(self):
        self.t.start("a", "django", "/api/a/")
        self.t.start("b", "django", "/api/b/")
        assert self.t.active_count() == 2

    def test_active_count_by_service(self):
        self.t.start("a", "django", "/api/")
        self.t.start("b", "celery", "process_order")
        assert self.t.active_count(service="django") == 1
        assert self.t.active_count(service="celery") == 1

    def test_slow_in_flight_filters_by_threshold(self):
        self.t.start("fast", "django", "/api/")
        self.t.start("slow", "django", "/api/")
        # Backdate slow entry to simulate 500ms in-flight
        self.t._active["slow"].start_time = (
            time.monotonic() - 0.5
        )
        slow = self.t.slow_in_flight(threshold_ms=200)
        ids = [s.trace_id for s in slow]
        assert "slow" in ids
        assert "fast" not in ids

    def test_all_patterns_summary_after_completions(self):
        for i in range(15):
            self.t.start(f"t{i}", "django", "/api/orders/")
            self.t._active[f"t{i}"].start_time = (
                time.monotonic() - 0.1
            )
            self.t.complete(f"t{i}")
        summary = self.t.all_patterns_summary()
        assert "/api/orders/" in summary
        assert summary["/api/orders/"]["count"] == 15

    def test_ttl_eviction_removes_stale_entries(self):
        tracker = ActiveRequestTracker(ttl_s=0.05, max_size=100)
        tracker.start("stale", "django", "/api/")
        # Backdate to trigger TTL
        tracker._active["stale"].last_event = (
            time.monotonic() - 1.0
        )
        tracker._evict()
        assert "stale" not in tracker._active

    def test_max_size_cap_evicts_oldest(self):
        tracker = ActiveRequestTracker(ttl_s=999, max_size=5)
        for i in range(10):
            tracker.start(f"t{i}", "django", "/api/")
        assert len(tracker._active) <= 5

    def test_start_complete_is_thread_safe(self):
        """Concurrent start/complete from multiple threads must not corrupt state."""
        import threading

        errors = []
        tracker = ActiveRequestTracker(
            ttl_s=30.0, max_size=10_000
        )

        def worker(thread_id):
            try:
                for i in range(100):
                    tid = f"t-{thread_id}-{i}"
                    tracker.start(tid, "django", "/api/")
                    tracker.event(tid, "db.query.start")
                    tracker.complete(tid)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=worker, args=(i,))
            for i in range(5)
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert not errors


# ====================================================================== #
# SemanticLayer
# ====================================================================== #


class TestSemanticLayer:

    def setup_method(self):
        self.layer = SemanticLayer()
        self.layer.register(
            SemanticAlias(
                label="export",
                description="Export pipeline",
                node_patterns=[
                    "django::handle_export",
                    r"django::flags_client\..*",
                ],
                services=["exporter"],
            )
        )
        self.layer.register(
            SemanticAlias(
                label="db",
                description="Database layer",
                services=["postgres", "redis"],
                node_patterns=[],
            )
        )

    def _make_graph(self):
        g = RuntimeGraph()
        g.upsert_node("django::handle_export", "fn", "django")
        g.upsert_node(
            "django::flags_client.call", "fn", "django"
        )
        g.upsert_node("exporter::publish", "fn", "exporter")
        g.upsert_node(
            "postgres::SELECT orders", "db", "postgres"
        )
        g.upsert_node("django::unrelated_view", "fn", "django")
        return g

    def test_resolve_by_exact_node_pattern(self):
        g = self._make_graph()
        nodes = self.layer.resolve_nodes("export", g)
        assert "django::handle_export" in nodes

    def test_resolve_by_regex_node_pattern(self):
        g = self._make_graph()
        nodes = self.layer.resolve_nodes("export", g)
        assert "django::flags_client.call" in nodes

    def test_resolve_by_service(self):
        g = self._make_graph()
        nodes = self.layer.resolve_nodes("export", g)
        assert "exporter::publish" in nodes

    def test_resolve_db_by_service(self):
        g = self._make_graph()
        nodes = self.layer.resolve_nodes("db", g)
        assert "postgres::SELECT orders" in nodes

    def test_unrelated_node_excluded(self):
        g = self._make_graph()
        nodes = self.layer.resolve_nodes("export", g)
        assert "django::unrelated_view" not in nodes

    def test_unknown_label_returns_empty_set(self):
        g = self._make_graph()
        assert (
            self.layer.resolve_nodes("nonexistent", g) == set()
        )

    def test_case_insensitive_label_lookup(self):
        g = self._make_graph()
        nodes_lower = self.layer.resolve_nodes("export", g)
        nodes_upper = self.layer.resolve_nodes("EXPORT", g)
        assert nodes_lower == nodes_upper

    def test_load_from_dict(self):
        data = [
            {
                "label": "auth",
                "description": "Auth service",
                "node_patterns": ["django::authenticate"],
                "services": ["auth"],
                "tags": ["security"],
            }
        ]
        layer = load_from_dict(data)
        g = RuntimeGraph()
        g.upsert_node("django::authenticate", "fn", "django")
        nodes = layer.resolve_nodes("auth", g)
        assert "django::authenticate" in nodes

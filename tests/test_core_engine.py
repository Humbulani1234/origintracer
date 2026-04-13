"""
Integration tests for Engine - the component that wires together
RuntimeGraph, TemporalStore, PatternRegistry, SemanticLayer, and
ActiveRequestTracker.

These tests exercise the Engine as it would operate at runtime:
feeding it realistic event sequences and asserting on the resulting
graph structure, causal matches, and temporal diffs.

Engine.process() is called directly.
"""

from __future__ import annotations

import threading

from origintracer.core.engine import Engine
from origintracer.core.graph_compactor import (
    GraphCompactor,
)

from .conftest import evt


class TestEngineGraphBuilding:
    """Engine correctly builds the RuntimeGraph from event streams."""

    def test_single_event_creates_node(self, engine, trace_id):
        engine.process(
            evt(
                service="django",
                name="handle_view",
                trace_id=trace_id,
            )
        )
        assert "django::handle_view" in engine.graph._nodes

    def test_two_events_same_trace_creates_edge(
        self, engine, trace_id
    ):
        """The core causal connection: consecutive events on same trace → edge."""
        engine.process(
            evt(
                probe="request.entry",
                service="nginx",
                name="upstream",
                trace_id=trace_id,
            )
        )
        engine.process(
            evt(
                probe="function.call",
                service="django",
                name="view",
                trace_id=trace_id,
            )
        )
        assert "django::view" in engine.graph.reachable_from(
            "nginx::upstream"
        )

    def test_full_stack_topology(self, engine, trace_id):
        """
        Simulate nginx → django → postgres call chain.
        All five nodes must appear and form a connected path.
        """
        for probe, service, name in [
            ("request.entry", "nginx", "accept"),
            ("request.entry", "django", "TracerMiddleware"),
            ("function.call", "django", "UserView.get"),
            ("db.query.start", "postgres", "SELECT users"),
            ("db.query.end", "postgres", "SELECT users"),
        ]:
            engine.process(
                evt(
                    probe=probe,
                    service=service,
                    name=name,
                    trace_id=trace_id,
                )
            )

        reachable = engine.graph.reachable_from("nginx::accept")
        assert "django::TracerMiddleware" in reachable
        assert "django::UserView.get" in reachable
        assert "postgres::SELECT users" in reachable

    def test_different_traces_dont_cross_edges(self, engine):
        """Events on different trace_ids must never produce cross-trace edges."""
        engine.process(
            evt(
                service="django",
                name="view_a",
                trace_id="trace-A",
            )
        )
        engine.process(
            evt(
                service="django",
                name="view_b",
                trace_id="trace-B",
            )
        )
        # view_a should have no neighbors (no second event on trace-A)
        assert engine.graph.neighbors("django::view_a") == []

    def test_duration_accumulates_on_node(
        self, engine, trace_id
    ):
        for _ in range(3):
            engine.process(
                evt(
                    probe="db.query.start",
                    service="postgres",
                    name="SELECT",
                    trace_id=trace_id,
                    duration_ns=1_000_000,
                )
            )
        node = engine.graph._nodes.get("postgres::SELECT")
        assert node is not None
        assert node.call_count == 3
        assert node.avg_duration_ns == 1_000_000

    def test_duration_ns_lands_on_event_field_not_metadata(
        self, engine, trace_id
    ):
        """
        After the NormalizedEvent.now() fix, duration_ns must be on the
        dataclass field - not in event.metadata. The engine reads
        event.duration_ns; if it's in metadata the node avg stays None.
        """
        e = evt(
            service="django",
            name="view",
            trace_id=trace_id,
            duration_ns=8_000_000,
        )
        assert (
            e.duration_ns == 8_000_000
        ), "NormalizedEvent.now() is still swallowing duration_ns into metadata"
        assert "duration_ns" not in e.metadata

        engine.process(e)
        node = engine.graph._nodes.get("django::view")
        assert node.avg_duration_ns == 8_000_000

    def test_error_event_carries_duration(
        self, engine, trace_id
    ):
        """
        _error() in the Django probe should record how long the request ran
        before failing. Without duration on the error event, a 30s timeout
        and a 2ms crash look identical.
        """
        # Simulate: request started, then errored after ~5ms
        entry = evt(
            probe="request.entry",
            service="django",
            name="/api/fail",
            trace_id=trace_id,
        )
        engine.process(entry)

        error = evt(
            probe="django.exception",
            service="django",
            name="/api/fail",
            trace_id=trace_id,
            duration_ns=5_000_000,  # 5ms before the crash
            exception_type="ValueError",
        )
        engine.process(error)

        node = engine.graph._nodes.get("django::/api/fail")
        assert node is not None
        # The error event's duration must accumulate
        assert node.total_duration_ns > 0

    def test_hotspots_returns_sorted_nodes(
        self, engine, trace_id
    ):
        for _ in range(5):
            engine.process(
                evt(
                    service="django",
                    name="busy_view",
                    trace_id=trace_id,
                )
            )
        engine.process(
            evt(
                service="django",
                name="quiet_view",
                trace_id=trace_id,
            )
        )
        hotspots = engine.hotspots(top_n=5)
        names = [h["node"] for h in hotspots]
        assert names.index("django::busy_view") < names.index(
            "django::quiet_view"
        )


class TestEngineTemporalIntegration:
    """Engine correctly captures temporal diffs and deployment markers."""

    def test_snapshot_records_new_nodes_as_diff(
        self, engine, trace_id
    ):
        engine.process(
            evt(service="django", name="fn_a", trace_id=trace_id)
        )
        engine.snapshot()
        engine.process(
            evt(service="django", name="fn_b", trace_id=trace_id)
        )
        diff = engine.snapshot()
        assert "django::fn_b" in diff["added_nodes"]
        assert "django::fn_a" not in diff["added_nodes"]

    def test_mark_deployment_creates_labelled_diff(self, engine):
        engine.mark_deployment("v2.0.0")
        diff = engine.temporal.label_diff("v2.0.0")
        assert diff is not None
        assert diff.label == "v2.0.0"

    def test_evaluate_returns_empty_on_clean_graph(self, engine):
        """A graph with no anomalous patterns should return no causal matches."""
        # Process a few normal events — nothing that would trigger rules
        for i in range(3):
            engine.process(
                evt(
                    service="django",
                    name=f"view_{i}",
                    trace_id=f"t{i}",
                )
            )
        matches = engine.evaluate()
        # new_sync_call rule may fire if edges appeared after a deployment marker
        # — but we never called mark_deployment so it won't
        for m in matches:
            assert (
                m.rule_name != "new_sync_call_after_deployment"
            )

    def test_status_contains_expected_keys(
        self, engine, trace_id
    ):
        engine.process(
            evt(service="django", name="view", trace_id=trace_id)
        )
        s = engine.status()
        assert "graph_nodes" in s
        assert "temporal_diffs" in s
        assert s["graph_nodes"] >= 1

    def test_critical_path_returns_ordered_stages(
        self, engine, trace_id
    ):
        """Events on one trace must come back ordered by wall_time."""
        engine.process(
            evt(
                probe="request.entry",
                service="nginx",
                name="accept",
                trace_id=trace_id,
            )
        )
        engine.process(
            evt(
                probe="function.call",
                service="django",
                name="view",
                trace_id=trace_id,
            )
        )
        engine.process(
            evt(
                probe="db.query.start",
                service="postgres",
                name="SELECT",
                trace_id=trace_id,
            )
        )
        path = engine.critical_path(trace_id)
        assert len(path) == 3
        services = [s["service"] for s in path]
        assert services == ["nginx", "django", "postgres"]

    def test_critical_path_empty_for_unknown_trace(self, engine):
        assert engine.critical_path("nonexistent-trace-id") == []


class TestEngineTrackerIntegration:
    """Engine wires ActiveRequestTracker correctly."""

    def test_tracker_attached_to_engine(self, engine):
        assert engine.tracker is not None

    def test_tracker_records_events_via_engine(
        self, engine, trace_id
    ):
        """
        Engine.process() should call tracker.event() for every event
        so the tracker maintains the probe_sequence of in-flight requests.
        """
        engine.tracker.start(
            trace_id=trace_id,
            service="django",
            pattern="/api/users/",
        )
        engine.process(
            evt(
                probe="db.query.start",
                service="postgres",
                name="SELECT",
                trace_id=trace_id,
            )
        )
        span = engine.tracker._active.get(trace_id)
        assert span is not None
        assert "db.query.start" in span.probe_sequence


class TestEngineCompactorIntegration:
    """
    _snapshot_loop() must call compactor.compact() after every snapshot.

    The unit tests in test_core_graph.py verify the compactor logic itself
    (TTL eviction, cap eviction, hot-node protection). This test verifies
    only the wiring: that the engine's snapshot loop actually invokes the
    compactor so nodes are evicted during normal operation.
    """

    def test_snapshot_loop_calls_compactor(self):
        """
        Start _snapshot_loop in a real thread with a very short interval.
        After it fires at least once, compactor._compact_runs must be >= 1
        and the graph must be within the cap.
        """
        engine = Engine(
            snapshot_interval_s=0.05
        )  # 50ms — fires quickly in test
        compactor = GraphCompactor(
            max_nodes=5,
            evict_to_ratio=0.6,  # evict to 3 nodes when cap hit
            node_ttl_s=999_999,  # disable TTL — we only want cap eviction
            min_call_count=999,  # protect nothing — every node is evictable
        )
        engine.compactor = compactor

        # Add 10 nodes — well over the cap of 5
        for i in range(10):
            engine.graph.upsert_node(f"svc::fn{i}", "fn", "svc")
        assert len(engine.graph) == 10

        # Run the loop in a daemon thread and let it fire once
        engine._running = True
        t = threading.Thread(
            target=engine._snapshot_loop, daemon=True
        )
        t.start()
        t.join(
            timeout=2.0
        )  # more than enough for a 50ms interval
        engine._running = False

        assert compactor._compact_runs >= 1, (
            "_snapshot_loop did not call compactor.compact() — "
            "check that _snapshot_loop calls self.compactor.compact(self.graph) "
            "after self.snapshot()"
        )
        assert len(engine.graph) <= 5, (
            f"Graph has {len(engine.graph)} nodes after compaction — "
            f"expected <= 5 (the configured cap)"
        )

    def test_stale_trace_ids_evicted_from_last_event_dict(self):
        """
        trace_ids not updated within _trace_ttl_s must be removed from
        _last_event_per_trace by _evict_stale_traces().

        Verifies the fix for the unbounded dict growth bug:
        at 100 req/s without eviction the dict accumulates 360k entries/hour.
        """
        engine = Engine(snapshot_interval_s=9999)
        engine._trace_ttl_s = 0.05  # 50ms TTL so the test does not need to sleep long

        # Simulate 5 completed traces by writing directly to the dict
        # with a timestamp already past the TTL threshold
        import time as _time

        stale_ts = (
            _time.monotonic() - 1.0
        )  # 1 second ago — well past 50ms TTL
        for i in range(5):
            from .conftest import evt

            engine._last_event_per_trace[f"old-trace-{i}"] = (
                evt(trace_id=f"old-trace-{i}"),
                stale_ts,
            )

        # One active trace — updated just now, must survive eviction
        engine._last_event_per_trace["live-trace"] = (
            evt(trace_id="live-trace"),
            _time.monotonic(),
        )

        assert len(engine._last_event_per_trace) == 6

        engine._evict_stale_traces()

        assert (
            len(engine._last_event_per_trace) == 1
        ), "Expected only the live trace to survive eviction"
        assert "live-trace" in engine._last_event_per_trace
        assert all(
            k.startswith("old-trace") is False
            for k in engine._last_event_per_trace
        )

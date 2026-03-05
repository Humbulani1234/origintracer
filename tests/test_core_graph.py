"""
tests/test_core_graph.py

Tests for the four graph-layer components:
    RuntimeGraph     — topology store and query engine
    TemporalStore    — diff capture and deployment correlation
    GraphNormalizer  — high-cardinality name collapsing
    GraphCompactor   — memory-bounded eviction

Tests here are a mix of unit (component in isolation) and integration
(two components working together, e.g. Normalizer feeding Compactor).
No Engine, no probes, no network.
"""

from __future__ import annotations

import time
import pytest

from conftest import evt


# ====================================================================== #
# RuntimeGraph
# ====================================================================== #

class TestRuntimeGraph:

    def setup_method(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        self.g = RuntimeGraph()

    def test_upsert_node_creates_and_increments(self):
        self.g.upsert_node("svc::fn", "function", "svc")
        self.g.upsert_node("svc::fn", "function", "svc", duration_ns=2000)
        n = self.g._nodes["svc::fn"]
        assert n.call_count == 2
        assert n.total_duration_ns == 2000

    def test_avg_duration_computed_correctly(self):
        self.g.upsert_node("svc::fn", "function", "svc", duration_ns=1000)
        self.g.upsert_node("svc::fn", "function", "svc", duration_ns=3000)
        assert self.g._nodes["svc::fn"].avg_duration_ns == 2000

    def test_edge_deduplication(self):
        """Same edge inserted twice should produce one edge with incremented count."""
        self.g.upsert_node("A", "fn", "svc")
        self.g.upsert_node("B", "fn", "svc")
        self.g.upsert_edge("A", "B", "calls")
        self.g.upsert_edge("A", "B", "calls")
        assert len(self.g.neighbors("A")) == 1
        assert self.g.neighbors("A")[0].call_count == 2

    def test_callers_reverse_index(self):
        self.g.upsert_node("A", "fn", "svc")
        self.g.upsert_node("B", "fn", "svc")
        self.g.upsert_edge("A", "B", "calls")
        callers = self.g.callers("B")
        assert len(callers) == 1
        assert callers[0].source == "A"

    def test_reachable_from_bfs(self):
        for n in ["A", "B", "C", "D"]:
            self.g.upsert_node(n, "fn", "svc")
        self.g.upsert_edge("A", "B", "calls")
        self.g.upsert_edge("B", "C", "calls")
        self.g.upsert_edge("B", "D", "calls")
        assert self.g.reachable_from("A") == {"B", "C", "D"}

    def test_reachable_from_cycle_terminates(self):
        """BFS must not infinite-loop on cyclic graphs (A→B→A)."""
        for n in ["A", "B"]:
            self.g.upsert_node(n, "fn", "svc")
        self.g.upsert_edge("A", "B", "calls")
        self.g.upsert_edge("B", "A", "calls")
        reachable = self.g.reachable_from("A")
        assert "B" in reachable  # terminates and finds B

    def test_add_from_event_creates_node(self):
        e = evt(service="django", name="handle_view")
        self.g.add_from_event(e)
        assert "django::handle_view" in self.g._nodes

    def test_add_from_event_creates_edge_between_consecutive(self):
        """Two events on the same trace_id should produce a directed edge."""
        e1 = evt(probe="request.entry",  service="nginx",   name="upstream",  trace_id="t1")
        e2 = evt(probe="function.call",  service="django",  name="view",       trace_id="t1")
        self.g.add_from_event(e1)
        self.g.add_from_event(e2, parent_event=e1)
        assert len(self.g.neighbors("nginx::upstream")) == 1
        assert self.g.neighbors("nginx::upstream")[0].target == "django::view"

    def test_snapshot_round_trip(self):
        self.g.upsert_node("A", "fn", "svc")
        self.g.upsert_node("B", "fn", "svc")
        self.g.upsert_edge("A", "B", "calls")
        snap = self.g.snapshot()
        assert "A" in snap["node_ids"]
        assert "B" in snap["node_ids"]
        assert any("A" in k for k in snap["edge_keys"])

    def test_hottest_nodes_ordering(self):
        self.g.upsert_node("hot", "fn", "svc")
        self.g.upsert_node("cold", "fn", "svc")
        for _ in range(5):
            self.g.upsert_node("hot", "fn", "svc")
        top = self.g.hottest_nodes(top_n=2)
        assert top[0].id == "hot"

    def test_thread_safety_concurrent_upserts(self):
        """Concurrent writes from multiple threads must not corrupt the graph."""
        import threading
        errors = []

        def worker(service_name):
            try:
                for i in range(100):
                    self.g.upsert_node(f"{service_name}::fn{i}", "fn", service_name)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(f"svc{i}",)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(self.g._nodes) == 500  # 5 services × 100 nodes each


# ====================================================================== #
# TemporalStore
# ====================================================================== #

class TestTemporalStore:

    def setup_method(self):
        from stacktracer.core.temporal import TemporalStore
        from stacktracer.core.runtime_graph import RuntimeGraph
        self.store = TemporalStore()
        self.g = RuntimeGraph()

    def test_first_capture_is_empty_diff(self):
        diff = self.store.capture(self.g.snapshot())
        assert diff.is_empty

    def test_capture_detects_new_node(self):
        self.store.capture(self.g.snapshot())
        self.g.upsert_node("A", "fn", "svc")
        diff = self.store.capture(self.g.snapshot())
        assert "A" in diff.added_node_ids

    def test_capture_detects_removed_node(self):
        self.g.upsert_node("A", "fn", "svc")
        self.store.capture(self.g.snapshot())
        del self.g._nodes["A"]  # simulate compaction removing node
        diff = self.store.capture(self.g.snapshot())
        assert "A" in diff.removed_node_ids

    def test_capture_detects_new_edge(self):
        self.g.upsert_node("A", "fn", "svc")
        self.g.upsert_node("B", "fn", "svc")
        self.store.capture(self.g.snapshot())
        self.g.upsert_edge("A", "B", "calls")
        diff = self.store.capture(self.g.snapshot())
        assert any("A" in k for k in diff.added_edge_keys)

    def test_new_edges_since_aggregates_across_diffs(self):
        self.g.upsert_node("A", "fn", "svc")
        self.g.upsert_node("B", "fn", "svc")
        self.g.upsert_node("C", "fn", "svc")
        t0 = time.time()
        self.store.capture(self.g.snapshot())
        self.g.upsert_edge("A", "B", "calls")
        self.store.capture(self.g.snapshot())
        self.g.upsert_edge("B", "C", "calls")
        self.store.capture(self.g.snapshot())
        edges = self.store.new_edges_since(t0)
        assert len(edges) == 2

    def test_label_diff_finds_deployment_marker(self):
        self.store.mark_event("deploy:v1.2.3")
        found = self.store.label_diff("deploy:v1.2.3")
        assert found is not None
        assert found.label == "deploy:v1.2.3"

    def test_label_diff_returns_none_for_unknown(self):
        assert self.store.label_diff("nonexistent") is None

    def test_changes_around_window(self):
        now = time.time()
        self.store.mark_event("before")
        time.sleep(0.01)
        self.store.mark_event("target")
        time.sleep(0.01)
        self.store.mark_event("after")
        target = self.store.label_diff("target")
        nearby = self.store.changes_around(target.timestamp, window_seconds=0.05)
        labels = [d.label for d in nearby]
        assert "before" in labels
        assert "target" in labels
        assert "after" in labels

    def test_ring_buffer_bounded(self):
        store = __import__("stacktracer.core.temporal", fromlist=["TemporalStore"]).TemporalStore(max_diffs=10)
        g = __import__("stacktracer.core.runtime_graph", fromlist=["RuntimeGraph"]).RuntimeGraph()
        for _ in range(15):
            store.capture(g.snapshot())
        assert len(store) == 10  # capped at max_diffs


# ====================================================================== #
# GraphNormalizer
# ====================================================================== #

class TestGraphNormalizer:

    def setup_method(self):
        from stacktracer.core.graph_normalizer import GraphNormalizer
        self.n = GraphNormalizer(enable_builtins=True)

    def test_uuid_collapsed(self):
        result = self.n.normalize("django", "/api/users/550e8400-e29b-41d4-a716-446655440000/profile")
        assert "550e8400" not in result
        assert "{uuid}" in result

    def test_numeric_id_collapsed(self):
        result = self.n.normalize("django", "/api/orders/12345/items")
        assert "12345" not in result
        assert "{id}" in result

    def test_memory_address_collapsed(self):
        result = self.n.normalize("asyncio", "coro <coroutine object at 0x7f3a2b4c>")
        assert "0x7f3a" not in result

    def test_sql_literals_collapsed(self):
        result = self.n.normalize("postgres", "SELECT * FROM users WHERE id = 42 AND name = 'alice'")
        assert "42" not in result
        assert "alice" not in result
        assert "?" in result

    def test_user_pattern_rule(self):
        self.n.add_pattern(
            service     = "django",
            pattern     = r"/api/v\d+/",
            replacement = "/api/{version}/",
        )
        result = self.n.normalize("django", "/api/v3/widgets/")
        assert "/api/{version}/" in result
        assert "v3" not in result

    def test_user_fn_rule(self):
        self.n.add_rule(
            service = "celery",
            fn      = lambda name: name.split("[")[0].strip(),
        )
        result = self.n.normalize("celery", "tasks.process_order[abc-123]")
        assert "[" not in result
        assert result == "tasks.process_order"

    def test_cache_hit_returns_same_result(self):
        r1 = self.n.normalize("django", "/api/users/999/")
        r2 = self.n.normalize("django", "/api/users/999/")
        assert r1 == r2

    def test_different_ids_collapse_to_same_pattern(self):
        """Two different user IDs should produce identical normalized names."""
        r1 = self.n.normalize("django", "/api/users/111/")
        r2 = self.n.normalize("django", "/api/users/222/")
        assert r1 == r2

    def test_cardinality_guard(self):
        """After max_unique_names_per_service distinct names, overflow to sentinel."""
        n = __import__(
            "stacktracer.core.graph_normalizer",
            fromlist=["GraphNormalizer"]
        ).GraphNormalizer(enable_builtins=False, max_unique_names_per_service=5)
        results = set()
        for i in range(10):
            results.add(n.normalize("svc", f"unique_name_{i}"))
        # First 5 pass through, remaining overflow to sentinel
        assert any("overflow" in r or "high_cardinality" in r for r in results)


# ====================================================================== #
# GraphCompactor
# ====================================================================== #

class TestGraphCompactor:

    def _make_graph_with_nodes(self, count: int, cold: bool = False):
        from stacktracer.core.runtime_graph import RuntimeGraph
        g = RuntimeGraph()
        for i in range(count):
            node_id = f"svc::fn{i}"
            g.upsert_node(node_id, "fn", "svc")
            if cold:
                # Backdate last_seen to trigger TTL eviction
                g._nodes[node_id].last_seen = time.time() - 7200
        return g

    def test_ttl_eviction_removes_cold_nodes(self):
        from stacktracer.core.graph_compactor import GraphCompactor
        g = self._make_graph_with_nodes(10, cold=True)
        c = GraphCompactor(node_ttl_s=3600.0, min_call_count=999)
        stats = c.compact(g)
        assert len(g._nodes) == 0
        assert stats["ttl_evicted"] == 10

    def test_ttl_eviction_spares_hot_nodes(self):
        """Nodes with call_count >= min_call_count must survive TTL eviction."""
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.graph_compactor import GraphCompactor
        g = RuntimeGraph()
        # Hot node — called many times, but not seen recently
        g.upsert_node("svc::hot", "fn", "svc")
        for _ in range(10):
            g.upsert_node("svc::hot", "fn", "svc")
        g._nodes["svc::hot"].last_seen = time.time() - 7200
        # Cold node — called once
        g.upsert_node("svc::cold", "fn", "svc")
        g._nodes["svc::cold"].last_seen = time.time() - 7200

        c = GraphCompactor(node_ttl_s=3600.0, min_call_count=5)
        c.compact(g)

        assert "svc::hot" in g._nodes    # protected by min_call_count
        assert "svc::cold" not in g._nodes

    def test_cap_eviction_when_over_limit(self):
        from stacktracer.core.graph_compactor import GraphCompactor
        g = self._make_graph_with_nodes(100)
        c = GraphCompactor(max_nodes=50, evict_to_ratio=0.8, node_ttl_s=999_999)
        stats = c.compact(g)
        assert len(g._nodes) <= 50
        assert stats["cap_evicted"] > 0

    def test_compact_runs_counter_increments(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.graph_compactor import GraphCompactor
        g = RuntimeGraph()
        c = GraphCompactor()
        c.compact(g)
        c.compact(g)
        assert c._compact_runs == 2
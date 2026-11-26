from __future__ import annotations

import time
import uuid

import pytest

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import (
    CausalRule,
    PatternRegistry,
)
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import GraphDiff, TemporalStore
from origintracer.rules.asyncio_rules import (
    LOOP_STARVATION,
    NEW_SYNC_CALL,
)
from origintracer.rules.django_rules import (
    DB_HOTSPOT,
    N_PLUS_ONE,
)
from origintracer.rules.langgraph_rules import (
    AGENT_LATENCY_HOTSPOT,
    LOOP_RUNAWAY,
)


def fresh(tracker):
    """
    Return a clean (graph, temporal, tracker) tuple.
    """
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
        assert "db_query_hotspot" in names

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
        g.upsert_node(
            "asyncio::SlowView.get",
            "asyncio",
            "asyncio",
            metadata={"probe": "asyncio.loop.tick"},
        )
        node = g._nodes["asyncio::SlowView.get"]
        # Simulate a measured 20ms task step (threshold is 10ms).
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
        g.upsert_node(
            "asyncio::SlowView.get",
            "asyncio",
            "asyncio",
            metadata={"probe": "asyncio.loop.tick"},
        )
        node = g._nodes["asyncio::SlowView.get"]
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


class TestLangGraphRules:
    """LangGraph rules operate on synthetic aggregate graph observations."""

    def test_loop_runaway_finds_llm_below_nearest_graph_root(
        self,
    ):
        """Wrapper depth must not hide repeated model work from its owner."""
        graph = RuntimeGraph()
        graph.upsert_node(
            "langgraph::research_graph",
            "langgraph",
            "langgraph",
            metadata={
                "langgraph_kind": "chain",
                "is_langgraph_root": True,
            },
        )
        graph.upsert_node(
            "langgraph::research_agent",
            "langgraph",
            "langgraph",
            metadata={
                "langgraph_kind": "chain",
                "is_langgraph_root": False,
            },
        )
        for _ in range(8):
            graph.upsert_node(
                "langgraph::chat_model",
                "langgraph",
                "langgraph",
                duration_ns=2_000_000,
                metadata={"langgraph_kind": "llm"},
            )
        graph.upsert_edge(
            "langgraph::research_graph",
            "langgraph::research_agent",
            "calls",
        )
        graph.upsert_edge(
            "langgraph::research_agent",
            "langgraph::chat_model",
            "calls",
        )

        matched, evidence = LOOP_RUNAWAY.predicate(
            graph,
            TemporalStore(),
        )

        assert matched is True
        assert (
            evidence["runaway_loops"][0]["root_invocation"]
            == "langgraph::research_graph"
        )
        assert evidence["runaway_loops"][0]["ratio"] == 8.0

    def test_latency_hotspot_counts_leaf_work_without_chain_duration(
        self,
    ):
        """Inclusive chain time must not dilute the LLM/tool comparison."""
        graph = RuntimeGraph()
        graph.upsert_node(
            "langgraph::graph",
            "langgraph",
            "langgraph",
            duration_ns=10_000_000_000,
            metadata={
                "langgraph_kind": "chain",
                "is_langgraph_root": True,
            },
        )
        graph.upsert_node(
            "langgraph::model",
            "langgraph",
            "langgraph",
            duration_ns=80_000_000,
            metadata={"langgraph_kind": "llm"},
        )
        graph.upsert_node(
            "langgraph::search",
            "langgraph",
            "langgraph",
            duration_ns=20_000_000,
            metadata={"langgraph_kind": "tool"},
        )

        matched, evidence = AGENT_LATENCY_HOTSPOT.predicate(
            graph,
            TemporalStore(),
        )

        assert matched is True
        hotspots = evidence["latency_hotspots"]
        assert [item["node"] for item in hotspots] == [
            "langgraph::model"
        ]
        assert hotspots[0]["pct_of_total_leaf_time"] == 80.0


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


def _build_cascade_graph(upstream_calls=100, downstream_calls=0):
    g = RuntimeGraph()
    # upstream service — gunicorn worker
    for _ in range(upstream_calls):
        g.upsert_node(
            "gunicorn::UvicornWorker",
            node_type="gunicorn",
            service="gunicorn",
            metadata={"probe": "gunicorn.worker.init"},
        )
    # downstream — uvicorn handler that depends on upstream
    for _ in range(downstream_calls):
        g.upsert_node(
            "uvicorn::/api/orders/",
            node_type="uvicorn",
            service="uvicorn",
            metadata={"probe": "uvicorn.request.complete"},
        )
    if upstream_calls and downstream_calls:
        g.upsert_edge(
            "gunicorn::UvicornWorker",
            "uvicorn::/api/orders/",
            "handles",
        )
    return g


def _temporal_with_removed(
    removed_nodes=None, removed_edges=None
):
    t = TemporalStore()
    t._diffs.append(
        GraphDiff(
            timestamp=time.time(),
            label="deployment",
            added_node_ids=set(),
            removed_node_ids=set(removed_nodes or []),
            added_edge_keys=set(),
            removed_edge_keys=set(removed_edges or []),
        )
    )
    return t


def _build_regression_graph(avg_duration_ns=500_000_000):
    g = RuntimeGraph()
    # simulate a view that got slower after deployment
    for _ in range(100):
        g.upsert_node(
            "django::/api/products/",
            node_type="django",
            service="django",
            metadata={
                "probe": "django.view.enter",
                "avg_duration_ns": avg_duration_ns,
            },
        )
    g.upsert_node(
        "django::SELECT product WHERE id=%s",
        node_type="django",
        service="django",
        metadata={"probe": "django.db.query"},
    )
    g.upsert_edge(
        "django::/api/products/",
        "django::SELECT product WHERE id=%s",
        "calls",
    )
    return g


def _temporal_with_new_edges(added_edges=None):
    t = TemporalStore()
    t._diffs.append(
        GraphDiff(
            timestamp=time.time(),
            label="deployment",
            added_node_ids=set(),
            removed_node_ids=set(),
            added_edge_keys=set(added_edges or []),
            removed_edge_keys=set(),
        )
    )
    return t


TIMEOUT_NS = 2_000_000_000  # 2 seconds


def _build_external_graph(
    avg_duration_ns=3_000_000_000, call_count=50
):
    g = RuntimeGraph()
    g.upsert_node(
        "asyncio::fetch_external",
        node_type="asyncio",
        service="asyncio",
        metadata={"probe": "asyncio.task.create"},
    )
    for _ in range(call_count):
        g.upsert_node(
            "uvicorn::external-payments-api",
            node_type="uvicorn",
            service="uvicorn",
            metadata={
                "probe": "uvicorn.request.complete",
                "external": True,
                "avg_duration_ns": avg_duration_ns,
            },
        )
    g.upsert_edge(
        "asyncio::fetch_external",
        "uvicorn::external-payments-api",
        "calls",
    )
    return g


def _build_spike_graph(call_count=1000):
    g = RuntimeGraph()
    for _ in range(call_count):
        g.upsert_node(
            "uvicorn::/api/",
            node_type="uvicorn",
            service="uvicorn",
            metadata={"probe": "uvicorn.request.complete"},
        )
    return g


def _temporal_with_added_nodes(count=50):
    t = TemporalStore()
    t._diffs.append(
        GraphDiff(
            timestamp=time.time(),
            label="traffic",
            added_node_ids={
                f"uvicorn::worker-{i}" for i in range(count)
            },
            removed_node_ids=set(),
            added_edge_keys=set(),
            removed_edge_keys=set(),
        )
    )
    return t


def _build_duplicate_graph(query_count=20, view_count=10):
    g = RuntimeGraph()
    for _ in range(view_count):
        g.upsert_node(
            "django::PaymentView",
            node_type="django",
            service="django",
            metadata={"probe": "django.view.enter"},
        )
    # same query fires twice per view — duplicate
    for _ in range(query_count):
        g.upsert_node(
            "django::INSERT INTO payments VALUES (%s)",
            node_type="django",
            service="django",
            metadata={
                "probe": "django.db.query",
                "db_alias": "default",
            },
        )
    g.upsert_edge(
        "django::PaymentView",
        "django::INSERT INTO payments VALUES (%s)",
        "calls",
    )
    return g

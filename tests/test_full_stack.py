"""
tests/test_full_stack.py

Test suite covering:
  - NormalizedEvent creation and serialisation
  - RuntimeGraph node/edge management and queries
  - TemporalStore diff and time-travel
  - PatternRegistry causal evaluation
  - SemanticLayer alias resolution
  - Engine event processing and critical path
  - DSL parser correctness
  - DSL executor against a live engine
  - InMemoryRepository
  - asyncio probe patch/unpatch lifecycle
  - Django middleware (minimal mock)

Run with: pytest tests/ -v
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ------------------------------------------------------------------ #
# Fixtures
# ------------------------------------------------------------------ #


@pytest.fixture
def engine():
    from stacktracer.core.causal import build_default_registry
    from stacktracer.core.engine import Engine
    from stacktracer.core.semantic import (
        SemanticAlias,
        SemanticLayer,
    )
    from stacktracer.sdk.emitter import bind_engine

    sem = SemanticLayer()
    sem.register(
        SemanticAlias(
            label="export",
            description="Export pipeline",
            node_patterns=["django::handle_export"],
            services=["exporter"],
        )
    )

    e = Engine(
        causal_registry=build_default_registry(),
        semantic_layer=sem,
        snapshot_interval_s=9999,  # disable periodic snapshots in tests
    )
    bind_engine(e)
    return e


@pytest.fixture
def trace_id():
    return str(uuid.uuid4())


def make_event(
    probe: str,
    service: str = "django",
    name: str = "test",
    trace_id: str = "trace-1",
    **meta,
):
    from stacktracer.core.event_schema import NormalizedEvent

    return NormalizedEvent.now(
        probe=probe,
        trace_id=trace_id,
        service=service,
        name=name,
        **meta,
    )


# ====================================================================== #
# NormalizedEvent tests
# ====================================================================== #


class TestNormalizedEvent:

    def test_creation(self):
        from stacktracer.core.event_schema import NormalizedEvent

        e = NormalizedEvent(
            probe="request.entry",
            service="django",
            name="/api/export",
            trace_id="t1",
        )
        assert e.probe == "request.entry"
        assert e.service == "django"
        assert e.span_id is not None
        assert e.metadata == {}

    def test_factory_method(self):
        from stacktracer.core.event_schema import NormalizedEvent

        e = NormalizedEvent.now(
            "asyncio.loop.tick",
            "t1",
            "asyncio",
            "step",
            foo="bar",
        )
        assert e.metadata["foo"] == "bar"

    def test_round_trip_serialisation(self):
        from stacktracer.core.event_schema import NormalizedEvent

        e = NormalizedEvent.now(
            "request.entry", "t1", "django", "/api", method="GET"
        )
        d = e.to_dict()
        restored = NormalizedEvent.from_dict(d)
        assert restored.probe == e.probe
        assert restored.trace_id == e.trace_id
        assert restored.span_id == e.span_id

    def test_repr(self):
        from stacktracer.core.event_schema import NormalizedEvent

        e = NormalizedEvent(
            probe="request.entry",
            service="django",
            name="/api",
            trace_id="abc123xyz",
        )
        assert "request.entry" in repr(e)
        assert "abc123" in repr(e)


# ====================================================================== #
# RuntimeGraph tests
# ====================================================================== #


class TestRuntimeGraph:

    def test_upsert_node(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        g.upsert_node("svc::fn", "function", "svc")
        assert "svc::fn" in g._nodes
        assert g._nodes["svc::fn"].call_count == 1

    def test_upsert_node_increments_count(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        g.upsert_node(
            "svc::fn", "function", "svc", duration_ns=1000
        )
        g.upsert_node(
            "svc::fn", "function", "svc", duration_ns=2000
        )
        n = g._nodes["svc::fn"]
        assert n.call_count == 2
        assert n.total_duration_ns == 3000

    def test_avg_duration(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        g.upsert_node(
            "svc::fn", "function", "svc", duration_ns=1000
        )
        g.upsert_node(
            "svc::fn", "function", "svc", duration_ns=3000
        )
        assert g._nodes["svc::fn"].avg_duration_ns == 2000

    def test_add_edge_and_neighbors(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        g.upsert_node("A", "function", "svc")
        g.upsert_node("B", "function", "svc")
        g.upsert_edge("A", "B", "calls")
        nbrs = g.neighbors("A")
        assert len(nbrs) == 1
        assert nbrs[0].target == "B"

    def test_callers(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        g.upsert_node("A", "function", "svc")
        g.upsert_node("B", "function", "svc")
        g.upsert_edge("A", "B", "calls")
        callers = g.callers("B")
        assert len(callers) == 1
        assert callers[0].source == "A"

    def test_reachable_from(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        for n in ["A", "B", "C", "D"]:
            g.upsert_node(n, "fn", "svc")
        g.upsert_edge("A", "B", "calls")
        g.upsert_edge("B", "C", "calls")
        g.upsert_edge("B", "D", "calls")
        reachable = g.reachable_from("A")
        assert reachable == {"B", "C", "D"}

    def test_reachable_from_cycle_safe(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        for n in ["A", "B"]:
            g.upsert_node(n, "fn", "svc")
        g.upsert_edge("A", "B", "calls")
        g.upsert_edge("B", "A", "calls")  # cycle
        reachable = g.reachable_from("A")
        assert (
            "B" in reachable
        )  # should terminate, not loop forever

    def test_snapshot_contains_node_and_edge_ids(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        g.upsert_node("A", "fn", "svc")
        g.upsert_node("B", "fn", "svc")
        g.upsert_edge("A", "B", "calls")
        snap = g.snapshot()
        assert "A" in snap["node_ids"]
        assert any("A" in k for k in snap["edge_keys"])

    def test_add_from_event(self):
        from stacktracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        e = make_event(
            "function.call", "django", "handle_export"
        )
        g.add_from_event(e)
        assert "django::handle_export" in g._nodes


# ====================================================================== #
# TemporalStore tests
# ====================================================================== #


class TestTemporalStore:

    def test_capture_empty_diff(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.temporal import TemporalStore

        g = RuntimeGraph()
        t = TemporalStore()
        diff = t.capture(g.snapshot())
        assert diff.is_empty

    def test_capture_detects_new_node(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.temporal import TemporalStore

        g = RuntimeGraph()
        t = TemporalStore()
        t.capture(g.snapshot())
        g.upsert_node("A", "fn", "svc")
        diff = t.capture(g.snapshot())
        assert "A" in diff.added_node_ids

    def test_capture_detects_new_edge(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.temporal import TemporalStore

        g = RuntimeGraph()
        t = TemporalStore()
        for n in ["A", "B"]:
            g.upsert_node(n, "fn", "svc")
        t.capture(g.snapshot())
        g.upsert_edge("A", "B", "calls")
        diff = t.capture(g.snapshot())
        assert any(
            "A" in k and "B" in k for k in diff.added_edge_keys
        )

    def test_new_edges_since(self):
        import time

        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.temporal import TemporalStore

        g = RuntimeGraph()
        t = TemporalStore()
        for n in ["A", "B"]:
            g.upsert_node(n, "fn", "svc")
        t.capture(g.snapshot())
        t_mark = time.time()
        g.upsert_edge("A", "B", "calls")
        t.capture(g.snapshot())
        new = t.new_edges_since(t_mark)
        assert len(new) > 0

    def test_mark_event_and_label_diff(self):
        from stacktracer.core.temporal import TemporalStore

        t = TemporalStore()
        t.mark_event("deployment:v1.2")
        found = t.label_diff("deployment:v1.2")
        assert found is not None
        assert found.label == "deployment:v1.2"


# ====================================================================== #
# PatternRegistry (causal) tests
# ====================================================================== #


class TestPatternRegistry:

    def test_register_and_match(self):
        from stacktracer.core.active_requests import (
            ActiveRequestTracker,
        )
        from stacktracer.core.causal import (
            CausalRule,
            PatternRegistry,
        )
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.temporal import TemporalStore

        def always_match(g, t, a):
            return True, {"evidence": "always"}

        registry = PatternRegistry()
        registry.register(
            CausalRule(
                "test_rule", "Always matches", always_match, 0.9
            )
        )

        g = RuntimeGraph()
        t = TemporalStore()
        a = ActiveRequestTracker()
        matches = registry.evaluate(g, t, a)
        assert len(matches) == 1
        assert matches[0].rule_name == "test_rule"
        assert matches[0].confidence == 0.9

    def test_rule_error_does_not_crash(self):
        from stacktracer.core.causal import (
            CausalRule,
            PatternRegistry,
        )
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.temporal import TemporalStore

        def broken_rule(g, t):
            raise RuntimeError("oops")

        registry = PatternRegistry()
        registry.register(
            CausalRule("broken", "Broken rule", broken_rule, 0.5)
        )

        matches = registry.evaluate(
            RuntimeGraph(), TemporalStore()
        )
        assert len(matches) == 1
        assert matches[0].confidence == 0.0

    def test_loop_starvation_rule(self):
        from stacktracer.core.causal import LOOP_STARVATION
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.temporal import TemporalStore

        g = RuntimeGraph()
        # Create a node that looks like a slow asyncio loop tick
        g.upsert_node(
            "asyncio::asyncio.loop.tick",
            "asyncio",
            "asyncio",
            duration_ns=50_000_000,
        )
        g.upsert_node(
            "asyncio::asyncio.loop.tick",
            "asyncio",
            "asyncio",
            duration_ns=50_000_000,
        )

        matched, evidence = LOOP_STARVATION.predicate(
            g, TemporalStore()
        )
        assert matched


# ====================================================================== #
# SemanticLayer tests
# ====================================================================== #


class TestSemanticLayer:

    def test_register_and_resolve(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.semantic import (
            SemanticAlias,
            SemanticLayer,
        )

        g = RuntimeGraph()
        g.upsert_node(
            "django::handle_export", "function", "django"
        )
        g.upsert_node("django::other_fn", "function", "django")

        layer = SemanticLayer()
        layer.register(
            SemanticAlias(
                label="export",
                description="The export system",
                node_patterns=["django::handle_export"],
                services=[],
            )
        )

        resolved = layer.resolve_nodes("export", g)
        assert "django::handle_export" in resolved
        assert "django::other_fn" not in resolved

    def test_resolve_by_service(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.semantic import (
            SemanticAlias,
            SemanticLayer,
        )

        g = RuntimeGraph()
        g.upsert_node("exporter::run", "function", "exporter")
        g.upsert_node(
            "exporter::cleanup", "function", "exporter"
        )
        g.upsert_node("django::view", "function", "django")

        layer = SemanticLayer()
        layer.register(
            SemanticAlias(
                label="export",
                description="",
                node_patterns=[],
                services=["exporter"],
            )
        )

        resolved = layer.resolve_nodes("export", g)
        assert "exporter::run" in resolved
        assert "exporter::cleanup" in resolved
        assert "django::view" not in resolved

    def test_missing_label_returns_empty(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.semantic import SemanticLayer

        layer = SemanticLayer()
        assert (
            layer.resolve_nodes("nonexistent", RuntimeGraph())
            == set()
        )

    def test_regex_pattern(self):
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.semantic import (
            SemanticAlias,
            SemanticLayer,
        )

        g = RuntimeGraph()
        g.upsert_node(
            "django::flags_client.call", "function", "django"
        )
        g.upsert_node(
            "django::flags_client.retry", "function", "django"
        )
        g.upsert_node("django::other", "function", "django")

        layer = SemanticLayer()
        layer.register(
            SemanticAlias(
                label="flags",
                description="",
                node_patterns=["django::flags_client.*"],
                services=[],
            )
        )

        resolved = layer.resolve_nodes("flags", g)
        assert "django::flags_client.call" in resolved
        assert "django::flags_client.retry" in resolved
        assert "django::other" not in resolved


# ====================================================================== #
# Engine tests
# ====================================================================== #


class TestEngine:

    def test_process_builds_graph(self, engine, trace_id):
        e = make_event(
            "function.call",
            "django",
            "handle_export",
            trace_id=trace_id,
        )
        engine.process(e)
        node = engine.graph.get_node("django::handle_export")
        assert node is not None
        assert node.call_count == 1

    def test_critical_path_ordering(self, engine, trace_id):
        probes_in_order = [
            "request.entry",
            "django.middleware.enter",
            "django.view.enter",
            "request.exit",
        ]
        for p in probes_in_order:
            time.sleep(0.001)  # ensure distinct timestamps
            engine.process(
                make_event(
                    p, "django", "/api", trace_id=trace_id
                )
            )

        path = engine.critical_path(trace_id)
        probes_found = [s["probe"] for s in path]
        assert probes_found == probes_in_order

    def test_critical_path_empty_for_unknown_trace(self, engine):
        path = engine.critical_path("nonexistent-trace")
        assert path == []

    def test_hotspots_sorted_by_call_count(
        self, engine, trace_id
    ):
        for _ in range(5):
            engine.process(
                make_event(
                    "function.call",
                    "django",
                    "hot_fn",
                    trace_id=trace_id,
                )
            )
        engine.process(
            make_event(
                "function.call",
                "django",
                "cold_fn",
                trace_id=trace_id,
            )
        )
        hotspots = engine.hotspots(top_n=5)
        names = [h["node"] for h in hotspots]
        assert names.index("django::hot_fn") < names.index(
            "django::cold_fn"
        )

    def test_snapshot_triggers_temporal_diff(self, engine):
        engine.process(
            make_event("function.call", "django", "fn_a")
        )
        engine.snapshot()
        engine.process(
            make_event("function.call", "django", "fn_b")
        )
        diff = engine.snapshot()
        assert "django::fn_b" in diff["added_nodes"]

    def test_mark_deployment(self, engine):
        engine.mark_deployment("deploy:v1")
        found = engine.temporal.label_diff("deploy:v1")
        assert found is not None

    def test_evaluate_returns_list(self, engine):
        matches = engine.evaluate()
        assert isinstance(matches, list)

    def test_status_dict(self, engine):
        s = engine.status()
        assert "graph_nodes" in s
        assert "temporal_diffs" in s


# ====================================================================== #
# DSL Parser tests
# ====================================================================== #


class TestQueryParser:

    def test_show_latency(self):
        from stacktracer.query.parser import parse

        q = parse('SHOW latency WHERE service = "django"')
        assert q.verb == "SHOW"
        assert q.metric == "latency"
        assert q.filters["service"] == "django"

    def test_show_events_with_limit(self):
        from stacktracer.query.parser import parse

        q = parse(
            'SHOW events WHERE probe = "asyncio.task.block" LIMIT 50'
        )
        assert q.verb == "SHOW"
        assert q.filters["probe"] == "asyncio.task.block"
        assert q.limit == 50

    def test_trace_verb(self):
        from stacktracer.query.parser import parse

        q = parse("TRACE abc123def456")
        assert q.verb == "TRACE"
        assert q.filters["trace_id"] == "abc123def456"

    def test_blame_verb(self):
        from stacktracer.query.parser import parse

        q = parse('BLAME WHERE system = "export"')
        assert q.verb == "BLAME"
        assert q.filters["system"] == "export"

    def test_hotspot_with_n(self):
        from stacktracer.query.parser import parse

        q = parse("HOTSPOT TOP 20")
        assert q.verb == "HOTSPOT"
        assert q.limit == 20

    def test_diff_since_label(self):
        from stacktracer.query.parser import parse

        q = parse("DIFF SINCE deployment")
        assert q.verb == "DIFF"
        assert q.filters["since"] == "deployment"

    def test_causal_verb(self):
        from stacktracer.query.parser import parse

        q = parse("CAUSAL")
        assert q.verb == "CAUSAL"

    def test_unknown_verb_raises(self):
        from stacktracer.query.parser import parse

        with pytest.raises(ValueError, match="Unknown verb"):
            parse("DELETE everything")

    def test_empty_query_raises(self):
        from stacktracer.query.parser import parse

        with pytest.raises(ValueError, match="Empty query"):
            parse("")


# ====================================================================== #
# DSL Executor tests
# ====================================================================== #


class TestQueryExecutor:

    def test_show_latency_returns_data(self, engine, trace_id):
        engine.process(
            make_event(
                "function.call",
                "django",
                "view",
                trace_id=trace_id,
                duration_ns=5_000_000,
            )
        )
        from stacktracer.query.parser import execute, parse

        result = execute(
            parse('SHOW latency WHERE service = "django"'),
            engine,
        )
        assert "data" in result
        assert any(
            r["service"] == "django" for r in result["data"]
        )

    def test_hotspot_executor(self, engine, trace_id):
        for _ in range(3):
            engine.process(
                make_event(
                    "function.call",
                    "django",
                    "busy_view",
                    trace_id=trace_id,
                )
            )
        from stacktracer.query.parser import execute, parse

        result = execute(parse("HOTSPOT TOP 5"), engine)
        assert "data" in result
        assert len(result["data"]) <= 5

    def test_causal_executor(self, engine):
        from stacktracer.query.parser import execute, parse

        result = execute(parse("CAUSAL"), engine)
        assert "data" in result

    def test_blame_unknown_system(self, engine):
        from stacktracer.query.parser import execute, parse

        result = execute(
            parse('BLAME WHERE system = "nonexistent"'), engine
        )
        assert "error" in result

    def test_trace_executor(self, engine, trace_id):
        engine.process(
            make_event(
                "request.entry",
                "django",
                "/api",
                trace_id=trace_id,
            )
        )
        engine.process(
            make_event(
                "request.exit",
                "django",
                "/api",
                trace_id=trace_id,
            )
        )
        from stacktracer.query.parser import execute, parse

        result = execute(parse(f"TRACE {trace_id}"), engine)
        assert result["trace_id"] == trace_id
        assert result["stages"] >= 2


# ====================================================================== #
# InMemoryRepository tests
# ====================================================================== #


class TestInMemoryRepository:

    def test_insert_and_query(self):
        from stacktracer.storage.base import (
            InMemoryRepository,
        )

        repo = InMemoryRepository()
        e = make_event(
            "request.entry", "django", "/api", trace_id="t-1"
        )
        repo.insert_event(e)
        results = repo.query_events(trace_id="t-1")
        assert len(results) == 1
        assert results[0]["probe"] == "request.entry"

    def test_filter_by_probe(self):
        from stacktracer.storage.base import (
            InMemoryRepository,
        )

        repo = InMemoryRepository()
        repo.insert_event(
            make_event("request.entry", trace_id="t-2")
        )
        repo.insert_event(
            make_event("asyncio.loop.tick", trace_id="t-2")
        )
        results = repo.query_events(probe="request.entry")
        assert all(
            r["probe"] == "request.entry" for r in results
        )

    def test_limit(self):
        from stacktracer.storage.base import (
            InMemoryRepository,
        )

        repo = InMemoryRepository()
        for i in range(20):
            repo.insert_event(
                make_event("request.entry", trace_id=f"t-{i}")
            )
        results = repo.query_events(limit=5)
        assert len(results) == 5

    def test_maxlen_enforced(self):
        from stacktracer.storage.base import (
            InMemoryRepository,
        )

        repo = InMemoryRepository(max_events=10)
        for i in range(20):
            repo.insert_event(
                make_event("request.entry", trace_id=f"t-{i}")
            )
        assert len(repo) == 10


# ====================================================================== #
# asyncio probe lifecycle tests
# ====================================================================== #


class TestAsyncioProbe:

    def test_start_stop_idempotent(self):
        from stacktracer.probes.asyncio_probe import AsyncioProbe

        probe = AsyncioProbe()
        probe.start()
        probe.stop()
        # Should not raise on double start/stop
        probe.start()
        probe.stop()

    @pytest.mark.asyncio
    async def test_task_step_does_not_break_execution(
        self, engine, trace_id
    ):
        """Most important test: patching must not interfere with async execution."""
        from stacktracer.context.vars import (
            reset_trace,
            set_trace,
        )
        from stacktracer.probes.asyncio_probe import AsyncioProbe

        probe = AsyncioProbe()
        probe.start()
        token = set_trace(trace_id)

        try:
            result = await asyncio.gather(
                asyncio.sleep(0.01),
                asyncio.sleep(0.01),
            )
            assert result == [None, None]
        finally:
            reset_trace(token)
            probe.stop()

"""
tests/test_integration.py

End-to-end integration tests that exercise the full data path:

    Probe events → emit() → Engine.process() → RuntimeGraph
                                              → TemporalStore
                                              → ActiveRequestTracker
                                              → CausalRegistry
                  → Uploader.insert_event()  → InMemoryRepository
                  → GraphSerializer          → Snapshot bytes
                  → GraphDeserializer        → Reconstructed graph
                  → DSL executor             → Query result

These tests don't mock anything. They run the real components together
and verify that the pieces that were designed and implemented separately
actually fit together correctly.

Each test class represents one realistic scenario:
    TestNginxDjangoPostgresTrace   — full stack HTTP request
    TestDeploymentCorrelation      — causal rule fires after deploy
    TestHighCardinalityNormalization — normalizer feeds compactor feeds graph
    TestSnapshotRoundTrip          — agent serialises, backend deserialises
    TestConfigMergePipeline        — defaults + user yaml + kwargs merge
"""

from __future__ import annotations

import time
import uuid
import pytest

from conftest import evt
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.core.engine import Engine
from stacktracer.core.causal import build_default_registry
from stacktracer.core.active_requests import ActiveRequestTracker
from stacktracer.core.semantic import SemanticLayer
from stacktracer.sdk.emitter import bind_engine, emit


# ====================================================================== #
# Full stack HTTP request trace
# ====================================================================== #

class TestNginxDjangoPostgresTrace:
    """
    Simulate a production HTTP request flowing through:
        nginx → (gunicorn) → django → postgres
    All events share one trace_id. Verify that:
        - The graph captures the full topology
        - The critical path is correct
        - The DSL query returns the right nodes
    """

    def setup_method(self):
        tracker = ActiveRequestTracker()
        self.engine = Engine(
            causal_registry    = build_default_registry(tracker=tracker),
            snapshot_interval_s= 9999,
        )
        self.engine.tracker = tracker
        bind_engine(self.engine)

    def test_topology_built_correctly(self):
        tid = str(uuid.uuid4())

        # nginx accepts the connection
        emit(NormalizedEvent.now("request.entry", tid, "nginx",    "upstream"))
        # django middleware enters
        emit(NormalizedEvent.now("request.entry", tid, "django",   "TracerMiddleware"))
        # Django view is called
        emit(NormalizedEvent.now("function.call", tid, "django",   "UserOrderView.get"))
        # ORM fires a query
        emit(NormalizedEvent.now("db.query.start", tid, "postgres", "SELECT orders WHERE user_id=?"))
        emit(NormalizedEvent.now("db.query.end",   tid, "postgres", "SELECT orders WHERE user_id=?",
                                  duration_ns=5_000_000))
        # Django view returns
        emit(NormalizedEvent.now("request.exit",  tid, "django",   "UserOrderView.get"))
        # nginx sends response
        emit(NormalizedEvent.now("request.exit",  tid, "nginx",    "upstream"))

        g = self.engine.graph

        # All services present
        services = {n.service for n in g.all_nodes()}
        assert "nginx"    in services
        assert "django"   in services
        assert "postgres" in services

        # Topology: nginx → django → postgres
        reachable_from_nginx = g.reachable_from("nginx::upstream")
        assert "django::TracerMiddleware"       in reachable_from_nginx
        assert "django::UserOrderView.get"      in reachable_from_nginx
        assert "postgres::SELECT orders WHERE user_id=?" in reachable_from_nginx

    def test_critical_path_returns_ordered_stages(self):
        tid = str(uuid.uuid4())
        stages = [
            ("request.entry", "nginx",    "upstream"),
            ("function.call", "django",   "view"),
            ("db.query.start","postgres", "SELECT"),
        ]
        for probe, service, name in stages:
            emit(NormalizedEvent.now(probe, tid, service, name))

        path = self.engine.critical_path(tid)
        assert len(path) == 3
        assert path[0]["service"] == "nginx"
        assert path[1]["service"] == "django"
        assert path[2]["service"] == "postgres"

    def test_dsl_hotspot_returns_most_called_node(self):
        """After many requests, the most-called node tops the hotspot list."""
        from stacktracer.query.parser import parse, execute

        for i in range(10):
            tid = str(uuid.uuid4())
            emit(NormalizedEvent.now("function.call", tid, "django", "busy_view"))

        for i in range(2):
            tid = str(uuid.uuid4())
            emit(NormalizedEvent.now("function.call", tid, "django", "quiet_view"))

        result = execute(parse("HOTSPOT TOP 5"), self.engine)
        top_node = result["data"][0]["node"]
        assert top_node == "django::busy_view"

    def test_tracker_records_probe_sequence(self):
        """
        Django probe would call tracker.start() and tracker.complete().
        Simulate that lifecycle and verify the probe_sequence accumulates.
        """
        tid = str(uuid.uuid4())
        self.engine.tracker.start(tid, service="django", pattern="/api/orders/")

        emit(NormalizedEvent.now("db.query.start", tid, "postgres", "SELECT"))
        emit(NormalizedEvent.now("db.query.end",   tid, "postgres", "SELECT"))

        self.engine.tracker.complete(tid)

        # Span is now in completions — tracker.all_patterns_summary() should show it
        summary = self.engine.tracker.all_patterns_summary()
        assert "/api/orders/" in summary


# ====================================================================== #
# Deployment correlation
# ====================================================================== #

class TestDeploymentCorrelation:
    """
    new_sync_call_after_deployment rule fires when edges appear after a
    deployment marker. This is the Antimetal 'Exporter→Flags' pattern.
    """

    def setup_method(self):
        tracker = ActiveRequestTracker()
        self.engine = Engine(
            causal_registry    = build_default_registry(tracker=tracker),
            snapshot_interval_s= 9999,
        )
        self.engine.tracker = tracker
        bind_engine(self.engine)

    def test_new_edge_after_deployment_fires_rule(self):
        # Some baseline traffic before deployment
        for i in range(3):
            tid = str(uuid.uuid4())
            emit(NormalizedEvent.now("function.call", tid, "django", "existing_view"))
        self.engine.snapshot()

        # Deployment happens
        self.engine.mark_deployment("v3.0.0-canary")
        time.sleep(0.01)

        # NEW synchronous call appears post-deploy
        tid = str(uuid.uuid4())
        emit(NormalizedEvent.now("request.entry", tid, "django",   "existing_view"))
        emit(NormalizedEvent.now("function.call", tid, "exporter", "call_feature_flags"))
        self.engine.snapshot()

        matches = self.engine.evaluate()
        rule_names = [m.rule_name for m in matches]
        assert "new_sync_call_after_deployment" in rule_names

    def test_deployment_marker_survives_snapshot_cycle(self):
        self.engine.mark_deployment("v3.0.0")
        self.engine.snapshot()  # temporal store captures the marker
        found = self.engine.temporal.label_diff("v3.0.0")
        assert found is not None


# ====================================================================== #
# High-cardinality normalization through to graph
# ====================================================================== #

class TestHighCardinalityNormalization:
    """
    Without normalization, 1000 different user IDs in URLs create 1000
    graph nodes. With normalization, they collapse to one pattern node.
    This test verifies the normalizer integrates correctly with the graph.
    """

    def test_url_variants_collapse_to_single_node(self):
        pytest.importorskip("stacktracer.core.graph_normalizer")
        from stacktracer.core.graph_normalizer import GraphNormalizer
        from stacktracer.core.runtime_graph import RuntimeGraph

        normalizer = GraphNormalizer(enable_builtins=True)
        g = RuntimeGraph()
        g.normalizer = normalizer

        # 100 different user IDs in the URL
        for uid in range(100):
            e = NormalizedEvent.now(
                "function.call", str(uuid.uuid4()),
                "django", f"/api/users/{uid}/orders/",
            )
            g.add_from_event(e)

        # All 100 should collapse to one normalized node
        django_nodes = [nid for nid in g._nodes if nid.startswith("django::")]
        assert len(django_nodes) == 1
        assert "{id}" in django_nodes[0] or "{uuid}" in django_nodes[0] or "id" in django_nodes[0]


# ====================================================================== #
# Graph snapshot round-trip
# ====================================================================== #

class TestSnapshotRoundTrip:
    """
    The agent serialises its RuntimeGraph to msgpack and ships it to
    FastAPI. FastAPI deserialises and serves queries from it.
    This test verifies the graph survives the serialise/deserialise cycle
    with correct topology, node stats, and edge counts.
    """

    def test_msgpack_round_trip_preserves_topology(self):
        pytest.importorskip("msgpack")
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.graph_serializer import MsgpackSerializer

        # Build source graph
        g = RuntimeGraph()
        for _ in range(5):
            g.upsert_node("django::view",     "fn", "django")
        g.upsert_node("postgres::SELECT",     "db", "postgres")
        g.upsert_edge("django::view", "postgres::SELECT", "calls")

        # Serialise
        data = MsgpackSerializer().serialize(g)

        # Deserialise
        g2 = MsgpackSerializer().deserialize(data)

        # Topology preserved
        assert "django::view"     in g2._nodes
        assert "postgres::SELECT" in g2._nodes
        assert len(g2.neighbors("django::view")) == 1
        assert g2.neighbors("django::view")[0].target == "postgres::SELECT"

    def test_msgpack_round_trip_preserves_call_counts(self):
        pytest.importorskip("msgpack")
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.graph_serializer import MsgpackSerializer

        g = RuntimeGraph()
        for _ in range(7):
            g.upsert_node("svc::fn", "fn", "svc", duration_ns=1_000_000)

        data = MsgpackSerializer().serialize(g)
        g2   = MsgpackSerializer().deserialize(data)

        node = g2._nodes["svc::fn"]
        assert node.call_count == 7
        assert node.avg_duration_ns == 1_000_000

    def test_snapshot_pipeline_via_backend(self):
        """
        Simulate the full agent→backend pipeline:
            1. Agent builds graph and serialises it
            2. Backend receives bytes and deserialises
            3. Backend serves a DSL query from the deserialised graph
        """
        pytest.importorskip("msgpack")
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.graph_serializer import MsgpackSerializer
        from stacktracer.query.parser import parse, execute

        # Agent side
        g = RuntimeGraph()
        for _ in range(3):
            g.upsert_node("django::api_view", "fn", "django")
        g.upsert_node("postgres::SELECT users", "db", "postgres")
        g.upsert_edge("django::api_view", "postgres::SELECT users", "calls")
        data = MsgpackSerializer().serialize(g)

        # Backend side — deserialise and query
        g2     = MsgpackSerializer().deserialize(data)
        result = execute(parse("HOTSPOT TOP 3"), g2)

        assert "data" in result
        top = result["data"][0]["node"]
        assert top == "django::api_view"  # called 3 times vs postgres 1


# ====================================================================== #
# Config merge pipeline
# ====================================================================== #

class TestConfigMergePipeline:
    """
    ResolvedConfig is built from three sources:
        defaults.yaml < user stacktracer.yaml < init() kwargs

    These tests verify the merge semantics without calling the full
    stacktracer.init() (which starts threads and needs a live app).
    """

    def _merge(self, user_yaml: dict, **kwargs) -> "ResolvedConfig":
        from stacktracer.__init__ import (
            _load_package_defaults, _deep_merge,
            _build_resolved_config,
        )
        defaults    = _load_package_defaults()
        merged_yaml = _deep_merge(defaults, user_yaml)
        return _build_resolved_config(
            merged_yaml       = merged_yaml,
            api_key           = kwargs.pop("api_key", ""),
            endpoint          = kwargs.pop("endpoint", "https://api.stacktracer.io"),
            sample_rate       = kwargs.pop("sample_rate", None),
            probes            = kwargs.pop("probes", None),
            semantic          = kwargs.pop("semantic", None),
            snapshot_interval = kwargs.pop("snapshot_interval", None),
            flush_interval    = kwargs.pop("flush_interval", None),
            debug             = kwargs.pop("debug", False),
            config_path       = None,
            normalize         = kwargs.pop("normalize", None),
            compactor         = kwargs.pop("compactor", None),
            active_requests   = kwargs.pop("active_requests", None),
        )

    def test_defaults_applied_when_no_user_config(self):
        cfg = self._merge({})
        assert cfg.sample_rate == 0.01
        assert "django" in cfg.probes

    def test_user_yaml_overrides_defaults(self):
        cfg = self._merge({"sample_rate": 0.10})
        assert cfg.sample_rate == 0.10

    def test_init_kwarg_overrides_user_yaml(self):
        cfg = self._merge({"sample_rate": 0.10}, sample_rate=0.50)
        assert cfg.sample_rate == 0.50

    def test_probes_list_replaced_not_merged(self):
        """User specifying probes: [django] should get ONLY django, not defaults+django."""
        cfg = self._merge({"probes": ["django"]})
        assert cfg.probes == ["django"]

    def test_compactor_dict_merged_key_by_key(self):
        """User overriding one compactor key should keep all other defaults."""
        cfg = self._merge({"compactor": {"max_nodes": 9999}})
        assert cfg.compactor["max_nodes"] == 9999
        assert "evict_to_ratio" in cfg.compactor    # default preserved

    def test_semantic_merged_by_label(self):
        """
        User adding a new semantic label should produce combined list.
        User overriding existing label should win.
        """
        cfg = self._merge({
            "semantic": [
                {"label": "api", "description": "override", "node_patterns": [], "services": []},
                {"label": "auth", "description": "new",    "node_patterns": [], "services": ["auth"]},
            ]
        })
        labels = {s["label"] for s in cfg.semantic}
        assert "api"  in labels   # overridden
        assert "auth" in labels   # added
        assert "db"   in labels   # default preserved

    def test_semantic_label_from_init_kwarg_wins_over_yaml(self):
        cfg = self._merge(
            {"semantic": [{"label": "api", "description": "from yaml", "node_patterns": [], "services": []}]},
            semantic  = [{"label": "api", "description": "from kwarg", "node_patterns": [], "services": []}],
        )
        api_entries = [s for s in cfg.semantic if s["label"] == "api"]
        assert len(api_entries) == 1
        assert api_entries[0]["description"] == "from kwarg"

    def test_sample_rate_clamped_to_valid_range(self):
        cfg_over  = self._merge({}, sample_rate=2.0)
        cfg_under = self._merge({}, sample_rate=-1.0)
        assert cfg_over.sample_rate  == 1.0
        assert cfg_under.sample_rate == 0.0
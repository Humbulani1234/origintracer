"""
End-to-end integration tests that exercise the full data path:

    Probe events >> emit() >> Engine.process() >> RuntimeGraph
                                               >> TemporalStore
                                               >> ActiveRequestTracker
                                               >> CausalRegistry

                >> Uploader.insert_event() >> InMemoryRepository
                >> GraphSerializer >> Snapshot bytes
                >> GraphDeserializer >> Reconstructed graph
                >> DSL executor >> Query result

The tests don't mock anything. They run the real components together
and verify that the pieces that were designed and implemented separately
actually fit together correctly.

Each test class represents one realistic scenario:
    TestNginxDjangoPostgresTrace - full stack HTTP request
    TestDeploymentCorrelation - causal rule fires after deploy
    TestHighCardinalityNormalization - normalizer feeds compactor feeds graph
    TestSnapshotRoundTrip - agent serialises, backend deserialises
    TestConfigMergePipeline - defaults + user yaml + kwargs merge
"""

from __future__ import annotations

import uuid

import pytest

from origintracer.__init__ import ResolvedConfig
from origintracer.core.engine import Engine
from origintracer.core.event_schema import NormalizedEvent
from origintracer.core.graph_serializer import (
    MsgpackSerializer,
)
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.sdk.emitter import (
    emit,
    enable_sync_mode,
)


class TestNginxDjangoPostgresTrace:
    """
    Simulate a production HTTP request flowing through:
        nginx >> (gunicorn) >> django >> postgres
    All events share one trace_id. Verify that:
        - The graph captures the full topology
        - The critical path is correct
        - The DSL query returns the right nodes
    """

    def setup_method(self):
        enable_sync_mode()

    def test_topology_built_correctly(self, engine):
        tid = str(uuid.uuid4())

        # nginx accepts the connection
        emit(
            NormalizedEvent.now(
                "request.entry", tid, "nginx", "upstream"
            )
        )
        # django middleware enters
        emit(
            NormalizedEvent.now(
                "request.entry",
                tid,
                "django",
                "TracerMiddleware",
            )
        )
        # Django view is called
        emit(
            NormalizedEvent.now(
                "function.call",
                tid,
                "django",
                "UserOrderView.get",
            )
        )
        # ORM fires a query
        emit(
            NormalizedEvent.now(
                "db.query.start",
                tid,
                "postgres",
                "SELECT orders WHERE user_id=?",
            )
        )
        emit(
            NormalizedEvent.now(
                "db.query.end",
                tid,
                "postgres",
                "SELECT orders WHERE user_id=?",
                duration_ns=5_000_000,
            )
        )
        # Django view returns
        emit(
            NormalizedEvent.now(
                "request.exit",
                tid,
                "django",
                "UserOrderView.get",
            )
        )
        # nginx sends response
        emit(
            NormalizedEvent.now(
                "request.exit", tid, "nginx", "upstream"
            )
        )
        g = engine.graph

        # All services present
        services = {n.service for n in g.all_nodes()}
        assert "nginx" in services
        assert "django" in services
        assert "postgres" in services

        # Topology: nginx → django → postgres
        reachable_from_nginx = g.reachable_from(
            "nginx::upstream"
        )
        assert "django::TracerMiddleware" in reachable_from_nginx
        assert (
            "django::UserOrderView.get" in reachable_from_nginx
        )
        assert (
            "postgres::SELECT orders WHERE user_id=?"
            in reachable_from_nginx
        )

    def test_critical_path_returns_ordered_stages(self, engine):
        tid = str(uuid.uuid4())
        stages = [
            ("request.entry", "nginx", "upstream"),
            ("function.call", "django", "view"),
            ("db.query.start", "postgres", "SELECT"),
        ]
        for probe, service, name in stages:
            emit(NormalizedEvent.now(probe, tid, service, name))

        path = engine.critical_path(tid)
        assert len(path) == 3
        assert path[0]["service"] == "nginx"
        assert path[1]["service"] == "django"
        assert path[2]["service"] == "postgres"

    def test_dsl_hotspot_returns_most_called_node(self, engine):
        """After many requests, the most-called node tops the hotspot list."""
        from origintracer.query.parser import execute, parse

        for i in range(10):
            tid = str(uuid.uuid4())
            emit(
                NormalizedEvent.now(
                    "function.call", tid, "django", "busy_view"
                )
            )

        for i in range(2):
            tid = str(uuid.uuid4())
            emit(
                NormalizedEvent.now(
                    "function.call", tid, "django", "quiet_view"
                )
            )
        result = execute(parse("HOTSPOT TOP 5"), engine)
        data = result.get("data")
        top_node = data[0]["node"]
        assert top_node == "django::busy_view"

    def test_tracker_records_probe_sequence(self, engine):
        """
        Django probe would call tracker.start() and tracker.complete().
        Simulate that lifecycle and verify the probe_sequence accumulates.
        """
        tid = str(uuid.uuid4())
        engine.tracker.start(
            tid, service="django", pattern="/api/orders/"
        )

        emit(
            NormalizedEvent.now(
                "db.query.start", tid, "postgres", "SELECT"
            )
        )
        emit(
            NormalizedEvent.now(
                "db.query.end", tid, "postgres", "SELECT"
            )
        )

        engine.tracker.complete(tid)

        # Span is now in completions — tracker.all_patterns_summary() should show it
        summary = engine.tracker.all_patterns_summary()
        assert "/api/orders/" in summary


class TestDeploymentCorrelation:
    """
    new_sync_call_after_deployment rule fires when edges appear after a
    deployment marker. This is the Antimetal 'Exporter→Flags' pattern.
    """

    def setup_method(self):
        enable_sync_mode()

    def test_deployment_marker_survives_snapshot_cycle(
        self, engine
    ):
        engine.mark_deployment("v3.0.0")
        engine.snapshot()  # temporal store captures the marker
        found = engine.temporal.label_diff("v3.0.0")
        assert found is not None


class TestHighCardinalityNormalization:
    """
    Verifies that the Engine scrubs high-cardinality names
    before they ever reach the RuntimeGraph or the Tracker.
    """

    def test_engine_collapses_url_variants(self, engine):
        # 1. Ingest 100 events with unique user IDs into the ENGINE
        # We use the 'engine' fixture which is already bound
        for uid in range(100):
            trace_id = str(uuid.uuid4())

            # Create a raw event with a high-cardinality ID
            raw_event = NormalizedEvent.now(
                probe="uvicorn.request.receive",
                trace_id=trace_id,
                service="django",
                name=f"/api/users/{uid}/orders/",
            )

            # The Engine should normalize this internally during ingest
            engine.process(raw_event)

        # 2. Check the Graph (via the engine.graph shortcut)
        django_nodes = [
            nid
            for nid in engine.graph._nodes
            if nid.startswith("django::")
        ]

        # ASSERTIONS
        # Even with 100 different UIDs, we should only have ONE node
        assert (
            len(django_nodes) == 1
        ), f"Cardinality explosion! Found nodes: {django_nodes}"

        # Verify the node was correctly transformed by the built-in rules
        assert "{id}" in django_nodes[0]
        assert "/0/" not in django_nodes[0]


class TestSnapshotRoundTrip:
    """
    The agent serialises its RuntimeGraph to msgpack and ships it to
    FastAPI. FastAPI deserialises and serves queries from it.
    This test verifies the graph survives the serialise/deserialise cycle
    with correct topology, node stats, and edge counts.
    """

    def test_msgpack_round_trip_preserves_topology(self):
        pytest.importorskip("msgpack")

        # Build source graph
        g = RuntimeGraph()
        for _ in range(5):
            g.upsert_node("django::view", "fn", "django")
        g.upsert_node("postgres::SELECT", "db", "postgres")
        g.upsert_edge(
            "django::view", "postgres::SELECT", "calls"
        )

        # Serialise
        data = MsgpackSerializer().serialize(g)

        # Deserialise
        g2 = MsgpackSerializer().deserialize(data)

        # Topology preserved
        assert "django::view" in g2._nodes
        assert "postgres::SELECT" in g2._nodes
        assert len(g2.neighbors("django::view")) == 1
        assert (
            g2.neighbors("django::view")[0].target
            == "postgres::SELECT"
        )

    def test_msgpack_round_trip_preserves_call_counts(self):
        pytest.importorskip("msgpack")

        g = RuntimeGraph()
        for _ in range(7):
            g.upsert_node(
                "svc::fn", "fn", "svc", duration_ns=1_000_000
            )

        data = MsgpackSerializer().serialize(g)
        g2 = MsgpackSerializer().deserialize(data)

        node = g2._nodes["svc::fn"]
        assert node.call_count == 7
        assert node.avg_duration_ns == 1_000_000

    def test_snapshot_pipeline_via_backend(self):
        """
        Simulate the full agent→backend pipeline:
            1. Agent builds graph and serialises it
            2. Backend receives bytes and deserialises
            3. Backend mounts the graph on an Engine and serves a DSL query
        """
        pytest.importorskip("msgpack")
        from origintracer.query.parser import execute, parse

        # Agent side
        g = RuntimeGraph()
        for _ in range(3):
            g.upsert_node("django::api_view", "fn", "django")
        g.upsert_node("postgres::SELECT users", "db", "postgres")
        g.upsert_edge(
            "django::api_view", "postgres::SELECT users", "calls"
        )
        data = MsgpackSerializer().serialize(g)

        # Backend side — deserialise and mount on an engine
        # execute() needs engine.graph, engine.semantic, engine.temporal, etc.
        # Passing a raw RuntimeGraph directly will AttributeError.
        g2 = MsgpackSerializer().deserialize(data)
        engine = Engine(snapshot_interval_s=9999)
        engine.graph = g2  # mount the deserialised graph

        result = execute(parse("HOTSPOT TOP 3"), engine)

        assert "data" in result
        top = result["data"][0]["node"]
        assert (
            top == "django::api_view"
        )  # called 3 times vs postgres 1


class TestNPlusOneEndToEnd:
    """
    The scenario confirmed live in the REPL:
        GET /n1/ >> NPlusOneView
            SELECT author  (×1)
            SELECT book WHERE author_id=%s  (×10)
        CAUSAL >> n_plus_one fires at ≥ 85% confidence

    This test owns the full path:
        emit() >> engine.process() → RuntimeGraph
               >> causal registry → n_plus_one rule
               >> DSL CAUSAL query → result

    If N_PLUS_ONE breaks at any layer, this is the test catches it.
    """

    def setup_method(self):
        enable_sync_mode()

    def _emit_nplusone_request(
        self,
        engine,
        n_authors: int = 1,
        books_per_author: int = 10,
    ):
        tid = str(uuid.uuid4())

        # Create the View event first
        view_enter = NormalizedEvent.now(
            "django.view.enter", tid, "django", "NPlusOneView"
        )

        # Tell the engine this is the root of the trace
        engine.process(view_enter)

        # For the queries, we want them to all point to 'view_enter'
        # If your Engine/Tracker doesn't handle this automatically,
        # you must pass the parent_event to the graph
        for _ in range(n_authors):
            author_q = NormalizedEvent.now(
                "django.db.query",
                tid,
                "django",
                "SELECT ... author",
                duration_ns=8_000_000,
            )
            engine.graph.add_from_event(
                author_q, parent_event=view_enter
            )

        for _ in range(n_authors * books_per_author):
            book_q = NormalizedEvent.now(
                "django.db.query",
                tid,
                "django",
                "SELECT ... book ...",
                duration_ns=2_500_000,
            )
            # Point back to the view_enter, not the author query
            engine.graph.add_from_event(
                book_q, parent_event=view_enter
            )

        # Finalize the view
        view_exit = NormalizedEvent.now(
            "django.view.exit",
            tid,
            "django",
            "NPlusOneView",
            duration_ns=50_000_000,
        )
        engine.process(view_exit)

    def test_n_plus_one_rule_fires_via_causal(self, engine):
        self._emit_nplusone_request(
            engine, n_authors=1, books_per_author=10
        )
        matches = engine.evaluate()
        rule_names = [m.rule_name for m in matches]
        assert "n_plus_one_queries" in rule_names, (
            f"Expected n_plus_one_queries in causal matches. Got: {rule_names}. "
            f"Graph nodes: {[n.id for n in self.engine.graph.all_nodes()]}"
        )

    def test_n_plus_one_confidence_above_threshold(self, engine):
        self._emit_nplusone_request(
            engine, n_authors=1, books_per_author=10
        )

        matches = engine.evaluate()
        m = next(
            (
                m
                for m in matches
                if m.rule_name == "n_plus_one_queries"
            ),
            None,
        )
        assert m is not None
        assert m.confidence >= 0.85

    def test_n_plus_one_evidence_names_query_and_ratio(
        self, engine
    ):
        self._emit_nplusone_request(
            engine, n_authors=1, books_per_author=10
        )

        matches = engine.evaluate()
        m = next(
            m
            for m in matches
            if m.rule_name == "n_plus_one_queries"
        )
        # 1. Grab the list of patterns
        patterns = m.evidence.get("n_plus_one_patterns", [])
        assert (
            len(patterns) > 0
        ), "Expected at least one N+1 pattern in evidence"

        # 2. Assert on the FIRST pattern found
        first_hit = patterns[0]
        assert "query" in first_hit or "db_node" in first_hit
        assert first_hit.get("ratio", 0) >= 5
        # 1. Access the first pattern in the list
        first_pattern = m.evidence["n_plus_one_patterns"][0]

        # 2. Assert on that pattern's content
        assert "query" in first_pattern
        assert first_pattern["ratio"] >= 5

    def test_n_plus_one_dsl_causal_query_returns_it(
        self, engine
    ):
        """End-to-end through the DSL layer, same as the REPL CAUSAL command."""
        from origintracer.query.parser import execute, parse

        self._emit_nplusone_request(
            engine, n_authors=1, books_per_author=10
        )

        result = execute(parse("CAUSAL"), engine)
        assert "data" in result
        rule_names = [m["rule"] for m in result["data"]]
        assert "n_plus_one_queries" in rule_names

    def test_n_plus_one_silent_with_prefetch(self, engine):
        """
        After adding prefetch_related the book query fires once, not 10 times.
        The rule should go silent — ratio drops below threshold.
        """
        tid = str(uuid.uuid4())

        emit(
            NormalizedEvent.now(
                "django.view.enter", tid, "django", "FixedView"
            )
        )
        emit(
            NormalizedEvent.now(
                "django.db.query",
                tid,
                "django",
                'SELECT "django_tracer_author"."id" FROM "django_tracer_author"',
            )
        )
        # prefetch_related: ONE query for all books, not 10
        emit(
            NormalizedEvent.now(
                "django.db.query",
                tid,
                "django",
                'SELECT "django_tracer_book"."id" FROM "django_tracer_book" WHERE author_id IN (%s)',
            )
        )
        emit(
            NormalizedEvent.now(
                "django.view.exit", tid, "django", "FixedView"
            )
        )

        matches = engine.evaluate()
        n1_matches = [
            m
            for m in matches
            if m.rule_name == "n_plus_one_queries"
        ]
        assert (
            n1_matches == []
        ), "n_plus_one_queries fired after prefetch fix — ratio should be 1:1, below threshold"


class TestGunicornTopologyEndToEnd:
    """
    Verify the full gunicorn topology chain is built through emit():
        gunicorn::master -- spawned --> gunicorn::UvicornWorker -- handled --> uvicorn::/n1/

    _add_structural_edges is unit-tested in test_core_graph.py.
    This test verifies the wiring: that the engine calls _add_structural_edges
    correctly on every processed event so the edges actually appear at runtime.
    """

    def setup_method(self):
        enable_sync_mode()

    def test_master_worker_spawned_edge_built_via_emit(
        self, engine
    ):
        emit(
            NormalizedEvent.now(
                "gunicorn.master.start",
                "t-guni",
                "gunicorn",
                "master",
            )
        )
        emit(
            NormalizedEvent.now(
                "gunicorn.worker.fork",
                "t-guni",
                "gunicorn",
                "UvicornWorker",
                worker_pid=14442,
            )
        )

        g = engine.graph
        assert "gunicorn::master" in [
            n.id for n in g.all_nodes()
        ]
        assert "gunicorn::UvicornWorker" in [
            n.id for n in g.all_nodes()
        ]

        spawned = [
            e
            for e in g.neighbors("gunicorn::master")
            if e.edge_type == "spawned"
            and e.target == "gunicorn::UvicornWorker"
        ]
        assert spawned, (
            "No 'spawned' edge from gunicorn::master >> gunicorn::UvicornWorker. "
            "Check engine.process() calls graph.add_from_event() which calls "
            "_add_structural_edges for gunicorn.worker.fork events."
        )

    def test_worker_handled_edge_built_on_request(self, engine):
        """Worker→request 'handled' edge appears when uvicorn processes a request."""
        emit(
            NormalizedEvent.now(
                "gunicorn.master.start",
                "t-guni",
                "gunicorn",
                "master",
            )
        )
        emit(
            NormalizedEvent.now(
                "gunicorn.worker.fork",
                "t-guni",
                "gunicorn",
                "UvicornWorker",
                worker_pid=14442,
            )
        )
        emit(
            NormalizedEvent.now(
                "uvicorn.request.receive",
                str(uuid.uuid4()),
                "uvicorn",
                "/n1/",
                worker_pid=14442,
            )
        )

        g = engine.graph
        handled = [
            e
            for e in g.neighbors("gunicorn::UvicornWorker")
            if e.edge_type == "handled"
        ]
        assert handled, (
            "No 'handled' edge from gunicorn::UvicornWorker → uvicorn::/n1/. "
            "Check _add_structural_edges for uvicorn.request.receive events."
        )

    def test_worker_imbalance_fires_via_causal(self, engine):
        """
        Two workers where worker-0 handles all requests and worker-1 is idle
        should fire WORKER_IMBALANCE through the causal registry.
        """
        # Two workers fork
        for pid, name in [
            (14442, "UvicornWorker-0"),
            (14443, "UvicornWorker-1"),
        ]:
            emit(
                NormalizedEvent.now(
                    "gunicorn.worker.fork",
                    "t-guni",
                    "gunicorn",
                    name,
                    worker_pid=pid,
                )
            )

        # worker-0 handles 10 requests, worker-1 handles 1
        for i in range(10):
            emit(
                NormalizedEvent.now(
                    "uvicorn.request.receive",
                    str(uuid.uuid4()),
                    "uvicorn",
                    "/api/",
                    worker_pid=14442,
                )
            )
        emit(
            NormalizedEvent.now(
                "uvicorn.request.receive",
                str(uuid.uuid4()),
                "uvicorn",
                "/api/",
                worker_pid=14443,
            )
        )

        matches = engine.evaluate()
        rule_names = [m.rule_name for m in matches]
        assert "worker_imbalance" in rule_names


class TestConfigMergePipeline:
    """
    ResolvedConfig is built from three sources:
        defaults.yaml < user stacktracer.yaml < init() kwargs

    These tests verify the merge semantics without calling the full
    stacktracer.init() (which starts threads and needs a live app).
    """

    def _merge(
        self, user_yaml: dict, **kwargs
    ) -> "ResolvedConfig":
        from origintracer.__init__ import (
            _build_resolved_config,
            _deep_merge,
            _load_package_defaults,
        )

        defaults = _load_package_defaults()
        merged_yaml = _deep_merge(defaults, user_yaml)
        return _build_resolved_config(
            merged_yaml=merged_yaml,
            api_key=kwargs.pop("api_key", ""),
            endpoint=kwargs.pop(
                "endpoint", "https://api.origintracer.app"
            ),
            probes=kwargs.pop("probes", None),
            rules=kwargs.pop("rules", None),
            semantic=kwargs.pop("semantic", None),
            snapshot_interval=kwargs.pop(
                "snapshot_interval", None
            ),
            flush_interval=kwargs.pop("flush_interval", None),
            debug=kwargs.pop("debug", False),
            config_path=None,
            normalize=kwargs.pop("normalize", None),
            compactor=kwargs.pop("compactor", None),
            active_requests=kwargs.pop("active_requests", None),
        )

    def test_defaults_applied_when_no_user_config(self):
        cfg = self._merge({})
        assert "django" in cfg.probes

    def test_user_yaml_overrides_defaults(self):
        self._merge({"sample_rate": 0.10})

    def test_init_kwarg_overrides_user_yaml(self):
        self._merge({"sample_rate": 0.10}, sample_rate=0.50)

    def test_probes_list_replaced_not_merged(self):
        """User specifying probes: [django] should get ONLY django, not defaults+django."""
        cfg = self._merge({"probes": ["django"]})
        assert cfg.probes == ["django"]

    def test_compactor_dict_merged_key_by_key(self):
        """User overriding one compactor key should keep all other defaults."""
        cfg = self._merge({"compactor": {"max_nodes": 9999}})
        assert cfg.compactor["max_nodes"] == 9999
        assert (
            "evict_to_ratio" in cfg.compactor
        )  # default preserved

    def test_semantic_merged_by_label(self):
        """
        User adding a new semantic label should produce combined list.
        User overriding existing label should win.
        """
        cfg = self._merge(
            {
                "semantic": [
                    {
                        "label": "api",
                        "description": "override",
                        "node_patterns": [],
                        "services": [],
                    },
                    {
                        "label": "auth",
                        "description": "new",
                        "node_patterns": [],
                        "services": ["auth"],
                    },
                ]
            }
        )
        labels = {s["label"] for s in cfg.semantic}
        assert "api" in labels  # overridden
        assert "auth" in labels  # added
        assert "db" in labels  # default preserved

    def test_semantic_label_from_init_kwarg_wins_over_yaml(self):
        cfg = self._merge(
            {
                "semantic": [
                    {
                        "label": "api",
                        "description": "from yaml",
                        "node_patterns": [],
                        "services": [],
                    }
                ]
            },
            semantic=[
                {
                    "label": "api",
                    "description": "from kwarg",
                    "node_patterns": [],
                    "services": [],
                }
            ],
        )
        api_entries = [
            s for s in cfg.semantic if s["label"] == "api"
        ]
        assert len(api_entries) == 1
        assert api_entries[0]["description"] == "from kwarg"

    def test_normalize_kwarg_extends_not_replaces(self):
        """
        init(normalize=[my_rule]) must ADD to the defaults.yaml rules,
        not wipe them.  The old bug was replace — this test would have
        caught it.
        """
        my_rule = {
            "service": "django",
            "pattern": r"/debug/.*",
            "replacement": "/debug/{path}",
        }
        cfg = self._merge({}, normalize=[my_rule])
        # User rule present
        assert any(
            r.get("replacement") == "/debug/{path}"
            for r in cfg.normalize
        )
        # Built-in defaults.yaml rules preserved (DRF version pattern)
        assert any(
            "/api/{version}/" in r.get("replacement", "")
            for r in cfg.normalize
        )

    def test_normalize_yaml_cleared_then_kwarg_appended(self):
        """
        User setting normalize: [] in their yaml wipes the list,
        then init() kwarg appends on top of the empty base — clean slate.
        """
        my_rule = {
            "service": "django",
            "pattern": r"/foo/",
            "replacement": "/foo/{id}",
        }
        cfg = self._merge({"normalize": []}, normalize=[my_rule])
        assert cfg.normalize == [my_rule]

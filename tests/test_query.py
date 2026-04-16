from __future__ import annotations

import pytest

from .conftest import evt


class TestQueryParser:

    def _parse(self, query: str):
        from origintracer.query.parser import parse

        return parse(query)

    def test_show_latency_with_service_filter(self):
        q = self._parse('SHOW latency WHERE service = "django"')
        assert q.verb == "SHOW"
        assert q.metric == "latency"
        assert q.filters["service"] == "django"

    def test_show_events_with_probe_and_limit(self):
        q = self._parse(
            'SHOW events WHERE probe = "db.query.start" LIMIT 50'
        )
        assert q.filters["probe"] == "db.query.start"
        assert q.limit == 50

    def test_show_graph_no_filters(self):
        q = self._parse("SHOW graph")
        assert q.verb == "SHOW"
        assert q.metric == "graph"
        assert q.filters == {}

    def test_hotspot_with_n(self):
        q = self._parse("HOTSPOT TOP 20")
        assert q.verb == "HOTSPOT"
        assert q.limit == 20

    def test_hotspot_default_limit(self):
        q = self._parse("HOTSPOT")
        assert q.verb == "HOTSPOT"
        assert (
            q.limit is None or q.limit > 0
        )  # must have a sensible default

    def test_trace_verb(self):
        q = self._parse("TRACE abc123def456")
        assert q.verb == "TRACE"
        assert q.filters["trace_id"] == "abc123def456"

    def test_blame_with_system(self):
        q = self._parse('BLAME WHERE system = "export"')
        assert q.verb == "BLAME"
        assert q.filters["system"] == "export"

    def test_diff_since_label(self):
        q = self._parse("DIFF SINCE deployment")
        assert q.verb == "DIFF"
        assert q.filters["since"] == "deployment"

    def test_causal_no_args(self):
        q = self._parse("CAUSAL")
        assert q.verb == "CAUSAL"

    def test_causal_with_tag_filter(self):
        q = self._parse('CAUSAL WHERE tags = "latency"')
        assert q.verb == "CAUSAL"
        assert "latency" in q.filters.get("tags", "")

    def test_empty_query_raises(self):
        from origintracer.query.parser import parse

        with pytest.raises(ValueError, match="[Ee]mpty"):
            parse("")

    def test_unknown_verb_raises(self):
        from origintracer.query.parser import parse

        with pytest.raises(ValueError, match="[Uu]nknown verb"):
            parse("DELETE everything")

    def test_whitespace_only_raises(self):
        from origintracer.query.parser import parse

        with pytest.raises(ValueError):
            parse("   ")


class TestQueryExecutor:

    def _run(self, query_str: str, engine):
        from origintracer.query.parser import execute, parse

        return execute(parse(query_str), engine)

    def test_show_latency_returns_matching_nodes(
        self, engine, trace_id
    ):
        engine.process(
            evt(
                service="django",
                name="orders_view",
                trace_id=trace_id,
                duration_ns=5_000_000,
            )
        )
        result = self._run(
            'SHOW latency WHERE service = "django"', engine
        )
        assert "data" in result
        assert any(
            r["service"] == "django" for r in result["data"]
        )

    def test_show_latency_fails_on_unsupported_operator(
        self, engine, trace_id
    ):
        engine.process(
            evt(
                service="django",
                name="orders_view",
                trace_id=trace_id,
                duration_ns=5_000_000,
            )
        )

        # Using '>' should fail since only '=' is supported
        with pytest.raises(
            ValueError, match="Unsupported operator"
        ):
            self._run(
                'SHOW latency WHERE service > "django"', engine
            )

        # Using '<' should also fail
        with pytest.raises(
            ValueError, match="Unsupported operator"
        ):
            self._run(
                'SHOW latency WHERE service < "django"', engine
            )

        # '=' still works
        result = self._run(
            'SHOW latency WHERE service = "django"', engine
        )
        assert "data" in result
        assert any(
            r["service"] == "django" for r in result["data"]
        )

    def test_show_latency_excludes_other_services(
        self, engine, trace_id
    ):
        engine.process(
            evt(service="django", name="view", trace_id=trace_id)
        )
        engine.process(
            evt(
                service="postgres",
                name="SELECT",
                trace_id=trace_id,
            )
        )
        result = self._run(
            'SHOW latency WHERE service = "django"', engine
        )
        services = [r["service"] for r in result["data"]]
        assert "postgres" not in services

    def test_show_latency_avg_ms_populated_after_duration_fix(
        self, engine, trace_id
    ):
        """
        duration_ns must land on event.duration_ns (not event.metadata) after
        the NormalizedEvent.now() fix. avg_ms must be non-None for nodes that
        received exit events carrying duration_ns.
        """
        engine.process(
            evt(
                service="django",
                name="view",
                trace_id=trace_id,
                duration_ns=10_000_000,  # 10ms explicit
            )
        )
        result = self._run(
            'SHOW latency WHERE service = "django"', engine
        )
        row = next(
            (r for r in result["data"] if "view" in r["node"]),
            None,
        )
        assert row is not None
        assert row["avg_duration_ms"] is not None
        assert row["avg_duration_ms"] > 0

    def test_show_nodes_returns_all_fields(
        self, engine, trace_id
    ):
        engine.process(
            evt(service="django", name="view", trace_id=trace_id)
        )
        result = self._run("SHOW nodes", engine)
        assert "data" in result
        row = result["data"][0]
        for field in (
            "id",
            "service",
            "type",
            "call_count",
            "first_seen",
            "last_seen",
        ):
            assert (
                field in row
            ), f"Missing field '{field}' in SHOW nodes row"

    def test_show_nodes_service_filter(self, engine, trace_id):
        engine.process(
            evt(service="django", name="view", trace_id=trace_id)
        )
        engine.process(
            evt(
                service="postgres",
                name="SELECT",
                trace_id=trace_id,
            )
        )
        result = self._run(
            'SHOW nodes WHERE service = "django"', engine
        )
        assert all(
            r["service"] == "django" for r in result["data"]
        )

    def test_show_edges_returns_source_target_type(
        self, engine, trace_id
    ):
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
        result = self._run("SHOW edges", engine)
        assert "data" in result
        if result["data"]:
            e = result["data"][0]
            assert "source" in e
            assert "target" in e
            assert "type" in e

    def test_show_status_returns_graph_counts(
        self, engine, trace_id
    ):
        engine.process(
            evt(service="django", name="view", trace_id=trace_id)
        )
        result = self._run("SHOW status", engine)
        data = result.get("data", result)
        assert "graph_nodes" in data
        assert data["graph_nodes"] >= 1

    def test_show_status_includes_pid_and_uptime(self, engine):
        result = self._run("SHOW status", engine)
        data = result.get("data", result)
        assert "pid" in data
        assert "uptime_s" in data

    def test_show_active_returns_list(self, engine):
        result = self._run("SHOW active", engine)
        assert "data" in result
        assert isinstance(result["data"], list)

    def test_show_probes_returns_list(self, engine):
        result = self._run("SHOW probes", engine)
        assert "data" in result
        assert isinstance(result["data"], list)

    def test_show_rules_includes_new_rules(self, engine):
        result = self._run("SHOW rules", engine)
        assert "data" in result
        names = result["data"]
        assert "db_query_hotspot" in names
        assert "n_plus_one_queries" in names
        assert "worker_imbalance" in names

    def test_show_semantic_lists_labels_with_descriptions(
        self, engine
    ):
        result = self._run("SHOW semantic", engine)
        assert "data" in result
        labels = [item["label"] for item in result["data"]]
        assert "export" in labels
        # Each entry must have a description key even if empty
        for item in result["data"]:
            assert "label" in item
            assert "description" in item

    def test_show_graph_scoped_by_system(self, engine, trace_id):
        """WHERE system = "export" scopes graph to export nodes only."""
        engine.process(
            evt(
                service="django",
                name="handle_export",
                trace_id=trace_id,
            )
        )
        engine.process(
            evt(
                service="postgres",
                name="SELECT",
                trace_id=trace_id,
            )
        )
        result = self._run(
            'SHOW graph WHERE system = "export"', engine
        )
        assert "data" in result
        node_ids = [n["id"] for n in result["data"]["nodes"]]
        assert "django::handle_export" in node_ids
        assert "postgres::SELECT" not in node_ids

    def test_show_nodes_unknown_system_returns_error_with_available(
        self, engine
    ):
        """system= must return error AND list available labels."""
        result = self._run(
            'SHOW nodes WHERE system = "xyz_unknown_zzz"', engine
        )
        assert "error" in result
        assert "available_labels" in result

    def test_show_latency_service_falls_through_to_literal_if_no_semantic_match(
        self, engine, trace_id
    ):
        """
        WHERE service = "django" — "django" is a real service name.
        Even if semantic resolution returns nothing, literal fallback must work.
        """
        engine.process(
            evt(service="django", name="view", trace_id=trace_id)
        )
        result = self._run(
            'SHOW latency WHERE service = "django"', engine
        )
        assert "data" in result
        assert any(
            r["service"] == "django" for r in result["data"]
        )

    def test_hotspot_returns_bounded_list(
        self, engine, trace_id
    ):
        for _ in range(10):
            engine.process(
                evt(
                    service="django",
                    name="view",
                    trace_id=trace_id,
                )
            )
        result = self._run("HOTSPOT TOP 3", engine)
        assert "data" in result
        assert len(result["data"]) <= 3

    def test_hotspot_sorted_by_call_count(
        self, engine, trace_id
    ):
        for _ in range(5):
            engine.process(
                evt(
                    service="django",
                    name="hot",
                    trace_id=trace_id,
                )
            )
        engine.process(
            evt(service="django", name="cold", trace_id=trace_id)
        )
        result = self._run("HOTSPOT TOP 10", engine)
        names = [r["node"] for r in result["data"]]
        assert names.index("django::hot") < names.index(
            "django::cold"
        )

    def test_hotspot_graceful_without_engine_method(
        self, engine, trace_id
    ):
        """Executor falls back to graph sort if engine.hotspots() doesn't exist."""
        engine.process(
            evt(service="django", name="view", trace_id=trace_id)
        )
        if hasattr(engine, "hotspots"):
            (
                delattr(engine.__class__, "hotspots")
                if "hotspots" in engine.__class__.__dict__
                else None
            )
        result = self._run("HOTSPOT TOP 5", engine)
        assert "data" in result

    def test_causal_returns_data_key(self, engine):
        result = self._run("CAUSAL", engine)
        assert "data" in result

    def test_causal_tag_filter_accepted(self, engine):
        result = self._run('CAUSAL WHERE tags = "n+1"', engine)
        assert "data" in result  # may be empty — must not crash

    def test_blame_known_system_returns_data(
        self, engine, trace_id
    ):
        engine.process(
            evt(
                service="django",
                name="/api/orders/",
                trace_id=trace_id,
            )
        )
        result = self._run('BLAME WHERE system = "api"', engine)
        assert (
            "data" in result or "error" in result
        )  # must not crash

    def test_blame_unknown_system_returns_error(self, engine):
        result = self._run(
            'BLAME WHERE system = "completely_unknown"', engine
        )
        assert "error" in result

    def test_trace_returns_critical_path(self, engine, trace_id):
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
        result = self._run(f"TRACE {trace_id}", engine)
        assert result.get("trace_id") == trace_id
        assert result.get("stages") == 3

    def test_trace_unknown_returns_error_or_empty(self, engine):
        result = self._run("TRACE nonexistent-trace-000", engine)
        assert "error" in result or result.get("stages", 0) == 0

    def test_diff_returns_dict(self, engine, trace_id):
        engine.process(
            evt(service="django", name="fn_a", trace_id=trace_id)
        )
        engine.snapshot()
        engine.process(
            evt(service="django", name="fn_b", trace_id=trace_id)
        )
        engine.snapshot()
        result = self._run("DIFF SINCE deployment", engine)
        assert isinstance(result, dict)

    def test_diff_graceful_when_temporal_methods_missing(
        self, engine
    ):
        """Executor must not crash if temporal store is partially implemented."""
        result = self._run("DIFF", engine)
        assert isinstance(result, dict)
        # Must have the structural keys even if empty
        assert "new_edges" in result
        assert "removed_edges" in result

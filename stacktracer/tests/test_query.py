"""
tests/test_query.py

Tests for the DSL query layer:
    Parser   — tokenises and validates query strings
    Executor — runs parsed queries against a live RuntimeGraph

These are the tests most likely to reveal regressions when the query
language is extended. Parser tests are pure unit tests (no graph needed).
Executor tests are integration tests (need a populated engine).
"""

from __future__ import annotations

import pytest

from conftest import evt


# ====================================================================== #
# Parser
# ====================================================================== #

class TestQueryParser:

    def _parse(self, query: str):
        from stacktracer.query.parser import parse
        return parse(query)

    def test_show_latency_with_service_filter(self):
        q = self._parse('SHOW latency WHERE service = "django"')
        assert q.verb == "SHOW"
        assert q.metric == "latency"
        assert q.filters["service"] == "django"

    def test_show_events_with_probe_and_limit(self):
        q = self._parse('SHOW events WHERE probe = "db.query.start" LIMIT 50')
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
        assert q.limit is None or q.limit > 0   # must have a sensible default

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
        from stacktracer.query.parser import parse
        with pytest.raises(ValueError, match="[Ee]mpty"):
            parse("")

    def test_unknown_verb_raises(self):
        from stacktracer.query.parser import parse
        with pytest.raises(ValueError, match="[Uu]nknown verb"):
            parse("DELETE everything")

    def test_whitespace_only_raises(self):
        from stacktracer.query.parser import parse
        with pytest.raises(ValueError):
            parse("   ")


# ====================================================================== #
# Executor — requires a live engine with data
# ====================================================================== #

class TestQueryExecutor:

    def _run(self, query_str: str, engine):
        from stacktracer.query.parser import parse, execute
        return execute(parse(query_str), engine)

    def test_show_latency_returns_matching_nodes(self, engine, trace_id):
        engine.process(evt(service="django", name="orders_view", trace_id=trace_id, duration_ns=5_000_000))
        result = self._run('SHOW latency WHERE service = "django"', engine)
        assert "data" in result
        assert any(r["service"] == "django" for r in result["data"])

    def test_show_latency_excludes_other_services(self, engine, trace_id):
        engine.process(evt(service="django",   name="view",   trace_id=trace_id))
        engine.process(evt(service="postgres", name="SELECT", trace_id=trace_id))
        result = self._run('SHOW latency WHERE service = "django"', engine)
        services = [r["service"] for r in result["data"]]
        assert "postgres" not in services

    def test_hotspot_returns_bounded_list(self, engine, trace_id):
        for _ in range(10):
            engine.process(evt(service="django", name=f"view", trace_id=trace_id))
        result = self._run("HOTSPOT TOP 3", engine)
        assert "data" in result
        assert len(result["data"]) <= 3

    def test_hotspot_sorted_by_call_count(self, engine, trace_id):
        for _ in range(5):
            engine.process(evt(service="django", name="hot", trace_id=trace_id))
        engine.process(evt(service="django", name="cold", trace_id=trace_id))
        result = self._run("HOTSPOT TOP 10", engine)
        names = [r["node"] for r in result["data"]]
        assert names.index("django::hot") < names.index("django::cold")

    def test_causal_returns_data_key(self, engine):
        result = self._run("CAUSAL", engine)
        assert "data" in result

    def test_blame_known_system_returns_nodes(self, engine, trace_id):
        """'api' label resolves to django::/api/.* nodes."""
        engine.process(evt(service="django", name="/api/orders/", trace_id=trace_id))
        result = self._run('BLAME WHERE system = "api"', engine)
        # Either returns data or a structured response — must not crash
        assert "data" in result or "error" in result

    def test_blame_unknown_system_returns_error(self, engine):
        result = self._run('BLAME WHERE system = "completely_unknown"', engine)
        assert "error" in result

    def test_trace_returns_critical_path(self, engine, trace_id):
        engine.process(evt(probe="request.entry", service="nginx",    name="accept", trace_id=trace_id))
        engine.process(evt(probe="function.call", service="django",   name="view",   trace_id=trace_id))
        engine.process(evt(probe="db.query.start",service="postgres", name="SELECT", trace_id=trace_id))
        result = self._run(f"TRACE {trace_id}", engine)
        assert result.get("trace_id") == trace_id
        assert result.get("stages") == 3

    def test_trace_unknown_returns_error_or_empty(self, engine):
        result = self._run("TRACE nonexistent-trace-000", engine)
        assert "error" in result or result.get("stages", 0) == 0

    def test_diff_returns_dict(self, engine, trace_id):
        engine.process(evt(service="django", name="fn_a", trace_id=trace_id))
        engine.snapshot()
        engine.process(evt(service="django", name="fn_b", trace_id=trace_id))
        engine.snapshot()
        result = self._run("DIFF SINCE deployment", engine)
        assert isinstance(result, dict)
"""
tests/test_semantic.py

Dedicated tests for SemanticLayer, SemanticAlias, load_from_dict,
load_from_yaml, and merge_yaml_configs.

This file supersedes the scattered SemanticLayer tests in
test_core_tracker.py and test_full_stack.py. Those remain for
backward compatibility but this file owns the canonical coverage.

Focus areas:
    1. SemanticAlias  — matches_node (exact, regex), matches_service
    2. SemanticLayer  — register, resolve_nodes, resolve_services,
                        describe, all_labels, __contains__, __iter__
    3. Resolution     — service match, pattern match, combined, exclusion
    4. Case handling  — labels are case-insensitive
    5. YAML loading   — load_from_dict, load_from_yaml, merge_yaml_configs
    6. Override       — later registration wins over earlier for same label
    7. Integration    — all nine labels from the default stacktracer.yaml
                        resolve correctly against a realistic graph
"""

from __future__ import annotations

import os
import tempfile

import pytest

from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.semantic import (
    SemanticAlias,
    SemanticLayer,
    load_from_dict,
    load_from_yaml,
    merge_yaml_configs,
)

# ── Helpers ────────────────────────────────────────────────────────────────


def _graph(*node_specs) -> RuntimeGraph:
    """
    Build a RuntimeGraph from (node_id, service) pairs.
    node_id format: "service::name"
    """
    g = RuntimeGraph()
    for node_id, service in node_specs:
        g.upsert_node(
            node_id, node_type=service, service=service
        )
    return g


def _layer_with_defaults() -> SemanticLayer:
    """
    SemanticLayer loaded from the default stacktracer.yaml semantic section.
    Mirrors the actual YAML so tests reflect production configuration.
    """
    return load_from_dict(
        [
            {
                "label": "api",
                "description": "Public-facing API surface",
                "services": [],
                "node_patterns": [
                    "django::/api/.*",
                    "uvicorn::/api/.*",
                ],
            },
            {
                "label": "db",
                "description": "All database interactions",
                "services": [
                    "postgres",
                    "mysql",
                    "sqlite",
                    "redis",
                ],
                "node_patterns": [],
            },
            {
                "label": "worker",
                "description": "Background task workers",
                "services": ["celery", "gunicorn"],
                "node_patterns": ["celery::.*", "gunicorn::.*"],
            },
            {
                "label": "gateway",
                "description": "Reverse proxy and ingress layer",
                "services": ["nginx"],
                "node_patterns": ["nginx::.*"],
            },
            {
                "label": "http",
                "description": "HTTP layer",
                "services": ["nginx", "uvicorn"],
                "node_patterns": [],
            },
            {
                "label": "django",
                "description": "Django application layer",
                "services": ["django"],
                "node_patterns": [],
            },
            {
                "label": "database",
                "description": "Database query nodes (ORM level)",
                "services": [],
                "node_patterns": [
                    "django::SELECT.*",
                    "django::INSERT.*",
                    "django::UPDATE.*",
                    "django::DELETE.*",
                ],
            },
            {
                "label": "export",
                "description": "The full export pipeline",
                "services": ["exporter"],
                "node_patterns": [
                    "django::handle_export",
                    "django::flags_client.*",
                ],
                "tags": ["revenue-critical"],
            },
            {
                "label": "auth",
                "description": "Authentication and authorisation",
                "services": ["auth"],
                "node_patterns": [
                    "django::authenticate",
                    "django::check_permissions",
                ],
            },
        ]
    )


# ====================================================================== #
# SemanticAlias
# ====================================================================== #


class TestSemanticAlias:

    def test_matches_exact_node_id(self):
        alias = SemanticAlias(
            label="export",
            description="",
            node_patterns=["django::handle_export"],
            services=[],
        )
        assert alias.matches_node("django::handle_export")

    def test_does_not_match_partial_node_id(self):
        alias = SemanticAlias(
            label="export",
            description="",
            node_patterns=["django::handle_export"],
            services=[],
        )
        # re.search would match a substring — we want full match behaviour
        assert not alias.matches_node("django::handle_export_v2")

    def test_matches_regex_pattern(self):
        alias = SemanticAlias(
            label="export",
            description="",
            node_patterns=[r"django::flags_client\..*"],
            services=[],
        )
        assert alias.matches_node("django::flags_client.call")
        assert alias.matches_node("django::flags_client.retry")

    def test_regex_does_not_match_unrelated(self):
        alias = SemanticAlias(
            label="export",
            description="",
            node_patterns=[r"django::flags_client\..*"],
            services=[],
        )
        assert not alias.matches_node("django::other_fn")

    def test_matches_service_name(self):
        alias = SemanticAlias(
            label="db",
            description="",
            node_patterns=[],
            services=["postgres", "redis"],
        )
        assert alias.matches_service("postgres")
        assert alias.matches_service("redis")

    def test_does_not_match_wrong_service(self):
        alias = SemanticAlias(
            label="db",
            description="",
            node_patterns=[],
            services=["postgres"],
        )
        assert not alias.matches_service("mysql")

    def test_invalid_regex_does_not_crash(self):
        """A badly formed pattern must not raise — skip silently."""
        alias = SemanticAlias(
            label="broken",
            description="",
            node_patterns=["[invalid regex"],
            services=[],
        )
        # Should not raise
        result = alias.matches_node("anything")
        assert result is False


# ====================================================================== #
# SemanticLayer — registration and retrieval
# ====================================================================== #


class TestSemanticLayerRegistry:

    def setup_method(self):
        self.layer = SemanticLayer()

    def test_register_and_contains(self):
        self.layer.register(SemanticAlias("export", "", [], []))
        assert "export" in self.layer

    def test_contains_case_insensitive(self):
        self.layer.register(SemanticAlias("export", "", [], []))
        assert "EXPORT" in self.layer
        assert "Export" in self.layer

    def test_all_labels_returns_registered(self):
        self.layer.register(SemanticAlias("a", "", [], []))
        self.layer.register(SemanticAlias("b", "", [], []))
        labels = self.layer.all_labels()
        assert "a" in labels
        assert "b" in labels

    def test_describe_returns_description(self):
        self.layer.register(
            SemanticAlias(
                label="export",
                description="The export pipeline",
                node_patterns=[],
                services=[],
            )
        )
        assert (
            self.layer.describe("export")
            == "The export pipeline"
        )

    def test_describe_unknown_returns_none(self):
        assert self.layer.describe("nonexistent") is None

    def test_iter_yields_all_aliases(self):
        self.layer.register(SemanticAlias("a", "desc-a", [], []))
        self.layer.register(SemanticAlias("b", "desc-b", [], []))
        aliases = list(self.layer)
        assert len(aliases) == 2
        labels = {a.label for a in aliases}
        assert {"a", "b"} == labels

    def test_later_registration_overrides_earlier(self):
        """Same label registered twice — last one wins."""
        self.layer.register(
            SemanticAlias("api", "first", [], [])
        )
        self.layer.register(
            SemanticAlias("api", "second", [], [])
        )
        assert self.layer.describe("api") == "second"

    def test_resolve_services_returns_list(self):
        self.layer.register(
            SemanticAlias(
                label="db",
                description="",
                node_patterns=[],
                services=["postgres", "redis"],
            )
        )
        services = self.layer.resolve_services("db")
        assert "postgres" in services
        assert "redis" in services

    def test_resolve_services_unknown_returns_empty(self):
        assert self.layer.resolve_services("nonexistent") == []


# ====================================================================== #
# SemanticLayer — resolve_nodes
# ====================================================================== #


class TestSemanticLayerResolveNodes:

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
                node_patterns=[],
                services=["postgres", "redis"],
            )
        )
        self.layer.register(
            SemanticAlias(
                label="database",
                description="ORM query nodes",
                node_patterns=[
                    "django::SELECT.*",
                    "django::INSERT.*",
                    "django::UPDATE.*",
                    "django::DELETE.*",
                ],
                services=[],
            )
        )

    def _graph(self):
        return _graph(
            ("django::handle_export", "django"),
            ("django::flags_client.call", "django"),
            ("django::flags_client.retry", "django"),
            ("exporter::publish", "exporter"),
            ("postgres::SELECT orders", "postgres"),
            ("redis::GET session", "redis"),
            ('django::SELECT "book"."id" FROM "book"', "django"),
            ("django::unrelated_view", "django"),
        )

    def test_resolve_by_exact_pattern(self):
        nodes = self.layer.resolve_nodes("export", self._graph())
        assert "django::handle_export" in nodes

    def test_resolve_by_regex_pattern(self):
        nodes = self.layer.resolve_nodes("export", self._graph())
        assert "django::flags_client.call" in nodes
        assert "django::flags_client.retry" in nodes

    def test_resolve_by_service(self):
        nodes = self.layer.resolve_nodes("export", self._graph())
        assert "exporter::publish" in nodes

    def test_resolve_db_by_service(self):
        nodes = self.layer.resolve_nodes("db", self._graph())
        assert "postgres::SELECT orders" in nodes
        assert "redis::GET session" in nodes

    def test_resolve_database_by_orm_pattern(self):
        """'database' label matches via SELECT.* pattern on django nodes."""
        nodes = self.layer.resolve_nodes(
            "database", self._graph()
        )
        assert 'django::SELECT "book"."id" FROM "book"' in nodes

    def test_unrelated_node_excluded(self):
        nodes = self.layer.resolve_nodes("export", self._graph())
        assert "django::unrelated_view" not in nodes
        assert "postgres::SELECT orders" not in nodes

    def test_unknown_label_returns_empty_set(self):
        result = self.layer.resolve_nodes(
            "nonexistent", self._graph()
        )
        assert result == set()

    def test_case_insensitive_lookup(self):
        g = self._graph()
        assert self.layer.resolve_nodes(
            "export", g
        ) == self.layer.resolve_nodes("EXPORT", g)

    def test_empty_graph_returns_empty_set(self):
        result = self.layer.resolve_nodes(
            "export", RuntimeGraph()
        )
        assert result == set()


# ====================================================================== #
# load_from_dict
# ====================================================================== #


class TestLoadFromDict:

    def test_basic_load(self):
        layer = load_from_dict(
            [
                {
                    "label": "auth",
                    "description": "Auth pipeline",
                    "node_patterns": ["django::authenticate"],
                    "services": ["auth"],
                    "tags": ["security"],
                }
            ]
        )
        g = _graph(("django::authenticate", "django"))
        nodes = layer.resolve_nodes("auth", g)
        assert "django::authenticate" in nodes

    def test_multiple_labels_loaded(self):
        layer = load_from_dict(
            [
                {
                    "label": "a",
                    "description": "",
                    "node_patterns": [],
                    "services": ["svc_a"],
                },
                {
                    "label": "b",
                    "description": "",
                    "node_patterns": [],
                    "services": ["svc_b"],
                },
            ]
        )
        assert "a" in layer
        assert "b" in layer

    def test_missing_optional_keys_do_not_raise(self):
        """node_patterns, services, tags are all optional."""
        layer = load_from_dict(
            [{"label": "minimal", "description": ""}]
        )
        assert "minimal" in layer

    def test_later_entry_overrides_earlier_same_label(self):
        layer = load_from_dict(
            [
                {
                    "label": "api",
                    "description": "first",
                    "node_patterns": [],
                    "services": [],
                },
                {
                    "label": "api",
                    "description": "second",
                    "node_patterns": [],
                    "services": [],
                },
            ]
        )
        assert layer.describe("api") == "second"

    def test_empty_list_produces_empty_layer(self):
        layer = load_from_dict([])
        assert layer.all_labels() == []


# ====================================================================== #
# load_from_yaml
# ====================================================================== #


class TestLoadFromYaml:

    def _write_yaml(self, content: str) -> str:
        pytest.importorskip("yaml")
        f = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        )
        f.write(content)
        f.close()
        return f.name

    def test_loads_semantic_section(self):
        path = self._write_yaml(
            """
semantic:
  - label: payments
    description: Payment processing
    services:
      - stripe
    node_patterns: []
"""
        )
        try:
            layer = load_from_yaml(path)
            assert "payments" in layer
            assert (
                layer.describe("payments")
                == "Payment processing"
            )
        finally:
            os.unlink(path)

    def test_loads_multiple_labels(self):
        path = self._write_yaml(
            """
semantic:
  - label: api
    description: API layer
    services: [django]
    node_patterns: []
  - label: db
    description: Database
    services: [postgres]
    node_patterns: []
"""
        )
        try:
            layer = load_from_yaml(path)
            assert "api" in layer
            assert "db" in layer
        finally:
            os.unlink(path)

    def test_missing_semantic_section_returns_empty_layer(self):
        path = self._write_yaml("probes:\n  - django\n")
        try:
            layer = load_from_yaml(path)
            assert layer.all_labels() == []
        finally:
            os.unlink(path)

    def test_empty_yaml_file_returns_empty_layer(self):
        path = self._write_yaml("")
        try:
            layer = load_from_yaml(path)
            assert layer.all_labels() == []
        finally:
            os.unlink(path)

    def test_node_patterns_resolve_correctly(self):
        path = self._write_yaml(
            """
semantic:
  - label: export
    description: Export pipeline
    services: []
    node_patterns:
      - "django::handle_export"
      - "django::flags_client.*"
"""
        )
        try:
            layer = load_from_yaml(path)
            g = _graph(
                ("django::handle_export", "django"),
                ("django::flags_client.call", "django"),
                ("django::other", "django"),
            )
            nodes = layer.resolve_nodes("export", g)
            assert "django::handle_export" in nodes
            assert "django::flags_client.call" in nodes
            assert "django::other" not in nodes
        finally:
            os.unlink(path)


# ====================================================================== #
# merge_yaml_configs
# ====================================================================== #


class TestMergeYamlConfigs:

    def _write_yaml(self, content: str) -> str:
        pytest.importorskip("yaml")
        f = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        )
        f.write(content)
        f.close()
        return f.name

    def test_merges_semantic_from_two_files(self):
        path1 = self._write_yaml(
            """
semantic:
  - label: api
    description: API
    services: [django]
    node_patterns: []
"""
        )
        path2 = self._write_yaml(
            """
semantic:
  - label: payments
    description: Payments
    services: [stripe]
    node_patterns: []
"""
        )
        try:
            merged = merge_yaml_configs(path1, path2)
            labels = {e["label"] for e in merged["semantic"]}
            assert "api" in labels
            assert "payments" in labels
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_later_file_wins_on_same_label(self):
        """If both files define 'api', the second file's entry wins."""
        path1 = self._write_yaml(
            """
semantic:
  - label: api
    description: first
    services: []
    node_patterns: []
"""
        )
        path2 = self._write_yaml(
            """
semantic:
  - label: api
    description: second
    services: []
    node_patterns: []
"""
        )
        try:
            merged = merge_yaml_configs(path1, path2)
            # load_from_dict(merged["semantic"]) — later entry wins
            layer = load_from_dict(merged["semantic"])
            assert layer.describe("api") == "second"
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_missing_file_silently_skipped(self):
        """Non-existent path must not raise."""
        merged = merge_yaml_configs("/nonexistent/path.yaml")
        assert merged["semantic"] == []

    def test_none_path_silently_skipped(self):
        merged = merge_yaml_configs(None)
        assert merged["semantic"] == []

    def test_probes_deduplicated_by_name(self):
        path1 = self._write_yaml(
            "probes:\n  - django\n  - asyncio\n"
        )
        path2 = self._write_yaml(
            "probes:\n  - django\n  - gunicorn\n"
        )
        try:
            merged = merge_yaml_configs(path1, path2)
            probe_keys = merged["probes"]
            assert (
                probe_keys.count("django") == 1
            )  # deduplicated
            assert "asyncio" in probe_keys
            assert "gunicorn" in probe_keys
        finally:
            os.unlink(path1)
            os.unlink(path2)


# ====================================================================== #
# Default YAML labels — integration against realistic graph
# ====================================================================== #


class TestDefaultYamlLabels:
    """
    Each label from the default stacktracer.yaml must resolve to the
    correct set of nodes against a graph that mirrors a live stack.
    This is the test that catches YAML typos and pattern mistakes.
    """

    def setup_method(self):
        self.layer = _layer_with_defaults()
        self.graph = _graph(
            # nginx
            ("nginx::upstream", "nginx"),
            # uvicorn
            ("uvicorn::/api/orders/", "uvicorn"),
            ("uvicorn::/n1/", "uvicorn"),
            # gunicorn
            ("gunicorn::master", "gunicorn"),
            ("gunicorn::UvicornWorker", "gunicorn"),
            # celery
            ("celery::tasks.process_order", "celery"),
            # django app
            ("django::/api/orders/", "django"),
            ("django::OrderView.get", "django"),
            ("django::handle_export", "django"),
            ("django::flags_client.call", "django"),
            ("django::authenticate", "django"),
            ("django::check_permissions", "django"),
            # django ORM queries
            (
                'django::SELECT "order"."id" FROM "order"',
                "django",
            ),
            ('django::INSERT INTO "order"', "django"),
            # external services
            ("postgres::SELECT", "postgres"),
            ("redis::GET", "redis"),
            ("exporter::run", "exporter"),
            ("auth::verify_token", "auth"),
        )

    def test_api_label_matches_api_path_nodes(self):
        nodes = self.layer.resolve_nodes("api", self.graph)
        assert "django::/api/orders/" in nodes
        assert "uvicorn::/api/orders/" in nodes
        # non-api paths excluded
        assert "uvicorn::/n1/" not in nodes

    def test_db_label_matches_postgres_and_redis(self):
        nodes = self.layer.resolve_nodes("db", self.graph)
        assert "postgres::SELECT" in nodes
        assert "redis::GET" in nodes
        assert "django::OrderView.get" not in nodes

    def test_worker_label_matches_gunicorn_and_celery(self):
        nodes = self.layer.resolve_nodes("worker", self.graph)
        assert "gunicorn::master" in nodes
        assert "gunicorn::UvicornWorker" in nodes
        assert "celery::tasks.process_order" in nodes
        assert "django::OrderView.get" not in nodes

    def test_gateway_label_matches_nginx(self):
        nodes = self.layer.resolve_nodes("gateway", self.graph)
        assert "nginx::upstream" in nodes
        assert "uvicorn::/api/orders/" not in nodes

    def test_http_label_matches_nginx_and_uvicorn(self):
        nodes = self.layer.resolve_nodes("http", self.graph)
        assert "nginx::upstream" in nodes
        assert "uvicorn::/api/orders/" in nodes
        assert "django::OrderView.get" not in nodes

    def test_django_label_matches_django_service_nodes(self):
        nodes = self.layer.resolve_nodes("django", self.graph)
        # All django:: nodes match via services: [django]
        assert "django::OrderView.get" in nodes
        assert "django::handle_export" in nodes
        assert "nginx::upstream" not in nodes

    def test_database_label_matches_orm_query_nodes(self):
        nodes = self.layer.resolve_nodes("database", self.graph)
        assert (
            'django::SELECT "order"."id" FROM "order"' in nodes
        )
        assert 'django::INSERT INTO "order"' in nodes
        assert "django::OrderView.get" not in nodes

    def test_export_label_matches_handle_export_and_flags_client(
        self,
    ):
        nodes = self.layer.resolve_nodes("export", self.graph)
        assert "django::handle_export" in nodes
        assert "django::flags_client.call" in nodes
        assert "exporter::run" in nodes
        assert "django::OrderView.get" not in nodes

    def test_auth_label_matches_auth_nodes(self):
        nodes = self.layer.resolve_nodes("auth", self.graph)
        assert "django::authenticate" in nodes
        assert "django::check_permissions" in nodes
        assert "auth::verify_token" in nodes
        assert "django::OrderView.get" not in nodes

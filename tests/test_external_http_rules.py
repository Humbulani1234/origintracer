from origintracer.core.causal import PatternRegistry
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore
from origintracer.rules.external_http_rules import (
    EXTERNAL_DEPENDENCY_TIMEOUT,
    _external_dependency_timeout,
)


def test_external_dependency_rule_registers_in_core():
    names = set()

    class Registry:
        @staticmethod
        def register(rule):
            names.add(rule.name)

    from origintracer.rules.external_http_rules import register

    register(Registry)

    assert names == {EXTERNAL_DEPENDENCY_TIMEOUT.name}


def test_external_dependency_rule_uses_core_probe_aggregate_duration():
    graph = RuntimeGraph()
    graph.upsert_node(
        "external::GET api.example.test",
        "external",
        "external",
        2_100_000_000,
        {
            "probe": "external.http.client",
            "external": True,
            "dependency_host": "api.example.test",
        },
    )

    matched, evidence = _external_dependency_timeout(
        graph, TemporalStore()
    )

    assert matched is True
    row = evidence["slow_dependencies"][0]
    assert row["dependency_host"] == "api.example.test"
    assert row["avg_ms"] == 2100.0
    assert row["calls"] == 1


def test_external_dependency_rule_does_not_match_below_threshold():
    graph = RuntimeGraph()
    graph.upsert_node(
        "external::GET api.example.test",
        "external",
        "external",
        1_900_000_000,
        {
            "external": True,
            "dependency_host": "api.example.test",
        },
    )

    matched, evidence = _external_dependency_timeout(
        graph, TemporalStore()
    )

    assert matched is False
    assert evidence == {}


def test_external_dependency_rule_ignores_non_external_nodes():
    graph = RuntimeGraph()
    graph.upsert_node(
        "django::/slow/",
        "django",
        "django",
        5_000_000_000,
        {"dependency_host": "api.example.test"},
    )

    matched, evidence = _external_dependency_timeout(
        graph, TemporalStore()
    )

    assert matched is False
    assert evidence == {}


def test_external_dependency_rule_is_core_registered():
    assert (
        "external_dependency_timeout"
        in PatternRegistry.rule_names()
    )

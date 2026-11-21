from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore
from origintracer.rules.django_rules import (
    _write_call_amplification,
)


def _add_view_and_write(
    graph, *, view_calls, write_calls, query
):
    view_id = "django::CheckoutView"
    query_id = f"django::{query}"
    for _ in range(view_calls):
        graph.upsert_node(
            view_id,
            "django",
            "django",
            metadata={"probe": "django.view.enter"},
        )
    for _ in range(write_calls):
        graph.upsert_node(
            query_id,
            "django",
            "django",
            metadata={"probe": "django.db.query"},
        )
    graph.upsert_edge(view_id, query_id, "calls")


def test_write_call_amplification_matches_repeated_view_writes():
    graph = RuntimeGraph()
    _add_view_and_write(
        graph,
        view_calls=2,
        write_calls=3,
        query='UPDATE "orders" SET "status" = %s',
    )

    matched, evidence = _write_call_amplification(
        graph, TemporalStore()
    )

    assert matched is True
    row = evidence["write_call_patterns"][0]
    assert row["query"].startswith("django::UPDATE")
    assert row["view"] == "django::CheckoutView"
    assert row["write_call_count"] == 3
    assert row["view_call_count"] == 2
    assert row["ratio"] == 1.5
    assert (
        "does not establish" in evidence["interpretation_limit"]
    )


def test_write_call_amplification_ignores_balanced_write_counts():
    graph = RuntimeGraph()
    _add_view_and_write(
        graph,
        view_calls=2,
        write_calls=2,
        query='UPDATE "orders" SET "status" = %s',
    )

    matched, evidence = _write_call_amplification(
        graph, TemporalStore()
    )

    assert matched is False
    assert evidence == {}


def test_write_call_amplification_ignores_read_queries():
    graph = RuntimeGraph()
    _add_view_and_write(
        graph,
        view_calls=2,
        write_calls=5,
        query='SELECT "orders"."id" FROM "orders"',
    )

    matched, evidence = _write_call_amplification(
        graph, TemporalStore()
    )

    assert matched is False
    assert evidence == {}

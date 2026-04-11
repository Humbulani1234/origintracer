from typing import Any, Callable, Dict, List, Optional, Tuple

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import (
    CausalRule,
    PatternRegistry,
    _is_db_node,
    _is_view_node,
)
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore

# ------------------------- N+1 query detection --------------------------


def _n_plus_one_queries(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict]:
    """
    Detects the N+1 pattern: a query node whose call_count is significantly
    higher than the view node that called it.

    How it works:
        For each DB query node, look up its callers via reverse edges.
        Find the nearest view node among callers.
        If query.call_count / view.call_count >= THRESHOLD → N+1.

    Threshold: 5x. A query firing 5+ times per view invocation is
    almost certainly an N+1. Tuned to avoid false positives on batch
    endpoints that legitimately run multiple queries.

    Real example from this codebase:
        django::NPlusOneView               call_count=1
        django::SELECT author              call_count=1   ← fine
        django::SELECT book (author_id=%s) call_count=10  ← N+1, ratio=10x
    """
    THRESHOLD = 5

    hits = []
    seen = set()
    for node in graph.all_nodes():
        if not _is_db_node(node):
            continue
        if node.call_count < THRESHOLD:
            continue

        def check_caller(caller, query_node):
            if (
                not _is_view_node(caller)
                or caller.call_count <= 0
            ):
                return
            key = (query_node.id, caller.id)
            if key in seen:
                return
            ratio = query_node.call_count / caller.call_count
            if ratio >= THRESHOLD:
                seen.add(key)
                hits.append(
                    {
                        "query": query_node.id,
                        "view": caller.id,
                        "query_count": query_node.call_count,
                        "view_count": caller.call_count,
                        "ratio": round(ratio, 1),
                        "avg_query_ms": round(
                            (query_node.avg_duration_ns or 0)
                            / 1e6,
                            2,
                        ),
                        "hint": (
                            "Use select_related() or prefetch_related() to batch "
                            f"this query. Estimated wasted queries per request: "
                            f"{int(ratio) - 1}"
                        ),
                    }
                )

        for edge in graph.callers(node.id):
            caller = graph._nodes.get(edge.source)
            if caller is None:
                continue
            # direct view caller
            check_caller(caller, node)
            # one hop up
            for edge2 in graph.callers(caller.id):
                grandcaller = graph._nodes.get(edge2.source)
                if grandcaller:
                    check_caller(grandcaller, node)

    if not hits:
        return False, {}

    hits.sort(key=lambda h: h["ratio"], reverse=True)
    return True, {"n_plus_one_patterns": hits[:10]}


N_PLUS_ONE = CausalRule(
    name="n_plus_one_queries",
    description=(
        "A database query fires N times per view invocation (ratio ≥5x). "
        "Classic ORM N+1 — the query is inside a loop iterating over a queryset. "
        "Use select_related() or prefetch_related() to batch into one query. "
        "Check the 'query' field for the exact SQL pattern."
    ),
    predicate=_n_plus_one_queries,
    confidence=0.90,
    tags=["db", "performance", "n+1"],
)

# ---------------------- DB query hotspot --------------------------------


def _db_query_hotspot(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict]:
    """
    A single query pattern accounts for >30% of all observed calls.
    Distinct from N+1 — fires even without a view caller, e.g. a
    celery task hammering the same query in a background loop.

    Note: _is_db_node() checks metadata.probe, not node_type, because
    Django DB query nodes have node_type="django" (probe prefix).
    """
    db_nodes = [n for n in graph.all_nodes() if _is_db_node(n)]
    print(">>>> DJANGO RULE", db_nodes)
    if not db_nodes:
        return False, {}
    total_calls = (
        sum(n.call_count for n in graph.all_nodes()) or 1
    )
    hotspots = [
        n
        for n in db_nodes
        if n.call_count > 5
        and (n.call_count / total_calls > 0.30)
    ]
    print(">>>> DJANGO RULE HOTSPOTS", hotspots)
    if not hotspots:
        return False, {}

    return True, {
        "hotspot_queries": [
            {
                "node": n.id,
                "call_count": n.call_count,
                "pct": round(
                    n.call_count / total_calls * 100, 1
                ),
                "avg_ms": round(
                    (n.avg_duration_ns or 0) / 1e6, 2
                ),
            }
            for n in hotspots
        ]
    }


DB_HOTSPOT = CausalRule(
    name="db_query_hotspot",
    description=(
        "A single database query accounts for >30% of all observed calls. "
        "N+1 query or missing cache — check the callers of this query node."
    ),
    predicate=_db_query_hotspot,
    confidence=0.70,
    tags=["db", "performance"],
)

PatternRegistry.register(N_PLUS_ONE)
PatternRegistry.register(DB_HOTSPOT)

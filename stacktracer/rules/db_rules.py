from typing import Any, Callable, Dict, List, Optional, Tuple

from ..core.causal import CausalRule
from ..core.runtime_graph import RuntimeGraph
from ..core.temporal import TemporalStore 

def _db_query_hotspot(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """Detect database query nodes that are called far more than expected."""
    db_nodes = [
        n for n in graph.all_nodes()
        if n.node_type == "db" and n.call_count > 100
    ]
    total_calls = sum(n.call_count for n in graph.all_nodes()) or 1
    hotspots = [
        n for n in db_nodes
        if n.call_count / total_calls > 0.3   # >30% of all calls are this query
    ]
    if not hotspots:
        return False, {}
    return True, {
        "hotspot_queries": [
            {"node": n.id, "call_count": n.call_count, "pct": round(n.call_count / total_calls * 100, 1)}
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
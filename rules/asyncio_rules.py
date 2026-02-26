from typing import Any, Callable, Dict, List, Optional, Tuple

from ..core.causal import CausalRule
from ..core.runtime_graph import RuntimeGraph
from ..core.temporal import TemporalStore 


def _new_sync_call_after_deployment(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detect edges (calls) that appeared after the most recent deployment marker.
    A new synchronous call introduced by a deployment is a common root cause
    of latency spikes (the Antimetal 'Exporter→Flags' pattern).
    """
    deployment_diff = temporal.label_diff("deployment")
    if not deployment_diff:
        return False, {}

    new_edges = temporal.new_edges_since(deployment_diff.timestamp)
    sync_call_edges = [k for k in new_edges if ":calls" in k]

    if not sync_call_edges:
        return False, {}

    return True, {
        "deployment_timestamp": deployment_diff.timestamp,
        "new_sync_edges": sync_call_edges[:10],
    }


NEW_SYNC_CALL = CausalRule(
    name="new_sync_call_after_deployment",
    description=(
        "New synchronous call edges appeared immediately after the most recent deployment. "
        "A newly introduced synchronous dependency is the probable root cause of latency — "
        "not the database or downstream metrics that degraded later."
    ),
    predicate=_new_sync_call_after_deployment,
    confidence=0.85,
    tags=["deployment", "latency", "async"],
)
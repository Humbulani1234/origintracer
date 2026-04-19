import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import CausalRule, PatternRegistry
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore


def _new_sync_call_after_deployment(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict[str, Any]]:
    deployment_diff = temporal.label_diff("deployment")
    if not deployment_diff:
        return False, {}

    if time.time() - deployment_diff.timestamp < 120:
        return False, {}

    # require post-deployment snapshots
    post_diffs = [
        d
        for d in temporal.changes_since(
            deployment_diff.timestamp
        )
        if not d.label
    ]
    if not post_diffs:
        return False, {}

    new_edges = temporal.new_edges_since(
        deployment_diff.timestamp
    )
    genuinely_new = [
        e
        for e in new_edges
        if ":calls" in e
        and e not in deployment_diff.edge_baseline
    ]
    if not genuinely_new:
        return False, {}

    # extract node ids from new edge keys e.g. "django::A→django::B:calls"
    new_edge_nodes = set()
    for edge in genuinely_new:
        parts = edge.replace(":calls", "").split("→")
        for p in parts:
            new_edge_nodes.add(p.strip())

    slow_nodes = [
        n
        for n in graph.all_nodes()
        if n.id in new_edge_nodes
        and n.avg_duration_ns
        and n.avg_duration_ns > 200 * 1e6
    ]
    if not slow_nodes:
        return False, {}

    return True, {
        "deployment_timestamp": deployment_diff.timestamp,
        "new_sync_edges": genuinely_new[:10],
        "slow_nodes": [n.id for n in slow_nodes],
    }


NEW_SYNC_CALL = CausalRule(
    name="new_sync_call_after_deployment",
    description=(
        "New synchronous call edges appeared after the most recent deployment "
        "and the nodes involved in those new edges are experiencing elevated "
        "latency (>200ms avg). The new dependency itself is the probable root "
        "cause - not unrelated parts of the system. "
        "Rule requires 120s post-deployment to establish a baseline. "
    ),
    predicate=_new_sync_call_after_deployment,
    confidence=0.85,
    tags=["deployment", "latency", "async"],
)


def _asyncio_loop_starvation(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    High average loop-tick duration means something is blocking the event
    loop — CPU work, a missing await, or a synchronous call on the hot path.
    Threshold: >10ms average per tick.
    """
    stalled = [
        n
        for n in graph.all_nodes()
        if (
            n.node_type == "asyncio"
            and n.id == "asyncio::loop.tick"
            and n.avg_duration_ns is not None
            and n.avg_duration_ns > 3_000_000
        )
    ]
    if not stalled:
        return False, {}

    return True, {
        "stalled_ticks": [
            {
                "node": n.id,
                "avg_ms": round(n.avg_duration_ns / 1e6, 1),
                "count": n.call_count,
            }
            for n in stalled[:5]
        ]
    }


LOOP_STARVATION = CausalRule(
    name="asyncio_event_loop_starvation",
    description=(
        "asyncio event loop ticks averaging >10ms. "
        "A blocking operation (CPU work, synchronous I/O, or missing await) "
        "is starving other coroutines. Check for sync calls on the hot path."
    ),
    predicate=_asyncio_loop_starvation,
    confidence=0.80,
    tags=["asyncio", "latency", "blocking"],
)

PatternRegistry.register(NEW_SYNC_CALL)
PatternRegistry.register(LOOP_STARVATION)

from typing import Any, Callable, Dict, List, Optional, Tuple

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import CausalRule, PatternRegistry
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore

# ------------- New synchronous call after deployment -----------


def _new_sync_call_after_deployment(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict]:
    deployment_diff = temporal.label_diff("deployment")
    if not deployment_diff:
        return False, {}

    new_edges = temporal.new_edges_since(
        deployment_diff.timestamp
    )
    sync_edges = [k for k in new_edges if ":calls" in k]
    if not sync_edges:
        return False, {}

    return True, {
        "deployment_timestamp": deployment_diff.timestamp,
        "new_sync_edges": sync_edges[:10],
    }


NEW_SYNC_CALL = CausalRule(
    name="new_sync_call_after_deployment",
    description=(
        "New synchronous call edges appeared immediately after the most recent "
        "deployment. A newly introduced synchronous dependency is the probable "
        "root cause of latency — not the database or downstream metrics that "
        "degraded later. (Ref: Antimetal Exporter→Flags incident pattern.)"
    ),
    predicate=_new_sync_call_after_deployment,
    confidence=0.85,
    tags=["deployment", "latency", "async"],
)

# --------------------- asyncio event loop starvation -------------------


def _asyncio_loop_starvation(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict]:
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

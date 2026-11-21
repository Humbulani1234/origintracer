"""Causal rules for outbound HTTP dependency observations."""

from typing import Dict, Optional, Tuple

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import CausalRule, PatternRegistry
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore

TIMEOUT_THRESHOLD_NS = 2_000_000_000


def _external_dependency_timeout(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict]:
    """Find external HTTP operations averaging more than two seconds."""
    slow_dependencies = []
    for node in graph.all_nodes():
        metadata = node.metadata or {}
        if not metadata.get("external"):
            continue
        average_duration_ns = node.avg_duration_ns or 0
        if average_duration_ns < TIMEOUT_THRESHOLD_NS:
            continue
        slow_dependencies.append(
            {
                "node": node.id,
                "dependency_host": metadata.get(
                    "dependency_host"
                ),
                "avg_ms": round(average_duration_ns / 1e6, 2),
                "calls": node.call_count,
            }
        )

    if not slow_dependencies:
        return False, {}
    return True, {
        "slow_dependencies": slow_dependencies,
        "interpretation_limit": (
            "This is an aggregate outbound-call duration pattern; it does "
            "not by itself establish request impact or root cause."
        ),
    }


EXTERNAL_DEPENDENCY_TIMEOUT = CausalRule(
    name="external_dependency_timeout",
    description=(
        "An observed outbound HTTP dependency averages more than two seconds. "
        "This identifies a slow client-call pattern, not proof that the "
        "dependency caused request latency or an outage."
    ),
    predicate=_external_dependency_timeout,
    confidence=0.78,
    tags=["latency", "external", "timeout"],
)


def register(
    registry: type[PatternRegistry] = PatternRegistry,
) -> None:
    """Register rules supported by the core external HTTP probe."""
    registry.register(EXTERNAL_DEPENDENCY_TIMEOUT)


register()

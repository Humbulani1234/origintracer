from typing import Any, Callable, Dict, List, Optional, Tuple

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import CausalRule, PatternRegistry
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore


def _retry_amplification(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detect downstream nodes where retry counts are high —
    a symptom of upstream latency being amplified by retry loops.
    """
    hot_edges = [
        e
        for e in graph.all_edges()
        if e.metadata.get("retries", 0) > 3
    ]
    if not hot_edges:
        return False, {}
    evidence = {
        "edges": [
            {
                "source": e.source,
                "target": e.target,
                "retries": e.metadata.get("retries"),
            }
            for e in hot_edges[:5]
        ]
    }
    return True, evidence


def register(registry: PatternRegistry) -> None:
    """
    Called automatically when this file is loaded.
    Register all rules from this file here.
    """
    registry.register(
        CausalRule(
            name="retry_amplification",
            description=(
                "High retry counts detected on downstream edges. "
                "A slow downstream dependency is being amplified by retry loops — "
                "investigate the slowest downstream node first, not the retrying caller."
            ),
            predicate=_retry_amplification,
            confidence=0.75,
            tags=["latency", "retry"],
        )
    )

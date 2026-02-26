"""
core/causal.py

Pattern-based causal matching over the RuntimeGraph.
Honest name: this is a rule engine, not probabilistic causal inference.
Rules are human-encoded failure patterns.

Each rule:
  - Has a unique name
  - Has a predicate that takes (graph, temporal_store) → bool
  - Returns a CausalMatch with explanation and confidence

This is v1. v2 would learn weights from confirmed incidents.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .runtime_graph import RuntimeGraph
from .temporal import TemporalStore


@dataclass
class CausalMatch:
    rule_name: str
    confidence: float           # 0.0 – 1.0 (rule-assigned, not learned yet)
    explanation: str
    evidence: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule": self.rule_name,
            "confidence": self.confidence,
            "explanation": self.explanation,
            "evidence": self.evidence,
            "timestamp": self.timestamp,
        }


# Type alias: a rule predicate returns (matched: bool, evidence: dict)
RuleFn = Callable[[RuntimeGraph, TemporalStore], Tuple[bool, Dict[str, Any]]]


@dataclass
class CausalRule:
    name: str
    description: str
    predicate: RuleFn
    confidence: float = 0.7
    tags: List[str] = field(default_factory=list)


class PatternRegistry:
    """
    Register causal rules and evaluate them over the live graph.

    Usage
    -----
        registry = PatternRegistry()
        registry.register(retry_amplification_rule)
        matches = registry.evaluate(graph, temporal)
    """

    def __init__(self) -> None:
        self._rules: Dict[str, CausalRule] = {}

    def register(self, rule: CausalRule) -> None:
        self._rules[rule.name] = rule

    def evaluate(
        self,
        graph: RuntimeGraph,
        temporal: TemporalStore,
        tags: Optional[List[str]] = None,
    ) -> List[CausalMatch]:
        """Run all registered rules (optionally filtered by tag)."""
        results: List[CausalMatch] = []

        for rule in self._rules.values():
            if tags and not set(tags) & set(rule.tags):
                continue
            try:
                matched, evidence = rule.predicate(graph, temporal)
                if matched:
                    results.append(CausalMatch(
                        rule_name=rule.name,
                        confidence=rule.confidence,
                        explanation=rule.description,
                        evidence=evidence,
                    ))
            except Exception as exc:
                # Rules must never crash the engine
                results.append(CausalMatch(
                    rule_name=rule.name,
                    confidence=0.0,
                    explanation=f"Rule evaluation error: {exc}",
                ))

        results.sort(key=lambda m: m.confidence, reverse=True)
        return results

    def rule_names(self) -> List[str]:
        return list(self._rules.keys())


# ====================================================================== #
# Built-in Rules
# ====================================================================== #

def _retry_amplification(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detect downstream nodes where retry counts are high —
    a symptom of upstream latency being amplified by retry loops.
    """
    hot_edges = [
        e for e in graph.all_edges()
        if e.metadata.get("retries", 0) > 3
    ]
    if not hot_edges:
        return False, {}
    evidence = {
        "edges": [
            {"source": e.source, "target": e.target, "retries": e.metadata.get("retries")}
            for e in hot_edges[:5]
        ]
    }
    return True, evidence


RETRY_AMPLIFICATION = CausalRule(
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


def _asyncio_event_loop_starvation(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detect asyncio loop-tick nodes with high average duration.
    High tick duration = something is blocking the event loop (CPU-bound work,
    missing await, or a slow synchronous call on the hot path).
    """
    loop_ticks = [
        n for n in graph.all_nodes()
        if n.node_type == "asyncio" and "loop.tick" in n.id
    ]
    stalled = [
        n for n in loop_ticks
        if n.avg_duration_ns and n.avg_duration_ns > 10_000_000  # >10ms per tick
    ]
    if not stalled:
        return False, {}
    return True, {
        "stalled_ticks": [
            {"node": n.id, "avg_ms": n.avg_duration_ns / 1e6, "count": n.call_count}
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
    predicate=_asyncio_event_loop_starvation,
    confidence=0.80,
    tags=["asyncio", "latency", "blocking"],
)


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


def build_default_registry() -> PatternRegistry:
    """Return a PatternRegistry pre-loaded with all built-in rules."""
    registry = PatternRegistry()
    registry.register(RETRY_AMPLIFICATION)
    registry.register(NEW_SYNC_CALL)
    registry.register(LOOP_STARVATION)
    registry.register(DB_HOTSPOT)
    return registry
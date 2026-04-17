"""
Pattern-based causal matching over the RuntimeGraph.

This is a rule engine, not probabilistic causal inference.
Rules are human-encoded failure patterns derived from real incidents.
Each rule observes the live graph and returns a CausalMatch when it fires.

Rule inventory
--------------
  retry_amplification - high retry counts on edges
  new_sync_call_after_deploy - new synchronous edges after deployment marker
  asyncio_loop_starvation - event loop ticks averaging >10ms
  n_plus_one_queries - query call_count >> parent view call_count
  worker_imbalance - one gunicorn worker handling far more than others
  db_query_hotspot - single query accounts for >30% of all calls
  request_duration_anomaly - recent P99 diverged 3x from historical avg (needs tracker)

Adding a new rule
-----------------
  1. Write a predicate: (RuntimeGraph, TemporalStore, ActiveRequestTracker) >> (bool, dict)
  2. Wrap it in a CausalRule.
  3. Register it.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)

from .runtime_graph import RuntimeGraph
from .temporal import TemporalStore

logger = logging.getLogger("origintracer.causal")


@dataclass
class CausalMatch:
    rule_name: str
    confidence: float  # 0.0 – 1.0, rule-assigned
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


# A rule predicate returns (matched, evidence_dict)
RuleFn = Callable[
    [RuntimeGraph, TemporalStore, ActiveRequestTracker],
    Tuple[bool, Dict[str, Any]],
]


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

        PatternRegistry.register(N_PLUS_ONE)
        matches = PatternRegistry.evaluate(graph, temporal)
    """

    _rules: Dict[str, CausalRule] = {}

    @classmethod
    def register(cls, rule: CausalRule) -> None:
        cls._rules[rule.name] = rule

    @classmethod
    def deregister(cls, name: str) -> None:
        cls._rules.pop(name, None)

    @classmethod
    def _reset(cls) -> None:
        """
        For testing only - clears all registered rules.
        """
        cls._rules.clear()

    @classmethod
    def evaluate(
        cls,
        graph: RuntimeGraph,
        temporal: TemporalStore,
        tracker: Optional[ActiveRequestTracker] = None,
        tags: Optional[List[str]] = None,
    ) -> List[CausalMatch]:
        """
        Run all registered rules, optionally filtered by tag.
        """
        if not cls._rules:
            logger.warning("No rules registered")
        results: List[CausalMatch] = []
        for rule in cls._rules.values():
            if tags and not set(tags) & set(rule.tags):
                continue
            try:
                matched, evidence = rule.predicate(
                    graph, temporal, tracker
                )
                if matched:
                    results.append(
                        CausalMatch(
                            rule_name=rule.name,
                            confidence=rule.confidence,
                            explanation=rule.description,
                            evidence=evidence,
                        )
                    )
            except Exception as exc:
                # Rules must never crash the engine
                results.append(
                    CausalMatch(
                        rule_name=rule.name,
                        confidence=0.0,
                        explanation=f"Rule evaluation error: {exc}",
                    )
                )

        results.sort(key=lambda m: m.confidence, reverse=True)
        return results

    @classmethod
    def rule_names(cls) -> List[str]:
        return list(cls._rules.keys())

    @classmethod
    def apply_filter(cls, allowed: List[str]) -> None:
        """
        If allowed is non-empty, deregister any rule not in that set.
        Empty list = no filtering (all rules remain active).
        """
        if not allowed:
            return
        allowed_set = set(allowed)
        unknown = allowed_set - set(cls._rules)
        if unknown:
            logger.warning(
                "cfg.rules references unknown rules: %s", unknown
            )
        for name in set(cls._rules) - allowed_set:
            cls.deregister(name)
            logger.debug(
                "Rule deregistered (not in cfg.rules): %s", name
            )


def _is_db_node(node) -> bool:
    """
    True if this node is a database query.
    node_type is "django" (from probe prefix) so we must check the
    probe field in metadata, not node_type.
    """
    return node.metadata.get("probe", "").endswith(".db.query")


def _is_view_node(node) -> bool:
    probe = node.metadata.get("probe", "")
    return probe in ("django.view.enter", "django.view.exit")


def _is_gunicorn_worker(node) -> bool:
    return (
        node.node_type == "gunicorn"
        and node.metadata.get("probe") == "gunicorn.worker.fork"
    )

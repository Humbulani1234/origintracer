"""
core/causal.py

Pattern-based causal matching over the RuntimeGraph.

This is a rule engine, not probabilistic causal inference.
Rules are human-encoded failure patterns derived from real incidents.
Each rule observes the live graph and returns a CausalMatch when it fires.

Rule inventory
--------------
  retry_amplification          — high retry counts on edges
  new_sync_call_after_deploy   — new synchronous edges after deployment marker
  asyncio_loop_starvation      — event loop ticks averaging >10ms
  n_plus_one_queries           — query call_count >> parent view call_count
  worker_imbalance             — one gunicorn worker handling far more than others
  db_query_hotspot             — single query accounts for >30% of all calls
  request_duration_anomaly     — recent P99 diverged 3x from historical avg (needs tracker)

Adding a new rule
-----------------
  1. Write a predicate: (RuntimeGraph, TemporalStore) → (bool, dict)
  2. Wrap it in a CausalRule
  3. Register it in build_default_registry()

v1: rule weights are hand-assigned.
v2: learn weights from confirmed incidents.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .runtime_graph import RuntimeGraph
from .temporal import TemporalStore

# ====================================================================== #
# Core types
# ====================================================================== #


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
    [RuntimeGraph, TemporalStore], Tuple[bool, Dict[str, Any]]
]


@dataclass
class CausalRule:
    name: str
    description: str
    predicate: RuleFn
    confidence: float = 0.7
    tags: List[str] = field(default_factory=list)


# ====================================================================== #
# Registry
# ====================================================================== #


class PatternRegistry:
    """
    Register causal rules and evaluate them over the live graph.

        registry = PatternRegistry()
        registry.register(N_PLUS_ONE)
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
        """Run all registered rules, optionally filtered by tag."""
        results: List[CausalMatch] = []

        for rule in self._rules.values():
            if tags and not set(tags) & set(rule.tags):
                continue
            try:
                matched, evidence = rule.predicate(
                    graph, temporal
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

    def rule_names(self) -> List[str]:
        return list(self._rules.keys())


# ====================================================================== #
# Helpers
# ====================================================================== #


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


# ====================================================================== #
# Rule 1 — Retry amplification
# ====================================================================== #


def _retry_amplification(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    hot_edges = [
        e
        for e in graph.all_edges()
        if e.metadata.get("retries", 0) > 3
    ]
    if not hot_edges:
        return False, {}
    return True, {
        "edges": [
            {
                "source": e.source,
                "target": e.target,
                "retries": e.metadata.get("retries"),
            }
            for e in hot_edges[:5]
        ]
    }


RETRY_AMPLIFICATION = CausalRule(
    name="retry_amplification",
    description=(
        "High retry counts on downstream edges. "
        "A slow dependency is being amplified by retry loops — "
        "investigate the slowest downstream node first, not the retrying caller."
    ),
    predicate=_retry_amplification,
    confidence=0.75,
    tags=["latency", "retry"],
)


# ====================================================================== #
# Rule 2 — New synchronous call after deployment
# ====================================================================== #


def _new_sync_call_after_deployment(
    graph: RuntimeGraph,
    temporal: TemporalStore,
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


# ====================================================================== #
# Rule 3 — asyncio event loop starvation
# ====================================================================== #


def _asyncio_loop_starvation(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    High average loop-tick duration means something is blocking the event
    loop — CPU work, a missing await, or a synchronous call on the hot path.
    Threshold: >10ms average per tick.
    """
    import pdb
    pdb.set_trace()
    stalled = [
        n
        for n in graph.all_nodes()
        if (
            n.node_type == "asyncio"
            and "loop.tick" in n.id
            and n.avg_duration_ns is not None
            and n.avg_duration_ns > 10_000_000
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


# ====================================================================== #
# Rule 4 — N+1 query detection
# ====================================================================== #


def _n_plus_one_queries(
    graph: RuntimeGraph,
    temporal: TemporalStore,
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
    for node in graph.all_nodes():
        if not _is_db_node(node):
            continue
        if node.call_count < THRESHOLD:
            continue

        # Walk reverse edges to find the nearest view caller
        for edge in graph.callers(node.id):
            caller = graph._nodes.get(edge.source)
            if caller is None:
                continue
            if _is_view_node(caller) and caller.call_count > 0:
                ratio = node.call_count / caller.call_count
                if ratio >= THRESHOLD:
                    hits.append(
                        {
                            "query": node.id,
                            "view": caller.id,
                            "query_count": node.call_count,
                            "view_count": caller.call_count,
                            "ratio": round(ratio, 1),
                            "avg_query_ms": round(
                                (node.avg_duration_ns or 0)
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


# ====================================================================== #
# Rule 5 — Worker imbalance
# ====================================================================== #


def _worker_imbalance(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects when one gunicorn worker handles significantly more requests
    than others — a signal that a worker is stuck on a blocking call.

    Uses the 'handled' edges from worker nodes to request nodes,
    built by RuntimeGraph._add_structural_edges when uvicorn.request.receive
    events arrive.

    Fires when: busiest_worker / least_busy_worker >= 2.0
    AND at least 2 workers are present.
    """
    worker_nodes = [
        n for n in graph.all_nodes() if _is_gunicorn_worker(n)
    ]
    if len(worker_nodes) < 2:
        return False, {}

    worker_loads = {}
    for worker in worker_nodes:
        handled = [
            e
            for e in graph.neighbors(worker.id)
            if e.edge_type == "handled"
        ]
        worker_loads[worker.id] = {
            "worker_pid": worker.metadata.get("worker_pid"),
            "worker_class": worker.metadata.get("worker_class"),
            "handled_count": len(handled),
        }

    counts = [v["handled_count"] for v in worker_loads.values()]
    max_load = max(counts)
    min_load = min(counts)

    if min_load == 0 or max_load / min_load < 2.0:
        return False, {}

    return True, {
        "workers": list(worker_loads.values()),
        "max_load": max_load,
        "min_load": min_load,
        "ratio": round(max_load / max(min_load, 1), 1),
        "hint": (
            "A worker with zero or few handled requests may be stuck on a "
            "blocking call. Check for synchronous I/O or CPU-bound work on "
            "the busiest worker's endpoints."
        ),
    }


WORKER_IMBALANCE = CausalRule(
    name="worker_imbalance",
    description=(
        "Gunicorn worker load is unbalanced — one worker is handling 2x+ "
        "more requests than another. A worker may be stuck on a blocking "
        "call, starving the others of available capacity."
    ),
    predicate=_worker_imbalance,
    confidence=0.80,
    tags=["gunicorn", "concurrency", "blocking"],
)


# ====================================================================== #
# Rule 6 — DB query hotspot
# ====================================================================== #


def _db_query_hotspot(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    A single query pattern accounts for >30% of all observed calls.
    Distinct from N+1 — fires even without a view caller, e.g. a
    celery task hammering the same query in a background loop.

    Note: _is_db_node() checks metadata.probe, not node_type, because
    Django DB query nodes have node_type="django" (probe prefix).
    """
    db_nodes = [n for n in graph.all_nodes() if _is_db_node(n)]
    if not db_nodes:
        return False, {}

    total_calls = (
        sum(n.call_count for n in graph.all_nodes()) or 1
    )
    hotspots = [
        n for n in db_nodes if n.call_count / total_calls > 0.30
    ]
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


# ====================================================================== #
# Rule 7 — Request duration anomaly  (requires ActiveRequestTracker)
# ====================================================================== #


def make_anomaly_rule(tracker) -> CausalRule:
    """
    Factory that closes over the ActiveRequestTracker instance.
    Pass the tracker at registry build time via build_default_registry().

    Fires when:
        recent P99 > historical_avg * 3.0
        AND historical avg has >= 50 samples  (enough to trust baseline)
        AND recent window has >= 10 samples   (enough to trust P99)

    The probe_sequence on slow_in_flight is the key diagnostic:
        Repeated django.db.query  → N+1 on this specific slow request
        Single long django.db.query → missing index
        Long asyncio.loop.tick    → blocking call on hot path
    """

    def _request_duration_anomaly(
        graph: RuntimeGraph,
        temporal: TemporalStore,
    ) -> Tuple[bool, Dict[str, Any]]:

        anomalies = []
        recent_summary = tracker.all_patterns_summary()

        for pattern, stats in recent_summary.items():
            if stats["count"] < 10:
                continue

            node = None
            for n in graph.all_nodes():
                if n.id == pattern or n.id.endswith(
                    f"::{pattern}"
                ):
                    node = n
                    break
                if (
                    "::" in n.id
                    and n.id.split("::", 1)[1] == pattern
                ):
                    node = n
                    break

            if node is None or (node.call_count or 0) < 50:
                continue

            historical_avg_ms = (node.avg_duration_ns or 0) / 1e6
            if historical_avg_ms < 1.0:
                continue

            ratio = (
                stats["p99_ms"] / historical_avg_ms
                if historical_avg_ms
                else 0
            )
            if ratio <= 3.0:
                continue

            anomaly = {
                "pattern": pattern,
                "service": node.service,
                "historical_avg_ms": round(historical_avg_ms, 1),
                "recent_p99_ms": stats["p99_ms"],
                "recent_avg_ms": stats["avg_ms"],
                "ratio": round(ratio, 1),
                "recent_n": stats["count"],
                "historical_n": node.call_count,
            }

            slow = [
                s
                for s in tracker.slow_in_flight(
                    threshold_ms=historical_avg_ms * 2
                )
                if s.pattern == pattern
            ]
            if slow:
                worst = max(slow, key=lambda s: s.in_flight_ms)
                anomaly["slow_in_flight"] = {
                    "trace_id": worst.trace_id,
                    "in_flight_ms": round(worst.in_flight_ms, 1),
                    "probe_sequence": worst.probe_sequence,
                }

            anomalies.append(anomaly)

        if not anomalies:
            return False, {}

        return True, {
            "anomalous_endpoints": sorted(
                anomalies,
                key=lambda a: a["ratio"],
                reverse=True,
            ),
            "hint": (
                "Recent P99 is 3x+ above historical average. "
                "Check probe_sequence of slow_in_flight requests. "
                "Repeated django.db.query → N+1. "
                "Single long django.db.query → missing index. "
                "Run DIFF SINCE to check if a deployment coincides with onset."
            ),
        }

    return CausalRule(
        name="request_duration_anomaly",
        description=(
            "Recent request latency has diverged from the historical baseline "
            "by 3x or more. Check probe_sequence of slow in-flight requests "
            "to identify the new bottleneck."
        ),
        predicate=_request_duration_anomaly,
        confidence=0.85,
        tags=["latency", "anomaly", "live"],
    )


# ====================================================================== #
# Registry builder
# ====================================================================== #


def build_default_registry(tracker=None) -> PatternRegistry:
    """
    Build and return the default PatternRegistry with all built-in rules.

    Parameters
    ----------
    tracker : ActiveRequestTracker | None
        If provided, the request_duration_anomaly rule is included.
        If None, that rule is omitted — all other rules are unaffected.

    Usage in Engine.__init__():
        self._tracker  = ActiveRequestTracker()
        self._registry = build_default_registry(tracker=self._tracker)
    """
    registry = PatternRegistry()

    registry.register(N_PLUS_ONE)  # 0.90 — highest confidence
    registry.register(
        NEW_SYNC_CALL
    )  # 0.85 — deployment correlation
    registry.register(LOOP_STARVATION)  # 0.80 — asyncio blocking
    registry.register(
        WORKER_IMBALANCE
    )  # 0.80 — gunicorn topology
    registry.register(
        RETRY_AMPLIFICATION
    )  # 0.75 — edge retry counts
    registry.register(DB_HOTSPOT)  # 0.70 — query call share

    if tracker is not None:
        registry.register(
            make_anomaly_rule(tracker)
        )  # 0.85 — live P99

    return registry

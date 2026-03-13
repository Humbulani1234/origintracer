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
    confidence: (
        float  # 0.0 – 1.0 (rule-assigned, not learned yet)
    )
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

    new_edges = temporal.new_edges_since(
        deployment_diff.timestamp
    )
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
        n
        for n in graph.all_nodes()
        if n.node_type == "asyncio" and "loop.tick" in n.id
    ]
    stalled = [
        n
        for n in loop_ticks
        if n.avg_duration_ns
        and n.avg_duration_ns > 10_000_000  # >10ms per tick
    ]
    if not stalled:
        return False, {}
    return True, {
        "stalled_ticks": [
            {
                "node": n.id,
                "avg_ms": n.avg_duration_ns / 1e6,
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
        n
        for n in graph.all_nodes()
        if n.node_type == "db" and n.call_count > 100
    ]
    total_calls = (
        sum(n.call_count for n in graph.all_nodes()) or 1
    )
    hotspots = [
        n
        for n in db_nodes
        if n.call_count / total_calls
        > 0.3  # >30% of all calls are this query
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
# request_duration_anomaly — requires ActiveRequestTracker
# ====================================================================== #


def _make_request_duration_anomaly(tracker: Any) -> CausalRule:
    """
    Factory that closes over a live ActiveRequestTracker instance.

    The tracker is the only rule that needs live request data —
    all other rules operate purely on the RuntimeGraph and TemporalStore.

    Fires when the recent P99 latency for any endpoint pattern is more
    than 3x its historical average stored in the graph node, provided
    there are enough samples to be statistically meaningful.

    Thresholds:
        historical_avg: at least 50 samples (graph node call_count)
        recent_window:  at least 10 completions in last 60 seconds
        ratio:          P99 > 3.0x historical_avg_ms

    Why 3x: 2x triggers too many false positives during normal variance.
    5x misses real degradations until they are severe. 3x is empirically
    the right balance for web services with reasonable latency profiles.
    """

    def _predicate(
        graph: RuntimeGraph,
        temporal: TemporalStore,
    ) -> Tuple[bool, Dict]:
        if tracker is None:
            return False, {}

        anomalies = []

        summary = tracker.all_patterns_summary()
        for pattern, stats in summary.items():
            recent_p99_ms = stats.get("p99_ms", 0.0)
            recent_count = stats.get("count", 0)

            if recent_count < 10:
                # Not enough recent completions to be meaningful
                continue

            # Look up the historical average from the graph node
            node = graph.get_node(pattern)
            if node is None:
                continue
            if node.call_count < 50:
                # Not enough historical data to compare against
                continue
            if not node.avg_duration_ns:
                continue

            historical_avg_ms = node.avg_duration_ns / 1e6
            if historical_avg_ms <= 0:
                continue

            ratio = recent_p99_ms / historical_avg_ms
            if ratio > 3.0:
                # Find the probe sequence of a currently slow in-flight
                # request matching this pattern — the smoking gun
                probe_sequence = None
                slow = tracker.slow_in_flight(
                    threshold_ms=historical_avg_ms * 2
                )
                for span in slow:
                    if span.pattern == pattern:
                        probe_sequence = span.probe_sequence
                        break

                anomalies.append(
                    {
                        "pattern": pattern,
                        "recent_p99_ms": round(recent_p99_ms, 2),
                        "historical_avg_ms": round(
                            historical_avg_ms, 2
                        ),
                        "ratio": round(ratio, 2),
                        "recent_samples": recent_count,
                        "probe_sequence": probe_sequence,
                    }
                )

        if not anomalies:
            return False, {}

        # Sort by worst ratio first
        anomalies.sort(key=lambda x: x["ratio"], reverse=True)
        return True, {"anomalies": anomalies}

    return CausalRule(
        name="request_duration_anomaly",
        description=(
            "Recent P99 latency is more than 3x the historical average for one "
            "or more endpoint patterns. The probe_sequence field (when present) "
            "shows what a currently slow in-flight request was doing — "
            "look for unexpected db.query chains indicating N+1 queries, "
            "or missing cache hits before a db call."
        ),
        predicate=_predicate,
        confidence=0.85,
        tags=["latency", "anomaly", "live"],
    )


# ====================================================================== #
# Shared helpers
# ====================================================================== #

_DB_SERVICES = {"postgres", "mysql", "sqlite", "redis"}
_DB_PROBES = {
    "django.db.query",
    "psycopg2.query.execute",
    "redis.command.execute",
}


def _is_db_node(node: Any) -> bool:
    """True for any node that represents a database or cache operation."""
    if node.service in _DB_SERVICES:
        return True
    return node.metadata.get("probe", "") in _DB_PROBES


# ====================================================================== #
# Rule 5 — gunicorn_worker_churn
# ====================================================================== #


def _gunicorn_worker_churn(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects gunicorn workers dying at a high rate.
    SIGABRT_timeout means workers are blocked and not responding to
    the master heartbeat — a synchronous blocking call is the usual cause.

    Fires when crash_count / total_gunicorn_events > 10%.
    """
    worker_nodes = [
        n for n in graph.all_nodes() if n.service == "gunicorn"
    ]
    if not worker_nodes:
        return False, {}

    crash_nodes = [
        n
        for n in worker_nodes
        if n.metadata.get("probe") == "gunicorn.worker.crash"
    ]
    if not crash_nodes:
        return False, {}

    total_events = sum(n.call_count for n in worker_nodes)
    crash_events = sum(n.call_count for n in crash_nodes)
    crash_rate = (
        crash_events / total_events if total_events else 0
    )

    if crash_rate < 0.10:
        return False, {}

    crash_reasons: Dict[str, int] = {}
    for n in crash_nodes:
        reason = n.metadata.get("reason", "unknown")
        crash_reasons[reason] = (
            crash_reasons.get(reason, 0) + n.call_count
        )

    return True, {
        "crash_rate": round(crash_rate, 2),
        "crash_events": crash_events,
        "total_events": total_events,
        "crash_reasons": crash_reasons,
        "hint": (
            "SIGABRT_timeout means workers stopped sending heartbeat — "
            "they are blocked on a slow synchronous call. "
            "Run CAUSAL WHERE tags='asyncio,blocking' to find it."
        ),
    }


WORKER_CHURN = CausalRule(
    name="gunicorn_worker_churn",
    description=(
        "gunicorn workers are crashing or timing out at an elevated rate. "
        "SIGABRT_timeout indicates workers are blocked and cannot respond to "
        "the master process heartbeat — a blocking synchronous call is the "
        "most common cause."
    ),
    predicate=_gunicorn_worker_churn,
    confidence=0.80,
    tags=["gunicorn", "latency", "blocking"],
)


# ====================================================================== #
# Rule 6 — cache_miss_amplification
# ====================================================================== #


def _cache_miss_amplification(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects when Redis GET call volume is not absorbing database load,
    meaning most cache lookups are misses and every request falls through
    to the database.

    Fires when:
        redis GET node exists  AND
        total DB call_count > redis GET call_count * 0.8
    """
    redis_get_node = None
    db_nodes: List[Any] = []

    for node in graph.all_nodes():
        if node.service == "redis" and "GET" in node.id.upper():
            if (
                redis_get_node is None
                or node.call_count > redis_get_node.call_count
            ):
                redis_get_node = node
        if _is_db_node(node) and node.call_count >= 10:
            db_nodes.append(node)

    if redis_get_node is None or not db_nodes:
        return False, {}

    redis_gets = redis_get_node.call_count or 0
    total_db = sum(n.call_count for n in db_nodes)

    if redis_gets < 10:
        return False, {}

    miss_ratio = total_db / redis_gets if redis_gets else 0
    if miss_ratio < 0.80:
        return False, {}

    return True, {
        "redis_get_count": redis_gets,
        "db_call_count": total_db,
        "miss_ratio": round(miss_ratio, 2),
        "redis_node": redis_get_node.id,
        "db_nodes": [n.id for n in db_nodes[:5]],
        "hint": (
            "Cache hit rate is low — most requests fall through to the database. "
            "Check: TTL too short, incorrect cache key construction, "
            "or cache invalidated too aggressively after writes."
        ),
    }


CACHE_MISS = CausalRule(
    name="cache_miss_amplification",
    description=(
        "Redis GET call volume is not absorbing database load. Cache misses "
        "are causing most requests to fall through to the database. "
        "Check TTL, key construction, and cache invalidation logic."
    ),
    predicate=_cache_miss_amplification,
    confidence=0.70,
    tags=["redis", "cache", "db", "performance"],
)

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .causal import CausalRule, PatternRegistry
from .runtime_graph import RuntimeGraph
from .temporal import TemporalStore


def make_anomaly_rule(tracker) -> CausalRule:
    """
    Factory that closes over the ActiveRequestTracker instance.
    The tracker is a singleton created by Engine — passed here at
    registry build time so the rule predicate can access it.

    Usage in Engine.__init__():
        self._tracker  = ActiveRequestTracker()
        self._registry = build_default_registry(tracker=self._tracker)
    """

    def _request_duration_anomaly(
        graph: RuntimeGraph,
        temporal: TemporalStore,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Detects endpoints/tasks whose recent completion latency
        has diverged significantly from their stored historical average.

        For each pattern that has:
            - historical avg from RuntimeGraph (call_count >= 50)
            - recent completions from ActiveRequestTracker (>= 10)
        we compare recent P99 against historical avg * 3.0.

        Evidence includes:
            - Which patterns are anomalous
            - The ratio (how much slower)
            - Probe sequence of a slow in-flight request if one exists
              (this is the "smoking gun" — what was the request doing?)
        """
        anomalies = []

        # Get all patterns with recent completions
        recent_summary = tracker.all_patterns_summary()

        for pattern, stats in recent_summary.items():
            if stats["count"] < 10:
                continue  # not enough recent data

            # Find the corresponding RuntimeGraph node
            # Pattern format from tracker is "service::normalized_name"
            # Try to find by matching the normalized name part
            node = None
            for n in graph.all_nodes():
                if (
                    n.id.endswith(f"::{pattern}")
                    or n.id == pattern
                ):
                    node = n
                    break
                # Also match by just the name portion after ::
                if (
                    "::" in n.id
                    and n.id.split("::", 1)[1] == pattern
                ):
                    node = n
                    break

            if node is None or (node.call_count or 0) < 50:
                continue  # not enough historical data to trust the average

            historical_avg_ms = (node.avg_duration_ns or 0) / 1e6
            if historical_avg_ms < 1.0:
                continue  # sub-millisecond operations — noise floor too low

            recent_p99_ms = stats["p99_ms"]
            ratio = (
                recent_p99_ms / historical_avg_ms
                if historical_avg_ms
                else 0
            )

            if ratio > 3.0:
                anomaly = {
                    "pattern": pattern,
                    "service": node.service,
                    "historical_avg_ms": round(
                        historical_avg_ms, 1
                    ),
                    "recent_p99_ms": recent_p99_ms,
                    "recent_avg_ms": stats["avg_ms"],
                    "ratio": round(ratio, 1),
                    "recent_sample_n": stats["count"],
                    "historical_n": node.call_count,
                }

                # Find a slow in-flight request for this pattern
                # to show the probe sequence (what was it doing?)
                slow = [
                    s
                    for s in tracker.slow_in_flight(
                        threshold_ms=historical_avg_ms * 2
                    )
                    if s.pattern == pattern
                ]
                if slow:
                    worst = max(
                        slow, key=lambda s: s.in_flight_ms
                    )
                    anomaly["slow_in_flight"] = {
                        "trace_id": worst.trace_id,
                        "in_flight_ms": round(
                            worst.in_flight_ms, 1
                        ),
                        # probe_sequence shows what the slow request was doing:
                        # e.g. ["request.entry", "django.db.query",
                        #        "django.db.query", "django.db.query"]
                        # Three DB queries in sequence → N+1 pattern on a slow request
                        "probe_sequence": worst.probe_sequence,
                    }

                anomalies.append(anomaly)

        if not anomalies:
            return False, {}

        return True, {
            "anomalous_endpoints": sorted(
                anomalies, key=lambda a: a["ratio"], reverse=True
            ),
            "hint": (
                "Recent P99 latency is 3x+ above historical average. "
                "Check probe_sequence of slow_in_flight requests for the pattern. "
                "Repeated 'django.db.query' entries suggest N+1. "
                "A single very long 'django.db.query' suggests a missing index. "
                "Run DIFF SINCE to check if a deployment coincides with onset."
            ),
        }

    return CausalRule(
        name="request_duration_anomaly",
        description=(
            "Recent request latency has diverged from historical baseline by 3x+. "
            "Something changed — compare probe_sequence of slow in-flight requests "
            "against the historical pattern to identify the new bottleneck."
        ),
        predicate=_request_duration_anomaly,
        confidence=0.85,
        tags=["latency", "anomaly", "live"],
    )


# ====================================================================== #
# Registry builder
# ====================================================================== #


def build_default_registry(
    tracker: Optional[Any] = None,
) -> PatternRegistry:
    """
    Return a PatternRegistry pre-loaded with all built-in rules.

    Parameters
    ----------
    tracker
        An ActiveRequestTracker instance. Required for the
        request_duration_anomaly rule to fire. If None, that rule
        is registered but will never match (safe — no crash).

    Called from:
        stacktracer/__init__.py  _init_pattern_registry()
        stacktracer/cli.py       build_engine()
        stacktracer/repl.py      bootstrap()
        stacktracer/backend/main.py  (no tracker — backend has no live requests)
    """
    registry = PatternRegistry()
    registry.register(RETRY_AMPLIFICATION)
    registry.register(NEW_SYNC_CALL)
    registry.register(LOOP_STARVATION)
    registry.register(DB_HOTSPOT)
    registry.register(WORKER_CHURN)
    registry.register(CACHE_MISS)
    registry.register(_make_request_duration_anomaly(tracker))
    return registry

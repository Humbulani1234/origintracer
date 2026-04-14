from typing import Any, Callable, Dict, List, Optional, Tuple

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import CausalRule, PatternRegistry
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore

# ---------------------- Request duration anomaly --------------------------


def _request_duration_anomaly(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict[str, Any]]:
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

    anomalies = []
    if tracker is None:
        return False, {}
    recent_summary = tracker.all_patterns_summary()
    for pattern, stats in recent_summary.items():
        if stats["count"] < 10:
            continue
        node = None
        for n in graph.all_nodes():
            if n.id == pattern or n.id.endswith(f"::{pattern}"):
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


REQUEST_DURATION_ANOMALY = CausalRule(
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

GROWTH_THRESHOLD = 10  # added nodes in one diff window


def _traffic_spike(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker=None,
) -> Tuple[bool, Dict]:
    total_added = set()
    for diff in temporal._diffs:
        total_added |= diff.added_node_ids

    if len(total_added) < GROWTH_THRESHOLD:
        return False, {}

    total_nodes = len(list(graph.all_nodes()))
    growth_rate = (
        round(len(total_added) / total_nodes * 100, 1)
        if total_nodes
        else 0
    )

    return True, {
        "added_node_count": len(total_added),
        "total_nodes": total_nodes,
        "growth_rate": growth_rate,
        "hint": "Consider horizontal scaling or rate limiting.",
    }


TRAFFIC_SPIKE = CausalRule(
    name="traffic_spike",
    description=(
        "A large number of new nodes appeared in the graph within the "
        "observed window — indicating a sudden traffic increase. "
        "Review autoscaling configuration and rate limiting."
    ),
    predicate=_traffic_spike,
    confidence=0.72,
    tags=["traffic", "scaling", "uvicorn", "gunicorn"],
)

TIMEOUT_THRESHOLD_NS = 2_000_000_000  # 2 seconds


def _external_dependency_timeout(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker=None,
) -> Tuple[bool, Dict]:
    slow = []
    for node in graph.all_nodes():
        meta = node.metadata or {}
        if not meta.get("external"):
            continue
        avg_ns = node.metadata.get("avg_duration_ns", 0)
        if avg_ns < TIMEOUT_THRESHOLD_NS:
            continue
        slow.append(
            {
                "node": node.id,
                "avg_ms": round(avg_ns / 1e6, 2),
                "hint": "Add a circuit breaker or reduce timeout threshold.",
            }
        )

    if not slow:
        return False, {}

    return True, {"slow_dependencies": slow}


EXTERNAL_DEPENDENCY_TIMEOUT = CausalRule(
    name="external_dependency_timeout",
    description=(
        "An external service dependency is averaging above 2 seconds. "
        "This will exhaust your thread pool or async task queue. "
        "Add a circuit breaker and aggressive timeout."
    ),
    predicate=_external_dependency_timeout,
    confidence=0.78,
    tags=["latency", "external", "asyncio", "timeout"],
)


def register(registry: PatternRegistry) -> None:
    registry.register(REQUEST_DURATION_ANOMALY)
    registry.register(TRAFFIC_SPIKE)
    registry.register(EXTERNAL_DEPENDENCY_TIMEOUT)

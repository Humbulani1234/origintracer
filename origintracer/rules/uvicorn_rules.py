from typing import Any, Callable, Dict, List, Optional, Tuple

from ..core.active_requests import ActiveRequestTracker
from ..core.causal import CausalRule
from ..core.runtime_graph import RuntimeGraph
from ..core.temporal import TemporalStore

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

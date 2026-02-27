"""
core/causal.py

Pattern-based causal matching over the RuntimeGraph.
Honest name: this is a rule engine, not probabilistic causal inference.
Rules are human-encoded failure patterns.

Each rule:
  - Has a unique name
  - Has a predicate that takes (graph, temporal_store) → (bool, dict)
  - Returns a CausalMatch with explanation, confidence, and evidence

This is v1. v2 would learn weights from confirmed incidents.

Built-in rules are calibrated against the revised probe set:

    asyncio probe  → asyncio.loop.epoll_wait, asyncio.loop.coro_call
    django probe   → django.db.query (via execute_wrapper), request.*
    uvicorn probe  → uvicorn.request.complete
    gunicorn probe → gunicorn.worker.*
    nginx probe    → nginx.connection.*, nginx.epoll.tick
    psycopg2 probe → psycopg2.query.execute  (service="postgres")
    redis probe    → redis.command.execute   (service="redis")
    celery probe   → celery.task.*           (service="celery")

Node type is derived from probe string prefix:
    "django.db.query"       → node_type = "django"
    "asyncio.loop.epoll_wait" → node_type = "asyncio"
    "psycopg2.query.execute"  → node_type = "psycopg2"
    "redis.command.execute"   → node_type = "redis"

Node service is set directly by the probe:
    "django", "asyncio", "postgres", "redis", "celery", "nginx" etc.

Rules that need to find database nodes should check:
    node.metadata.get("probe") in ("django.db.query", "psycopg2.query.execute")
    OR node.service in ("postgres", "mysql", "sqlite", "redis")
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
    confidence: float           # 0.0 – 1.0 (rule-assigned, not learned)
    explanation: str
    evidence: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule":        self.rule_name,
            "confidence":  self.confidence,
            "explanation": self.explanation,
            "evidence":    self.evidence,
            "timestamp":   self.timestamp,
        }


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

    Usage:
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
        """Run all registered rules, optionally filtered by tag."""
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
# Helpers shared across rules
# ====================================================================== #

_DB_SERVICES   = {"postgres", "mysql", "sqlite", "redis"}
_DB_PROBES     = {"django.db.query", "psycopg2.query.execute", "redis.command.execute"}
_ASYNC_PROBES  = {"asyncio.loop.epoll_wait", "asyncio.loop.coro_call"}


def _is_db_node(node) -> bool:
    """True for any node that represents a database or cache operation."""
    if node.service in _DB_SERVICES:
        return True
    probe = node.metadata.get("probe", "")
    return probe in _DB_PROBES


def _is_slow(node, threshold_ns: int = 50_000_000) -> bool:
    """True if avg_duration_ns exceeds threshold (default 50ms)."""
    return (node.avg_duration_ns or 0) > threshold_ns


# ====================================================================== #
# Rule 1 — new_sync_call_after_deployment
# ====================================================================== #

def _new_sync_call_after_deployment(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects edges that appeared after the most recent deployment marker.

    A new synchronous call introduced by a deployment is a common root
    cause of latency spikes — the new edge represents code that was not
    in the call graph before the deploy.

    Probe dependency: temporal.mark_event("deployment") must have been
    called by the CD pipeline. Without a deployment marker this rule
    returns False immediately and adds no overhead.
    """
    deployment_diff = temporal.label_diff("deployment")
    if not deployment_diff:
        return False, {}

    new_edges    = temporal.new_edges_since(deployment_diff.timestamp)
    new_db_edges = [k for k in new_edges if any(
        svc in k for svc in ("postgres", "sqlite", "mysql", "redis", "django")
    )]
    new_sync_edges = [k for k in new_edges if ":calls" in k]

    if not new_sync_edges:
        return False, {}

    return True, {
        "deployment_timestamp": deployment_diff.timestamp,
        "new_sync_edges":       new_sync_edges[:10],
        "new_db_edges":         new_db_edges[:5],
        "hint": (
            "New synchronous calls after deployment often indicate a blocking "
            "dependency added to the hot path. Correlate with HOTSPOT TOP 10."
        ),
    }


NEW_SYNC_CALL = CausalRule(
    name="new_sync_call_after_deployment",
    description=(
        "New synchronous call edges appeared immediately after the most recent "
        "deployment. A newly introduced dependency is the probable root cause "
        "of any latency increase — not the downstream metrics that degraded later."
    ),
    predicate=_new_sync_call_after_deployment,
    confidence=0.85,
    tags=["deployment", "latency", "async"],
)


# ====================================================================== #
# Rule 2 — asyncio_event_loop_starvation
# ====================================================================== #

def _asyncio_event_loop_starvation(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects event loop starvation from two independent signals, either
    of which alone is sufficient to fire the rule.

    Signal A — short epoll_wait duration (loop never idles):
        The asyncio epoll kprobe captures how long epoll_wait() blocked.
        A healthy loop alternates between long epoll_wait pauses (waiting
        for I/O) and short callback dispatch bursts.
        When epoll_wait avg_duration_ns < 1ms, the loop is finding work
        immediately on every tick — it is CPU-saturated or there is a
        blocking call eating the tick budget before reaching epoll_wait.
        Node: asyncio::epoll_wait   probe: asyncio.loop.epoll_wait

    Signal B — slow coroutine steps:
        sys.monitoring fires on every coroutine entry and return.
        If a coroutine node has avg_duration_ns > 50ms, it is executing
        for 50ms between awaits — blocking the entire event loop for that
        duration. Every other coroutine waiting on that loop is starved.
        Node: asyncio::<coro_name>  probe: asyncio.loop.coro_call

    One caveat on Signal A: a short epoll_wait can also mean the
    application is genuinely handling a high volume of I/O events
    (lots of concurrent connections). Check Signal B before concluding
    starvation — if coroutine steps are fast, high I/O throughput is
    the more likely explanation.
    """
    starvation_signals = []
    slow_coroutines    = []

    for node in graph.all_nodes():
        if node.node_type != "asyncio":
            continue

        probe = node.metadata.get("probe", "")

        # Signal A: epoll_wait almost never blocking
        if probe == "asyncio.loop.epoll_wait":
            avg = node.avg_duration_ns or 0
            if avg < 1_000_000 and node.call_count >= 20:   # <1ms avg, enough samples
                starvation_signals.append({
                    "signal":  "short_epoll_wait",
                    "node":    node.id,
                    "avg_ms":  round(avg / 1e6, 3),
                    "samples": node.call_count,
                })

        # Signal B: coroutine holding the loop for >50ms between awaits
        elif probe == "asyncio.loop.coro_call":
            avg = node.avg_duration_ns or 0
            if avg > 50_000_000 and node.call_count >= 5:   # >50ms, enough samples
                slow_coroutines.append({
                    "signal":  "slow_coro_step",
                    "node":    node.id,
                    "avg_ms":  round(avg / 1e6, 1),
                    "samples": node.call_count,
                })

    if not starvation_signals and not slow_coroutines:
        return False, {}

    return True, {
        "epoll_starvation_signals": starvation_signals,
        "slow_coroutines":          slow_coroutines[:5],
        "hint": (
            "If slow_coroutines is non-empty, those coroutines are the blockers. "
            "Look for sync I/O, time.sleep(), or CPU-bound work without await. "
            "If only epoll_starvation_signals fires with no slow coroutines, "
            "the loop may be handling legitimately high I/O volume — check HOTSPOT."
        ),
    }


LOOP_STARVATION = CausalRule(
    name="asyncio_event_loop_starvation",
    description=(
        "asyncio event loop starvation detected. Either epoll_wait is returning "
        "immediately (loop has no idle time) or coroutines are blocking the loop "
        "for >50ms between awaits. A blocking synchronous call on the async path "
        "is the most common cause."
    ),
    predicate=_asyncio_event_loop_starvation,
    confidence=0.80,
    tags=["asyncio", "latency", "blocking"],
)


# ====================================================================== #
# Rule 3 — db_query_hotspot
# ====================================================================== #

def _db_query_hotspot(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects database or cache nodes that account for a disproportionate
    share of observed calls or wall time.

    Covers all database probes:
        django.db.query         → ORM and raw SQL via execute_wrapper
        psycopg2.query.execute  → direct psycopg2 calls (service="postgres")
        redis.command.execute   → Redis calls (service="redis")

    Two hotspot conditions, either fires the rule:

    Condition 1 — call count hotspot (N+1 pattern):
        One query accounts for >30% of ALL calls in the graph.
        Classic symptom: an ORM query inside a loop.

    Condition 2 — duration hotspot (slow query):
        One query has avg_duration_ns > 200ms AND is called frequently
        (call_count >= 10). A single slow query dominating request time.
    """
    db_nodes    = [n for n in graph.all_nodes() if _is_db_node(n)]
    total_calls = sum(n.call_count for n in graph.all_nodes()) or 1

    call_hotspots     = []
    duration_hotspots = []

    for node in db_nodes:
        if node.call_count < 10:   # not enough data
            continue

        # Condition 1: call share
        pct = node.call_count / total_calls
        if pct > 0.30:
            call_hotspots.append({
                "node":       node.id,
                "service":    node.service,
                "call_count": node.call_count,
                "pct_total":  round(pct * 100, 1),
                "avg_ms":     round((node.avg_duration_ns or 0) / 1e6, 1),
            })

        # Condition 2: slow and frequent
        if (node.avg_duration_ns or 0) > 200_000_000:
            duration_hotspots.append({
                "node":       node.id,
                "service":    node.service,
                "avg_ms":     round((node.avg_duration_ns or 0) / 1e6, 1),
                "call_count": node.call_count,
                "total_ms":   round((node.total_duration_ns or 0) / 1e6, 0),
            })

    if not call_hotspots and not duration_hotspots:
        return False, {}

    return True, {
        "call_hotspots":     call_hotspots[:5],
        "duration_hotspots": sorted(
            duration_hotspots, key=lambda d: d["avg_ms"], reverse=True
        )[:5],
        "hint": (
            "For call_hotspots: run BLAME to find what is calling this query. "
            "Check for ORM queries inside loops (N+1). "
            "For duration_hotspots: check EXPLAIN on the query. "
            "Missing index or table scan is the most common cause."
        ),
    }


DB_HOTSPOT = CausalRule(
    name="db_query_hotspot",
    description=(
        "A database or cache node is a disproportionate share of observed calls "
        "or wall time. Either an N+1 query pattern (call count hotspot) or a "
        "slow query without an index (duration hotspot)."
    ),
    predicate=_db_query_hotspot,
    confidence=0.75,
    tags=["db", "performance", "latency"],
)


# ====================================================================== #
# Rule 4 — retry_amplification
# ====================================================================== #

def _retry_amplification(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects nodes where retry counts are a significant fraction of call
    counts — a sign that a downstream dependency is failing and retry
    loops are amplifying the load.

    Retry counts are recorded by two probes:
        celery probe  → emits retries=N on celery task nodes
        custom probes → any probe that emits retries=N in metadata

    The rule looks at node.metadata["retries"] which is populated when
    a probe emits an event with retries=N. It does NOT look at edge
    metadata because retry counts are per-task-invocation, not per-edge.

    Threshold: fires when any single node has retry_rate > 20% OR
    the aggregate retry rate across all observed nodes exceeds 15%.
    These are conservative thresholds — both conditions require
    meaningful sample sizes (call_count >= 10) to avoid false positives
    on cold starts.
    """
    retrying_nodes = []
    total_calls    = 0
    total_retries  = 0

    for node in graph.all_nodes():
        retries = int(node.metadata.get("retries", 0))
        calls   = node.call_count or 0

        if calls < 10:
            continue   # not enough data

        total_calls   += calls
        total_retries += retries

        if retries == 0:
            continue

        retry_rate = retries / calls
        if retry_rate > 0.20:   # >20% of executions retried
            retrying_nodes.append({
                "node":        node.id,
                "service":     node.service,
                "calls":       calls,
                "retries":     retries,
                "retry_rate":  round(retry_rate, 2),
                "probe":       node.metadata.get("probe", ""),
            })

    if total_calls == 0:
        return False, {}

    overall_retry_rate = total_retries / total_calls if total_calls else 0
    per_node_fired     = bool(retrying_nodes)
    aggregate_fired    = overall_retry_rate > 0.15

    if not per_node_fired and not aggregate_fired:
        return False, {}

    return True, {
        "overall_retry_rate": round(overall_retry_rate, 3),
        "total_calls":        total_calls,
        "total_retries":      total_retries,
        "worst_offenders":    sorted(
            retrying_nodes, key=lambda n: n["retry_rate"], reverse=True
        )[:5],
        "hint": (
            "Investigate the downstream dependencies of the retrying nodes. "
            "High retry rates mean a shared dependency (database, external API, "
            "message broker) is failing intermittently. Fix the dependency, "
            "not the retry policy."
        ),
    }


RETRY_AMPLIFICATION = CausalRule(
    name="retry_amplification",
    description=(
        "High retry rates detected. A downstream dependency is failing and retry "
        "loops are amplifying load. Investigate the dependency, not the caller."
    ),
    predicate=_retry_amplification,
    confidence=0.75,
    tags=["latency", "retry", "celery"],
)


# ====================================================================== #
# Rule 5 — gunicorn_worker_churn  (new — uses gunicorn probe events)
# ====================================================================== #

def _gunicorn_worker_churn(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects gunicorn workers dying and being replaced at a high rate.

    Uses gunicorn probe events (gunicorn.worker.crash, gunicorn.worker.exit).
    Worker churn with SIGABRT usually means workers are timing out
    (not sending heartbeat) — a sign the worker is blocked on a slow
    synchronous call and cannot respond to the master.

    Fires when:
        crash nodes exist AND crash_count / total_worker_events > 10%
    """
    worker_nodes = [
        n for n in graph.all_nodes()
        if n.service == "gunicorn"
    ]
    if not worker_nodes:
        return False, {}

    crash_nodes = [
        n for n in worker_nodes
        if n.metadata.get("probe") == "gunicorn.worker.crash"
    ]
    if not crash_nodes:
        return False, {}

    total_events = sum(n.call_count for n in worker_nodes)
    crash_events = sum(n.call_count for n in crash_nodes)
    crash_rate   = crash_events / total_events if total_events else 0

    if crash_rate < 0.10:
        return False, {}

    crash_reasons = {}
    for n in crash_nodes:
        reason = n.metadata.get("reason", "unknown")
        crash_reasons[reason] = crash_reasons.get(reason, 0) + n.call_count

    return True, {
        "crash_rate":    round(crash_rate, 2),
        "crash_events":  crash_events,
        "total_events":  total_events,
        "crash_reasons": crash_reasons,
        "hint": (
            "SIGABRT_timeout means workers stopped sending heartbeat — "
            "they are blocked on a slow synchronous call. "
            "Run CAUSAL WHERE tags='asyncio,blocking' to find it. "
            "Increase --timeout only as a temporary measure."
        ),
    }


WORKER_CHURN = CausalRule(
    name="gunicorn_worker_churn",
    description=(
        "gunicorn workers are crashing or timing out at an elevated rate. "
        "SIGABRT_timeout indicates workers are blocked and cannot respond to "
        "the master process heartbeat. A blocking synchronous call is the "
        "most common cause."
    ),
    predicate=_gunicorn_worker_churn,
    confidence=0.80,
    tags=["gunicorn", "latency", "blocking"],
)


# ====================================================================== #
# Rule 6 — cache_miss_amplification  (new — redis probe)
# ====================================================================== #

def _cache_miss_amplification(
    graph: RuntimeGraph,
    temporal: TemporalStore,
) -> Tuple[bool, Dict]:
    """
    Detects high Redis GET call volume without corresponding cache hits,
    suggesting cache misses are causing every request to fall through to
    the database.

    Fires when:
        redis::GET node exists AND
        postgres/django db node call_count > redis GET call_count * 0.8
        (meaning most cache lookups are misses — DB is handling almost
         everything the cache should be absorbing)

    This requires both a Redis probe and a database probe to be active.
    """
    redis_get_node = None
    db_nodes       = []

    for node in graph.all_nodes():
        if node.service == "redis" and "GET" in node.id.upper():
            if redis_get_node is None or node.call_count > redis_get_node.call_count:
                redis_get_node = node
        if _is_db_node(node) and node.call_count >= 10:
            db_nodes.append(node)

    if redis_get_node is None or not db_nodes:
        return False, {}

    redis_gets  = redis_get_node.call_count or 0
    total_db    = sum(n.call_count for n in db_nodes)

    if redis_gets < 10:   # not enough data
        return False, {}

    # If DB calls >= 80% of Redis GET calls, cache is not absorbing load
    miss_ratio = total_db / redis_gets if redis_gets else 0
    if miss_ratio < 0.80:
        return False, {}

    return True, {
        "redis_get_count":  redis_gets,
        "db_call_count":    total_db,
        "miss_ratio":       round(miss_ratio, 2),
        "redis_node":       redis_get_node.id,
        "db_nodes":         [n.id for n in db_nodes[:5]],
        "hint": (
            "Cache hit rate is low — most requests fall through to the database. "
            "Check: (1) cache TTL too short, (2) cache key construction incorrect, "
            "(3) cache invalidated too aggressively after writes."
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


# ====================================================================== #
# Registry builder
# ====================================================================== #

def build_default_registry() -> PatternRegistry:
    """Return a PatternRegistry pre-loaded with all built-in rules."""
    registry = PatternRegistry()
    registry.register(NEW_SYNC_CALL)
    registry.register(LOOP_STARVATION)
    registry.register(DB_HOTSPOT)
    registry.register(RETRY_AMPLIFICATION)
    registry.register(WORKER_CHURN)
    registry.register(CACHE_MISS)
    return registry
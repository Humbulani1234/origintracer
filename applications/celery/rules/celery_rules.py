"""
stacktracer/rules/celery_rules.py

Custom causal rules for the Celery probe.

Auto-discovered by StackTracer because:
  - This file is named *_rules.py
  - It lives in stacktracer/rules/ in the project root
  - It exposes a register(registry) function

Each rule is a predicate function that:
  - Receives the live RuntimeGraph and TemporalStore
  - Returns (bool, dict) — (fired, evidence)
  - The evidence dict is what the engineer sees in the REPL

These rules are queried via:
    CAUSAL WHERE tags = "celery"
    CAUSAL                          (runs all rules including these)
"""

from __future__ import annotations

from stacktracer.core.causal import CausalRule, PatternRegistry


def register(registry: PatternRegistry) -> None:
    """
    Called automatically when this file is loaded.
    Register all rules from this file here.
    """
    registry.register(CausalRule(
        name="celery_sync_db_call",
        description=(
            "A Celery task is making synchronous database calls. "
            "In a prefork worker, this blocks the worker process thread "
            "for the full query duration, reducing pool throughput."
        ),
        tags=["celery", "blocking", "database"],
        predicate=_sync_db_in_celery,
        confidence=0.85,
    ))

    registry.register(CausalRule(
        name="celery_retry_amplification",
        description=(
            "Celery tasks are being retried at a high rate. "
            "A downstream failure (database, external API) is amplifying "
            "into many retried tasks consuming the worker pool."
        ),
        tags=["celery", "retry"],
        predicate=_retry_amplification,
        confidence=0.80,
    ))

    registry.register(CausalRule(
        name="celery_task_duration_spike",
        description=(
            "One or more Celery tasks have significantly higher average "
            "duration than the rest of the task queue. Likely a new "
            "slow operation introduced in a recent deployment."
        ),
        tags=["celery", "latency"],
        predicate=_task_duration_spike,
        confidence=0.75,
    ))


# ====================================================================== #
# Predicates
# ====================================================================== #

def _sync_db_in_celery(graph, temporal) -> tuple[bool, dict]:
    """
    Fires when a celery node has a direct edge to a postgres/sqlite node
    where avg_duration_ns > 50ms.

    Graph pattern:
        celery::myapp.tasks.process_report
            → postgres::PQexec  (avg=340ms, blocking_call=True)

    This means the Celery task is calling the database synchronously
    on the worker thread — not via an async path or thread pool.
    """
    evidence = []

    for node in graph.all_nodes():
        if node.service != "celery":
            continue

        for edge in graph.neighbors(node.id):
            target = graph.get_node(edge.target)
            if target is None:
                continue

            is_db = target.service in ("postgres", "sqlite", "mysql")
            is_slow = (target.avg_duration_ns or 0) > 50_000_000   # 50ms

            if is_db and is_slow:
                evidence.append({
                    "task": node.id,
                    "db_node": target.id,
                    "avg_ms": round((target.avg_duration_ns or 0) / 1e6, 1),
                    "call_count": edge.call_count,
                })

    return bool(evidence), {
        "blocking_db_calls": evidence,
        "remediation": (
            "Use sync_to_async() to run database calls in a thread pool, "
            "or switch to an async database driver."
        ),
    }


def _retry_amplification(graph, temporal) -> tuple[bool, dict]:
    """
    Fires when the total retry count across celery nodes is more than
    30% of the total task call count.

    A healthy system has retries << calls.
    A system under downstream failure has retries approaching calls.

    Evidence includes the specific tasks retrying most frequently.
    """
    total_calls = 0
    total_retries = 0
    retrying_tasks = []

    for node in graph.all_nodes():
        if node.service != "celery":
            continue

        calls = node.call_count or 0
        # Retry count is stored in metadata.retries if the probe emitted it
        retries = node.metadata.get("retries", 0) if hasattr(node, "metadata") else 0

        total_calls  += calls
        total_retries += retries

        if retries > 0 and calls > 0:
            retry_rate = retries / calls
            if retry_rate > 0.1:   # more than 10% of executions retried
                retrying_tasks.append({
                    "task": node.id,
                    "calls": calls,
                    "retries": retries,
                    "retry_rate": round(retry_rate, 2),
                })

    if total_calls == 0:
        return False, {}

    overall_retry_rate = total_retries / total_calls
    fired = overall_retry_rate > 0.30   # more than 30% overall retry rate

    return fired, {
        "overall_retry_rate": round(overall_retry_rate, 2),
        "total_calls": total_calls,
        "total_retries": total_retries,
        "worst_offenders": sorted(
            retrying_tasks,
            key=lambda t: t["retry_rate"],
            reverse=True,
        )[:5],
        "remediation": (
            "Check downstream dependencies (database, external APIs). "
            "High retry rates usually mean a shared dependency is failing."
        ),
    }


def _task_duration_spike(graph, temporal) -> tuple[bool, dict]:
    """
    Fires when any celery node has avg_duration_ns more than 5× the
    median across all celery nodes with at least 5 calls.

    This detects a specific slow task standing out from the baseline,
    which often indicates a new slow operation in a recent deployment.

    Correlate with DIFF SINCE deployment to confirm.
    """
    celery_nodes = [
        n for n in graph.all_nodes()
        if n.service == "celery"
        and (n.call_count or 0) >= 5
        and n.avg_duration_ns
    ]

    if len(celery_nodes) < 2:
        return False, {}

    durations = sorted(n.avg_duration_ns for n in celery_nodes)
    median = durations[len(durations) // 2]

    if median == 0:
        return False, {}

    spikes = [
        {
            "task": n.id,
            "avg_ms": round(n.avg_duration_ns / 1e6, 1),
            "median_ms": round(median / 1e6, 1),
            "ratio": round(n.avg_duration_ns / median, 1),
        }
        for n in celery_nodes
        if n.avg_duration_ns > median * 5
    ]

    return bool(spikes), {
        "slow_tasks": spikes,
        "median_ms": round(median / 1e6, 1),
        "remediation": (
            "Run 'DIFF SINCE deployment' to check if these tasks "
            "gained new slow edges after a recent deployment."
        ),
    }
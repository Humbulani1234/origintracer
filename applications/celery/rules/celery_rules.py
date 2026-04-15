from __future__ import annotations

from typing import Optional

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import CausalRule, PatternRegistry
from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.core.temporal import TemporalStore


def sync_db_in_celery(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> tuple[bool, dict]:
    """
    Fires when a celery node has a direct edge to a postgres/sqlite node
    where avg_duration_ns > 50ms.

    This means the Celery task is calling the database synchronously
    on the worker thread - not via an async path or thread pool.
    """
    evidence = []

    for node in graph.all_nodes():
        if node.service != "celery":
            continue
        for edge in graph.neighbors(node.id):
            target = graph.get_node(edge.target)
            if target is None:
                continue

            is_db = target.service in (
                "postgres",
                "sqlite",
                "mysql",
            )
            is_slow = (
                target.avg_duration_ns or 0
            ) > 50_000_000  # 50ms

            if is_db and is_slow:
                evidence.append(
                    {
                        "task": node.id,
                        "db_node": target.id,
                        "avg_ms": round(
                            (target.avg_duration_ns or 0) / 1e6,
                            1,
                        ),
                        "call_count": edge.call_count,
                    }
                )

    return bool(evidence), {
        "blocking_db_calls": evidence,
        "remediation": (
            "Use sync_to_async() to run database calls in a thread pool, "
            "or switch to an async database driver."
        ),
    }


def celery_task_duration_spike(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> tuple[bool, dict]:
    """
    Fires when any celery node has avg_duration_ns more than 5× the
    median across all celery nodes with at least 5 calls.

    This detects a specific slow task standing out from the baseline,
    which often indicates a new slow operation in a recent deployment.

    Correlate with DIFF SINCE deployment to confirm.
    """
    celery_nodes = [
        n
        for n in graph.all_nodes()
        if n.service == "celery"
        and n.metadata.get("probe") == "celery.task.end"
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
        if n.avg_duration_ns > median * 2
    ]

    return bool(spikes), {
        "slow_tasks": spikes,
        "median_ms": round(median / 1e6, 1),
        "remediation": (
            "Run 'DIFF SINCE deployment' to check if these tasks "
            "gained new slow edges after a recent deployment."
        ),
    }


SYNC_DB_IN_CELERY = CausalRule(
    name="sync_db_in_celery",
    description=(
        "A Celery task is making synchronous database calls. "
        "In a prefork worker, this blocks the worker process thread "
        "for the full query duration, reducing pool throughput."
    ),
    tags=["celery", "blocking", "database"],
    predicate=sync_db_in_celery,
    confidence=0.85,
)

CELERY_TASK_DURATION_SPIKE = CausalRule(
    name="celery_task_duration_spike",
    description=(
        "One or more Celery tasks have significantly higher average "
        "duration than the rest of the task queue. Likely a new "
        "slow operation introduced in a recent deployment."
    ),
    tags=["celery", "latency"],
    predicate=celery_task_duration_spike,
    confidence=0.75,
)


def register(registry: PatternRegistry) -> None:
    registry.register(SYNC_DB_IN_CELERY)
    registry.register(CELERY_TASK_DURATION_SPIKE)

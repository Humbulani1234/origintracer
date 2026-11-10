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
    """Match slow synchronous database calls directly from Celery tasks."""
    evidence = []
    for node in graph.all_nodes():
        if node.service != "celery":
            continue
        for edge in graph.neighbors(node.id):
            target = graph.get_node(edge.target)
            if target is None or target.service not in {
                "postgres",
                "sqlite",
                "mysql",
            }:
                continue
            if (target.avg_duration_ns or 0) <= 50_000_000:
                continue
            evidence.append(
                {
                    "task": node.id,
                    "db_node": target.id,
                    "avg_ms": round(
                        (target.avg_duration_ns or 0) / 1e6, 1
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
    """Match Celery task types averaging more than twice the task median."""
    celery_nodes = [
        node
        for node in graph.all_nodes()
        if node.service == "celery"
        and (
            node.metadata.get("last_probe") == "celery.task.end"
            or node.metadata.get("probe") == "celery.task.end"
        )
        and (node.call_count or 0) >= 5
        and node.avg_duration_ns
    ]
    if len(celery_nodes) < 2:
        return False, {}

    durations = sorted(
        node.avg_duration_ns for node in celery_nodes
    )
    middle = len(durations) // 2
    median = (
        durations[middle]
        if len(durations) % 2
        else (durations[middle - 1] + durations[middle]) / 2
    )
    if median <= 0:
        return False, {}

    spikes = [
        {
            "task": node.id,
            "avg_ms": round(node.avg_duration_ns / 1e6, 1),
            "median_ms": round(median / 1e6, 1),
            "ratio": round(node.avg_duration_ns / median, 1),
        }
        for node in celery_nodes
        if node.avg_duration_ns > median * 2
    ]
    return bool(spikes), {
        "slow_tasks": spikes,
        "median_ms": round(median / 1e6, 1),
        "remediation": (
            "Run 'DIFF SINCE deployment' to check whether these tasks "
            "gained new slow edges after a recent deployment."
        ),
    }


SYNC_DB_IN_CELERY = CausalRule(
    name="sync_db_in_celery",
    description=(
        "A Celery task directly calls a synchronous database operation "
        "whose measured average duration exceeds 50 ms."
    ),
    tags=["celery", "blocking", "database"],
    predicate=sync_db_in_celery,
    confidence=0.85,
)

CELERY_TASK_DURATION_SPIKE = CausalRule(
    name="celery_task_duration_spike",
    description=(
        "A Celery task type has an average duration more than twice the "
        "median of task types with at least five observations."
    ),
    tags=["celery", "latency"],
    predicate=celery_task_duration_spike,
    confidence=0.75,
)


def register(registry: PatternRegistry) -> None:
    """Register the application-local Celery causal rules."""
    registry.register(SYNC_DB_IN_CELERY)
    registry.register(CELERY_TASK_DURATION_SPIKE)

"""
stacktracer/rules/celery_rules.py

Custom causal rules for the Celery application.

Auto-discovered by StackTracer because:
  - Named *_rules.py
  - Lives in stacktracer/rules/ in the project root
  - Exposes a register(registry) function

Updated for revised probe set:
    celery probe    → service="celery"     probe="celery.task.*"
    psycopg2 probe  → service="postgres"   probe="psycopg2.query.execute"
    redis probe     → service="redis"      probe="redis.command.execute"
    django probe    → service="django"     probe="django.db.query"  (if Django is also active)

The previous version checked target.service in ("postgres", "sqlite", "mysql")
which was correct but depended on the psycopg2 probe being active and emitting
service="postgres". That is now consistent with psycopg2_probe.py.

Query via REPL:
    CAUSAL WHERE tags = "celery"
    CAUSAL                              (runs all rules including these)
"""

from __future__ import annotations

from stacktracer.core.causal import CausalRule, PatternRegistry


def register(registry: PatternRegistry) -> None:
    registry.register(CausalRule(
        name="celery_sync_db_call",
        description=(
            "A Celery task is making synchronous database calls directly on the "
            "worker thread. In a prefork worker this blocks the entire worker "
            "for the query duration, reducing pool throughput."
        ),
        tags=["celery", "blocking", "database"],
        predicate=_sync_db_in_celery,
        confidence=0.85,
    ))

    registry.register(CausalRule(
        name="celery_retry_amplification",
        description=(
            "Celery tasks are being retried at a high rate. A downstream failure "
            "(database, Redis, external API) is amplifying into many retried tasks "
            "consuming the worker pool."
        ),
        tags=["celery", "retry"],
        predicate=_retry_amplification,
        confidence=0.80,
    ))

    registry.register(CausalRule(
        name="celery_task_duration_spike",
        description=(
            "One or more Celery tasks have significantly higher average duration "
            "than the rest of the task queue. Likely a new slow operation introduced "
            "in a recent deployment."
        ),
        tags=["celery", "latency"],
        predicate=_task_duration_spike,
        confidence=0.75,
    ))

    registry.register(CausalRule(
        name="celery_redis_slow_broker",
        description=(
            "Redis commands from Celery tasks are averaging more than 10ms. "
            "If Redis is used as the Celery broker, slow Redis means slow task "
            "dispatch and acknowledgement for every task in the queue."
        ),
        tags=["celery", "redis", "latency"],
        predicate=_redis_slow_broker,
        confidence=0.75,
    ))


# ====================================================================== #
# Helpers
# ====================================================================== #

# Services that represent relational databases
_DB_SERVICES = {"postgres", "mysql", "sqlite"}

# Services that represent all data stores (including cache)
_DATA_SERVICES = {"postgres", "mysql", "sqlite", "redis"}


def _is_db_node(node) -> bool:
    """True for postgres/mysql/sqlite nodes — relational DB only."""
    if node.service in _DB_SERVICES:
        return True
    # Django ORM queries go through service="django", probe="django.db.query"
    return node.metadata.get("probe") == "django.db.query"


def _is_data_node(node) -> bool:
    """True for any data store node including Redis."""
    return node.service in _DATA_SERVICES or _is_db_node(node)


# ====================================================================== #
# Predicates
# ====================================================================== #

def _sync_db_in_celery(graph, temporal) -> tuple[bool, dict]:
    """
    Fires when a celery task node has a direct edge to a database node
    where the average query duration exceeds 50ms.

    Graph pattern matched:
        celery::myapp.tasks.process_report
            → postgres::SELECT * FROM reports WHERE id = ?   (avg=340ms)

    Service names that count as database:
        "postgres"  → from psycopg2_probe.py (traced_connect)
        "mysql"     → from a mysql probe following the same pattern
        "sqlite"    → from direct sqlite3 calls with a probe
        "django"    → from django execute_wrapper (probe=django.db.query)
    """
    evidence = []

    for node in graph.all_nodes():
        if node.service != "celery":
            continue

        for edge in graph.neighbors(node.id):
            target = graph.get_node(edge.target)
            if target is None:
                continue

            if not _is_db_node(target):
                continue

            avg_ns = target.avg_duration_ns or 0
            if avg_ns > 50_000_000:   # 50ms
                evidence.append({
                    "task":       node.id,
                    "db_node":    target.id,
                    "db_service": target.service,
                    "avg_ms":     round(avg_ns / 1e6, 1),
                    "call_count": edge.call_count,
                })

    return bool(evidence), {
        "blocking_db_calls": evidence,
        "remediation": (
            "Use sync_to_async() to run database calls in a thread pool, "
            "or switch to an async database driver (asyncpg, databases)."
        ),
    }


def _retry_amplification(graph, temporal) -> tuple[bool, dict]:
    """
    Fires when celery task retry rate exceeds 30% of total task calls,
    or when any single task has a retry rate > 20%.

    Retry counts come from the celery probe which emits
    retries=task.request.retries on each TASK_START event.
    The Engine accumulates this in node.metadata["retries"].
    """
    total_calls   = 0
    total_retries = 0
    retrying_tasks = []

    for node in graph.all_nodes():
        if node.service != "celery":
            continue

        calls   = node.call_count or 0
        retries = int(node.metadata.get("retries", 0))

        if calls < 5:   # not enough data
            continue

        total_calls   += calls
        total_retries += retries

        if retries > 0:
            retry_rate = retries / calls
            if retry_rate > 0.20:
                retrying_tasks.append({
                    "task":       node.id,
                    "calls":      calls,
                    "retries":    retries,
                    "retry_rate": round(retry_rate, 2),
                })

    if total_calls == 0:
        return False, {}

    overall_retry_rate = total_retries / total_calls
    fired = overall_retry_rate > 0.30 or bool(retrying_tasks)

    return fired, {
        "overall_retry_rate": round(overall_retry_rate, 2),
        "total_calls":        total_calls,
        "total_retries":      total_retries,
        "worst_offenders":    sorted(
            retrying_tasks, key=lambda t: t["retry_rate"], reverse=True
        )[:5],
        "remediation": (
            "Check downstream dependencies (database, Redis, external APIs). "
            "High retry rates usually mean a shared dependency is failing. "
            "Fix the dependency root cause, not the retry policy."
        ),
    }


def _task_duration_spike(graph, temporal) -> tuple[bool, dict]:
    """
    Fires when any celery task has avg_duration_ns more than 5× the
    median of all celery tasks with sufficient call count.

    Correlate with DIFF SINCE deployment to confirm a new slow edge.
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
    median    = durations[len(durations) // 2]

    if median == 0:
        return False, {}

    spikes = [
        {
            "task":      n.id,
            "avg_ms":    round(n.avg_duration_ns / 1e6, 1),
            "median_ms": round(median / 1e6, 1),
            "ratio":     round(n.avg_duration_ns / median, 1),
        }
        for n in celery_nodes
        if n.avg_duration_ns > median * 5
    ]

    return bool(spikes), {
        "slow_tasks": spikes,
        "median_ms":  round(median / 1e6, 1),
        "remediation": (
            "Run 'DIFF SINCE deployment' to check if these tasks "
            "gained new slow edges after a recent deployment."
        ),
    }


def _redis_slow_broker(graph, temporal) -> tuple[bool, dict]:
    """
    Fires when Redis commands average more than 10ms.

    If Redis is used as the Celery broker or result backend, slow Redis
    affects every task dispatch and acknowledgement. This rule detects
    Redis slowness in the context of a Celery worker process.

    Only fires in graphs that have both celery and redis nodes —
    meaning the celery and redis probes are both active.
    """
    has_celery = any(n.service == "celery" for n in graph.all_nodes())
    if not has_celery:
        return False, {}

    slow_redis = []
    for node in graph.all_nodes():
        if node.service != "redis":
            continue
        if (node.call_count or 0) < 10:
            continue

        avg_ns = node.avg_duration_ns or 0
        if avg_ns > 10_000_000:   # 10ms
            slow_redis.append({
                "command": node.id,
                "avg_ms":  round(avg_ns / 1e6, 1),
                "calls":   node.call_count,
            })

    return bool(slow_redis), {
        "slow_redis_commands": sorted(
            slow_redis, key=lambda r: r["avg_ms"], reverse=True
        )[:10],
        "remediation": (
            "Check Redis memory usage, network latency to Redis, and whether "
            "Redis is being used for large value storage (should be a cache, "
            "not a document store). "
            "For Celery specifically: consider using a separate Redis instance "
            "for the broker vs the result backend."
        ),
    }
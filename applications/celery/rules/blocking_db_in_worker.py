# myapp/rules/blocking_db_in_worker.py

from stacktracer.core.causal import CausalRule
from stacktracer import get_engine


def _sync_db_inside_async_worker(graph, temporal):
    """
    Pattern: celery node → postgres node where duration_ns is high.

    This means a Celery task made a synchronous PQexec call.
    If the asyncio event loop is also running on this thread,
    that call blocks the entire loop — starvation.

    Structural pattern we're looking for:

        [celery::some.task.name]
                │  (calls edge)
                ▼
        [postgres::PQexec]   ← blocking_call=True, avg_duration > 50ms
    """

    evidence = []

    for node in graph.all_nodes():
        if node.service != "celery":
            continue

        # Walk outgoing edges from this celery node
        for edge in graph.neighbors(node.id):
            target = graph.get_node(edge.target)
            if target is None:
                continue
            if target.service != "postgres":
                continue

            # Check the postgres node's average blocking duration
            avg_ms = (target.avg_duration_ns or 0) / 1e6
            if avg_ms < 50:                         # threshold: 50ms = clearly blocking
                continue

            blocking = target.metadata.get("blocking_call", False)
            if not blocking:
                continue

            evidence.append({
                "worker_task": node.id,
                "db_node": target.id,
                "avg_blocking_ms": round(avg_ms, 2),
                "call_count": edge.call_count,
            })

    if not evidence:
        return False, {}

    # Sort by worst offender first
    evidence.sort(key=lambda e: e["avg_blocking_ms"], reverse=True)
    return True, {"blocking_calls": evidence}


# Register the rule with the live engine
engine = get_engine()
engine.causal.register(CausalRule(
    name="sync_db_call_in_async_worker",
    description=(
        "A Celery task is making synchronous PostgreSQL calls (via PQexec) "
        "from inside an async context. The blocking kernel call is confirmed "
        "by eBPF. This stalls the asyncio event loop for the duration of each "
        "query — use asyncpg or run the query in a thread pool executor."
    ),
    predicate=_sync_db_inside_async_worker,
    confidence=0.92,
    tags=["celery", "db", "blocking", "asyncio"],
))
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..core.active_requests import ActiveRequestTracker
from ..core.causal import CausalRule, _is_gunicorn_worker
from ..core.runtime_graph import RuntimeGraph
from ..core.temporal import TemporalStore

# ----------------------- Worker imbalance ------------------------------


def _worker_imbalance(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
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
        # Sum the call_counts instead of counting the list length
        total_handled = sum(e.call_count for e in handled)

        worker_loads[worker.id] = {
            "worker_pid": worker.metadata.get("worker_pid"),
            "worker_class": worker.metadata.get("worker_class"),
            "handled_count": total_handled,
        }

    counts = [v["handled_count"] for v in worker_loads.values()]
    max_load = max(counts)
    min_load = min(counts)

    if min_load == 0 or max_load / min_load < 2.0:
        return False, {}

    # First, identify which worker has the max load
    # (Assuming worker_loads is a dict where keys are IDs)
    busiest_id = max(
        worker_loads,
        key=lambda k: worker_loads[k]["handled_count"],
    )

    return True, {
        "workers": list(worker_loads.values()),
        "busiest_worker": busiest_id,  # <--- Add this to satisfy the test
        "max_load": max_load,
        "min_load": min_load,
        "ratio": round(max_load / max(min_load, 1), 1),
        "hint": (
            "A worker with zero or few handled requests may be stuck on a "
            "blocking call. Check for synchronous I/O or CPU-bound work on "
            f"the busiest worker ({busiest_id}) endpoints."
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

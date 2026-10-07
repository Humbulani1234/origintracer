"""
Deterministic causal rules for events produced by the LangGraph probe.

Separation from the engine
--------------------------
The runtime engine constructs a graph from normalized lineage and deliberately
does not understand graph-framework concepts. This module owns the corresponding
interpretation layer: it classifies nodes using stable metadata emitted by the
LangGraph probe and evaluates framework-specific symptoms over the aggregate
runtime graph.

Rules implemented
-----------------
``LOOP_RUNAWAY``
    Finds an LLM node invoked at least eight times per owning graph invocation.

``AGENT_LATENCY_HOTSPOT``
    Finds one leaf-work node (LLM or tool) responsible for more than 40 percent
    of observed leaf-work time.

Aggregation limits
------------------
``RuntimeGraph`` groups nodes by ``service::name`` across the process lifetime.
Consequently these rules compare aggregate call counts and durations, not one
individual trace. Metadata that changes from invocation to invocation is unsafe
for classification; the probe's stable ``langgraph_kind`` and
``is_langgraph_root`` fields are the only fields used here for that purpose.

The rules intentionally do not claim to detect branch overlap, identical-argument
retries, or unused state. Those questions require ordered event history, argument
history, or static source analysis, none of which is present in the aggregate
graph passed to a ``CausalRule`` predicate.
"""

from collections import deque
from typing import Any, Dict, Iterable, List, Optional, Tuple

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import CausalRule, PatternRegistry
from origintracer.core.runtime_graph import (
    GraphNode,
    RuntimeGraph,
)
from origintracer.core.temporal import TemporalStore


def _is_langgraph_node(node: Any) -> bool:
    """Return whether a graph node originated from the LangGraph probe."""
    return getattr(node, "service", None) == "langgraph"


def _langgraph_kind(node: Any) -> Optional[str]:
    """Read the probe's stable operation classification."""
    if not _is_langgraph_node(node):
        return None
    return getattr(node, "metadata", {}).get("langgraph_kind")


def _is_langgraph_root(node: Any) -> bool:
    """Return whether the probe classified this chain as an outer graph run."""
    return (
        _langgraph_kind(node) == "chain"
        and getattr(node, "metadata", {}).get(
            "is_langgraph_root"
        )
        is True
    )


def _is_langgraph_llm_node(node: Any) -> bool:
    """Return whether a graph node represents model work."""
    return _langgraph_kind(node) == "llm"


def _is_langgraph_tool_node(node: Any) -> bool:
    """Return whether a graph node represents tool work."""
    return _langgraph_kind(node) == "tool"


def _nearest_roots(
    graph: RuntimeGraph, node_id: str, max_depth: int = 20
) -> Iterable[GraphNode]:
    """
    Yield the nearest owning LangGraph roots found by upstream breadth-first search.

    Real callback trees often contain more than two wrapper levels between an LLM
    and the compiled graph. A bounded BFS follows actual graph edges without
    assuming a fixed framework nesting depth. Traversal stops beyond the first
    depth containing roots so a nested graph is attributed to its nearest owner.
    """
    queue = deque(
        (edge.source, 1) for edge in graph.callers(node_id)
    )
    visited = {node_id}
    root_depth: Optional[int] = None

    while queue:
        current_id, depth = queue.popleft()
        if current_id in visited or depth > max_depth:
            continue
        if root_depth is not None and depth > root_depth:
            break
        visited.add(current_id)

        current = graph.get_node(current_id)
        if current is None:
            continue
        if _is_langgraph_root(current):
            root_depth = depth
            yield current
            continue

        for edge in graph.callers(current_id):
            queue.append((edge.source, depth + 1))


def _loop_runaway(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Detect an LLM node that iterates excessively per owning graph run.

    Each unique callback span is counted once by the lineage-aware engine, so the
    ratio compares logical model invocations with logical outer invocations. The
    nearest root is discovered from causal edges rather than a fixed parent or
    grandparent assumption.
    """
    threshold = 8
    hits: List[Dict[str, Any]] = []
    seen = set()

    for node in graph.all_nodes():
        if (
            not _is_langgraph_llm_node(node)
            or node.call_count < threshold
        ):
            continue

        for root in _nearest_roots(graph, node.id):
            if root.call_count <= 0:
                continue
            key = (node.id, root.id)
            if key in seen:
                continue
            ratio = node.call_count / root.call_count
            if ratio < threshold:
                continue

            seen.add(key)
            hits.append(
                {
                    "llm_node": node.id,
                    "root_invocation": root.id,
                    "iteration_count": node.call_count,
                    "graph_invocations": root.call_count,
                    "ratio": round(ratio, 1),
                    "avg_llm_call_ms": round(
                        (node.avg_duration_ns or 0) / 1e6, 2
                    ),
                    "hint": (
                        "The model is iterating far more than expected per "
                        "graph run. Inspect the conditional exit and tool error "
                        "paths for state that repeatedly requests more work."
                    ),
                }
            )

    if not hits:
        return False, {}
    hits.sort(key=lambda hit: hit["ratio"], reverse=True)
    return True, {"runaway_loops": hits[:10]}


LOOP_RUNAWAY = CausalRule(
    name="loop_runaway",
    description=(
        "An LLM node inside a LangGraph execution runs at least eight "
        "times per owning graph invocation. Inspect the conditional exit "
        "and tool error paths for a non-converging state transition."
    ),
    predicate=_loop_runaway,
    confidence=0.85,
    tags=["langgraph", "agent", "loop"],
)


def _agent_latency_hotspot(
    graph: RuntimeGraph,
    temporal: TemporalStore,
    tracker: Optional[ActiveRequestTracker] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Detect one LLM or tool node dominating total observed leaf-work time.

    Chain durations are normally inclusive of their descendants. Including them
    with LLM and tool durations would double-count nested time and make the result
    depend on wrapper depth, so both the denominator and candidates are restricted
    to leaf work observed by the probe.
    """
    threshold_pct = 0.40
    work_nodes = [
        node
        for node in graph.all_nodes()
        if _is_langgraph_llm_node(node)
        or _is_langgraph_tool_node(node)
    ]
    if len(work_nodes) < 2:
        return False, {}

    total_duration_ns = sum(
        (node.avg_duration_ns or 0) * node.call_count
        for node in work_nodes
    )
    if total_duration_ns <= 0:
        return False, {}

    hotspots = []
    for node in work_nodes:
        node_total_ns = (
            node.avg_duration_ns or 0
        ) * node.call_count
        share = node_total_ns / total_duration_ns
        if node_total_ns > 0 and share > threshold_pct:
            hotspots.append((node, share))

    if not hotspots:
        return False, {}
    hotspots.sort(key=lambda item: item[1], reverse=True)

    return True, {
        "latency_hotspots": [
            {
                "node": node.id,
                "kind": _langgraph_kind(node),
                "call_count": node.call_count,
                "avg_ms": round(
                    (node.avg_duration_ns or 0) / 1e6, 2
                ),
                "pct_of_total_leaf_time": round(share * 100, 1),
            }
            for node, share in hotspots
        ]
    }


AGENT_LATENCY_HOTSPOT = CausalRule(
    name="agent_latency_hotspot",
    description=(
        "A single LangGraph LLM or tool node accounts for more than 40% "
        "of aggregate leaf-work time. Check for an unbounded operation, "
        "slow external dependency, missing timeout, or repeated work."
    ),
    predicate=_agent_latency_hotspot,
    confidence=0.65,
    tags=["langgraph", "agent", "performance"],
)


def register(
    registry: type[PatternRegistry] = PatternRegistry,
) -> None:
    """Register this module's rules with a compatible pattern registry."""
    registry.register(LOOP_RUNAWAY)
    registry.register(AGENT_LATENCY_HOTSPOT)


register()

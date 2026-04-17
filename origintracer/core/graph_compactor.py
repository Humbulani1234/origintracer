from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Set

logger = logging.getLogger("origintracer.compactor")

if TYPE_CHECKING:
    from .runtime_graph import RuntimeGraph


class GraphCompactor:
    """
    Bounds RuntimeGraph memory via two mechanisms:

    Mechanism 1 - Node TTL
        Nodes not seen for longer than `node_ttl_s` seconds are evicted.
        This handles services that appear temporarily (a feature flag endpoint
        that was removed, a task type that is no longer dispatched).
        Default TTL: 3600 seconds.

    Mechanism 2 - Node cap with LRU eviction
        When the node count exceeds `max_nodes`, the least recently seen
        nodes are evicted to bring the count back to `max_nodes * evict_to_ratio`.
        Default: cap 5000 nodes, evict to 80% (4000 nodes) when exceeded.

    Edge handling on eviction:
        When a node is evicted, all edges incident to it are also removed.
        Dangling edges pointing to evicted nodes are invalid for causal
        reasoning and must not persist.

    When to compact:
        Compaction runs on a background thread (called by Engine's snapshot loop)
        or can be triggered manually.

    Usage:
        compactor = GraphCompactor(max_nodes=5000, node_ttl_s=3600)
        # Called by Engine._snapshot_loop() automatically:
        stats = compactor.compact(graph)
        # stats = {"evicted_nodes": 12, "evicted_edges": 34, "reason": "ttl+cap"}
    """

    def __init__(
        self,
        max_nodes: int = 5_000,
        evict_to_ratio: float = 0.80,  # evict to 80% of max when cap hit
        node_ttl_s: float = 3600.0,  # evict nodes not seen in 1 hour
        min_call_count: int = 1,  # never evict nodes called >= this often
    ) -> None:
        self.max_nodes = max_nodes
        self.evict_to = int(max_nodes * evict_to_ratio)
        self.node_ttl_s = node_ttl_s
        self.min_call_count = min_call_count

        self._total_evictions = 0
        self._compact_runs = 0

    def compact(self, graph: RuntimeGraph) -> Dict[str, Any]:
        """
        Run one compaction pass against the graph.
        Returns a stats dict describing what was evicted and why.
        """
        self._compact_runs += 1
        now = time.time()
        evicted_nodes: Set[str] = set()
        reasons: List[str] = []

        with graph._lock:
            node_count = len(graph._nodes)

            # Pass 1: TTL eviction
            # Evict nodes not seen recently, unless they are very hot.
            ttl_candidates = [
                node_id
                for node_id, node in graph._nodes.items()
                if (now - node.last_seen) > self.node_ttl_s
                and node.call_count < self.min_call_count
            ]
            if ttl_candidates:
                evicted_nodes.update(ttl_candidates)
                reasons.append(f"ttl({len(ttl_candidates)})")

            # Pass 2: Cap eviction (LRU by last_seen)
            # Only runs if we are still over the cap after TTL eviction.
            remaining_after_ttl = node_count - len(evicted_nodes)
            if remaining_after_ttl > self.max_nodes:
                over_by = remaining_after_ttl - self.evict_to
                # Remove the 'min_call_count' check here.
                # If we are over the CAP, the coldest nodes MUST go, regardless of call count.
                cold_nodes = sorted(
                    [
                        (node.last_seen, node_id)
                        for node_id, node in graph._nodes.items()
                        if node_id not in evicted_nodes
                    ],
                    key=lambda t: t[0],
                )

                cap_evictions = [
                    node_id
                    for _, node_id in cold_nodes[:over_by]
                ]
                evicted_nodes.update(cap_evictions)
                reasons.append(f"cap({len(cap_evictions)})")

            if not evicted_nodes:
                return {
                    "evicted_nodes": 0,
                    "evicted_edges": 0,
                    "node_count": node_count,
                    "reason": "none",
                    "compact_runs": self._compact_runs,
                }

            # Remove nodes and all incident edges
            evicted_edge_count = self._remove_nodes(
                graph, evicted_nodes
            )

        self._total_evictions += len(evicted_nodes)

        logger.info(
            "GraphCompactor: evicted %d nodes, %d edges. reason=%s. "
            "graph now has %d nodes.",
            len(evicted_nodes),
            evicted_edge_count,
            "+".join(reasons),
            len(graph._nodes),
        )

        return {
            "evicted_nodes": len(evicted_nodes),
            "evicted_edges": evicted_edge_count,
            "node_count_after": len(graph._nodes),
            "reason": "+".join(reasons),
            "compact_runs": self._compact_runs,
            "total_evictions": self._total_evictions,
        }

    def _remove_nodes(
        self, graph: Any, node_ids: Set[str]
    ) -> int:
        """
        Remove `node_ids` from the graph and all incident edges.
        Caller must hold graph._lock.
        Returns number of edges removed.
        """
        edges_removed = 0

        for node_id in node_ids:
            graph._nodes.pop(node_id, None)

        # Remove all edges where source or target was evicted
        dead_edge_keys = [
            key
            for key, edge in graph._edge_index.items()
            if edge.source in node_ids or edge.target in node_ids
        ]

        for key in dead_edge_keys:
            edge = graph._edge_index.pop(key, None)
            if edge is None:
                continue
            edges_removed += 1

            # Remove from adjacency lists
            adj_list = graph._adj.get(edge.source, [])
            try:
                adj_list.remove(edge)
            except ValueError:
                pass

            rev_list = graph._rev.get(edge.target, [])
            try:
                rev_list.remove(edge)
            except ValueError:
                pass

        # Clean up empty adjacency list entries for evicted nodes
        for node_id in node_ids:
            graph._adj.pop(node_id, None)
            graph._rev.pop(node_id, None)

        return edges_removed

    def estimate_memory_bytes(
        self, graph: Any
    ) -> Dict[str, int]:
        r"""
        Rough memory estimate for the current graph.
        Useful for \status in the REPL.

        These are order-of-magnitude estimates, not exact measurements.
        Use tracemalloc for precise profiling.
        """
        node_count = len(graph._nodes)
        edge_count = len(graph._edge_index)

        # Empirical estimates from profiling typical OriginTracer graphs:
        #   GraphNode dataclass: ~200 bytes base + metadata dict overhead
        #   GraphEdge dataclass: ~160 bytes base + metadata dict overhead
        #   Dict overhead per entry: ~50-80 bytes
        node_bytes = node_count * 280
        edge_bytes = edge_count * 220
        index_bytes = (
            node_count + edge_count
        ) * 70  # dict key overhead

        return {
            "nodes": node_count,
            "edges": edge_count,
            "node_bytes": node_bytes,
            "edge_bytes": edge_bytes,
            "index_bytes": index_bytes,
            "total_bytes": node_bytes + edge_bytes + index_bytes,
            "total_mb": round(
                (node_bytes + edge_bytes + index_bytes) / 1e6, 2
            ),
        }

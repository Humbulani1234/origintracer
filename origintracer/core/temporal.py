from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from threading import RLock
from typing import (
    Any,
    Deque,
    Dict,
    FrozenSet,
    List,
    Optional,
    Set,
)


@dataclass
class GraphDiff:
    """
    The delta between two consecutive graph snapshots.
    Stored, not the full graphs.
    """

    timestamp: float
    label: Optional[str]  # e.g. "deployment:abc123"

    added_node_ids: Set[str] = field(default_factory=set)
    removed_node_ids: Set[str] = field(default_factory=set)
    added_edge_keys: Set[str] = field(default_factory=set)
    removed_edge_keys: Set[str] = field(default_factory=set)
    edge_baseline: FrozenSet[str] = field(
        default_factory=frozenset
    )

    @property
    def is_empty(self) -> bool:
        return not (
            self.added_node_ids
            or self.removed_node_ids
            or self.added_edge_keys
            or self.removed_edge_keys
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "label": self.label,
            "added_nodes": list(self.added_node_ids),
            "removed_nodes": list(self.removed_node_ids),
            "added_edges": list(self.added_edge_keys),
            "removed_edges": list(self.removed_edge_keys),
        }


class TemporalStore:
    """
    Stores graph snapshots as incremental diffs, NOT full copies.

    Supports:
        - Time travel: "show me the graph at T"
        - Change detection: "what new edges appeared after this deployment?"
        - Deployment correlation: "what changed in the 60s after this commit?

    Usage
    -----
    Call `capture(graph, label=None)` periodically (e.g. every 10 s)
    or on significant events (deployments, config changes).
    """

    def __init__(self, max_diffs: int = 500) -> None:
        self._diffs: Deque[GraphDiff] = deque(maxlen=max_diffs)
        self._prev_node_ids: Set[str] = set()
        self._prev_edge_keys: Set[str] = set()
        self._lock = RLock()

    def capture(
        self,
        graph_snapshot: Dict[str, Any],
        label: Optional[str] = None,
    ) -> GraphDiff:
        """
        Compute the diff between the last snapshot and the current graph state,
        store it, and return it.

        graph_snapshot comes from RuntimeGraph.snapshot().
        """
        current_nodes: Set[str] = graph_snapshot.get(
            "node_ids", set()
        )
        current_edges: Set[str] = graph_snapshot.get(
            "edge_keys", set()
        )

        with self._lock:
            diff = GraphDiff(
                timestamp=graph_snapshot.get(
                    "timestamp", time.time()
                ),
                label=label,
                added_node_ids=current_nodes
                - self._prev_node_ids,
                removed_node_ids=self._prev_node_ids
                - current_nodes,
                added_edge_keys=current_edges
                - self._prev_edge_keys,
                removed_edge_keys=self._prev_edge_keys
                - current_edges,
            )
            self._diffs.append(diff)
            self._prev_node_ids = current_nodes
            self._prev_edge_keys = current_edges

        return diff

    def mark_event(self, label: str) -> None:
        """
        Insert a labelled marker at the current time with no structural change.
        Useful for deployment boundaries, config changes, etc.
        """
        with self._lock:
            self._diffs.append(
                GraphDiff(
                    timestamp=time.time(),
                    label=label,
                    edge_baseline=frozenset(
                        self._prev_edge_keys
                    ),
                )
            )

    def latest_diff(self) -> Optional[Dict]:
        if not self._diffs:
            return None
        last = self._diffs[-1]
        return {
            "added_nodes": list(last.added_nodes),
            "removed_nodes": list(last.removed_nodes),
            "added_edges": list(last.added_edges),
            "removed_edges": list(last.removed_edges),
            "timestamp": last.timestamp,
            "label": last.label,
        }

    def changes_since(self, since: float) -> List[GraphDiff]:
        with self._lock:
            return [
                d for d in self._diffs if d.timestamp >= since
            ]

    def new_edges_since(self, since: float) -> Set[str]:
        """Which edges appeared after a given timestamp? (Deployment analysis)"""
        result: Set[str] = set()
        for diff in self.changes_since(since):
            result |= diff.added_edge_keys
        return result

    def removed_edges_since(self, since: float) -> Set[str]:
        result: Set[str] = set()
        for diff in self.changes_since(since):
            result |= diff.removed_edge_keys
        return result

    def changes_around(
        self, t: float, window_seconds: float = 60.0
    ) -> List[GraphDiff]:
        lo = t - window_seconds
        hi = t + window_seconds
        with self._lock:
            return [
                d for d in self._diffs if lo <= d.timestamp <= hi
            ]

    def label_diff(self, label: str) -> Optional[GraphDiff]:
        """Find the diff (or marker) with this label."""
        with self._lock:
            for d in reversed(self._diffs):
                if d.label == label:
                    return d
        return None

    def diff_summary(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [d.to_dict() for d in self._diffs]

    def __len__(self) -> int:
        return len(self._diffs)

    def __repr__(self) -> str:
        return f"<TemporalStore diffs={len(self._diffs)}>"

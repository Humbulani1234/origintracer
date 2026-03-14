"""
core/runtime_graph.py

An in-memory directed graph that grows as NormalizedEvents arrive.
Supports neighbor traversal, upstream blame, and reachability BFS —
the minimum needed for causal reasoning.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Dict, Iterator, List, Optional, Set

from .event_schema import NormalizedEvent


@dataclass
class GraphNode:
    id: str  # e.g.  "django::handle_export"
    node_type: str  # ProbeType family: "function", "service", "syscall" …
    service: str
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    call_count: int = 0
    total_duration_ns: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def avg_duration_ns(self) -> Optional[float]:
        if self.call_count == 0:
            return None
        return self.total_duration_ns / self.call_count

    def touch(self, duration_ns: Optional[int] = None) -> None:
        self.last_seen = time.time()
        self.call_count += 1
        if duration_ns:
            self.total_duration_ns += duration_ns


@dataclass
class GraphEdge:
    source: str
    target: str
    edge_type: str  # "calls", "awaits", "owns", "sends_to" …
    call_count: int = 0
    total_duration_ns: int = 0
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def touch(self, duration_ns: Optional[int] = None) -> None:
        self.call_count += 1
        self.last_seen = time.time()
        if duration_ns:
            self.total_duration_ns += duration_ns


class RuntimeGraph:
    """
    Thread-safe directed graph.

    Node ID convention:  "<service>::<name>"
    Edge key convention: "<source>→<target>:<type>"

    Key methods
    -----------
    add_from_event()    — primary ingest path
    neighbors()         — downstream edges from a node
    callers()           — upstream (reverse) edges into a node
    reachable_from()    — BFS downstream reachability
    hottest_nodes()     — sorted by call count or duration
    snapshot()          — serialisable dict for storage / diff
    """

    def __init__(self) -> None:
        self._nodes: Dict[str, GraphNode] = {}
        self._adj: Dict[str, List[GraphEdge]] = defaultdict(
            list
        )  # forward
        self._rev: Dict[str, List[GraphEdge]] = defaultdict(
            list
        )  # reverse
        self._edge_index: Dict[str, GraphEdge] = (
            {}
        )  # dedup key → edge
        self._lock = RLock()
        self.last_updated: float = time.time()

    # ------------------------------------------------------------------ #
    # Mutation
    # ------------------------------------------------------------------ #

    def _node_id(self, service: str, name: str) -> str:
        return f"{service}::{name}"

    def upsert_node(
        self,
        node_id: str,
        node_type: str,
        service: str,
        duration_ns: Optional[int] = None,
        metadata: Optional[Dict] = None,
    ) -> GraphNode:
        with self._lock:
            if node_id not in self._nodes:
                self._nodes[node_id] = GraphNode(
                    id=node_id,
                    node_type=node_type,
                    service=service,
                    metadata=metadata or {},
                )
            node = self._nodes[node_id]
            node.touch(duration_ns)
            if metadata:
                node.metadata.update(metadata)
            self.last_updated = time.time()
            return node

    def upsert_edge(
        self,
        source: str,
        target: str,
        edge_type: str,
        duration_ns: Optional[int] = None,
        metadata: Optional[Dict] = None,
    ) -> GraphEdge:
        key = f"{source}→{target}:{edge_type}"
        with self._lock:
            if key not in self._edge_index:
                edge = GraphEdge(
                    source=source,
                    target=target,
                    edge_type=edge_type,
                    metadata=metadata or {},
                )
                self._edge_index[key] = edge
                self._adj[source].append(edge)
                self._rev[target].append(edge)
            edge = self._edge_index[key]
            edge.touch(duration_ns)
            if metadata:
                edge.metadata.update(metadata)
            self.last_updated = time.time()
            return edge

    def add_from_event(
        self,
        event: NormalizedEvent,
        parent_event: Optional[NormalizedEvent] = None,
    ) -> None:
        """
        Translate one NormalizedEvent into graph mutations.
        If parent_event is provided, an edge is drawn from parent → this event.
        """
        # Normalise name before it enters the graph

        # import pdb
        # pdb.set_trace()

        print(f">>> graph after add: {list(self._nodes.keys())}")
        name = event.name
        if (
            hasattr(self, "normalizer")
            and self.normalizer is not None
        ):
            name = self.normalizer.normalize(event.service, name)

        node_id = self._node_id(event.service, name)
        node_type = (
            event.service
        )  # e.g. "asyncio", "django", "syscall"
        is_entry = event.probe.endswith(
            (".enter", ".receive", ".entry", ".start")
        )
        self.upsert_node(
            node_id=node_id,
            node_type=node_type,
            service=event.service,
            duration_ns=None if is_entry else event.duration_ns,
            metadata={"probe": event.probe, **event.metadata},
        )

        if parent_event:
            parent_name = parent_event.name
            if (
                hasattr(self, "normalizer")
                and self.normalizer is not None
            ):
                parent_name = self.normalizer.normalize(
                    parent_event.service, parent_name
                )
            parent_id = self._node_id(
                parent_event.service, parent_name
            )
            if parent_id != node_id:
                self.upsert_edge(
                    source=parent_id,
                    target=node_id,
                    edge_type="calls",
                    duration_ns=event.duration_ns,
                )

        # Probe-specific structural edges — topology, not request flow
        self._add_structural_edges(node_id, event)

    def _add_structural_edges(
        self, node_id: str, event: NormalizedEvent
    ) -> None:
        """
        Draw infrastructure topology edges based on probe type.
        Called once per event after the node is upserted.
        Each probe type gets its own block — add new probes here, not above.
        """
        probe = event.probe

        if probe == "gunicorn.worker.fork":
            # master ──spawned──► worker
            master_node_id = self._node_id("gunicorn", "master")
            if master_node_id in self._nodes:
                self.upsert_edge(
                    source=master_node_id,
                    target=node_id,
                    edge_type="spawned",
                )

        elif probe == "uvicorn.request.receive":
            # worker ──handled──► request
            worker_pid = event.metadata.get("worker_pid")
            if worker_pid:
                for nid, node in self._nodes.items():
                    if (
                        node.node_type == "gunicorn"
                        and node.metadata.get("worker_pid")
                        == worker_pid
                    ):
                        self.upsert_edge(
                            source=nid,
                            target=node_id,
                            edge_type="handled",
                        )
                        break

        elif probe == "celery.worker.fork":
            # MainProcess ──spawned──► ForkPoolWorker
            # The main process emits celery.worker.fork with master_pid.
            # We look for a node whose pid matches that master_pid.
            master_pid = event.metadata.get("master_pid")
            if master_pid:
                for nid, node in self._nodes.items():
                    if (
                        node.node_type == "celery"
                        and node.metadata.get("worker_pid")
                        == master_pid
                    ):
                        self.upsert_edge(
                            source=nid,
                            target=node_id,
                            edge_type="spawned",
                        )
                        break

        elif probe == "celery.task.start":
            worker_pid = event.metadata.get("worker_pid")
            if worker_pid:
                for nid, node in self._nodes.items():
                    node_name = (
                        nid.split("::", 1)[1]
                        if "::" in nid
                        else nid
                    )
                    if (
                        node.node_type == "celery"
                        and node_name == "ForkPoolWorker"
                        and node.metadata.get("worker_pid")
                        == worker_pid
                    ):
                        self.upsert_edge(
                            source=nid,
                            target=node_id,
                            edge_type="ran",
                        )
                        break

        # ── nginx ─────────────────────────────────────────────────────
        elif probe == "nginx.worker.discovered":
            master_pid = event.metadata.get("master_pid")
            if master_pid:
                for nid, node in self._nodes.items():
                    node_name = (
                        nid.split("::", 1)[1]
                        if "::" in nid
                        else nid
                    )
                    if (
                        node.node_type == "nginx"
                        and node_name == "master"
                        and node.metadata.get("worker_pid")
                        == master_pid
                    ):
                        self.upsert_edge(
                            source=nid,
                            target=node_id,
                            edge_type="spawned",
                        )
                        break

        elif probe in (
            "nginx.request.complete",
            "nginx.request.enriched",
        ):
            worker_pid = event.metadata.get("worker_pid")
            if worker_pid:
                for nid, node in self._nodes.items():
                    node_name = (
                        nid.split("::", 1)[1]
                        if "::" in nid
                        else nid
                    )
                    if (
                        node.node_type == "nginx"
                        and "worker-" in node_name
                        and node.metadata.get("worker_pid")
                        == worker_pid
                    ):
                        self.upsert_edge(
                            source=nid,
                            target=node_id,
                            edge_type="handled",
                        )
                        break

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def neighbors(self, node_id: str) -> List[GraphEdge]:
        """Downstream: what does this node call?"""
        with self._lock:
            return list(self._adj.get(node_id, []))

    def callers(self, node_id: str) -> List[GraphEdge]:
        """Upstream: what calls this node? (reverse edges)"""
        with self._lock:
            return list(self._rev.get(node_id, []))

    def reachable_from(
        self, node_id: str, max_depth: int = 20
    ) -> Set[str]:
        """BFS downstream — all nodes reachable from node_id."""
        visited: Set[str] = set()
        queue: deque = deque([(node_id, 0)])
        with self._lock:
            while queue:
                current, depth = queue.popleft()
                if current in visited or depth > max_depth:
                    continue
                visited.add(current)
                for edge in self._adj.get(current, []):
                    queue.append((edge.target, depth + 1))
        return visited - {node_id}

    def reachable_to(
        self, node_id: str, max_depth: int = 20
    ) -> Set[str]:
        """BFS upstream — all nodes that can reach this node."""
        visited: Set[str] = set()
        queue: deque = deque([(node_id, 0)])
        with self._lock:
            while queue:
                current, depth = queue.popleft()
                if current in visited or depth > max_depth:
                    continue
                visited.add(current)
                for edge in self._rev.get(current, []):
                    queue.append((edge.source, depth + 1))
        return visited - {node_id}

    def nodes_by_service(self, service: str) -> List[GraphNode]:
        with self._lock:
            return [
                n
                for n in self._nodes.values()
                if n.service == service
            ]

    def hottest_nodes(
        self, top_n: int = 10, by: str = "call_count"
    ) -> List[GraphNode]:
        """Return the N most-called (or slowest) nodes."""
        with self._lock:
            nodes = list(self._nodes.values())
        if by == "duration":
            nodes.sort(
                key=lambda n: n.total_duration_ns, reverse=True
            )
        else:
            nodes.sort(key=lambda n: n.call_count, reverse=True)
        return nodes[:top_n]

    def get_node(self, node_id: str) -> Optional[GraphNode]:
        return self._nodes.get(node_id)

    def all_nodes(self) -> Iterator[GraphNode]:
        with self._lock:
            yield from self._nodes.values()

    def all_edges(self) -> Iterator[GraphEdge]:
        with self._lock:
            yield from self._edge_index.values()

    # ------------------------------------------------------------------ #
    # Snapshot (for temporal diff and storage)
    # ------------------------------------------------------------------ #

    def snapshot(self) -> Dict[str, Any]:
        """Return a serialisable point-in-time copy."""
        with self._lock:
            return {
                "timestamp": self.last_updated,
                "node_ids": set(self._nodes.keys()),
                "edge_keys": set(self._edge_index.keys()),
                "nodes": {
                    nid: {
                        "type": n.node_type,
                        "service": n.service,
                        "call_count": n.call_count,
                        "avg_duration_ns": n.avg_duration_ns,
                    }
                    for nid, n in self._nodes.items()
                },
            }

    def __len__(self) -> int:
        return len(self._nodes)

    def __repr__(self) -> str:
        return f"<RuntimeGraph nodes={len(self._nodes)} edges={len(self._edge_index)}>"

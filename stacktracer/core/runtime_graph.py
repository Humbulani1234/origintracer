from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import RLock
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
)

if TYPE_CHECKING:
    from .event_schema import NormalizedEvent

logger = logging.getLogger("core.runtime_graph")


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
    An in-memory directed graph that grows as NormalizedEvents arrive.
    Supports neighbor traversal, upstream blame, and reachability BFS —
    the minimum needed for causal reasoning.

    Node ID convention:  "<service>::<name>"
    Edge key convention: "<source>→<target>:<type>"
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
        )  # dedup key:edge
        self._lock = RLock()
        self.last_updated: float = time.time()

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
        name = event.name
        node_id = self._node_id(event.service, name)
        node_type = (
            event.service
        )  # e.g. "asyncio", "django", "syscall"
        duration = event.duration_ns
        # 2. Fallback (with a quiet "fix this" note for yourself)
        if duration is None and event.metadata:
            duration = event.metadata.get("duration_ns")
            if duration is not None:
                logger.debug(
                    "Probe %s sent duration in metadata; move to top-level.",
                    event.probe,
                )
        self.upsert_node(
            node_id=node_id,
            node_type=node_type,
            service=event.service,
            duration_ns=duration,
            metadata={"probe": event.probe, **event.metadata},
        )

        if parent_event:
            parent_name = parent_event.name
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
        probe = event.probe
        meta = event.metadata

        if probe == "gunicorn.worker.fork":
            master_id = self._node_id("gunicorn", "master")
            if master_id in self._nodes:
                self.upsert_edge(master_id, node_id, "spawned")

        elif probe == "uvicorn.request.receive":
            src = self._find_node(
                "gunicorn", "worker_pid", meta.get("worker_pid")
            )
            if src:
                self.upsert_edge(src, node_id, "handled")

        elif probe == "celery.worker.fork":
            src = self._find_node(
                "celery", "worker_pid", meta.get("master_pid")
            )
            if src:
                self.upsert_edge(src, node_id, "spawned")

        elif probe == "celery.task.start":
            src = self._find_node(
                "celery",
                "worker_pid",
                meta.get("worker_pid"),
                name_equals="ForkPoolWorker",
            )
            if src:
                self.upsert_edge(src, node_id, "ran")

        elif probe == "nginx.worker.discovered":
            src = self._find_node(
                "nginx",
                "worker_pid",
                meta.get("master_pid"),
                name_equals="master",
            )
            if src:
                self.upsert_edge(src, node_id, "spawned")

        elif probe in (
            "nginx.request.complete",
            "nginx.request.enriched",
        ):
            src = self._find_node(
                "nginx",
                "worker_pid",
                meta.get("worker_pid"),
                name_contains="worker-",
            )
            if src:
                self.upsert_edge(src, node_id, "handled")

    def _find_node(
        self,
        node_type: str,
        metadata_key: str,
        metadata_value: Any,
        name_contains: Optional[str] = None,
        name_equals: Optional[str] = None,
    ) -> Optional[str]:
        """
        Find the first node id matching type and a metadata field value.
        Optionally filter by node name containing or equalling a string.
        Returns the node id or None.
        """
        for nid, node in self._nodes.items():
            if node.node_type != node_type:
                continue
            if node.metadata.get(metadata_key) != metadata_value:
                continue
            if name_contains or name_equals:
                node_name = (
                    nid.split("::", 1)[1] if "::" in nid else nid
                )
                if (
                    name_contains
                    and name_contains not in node_name
                ):
                    continue
                if name_equals and node_name != name_equals:
                    continue
            return nid
        return None

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

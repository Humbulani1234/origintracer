from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from origintracer.core.runtime_graph import (
    GraphEdge,
    GraphNode,
    RuntimeGraph,
)

logger = logging.getLogger("origintracer.serializer")


def graph_to_dict(graph: Any) -> Dict:
    """
    Convert a RuntimeGraph to a plain Python dict.
    Used as the intermediate form before encoding to protobuf or msgpack.

    Metadata values are cast to strings because both serialization formats
    have constraints on value types. String metadata is always safe.
    The original types (int, float, bool) are preserved in GraphNode in memory -
    only the serialized form is stringified.
    """
    with graph._lock:
        nodes = [
            {
                "id": n.id,
                "node_type": n.node_type,
                "service": n.service,
                "first_seen": n.first_seen,
                "last_seen": n.last_seen,
                "call_count": n.call_count,
                "total_duration_ns": n.total_duration_ns,
                "metadata": {
                    k: str(v) for k, v in n.metadata.items()
                },
            }
            for n in graph._nodes.values()
        ]

        edges = [
            {
                "source": e.source,
                "target": e.target,
                "edge_type": e.edge_type,
                "call_count": e.call_count,
                "total_duration_ns": e.total_duration_ns,
                "first_seen": e.first_seen,
                "last_seen": e.last_seen,
                "metadata": {
                    k: str(v) for k, v in e.metadata.items()
                },
            }
            for e in graph._edge_index.values()
        ]

        return {
            "schema_version": "1.0",
            "serialized_at": time.time(),
            "graph_last_updated": graph.last_updated,
            "nodes": nodes,
            "edges": edges,
        }


def dict_to_graph(data: Dict) -> Any:
    """
    Reconstruct a RuntimeGraph from the plain dict form.
    Returns a fully functional RuntimeGraph.
    """
    graph = RuntimeGraph()

    with graph._lock:
        for n in data.get("nodes", []):
            node = GraphNode(
                id=n["id"],
                node_type=n["node_type"],
                service=n["service"],
                first_seen=n["first_seen"],
                last_seen=n["last_seen"],
                call_count=n["call_count"],
                total_duration_ns=n["total_duration_ns"],
                metadata=n.get("metadata", {}),
            )
            graph._nodes[node.id] = node

        for e in data.get("edges", []):
            edge = GraphEdge(
                source=e["source"],
                target=e["target"],
                edge_type=e["edge_type"],
                call_count=e["call_count"],
                total_duration_ns=e["total_duration_ns"],
                first_seen=e["first_seen"],
                last_seen=e["last_seen"],
                metadata=e.get("metadata", {}),
            )
            key = f"{edge.source}→{edge.target}:{edge.edge_type}"
            graph._edge_index[key] = edge
            graph._adj[edge.source].append(edge)
            graph._rev[edge.target].append(edge)

        graph.last_updated = data.get(
            "graph_last_updated", time.time()
        )

    logger.info(
        "Graph deserialized: %d nodes, %d edges (schema_version=%s)",
        len(graph._nodes),
        len(graph._edge_index),
        data.get("schema_version", "unknown"),
    )
    return graph


class GraphSerializer(ABC):
    """
    Serializes and deserializes ``RuntimeGraph`` using protobuf or MessagePack.

    Two backends are provided.

    MsgpackSerializer
    -----------------
    Uses MessagePack. Requires::

        pip install msgpack

    ProtobufSerializer *(experimental)*
    ------------------------------------
    Uses the compiled ``origintracer_pb2`` module generated from
    ``origintracer.proto``. Requires::

        pip install protobuf grpcio-tools

    Compile the proto file first::

        python -m grpc_tools.protoc          \\
            -I origintracer/core             \\
            --python_out=origintracer/core   \\
            origintracer/core/origintracer.proto

    Usage
    -----
    MessagePack::

        from origintracer.core.graph_serializer import MsgpackSerializer

        serializer = MsgpackSerializer()

        # Save graph to file
        with open("graph.msgpack", "wb") as f:
            f.write(serializer.serialize(graph))

        # Load graph from file
        with open("graph.msgpack", "rb") as f:
            restored_graph = serializer.deserialize(f.read())

    Protobuf::

        from origintracer.core.graph_serializer import ProtobufSerializer

        serializer = ProtobufSerializer()
        data  = serializer.serialize(graph) # bytes
        graph2 = serializer.deserialize(data) # RuntimeGraph

    Note
    ----
    Loaded graphs are fully functional ``RuntimeGraph`` instances — all methods
    (``neighbors``, ``callers``, causal rules, DSL queries) work identically on
    a deserialized graph as on a live one.
    """

    @abstractmethod
    def serialize(self, graph: Any) -> bytes:
        """
        Serialize a RuntimeGraph to bytes.
        """

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """
        Deserialize bytes to a RuntimeGraph.
        """

    def save(self, graph: Any, path: str) -> int:
        """
        Serialize and write to file. Returns bytes written.
        """
        payload = self.serialize(graph)
        with open(path, "wb") as f:
            f.write(payload)
        logger.info(
            "Graph saved to %s (%d bytes)", path, len(payload)
        )
        return len(payload)

    def load(self, path: str) -> Any:
        """
        Read from file and deserialize.
        """
        with open(path, "rb") as f:
            data = f.read()
        graph = self.deserialize(data)
        logger.info("Graph loaded from %s", path)
        return graph


class MsgpackSerializer(GraphSerializer):
    """
    Serialize RuntimeGraph using MessagePack.
    """

    def serialize(self, graph: Any) -> bytes:
        try:
            import msgpack
        except ImportError:
            raise ImportError(
                "msgpack not installed. pip install msgpack"
            )

        payload = graph_to_dict(graph)
        return msgpack.packb(payload, use_bin_type=True)

    def deserialize(self, data: bytes) -> Any:
        try:
            import msgpack
        except ImportError:
            raise ImportError(
                "msgpack not installed. pip install msgpack"
            )

        payload = msgpack.unpackb(data, raw=False)
        return dict_to_graph(payload)


class ProtobufSerializer(GraphSerializer):
    """
    Experimental.
    Serialize RuntimeGraph using Protocol Buffers.

    Requires:
        pip install protobuf grpcio-tools
        python -m grpc_tools.protoc \\
            -I origintracer/core \\
            --python_out=origintracer/core \\
            origintracer/core/origintracer.proto
    """

    def serialize(self, graph: Any) -> bytes:
        pb2 = self._import_pb2()
        payload = graph_to_dict(graph)

        snapshot = pb2.SerializedGraph()
        snapshot.serialized_at = payload["serialized_at"]
        snapshot.graph_last_updated = payload[
            "graph_last_updated"
        ]
        snapshot.schema_version = payload["schema_version"]

        for n in payload["nodes"]:
            pb_node = snapshot.nodes.add()
            pb_node.id = n["id"]
            pb_node.node_type = n["node_type"]
            pb_node.service = n["service"]
            pb_node.first_seen = n["first_seen"]
            pb_node.last_seen = n["last_seen"]
            pb_node.call_count = n["call_count"]
            pb_node.total_duration_ns = n["total_duration_ns"]
            pb_node.metadata.update(n.get("metadata", {}))

        for e in payload["edges"]:
            pb_edge = snapshot.edges.add()
            pb_edge.source = e["source"]
            pb_edge.target = e["target"]
            pb_edge.edge_type = e["edge_type"]
            pb_edge.call_count = e["call_count"]
            pb_edge.total_duration_ns = e["total_duration_ns"]
            pb_edge.first_seen = e["first_seen"]
            pb_edge.last_seen = e["last_seen"]
            pb_edge.metadata.update(e.get("metadata", {}))

        return snapshot.SerializeToString()

    def deserialize(self, data: bytes) -> Any:
        pb2 = self._import_pb2()

        snapshot = pb2.SerializedGraph()
        snapshot.ParseFromString(data)

        payload = {
            "schema_version": snapshot.schema_version,
            "serialized_at": snapshot.serialized_at,
            "graph_last_updated": snapshot.graph_last_updated,
            "nodes": [
                {
                    "id": n.id,
                    "node_type": n.node_type,
                    "service": n.service,
                    "first_seen": n.first_seen,
                    "last_seen": n.last_seen,
                    "call_count": n.call_count,
                    "total_duration_ns": n.total_duration_ns,
                    "metadata": dict(n.metadata),
                }
                for n in snapshot.nodes
            ],
            "edges": [
                {
                    "source": e.source,
                    "target": e.target,
                    "edge_type": e.edge_type,
                    "call_count": e.call_count,
                    "total_duration_ns": e.total_duration_ns,
                    "first_seen": e.first_seen,
                    "last_seen": e.last_seen,
                    "metadata": dict(e.metadata),
                }
                for e in snapshot.edges
            ],
        }

        return dict_to_graph(payload)

    @staticmethod
    def _import_pb2():
        try:
            from origintracer.core import origintracer_pb2

            return origintracer_pb2
        except ImportError:
            raise ImportError(
                "origintracer_pb2 not found. Compile the protobuf schema first:\n"
                "  pip install grpcio-tools\n"
                "  python -m grpc_tools.protoc \\\n"
                "      -I origintracer/core \\\n"
                "      --python_out=origintracer/core \\\n"
                "      origintracer/core/origintracer.proto"
            )


class JSONSerializer(GraphSerializer):
    """
    Serialize to JSON. Use for debugging and development only.
    """

    def __init__(self, indent: Optional[int] = None) -> None:
        self._indent = indent

    def serialize(self, graph: Any) -> bytes:
        payload = graph_to_dict(graph)
        return json.dumps(payload, indent=self._indent).encode(
            "utf-8"
        )

    def deserialize(self, data: bytes) -> Any:
        payload = json.loads(data.decode("utf-8"))
        return dict_to_graph(payload)

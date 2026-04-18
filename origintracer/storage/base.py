from __future__ import annotations

import base64
import json
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional

from origintracer.core.event_schema import NormalizedEvent
from origintracer.core.temporal import GraphDiff

logger = logging.getLogger("origintracer.storage")


class BaseRepository(ABC):

    @abstractmethod
    def insert_event(self, event: NormalizedEvent) -> None:
        """
        Store one probe event.
        """
        ...

    @abstractmethod
    def query_events(
        self,
        *,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve events matching filters, newest first.
        """
        ...

    @abstractmethod
    def insert_snapshot(
        self,
        customer_id: str,
        data: bytes,
        content_type: str = "application/msgpack",
        node_count: int = 0,
        edge_count: int = 0,
    ) -> None:
        """
        Persist a serialised RuntimeGraph snapshot.
        Called by FastAPI on every POST /api/v1/graph/snapshot.
        Only the most recent snapshot per customer needs to be queryable -
        older ones can be overwritten or retained for audit.
        """
        ...

    @abstractmethod
    def get_latest_snapshot(
        self,
        customer_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Return the latest snapshot for this customer, or None.
        Return dict keys: data (bytes), content_type (str), received_at (float).
        Called by FastAPI on startup to restore graph without waiting for agent.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """
        Release any open connections.
        """
        ...

    @abstractmethod
    def insert_deployment_marker(
        self, customer_id: str, label: str
    ) -> None:
        """
        Store a deployment marker with the current timestamp.
        """
        ...

    @abstractmethod
    def insert_graph_diff(
        self, customer_id: str, diff: Dict
    ) -> None:
        """
        Store one graph diff snapshot from the agent.
        """
        ...

    @abstractmethod
    def save_causal_matches(
        self,
        customer_id: str,
        matches: List[Dict],
        timestamp: float,
    ) -> None:
        """
        Persist causal rule matches for historical review.
        """
        ...

    @abstractmethod
    def get_causal_history(
        self,
        customer_id: str,
        limit: int = 50,
    ) -> List[Dict]:
        """
        Return most recent causal match snapshots.
        """
        ...


_PG_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS st_events (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL DEFAULT 'default',
    trace_id TEXT NOT NULL,
    span_id TEXT,
    parent_span_id TEXT,
    probe TEXT NOT NULL,
    service TEXT NOT NULL,
    name TEXT NOT NULL,
    wall_time DOUBLE PRECISION NOT NULL,
    duration_ns BIGINT,
    pid INT,
    tid INT,
    metadata JSONB
);
CREATE INDEX IF NOT EXISTS idx_st_events_trace ON st_events (trace_id);
CREATE INDEX IF NOT EXISTS idx_st_events_customer ON st_events (customer_id, wall_time DESC);
CREATE INDEX IF NOT EXISTS idx_st_events_service ON st_events (service);
CREATE INDEX IF NOT EXISTS idx_st_events_probe ON st_events (probe);
"""

_PG_CREATE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS st_snapshots (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    received_at DOUBLE PRECISION NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/msgpack',
    node_count INT,
    edge_count INT,
    data BYTEA NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_st_snapshots_customer
    ON st_snapshots (customer_id, received_at DESC);
"""

_PG_CREATE_MARKERS = """
CREATE TABLE IF NOT EXISTS st_markers (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    label TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_st_markers_customer
    ON st_markers (customer_id, created_at DESC);
"""


_PG_CREATE_GRAPH_DIFFS = """
CREATE TABLE graph_diffs (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
    added_nodes JSONB,
    removed_nodes JSONB,
    added_edges JSONB,
    removed_edges JSONB,
    label VARCHAR(200) -- populated when a deployment marker exists
);
"""

_PG_CREATE_CAUSAL_RULES = """
CREATE TABLE causal_matches (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    matches JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_causal_customer ON causal_matches(customer_id, timestamp DESC);
"""


class PGEventRepository(BaseRepository):
    """
    PostgreSQL-backed store.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(_PG_CREATE_EVENTS)
            cur.execute(_PG_CREATE_SNAPSHOTS)
            cur.execute(_PG_CREATE_MARKERS)
            cur.execute(_PG_CREATE_GRAPH_DIFFS)
        self._conn.commit()

    def insert_event(self, event: NormalizedEvent) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO st_events
                        (customer_id, trace_id, span_id, parent_span_id,
                         probe, service, name, wall_time,
                         duration_ns, pid, tid, metadata)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        event.metadata.get(
                            "customer_id", "default"
                        ),
                        event.trace_id,
                        event.span_id,
                        event.parent_span_id,
                        event.probe,
                        event.service,
                        event.name,
                        event.wall_time,
                        event.duration_ns,
                        event.pid,
                        event.tid,
                        json.dumps(event.metadata),
                    ),
                )
            self._conn.commit()
        except Exception as exc:
            logger.warning("PG insert_event failed: %s", exc)
            self._safe_rollback()

    def query_events(
        self,
        *,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        conditions: List[str] = []
        params: List[Any] = []

        if trace_id:
            conditions.append("trace_id = %s")
            params.append(trace_id)
        if probe:
            conditions.append("probe = %s")
            params.append(probe)
        if service:
            conditions.append("service = %s")
            params.append(service)
        if since:
            conditions.append("wall_time >= %s")
            params.append(since)

        where = (
            ("WHERE " + " AND ".join(conditions))
            if conditions
            else ""
        )
        params.append(limit)

        sql = f"""
            SELECT trace_id, span_id, parent_span_id, probe, service, name,
                   wall_time, duration_ns, pid, tid, metadata
            FROM   st_events
            {where}
            ORDER  BY wall_time DESC
            LIMIT  %s
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            cols = [
                "trace_id",
                "span_id",
                "parent_span_id",
                "probe",
                "service",
                "name",
                "wall_time",
                "duration_ns",
                "pid",
                "tid",
                "metadata",
            ]
            return [dict(zip(cols, r)) for r in rows]
        except Exception as exc:
            logger.error("PG query_events failed: %s", exc)
            return []

    def insert_snapshot(
        self,
        customer_id: str,
        data: bytes,
        content_type: str = "application/msgpack",
        node_count: int = 0,
        edge_count: int = 0,
    ) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO st_snapshots
                        (customer_id, received_at, content_type,
                         node_count, edge_count, data)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        customer_id,
                        time.time(),
                        content_type,
                        node_count,
                        edge_count,
                        data,
                    ),
                )
            self._conn.commit()
        except Exception as exc:
            logger.warning("PG insert_snapshot failed: %s", exc)
            self._safe_rollback()

    def get_latest_snapshot(
        self,
        customer_id: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data, content_type, received_at
                    FROM   st_snapshots
                    WHERE  customer_id = %s
                    ORDER  BY received_at DESC
                    LIMIT  1
                    """,
                    (customer_id,),
                )
                row = cur.fetchone()
            if row is None:
                return None
            return {
                "data": bytes(
                    row[0]
                ),  # psycopg2 returns memoryview
                "content_type": row[1],
                "received_at": row[2],
            }
        except Exception as exc:
            logger.error(
                "PG get_latest_snapshot failed: %s", exc
            )
            return None

    def insert_graph_diff(
        self, customer_id: str, diff: Dict
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO graph_diffs
                (customer_id, added_nodes, removed_nodes, added_edges, removed_edges, label)
                VALUES (%s, %s, %s, %s, %s, %s)""",
                (
                    customer_id,
                    json.dumps(
                        list(diff.get("added_nodes", []))
                    ),
                    json.dumps(
                        list(diff.get("removed_nodes", []))
                    ),
                    json.dumps(
                        list(diff.get("added_edges", []))
                    ),
                    json.dumps(
                        list(diff.get("removed_edges", []))
                    ),
                    diff.get("label"),
                ),
            )
        self._conn.commit()

    def insert_marker(
        self, customer_id: str, label: str
    ) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO st_markers (customer_id, label, created_at)
                    VALUES (%s, %s, %s)
                    """,
                    (customer_id, label, time.time()),
                )
            self._conn.commit()
        except Exception as exc:
            logger.warning("PG insert_marker failed: %s", exc)
            self._safe_rollback()

    def save_causal_matches(
        self, customer_id, matches, timestamp
    ):
        self._conn.execute(
            """
            INSERT INTO causal_matches (customer_id, timestamp, label, matches)
            VALUES (%s, %s, %s, %s)
        """,
            (customer_id, timestamp, label, json.dumps(matches)),
        )

    def get_causal_history(self, customer_id, limit=50):
        rows = self._conn.execute(
            """
            SELECT timestamp, label, matches FROM causal_matches
            WHERE customer_id = %s
            ORDER BY timestamp DESC LIMIT %s
        """,
            (customer_id, limit),
        )
        return [dict(r) for r in rows]

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def _safe_rollback(self) -> None:
        try:
            self._conn.rollback()
        except Exception:
            pass


class InMemoryRepository(BaseRepository):
    """
    Refactored In-Memory store using collections for performance.
    - Uses deque(maxlen=X) to automatically rotate old data out.
    """

    def __init__(
        self, max_events: int = 100_000, max_diffs: int = 500
    ) -> None:
        # Events, global rolling buffer
        self._events: deque = deque(maxlen=max_events)
        # Snapshots, usually one-per-customer
        self._snapshots: Dict[str, Dict] = {}
        # Markers, using defaultdict(list)
        self._markers: Dict[str, List[Dict]] = defaultdict(list)
        # Diffs, using defaultdict with a lambda to create deques automatically
        # This keeps exactly the last 500 diffs per customer with zero
        # manual work.
        self._diffs: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_diffs)
        )
        # For rules that matched - anti-patterns
        self._causal_history: List[Dict] = []

    def insert_event(self, event: NormalizedEvent) -> None:
        # deque handles the maxlen=100_000 internally
        self._events.append(event.to_dict())

    def query_events(
        self,
        *,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        results = []
        for e in reversed(self._events):
            if trace_id and e.get("trace_id") != trace_id:
                continue
            if probe and e.get("probe") != probe:
                continue
            if service and e.get("service") != service:
                continue
            if since and e.get("wall_time", 0) < since:
                continue
            results.append(e)
            if len(results) >= limit:
                break
        return results

    def insert_snapshot(
        self,
        customer_id: str,
        data: bytes,
        content_type: str = "application/msgpack",
        node_count: int = 0,
        edge_count: int = 0,
    ) -> None:
        self._snapshots[customer_id] = {
            "data": data,
            "content_type": content_type,
            "received_at": time.time(),
            "node_count": node_count,
            "edge_count": edge_count,
        }

    def get_latest_snapshot(
        self,
        customer_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves the most recent full graph snapshot for a customer.
        Returns None if no snapshot exists.
        """
        entry = self._snapshots.get(customer_id)

        if entry is None:
            return None

        return {
            "data": entry["data"],
            "content_type": entry["content_type"],
            "received_at": entry["received_at"],
            "node_count": entry.get("node_count", 0),
            "edge_count": entry.get("edge_count", 0),
        }

    def insert_deployment_marker(
        self, customer_id: str, label: str
    ) -> None:
        self._markers[customer_id].append(
            {"label": label, "created_at": time.time()}
        )

    def insert_graph_diff(
        self, customer_id: str, diff: Dict
    ) -> None:
        self._diffs[customer_id].append(diff)

    def get_label_diff(
        self, customer_id: str, label: Optional[str]
    ) -> Optional[Dict]:
        for d in reversed(self._diffs[customer_id]):
            if d.get("label") == label:
                return d
        return None

    def get_diffs(self, customer_id: str) -> List[GraphDiff]:
        return list(self._diffs.get(customer_id, []))

    def save_causal_matches(
        self, customer_id, matches, timestamp, label=None
    ):
        self._causal_history.append(
            {
                "customer_id": customer_id,
                "timestamp": timestamp,
                "matches": matches,
            }
        )
        # keep last 200 snapshots - causal runs every 5s, 200 = ~16 mins
        if len(self._causal_history) > 200:
            self._causal_history = self._causal_history[-200:]

    def get_causal_history(self, customer_id, limit=50):
        return [
            e
            for e in reversed(self._causal_history)
            if e["customer_id"] == customer_id
        ][:limit]

    def close(self) -> None:
        pass

    def __len__(self) -> int:
        return len(self._events)

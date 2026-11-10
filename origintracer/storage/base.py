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
    """Abstract persistence interface for events and worker state.

    Implementations provide storage for probe events, graph snapshots, runtime
    heartbeats, deployment markers, graph diffs, and causal rule matches.
    """

    @abstractmethod
    def insert_event(self, event: NormalizedEvent) -> None:
        """Store one normalized probe event.

        Parameters
        ----------
        event : NormalizedEvent
            Event emitted by a runtime probe.

        Returns
        -------
        None
        """
        ...

    @abstractmethod
    def query_events(
        self,
        *,
        customer_id: Optional[str] = None,
        worker_pid: Optional[str] = None,
        worker_instance_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Retrieve matching events, newest first.

        Parameters
        ----------
        customer_id : str or None, optional
            Customer namespace to filter by.
        worker_pid : str or None, optional
            Operating-system process ID to filter by.
        worker_instance_id : str or None, optional
            Process-instance identity stored in event metadata. This separates
            process lifetimes when an operating-system PID is reused.
        trace_id : str or None, optional
            Trace identifier to filter by.
        probe : str or None, optional
            Probe name to filter by.
        service : str or None, optional
            Service name to filter by.
        since : float or None, optional
            Earliest event wall time, as a Unix timestamp.
        limit : int, optional
            Maximum number of events to return.

        Returns
        -------
        List[Dict[str, Any]]
            Matching event records, ordered from newest to oldest.
        """
        ...

    @abstractmethod
    def insert_snapshot(
        self,
        customer_id: str,
        worker_instance_id: str,
        worker_pid: int,
        data: bytes,
        content_type: str = "application/msgpack",
        node_count: int = 0,
        edge_count: int = 0,
    ) -> None:
        """Persist a serialized graph snapshot for a worker instance.

        Parameters
        ----------
        customer_id : str
            Customer namespace that owns the snapshot.
        worker_instance_id : str
            Identity of the process instance that produced the snapshot.
        worker_pid : int
            Operating-system process ID of the worker.
        data : bytes
            Serialized graph payload.
        content_type : str, optional
            MIME type of ``data``.
        node_count : int, optional
            Number of graph nodes represented by the snapshot.
        edge_count : int, optional
            Number of graph edges represented by the snapshot.

        Returns
        -------
        None

        Notes
        -----
        Implementations may replace older snapshots or retain them for audit.
        """
        ...

    @abstractmethod
    def get_latest_snapshot(
        self,
        customer_id: str,
        worker_instance_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return the latest snapshot for a worker instance.

        Parameters
        ----------
        customer_id : str
            Customer namespace that owns the snapshot.
        worker_instance_id : str
            Process-instance identity to look up.

        Returns
        -------
        Dict[str, Any] or None
            Snapshot record containing ``data``, ``content_type``, and
            ``received_at``, or ``None`` when no snapshot is stored.
        """
        ...

    @abstractmethod
    def list_latest_snapshots(
        self, customer_id: str
    ) -> List[Dict[str, Any]]:
        """List the latest snapshot for each worker instance.

        Parameters
        ----------
        customer_id : str
            Customer namespace whose snapshots are requested.

        Returns
        -------
        List[Dict[str, Any]]
            Snapshot records, each associated with its worker instance.
        """
        ...

    @abstractmethod
    def upsert_worker_runtime(
        self,
        customer_id: str,
        worker_instance_id: str,
        worker_pid: str,
        status: Dict[str, Any],
        observed_at: float,
    ) -> None:
        """Insert or replace the latest runtime heartbeat for one instance.

        A heartbeat contains status values that cannot be reconstructed from a graph snapshot,
        including uptime, active requests, active probes, and observation time. Implementations
        key records by customer and worker instance so PID reuse cannot merge separate process
        lifetimes.

        Parameters
        ----------
        customer_id : str
            Owner namespace for the stored process data.
        worker_instance_id : str
            Stable ID identifying one process lifetime.
        worker_pid : str
            Operating-system PID associated with the snapshot.
        status : Dict[str, Any]
            Runtime status payload reported by the uploader.
        observed_at : float
            Unix timestamp when the status was observed.
        """
        ...

    @abstractmethod
    def get_worker_runtime(
        self, customer_id: str, worker_instance_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return the latest runtime heartbeat for one process instance.

        Parameters
        ----------
        customer_id : str
            Owner namespace for the stored process data.
        worker_instance_id : str
            Stable ID identifying the process lifetime.

        Returns
        -------
        Dict[str, Any] or None
            Mapping with ``worker_pid``, ``observed_at``, and ``status``, or ``None``
            when no heartbeat has been uploaded for this instance.
        """
        ...

    @abstractmethod
    def list_worker_runtime(
        self, customer_id: str
    ) -> List[Dict[str, Any]]:
        """List the latest runtime heartbeat for each owned process instance.

        Parameters
        ----------
        customer_id : str
            Owner namespace whose heartbeat records are requested.

        Returns
        -------
        List[Dict[str, Any]]
            Rows containing ``worker_instance_id``, ``worker_pid``, ``observed_at``,
            and the reported ``status`` mapping. An empty list means no heartbeat is
            stored for this customer.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Release resources held by the repository.

        Returns
        -------
        None
        """
        ...

    @abstractmethod
    def insert_deployment_marker(
        self,
        customer_id: str,
        worker_instance_id: str,
        label: str,
        worker_pid: Optional[str] = None,
    ) -> None:
        """Store a deployment marker for a worker instance.

        Parameters
        ----------
        customer_id : str
            Customer namespace that owns the marker.
        worker_instance_id : str
            Identity of the process instance being marked.
        label : str
            Deployment label to record.
        worker_pid : str or None, optional
            Operating-system process ID associated with the marker.

        Returns
        -------
        None
        """
        ...

    @abstractmethod
    def get_deployment_marker_time(
        self,
        customer_id: str,
        worker_instance_id: str,
        label: str,
    ) -> Optional[float]:
        """Return the newest timestamp for a named deployment marker.

        Parameters
        ----------
        customer_id : str
            Owner namespace for the marker.
        worker_instance_id : str
            Stable identity of the marked process.
        label : str
            Deployment marker label to find.

        Returns
        -------
        float or None
            Newest marker timestamp, or ``None`` when no matching marker exists.
        """
        ...

    @abstractmethod
    def insert_graph_diff(
        self,
        customer_id: str,
        worker_instance_id: str,
        diff: Dict,
        worker_pid: Optional[str] = None,
    ) -> None:
        """Store one incremental graph diff from a worker.

        Parameters
        ----------
        customer_id : str
            Customer namespace that owns the diff.
        worker_instance_id : str
            Identity of the process instance that produced the diff.
        diff : Dict
            Incremental graph changes.
        worker_pid : str or None, optional
            Operating-system process ID associated with the diff.

        Returns
        -------
        None
        """
        ...

    @abstractmethod
    def save_causal_matches(
        self,
        customer_id: str,
        worker_instance_id: str,
        matches: List[Dict],
        timestamp: float,
        worker_pid: Optional[str] = None,
    ) -> None:
        """Persist a causal rule evaluation for historical review.

        Parameters
        ----------
        customer_id : str
            Customer namespace that owns the matches.
        worker_instance_id : str
            Identity of the process instance evaluated.
        matches : List[Dict]
            Rule matches produced by the evaluation.
        timestamp : float
            Evaluation time as a Unix timestamp.
        worker_pid : str or None, optional
            Operating-system process ID associated with the matches.

        Returns
        -------
        None
        """
        ...

    @abstractmethod
    def get_causal_history(
        self,
        customer_id: str,
        worker_instance_id: str,
        limit: int = 50,
    ) -> List[Dict]:
        """Return recent causal match snapshots for a worker instance.

        Parameters
        ----------
        customer_id : str
            Customer namespace that owns the history.
        worker_instance_id : str
            Process-instance identity to query.
        limit : int, optional
            Maximum number of snapshots to return.

        Returns
        -------
        List[Dict]
            Match snapshots ordered from newest to oldest.
        """
        ...


_PG_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS ot_events (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL DEFAULT 'default',
    worker_pid TEXT,
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

CREATE INDEX IF NOT EXISTS idx_ot_events_trace
    ON ot_events (trace_id);

CREATE INDEX IF NOT EXISTS idx_ot_events_customer
    ON ot_events (customer_id, wall_time DESC);

CREATE INDEX IF NOT EXISTS idx_ot_events_worker
    ON ot_events (customer_id, pid, wall_time DESC);

CREATE INDEX IF NOT EXISTS idx_ot_events_service
    ON ot_events (service);

CREATE INDEX IF NOT EXISTS idx_ot_events_probe
    ON ot_events (probe);

CREATE INDEX IF NOT EXISTS idx_ot_events_instance_probe
    ON ot_events (
        customer_id,
        (metadata->>'worker_instance_id'),
        probe,
        wall_time DESC
    );
"""

_PG_CREATE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS ot_snapshots (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    worker_instance_id TEXT NOT NULL,
    worker_pid TEXT,
    received_at DOUBLE PRECISION NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/msgpack',
    node_count INT,
    edge_count INT,
    data BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ot_snapshots_customer
    ON ot_snapshots (
        customer_id,
        worker_instance_id,
        received_at DESC
    );
"""

_PG_CREATE_WORKER_RUNTIME = """
CREATE TABLE IF NOT EXISTS worker_runtime_status (
    customer_id TEXT NOT NULL,
    worker_instance_id TEXT NOT NULL,
    worker_pid TEXT NOT NULL,
    observed_at DOUBLE PRECISION NOT NULL,
    status JSONB NOT NULL,
    PRIMARY KEY (customer_id, worker_instance_id)
);

CREATE INDEX IF NOT EXISTS idx_worker_runtime_seen
    ON worker_runtime_status (customer_id, observed_at DESC);
"""

_PG_CREATE_MARKERS = """
CREATE TABLE IF NOT EXISTS ot_markers (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    worker_instance_id TEXT,
    worker_pid TEXT,
    label TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL
);
"""

_PG_CREATE_GRAPH_DIFFS = """
CREATE TABLE IF NOT EXISTS ot_graph_diffs (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    worker_instance_id TEXT,
    worker_pid TEXT,
    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
    added_nodes JSONB,
    removed_nodes JSONB,
    added_edges JSONB,
    removed_edges JSONB,
    label VARCHAR(200)
);
"""

_PG_CREATE_CAUSAL_RULES = """
CREATE TABLE IF NOT EXISTS ot_causal_matches (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    worker_instance_id TEXT,
    worker_pid TEXT,
    timestamp DOUBLE PRECISION NOT NULL,
    matches JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

_PG_CREATE_WORKER_RUNTIME = """
CREATE TABLE IF NOT EXISTS worker_runtime_status (
    customer_id TEXT NOT NULL,
    worker_instance_id TEXT NOT NULL,
    worker_pid TEXT NOT NULL,
    observed_at DOUBLE PRECISION NOT NULL,
    status JSONB NOT NULL,
    PRIMARY KEY (customer_id, worker_instance_id)
);
CREATE INDEX IF NOT EXISTS idx_worker_runtime_seen
    ON worker_runtime_status (customer_id, observed_at DESC);
"""

_PG_CREATE_MARKERS = """
CREATE TABLE IF NOT EXISTS st_markers (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    worker_instance_id TEXT,
    worker_pid TEXT,
    label TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL
);
"""


_PG_CREATE_GRAPH_DIFFS = """
CREATE TABLE IF NOT EXISTS graph_diffs (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    worker_instance_id TEXT,
    worker_pid TEXT,
    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
    added_nodes JSONB,
    removed_nodes JSONB,
    added_edges JSONB,
    removed_edges JSONB,
    label VARCHAR(200) -- populated when a deployment marker exists
);
"""

_PG_CREATE_CAUSAL_RULES = """
CREATE TABLE IF NOT EXISTS causal_matches (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    worker_instance_id TEXT,
    worker_pid TEXT,
    timestamp DOUBLE PRECISION NOT NULL,
    matches JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
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
            cur.execute(_PG_CREATE_WORKER_RUNTIME)
            cur.execute(_PG_CREATE_MARKERS)
            cur.execute(_PG_CREATE_GRAPH_DIFFS)
            cur.execute(_PG_CREATE_CAUSAL_RULES)
            # Additional indexes for worker-specific lookups
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ot_markers_worker
                ON ot_markers (
                    customer_id,
                    worker_instance_id,
                    created_at DESC
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ot_graph_diffs_worker
                ON ot_graph_diffs (
                    customer_id,
                    worker_instance_id,
                    snapshot_time DESC
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ot_causal_worker
                ON ot_causal_matches (
                    customer_id,
                    worker_instance_id,
                    timestamp DESC
                )
            """)
        self._conn.commit()

    def get_diffs(
        self, customer_id: str, worker_instance_id: str
    ) -> List[Dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT added_nodes, removed_nodes, added_edges, removed_edges, snapshot_time, label FROM graph_diffs WHERE customer_id=%s AND worker_instance_id=%s ORDER BY snapshot_time DESC LIMIT 500",
                (customer_id, worker_instance_id),
            )
            rows = cur.fetchall()
        return [
            {
                "added_nodes": r[0] or [],
                "removed_nodes": r[1] or [],
                "added_edges": r[2] or [],
                "removed_edges": r[3] or [],
                "timestamp": (
                    r[4].timestamp() if r[4] else time.time()
                ),
                "label": r[5],
            }
            for r in rows
        ]

    def get_label_diff(
        self,
        customer_id: str,
        worker_instance_id: str,
        label: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT added_nodes, removed_nodes, added_edges, removed_edges, snapshot_time, label FROM graph_diffs WHERE customer_id=%s AND worker_instance_id=%s AND label IS NOT DISTINCT FROM %s ORDER BY snapshot_time DESC LIMIT 1",
                (customer_id, worker_instance_id, label),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return {
            "added_nodes": row[0] or [],
            "removed_nodes": row[1] or [],
            "added_edges": row[2] or [],
            "removed_edges": row[3] or [],
            "timestamp": row[4].timestamp() if row[4] else None,
            "label": row[5],
        }

    def insert_event(self, event: NormalizedEvent) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO st_events
                        (customer_id, worker_pid, trace_id, span_id, parent_span_id,
                         probe, service, name, wall_time,
                         duration_ns, pid, tid, metadata)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        event.metadata.get(
                            "customer_id", "default"
                        ),
                        str(
                            event.metadata.get(
                                "worker_pid", event.pid
                            )
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
        customer_id: Optional[str] = None,
        worker_pid: Optional[str] = None,
        worker_instance_id: Optional[str] = None,
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
        if customer_id:
            conditions.append("customer_id = %s")
            params.append(customer_id)
        if worker_pid:
            conditions.append("pid = %s")
            try:
                params.append(int(worker_pid))
            except (TypeError, ValueError):
                params.append(worker_pid)
        if worker_instance_id:
            conditions.append(
                "metadata->>'worker_instance_id' = %s"
            )
            params.append(worker_instance_id)

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
        worker_instance_id: str,
        worker_pid: int,
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
                        (customer_id, worker_instance_id, worker_pid, received_at, content_type,
                         node_count, edge_count, data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        customer_id,
                        worker_instance_id,
                        str(worker_pid),
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
        worker_instance_id: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data, content_type, received_at, worker_pid, worker_instance_id, node_count, edge_count
                    FROM   st_snapshots
                    WHERE  customer_id = %s AND worker_instance_id = %s
                    ORDER  BY received_at DESC
                    LIMIT  1
                    """,
                    (customer_id, worker_instance_id),
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
                "worker_pid": row[3],
                "worker_instance_id": row[4],
                "node_count": row[5],
                "edge_count": row[6],
            }
        except Exception as exc:
            logger.error(
                "PG get_latest_snapshot failed: %s", exc
            )
            return None

    def list_latest_snapshots(
        self, customer_id: str
    ) -> List[Dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (worker_instance_id) worker_instance_id, worker_pid,
                       data, content_type, received_at, node_count, edge_count
                FROM st_snapshots WHERE customer_id = %s
                ORDER BY worker_instance_id, received_at DESC
            """,
                (customer_id,),
            )
            rows = cur.fetchall()
        return [
            {
                "worker_instance_id": r[0],
                "worker_pid": r[1],
                "data": bytes(r[2]),
                "content_type": r[3],
                "received_at": r[4],
                "node_count": r[5],
                "edge_count": r[6],
            }
            for r in rows
        ]

    def upsert_worker_runtime(
        self,
        customer_id: str,
        worker_instance_id: str,
        worker_pid: str,
        status: Dict[str, Any],
        observed_at: float,
    ) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO worker_runtime_status
                        (customer_id, worker_instance_id, worker_pid, observed_at, status)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id, worker_instance_id)
                    DO UPDATE SET worker_pid = EXCLUDED.worker_pid,
                                  observed_at = EXCLUDED.observed_at,
                                  status = EXCLUDED.status
                    """,
                    (
                        customer_id,
                        worker_instance_id,
                        str(worker_pid),
                        observed_at,
                        json.dumps(status),
                    ),
                )
            self._conn.commit()
        except Exception as exc:
            logger.warning(
                "PG upsert_worker_runtime failed: %s", exc
            )
            self._safe_rollback()

    def get_worker_runtime(
        self, customer_id: str, worker_instance_id: str
    ) -> Optional[Dict[str, Any]]:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT worker_pid, observed_at, status
                    FROM worker_runtime_status
                    WHERE customer_id = %s AND worker_instance_id = %s
                    """,
                    (customer_id, worker_instance_id),
                )
                row = cur.fetchone()
            if row is None:
                return None
            status = row[2]
            if isinstance(status, str):
                status = json.loads(status)
            return {
                "worker_pid": row[0],
                "observed_at": row[1],
                "status": status or {},
            }
        except Exception as exc:
            logger.warning(
                "PG get_worker_runtime failed: %s", exc
            )
            return None

    def list_worker_runtime(
        self, customer_id: str
    ) -> List[Dict[str, Any]]:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT worker_instance_id, worker_pid, observed_at, status
                    FROM worker_runtime_status
                    WHERE customer_id = %s
                    ORDER BY observed_at DESC
                    """,
                    (customer_id,),
                )
                rows = cur.fetchall()
            result = []
            for instance_id, pid, observed_at, status in rows:
                if isinstance(status, str):
                    status = json.loads(status)
                result.append(
                    {
                        "worker_instance_id": instance_id,
                        "worker_pid": pid,
                        "observed_at": observed_at,
                        "status": status or {},
                    }
                )
            return result
        except Exception as exc:
            logger.warning(
                "PG list_worker_runtime failed: %s", exc
            )
            return []

    def insert_graph_diff(
        self,
        customer_id: str,
        worker_instance_id: str,
        diff: Dict,
        worker_pid: Optional[str] = None,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO graph_diffs
                (customer_id, worker_instance_id, worker_pid, snapshot_time, added_nodes, removed_nodes, added_edges, removed_edges, label)
                VALUES (%s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s)""",
                (
                    customer_id,
                    worker_instance_id,
                    worker_pid or "unknown",
                    diff.get("timestamp") or time.time(),
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

    def insert_deployment_marker(
        self,
        customer_id: str,
        worker_instance_id: str,
        label: str,
        worker_pid: Optional[str] = None,
    ) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO st_markers (customer_id, worker_instance_id, worker_pid, label, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        customer_id,
                        worker_instance_id,
                        worker_pid or "unknown",
                        label,
                        time.time(),
                    ),
                )
            self._conn.commit()
        except Exception as exc:
            logger.warning("PG insert_marker failed: %s", exc)
            self._safe_rollback()

    def get_deployment_marker_time(
        self,
        customer_id: str,
        worker_instance_id: str,
        label: str,
    ) -> Optional[float]:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT created_at
                    FROM st_markers
                    WHERE customer_id = %s
                      AND worker_instance_id = %s
                      AND label = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (customer_id, worker_instance_id, label),
                )
                row = cur.fetchone()
            return float(row[0]) if row is not None else None
        except Exception as exc:
            logger.warning(
                "PG get_deployment_marker_time failed: %s", exc
            )
            return None

    def save_causal_matches(
        self,
        customer_id,
        worker_instance_id,
        matches,
        timestamp,
        worker_pid=None,
    ):
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO causal_matches (customer_id, worker_instance_id, worker_pid, timestamp, matches) VALUES (%s, %s, %s, %s, %s)",
                (
                    customer_id,
                    worker_instance_id,
                    worker_pid or "unknown",
                    timestamp,
                    json.dumps(matches),
                ),
            )
        self._conn.commit()

    def get_causal_history(
        self, customer_id, worker_instance_id, limit=50
    ):
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT worker_instance_id, timestamp, matches FROM causal_matches WHERE customer_id = %s AND worker_instance_id = %s ORDER BY timestamp DESC LIMIT %s",
                (customer_id, worker_instance_id, limit),
            )
            rows = cur.fetchall()
        return [
            {
                "worker_instance_id": row[0],
                "timestamp": row[1],
                "matches": row[2],
            }
            for row in rows
        ]

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
        self._snapshots: Dict[tuple[str, str], Dict] = {}
        # Latest uploader heartbeat keyed by (customer, process instance).
        # Kept separately from snapshots because runtime health changes more
        # often and cannot be derived from the serialized graph.
        self._worker_runtime: Dict[
            tuple[str, str], Dict[str, Any]
        ] = {}
        # Markers, using defaultdict(list)
        self._markers: Dict[tuple[str, str], List[Dict]] = (
            defaultdict(list)
        )
        # Diffs, using defaultdict with a lambda to create deques automatically
        # This keeps exactly the last 500 diffs per customer with zero
        # manual work.
        self._diffs: Dict[tuple[str, str], deque] = defaultdict(
            lambda: deque(maxlen=max_diffs)
        )
        # For rules that matched - anti-patterns
        self._causal_history: List[Dict] = []

    def insert_event(self, event: NormalizedEvent) -> None:
        # deque handles the maxlen=100_000 internally
        if event.metadata is None:
            raise TypeError("Event metadata is required")
        customer_id = event.metadata.get("customer_id")
        worker_pid = event.metadata.get("worker_pid")
        if not customer_id or not worker_pid:
            raise ValueError(
                "Missing customer_id or pid in event metadata"
            )
        self._events.append(event.to_dict())

    def query_events(
        self,
        *,
        customer_id: Optional[str] = None,
        worker_pid: Optional[str] = None,
        worker_instance_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        results = []
        for e in reversed(self._events):
            metadata = e.get("metadata") or {}
            if (
                customer_id
                and metadata.get("customer_id") != customer_id
            ):
                continue
            if worker_pid and str(
                metadata.get("worker_pid", e.get("pid"))
            ) != str(worker_pid):
                continue
            if (
                worker_instance_id
                and metadata.get("worker_instance_id")
                != worker_instance_id
            ):
                continue
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
        worker_instance_id: str,
        worker_pid: int,
        data: bytes,
        content_type: str = "application/msgpack",
        node_count: int = 0,
        edge_count: int = 0,
    ) -> None:
        self._snapshots[(customer_id, worker_instance_id)] = {
            "worker_instance_id": worker_instance_id,
            "worker_pid": worker_pid,
            "data": data,
            "content_type": content_type,
            "received_at": time.time(),
            "node_count": node_count,
            "edge_count": edge_count,
        }

    def get_latest_snapshot(
        self,
        customer_id: str,
        worker_instance_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves the most recent full graph snapshot for a customer.
        Returns None if no snapshot exists.
        """
        entry = self._snapshots.get(
            (customer_id, worker_instance_id)
        )

        if entry is None:
            return None

        return {
            "data": entry["data"],
            "content_type": entry["content_type"],
            "received_at": entry["received_at"],
            "node_count": entry.get("node_count", 0),
            "edge_count": entry.get("edge_count", 0),
            "worker_instance_id": entry["worker_instance_id"],
            "worker_pid": entry["worker_pid"],
        }

    def list_latest_snapshots(
        self, customer_id: str
    ) -> List[Dict[str, Any]]:
        return [
            dict(value)
            for (owner, _), value in self._snapshots.items()
            if owner == customer_id
        ]

    def upsert_worker_runtime(
        self,
        customer_id: str,
        worker_instance_id: str,
        worker_pid: str,
        status: Dict[str, Any],
        observed_at: float,
    ) -> None:
        self._worker_runtime[
            (customer_id, worker_instance_id)
        ] = {
            "worker_pid": str(worker_pid),
            "observed_at": observed_at,
            "status": dict(status),
        }

    def get_worker_runtime(
        self, customer_id: str, worker_instance_id: str
    ) -> Optional[Dict[str, Any]]:
        entry = self._worker_runtime.get(
            (customer_id, worker_instance_id)
        )
        return dict(entry) if entry is not None else None

    def list_worker_runtime(
        self, customer_id: str
    ) -> List[Dict[str, Any]]:
        return [
            {
                "worker_instance_id": instance_id,
                **dict(status),
            }
            for (
                owner,
                instance_id,
            ), status in self._worker_runtime.items()
            if owner == customer_id
        ]

    def insert_deployment_marker(
        self,
        customer_id: str,
        worker_instance_id: str,
        label: str,
        worker_pid: Optional[str] = None,
    ) -> None:
        self._markers[(customer_id, worker_instance_id)].append(
            {
                "label": label,
                "worker_pid": worker_pid,
                "created_at": time.time(),
            }
        )

    def get_deployment_marker_time(
        self,
        customer_id: str,
        worker_instance_id: str,
        label: str,
    ) -> Optional[float]:
        markers = self._markers.get(
            (customer_id, worker_instance_id), []
        )
        matches = [
            float(marker["created_at"])
            for marker in markers
            if marker.get("label") == label
        ]
        return max(matches) if matches else None

    def insert_graph_diff(
        self,
        customer_id: str,
        worker_instance_id: str,
        diff: Dict,
        worker_pid: Optional[str] = None,
    ) -> None:
        stored_diff = dict(diff)
        stored_diff.setdefault("worker_pid", worker_pid)
        self._diffs[(customer_id, worker_instance_id)].append(
            stored_diff
        )

    def get_label_diff(
        self,
        customer_id: str,
        worker_instance_id: str,
        label: Optional[str],
    ) -> Optional[Dict]:
        for d in reversed(
            self._diffs[(customer_id, worker_instance_id)]
        ):
            if d.get("label") == label:
                return d
        return None

    def get_diffs(
        self, customer_id: str, worker_instance_id: str
    ) -> List[Dict[str, Any]]:
        return list(
            self._diffs.get(
                (customer_id, worker_instance_id), []
            )
        )

    def save_causal_matches(
        self,
        customer_id,
        worker_instance_id,
        matches,
        timestamp,
        worker_pid=None,
    ):
        entry = {
            "customer_id": customer_id,
            "worker_instance_id": worker_instance_id,
            "timestamp": timestamp,
            "matches": matches,
        }
        if worker_pid is not None:
            entry["worker_pid"] = worker_pid
        self._causal_history.append(entry)
        # keep last 200 snapshots - causal runs every 5s, 200 = ~16 mins
        if len(self._causal_history) > 200:
            self._causal_history = self._causal_history[-200:]

    def get_causal_history(
        self, customer_id, worker_instance_id, limit=50
    ):
        return [
            e
            for e in reversed(self._causal_history)
            if e["customer_id"] == customer_id
            and e["worker_instance_id"] == worker_instance_id
        ][:limit]

    def close(self) -> None:
        pass

    def __len__(self) -> int:
        return len(self._events)

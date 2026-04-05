from __future__ import annotations

import base64
import json
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional

from origintracer.core.event_schema import NormalizedEvent

logger = logging.getLogger("origintracer.storage")


class BaseRepository(ABC):

    @abstractmethod
    def insert_event(self, event: NormalizedEvent) -> None:
        """Store one probe event."""
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
        """Release any open connections."""
        ...

    @abstractmethod
    def insert_deployment_marker(
        self, customer_id: str, label: str
    ) -> None:
        """Store a deployment marker with the current timestamp."""
        ...

    @abstractmethod
    def insert_graph_diff(
        self, customer_id: str, diff: Dict
    ) -> None:
        """Store one graph diff snapshot from the agent."""
        ...


# ====================================================================== #
# PostgreSQL
# ====================================================================== #

_PG_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS st_events (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     TEXT         NOT NULL DEFAULT 'default',
    trace_id        TEXT         NOT NULL,
    span_id         TEXT,
    parent_span_id  TEXT,
    probe           TEXT         NOT NULL,
    service         TEXT         NOT NULL,
    name            TEXT         NOT NULL,
    wall_time       DOUBLE PRECISION NOT NULL,
    duration_ns     BIGINT,
    pid             INT,
    tid             INT,
    metadata        JSONB
);
CREATE INDEX IF NOT EXISTS idx_st_events_trace    ON st_events (trace_id);
CREATE INDEX IF NOT EXISTS idx_st_events_customer ON st_events (customer_id, wall_time DESC);
CREATE INDEX IF NOT EXISTS idx_st_events_service  ON st_events (service);
CREATE INDEX IF NOT EXISTS idx_st_events_probe    ON st_events (probe);
"""

_PG_CREATE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS st_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     TEXT             NOT NULL,
    received_at     DOUBLE PRECISION NOT NULL,
    content_type    TEXT             NOT NULL DEFAULT 'application/msgpack',
    node_count      INT,
    edge_count      INT,
    data            BYTEA            NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_st_snapshots_customer
    ON st_snapshots (customer_id, received_at DESC);
"""

_PG_CREATE_MARKERS = """
CREATE TABLE IF NOT EXISTS st_markers (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     TEXT             NOT NULL,
    label           TEXT             NOT NULL,
    created_at      DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_st_markers_customer
    ON st_markers (customer_id, created_at DESC);
"""


_PG_CREATE_GRAPH_DIFFS = """
CREATE TABLE graph_diffs (
    id              SERIAL PRIMARY KEY,
    customer_id     VARCHAR(100) NOT NULL,
    snapshot_time   TIMESTAMPTZ  DEFAULT NOW(),
    added_nodes     JSONB,
    removed_nodes   JSONB,
    added_edges     JSONB,
    removed_edges   JSONB,
    label           VARCHAR(200)   -- populated when a deployment marker exists
);
"""


class PGEventRepository(BaseRepository):
    """
    PostgreSQL-backed store.
    Uses psycopg2 with a plain connection for MVP.
    For production: replace with psycopg2.pool.ThreadedConnectionPool
    or asyncpg for async FastAPI handlers.
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

    # ── Events ──────────────────────────────────────────────────────────

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

    # ── Snapshots ────────────────────────────────────────────────────────

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

    # --------------------- Graph diffs-------------------------------

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

    # ── Markers ──────────────────────────────────────────────────────────

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


# -------------------- ClickHouse ---------------------------

_CH_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS st_probe_events (
    customer_id     String       DEFAULT 'default',
    trace_id        String,
    span_id         String       DEFAULT '',
    parent_span_id  String       DEFAULT '',
    probe           String,
    service         String,
    name            String,
    wall_time       DateTime64(3),
    duration_ns     Nullable(Int64),
    pid             Nullable(Int32),
    tid             Nullable(Int32),
    metadata        String       DEFAULT '{}',
    date            Date         DEFAULT toDate(wall_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (customer_id, trace_id, wall_time)
TTL date + INTERVAL 30 DAY
"""

_CH_TRACE_SUMMARY_DDL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS st_trace_summary
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(toDate(min_wall_time))
ORDER BY (customer_id, trace_id)
AS
SELECT
    customer_id,
    trace_id,
    minState(wall_time)    AS min_wall_time,
    maxState(wall_time)    AS max_wall_time,
    countState()           AS event_count,
    groupArrayState(probe) AS probes
FROM st_probe_events
GROUP BY customer_id, trace_id
"""

# ClickHouse has no BYTEA — store snapshots base64-encoded in a String column.
_CH_SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS st_snapshots (
    customer_id     String,
    received_at     Float64,
    content_type    String  DEFAULT 'application/msgpack',
    node_count      Int32   DEFAULT 0,
    edge_count      Int32   DEFAULT 0,
    data_b64        String
)
ENGINE = ReplacingMergeTree(received_at)
ORDER BY (customer_id, received_at)
"""

_CH_MARKERS_DDL = """
CREATE TABLE IF NOT EXISTS st_markers (
    customer_id     String,
    label           String,
    created_at      Float64
)
ENGINE = MergeTree()
ORDER BY (customer_id, created_at)
"""


_CH_GRAPH_DIFFS_DDL = """
CREATE TABLE IF NOT EXISTS graph_diffs (
    customer_id    String,
    snapshot_time  DateTime64(3) DEFAULT now(),
    added_nodes    String,   -- JSON array
    removed_nodes  String,
    added_edges    String,
    removed_edges  String,
    label          Nullable(String)
) ENGINE = MergeTree()
ORDER BY (customer_id, snapshot_time);
"""


class ClickHouseRepository(BaseRepository):
    """
    ClickHouse-backed analytical store.
    Requires: pip install clickhouse-driver
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        database: str = "origintracer",
    ) -> None:
        try:
            from clickhouse_driver import Client

            self._client = Client(
                host=host, port=port, database=database
            )
            self._ensure_schema()
            logger.info(
                "ClickHouse connected: %s:%d/%s",
                host,
                port,
                database,
            )
        except ImportError:
            raise RuntimeError(
                "clickhouse-driver not installed: pip install clickhouse-driver"
            )
        except Exception as exc:
            raise RuntimeError(
                f"ClickHouse connection failed: {exc}"
            ) from exc

    def _ensure_schema(self) -> None:
        for ddl in (
            _CH_EVENTS_DDL,
            _CH_TRACE_SUMMARY_DDL,
            _CH_SNAPSHOTS_DDL,
            _CH_MARKERS_DDL,
            _CH_GRAPH_DIFFS_DDL,
        ):
            try:
                self._client.execute(ddl)
            except Exception as exc:
                logger.debug(
                    "ClickHouse DDL skip (may exist): %s", exc
                )

    # ── Events ──────────────────────────────────────────────────────────

    def insert_event(self, event: NormalizedEvent) -> None:
        from datetime import datetime

        try:
            self._client.execute(
                """
                INSERT INTO st_probe_events
                    (customer_id, trace_id, span_id, parent_span_id,
                     probe, service, name, wall_time,
                     duration_ns, pid, tid, metadata)
                VALUES
                """,
                [
                    (
                        event.metadata.get(
                            "customer_id", "default"
                        ),
                        event.trace_id,
                        event.span_id or "",
                        event.parent_span_id or "",
                        event.probe,
                        event.service,
                        event.name,
                        datetime.utcfromtimestamp(
                            event.wall_time
                        ),
                        event.duration_ns,
                        event.pid,
                        event.tid,
                        json.dumps(event.metadata),
                    )
                ],
            )
        except Exception as exc:
            logger.warning(
                "ClickHouse insert_event failed: %s", exc
            )

    def query_events(
        self,
        *,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        conditions: List[str] = ["1=1"]
        params: Dict[str, Any] = {}

        if trace_id:
            conditions.append("trace_id = %(trace_id)s")
            params["trace_id"] = trace_id
        if probe:
            conditions.append("probe = %(probe)s")
            params["probe"] = probe
        if service:
            conditions.append("service = %(service)s")
            params["service"] = service
        if since:
            from datetime import datetime

            conditions.append("wall_time >= %(since)s")
            params["since"] = datetime.utcfromtimestamp(since)

        params["limit"] = limit

        sql = f"""
            SELECT trace_id, span_id, probe, service, name,
                   wall_time, duration_ns, metadata
            FROM   st_probe_events
            WHERE  {' AND '.join(conditions)}
            ORDER  BY wall_time DESC
            LIMIT  %(limit)s
        """
        try:
            rows = self._client.execute(sql, params)
            cols = [
                "trace_id",
                "span_id",
                "probe",
                "service",
                "name",
                "wall_time",
                "duration_ns",
                "metadata",
            ]
            return [dict(zip(cols, r)) for r in rows]
        except Exception as exc:
            logger.error(
                "ClickHouse query_events failed: %s", exc
            )
            return []

    # ── Snapshots ────────────────────────────────────────────────────────

    def insert_snapshot(
        self,
        customer_id: str,
        data: bytes,
        content_type: str = "application/msgpack",
        node_count: int = 0,
        edge_count: int = 0,
    ) -> None:
        try:
            self._client.execute(
                """
                INSERT INTO st_snapshots
                    (customer_id, received_at, content_type,
                     node_count, edge_count, data_b64)
                VALUES
                """,
                [
                    (
                        customer_id,
                        time.time(),
                        content_type,
                        node_count,
                        edge_count,
                        base64.b64encode(data).decode("ascii"),
                    )
                ],
            )
        except Exception as exc:
            logger.warning(
                "ClickHouse insert_snapshot failed: %s", exc
            )

    def get_latest_snapshot(
        self,
        customer_id: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            rows = self._client.execute(
                """
                SELECT data_b64, content_type, received_at
                FROM   st_snapshots
                WHERE  customer_id = %(cid)s
                ORDER  BY received_at DESC
                LIMIT  1
                """,
                {"cid": customer_id},
            )
            if not rows:
                return None
            row = rows[0]
            return {
                "data": base64.b64decode(row[0]),
                "content_type": row[1],
                "received_at": float(row[2]),
            }
        except Exception as exc:
            logger.error(
                "ClickHouse get_latest_snapshot failed: %s", exc
            )
            return None

    # ── Markers ──────────────────────────────────────────────────────────

    def insert_deployment_marker(
        self, customer_id: str, label: str
    ) -> None:
        self._client.execute(
            "INSERT INTO deployment_markers (customer_id, label) VALUES",
            [{"customer_id": customer_id, "label": label}],
        )

    def insert_graph_diff(
        self, customer_id: str, diff: Dict
    ) -> None:
        import json

        self._client.execute(
            """INSERT INTO graph_diffs
            (customer_id, added_nodes, removed_nodes,
                added_edges, removed_edges, label)
            VALUES""",
            [
                {
                    "customer_id": customer_id,
                    "added_nodes": json.dumps(
                        list(diff.get("added_nodes", []))
                    ),
                    "removed_nodes": json.dumps(
                        list(diff.get("removed_nodes", []))
                    ),
                    "added_edges": json.dumps(
                        list(diff.get("added_edges", []))
                    ),
                    "removed_edges": json.dumps(
                        list(diff.get("removed_edges", []))
                    ),
                    "label": diff.get("label"),
                }
            ],
        )

    def close(self) -> None:
        pass  # clickhouse-driver manages connections internally


# ====================================================================== #
# InMemory — dev / tests
# ====================================================================== #


class InMemoryRepository(BaseRepository):
    """
    Refactored In-Memory store using collections for performance.
    - Uses deque(maxlen=X) to automatically rotate old data out.
    - Uses defaultdict to eliminate boilerplate key-initialization.
    """

    def __init__(
        self, max_events: int = 100_000, max_diffs: int = 500
    ) -> None:
        # Events: Global rolling buffer
        self._events: deque = deque(maxlen=max_events)

        # Snapshots: Usually one-per-customer, so a dict is fine
        self._snapshots: Dict[str, Dict] = {}

        # Markers: Using defaultdict(list) means we never need .setdefault()
        self._markers: Dict[str, List[Dict]] = defaultdict(list)

        # Diffs: Using defaultdict with a lambda to create deques automatically
        # This keeps exactly the last 500 diffs per customer with zero manual work.
        self._diffs: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_diffs)
        )

    # ── Events ──────────────────────────────────────────────────────────

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
        # reversed() on a deque is O(1) for the iterator setup
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

    # ── Snapshots ────────────────────────────────────────────────────────

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

    # ── Snapshots (Continued) ──────────────────────────────────────────

    def get_latest_snapshot(
        self,
        customer_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves the most recent full graph snapshot for a customer.
        Returns None if no snapshot exists.
        """
        # .get() is perfect here—it handles the "None" case automatically
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

    # ── Markers & Diffs ──────────────────────────────────────────────────

    def insert_deployment_marker(
        self, customer_id: str, label: str
    ) -> None:
        # No more .setdefault()! defaultdict handles the creation of the list.
        self._markers[customer_id].append(
            {"label": label, "created_at": time.time()}
        )

    def insert_graph_diff(
        self, customer_id: str, diff: Dict
    ) -> None:
        # deque(maxlen=500) handles the truncation automatically.
        self._diffs[customer_id].append(diff)

    def close(self) -> None:
        pass

    def __len__(self) -> int:
        return len(self._events)

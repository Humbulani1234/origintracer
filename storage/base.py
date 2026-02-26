"""
storage/models.py + storage/repository.py

Two storage backends:

1. PostgreSQL (EventRepository)
   — Primary operational store
   — Stores recent events (last 7 days)
   — Used for real-time queries and the API

2. ClickHouse (ClickHouseRepository)
   — Analytical store
   — Stores long-term trace history
   — Used for aggregate queries, trends, causal pattern discovery

Both implement the same interface:
    insert_event(event: NormalizedEvent) -> None
    query_events(trace_id=None, probe=None, service=None, limit=100) -> List[dict]

Switch between them via config.
"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..core.event_schema import NormalizedEvent

logger = logging.getLogger("stacktracer.storage")


# ====================================================================== #
# Abstract interface
# ====================================================================== #

class BaseRepository(ABC):
    @abstractmethod
    def insert_event(self, event: NormalizedEvent) -> None: ...

    @abstractmethod
    def query_events(
        self,
        *,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]: ...

    @abstractmethod
    def close(self) -> None: ...


# ====================================================================== #
# PostgreSQL
# ====================================================================== #

CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS st_events (
    id              BIGSERIAL PRIMARY KEY,
    trace_id        TEXT NOT NULL,
    span_id         TEXT,
    parent_span_id  TEXT,
    probe           TEXT NOT NULL,
    service         TEXT NOT NULL,
    name            TEXT NOT NULL,
    wall_time       DOUBLE PRECISION NOT NULL,
    duration_ns     BIGINT,
    pid             INT,
    tid             INT,
    metadata        JSONB,
    customer_id     TEXT DEFAULT 'default'
);

CREATE INDEX IF NOT EXISTS idx_st_events_trace    ON st_events (trace_id);
CREATE INDEX IF NOT EXISTS idx_st_events_service  ON st_events (service);
CREATE INDEX IF NOT EXISTS idx_st_events_wall     ON st_events (wall_time DESC);
CREATE INDEX IF NOT EXISTS idx_st_events_probe    ON st_events (probe);
"""


class EventRepository(BaseRepository):
    """
    PostgreSQL-backed event store.
    Uses psycopg2 with a simple connection (for MVP).
    For production: use a connection pool (psycopg2.pool or asyncpg).
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(CREATE_EVENTS_TABLE)
        self._conn.commit()

    def insert_event(self, event: NormalizedEvent) -> None:
        row = (
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
        )
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO st_events
                        (trace_id, span_id, parent_span_id, probe, service, name,
                         wall_time, duration_ns, pid, tid, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    row,
                )
            self._conn.commit()
        except Exception as exc:
            logger.warning("PostgreSQL insert failed: %s", exc)
            try:
                self._conn.rollback()
            except Exception:
                pass

    def query_events(
        self,
        *,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        conditions = []
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

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit)

        sql = f"""
            SELECT trace_id, span_id, parent_span_id, probe, service, name,
                   wall_time, duration_ns, pid, tid, metadata
            FROM st_events
            {where}
            ORDER BY wall_time DESC
            LIMIT %s
        """

        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            cols = ["trace_id", "span_id", "parent_span_id", "probe", "service",
                    "name", "wall_time", "duration_ns", "pid", "tid", "metadata"]
            return [dict(zip(cols, r)) for r in rows]
        except Exception as exc:
            logger.error("PostgreSQL query failed: %s", exc)
            return []

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass


# ====================================================================== #
# ClickHouse
# ====================================================================== #

CLICKHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS st_probe_events (
    customer_id     String DEFAULT 'default',
    trace_id        String,
    span_id         String DEFAULT '',
    parent_span_id  String DEFAULT '',
    probe           String,
    service         String,
    name            String,
    wall_time       DateTime64(3),
    duration_ns     Nullable(Int64),
    pid             Nullable(Int32),
    tid             Nullable(Int32),
    metadata        String DEFAULT '{}',   -- JSON string
    date            Date DEFAULT toDate(wall_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (customer_id, trace_id, wall_time)
TTL date + INTERVAL 30 DAY;

-- Materialised view: per-trace summary
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
GROUP BY customer_id, trace_id;
"""


class ClickHouseRepository(BaseRepository):
    """
    ClickHouse-backed analytical event store.
    Uses clickhouse-driver (pip install clickhouse-driver).
    """

    def __init__(self, host: str = "localhost", port: int = 9000, database: str = "stacktracer") -> None:
        try:
            from clickhouse_driver import Client
            self._client = Client(host=host, port=port, database=database)
            self._ensure_schema()
            logger.info("ClickHouse repository connected (%s:%d/%s)", host, port, database)
        except ImportError:
            raise RuntimeError("clickhouse-driver not installed: pip install clickhouse-driver")
        except Exception as exc:
            raise RuntimeError(f"ClickHouse connection failed: {exc}") from exc

    def _ensure_schema(self) -> None:
        for statement in CLICKHOUSE_DDL.split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    self._client.execute(stmt)
                except Exception as exc:
                    logger.debug("ClickHouse DDL skip (may already exist): %s", exc)

    def insert_event(self, event: NormalizedEvent) -> None:
        from datetime import datetime
        try:
            self._client.execute(
                """
                INSERT INTO st_probe_events
                    (trace_id, span_id, parent_span_id, probe, service, name,
                     wall_time, duration_ns, pid, tid, metadata)
                VALUES
                """,
                [(
                    event.trace_id,
                    event.span_id or "",
                    event.parent_span_id or "",
                    event.probe,
                    event.service,
                    event.name,
                    datetime.utcfromtimestamp(event.wall_time),
                    event.duration_ns,
                    event.pid,
                    event.tid,
                    json.dumps(event.metadata),
                )],
            )
        except Exception as exc:
            logger.warning("ClickHouse insert failed: %s", exc)

    def query_events(
        self,
        *,
        trace_id: Optional[str] = None,
        probe: Optional[str] = None,
        service: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        conditions = ["1=1"]
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
            FROM st_probe_events
            WHERE {' AND '.join(conditions)}
            ORDER BY wall_time DESC
            LIMIT %(limit)s
        """

        try:
            rows = self._client.execute(sql, params)
            cols = ["trace_id", "span_id", "probe", "service", "name",
                    "wall_time", "duration_ns", "metadata"]
            return [dict(zip(cols, r)) for r in rows]
        except Exception as exc:
            logger.error("ClickHouse query failed: %s", exc)
            return []

    def close(self) -> None:
        pass  # clickhouse-driver manages connections internally


# ====================================================================== #
# In-memory repository (tests / development)
# ====================================================================== #

class InMemoryRepository(BaseRepository):
    """
    No-dependency in-memory store for tests and local development.
    Data is lost on restart.
    """

    def __init__(self, max_events: int = 100_000) -> None:
        from collections import deque
        self._events: deque = deque(maxlen=max_events)

    def insert_event(self, event: NormalizedEvent) -> None:
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
            if trace_id and e["trace_id"] != trace_id:
                continue
            if probe and e["probe"] != probe:
                continue
            if service and e["service"] != service:
                continue
            if since and e["wall_time"] < since:
                continue
            results.append(e)
            if len(results) >= limit:
                break
        return results

    def close(self) -> None:
        pass

    def __len__(self) -> int:
        return len(self._events)
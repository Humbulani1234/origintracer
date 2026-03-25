"""
stacktracer/probes/psycopg2_probe.py

Observes direct psycopg2 usage without patching the global cursor class.

Context:
    In Django, all database queries go through the ORM's execute_wrapper
    (see django_probe.py) which handles SQL observation cleanly.
    This probe is for direct psycopg2 use OUTSIDE Django — specifically
    Celery tasks that open their own psycopg2 connections without Django.

Approach — TracedConnection + TracedCursor:
    We provide a traced_connect() function that wraps psycopg2.connect()
    and returns a connection whose cursor_factory produces traced cursors.

    The user calls traced_connect() instead of psycopg2.connect():
        from stacktracer.probes.psycopg2_probe import traced_connect
        conn = traced_connect(dsn)   # one line change

    No global patching. If the user wants to keep using psycopg2.connect()
    directly they can wrap at the connection pool level instead:
        from stacktracer.probes.psycopg2_probe import TracedCursorFactory
        conn = psycopg2.connect(dsn)
        conn.cursor_factory = TracedCursorFactory

    TracedCursor subclasses psycopg2.extensions.cursor — this is a
    documented psycopg2 extension pattern used by connection pools,
    Django's database backend, and pgbouncer clients.

What we observe:
    cursor.execute(query, params)    → one SQL statement
    cursor.executemany(query, seq)   → batch SQL statement
    cursor.callproc(procname, args)  → stored procedure

    For each: query text, duration, row count, error if any.
    Query text is passed through GraphNormalizer which collapses
    literal values to ? — so all SELECTs with different IDs converge
    to the same graph node.

ProbeTypes (registered at import):
    psycopg2.query.execute     single SQL statement
    psycopg2.query.executemany batch SQL statement
    psycopg2.query.callproc    stored procedure call
    psycopg2.connection.open   new connection established
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from stacktracer.context.vars import get_span_id, get_trace_id
from stacktracer.core.event_schema import (
    NormalizedEvent,
    ProbeTypes,
)
from stacktracer.sdk.emitter import emit

logger = logging.getLogger("stacktracer.probes.psycopg2")

ProbeTypes.register_many(
    {
        "psycopg2.query.execute": "psycopg2 cursor.execute() — single SQL statement",
        "psycopg2.query.executemany": "psycopg2 cursor.executemany() — batch SQL",
        "psycopg2.query.callproc": "psycopg2 cursor.callproc() — stored procedure",
        "psycopg2.connection.open": "psycopg2 connection established",
    }
)


def _get_psycopg2():
    try:
        import psycopg2
        import psycopg2.extensions

        return psycopg2
    except ImportError:
        raise ImportError(
            "psycopg2 not installed. pip install psycopg2-binary"
        )


# ====================================================================== #
# TracedCursor
# ====================================================================== #


class TracedCursor:
    """
    Wraps a psycopg2 cursor and emits NormalizedEvents on each query.

    This is a composition wrapper rather than a subclass because
    psycopg2 cursor objects cannot always be subclassed safely
    (the C extension cursor has restrictions in some versions).

    Usage is identical to a normal cursor — all attributes and methods
    delegate to the underlying cursor.
    """

    def __init__(self, cursor: Any, dsn: str = "") -> None:
        self._cursor = cursor
        self._dsn = dsn  # for service identification

    def execute(self, query: str, params=None) -> Any:
        return self._traced_execute(
            "psycopg2.query.execute", query, params
        )

    def executemany(self, query: str, seq) -> Any:
        trace_id = get_trace_id()
        t0 = time.perf_counter()
        try:
            result = self._cursor.executemany(query, seq)
        except Exception as exc:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(
                    NormalizedEvent.now(
                        probe="psycopg2.query.executemany",
                        trace_id=trace_id,
                        service="postgres",
                        name=query[:200],
                        duration_ns=duration_ns,
                        error=str(exc)[:200],
                        success=False,
                    )
                )
            raise
        else:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(
                    NormalizedEvent.now(
                        probe="psycopg2.query.executemany",
                        trace_id=trace_id,
                        service="postgres",
                        name=query[:200],
                        duration_ns=duration_ns,
                        success=True,
                    )
                )
            return result

    def callproc(self, procname: str, parameters=None) -> Any:
        return self._traced_execute(
            "psycopg2.query.callproc",
            procname,
            parameters,
            is_proc=True,
        )

    def _traced_execute(
        self,
        probe_type: str,
        query: str,
        params: Any,
        is_proc: bool = False,
    ) -> Any:
        trace_id = get_trace_id()
        t0 = time.perf_counter()

        try:
            if is_proc:
                result = self._cursor.callproc(
                    query, params or []
                )
            elif params is not None:
                result = self._cursor.execute(query, params)
            else:
                result = self._cursor.execute(query)
        except Exception as exc:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(
                    NormalizedEvent.now(
                        probe=probe_type,
                        trace_id=trace_id,
                        service="postgres",
                        name=query[:200],
                        parent_span_id=get_span_id(),
                        duration_ns=duration_ns,
                        error=str(exc)[:200],
                        success=False,
                    )
                )
            raise
        else:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(
                    NormalizedEvent.now(
                        probe=probe_type,
                        trace_id=trace_id,
                        service="postgres",
                        name=query[:200],
                        parent_span_id=get_span_id(),
                        duration_ns=duration_ns,
                        rowcount=self._cursor.rowcount,
                        success=True,
                    )
                )
            return result

    # ── Delegate everything else to the real cursor ──────────────────

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    def __iter__(self):
        return iter(self._cursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cursor.__exit__(exc_type, exc_val, exc_tb)
        return False


# ====================================================================== #
# TracedConnection
# ====================================================================== #


class TracedConnection:
    """
    Wraps a psycopg2 connection and returns TracedCursors.

    Usage:
        conn = TracedConnection(psycopg2.connect(dsn))
        # or use the convenience function:
        conn = traced_connect(dsn)

        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._dsn = getattr(conn, "dsn", "")

    def cursor(self, *args, **kwargs) -> TracedCursor:
        raw_cursor = self._conn.cursor(*args, **kwargs)
        return TracedCursor(raw_cursor, self._dsn)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.__exit__(exc_type, exc_val, exc_tb)
        return False


# ====================================================================== #
# Public convenience functions
# ====================================================================== #


def traced_connect(*args, **kwargs) -> TracedConnection:
    """
    Drop-in replacement for psycopg2.connect().

    Usage:
        # Before:
        conn = psycopg2.connect(dsn)

        # After:
        from stacktracer.probes.psycopg2_probe import traced_connect
        conn = traced_connect(dsn)

    Returns a TracedConnection that emits NormalizedEvents on every
    cursor.execute() call. All other psycopg2 behaviour is unchanged.
    """
    psycopg2 = _get_psycopg2()
    trace_id = get_trace_id()

    t0 = time.perf_counter()
    conn = psycopg2.connect(*args, **kwargs)
    duration_ns = int((time.perf_counter() - t0) * 1e9)

    if trace_id:
        emit(
            NormalizedEvent.now(
                probe="psycopg2.connection.open",
                trace_id=trace_id,
                service="postgres",
                name="connect",
                duration_ns=duration_ns,
                dsn=str(
                    kwargs.get("dsn", args[0] if args else "")
                )[:100],
            )
        )

    return TracedConnection(conn)

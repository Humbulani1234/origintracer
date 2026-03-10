"""
core/local_server.py

Unix domain socket server that the StackTracer REPL connects to.

The agent process (gunicorn worker) starts this server at init() time.
The REPL (running in a separate terminal) connects to the socket and
sends query strings. The server evaluates them against the live engine
and returns JSON responses.

Socket path: /tmp/stacktracer-{pid}.sock
    pid = worker process pid, so each worker has its own socket.
    The REPL discovers the socket by listing /tmp/stacktracer-*.sock.

Protocol: newline-delimited JSON.
    Request:  {"query": "SHOW nodes", "id": "1"}\\n
    Response: {"id": "1", "ok": true, "data": [...]}\\n
              {"id": "1", "ok": false, "error": "..."}\\n

Supported query types (passed to engine.query()):
    SHOW nodes                      — all graph nodes
    SHOW edges                      — all graph edges
    SHOW graph                      — nodes + edges together
    SHOW trace <trace_id>           — all events for a trace
    SHOW status                     — engine health/stats
    BLAME WHERE service = "django"  — causal blame query

The server is a single daemon thread — it blocks on accept() in a loop.
Each connection is handled synchronously (one query per connection).
This is a debug/REPL interface, not a high-throughput API.

REPL usage:
    # In a separate terminal
    python -m stacktracer.repl
    # or
    st-repl

    The REPL auto-discovers the socket, connects, and presents
    an interactive prompt where you can type queries.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from typing import Any, Optional

logger = logging.getLogger("stacktracer.local_server")

_SOCKET_DIR  = "/tmp"
_SOCKET_PREFIX = "stacktracer-"
_SOCKET_SUFFIX = ".sock"
_READ_TIMEOUT  = 30.0   # seconds — drop idle connections
_MAX_MSG_BYTES = 65_536  # 64 KB — max query size


def _socket_path(pid: int) -> str:
    return os.path.join(_SOCKET_DIR, f"{_SOCKET_PREFIX}{pid}{_SOCKET_SUFFIX}")


def discover_sockets() -> list[str]:
    """
    Return all live StackTracer sockets in /tmp.
    Called by the REPL to find running agents.
    """
    try:
        return [
            os.path.join(_SOCKET_DIR, f)
            for f in os.listdir(_SOCKET_DIR)
            if f.startswith(_SOCKET_PREFIX) and f.endswith(_SOCKET_SUFFIX)
        ]
    except OSError:
        return []


class LocalQueryServer:
    """
    Unix socket server — one per worker process.

    Starts a daemon thread that accepts connections from the REPL.
    Each connection receives one JSON query, returns one JSON response,
    then closes.

    Thread safety: all engine queries go through engine.query() which
    is already thread-safe (uses internal locks on graph and event_log).
    """

    def __init__(self, engine: Any) -> None:
        self._engine  = engine
        self._pid     = os.getpid()
        self._path    = _socket_path(self._pid)
        self._sock:   Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        # Remove stale socket file if it exists (e.g. after crash)
        self._cleanup_socket()

        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(self._path)
        self._sock.listen(5)
        self._sock.settimeout(1.0)   # allow periodic check of self._running

        self._running = True
        self._thread  = threading.Thread(
            target  = self._serve,
            daemon  = True,
            name    = f"stacktracer-local-server-{self._pid}",
        )
        self._thread.start()
        logger.info("Local query server started at %s", self._path)

    def stop(self) -> None:
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        self._cleanup_socket()
        if self._thread:
            self._thread.join(timeout=2)
        logger.info("Local query server stopped")

    def _cleanup_socket(self) -> None:
        if os.path.exists(self._path):
            try:
                os.unlink(self._path)
            except OSError:
                pass

    def _serve(self) -> None:
        while self._running:
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                self._handle(conn)
            except Exception as exc:
                logger.debug("local server connection error: %s", exc)
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def _handle(self, conn: socket.socket) -> None:
        conn.settimeout(_READ_TIMEOUT)
        raw = b""
        while b"\n" not in raw:
            chunk = conn.recv(4096)
            if not chunk:
                return
            raw += chunk
            if len(raw) > _MAX_MSG_BYTES:
                self._send(conn, {"ok": False, "error": "query too large"})
                return

        line = raw.split(b"\n")[0]
        try:
            msg = json.loads(line.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._send(conn, {"ok": False, "error": f"invalid JSON: {exc}"})
            return

        query_str = msg.get("query", "").strip()
        req_id    = msg.get("id", "")

        if not query_str:
            self._send(conn, {"id": req_id, "ok": False, "error": "empty query"})
            return

        result = self._evaluate(query_str)
        result["id"] = req_id
        self._send(conn, result)

    def _evaluate(self, query_str: str) -> dict:
        """
        Evaluate a query string against the live engine.

        Supports a small set of built-in commands plus forwarding to
        engine.query() for DSL queries.
        """
        q = query_str.upper().strip()

        # ── Built-in commands ──────────────────────────────────────────

        if q == "SHOW NODES":
            nodes = [
                {
                    "id":           n.id,
                    "service":      n.service,
                    "node_type":    n.node_type,
                    "call_count":   n.call_count,
                    "duration_ns":  n.total_duration_ns,
                    "first_seen":   n.first_seen,
                    "last_seen":    n.last_seen,
                }
                for n in self._engine.graph.nodes()
            ]
            return {"ok": True, "data": nodes}

        if q == "SHOW EDGES":
            edges = [
                {"from": e.source_id, "to": e.target_id, "weight": e.weight}
                for e in self._engine.graph.edges()
            ]
            return {"ok": True, "data": edges}

        if q == "SHOW GRAPH":
            nodes = [
                {
                    "id":          n.id,
                    "service":     n.service,
                    "call_count":  n.call_count,
                    "duration_ns": n.total_duration_ns,
                }
                for n in self._engine.graph.nodes()
            ]
            edges = [
                {"from": e.source_id, "to": e.target_id, "weight": e.weight}
                for e in self._engine.graph.edges()
            ]
            return {"ok": True, "data": {"nodes": nodes, "edges": edges}}

        if q == "SHOW STATUS":
            graph   = self._engine.graph
            tracker = getattr(self._engine, "tracker", None)
            return {
                "ok": True,
                "data": {
                    "pid":             self._pid,
                    "socket":          self._path,
                    "uptime_s":        round(time.monotonic() - getattr(self._engine, "_started_at", 0), 1),
                    "graph_nodes":     len(list(graph.nodes())),
                    "graph_edges":     len(list(graph.edges())),
                    "event_log_size":  len(getattr(self._engine, "_event_log", [])),
                    "active_requests": tracker.count() if tracker else 0,
                },
            }

        if q.startswith("SHOW TRACE "):
            trace_id = query_str[len("SHOW TRACE "):].strip()
            events   = [
                {
                    "probe":    e.probe,
                    "service":  e.service,
                    "name":     e.name,
                    "ts":       e.timestamp_ns,
                }
                for e in getattr(self._engine, "_event_log", [])
                if e.trace_id == trace_id
            ]
            return {"ok": True, "data": events}

        if q == "SHOW ACTIVE":
            tracker = getattr(self._engine, "tracker", None)
            if tracker is None:
                return {"ok": True, "data": []}
            active = [
                {"trace_id": t, "started_at": v}
                for t, v in tracker.all().items()
            ]
            return {"ok": True, "data": active}

        # ── DSL parser — handles everything else ──────────────────────
        #
        # This is identical to what the old repl.py did locally:
        #
        #   from stacktracer.query.parser import parse, execute
        #   parsed = parse(raw)
        #   result = execute(parsed, engine)
        #
        # The parser now runs here, inside the worker process, against
        # the live engine.  The REPL just sends the raw string and
        # renders whatever JSON comes back.  No parser code lives in
        # the REPL at all.
        #
        # Supported DSL verbs (handled by parse + execute):
        #   SHOW latency WHERE service = "django"
        #   SHOW latency WHERE system = "export"
        #   SHOW graph
        #   SHOW graph WHERE system = "worker"
        #   SHOW events WHERE probe = "db.query.start" LIMIT 20
        #   HOTSPOT TOP 10
        #   CAUSAL
        #   CAUSAL WHERE tags = "blocking,celery"
        #   BLAME WHERE system = "export"
        #   DIFF SINCE deployment
        #   TRACE <trace_id>

        try:
            from stacktracer.query.parser import parse, execute
            parsed = parse(query_str)
            result = execute(parsed, self._engine)
            return {"ok": True, "data": result}
        except ValueError as exc:
            return {"ok": False, "error": f"Parse error: {exc}"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @staticmethod
    def _send(conn: socket.socket, payload: dict) -> None:
        try:
            data = json.dumps(payload).encode("utf-8") + b"\n"
            conn.sendall(data)
        except OSError:
            pass
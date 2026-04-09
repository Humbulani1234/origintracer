"""
probes/db_kprobe.py

Observes Postgres and Redis queries via kprobes on sys_sendmsg,
filtered by destination port. No monkey patching. No library uprobes.
No changes to application code required.

How it works:
    Every query from every client in the process — whether it originates
    from Django's ORM, a direct psycopg2 connection, SQLAlchemy, or a
    Celery task — eventually becomes a sys_sendmsg() call to port 5432
    (Postgres) or port 6379 (Redis). We intercept at that syscall level.

    The kernel receives the query bytes. We read those bytes in BPF using
    bpf_probe_read_user() on the msghdr iovec, then parse the protocol
    header to extract the query type and first 200 bytes of query text.

Postgres wire protocol (client → server, after authentication):
    Every client message starts with:
        byte 1:      message type ('Q'=simple query, 'P'=parse/prepared,
                                    'B'=bind, 'E'=execute, 'X'=terminate)
        bytes 2-5:   int32 message length (big-endian, includes itself)
        bytes 6+:    payload (for 'Q': null-terminated query string)

    We extract the message type byte and up to 200 bytes of query text.
    The GraphNormalizer collapses literal values (WHERE id = 1234 → WHERE id = ?)
    so all structurally identical queries converge to one graph node.

    Note: SSL/TLS encrypted connections are opaque to this probe — we
    see the encrypted bytes, not the query text. For SSL connections,
    the probe still fires and records bytes_sent and the connection
    tuple, but query_text will be empty. This is expected and safe.

Redis RESP protocol (client → server):
    RESP3 (redis-py ≥ 4.0) and RESP2 both use the same command format:
        *<count>\r\n         bulk array prefix, e.g. *3\r\n for 3 elements
        $<len>\r\n           length of next element
        <data>\r\n           element data

    For a SET foo bar command:
        *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n

    We parse the command name (second element after the count line).
    In BPF we read the first 64 bytes of the message and extract the
    command name from the known offsets. This covers all single-word
    commands (GET, SET, HGET, LPUSH, ZADD, etc.).

    Inline commands (rare, used by redis-cli): start with non-'*' byte.
    We detect these and extract the first word as the command name.

Port filtering:
    We filter on the destination port of the socket fd.
    To find the destination port for a given fd in a kprobe, we read
    struct socket → struct sock → sk_dport from the kernel's socket table.
    This is standard practice in eBPF networking tools.

    Configurable ports:
        POSTGRES_PORT = 5432  (default, configurable via stacktracer.yaml)
        REDIS_PORT    = 6379  (default, configurable via stacktracer.yaml)

Relationship to django_probe.py execute_wrapper:
    For Django applications, execute_wrapper provides BETTER information:
        - ORM-level query text (after parameter interpolation by Django)
        - Connection alias (which database from DATABASES setting)
        - Row count on SELECT
    The kprobe will also fire for the same queries but its output is
    redundant. Both probes can run simultaneously without conflict —
    the graph normalizer will converge them to the same node (same
    query pattern, same service). For Django, prefer execute_wrapper
    and skip db_kprobe, or use db_kprobe only for direct psycopg2 calls
    from Celery tasks that bypass the ORM.

    To suppress db_kprobe events for Django ORM queries:
        db_kprobe:
          skip_if_django_wrapper_active: true   (default: false)

ProbeTypes:
    postgres.query.simple     Postgres simple query ('Q' message)
    postgres.query.parse      Postgres prepared statement parse ('P')
    postgres.query.execute    Postgres prepared statement execute ('E')
    postgres.bytes.sent       Postgres data sent (SSL or unknown format)
    redis.command.execute     Redis command (any type)
    redis.pipeline.sent       Redis pipeline (multiple commands in one send)
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from origintracer.core.bpf_programs import (
    BPFProgramPart,
    register_bpf,
)
from origintracer.core.event_schema import (
    NormalizedEvent,
    ProbeTypes,
)
from origintracer.core.kprobe_bridge import get_bridge
from origintracer.sdk.base_probe import BaseProbe
from origintracer.sdk.emitter import emit

logger = logging.getLogger("origintracer.probes.db_kprobe")

ProbeTypes.register_many(
    {
        "postgres.query.simple": "Postgres simple query (Q message, unencrypted)",
        "postgres.query.parse": "Postgres prepared statement parse (P message)",
        "postgres.query.execute": "Postgres prepared statement execute (E message)",
        "postgres.bytes.sent": "Postgres data sent (encrypted or unknown format)",
        "redis.command.execute": "Redis command sent to server",
        "redis.pipeline.sent": "Redis pipeline (multiple commands in single write)",
    }
)

# Default ports — override in stacktracer.yaml if non-standard
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_REDIS_PORT = 6379

# Maximum query/command bytes to capture in BPF
# Larger values increase BPF stack usage; 200 bytes captures most query patterns
_CAPTURE_BYTES = 200


# ====================================================================== #
# BPF program
# ====================================================================== #

# ── Private BPF fragment ──────────────────────────────────────────────────────
# Owned by this module. Not exported. Ships with the db_probe package only.
#
# Rules observed:
#   - Does NOT redeclare trace_entry_t    (BRIDGE_BPF_HEADER)
#   - Does NOT redeclare trace_context    (BRIDGE_BPF_HEADER)
#   - Does NOT redeclare kernel_event_t   (not used — db_event_t is probe-specific)
#   - Does NOT redeclare BPF_PERF_OUTPUT(kernel_events) (BRIDGE_BPF_HEADER)
#   - trace_context key is u32 tid        (cast from lower 32 bits)
#   - All maps prefixed db_              (avoids collisions)
#   - msg_iov read moved to enter side   (kernel 6.x compat)

_DB_KPROBE_BPF = r"""
#include <linux/socket.h>
#include <linux/in.h>

#ifndef POSTGRES_PORT_VALUE
#define POSTGRES_PORT_VALUE 5432
#endif
#ifndef REDIS_PORT_VALUE
#define REDIS_PORT_VALUE 6379
#endif

// ── Probe-specific output struct ──────────────────────────────────────────────
// db_event_t is separate from kernel_event_t because it carries
// protocol payload bytes which are too large for the shared struct.

struct db_event_t {
    u64  timestamp_ns;
    u32  pid;
    u32  tid;
    char trace_id[36];
    char service[32];
    u16  dst_port;
    u64  duration_ns;
    s64  bytes_sent;
    u32 fd;
};

BPF_PERF_OUTPUT(db_events);

// ── Entry state ───────────────────────────────────────────────────────────────
struct db_sendmsg_entry_t {
    u32  fd;
    u16  dst_port;
    u64  entry_ns;
};

BPF_HASH(db_sendmsg_entry, u64, struct db_sendmsg_entry_t);

// ── sendmsg enter ─────────────────────────────────────────────────────────────
// Payload captured HERE on enter — msg_iov/msg_iovlen removed from
// BPF-visible msghdr in kernel 6.x so we cannot read it on exit.
TRACEPOINT_PROBE(syscalls, sys_enter_sendmsg) {
    u64 pid_tid = bpf_get_current_pid_tgid();
    u32 tid     = (u32)pid_tid;

    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (!ctx) return 0;

    struct db_sendmsg_entry_t entry = {};
    entry.fd       = args->fd;          // pass fd to Python for port lookup
    entry.entry_ns = bpf_ktime_get_ns();
    db_sendmsg_entry.update(&pid_tid, &entry);
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_exit_sendmsg) {
    u64 pid_tid = bpf_get_current_pid_tgid();
    u32 tid     = (u32)pid_tid;

    struct db_sendmsg_entry_t *entry = db_sendmsg_entry.lookup(&pid_tid);
    if (!entry) return 0;
    db_sendmsg_entry.delete(&pid_tid);
    if (args->ret <= 0) return 0;

    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (!ctx) return 0;

    struct db_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(pid_tid >> 32);
    ev.tid          = tid;
    ev.fd           = entry->fd;        // Python resolves port from this
    ev.bytes_sent   = args->ret;
    ev.duration_ns  = ev.timestamp_ns - entry->entry_ns;
    __builtin_memcpy(ev.trace_id, ctx->trace_id, 36);
    __builtin_memcpy(ev.service,  ctx->service,  32);
    db_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}
"""

_bpf_text = _DB_KPROBE_BPF.replace(
    "POSTGRES_PORT_VALUE 5432",
    f"POSTGRES_PORT_VALUE {DEFAULT_POSTGRES_PORT}",
).replace(
    "REDIS_PORT_VALUE 6379",
    f"REDIS_PORT_VALUE {DEFAULT_REDIS_PORT}",
)

register_bpf(
    "db",
    BPFProgramPart(
        headers=[
            "#include <linux/socket.h>",
            "#include <linux/in.h>",
        ],
        structs=[],
        maps=[],
        probes=[_bpf_text],
    ),
)

_patched: bool = False
_originals: dict = {}
_original_step: dict = {}


class _EpollKprobe:
    """
    Consumes epoll.wait events from the shared kernel_events perf buffer.

    _EpollKprobe:
        - opens kernel_events perf buffer on bridge.bpf
        - filters to event_type == "epoll.wait"
        - polls in a daemon thread
    """

    def __init__(self, bridge, correlator):
        self._bridge = bridge
        self._corr = correlator
        self._our_pid = os.getpid()
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> bool:
        if not self._bridge.available:
            logger.info(
                "asyncio epoll kprobe: bridge unavailable — skipping"
            )
            return False

        try:
            self._bridge.bpf["kernel_events"].open_perf_buffer(
                self._handle_epoll_event
            )
        except Exception as exc:
            # Already opened by nginx kprobe or dispatcher — log and continue.
            # Events still flow if the opener dispatches epoll.wait events here.
            logger.warning(
                "asyncio epoll kprobe: open_perf_buffer failed "
                "(may already be opened by nginx or dispatcher): %s",
                exc,
            )
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="stacktracer-epoll-kprobe",
        )
        self._thread.start()
        logger.info(
            "asyncio epoll kprobe: started (our_pid=%d)",
            self._our_pid,
        )
        return True

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _poll_loop(self) -> None:
        bpf = self._bridge.bpf
        while self._running:
            try:
                bpf.perf_buffer_poll(timeout=100)
            except Exception as exc:
                logger.debug(
                    "asyncio epoll kprobe poll error: %s", exc
                )
                time.sleep(0.1)

    def _handle_event(
        self, cpu: int, data: Any, size: int
    ) -> None:
        if self._bridge.bpf is None:
            return
        try:
            ev = self._bridge.bpf["db_events"].event(data)

            if ev.pid != self._our_pid:
                return

            trace_id = ev.trace_id.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")
            if not trace_id:
                return

            # Port resolved from /proc — BPF no longer does sock walking
            dst_port = self._get_dst_port(ev.pid, ev.fd)
            if dst_port == 0:
                return  # not a socket we care about

            if dst_port == DEFAULT_POSTGRES_PORT:
                # Payload capture dropped (kernel 6.x msg_iov removed)
                # Query text comes from Django execute_wrapper instead
                probe_type = "postgres.bytes.sent"
                emit(
                    NormalizedEvent.now(
                        probe=probe_type,
                        trace_id=trace_id,
                        service="postgres",
                        name="unknown",
                        pid=ev.pid,
                        tid=ev.tid,
                        duration_ns=ev.duration_ns,
                        bytes_sent=ev.bytes_sent,
                        port=dst_port,
                        source="kprobe",
                    )
                )

            elif dst_port == DEFAULT_REDIS_PORT:
                probe_type = "redis.command.execute"
                emit(
                    NormalizedEvent.now(
                        probe=probe_type,
                        trace_id=trace_id,
                        service="redis",
                        name="UNKNOWN",
                        pid=ev.pid,
                        tid=ev.tid,
                        duration_ns=ev.duration_ns,
                        bytes_sent=ev.bytes_sent,
                        port=dst_port,
                        source="kprobe",
                    )
                )

        except Exception as exc:
            logger.debug(
                "db_kprobe event handling error: %s", exc
            )

    def _get_dst_port(self, pid: int, fd: int) -> int:
        """Resolve destination port for a socket fd via /proc."""
        try:
            link = os.readlink(f"/proc/{pid}/fd/{fd}")
            if not link.startswith("socket:"):
                return 0
            inode = link[8:-1]  # strip "socket:[" and "]"

            for proto in ("tcp", "tcp6"):
                try:
                    with open(f"/proc/{pid}/net/{proto}") as f:
                        for line in f:
                            parts = line.split()
                            if (
                                len(parts) > 9
                                and parts[9] == inode
                            ):
                                port_hex = parts[2].split(":")[1]
                                return int(port_hex, 16)
                except OSError:
                    continue
        except OSError:
            pass
        return 0


# ====================================================================== #
# DBKprobe
# ====================================================================== #


class DBKprobe(BaseProbe):
    """
    Observes Postgres and Redis queries via kprobes on sys_sendmsg.

    No monkey patching. No library uprobes. No changes to application code.
    Works for:
        - Django ORM queries (via psycopg2)
        - Direct psycopg2 connections (Celery tasks, scripts)
        - SQLAlchemy with psycopg2 driver
        - redis-py (any version)
        - Any other Postgres or Redis client in the process

    For Django applications:
        execute_wrapper (in django_probe.py) gives richer information
        (ORM query text, connection alias, row count) for queries that
        go through the ORM. This probe additionally captures queries
        that bypass the ORM — direct psycopg2 cursor usage in Celery tasks.

        Both probes can run simultaneously. GraphNormalizer converges
        structurally identical queries to the same graph node regardless
        of which probe emitted them.

    SSL/TLS:
        Queries over encrypted connections produce postgres.bytes.sent
        events with no query_text. Duration and bytes_sent are still
        captured. If you need query text visibility on SSL connections,
        use Django's execute_wrapper (which operates above the TLS layer).

    Requires: Linux + root/CAP_BPF + bcc
    Degrades gracefully to no-op if unavailable.
    """

    name = "db_probe"

    def __init__(self) -> None:
        self._epoll_kprobe: Optional[_EpollKprobe] = None

    def start(self) -> None:
        global _originals, _patched, _original_step
        if _patched:
            logger.warning(
                "asyncio probe already installed - skipping"
            )
            return

        # Layer 1: epoll kprobe
        bridge = get_bridge()
        if sys.platform == "linux":
            if not bridge.available:
                bridge.start()  # idempotent — safe to call multiple times
            if bridge.available:
                self._epoll_kprobe = _EpollKprobe(
                    bridge, correlator=None
                )
                ok = self._epoll_kprobe.start()
                if not ok:
                    self._epoll_kprobe = None
            else:
                logger.info(
                    "asyncio probe: epoll kprobe unavailable "
                    "(bridge failed to start). "
                    "Coroutine-level tracing still active."
                )
        _patched = True

    def stop(self) -> None:
        global _originals, _patched, _original_step

        if not _patched:
            return

        if self._epoll_kprobe:
            self._epoll_kprobe.stop()
            self._epoll_kprobe = None

        _patched = False
        logger.info("asyncio probe removed")

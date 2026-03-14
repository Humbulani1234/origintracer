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

import logging
import os
import socket
import struct
import threading
from typing import Dict, List, Optional, Tuple

from stacktracer.sdk.base_probe import BaseProbe
from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import (
    NormalizedEvent,
    ProbeTypes,
)
from stacktracer.core.kprobe_bridge import get_bridge

logger = logging.getLogger("stacktracer.probes.db_kprobe")

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

_DB_KPROBE_BPF = r"""
#include <uapi/linux/ptrace.h>
#include <linux/socket.h>
#include <linux/in.h>
#include <linux/tcp.h>
#include <net/sock.h>
#include <uapi/linux/if_ether.h>
#include <uapi/linux/ip.h>

// ── Configuration (populated by Python before load) ──────────────────
// These are compile-time constants injected via BPF_CFLAGS.
// Python replaces POSTGRES_PORT_VALUE and REDIS_PORT_VALUE before
// compiling the BPF program.
#ifndef POSTGRES_PORT_VALUE
#define POSTGRES_PORT_VALUE 5432
#endif
#ifndef REDIS_PORT_VALUE
#define REDIS_PORT_VALUE 6379
#endif

// ── Shared trace context from KprobeBridge ────────────────────────────

struct trace_entry_t {
    char trace_id[36];
    u64  start_ns;
    char service[32];
    u32  pid;
    u32  tid;
};

BPF_HASH(trace_context, u64, struct trace_entry_t, 65536);

// ── Entry state for sys_sendmsg ───────────────────────────────────────
// We need to save msghdr pointer at entry because at return we only
// have the return value. We also save the destination port discovered
// at entry from the socket fd.

struct sendmsg_entry_t {
    u64   msg_ptr;      // struct msghdr __user *
    u16   dst_port;     // destination port (host byte order)
    u64   entry_ns;
};

BPF_HASH(sendmsg_entry, u64, struct sendmsg_entry_t);   // tid → entry

// ── Output event ─────────────────────────────────────────────────────

struct db_event_t {
    u64   timestamp_ns;
    u32   pid;
    u32   tid;
    char  trace_id[36];
    char  service[32];     // "postgres" or "redis"
    u16   dst_port;
    u64   duration_ns;
    s64   bytes_sent;

    // First CAPTURE_BYTES of the message payload for protocol parsing
    // Python-side does the protocol parsing — not BPF (simpler, safer)
    char  payload[200];
    u32   payload_len;     // actual captured length (≤ 200)
};

BPF_PERF_OUTPUT(db_events);

// ── Port lookup from socket fd ────────────────────────────────────────
// Walk: fd → file → socket → sock → sk_dport
// This is a standard eBPF pattern used by tcptrace, bpftrace tcp_*, etc.

static inline u16 get_dst_port_for_fd(u32 fd) {
    struct task_struct *task = (struct task_struct *)bpf_get_current_task();
    if (!task) return 0;

    struct files_struct *files = NULL;
    bpf_probe_read_kernel(&files, sizeof(files), &task->files);
    if (!files) return 0;

    struct fdtable *fdt = NULL;
    bpf_probe_read_kernel(&fdt, sizeof(fdt), &files->fdt);
    if (!fdt) return 0;

    struct file **file_arr = NULL;
    bpf_probe_read_kernel(&file_arr, sizeof(file_arr), &fdt->fd);
    if (!file_arr) return 0;

    struct file *filep = NULL;
    bpf_probe_read_kernel(&filep, sizeof(filep), &file_arr[fd]);
    if (!filep) return 0;

    struct socket *sockp = NULL;
    bpf_probe_read_kernel(&sockp, sizeof(sockp), &filep->private_data);
    if (!sockp) return 0;

    struct sock *sk = NULL;
    bpf_probe_read_kernel(&sk, sizeof(sk), &sockp->sk);
    if (!sk) return 0;

    u16 dport_be = 0;
    bpf_probe_read_kernel(&dport_be, sizeof(dport_be), &sk->sk_dport);

    // sk_dport is in network byte order — convert to host byte order
    return (dport_be >> 8) | ((dport_be & 0xff) << 8);
}

// ── sys_sendmsg entry: save fd, message ptr, discover port ───────────

TRACEPOINT_PROBE(syscalls, sys_enter_sendmsg) {
    u64 tid = bpf_get_current_pid_tgid();

    // Only trace threads with active Python trace context
    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (!ctx) return 0;

    u16 dst_port = get_dst_port_for_fd(args->fd);
    if (dst_port != POSTGRES_PORT_VALUE && dst_port != REDIS_PORT_VALUE) {
        return 0;   // not a database port — skip
    }

    struct sendmsg_entry_t entry = {
        .msg_ptr  = (u64)args->msg,
        .dst_port = dst_port,
        .entry_ns = bpf_ktime_get_ns(),
    };
    sendmsg_entry.update(&tid, &entry);
    return 0;
}

// ── sys_sendmsg return: read iovec payload, emit event ───────────────

TRACEPOINT_PROBE(syscalls, sys_exit_sendmsg) {
    u64 tid = bpf_get_current_pid_tgid();

    struct sendmsg_entry_t *entry = sendmsg_entry.lookup(&tid);
    if (!entry) return 0;
    sendmsg_entry.delete(&tid);

    s64 bytes_sent = args->ret;
    if (bytes_sent <= 0) return 0;   // sendmsg failed

    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (!ctx) return 0;

    struct db_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(tid >> 32);
    ev.tid          = (u32)tid;
    ev.dst_port     = entry->dst_port;
    ev.bytes_sent   = bytes_sent;
    ev.duration_ns  = ev.timestamp_ns - entry->entry_ns;
    __builtin_memcpy(ev.trace_id, ctx->trace_id, 36);
    __builtin_memcpy(ev.service,  ctx->service,  32);

    // Read first 200 bytes of the message payload via iovec
    // msghdr.msg_iov → iovec[0].iov_base is the payload start
    struct msghdr msg = {};
    struct iovec  iov = {};
    if (bpf_probe_read_user(&msg, sizeof(msg), (void *)entry->msg_ptr) == 0) {
        if (msg.msg_iov && msg.msg_iovlen > 0) {
            if (bpf_probe_read_user(&iov, sizeof(iov), msg.msg_iov) == 0) {
                if (iov.iov_base && iov.iov_len > 0) {
                    u32 cap = iov.iov_len < 200 ? iov.iov_len : 200;
                    bpf_probe_read_user(ev.payload, cap, iov.iov_base);
                    ev.payload_len = cap;
                }
            }
        }
    }

    db_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}
"""


# ====================================================================== #
# Python-side protocol parsing
# ====================================================================== #
# We do protocol parsing in Python, not BPF.
# BPF captures raw bytes; Python extracts meaning.
# Easier to maintain, easier to test, no BPF stack size concerns.


def _parse_postgres(
    payload: bytes, payload_len: int
) -> Tuple[str, str]:
    """
    Parse the Postgres wire protocol frontend message.
    Returns (probe_type, query_text).

    Frontend message format (after authentication):
        byte[0]:   message type
        byte[1-4]: int32 message length (big-endian, includes itself)
        byte[5+]:  payload

    Returns empty query_text for SSL/encrypted connections where
    the first byte is not a known Postgres message type byte.
    """
    if payload_len < 5:
        return "postgres.bytes.sent", ""

    msg_type = chr(payload[0]) if payload[0] >= 32 else ""

    if msg_type == "Q":
        # Simple query: payload[5:] is null-terminated query string
        raw = payload[5:payload_len]
        query = raw.split(b"\x00")[0].decode(
            "utf-8", errors="replace"
        )
        return "postgres.query.simple", query

    elif msg_type == "P":
        # Parse (prepared statement):
        # byte[5]: null-terminated statement name
        # then: null-terminated query string
        raw = payload[5:payload_len]
        parts = raw.split(b"\x00", 2)
        query = (
            parts[1].decode("utf-8", errors="replace")
            if len(parts) > 1
            else ""
        )
        return "postgres.query.parse", query

    elif msg_type == "E":
        # Execute (prepared statement): named portal being executed
        raw = payload[5:payload_len]
        portal = raw.split(b"\x00")[0].decode(
            "utf-8", errors="replace"
        )
        return "postgres.query.execute", f"EXECUTE {portal}"

    elif msg_type == "X":
        # Terminate — not interesting for causal analysis
        return "postgres.bytes.sent", ""

    else:
        # SSL, unknown, or startup message — record bytes only
        return "postgres.bytes.sent", ""


def _parse_redis(
    payload: bytes, payload_len: int
) -> Tuple[str, str]:
    """
    Parse the Redis RESP protocol.
    Returns (probe_type, command_name).

    RESP2/3 format for commands:
        *<count>\r\n          bulk array start
        $<len>\r\n            first element (command name) length
        <command>\r\n         command name bytes (GET, SET, HGET, ...)
        ...

    Example: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
    """
    if payload_len < 4:
        return "redis.command.execute", ""

    try:
        text = payload[:payload_len].decode(
            "utf-8", errors="replace"
        )

        if text[0] == "*":
            # Bulk array format — standard redis-py encoding
            lines = text.split("\r\n")
            # Count line: *3 → 3 elements
            count_str = lines[0][1:]
            count = int(count_str) if count_str.isdigit() else 0

            if count >= 2:
                # Pipeline: multiple commands in one write
                # We take the first command name as representative
                probe_type = (
                    "redis.pipeline.sent"
                    if count > 2
                    else "redis.command.execute"
                )
            else:
                probe_type = "redis.command.execute"

            # lines[1] = $<len>, lines[2] = command name
            if len(lines) >= 3:
                command = lines[2].strip().upper()
                return probe_type, command
            return probe_type, ""

        else:
            # Inline command format: "SET foo bar\r\n"
            command = (
                text.split()[0].upper() if text.strip() else ""
            )
            return "redis.command.execute", command

    except Exception:
        return "redis.command.execute", ""


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

    def __init__(
        self,
        postgres_port: int = DEFAULT_POSTGRES_PORT,
        redis_port: int = DEFAULT_REDIS_PORT,
    ) -> None:
        self._postgres_port = postgres_port
        self._redis_port = redis_port
        self._bpf = None
        self._thread = None
        self._running = False
        self._our_pid = os.getpid()

    def start(self) -> None:
        bridge = get_bridge()
        if not bridge.available:
            logger.info(
                "db_probe: KprobeBridge not available — "
                "Postgres/Redis kprobes inactive. "
                "Django execute_wrapper still provides ORM query visibility."
            )
            return

        try:
            from bcc import BPF
        except ImportError:
            logger.info(
                "db_kprobe: bcc not installed — inactive"
            )
            return

        # Inject port constants into BPF at compile time
        cflags = [
            f"-DPOSTGRES_PORT_VALUE={self._postgres_port}",
            f"-DREDIS_PORT_VALUE={self._redis_port}",
        ]

        try:
            self._bpf = BPF(text=_DB_KPROBE_BPF, cflags=cflags)
        except Exception as exc:
            logger.warning(
                "db_probe BPF compile failed: %s", exc
            )
            return

        self._bpf["db_events"].open_perf_buffer(
            self._handle_event
        )
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="stacktracer-db-kprobe",
        )
        self._thread.start()

        logger.info(
            "db_probe active — observing port %d (postgres) and port %d (redis) "
            "via sys_sendmsg tracepoint",
            self._postgres_port,
            self._redis_port,
        )

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        self._bpf = None
        logger.info("db_probe stopped")

    def _poll_loop(self) -> None:
        while self._running and self._bpf:
            try:
                self._bpf.perf_buffer_poll(timeout=100)
            except Exception as exc:
                logger.debug("db_probe poll error: %s", exc)

    def _handle_event(self, cpu: int, data, size: int) -> None:
        if self._bpf is None:
            return
        try:
            ev = self._bpf["db_events"].event(data)

            if ev.pid != self._our_pid:
                return  # ignore other processes

            trace_id = ev.trace_id.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")
            if not trace_id:
                return

            payload = bytes(ev.payload[: ev.payload_len])
            payload_len = ev.payload_len
            dst_port = ev.dst_port

            if dst_port == self._postgres_port:
                probe_type, query_text = _parse_postgres(
                    payload, payload_len
                )
                emit(
                    NormalizedEvent.now(
                        probe=probe_type,
                        trace_id=trace_id,
                        service="postgres",
                        # name is the query text — GraphNormalizer collapses literals
                        name=(
                            query_text[:200]
                            if query_text
                            else "unknown"
                        ),
                        pid=ev.pid,
                        tid=ev.tid,
                        duration_ns=ev.duration_ns,
                        bytes_sent=ev.bytes_sent,
                        port=dst_port,
                        source="kprobe",
                        encrypted=(
                            probe_type == "postgres.bytes.sent"
                            and not query_text
                        ),
                    )
                )

            elif dst_port == self._redis_port:
                probe_type, command = _parse_redis(
                    payload, payload_len
                )
                emit(
                    NormalizedEvent.now(
                        probe=probe_type,
                        trace_id=trace_id,
                        service="redis",
                        name=command or "UNKNOWN",
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

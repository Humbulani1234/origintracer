"""
probes/nginx_probe.py

nginx probe: kprobes + Lua UDP receiver, working together.

What each layer contributes to the causal graph
------------------------------------------------
kprobe layer (syscalls, no nginx binary dependency):
    - Connection accepted: fd number, client IP, duration of accept4()
    - Epoll tick: how long nginx waited in epoll_wait, how many fds woke
    - Bytes in/out: volume of data nginx read and wrote per connection
    - ALL of this fires at the exact kernel boundary — not an approximation

Lua layer (OpenResty log_by_lua_block, post-response):
    - HTTP semantics: method, URI (parsed by nginx C code), status code
    - Upstream timing: how long the upstream (uvicorn) took to respond
    - Request ID: the correlation key shared with uvicorn and Python layers
    - Variables only nginx knows after parsing: $host, $upstream_addr

What neither layer alone can give you:
    kprobe alone: bytes and timing, but "GET /api/users/{id}" is invisible
    Lua alone: URI and status, but no connection lifecycle or epoll insight

Combined in NginxRequestRecord:
    One record per request_id accumulates events from both layers.
    When both kprobe connection data AND Lua HTTP data arrive for the
    same request_id, the engine has the complete picture:

        nginx accepted fd=7 from 10.0.0.1:52341
        → nginx epoll_wait returned 3 times (loop ticks) while processing
        → nginx forwarded to upstream 127.0.0.1:8000 (uvicorn)
        → upstream responded in 43ms
        → nginx sent 2.4KB response, status 200
        → total wall time 45ms

    trace_id = request_id = shared with uvicorn X-Request-ID header
    → this nginx record connects to the uvicorn + django graph nodes

Correlation chain:
    nginx sets:     $request_id (built-in nginx variable, unique per request)
    nginx forwards: proxy_set_header X-Request-ID $request_id;
    uvicorn reads:  scope["headers"][b"x-request-id"]
    django reads:   request.META["HTTP_X_REQUEST_ID"]
    All layers:     use request_id as trace_id
    Result:         one causal chain from nginx accept → django response

Lua emit path:
    log_by_lua_block emits UDP JSON to 127.0.0.1:9999 (configurable).
    UDP is non-blocking — log phase cannot be delayed by a slow receiver.
    The Python probe runs a background UDP socket thread.

Mode selection (auto):
    - kprobe mode: Linux + root/CAP_BPF + bcc + nginx running
    - Lua mode: OpenResty installed + stacktracer_nginx.lua configured
    - Log-tail: fallback when neither above is available
    Any combination of all three can run simultaneously.

New ProbeTypes:
    nginx.request.semantic    HTTP-level event from Lua (URI, status, upstream_ms)
    nginx.request.combined    Merged kprobe+Lua record (richest, one per request)
    (existing kprobe types unchanged)
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import struct
import sys
import threading
import time
from collections import OrderedDict
from typing import Dict, List, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..core.kprobe_bridge import get_bridge

logger = logging.getLogger("stacktracer.probes.nginx")

ProbeTypes.register_many({
    "nginx.connection.accept":    "nginx worker accepted a new client connection",
    "nginx.connection.data_in":   "nginx received request bytes from client",
    "nginx.connection.data_out":  "nginx sent response bytes to client",
    "nginx.epoll.tick":           "nginx epoll_wait returned with ready events",
    "nginx.request.complete":     "nginx request completed (from access log)",
    "nginx.request.semantic":     "HTTP-level request data from Lua (URI, status, upstream)",
    "nginx.request.combined":     "Complete request record: kprobe connection + Lua HTTP semantics",
})

_NGINX_TRACE_PREFIX = "nginx-"
_COMBINED_RECORD_TTL_S = 30.0     # drop incomplete records after 30s
_COMBINED_RECORD_MAX   = 10_000   # cap in-flight records


# ====================================================================== #
# NginxRequestRecord — accumulates kprobe and Lua data for one request
# ====================================================================== #

class NginxRequestRecord:
    """
    Accumulates connection-level (kprobe) and HTTP-level (Lua) events
    for a single nginx request, identified by request_id.

    The record becomes "complete" when both layers have contributed.
    An incomplete record is still emitted — partial data is better
    than no data. TTL eviction cleans up requests where one layer
    never fired (e.g. connection dropped before Lua log phase).
    """

    __slots__ = (
        "request_id", "created_at",
        # kprobe fields
        "conn_key", "client_ip", "client_port", "fd",
        "accept_duration_ns", "bytes_in", "bytes_out",
        "epoll_ticks", "epoll_total_duration_ns",
        "kprobe_done",
        # Lua fields
        "method", "uri", "status_code",
        "upstream_ms", "upstream_addr",
        "total_ms", "lua_done",
    )

    def __init__(self, request_id: str) -> None:
        self.request_id            = request_id
        self.created_at            = time.monotonic()
        self.conn_key              = ""
        self.client_ip             = ""
        self.client_port           = 0
        self.fd                    = 0
        self.accept_duration_ns    = 0
        self.bytes_in              = 0
        self.bytes_out             = 0
        self.epoll_ticks           = 0
        self.epoll_total_duration_ns = 0
        self.kprobe_done           = False
        self.method                = ""
        self.uri                   = ""
        self.status_code           = 0
        self.upstream_ms           = 0.0
        self.upstream_addr         = ""
        self.total_ms              = 0.0
        self.lua_done              = False

    @property
    def is_complete(self) -> bool:
        return self.kprobe_done and self.lua_done

    def emit_combined(self) -> None:
        emit(NormalizedEvent.now(
            probe="nginx.request.combined",
            trace_id=self.request_id,
            service="nginx",
            name=self.uri or "unknown",
            method=self.method,
            status_code=self.status_code,
            # kprobe data
            client_ip=self.client_ip,
            client_port=self.client_port,
            fd=self.fd,
            bytes_in=self.bytes_in,
            bytes_out=self.bytes_out,
            epoll_ticks=self.epoll_ticks,
            epoll_total_ms=round(self.epoll_total_duration_ns / 1e6, 3),
            accept_duration_ns=self.accept_duration_ns,
            # Lua data
            upstream_ms=self.upstream_ms,
            upstream_addr=self.upstream_addr,
            total_ms=self.total_ms,
            # completeness flags
            has_kprobe=self.kprobe_done,
            has_lua=self.lua_done,
        ))


class _RequestRegistry:
    """
    Thread-safe LRU registry of in-flight NginxRequestRecords.
    Keyed by request_id. Both the kprobe handler thread and the
    Lua UDP receiver thread write into this registry.
    """

    def __init__(self, max_size: int = _COMBINED_RECORD_MAX) -> None:
        self._records: OrderedDict[str, NginxRequestRecord] = OrderedDict()
        self._lock    = threading.Lock()
        self._max     = max_size

    def get_or_create(self, request_id: str) -> NginxRequestRecord:
        with self._lock:
            if request_id in self._records:
                self._records.move_to_end(request_id)
                return self._records[request_id]
            rec = NginxRequestRecord(request_id)
            self._records[request_id] = rec
            if len(self._records) > self._max:
                self._records.popitem(last=False)   # evict oldest
            return rec

    def remove(self, request_id: str) -> None:
        with self._lock:
            self._records.pop(request_id, None)

    def evict_stale(self) -> int:
        cutoff = time.monotonic() - _COMBINED_RECORD_TTL_S
        with self._lock:
            stale = [
                rid for rid, rec in self._records.items()
                if rec.created_at < cutoff
            ]
            for rid in stale:
                del self._records[rid]
        return len(stale)


# Global registry — shared between kprobe handler and Lua receiver
_registry = _RequestRegistry()


# ====================================================================== #
# nginx pid discovery (unchanged from previous version)
# ====================================================================== #

def _find_nginx_pids() -> List[int]:
    pids = []
    for pid_file in ["/var/run/nginx.pid", "/run/nginx.pid"]:
        if os.path.exists(pid_file):
            try:
                with open(pid_file) as f:
                    master_pid = int(f.read().strip())
                children = _children_of(master_pid)
                pids.extend(children if children else [master_pid])
                return pids
            except (ValueError, OSError):
                pass
    try:
        for entry in os.scandir("/proc"):
            if not entry.name.isdigit():
                continue
            try:
                with open(f"/proc/{entry.name}/comm") as f:
                    if f.read().strip() == "nginx":
                        pids.append(int(entry.name))
            except (OSError, ValueError):
                continue
    except OSError:
        pass
    return pids


def _children_of(parent_pid: int) -> List[int]:
    children = []
    try:
        for entry in os.scandir("/proc"):
            if not entry.name.isdigit():
                continue
            try:
                with open(f"/proc/{entry.name}/status") as f:
                    for line in f:
                        if line.startswith("PPid:"):
                            if int(line.split()[1]) == parent_pid:
                                children.append(int(entry.name))
                            break
            except (OSError, ValueError):
                continue
    except OSError:
        pass
    return children


def _ip_to_str(ip_be: int) -> str:
    return socket.inet_ntoa(struct.pack("!I", socket.ntohl(ip_be)))


# ====================================================================== #
# kprobe BPF program (unchanged — robust, no nginx binary dependency)
# ====================================================================== #

_NGINX_BPF_PROGRAM = r"""
#include <uapi/linux/ptrace.h>
#include <linux/in.h>
#include <linux/socket.h>

BPF_HASH(nginx_pids, u32, u8);

static inline int is_nginx_pid() {
    u32 pid = bpf_get_current_pid_tgid() >> 32;
    return nginx_pids.lookup(&pid) != NULL;
}

struct nginx_event_t {
    u64 timestamp_ns;
    u32 pid;
    u32 tid;
    char event_type[24];
    s64 value1;
    s64 value2;
    u64 duration_ns;
    u32 client_ip;
    u16 client_port;
    u16 server_port;
};

BPF_PERF_OUTPUT(nginx_events);

BPF_HASH(accept_ts, u64, u64);
BPF_HASH(epoll_ts, u64, u64);

TRACEPOINT_PROBE(syscalls, sys_enter_accept4) {
    if (!is_nginx_pid()) return 0;
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts  = bpf_ktime_get_ns();
    accept_ts.update(&tid, &ts);
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_exit_accept4) {
    if (!is_nginx_pid()) return 0;
    int fd = args->ret;
    if (fd < 0) return 0;
    u64 tid = bpf_get_current_pid_tgid();
    u64 *start = accept_ts.lookup(&tid);
    if (!start) return 0;
    accept_ts.delete(&tid);
    struct nginx_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(tid >> 32);
    ev.tid          = (u32)tid;
    ev.duration_ns  = ev.timestamp_ns - *start;
    ev.value1       = fd;
    __builtin_memcpy(ev.event_type, "accept", 7);
    struct sockaddr_in addr = {};
    if (args->upeer_sockaddr) {
        bpf_probe_read_user(&addr, sizeof(addr), args->upeer_sockaddr);
        if (addr.sin_family == 2) {
            ev.client_ip   = addr.sin_addr.s_addr;
            ev.client_port = addr.sin_port;
        }
    }
    nginx_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_enter_epoll_wait) {
    if (!is_nginx_pid()) return 0;
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts  = bpf_ktime_get_ns();
    epoll_ts.update(&tid, &ts);
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_exit_epoll_wait) {
    if (!is_nginx_pid()) return 0;
    int n = args->ret;
    if (n <= 0) return 0;
    u64 tid = bpf_get_current_pid_tgid();
    u64 *start = epoll_ts.lookup(&tid);
    if (!start) return 0;
    epoll_ts.delete(&tid);
    struct nginx_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(tid >> 32);
    ev.tid          = (u32)tid;
    ev.duration_ns  = ev.timestamp_ns - *start;
    ev.value1       = n;
    __builtin_memcpy(ev.event_type, "epoll", 6);
    nginx_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_enter_sendmsg) {
    if (!is_nginx_pid()) return 0;
    u64 tid = bpf_get_current_pid_tgid();
    struct nginx_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(tid >> 32);
    ev.tid          = (u32)tid;
    ev.value2       = args->fd;
    __builtin_memcpy(ev.event_type, "data_out", 9);
    struct iovec iov = {};
    struct msghdr msg = {};
    if (args->msg) {
        bpf_probe_read_user(&msg, sizeof(msg), args->msg);
        if (msg.msg_iov && msg.msg_iovlen > 0) {
            bpf_probe_read_user(&iov, sizeof(iov), msg.msg_iov);
            ev.value1 = iov.iov_len;
        }
    }
    nginx_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_exit_recvmsg) {
    if (!is_nginx_pid()) return 0;
    s64 bytes = args->ret;
    if (bytes <= 0) return 0;
    u64 tid = bpf_get_current_pid_tgid();
    struct nginx_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(tid >> 32);
    ev.tid          = (u32)tid;
    ev.value1       = bytes;
    __builtin_memcpy(ev.event_type, "data_in", 8);
    nginx_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}
"""


class _NginxKprobeMode:
    """
    kprobe layer: observes nginx at the syscall level.
    Now writes into _RequestRegistry rather than emitting directly.
    Still emits individual kprobe events AND contributes to combined records.

    The combined record needs a request_id to correlate with Lua.
    nginx workers process one connection per worker thread. We use
    pid-tid as a connection key: one worker thread handles one connection
    at a time (nginx event-driven worker), so pid-tid uniquely identifies
    the in-flight connection at any moment.
    """

    def __init__(self, registry: _RequestRegistry) -> None:
        self._registry = registry
        self._bpf      = None
        self._thread   = None
        self._running  = False
        self._pids: List[int] = []
        # pid-tid → request_id (set when Lua or log-tail gives us the request_id)
        # Without this mapping, kprobe events use pid-tid as trace_id
        self._conn_to_request: Dict[str, str] = {}

    def register_request_id(self, pid: int, tid: int, request_id: str) -> None:
        """Called by Lua receiver when it knows the request_id for a pid-tid."""
        self._conn_to_request[f"{pid}-{tid}"] = request_id

    def start(self) -> bool:
        try:
            from bcc import BPF
        except ImportError:
            logger.info("nginx kprobe: bcc not installed")
            return False
        if os.geteuid() != 0:
            logger.info("nginx kprobe: requires root/CAP_BPF")
            return False
        self._pids = _find_nginx_pids()
        if not self._pids:
            logger.warning("nginx kprobe: no nginx processes found")
            return False
        try:
            self._bpf = BPF(text=_NGINX_BPF_PROGRAM)
        except Exception as exc:
            logger.warning("nginx BPF compile: %s", exc)
            return False
        pid_map = self._bpf["nginx_pids"]
        for pid in self._pids:
            k = pid_map.Key(pid)
            v = pid_map.Leaf(1)
            pid_map[k] = v
        self._bpf["nginx_events"].open_perf_buffer(self._handle_event)
        self._running = True
        self._thread  = threading.Thread(
            target=self._poll_loop, daemon=True, name="stacktracer-nginx-kprobe"
        )
        self._thread.start()
        logger.info("nginx kprobe active for pids: %s", self._pids)
        return True

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        self._bpf = None

    def _poll_loop(self) -> None:
        while self._running and self._bpf:
            try:
                self._bpf.perf_buffer_poll(timeout=100)
            except Exception as exc:
                logger.debug("nginx kprobe poll: %s", exc)

    def _handle_event(self, cpu, data, size) -> None:
        if self._bpf is None:
            return
        try:
            ev    = self._bpf["nginx_events"].event(data)
            etype = ev.event_type.decode("ascii", errors="replace").rstrip("\x00")

            conn_key   = f"{ev.pid}-{ev.tid}"
            request_id = self._conn_to_request.get(conn_key, f"{_NGINX_TRACE_PREFIX}{conn_key}")

            if etype == "accept":
                client_ip   = _ip_to_str(ev.client_ip) if ev.client_ip else ""
                client_port = socket.ntohs(ev.client_port) if ev.client_port else 0

                # Emit individual kprobe event
                emit(NormalizedEvent.now(
                    probe="nginx.connection.accept",
                    trace_id=request_id,
                    service="nginx",
                    name="accept",
                    pid=ev.pid,
                    fd=ev.value1,
                    client_ip=client_ip,
                    client_port=client_port,
                    duration_ns=ev.duration_ns,
                    source="kprobe",
                ))

                # Update combined record
                rec = self._registry.get_or_create(request_id)
                rec.conn_key           = conn_key
                rec.client_ip          = client_ip
                rec.client_port        = client_port
                rec.fd                 = int(ev.value1)
                rec.accept_duration_ns = ev.duration_ns

            elif etype == "epoll":
                emit(NormalizedEvent.now(
                    probe="nginx.epoll.tick",
                    trace_id=request_id,
                    service="nginx",
                    name="epoll_wait",
                    pid=ev.pid,
                    n_events=ev.value1,
                    duration_ns=ev.duration_ns,
                    source="kprobe",
                ))
                rec = self._registry.get_or_create(request_id)
                rec.epoll_ticks             += 1
                rec.epoll_total_duration_ns += ev.duration_ns

            elif etype == "data_out":
                emit(NormalizedEvent.now(
                    probe="nginx.connection.data_out",
                    trace_id=request_id,
                    service="nginx",
                    name="sendmsg",
                    pid=ev.pid,
                    bytes_sent=ev.value1,
                    source="kprobe",
                ))
                rec = self._registry.get_or_create(request_id)
                rec.bytes_out += max(0, int(ev.value1))
                # Mark kprobe side done when we see data going out
                if not rec.kprobe_done:
                    rec.kprobe_done = True
                    if rec.is_complete:
                        rec.emit_combined()
                        self._registry.remove(request_id)

            elif etype == "data_in":
                emit(NormalizedEvent.now(
                    probe="nginx.connection.data_in",
                    trace_id=request_id,
                    service="nginx",
                    name="recvmsg",
                    pid=ev.pid,
                    bytes_received=ev.value1,
                    source="kprobe",
                ))
                rec = self._registry.get_or_create(request_id)
                rec.bytes_in += max(0, int(ev.value1))

        except Exception as exc:
            logger.debug("nginx kprobe event error: %s", exc)


# ====================================================================== #
# Lua UDP receiver — the HTTP semantics layer
# ====================================================================== #

class _NginxLuaReceiver:
    """
    UDP socket server that receives JSON datagrams from Lua log_by_lua_block.

    Lua emits one UDP packet per request at the end of the log phase.
    The packet is a JSON object with HTTP-level fields that only nginx
    knows after its C parsing code has run.

    Expected JSON schema (emitted by stacktracer_nginx.lua):
    {
      "request_id": "abc-123",          ← ngx.var.request_id
      "method":     "GET",              ← ngx.var.request_method
      "uri":        "/api/users/42/",   ← ngx.var.uri
      "status":     200,                ← ngx.status
      "total_ms":   45.3,               ← total request time in ms
      "upstream_ms": 43.1,              ← upstream response time
      "upstream_addr": "127.0.0.1:8000",
      "bytes_sent":  2401,
      "bytes_recv":  840,
      "pid":  1234,                     ← ngx.worker.pid() for kprobe correlation
      "tid":  1234,                     ← not available in Lua, same as pid for nginx
    }
    """

    def __init__(
        self,
        registry: _RequestRegistry,
        kprobe: Optional[_NginxKprobeMode],
        host: str = "127.0.0.1",
        port: int = 9999,
    ) -> None:
        self._registry = registry
        self._kprobe   = kprobe
        self._host     = host
        self._port     = port
        self._sock: Optional[socket.socket] = None
        self._thread   = None
        self._running  = False

    def start(self) -> bool:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind((self._host, self._port))
            self._sock.settimeout(0.5)
        except OSError as exc:
            logger.warning("nginx Lua receiver: cannot bind %s:%d — %s", self._host, self._port, exc)
            return False

        self._running = True
        self._thread  = threading.Thread(
            target=self._recv_loop, daemon=True, name="stacktracer-nginx-lua"
        )
        self._thread.start()
        logger.info("nginx Lua UDP receiver listening on %s:%d", self._host, self._port)
        return True

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        if self._sock:
            self._sock.close()

    def _recv_loop(self) -> None:
        while self._running:
            try:
                data, _ = self._sock.recvfrom(4096)
                self._handle_packet(data)
            except socket.timeout:
                continue
            except Exception as exc:
                logger.debug("nginx Lua recv error: %s", exc)

    def _handle_packet(self, data: bytes) -> None:
        try:
            rec_data = json.loads(data.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            return

        request_id  = rec_data.get("request_id", "")
        method      = rec_data.get("method", "")
        uri         = rec_data.get("uri", "")
        status      = int(rec_data.get("status", 0))
        total_ms    = float(rec_data.get("total_ms", 0))
        upstream_ms = float(rec_data.get("upstream_ms", 0))
        upstream_addr = rec_data.get("upstream_addr", "")
        pid         = int(rec_data.get("pid", 0))
        bytes_sent  = int(rec_data.get("bytes_sent", 0))

        if not request_id:
            request_id = f"{_NGINX_TRACE_PREFIX}{time.time_ns()}"

        # Tell kprobe layer about this request_id→pid mapping
        # so subsequent kprobe events can use the right trace_id
        if self._kprobe and pid:
            self._kprobe.register_request_id(pid, pid, request_id)

        # Emit semantic event (always, even without kprobe)
        emit(NormalizedEvent.now(
            probe="nginx.request.semantic",
            trace_id=request_id,
            service="nginx",
            name=uri or "unknown",
            method=method,
            status_code=status,
            total_ms=total_ms,
            upstream_ms=upstream_ms,
            upstream_addr=upstream_addr,
            bytes_sent=bytes_sent,
            source="lua",
        ))

        # Update combined record with Lua HTTP data
        rec = self._registry.get_or_create(request_id)
        rec.method        = method
        rec.uri           = uri
        rec.status_code   = status
        rec.total_ms      = total_ms
        rec.upstream_ms   = upstream_ms
        rec.upstream_addr = upstream_addr
        rec.bytes_out     = rec.bytes_out or bytes_sent
        rec.lua_done      = True

        # If kprobe side already completed, emit combined now
        if rec.is_complete:
            rec.emit_combined()
            self._registry.remove(request_id)
        # If no kprobe at all, emit combined immediately with Lua data only
        elif self._kprobe is None:
            rec.kprobe_done = True
            rec.emit_combined()
            self._registry.remove(request_id)


# ====================================================================== #
# Log-tail fallback (unchanged from previous version)
# ====================================================================== #

class _NginxLogMode:
    def __init__(self, log_path: str, registry: _RequestRegistry) -> None:
        self._log_path = log_path
        self._registry = registry
        self._thread   = None
        self._running  = False

    def start(self) -> bool:
        if not os.path.exists(self._log_path):
            logger.warning("nginx log not found at %s", self._log_path)
            return False
        self._running = True
        self._thread  = threading.Thread(
            target=self._tail_loop, daemon=True, name="stacktracer-nginx-log"
        )
        self._thread.start()
        logger.info("nginx log-tail fallback: %s", self._log_path)
        return True

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _tail_loop(self) -> None:
        try:
            with open(self._log_path, "r") as f:
                f.seek(0, 2)
                while self._running:
                    line = f.readline()
                    if line:
                        self._handle_line(line.strip())
                    else:
                        time.sleep(0.05)
        except Exception as exc:
            logger.error("nginx log tail: %s", exc)

    def _handle_line(self, line: str) -> None:
        try:
            rec_data = json.loads(line)
        except json.JSONDecodeError:
            return

        request_id = (
            rec_data.get("request_id") or
            rec_data.get("x_request_id") or
            f"{_NGINX_TRACE_PREFIX}{time.time_ns()}"
        )
        emit(NormalizedEvent.now(
            probe="nginx.request.complete",
            trace_id=request_id,
            service="nginx",
            name=rec_data.get("uri", "unknown"),
            status_code=int(rec_data.get("status", 0)),
            duration_ns=int(float(rec_data.get("request_time", 0)) * 1e9),
            upstream_ms=float(rec_data.get("upstream_response_time", 0) or 0) * 1000,
            source="log",
        ))


# ====================================================================== #
# NginxProbe — unified entry point
# ====================================================================== #

class NginxProbe(BaseProbe):
    """
    nginx probe combining kprobe connection data with Lua HTTP semantics.

    Each layer works independently and degrades gracefully:

    kprobe only (no OpenResty):
        Connection lifecycle, epoll ticks, bytes in/out.
        trace_id = pid-tid (synthetic, not correlated with Python layer).

    Lua only (no root/CAP_BPF):
        Full HTTP semantics: URI, status, upstream timing.
        trace_id = request_id (correlated with uvicorn via X-Request-ID).
        This is the minimum useful configuration.

    kprobe + Lua (recommended):
        Combined record per request: connection timing + HTTP semantics.
        Both layers share request_id → unified trace from accept to response.

    log-tail only (all platforms, zero privileges):
        Fallback. One event per completed request. Works everywhere.

    Configure in stacktracer.yaml:
        nginx:
          mode: auto          # auto, kprobe, lua, combined, log
          log_path: /var/log/nginx/access.log
          lua_port: 9999      # UDP port matching stacktracer_nginx.lua

    Required nginx config (for Lua + kprobe correlation):
        proxy_set_header X-Request-ID $request_id;
        # And load stacktracer_nginx.lua in your OpenResty config
    """
    name = "nginx"

    def __init__(
        self,
        log_path: str  = "/var/log/nginx/access.log",
        lua_host: str  = "127.0.0.1",
        lua_port: int  = 9999,
        mode: str      = "auto",
    ) -> None:
        self._log_path = log_path
        self._lua_host = lua_host
        self._lua_port = lua_port
        self._mode     = mode
        self._kprobe:  Optional[_NginxKprobeMode] = None
        self._lua:     Optional[_NginxLuaReceiver] = None
        self._log:     Optional[_NginxLogMode]     = None
        self._evict_thread: Optional[threading.Thread] = None
        self._running  = False

    def start(self) -> None:
        use_kprobe = self._mode in ("auto", "kprobe", "combined")
        use_lua    = self._mode in ("auto", "lua", "combined")
        use_log    = self._mode in ("log",)

        kprobe_ok = False
        if use_kprobe and sys.platform == "linux":
            self._kprobe = _NginxKprobeMode(_registry)
            kprobe_ok    = self._kprobe.start()
            if not kprobe_ok:
                self._kprobe = None

        lua_ok = False
        if use_lua:
            self._lua = _NginxLuaReceiver(_registry, self._kprobe, self._lua_host, self._lua_port)
            lua_ok    = self._lua.start()
            if not lua_ok:
                self._lua = None

        # Log-tail: explicit mode, or auto fallback when nothing else started
        if use_log or (self._mode == "auto" and not kprobe_ok and not lua_ok):
            self._log = _NginxLogMode(self._log_path, _registry)
            if not self._log.start():
                self._log = None

        if not kprobe_ok and not lua_ok and self._log is None:
            logger.warning(
                "nginx probe: no mode could start. "
                "Install OpenResty for Lua mode, or ensure nginx is running "
                "and root is available for kprobe mode."
            )
            return

        # Background thread to evict stale incomplete records
        self._running = True
        self._evict_thread = threading.Thread(
            target=self._evict_loop, daemon=True, name="stacktracer-nginx-evict"
        )
        self._evict_thread.start()

        active = []
        if kprobe_ok: active.append("kprobe")
        if lua_ok:    active.append("lua-udp")
        if self._log: active.append("log-tail")
        logger.info("nginx probe active: %s", "+".join(active))

    def stop(self) -> None:
        self._running = False
        for component in (self._kprobe, self._lua, self._log):
            if component:
                component.stop()
        if self._evict_thread:
            self._evict_thread.join(timeout=2)

    def _evict_loop(self) -> None:
        while self._running:
            time.sleep(10)
            evicted = _registry.evict_stale()
            if evicted:
                logger.debug("nginx probe: evicted %d stale request records", evicted)
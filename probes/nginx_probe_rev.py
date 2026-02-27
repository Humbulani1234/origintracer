"""
probes/nginx_probe.py  (revised — kprobes on syscalls, no uprobes)

Observes nginx without attaching to nginx binary symbols.

Previous approach:
    Uprobes on 6 nginx C functions inside /usr/sbin/nginx:
        ngx_http_wait_request_handler, ngx_http_init_request, etc.
    Risk: nginx binary must have debug symbols or predictable symbol layout.
    Sensitive to nginx version, build flags, stripped binaries.

This approach — three modes, auto-selected:

Mode A — kprobes on syscalls (Linux + root/CAP_BPF):
    nginx is a regular process. It uses the same syscalls as anything else.
    We observe nginx's syscall activity by filtering on nginx's pid.
    No dependency on nginx binary internals whatsoever.

    sys_accept4     → new client connection accepted by nginx worker
    sys_epoll_wait  → nginx epoll loop tick (same syscall as asyncio)
    sys_sendmsg     → nginx sending response data to client
    sys_recvmsg     → nginx receiving request data from client

    What we lose vs uprobes:
        The per-function granularity (ngx_http_init_request etc.)
        We no longer know exactly when nginx finishes parsing headers
        vs when it dispatches to upstream.

    What we gain:
        Works on any nginx build, stripped or not.
        Works on any nginx version.
        Lower fragility — syscall numbers do not change between nginx versions.
        No symbol lookup required.

    Correlation with Python traces:
        nginx and uvicorn are separate processes so there is no shared
        BPF trace_context map. Correlation works via connection tuple:
            nginx emits {src_ip, src_port, dst_port} per connection
            uvicorn TracerMiddleware emits the same via REMOTE_ADDR
        The engine correlates nginx connection events to uvicorn request
        events by matching (client_ip, client_port) to REMOTE_ADDR.

        Additionally, if nginx forwards X-Request-ID:
            nginx access log contains $request_id
            uvicorn reads HTTP_X_REQUEST_ID header
            Both use it as trace_id → direct correlation, no tuple matching

Mode B — access log tail (all platforms, no privileges):
    Tails nginx JSON access log.
    Works everywhere with zero privilege requirement.
    Less granular — one event per completed request, no in-flight visibility.
    Correct fallback for production environments without CAP_BPF.

Mode C — combined (kprobe connection events + log completion events):
    kprobe fires on accept4 (connection opened) and sendmsg (data sent)
    Log fires on request completion with full timing
    Together: open time + transfer time + upstream time in one trace

Nginx pid discovery:
    We read /var/run/nginx.pid or scan /proc for processes named "nginx".
    The BPF filter uses this pid to ignore all non-nginx epoll_wait calls.
    Without pid filtering, sys_epoll_wait would fire for every process
    including uvicorn, redis, postgres — too much noise.

ProbeTypes:
    nginx.connection.accept     client connected to nginx
    nginx.connection.data_in    nginx received request bytes
    nginx.connection.data_out   nginx sent response bytes
    nginx.epoll.tick            nginx epoll_wait returned with events
    nginx.request.complete      request completed (from access log)
"""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import socket
import struct
import threading
import time
from typing import List, Optional

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
})

# nginx synthetic trace prefix — nginx has no Python trace context
_NGINX_TRACE_PREFIX = "nginx-"


# ====================================================================== #
# nginx pid discovery
# ====================================================================== #

def _find_nginx_pids() -> List[int]:
    """
    Find all nginx worker process pids.
    Returns empty list if nginx is not running or /proc not available.
    """
    pids = []

    # Method 1: /var/run/nginx.pid (master pid)
    for pid_file in ["/var/run/nginx.pid", "/run/nginx.pid"]:
        if os.path.exists(pid_file):
            try:
                with open(pid_file) as f:
                    master_pid = int(f.read().strip())
                # Find worker pids (children of master)
                pids.extend(_children_of(master_pid))
                if not pids:
                    pids.append(master_pid)
                logger.info("nginx: found master pid %d, workers: %s", master_pid, pids)
                return pids
            except (ValueError, OSError):
                pass

    # Method 2: scan /proc for nginx processes
    try:
        for entry in os.scandir("/proc"):
            if not entry.name.isdigit():
                continue
            try:
                with open(f"/proc/{entry.name}/comm") as f:
                    name = f.read().strip()
                if name == "nginx":
                    pids.append(int(entry.name))
            except (OSError, ValueError):
                continue
    except OSError:
        pass

    if pids:
        logger.info("nginx: found pids via /proc scan: %s", pids)
    return pids


def _children_of(parent_pid: int) -> List[int]:
    """Find child pids of parent_pid via /proc."""
    children = []
    try:
        for entry in os.scandir("/proc"):
            if not entry.name.isdigit():
                continue
            try:
                with open(f"/proc/{entry.name}/status") as f:
                    for line in f:
                        if line.startswith("PPid:"):
                            ppid = int(line.split()[1])
                            if ppid == parent_pid:
                                children.append(int(entry.name))
                            break
            except (OSError, ValueError):
                continue
    except OSError:
        pass
    return children


def _ip_to_str(ip_be: int) -> str:
    """Convert network-byte-order u32 to dotted-decimal string."""
    return socket.inet_ntoa(struct.pack("!I", socket.ntohl(ip_be)))


# ====================================================================== #
# Mode A — kprobe BPF program
# ====================================================================== #

# We use tracepoints (sys_enter/exit_*) rather than kprobes for syscalls
# because tracepoints have a stable ABI across kernel versions.
# kprobes on internal kernel functions can change — tracepoints do not.

_NGINX_BPF_PROGRAM = r"""
#include <uapi/linux/ptrace.h>
#include <linux/in.h>
#include <linux/socket.h>

// ── pid filter ────────────────────────────────────────────────────────
// Populated by Python before probe starts.
// We only trace nginx worker pids to avoid capturing all processes.
BPF_HASH(nginx_pids, u32, u8);   // pid → 1 if this is an nginx worker

static inline int is_nginx_pid() {
    u32 pid = bpf_get_current_pid_tgid() >> 32;
    return nginx_pids.lookup(&pid) != NULL;
}

// ── shared event output ───────────────────────────────────────────────
struct nginx_event_t {
    u64 timestamp_ns;
    u32 pid;
    u32 tid;
    char event_type[24];   // "accept", "epoll", "data_in", "data_out"
    s64 value1;            // accept: fd; epoll: n_events; data: bytes
    s64 value2;            // epoll: duration_ns; data: fd
    u64 duration_ns;
    // For accept: client address
    u32 client_ip;
    u16 client_port;
    u16 server_port;
};

BPF_PERF_OUTPUT(nginx_events);

// ── sys_accept4: new client connection ───────────────────────────────
// Fires when nginx worker calls accept4() to accept a queued connection.
// On return: ret = new socket fd, or negative on error.

BPF_HASH(accept_ts, u64, u64);   // tid → entry timestamp

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
    if (fd < 0) return 0;   // accept failed

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

    // Try to read client address from the sockaddr written by accept4.
    // args->upeer_sockaddr is the struct sockaddr __user * argument.
    // For AF_INET sockets this is struct sockaddr_in.
    struct sockaddr_in addr = {};
    if (args->upeer_sockaddr) {
        bpf_probe_read_user(&addr, sizeof(addr), args->upeer_sockaddr);
        if (addr.sin_family == AF_INET) {
            ev.client_ip   = addr.sin_addr.s_addr;
            ev.client_port = addr.sin_port;
        }
    }

    nginx_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}

// ── sys_epoll_wait: nginx event loop tick ─────────────────────────────
// nginx's event loop calls epoll_wait() on each iteration.
// Duration = how long nginx waited for kernel I/O events.
// n_events = how many connections had activity.

BPF_HASH(epoll_ts, u64, u64);

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
    ev.value1       = n;   // number of ready fds
    __builtin_memcpy(ev.event_type, "epoll", 6);

    nginx_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}

// ── sys_sendmsg: nginx sending response to client ─────────────────────
// Fires when nginx writes HTTP response bytes to a client socket.
// value1 = bytes sent, value2 = socket fd.

TRACEPOINT_PROBE(syscalls, sys_enter_sendmsg) {
    if (!is_nginx_pid()) return 0;

    u64 tid = bpf_get_current_pid_tgid();
    struct nginx_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(tid >> 32);
    ev.tid          = (u32)tid;
    ev.value2       = args->fd;
    __builtin_memcpy(ev.event_type, "data_out", 9);

    // Read iovec to get byte count — msghdr.msg_iov[0].iov_len
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

// ── sys_recvmsg: nginx receiving request from client ─────────────────
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
    kprobe-based nginx observer.
    Attaches to syscall tracepoints filtered to nginx worker pids.
    No uprobe on nginx binary required.
    """

    def __init__(self) -> None:
        self._bpf      = None
        self._thread   = None
        self._running  = False
        self._pids: List[int] = []

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
            logger.warning(
                "nginx kprobe: no nginx processes found. "
                "Is nginx running? Falling back to log mode."
            )
            return False

        try:
            self._bpf = BPF(text=_NGINX_BPF_PROGRAM)
        except Exception as exc:
            logger.warning("nginx BPF compile failed: %s", exc)
            return False

        # Populate the nginx_pids filter map
        pid_map = self._bpf["nginx_pids"]
        for pid in self._pids:
            pid_map[self._bpf.get_table("nginx_pids").Key(pid)] = \
                self._bpf.get_table("nginx_pids").Leaf(1)

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
            ev = self._bpf["nginx_events"].event(data)
            etype = ev.event_type.decode("ascii", errors="replace").rstrip("\x00")

            # nginx has no Python trace context — generate a connection-level ID
            # using pid+tid as a stable key for the duration of the connection
            conn_key = f"{_NGINX_TRACE_PREFIX}{ev.pid}-{ev.tid}"

            if etype == "accept":
                client_ip   = _ip_to_str(ev.client_ip) if ev.client_ip else ""
                client_port = socket.ntohs(ev.client_port) if ev.client_port else 0
                emit(NormalizedEvent.now(
                    probe="nginx.connection.accept",
                    trace_id=conn_key,
                    service="nginx",
                    name="accept",
                    pid=ev.pid,
                    fd=ev.value1,
                    client_ip=client_ip,
                    client_port=client_port,
                    duration_ns=ev.duration_ns,
                    source="kprobe",
                ))

            elif etype == "epoll":
                emit(NormalizedEvent.now(
                    probe="nginx.epoll.tick",
                    trace_id=conn_key,
                    service="nginx",
                    name="epoll_wait",
                    pid=ev.pid,
                    n_events=ev.value1,
                    duration_ns=ev.duration_ns,
                    source="kprobe",
                ))

            elif etype == "data_out":
                emit(NormalizedEvent.now(
                    probe="nginx.connection.data_out",
                    trace_id=conn_key,
                    service="nginx",
                    name="sendmsg",
                    pid=ev.pid,
                    fd=ev.value2,
                    bytes_sent=ev.value1,
                    source="kprobe",
                ))

            elif etype == "data_in":
                emit(NormalizedEvent.now(
                    probe="nginx.connection.data_in",
                    trace_id=conn_key,
                    service="nginx",
                    name="recvmsg",
                    pid=ev.pid,
                    bytes_received=ev.value1,
                    source="kprobe",
                ))

        except Exception as exc:
            logger.debug("nginx event handling error: %s", exc)


# ====================================================================== #
# Mode B — access log tail (fallback, all platforms)
# ====================================================================== #

class _NginxLogMode:
    """
    Tails nginx JSON access log for request completion events.
    Zero privilege requirement. Works on all platforms.
    Less granular than kprobe — one event per completed request only.
    """

    def __init__(self, log_path: str) -> None:
        self._log_path = log_path
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
        logger.info("nginx log-tail mode: %s", self._log_path)
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
            logger.error("nginx log tail error: %s", exc)

    def _handle_line(self, line: str) -> None:
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            rec = self._parse_common(line)
            if not rec:
                return

        # Use X-Request-ID if nginx was configured to log it —
        # this gives direct correlation with uvicorn traces.
        trace_id = (
            rec.get("request_id")
            or rec.get("x_request_id")
            or f"{_NGINX_TRACE_PREFIX}{time.time_ns()}"
        )

        uri             = rec.get("uri", rec.get("request", "unknown"))
        status          = int(rec.get("status", 0))
        request_time_s  = float(rec.get("request_time", 0))
        upstream_time   = rec.get("upstream_response_time", "-")
        client          = rec.get("remote_addr", "")

        emit(NormalizedEvent.now(
            probe="nginx.request.complete",
            trace_id=trace_id,
            service="nginx",
            name=uri,
            status_code=status,
            duration_ns=int(request_time_s * 1e9),
            upstream_duration_ns=(
                int(float(upstream_time) * 1e9)
                if upstream_time and upstream_time not in ("-", "")
                else None
            ),
            client_ip=client,
            source="log",
        ))

    @staticmethod
    def _parse_common(line: str) -> Optional[dict]:
        try:
            parts = line.split('"')
            if len(parts) < 5:
                return None
            req = parts[1]
            status_part = parts[2].strip().split()
            method, uri = (req.split()[:2] if " " in req else ("", req))
            return {
                "uri":    uri,
                "method": method,
                "status": status_part[0] if status_part else "0",
            }
        except Exception:
            return None


# ====================================================================== #
# NginxProbe — unified entry point
# ====================================================================== #

class NginxProbe(BaseProbe):
    """
    Observes nginx using syscall kprobes (preferred) or log-tail (fallback).

    Mode selection is automatic:
        - kprobe mode if: Linux + root/CAP_BPF + bcc + nginx running
        - log mode otherwise

    Both modes can run simultaneously (combined mode):
        kprobe gives in-flight connection/epoll events
        log gives completed request events with upstream timing

    Configure log path in stacktracer.yaml:
        nginx:
          log_path: /var/log/nginx/access.log
          mode: auto   # "auto", "kprobe", "log", or "combined"

    For X-Request-ID correlation, add to nginx config:
        log_format with "$request_id"
        proxy_set_header X-Request-ID $request_id;
    """
    name = "nginx"

    def __init__(
        self,
        log_path: str = "/var/log/nginx/access.log",
        mode: str = "auto",
    ) -> None:
        self._log_path   = log_path
        self._mode       = mode
        self._kprobe: Optional[_NginxKprobeMode] = None
        self._log:    Optional[_NginxLogMode]    = None

    def start(self) -> None:
        use_kprobe = self._mode in ("auto", "kprobe", "combined")
        use_log    = self._mode in ("auto", "log", "combined")

        kprobe_ok = False
        if use_kprobe and sys.platform == "linux":
            self._kprobe = _NginxKprobeMode()
            kprobe_ok = self._kprobe.start()
            if not kprobe_ok:
                self._kprobe = None

        # In auto mode: if kprobe failed, use log. In combined: use both.
        if use_log or (self._mode == "auto" and not kprobe_ok):
            self._log = _NginxLogMode(self._log_path)
            if not self._log.start():
                self._log = None

        if self._kprobe is None and self._log is None:
            logger.warning(
                "nginx probe: neither kprobe nor log-tail could start. "
                "Check nginx is running and log_path is correct."
            )

    def stop(self) -> None:
        if self._kprobe:
            self._kprobe.stop()
            self._kprobe = None
        if self._log:
            self._log.stop()
            self._log = None


import sys
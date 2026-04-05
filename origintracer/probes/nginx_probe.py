"""
BCC (BPF Compiler Collection) Installation & Setup Guide
=========================================================

BCC is a system-level tool and CANNOT be installed inside a virtualenv with pip.
It must be installed at the OS level and imported from there.

IMPORTANT: Run all commands as shown — order matters.

STEP 1 — Check your kernel version
----------------------------------
    $ uname -r
    # Requires 4.9 or higher (5.x / 6.x are fine)

    $ uname -a
    # If output contains "microsoft" or "WSL" → see WSL2 note at the bottom

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 2 — Install kernel headers
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BCC compiles BPF programs at runtime against your running kernel and needs
headers for that exact version.

    $ sudo apt update
    $ sudo apt install linux-headers-$(uname -r)

Verify:
    $ ls /lib/modules/$(uname -r)/build
    # Must show a directory — not "No such file or directory"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 3 — Install BCC from apt (NOT pip)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    $ sudo apt install -y bpfcc-tools libbpfcc-dev python3-bpfcc

    Packages installed:
        bpfcc-tools    → command-line BCC tools
        python3-bpfcc  → Python bindings  →  from bcc import BPF
        libbpfcc-dev   → C headers needed to compile BPF programs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 4 — Verify the install OUTSIDE your virtualenv
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    $ deactivate
    $ python3 -c "from bcc import BPF; print('bcc ok')"
    # Expected output: bcc ok

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 5 — Make BCC visible INSIDE your virtualenv  ← key step
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BCC's Python bindings live in the system Python, not in your venv.
A .pth file bridges the gap.

Find where python3-bpfcc installed to:
    $ python3 -c "import bcc; print(bcc.__file__)"
    # Typically: /usr/lib/python3/dist-packages/bcc/__init__.py

Activate your venv and add the system packages path:
    $ source /path/to/your/venv/bin/activate
    $ echo "/usr/lib/python3/dist-packages" > \
          $(python -c "import site; print(site.getsitepackages()[0])")/system_bcc.pth

Verify inside the venv:
    $ python -c "from bcc import BPF; print('bcc visible in venv')"
    # Expected output: bcc visible in venv

Once this passes, get_bridge() will work and bridge.available will be True.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 6 — Verify BPF permissions
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BPF requires root or CAP_BPF. Test with:
    $ sudo $(which python) -c "
        from bcc import BPF
        b = BPF(text='int kprobe__sys_clone(void *ctx) { return 0; }')
        print('BPF compile ok')
    "
    # Expected output: BPF compile ok

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 7 — Run gunicorn as root (dev only)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Kprobes require root to attach, so gunicorn must run as root in dev:

    $ sudo /path/to/your/venv/bin/gunicorn \
          -c gunicorn.conf.py \
          config.asgi:application \
          --worker-class uvicorn.workers.UvicornWorker

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WSL2 NOTE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WSL2 runs a Microsoft-patched kernel that often has BPF disabled or restricted.
Kprobes may not work at all, regardless of correct installation.

Check if BPF is enabled:
    $ zcat /proc/config.gz 2>/dev/null | grep CONFIG_BPF
    # or
    $ cat /boot/config-$(uname -r) 2>/dev/null | grep CONFIG_BPF

If these return nothing or show CONFIG_BPF=n — kprobes are unavailable.
The epoll probe will degrade gracefully to sys.monitoring. This is the
intended fallback, not a bug. The four-service graph does not need BPF.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUICK CHECKLIST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    1. uname -r                               →  kernel >= 4.9
    2. apt install linux-headers-$(uname -r)  →  headers present
    3. apt install python3-bpfcc              →  BCC installed at OS level
    4. .pth file written into venv            →  BCC visible inside venv
    5. sudo python -c "from bcc import BPF"   →  compile test passes
    6. gunicorn run as sudo                   →  kprobes can attach

**************************** INSTALLATION COMPLETE ************************

probes/nginx_probe.py  (final — kprobe + Lua UDP, correlated)
Three layers. Two correlate with each other. One is the fallback.

Layer A — kprobe (accept4, epoll_wait, sendmsg, recvmsg):
    Knows: fd, client_ip, client_port, kernel duration, bytes, epoll state.
    Does NOT know: URI, method, status, upstream timing.

Layer B — Lua UDP receiver (log_by_lua → UDP port 9119):
    Knows: URI, method, status, upstream timing, request_id, remote_addr.
    Does NOT know: fd numbers, accept latency, epoll state.

Correlation: (client_ip, client_port) == (remote_addr, remote_port).
When both fire for the same connection, they are merged into
nginx.request.enriched — one event with kernel + HTTP fields combined.

Layer C — access log tail: zero-privilege fallback.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import socketserver
import struct
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..core.kprobe_bridge import get_bridge
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("stacktracer.probes.nginx")

ProbeTypes.register_many(
    {
        "nginx.connection.accept": "accept4 — new connection, client addr captured",
        "nginx.connection.data_in": "recvmsg — bytes received from client",
        "nginx.connection.data_out": "sendmsg — bytes sent to client",
        "nginx.epoll.tick": "epoll_wait — nginx loop tick with n_events",
        "nginx.request.complete": "log_by_lua — full HTTP request completed",
        "nginx.request.enriched": "kprobe+lua merged — kernel+HTTP in one event",
        "nginx.main.start": "nginx master process discovered at probe start",
        "nginx.worker.discovered": "nginx worker process discovered at probe start",
    }
)

_NGINX_TRACE_PREFIX = "nginx-"

# ── Pre-fork event parking ─────────────────────────────────────────────────
# Same pattern as gunicorn_probe and celery_probe.
# NginxProbe.start() runs in the gunicorn master before fork().
# Events emitted here would land in the master's engine — which is discarded
# after fork. We park them in this list and drain them after init() completes
# in the worker, so the worker's engine receives the full nginx topology.

_pre_fork_events: list = []


def _drain_pre_fork_events() -> None:
    """Drain parked nginx topology events into the worker's live engine."""
    from stacktracer.sdk.emitter import emit_direct

    for event in _pre_fork_events:
        emit_direct(event)
    _pre_fork_events.clear()


# ── pid discovery ─────────────────────────────────────────────────────


def _find_nginx_pids() -> List[int]:
    pids = []
    for pid_file in ["/var/run/nginx.pid", "/run/nginx.pid"]:
        if os.path.exists(pid_file):
            try:
                with open(pid_file) as f:
                    master = int(f.read().strip())
                workers = _children_of(master)
                return workers or [master]
            except (ValueError, OSError):
                pass
    try:
        for e in os.scandir("/proc"):
            if not e.name.isdigit():
                continue
            try:
                with open(f"/proc/{e.name}/comm") as f:
                    if f.read().strip() == "nginx":
                        pids.append(int(e.name))
            except (OSError, ValueError):
                pass
    except OSError:
        pass
    return pids


def _find_nginx_master_and_workers() -> (
    Tuple[Optional[int], List[int]]
):
    """
    Returns (master_pid_or_None, [worker_pids]).
    Separates master from workers so structural edges can be drawn:
        nginx::master ──spawned──► nginx::worker-{pid}
    """
    for pid_file in ["/var/run/nginx.pid", "/run/nginx.pid"]:
        if os.path.exists(pid_file):
            try:
                with open(pid_file) as f:
                    master = int(f.read().strip())
                workers = _children_of(master)
                return master, workers
            except (ValueError, OSError):
                pass
    # fallback — no pid file, no master identity
    return None, _find_nginx_pids()


def _children_of(ppid: int) -> List[int]:
    kids = []
    try:
        for e in os.scandir("/proc"):
            if not e.name.isdigit():
                continue
            try:
                with open(f"/proc/{e.name}/status") as f:
                    for line in f:
                        if line.startswith("PPid:"):
                            if int(line.split()[1]) == ppid:
                                kids.append(int(e.name))
                            break
            except (OSError, ValueError):
                pass
    except OSError:
        pass
    return kids


def _ip_str(ip_be: int) -> str:
    return socket.inet_ntoa(
        struct.pack("!I", socket.ntohl(ip_be))
    )


# ── BPF program ───────────────────────────────────────────────────────

_NGINX_BPF = r"""
#include <uapi/linux/ptrace.h>
#include <linux/in.h>
#include <linux/socket.h>

BPF_HASH(nginx_pids, u32, u8);
static inline int is_nginx() {
    u32 p = bpf_get_current_pid_tgid() >> 32;
    return nginx_pids.lookup(&p) != NULL;
}

struct nginx_ev_t {
    u64 ts_ns; u32 pid; u32 tid;
    char etype[24];
    s64 v1; s64 v2; u64 dur_ns;
    u32 client_ip; u16 client_port; u16 _pad;
};
BPF_PERF_OUTPUT(nginx_events);

BPF_HASH(accept_ts, u64, u64);
TRACEPOINT_PROBE(syscalls, sys_enter_accept4) {
    if (!is_nginx()) return 0;
    u64 t = bpf_get_current_pid_tgid();
    u64 n = bpf_ktime_get_ns();
    accept_ts.update(&t, &n); return 0;
}
TRACEPOINT_PROBE(syscalls, sys_exit_accept4) {
    if (!is_nginx()) return 0;
    if (args->ret < 0) return 0;
    u64 t = bpf_get_current_pid_tgid();
    u64 *s = accept_ts.lookup(&t); if (!s) return 0;
    accept_ts.delete(&t);
    struct nginx_ev_t ev = {};
    ev.ts_ns = bpf_ktime_get_ns(); ev.pid = t>>32; ev.tid = (u32)t;
    ev.dur_ns = ev.ts_ns - *s; ev.v1 = args->ret;
    __builtin_memcpy(ev.etype, "accept", 7);
    struct sockaddr_in a = {};
    if (args->upeer_sockaddr) {
        bpf_probe_read_user(&a, sizeof(a), args->upeer_sockaddr);
        if (a.sin_family == AF_INET) {
            ev.client_ip = a.sin_addr.s_addr;
            ev.client_port = a.sin_port;
        }
    }
    nginx_events.perf_submit(args, &ev, sizeof(ev)); return 0;
}

BPF_HASH(epoll_ts, u64, u64);
TRACEPOINT_PROBE(syscalls, sys_enter_epoll_wait) {
    if (!is_nginx()) return 0;
    u64 t = bpf_get_current_pid_tgid();
    u64 n = bpf_ktime_get_ns();
    epoll_ts.update(&t, &n); return 0;
}
TRACEPOINT_PROBE(syscalls, sys_exit_epoll_wait) {
    if (!is_nginx()) return 0;
    if (args->ret <= 0) return 0;
    u64 t = bpf_get_current_pid_tgid();
    u64 *s = epoll_ts.lookup(&t); if (!s) return 0;
    epoll_ts.delete(&t);
    struct nginx_ev_t ev = {};
    ev.ts_ns = bpf_ktime_get_ns(); ev.pid = t>>32; ev.tid = (u32)t;
    ev.dur_ns = ev.ts_ns - *s; ev.v1 = args->ret;
    __builtin_memcpy(ev.etype, "epoll", 6);
    nginx_events.perf_submit(args, &ev, sizeof(ev)); return 0;
}

TRACEPOINT_PROBE(syscalls, sys_enter_sendmsg) {
    if (!is_nginx()) return 0;
    u64 t = bpf_get_current_pid_tgid();
    struct nginx_ev_t ev = {};
    ev.ts_ns = bpf_ktime_get_ns(); ev.pid = t>>32; ev.tid = (u32)t;
    ev.v2 = args->fd;
    struct iovec iov = {}; struct msghdr msg = {};
    if (args->msg) {
        bpf_probe_read_user(&msg, sizeof(msg), args->msg);
        if (msg.msg_iov && msg.msg_iovlen > 0) {
            bpf_probe_read_user(&iov, sizeof(iov), msg.msg_iov);
            ev.v1 = iov.iov_len;
        }
    }
    __builtin_memcpy(ev.etype, "data_out", 9);
    nginx_events.perf_submit(args, &ev, sizeof(ev)); return 0;
}
TRACEPOINT_PROBE(syscalls, sys_exit_recvmsg) {
    if (!is_nginx()) return 0;
    if (args->ret <= 0) return 0;
    u64 t = bpf_get_current_pid_tgid();
    struct nginx_ev_t ev = {};
    ev.ts_ns = bpf_ktime_get_ns(); ev.pid = t>>32; ev.tid = (u32)t;
    ev.v1 = args->ret;
    __builtin_memcpy(ev.etype, "data_in", 8);
    nginx_events.perf_submit(args, &ev, sizeof(ev)); return 0;
}
"""

# ── Correlator ────────────────────────────────────────────────────────


class _NginxCorrelator:
    _TTL = 60.0

    def __init__(self):
        self._table: Dict[Tuple[str, int], dict] = {}
        self._lock = threading.Lock()
        self._alive = True
        t = threading.Thread(
            target=self._evict,
            daemon=True,
            name="stacktracer-nginx-evict",
        )
        t.start()

    def stop(self):
        self._alive = False

    def register_connection(
        self,
        conn_key,
        client_ip,
        client_port,
        accept_ns,
        accept_dur_ns,
        pid,
        fd,
    ):
        if not client_ip or not client_port:
            return
        with self._lock:
            self._table[(client_ip, client_port)] = {
                "conn_key": conn_key,
                "client_ip": client_ip,
                "client_port": client_port,
                "accept_ns": accept_ns,
                "accept_dur_ns": accept_dur_ns,
                "pid": pid,
                "fd": fd,
                "_at": time.monotonic(),
            }

    def on_lua_event(self, d: dict):
        addr = d.get("remote_addr", "")
        port = int(d.get("remote_port", 0))
        with self._lock:
            conn = self._table.pop((addr, port), None)
        if conn:
            self._emit_enriched(d, conn)
        else:
            self._emit_lua_only(d)

    def _emit_enriched(self, lua, conn):
        trace_id = lua.get("trace_id") or conn["conn_key"]
        dur_ms = lua.get("duration_ms", 0)
        up_ms = lua.get("upstream_ms", -1)
        own_ms = lua.get("nginx_own_ms", -1)
        from stacktracer.sdk.emitter import emit_direct

        emit_direct(
            NormalizedEvent.now(
                probe="nginx.request.enriched",
                trace_id=trace_id,
                service="nginx",
                name=lua.get("uri", "unknown"),
                method=lua.get("method", ""),
                status_code=lua.get("status", 0),
                bytes_sent=lua.get("bytes_sent", 0),
                upstream_addr=lua.get("upstream_addr", ""),
                duration_ns=int(dur_ms * 1e6),
                upstream_duration_ns=(
                    int(up_ms * 1e6) if up_ms > 0 else None
                ),
                nginx_own_duration_ns=(
                    int(own_ms * 1e6) if own_ms > 0 else None
                ),
                accept_duration_ns=conn["accept_dur_ns"],
                client_ip=conn["client_ip"],
                client_port=conn["client_port"],
                worker_pid=conn["pid"],
                fd=conn["fd"],
                source="kprobe+lua",
            )
        )

    def _emit_lua_only(self, lua):
        trace_id = (
            lua.get("trace_id")
            or f"{_NGINX_TRACE_PREFIX}{time.time_ns()}"
        )
        dur_ms = lua.get("duration_ms", 0)
        up_ms = lua.get("upstream_ms", -1)
        from stacktracer.sdk.emitter import emit_direct

        emit_direct(
            NormalizedEvent.now(
                probe="nginx.request.complete",
                trace_id=trace_id,
                service="nginx",
                name=lua.get("uri", "unknown"),
                method=lua.get("method", ""),
                status_code=lua.get("status", 0),
                duration_ns=int(dur_ms * 1e6),
                upstream_duration_ns=(
                    int(up_ms * 1e6) if up_ms > 0 else None
                ),
                client_ip=lua.get("remote_addr", ""),
                source="lua",
            )
        )

    def _evict(self):
        while self._alive:
            time.sleep(30)
            cutoff = time.monotonic() - self._TTL
            with self._lock:
                stale = [
                    k
                    for k, v in self._table.items()
                    if v["_at"] < cutoff
                ]
                for k in stale:
                    del self._table[k]


# ── kprobe layer ─────────────────────────────────────────────────────


class _NginxKprobeMode:
    def __init__(self, correlator: _NginxCorrelator):
        self._corr = correlator
        self._bpf = None
        self._thread = None
        self._running = False

    def start(self) -> bool:
        try:
            from bcc import BPF
        except ImportError:
            return False
        if os.geteuid() != 0:
            return False
        pids = _find_nginx_pids()
        if not pids:
            logger.warning("nginx kprobe: nginx not found")
            return False
        try:
            self._bpf = BPF(text=_NGINX_BPF)
        except Exception as e:
            logger.warning("nginx BPF compile: %s", e)
            return False
        pm = self._bpf["nginx_pids"]
        for p in pids:
            pm[pm.Key(p)] = pm.Leaf(1)
        self._bpf["nginx_events"].open_perf_buffer(
            self._on_event
        )
        self._running = True
        self._thread = threading.Thread(
            target=self._poll,
            daemon=True,
            name="stacktracer-nginx-kprobe",
        )
        self._thread.start()
        logger.info("nginx kprobe active pids=%s", pids)
        return True

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        self._bpf = None

    def _poll(self):
        while self._running and self._bpf:
            try:
                self._bpf.perf_buffer_poll(timeout=100)
            except Exception:
                pass

    def _on_event(self, cpu, data, size):
        if not self._bpf:
            return
        from stacktracer.sdk.emitter import emit_direct

        try:
            ev = self._bpf["nginx_events"].event(data)
            etype = ev.etype.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")
            ckey = f"{_NGINX_TRACE_PREFIX}{ev.pid}-{ev.tid}"

            if etype == "accept":
                cip = (
                    _ip_str(ev.client_ip) if ev.client_ip else ""
                )
                cport = (
                    socket.ntohs(ev.client_port)
                    if ev.client_port
                    else 0
                )
                self._corr.register_connection(
                    conn_key=ckey,
                    client_ip=cip,
                    client_port=cport,
                    accept_ns=ev.ts_ns,
                    accept_dur_ns=ev.dur_ns,
                    pid=ev.pid,
                    fd=int(ev.v1),
                )
                emit_direct(
                    NormalizedEvent.now(
                        probe="nginx.connection.accept",
                        trace_id=ckey,
                        service="nginx",
                        name="accept",
                        pid=ev.pid,
                        fd=int(ev.v1),
                        client_ip=cip,
                        client_port=cport,
                        duration_ns=ev.dur_ns,
                        source="kprobe",
                    )
                )
            elif etype == "epoll":
                emit_direct(
                    NormalizedEvent.now(
                        probe="nginx.epoll.tick",
                        trace_id=ckey,
                        service="nginx",
                        name="epoll_wait",
                        pid=ev.pid,
                        n_events=int(ev.v1),
                        duration_ns=ev.dur_ns,
                        source="kprobe",
                    )
                )
            elif etype == "data_out":
                emit_direct(
                    NormalizedEvent.now(
                        probe="nginx.connection.data_out",
                        trace_id=ckey,
                        service="nginx",
                        name="sendmsg",
                        pid=ev.pid,
                        fd=int(ev.v2),
                        bytes_sent=int(ev.v1),
                        source="kprobe",
                    )
                )
            elif etype == "data_in":
                emit_direct(
                    NormalizedEvent.now(
                        probe="nginx.connection.data_in",
                        trace_id=ckey,
                        service="nginx",
                        name="recvmsg",
                        pid=ev.pid,
                        bytes_received=int(ev.v1),
                        source="kprobe",
                    )
                )
        except Exception as e:
            logger.debug("nginx kprobe event: %s", e)


# ── Lua UDP receiver ──────────────────────────────────────────────────


class _LuaHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            d = json.loads(
                self.request[0].decode("utf-8", errors="replace")
            )
            self.server.corr.on_lua_event(d)
        except Exception:
            pass


class _LuaServer(socketserver.UDPServer):
    allow_reuse_address = True

    def __init__(self, corr, host, port):
        self.corr = corr
        super().__init__((host, port), _LuaHandler)


class _NginxLuaMode:
    def __init__(
        self, corr: _NginxCorrelator, host="127.0.0.1", port=9119
    ):
        self._corr = corr
        self._host = host
        self._port = port
        self._srv = None

    def start(self) -> bool:
        try:
            self._srv = _LuaServer(
                self._corr, self._host, self._port
            )
        except OSError as e:
            logger.warning(
                "nginx Lua UDP bind %s:%d failed: %s",
                self._host,
                self._port,
                e,
            )
            return False
        t = threading.Thread(
            target=self._srv.serve_forever,
            daemon=True,
            name="stacktracer-nginx-lua-udp",
        )
        t.start()
        logger.info(
            "nginx Lua UDP receiver on %s:%d",
            self._host,
            self._port,
        )
        return True

    def stop(self):
        if self._srv:
            self._srv.shutdown()


# ── Log tail fallback ─────────────────────────────────────────────────


class _NginxLogMode:
    def __init__(self, log_path: str):
        self._path = log_path
        self._thread = None
        self._alive = False

    def start(self) -> bool:
        if not os.path.exists(self._path):
            try:
                os.makedirs(
                    os.path.dirname(self._path), exist_ok=True
                )
                open(self._path, "a").close()
                logger.info(
                    "nginx log probe: created %s", self._path
                )
            except OSError:
                logger.warning(
                    "nginx log probe: cannot create %s",
                    self._path,
                )
                return False
        self._alive = True
        self._thread = threading.Thread(
            target=self._tail,
            daemon=True,
            name="stacktracer-nginx-log",
        )
        self._thread.start()
        logger.info("nginx log tail: %s", self._path)
        return True

    def stop(self):
        self._alive = False

    def _tail(self):
        try:
            with open(self._path) as f:
                f.seek(0, 2)
                while self._alive:
                    line = f.readline()
                    if line:
                        self._line(line.strip())
                    else:
                        time.sleep(0.05)
        except Exception as e:
            logger.error("nginx log tail: %s", e)

    def _line(self, line: str):
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            try:
                parts = line.split('"')
                req = parts[1]
                sp = parts[2].strip().split()
                m, u = (
                    req.split()[:2] if " " in req else ("", req)
                )
                r = {
                    "uri": u,
                    "method": m,
                    "status": sp[0] if sp else "0",
                }
            except Exception:
                return

        trace_id = (
            r.get("request_id")
            or r.get("x_request_id")
            or f"{_NGINX_TRACE_PREFIX}{time.time_ns()}"
        )
        rt = float(r.get("request_time", 0))
        ut = r.get("upstream_response_time", "-")
        from stacktracer.sdk.emitter import emit_direct

        emit_direct(
            NormalizedEvent.now(
                probe="nginx.request.complete",
                trace_id=trace_id,
                service="nginx",
                name=r.get("uri", "unknown"),
                status_code=int(r.get("status", 0)),
                duration_ns=int(rt * 1e9),
                upstream_duration_ns=(
                    int(float(ut) * 1e9)
                    if ut not in ("-", "", "None")
                    else None
                ),
                client_ip=r.get("remote_addr", ""),
                source="log",
            )
        )


# ── NginxProbe ────────────────────────────────────────────────────────


class NginxProbe(BaseProbe):
    """
    nginx observation — three complementary layers:

    kprobe  → kernel timing, connection identity, epoll state
    Lua     → HTTP semantics, upstream timing, request_id
    merged  → nginx.request.enriched when both fire for same connection

    Pre-fork topology events:
        On start(), the probe discovers nginx master + worker pids and parks
        nginx.main.start and nginx.worker.discovered events into _pre_fork_events.
        These are drained into the live engine after init() completes in each
        gunicorn worker, giving every worker's graph the correct nginx topology:

            nginx::master ──spawned──► nginx::worker-{pid}
            nginx::worker-{pid} ──handled──► nginx::{uri}

    Configure:
        nginx:
          mode: auto       # auto | kprobe | lua | log | combined
          log_path: /var/log/nginx/access.log
          lua_host: 127.0.0.1
          lua_port: 9119
    """

    name = "nginx"

    def __init__(
        self,
        log_path="/mnt/c/Users/humbulani/nginx-1.24.0/logs/access.log",
        mode="log",
        lua_host="127.0.0.1",
        lua_port=9119,
    ):
        self._log_path = log_path
        self._mode = mode
        self._lua_host = lua_host
        self._lua_port = lua_port
        self._corr: Optional[_NginxCorrelator] = None
        self._kp: Optional[_NginxKprobeMode] = None
        self._lua: Optional[_NginxLuaMode] = None
        self._log: Optional[_NginxLogMode] = None

    def start(self):
        # ── 1. Discover nginx topology and park pre-fork events ───────────
        # Must happen before any fork() so the events are in _pre_fork_events
        # when the post-init callback drains them into the worker's engine.
        master_pid, worker_pids = (
            _find_nginx_master_and_workers()
        )
        struct_trace_id = f"{_NGINX_TRACE_PREFIX}struct-{master_pid or os.getpid()}"

        if master_pid:
            _pre_fork_events.append(
                NormalizedEvent.now(
                    probe="nginx.main.start",
                    trace_id=struct_trace_id,
                    service="nginx",
                    name="master",
                    worker_pid=master_pid,
                )
            )
            for wpid in worker_pids:
                _pre_fork_events.append(
                    NormalizedEvent.now(
                        probe="nginx.worker.discovered",
                        trace_id=struct_trace_id,
                        service="nginx",
                        name=f"worker-{wpid}",
                        worker_pid=wpid,
                        master_pid=master_pid,
                    )
                )
            logger.info(
                "nginx probe: discovered master=%d workers=%s — parked %d topology events",
                master_pid,
                worker_pids,
                len(_pre_fork_events),
            )
        else:
            logger.info(
                "nginx probe: nginx not running — topology events skipped"
            )

        # Register drain callback — fires after init() completes in worker
        from stacktracer import _register_post_init_callback

        _register_post_init_callback(_drain_pre_fork_events)

        # ── 2. Start the appropriate observation layer ────────────────────
        self._corr = _NginxCorrelator()
        wk = self._mode in ("auto", "kprobe", "combined")
        wl = self._mode in ("auto", "lua", "combined")

        kp_ok = lu_ok = False
        if wk and sys.platform == "linux":
            self._kp = _NginxKprobeMode(self._corr)
            kp_ok = self._kp.start()
            if not kp_ok:
                self._kp = None
        if wl:
            self._lua = _NginxLuaMode(
                self._corr, self._lua_host, self._lua_port
            )
            lu_ok = self._lua.start()
            if not lu_ok:
                self._lua = None
        if not kp_ok and not lu_ok:
            self._log = _NginxLogMode(self._log_path)
            if not self._log.start():
                self._log = None

        active = (
            (["kprobe"] if kp_ok else [])
            + (["lua"] if lu_ok else [])
            + (["log"] if self._log else [])
        )
        if active:
            logger.info("nginx probe: %s", "+".join(active))
            if kp_ok and lu_ok:
                logger.info(
                    "nginx: kprobe+lua combined — emitting "
                    "nginx.request.enriched on correlated connections"
                )
        else:
            logger.warning("nginx probe: no layer started")

    def stop(self):
        for x in (self._kp, self._lua, self._log, self._corr):
            if x:
                x.stop()
        self._kp = self._lua = self._log = self._corr = None

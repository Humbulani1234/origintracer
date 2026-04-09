"""
BCC (BPF Compiler Collection) Installation & Setup Guide
--------------------------------------------------------

BCC is a system-level tool and CANNOT be installed inside a virtualenv with pip.
It must be installed at the OS level and imported from there.

NOTE: Run all commands as shown - order matters.


STEP 1 — Check your kernel version
----------------------------------

    $ uname -r
    # Requires 4.9 or higher (5.x / 6.x are fine)


STEP 2 — Install kernel headers
-------------------------------

BCC compiles BPF programs at runtime against your running kernel and needs
headers for that exact version.

    $ sudo apt update
    $ sudo apt install linux-headers-$(uname -r)

Verify:
    $ ls /lib/modules/$(uname -r)/build


STEP 3 — Install BCC from apt
---------------------------------------

    $ sudo apt install -y bpfcc-tools libbpfcc-dev python3-bpfcc

    Packages installed:
        bpfcc-tools >> command-line BCC tools
        python3-bpfcc >> Python bindings > from bcc import BPF
        libbpfcc-dev >> C headers needed to compile BPF programs


STEP 4 — Verify the install OUTSIDE your virtualenv
---------------------------------------------------

    $ deactivate
    $ python3 -c "from bcc import BPF; print('bcc ok')"
    # Expected output: bcc ok


STEP 5 — Make BCC visible INSIDE your virtualenv
--------------------------------------------------

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


STEP 6 — Verify BPF permissions
--------------------------------

BPF requires root or CAP_BPF. Test with:
    $ sudo $(which python) -c "
        from bcc import BPF
        b = BPF(text='int kprobe__sys_clone(void *ctx) { return 0; }')
        print('BPF compile ok')
    "
    # Expected output: BPF compile ok


STEP 7 — Run gunicorn as root (dev only)
----------------------------------------

Kprobes require root to attach, so gunicorn must run as root in dev:

    $ sudo /path/to/your/venv/bin/gunicorn \
          -c gunicorn.conf.py \
          config.asgi:application \
          --worker-class uvicorn.workers.UvicornWorker


QUICK CHECKLIST
---------------

    1. uname -r                               >>  kernel >= 4.9
    2. apt install linux-headers-$(uname -r)  >>  headers present
    3. apt install python3-bpfcc              >>  BCC installed at OS level
    4. .pth file written into venv            >>  BCC visible inside venv
    5. sudo python -c "from bcc import BPF"   >>  compile test passes
    6. gunicorn run as sudo                   >>  kprobes can attach

**************************** INSTALLATION COMPLETE ************************

Two layers, and one is the fallback.

Layer A - kprobe (accept4, epoll_wait, sendmsg, recvmsg):
    Handles: fd, client_ip, client_port, kernel duration, bytes, epoll state.
    Does not handle: URI, method, status, upstream timing.

Layer B — access log tail: as a fallback.
"""

from __future__ import annotations

import ctypes
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

from ..core.bpf_programs import BPFProgramPart, register_bpf
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..core.kprobe_bridge import get_bridge
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("origintracer.probes.nginx")

ProbeTypes.register_many(
    {
        "nginx.connection.accept": "accept4 - new connection, client addr captured",
        "nginx.connection.data_in": "recvmsg - bytes received from client",
        "nginx.connection.data_out": "sendmsg - bytes sent to client",
        "nginx.epoll.tick": "epoll_wait - nginx loop tick with n_events",
        "nginx.request.complete": "log_by_lua - full HTTP request completed",
        "nginx.request.enriched": "kprobe+lua merged - kernel+HTTP in one event",
        "nginx.main.start": "nginx master process discovered at probe start",
        "nginx.worker.discovered": "nginx worker process discovered at probe start",
    }
)

_NGINX_TRACE_PREFIX = "nginx-"

# Events emitted here would land in the master's engine — which is discarded
# after fork. We park them in this list and drain them after init() completes
# in the worker, so the worker's engine receives the full nginx topology.

_pre_fork_events: list = []


def _drain_pre_fork_events() -> None:
    """
    Drain parked nginx topology events into the worker's live engine.
    """
    from origintracer.sdk.emitter import emit_direct

    for event in _pre_fork_events:
        emit_direct(event)
    _pre_fork_events.clear()


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
        nginx::master - spawned >> nginx::worker-{pid}
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


# ------------ Ngnix BPF --------------------------------

_NGINX_BPF = r"""
/* ------------- nginx pid filter ---------------------- */
static inline int nginx_is_nginx(void) {
    u32 pid = bpf_get_current_pid_tgid() >> 32;
    return nginx_pids.lookup(&pid) != NULL;
}

// ************ Internal Helpers (Replaces Macros) ************

static inline int handle_epoll_enter(void) {
    u64 pid_tid = bpf_get_current_pid_tgid();
    u64 ts      = bpf_ktime_get_ns();
    // Unconditional — save for ALL processes
    nginx_epoll_ts.update(&pid_tid, &ts);
    return 0;
}

static inline int handle_epoll_exit(void *args, int ret_val) {
    u64 pid_tid   = bpf_get_current_pid_tgid();
    u32 pid       = (u32)(pid_tid >> 32);
    u32 tid       = (u32)pid_tid;

    u64 *entry_ts = nginx_epoll_ts.lookup(&pid_tid);
    if (!entry_ts) return 0;

    u64 now = bpf_ktime_get_ns();

    // nginx path — no trace_id, correlation happens in Python
    if (nginx_is_nginx() && ret_val > 0) {
        struct kernel_event_t ev = {};
        ev.timestamp_ns = now;
        ev.pid          = pid;
        ev.tid          = tid;
        ev.duration_ns  = now - *entry_ts;
        ev.value1       = ret_val;
        __builtin_memcpy(ev.event_type, "nginx.epoll", 12);
        // no trace_id copy — nginx doesn't have it
        kernel_events.perf_submit(args, &ev, sizeof(ev));
    }

    // Logic for Asyncio/Tracing context
    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (ctx && ret_val > 0) {
        struct kernel_event_t ev = {};
        ev.timestamp_ns = now;
        ev.pid = pid;
        ev.tid = tid;
        ev.duration_ns = now - *entry_ts;
        ev.value1 = ret_val;
        __builtin_memcpy(ev.event_type, "epoll.wait", 11);
        __builtin_memcpy(ev.trace_id,   ctx->trace_id, 36);
        __builtin_memcpy(ev.service,    ctx->service,  32);
        kernel_events.perf_submit(args, &ev, sizeof(ev));
    }

    nginx_epoll_ts.delete(&pid_tid);
    return 0;
}

// ******** accept4 enter ***********************
TRACEPOINT_PROBE(syscalls, sys_enter_accept4) {
    if (!nginx_is_nginx()) return 0;
    u64 pid_tid = bpf_get_current_pid_tgid();
    u64 ts      = bpf_ktime_get_ns();
    nginx_accept_ts.update(&pid_tid, &ts);
    return 0;
}

// *********** accept4 exit ********************
TRACEPOINT_PROBE(syscalls, sys_exit_accept4) {
    if (!nginx_is_nginx()) return 0;
    if (args->ret < 0) return 0;

    u64 pid_tid   = bpf_get_current_pid_tgid();
    u64 *entry_ts = nginx_accept_ts.lookup(&pid_tid);
    if (!entry_ts) return 0;
    nginx_accept_ts.delete(&pid_tid);

    struct kernel_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(pid_tid >> 32);
    ev.tid          = (u32)pid_tid;
    ev.duration_ns  = ev.timestamp_ns - *entry_ts;
    ev.value1       = args->ret;   // accepted fd
    __builtin_memcpy(ev.event_type, "nginx.accept", 13);
    kernel_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}

// ************ epoll_wait Probes ************************
TRACEPOINT_PROBE(syscalls, sys_enter_epoll_wait)  { return handle_epoll_enter(); }
TRACEPOINT_PROBE(syscalls, sys_enter_epoll_pwait) { return handle_epoll_enter(); }

TRACEPOINT_PROBE(syscalls, sys_exit_epoll_wait)   { return handle_epoll_exit(args, args->ret); }
TRACEPOINT_PROBE(syscalls, sys_exit_epoll_pwait)  { return handle_epoll_exit(args, args->ret); }

<<<<<<< HEAD
    // asyncio path: any thread registered in trace_context
    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (ctx) {
        nginx_epoll_ts.update(&pid_tid, &ts);
    }

    return 0;
}

// ************ epoll_wait exit ****************************

TRACEPOINT_PROBE(syscalls, sys_exit_epoll_wait) {
    u64 pid_tid   = bpf_get_current_pid_tgid();
    u32 pid       = (u32)(pid_tid >> 32);
    u32 tid       = (u32)pid_tid;
    u64 *entry_ts = nginx_epoll_ts.lookup(&pid_tid);
    if (!entry_ts) return 0;
    nginx_epoll_ts.delete(&pid_tid);

    u64 now = bpf_ktime_get_ns();

    // nginx path
    if (nginx_is_nginx() && args->ret > 0) {
        struct kernel_event_t ev = {};
        ev.timestamp_ns = now;
        ev.pid = pid;
        ev.tid = tid;
        ev.duration_ns = now - *entry_ts;
        ev.value1 = args->ret;
        __builtin_memcpy(ev.event_type, "nginx.epoll", 12);
        kernel_events.perf_submit(args, &ev, sizeof(ev));
    }

    // asyncio path
    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (ctx && args->ret > 0) {
        struct kernel_event_t ev = {};
        ev.timestamp_ns = now;
        ev.pid          = pid;
        ev.tid          = tid;
        ev.duration_ns  = now - *entry_ts;
        ev.value1       = args->ret;
        __builtin_memcpy(ev.event_type, "epoll.wait", 11);
        __builtin_memcpy(ev.trace_id,   ctx->trace_id, 36);
        __builtin_memcpy(ev.service,    ctx->service,  32);
        kernel_events.perf_submit(args, &ev, sizeof(ev));
    }

    return 0;
}

// *********** sendmsg exit ***********************************

// Exit probe only: msghdr.msg_iov / msg_iovlen removed from BPF-visible
// msghdr in kernel 6.x. args->ret gives actual bytes sent.
TRACEPOINT_PROBE(syscalls, sys_exit_sendmsg) {
=======
// *********** write exit ***********************************
TRACEPOINT_PROBE(syscalls, sys_exit_write) {
>>>>>>> origin/deploy_stack_tracer
    if (!nginx_is_nginx()) return 0;
    if (args->ret <= 0) return 0;

    u64 pid_tid = bpf_get_current_pid_tgid();
    struct kernel_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid = (u32)(pid_tid >> 32);
    ev.tid = (u32)pid_tid;
    ev.value1 = args->ret;   // bytes sent
    __builtin_memcpy(ev.event_type, "nginx.data_out", 15);
    kernel_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}

<<<<<<< HEAD
// ************ recvmsg exit ********************************

TRACEPOINT_PROBE(syscalls, sys_exit_recvmsg) {
=======
// ************ recvfrom exit ********************************
TRACEPOINT_PROBE(syscalls, sys_exit_recvfrom) {
>>>>>>> origin/deploy_stack_tracer
    if (!nginx_is_nginx()) return 0;
    if (args->ret <= 0) return 0;

    u64 pid_tid = bpf_get_current_pid_tgid();
    struct kernel_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = (u32)(pid_tid >> 32);
    ev.tid          = (u32)pid_tid;
    ev.value1       = args->ret; // bytes received
    __builtin_memcpy(ev.event_type, "nginx.data_in", 14);
    kernel_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}
"""

# ------------------ Register at import time -------------------------

# This runs when the module is imported - before bridge.start() is called.
# The BPFProgramPart wraps the private _NGINX_BPF string.

register_bpf(
    "nginx",
    BPFProgramPart(
        headers=[
            "#include <linux/in.h>",
            "#include <linux/socket.h>",
        ],
        structs=[],
        maps=[
            "BPF_HASH(nginx_pids, u32, u8);",
            "BPF_HASH(nginx_accept_ts, u64, u64);",
            "BPF_HASH(nginx_epoll_ts, u64, u64);",
        ],
        probes=[_NGINX_BPF],
    ),
)

# ------------ Correlator ----------------------------


class _NginxCorrelator:
    _TTL = 60.0

    def __init__(self):
        self._table: Dict[Tuple[str, int], dict] = {}
        self._lock = threading.Lock()
        self._alive = True
        t = threading.Thread(
            target=self._evict,
            daemon=True,
            name="origintracer-nginx-evict",
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


# ---------------- kprobe layer ------------------------------


class _NginxKprobeMode:
    """
    Observes nginx syscall events via the shared bridge BPF object.

    Does NOT own a BPF() object.
    Does NOT attach tracepoints — BCC auto-attaches TRACEPOINT_PROBE macros
    at compile time when BPF(text=...) is called in the bridge.

    Responsibilities:
      - populate nginx_pids BPF map  (so the kernel filter knows nginx pids)
      - open kernel_events perf buffer
      - poll in a daemon thread
      - dispatch events to correlator + emitter
    """

    def __init__(self, bridge, correlator):
        self._bridge = bridge
        self._our_pid = os.getpid()
        self._corr = correlator
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def _populate_nginx_pids(self) -> bool:
        """
        Populate the nginx_pids BPF map. Separated from start() so the
        dispatcher pattern can call it without opening the perf buffer.
        """
        pids = _find_nginx_pids()
        if not pids:
            logger.warning(
                "nginx kprobe: no nginx processes found"
            )
            return False
        try:
            nginx_pids_map = self._bridge.bpf["nginx_pids"]
            for pid in pids:
                nginx_pids_map[ctypes.c_uint32(pid)] = (
                    ctypes.c_uint8(1)
                )
            logger.info("nginx kprobe: tracking pids %s", pids)
            return True
        except Exception as exc:
            logger.warning(
                "nginx kprobe: failed to populate nginx_pids: %s",
                exc,
            )
            return False

    def start(self) -> bool:
        if not self._bridge.available:
            logger.info(
                "nginx kprobe: bridge unavailable — skipping"
            )
            return False

        if not self._populate_nginx_pids():
            return False

        try:
            self._bridge.bpf["kernel_events"].open_perf_buffer(
                self._handle_event
            )
        except Exception as exc:
            logger.warning(
                "nginx kprobe: open_perf_buffer failed: %s", exc
            )
            return False

        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="stacktracer-nginx-kprobe",
        )
        self._thread.start()
        logger.info("nginx kprobe: started")
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
                logger.debug("nginx kprobe poll error: %s", exc)
                time.sleep(0.1)

    def _handle_event(self, cpu, data, size):
        if self._bridge.bpf is None:
            return

        from origintracer.sdk.emitter import emit_direct

        try:
            ev = self._bridge.bpf["kernel_events"].event(data)
            event_type_full = ev.event_type.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")
            parts = event_type_full.split(".")
            etype = (
                parts[1] if len(parts) > 1 else event_type_full
            )
            ckey = f"{_NGINX_TRACE_PREFIX}{ev.pid}-{ev.tid}"
            print(
                f">>>>> nginx epoll: ev.pid={ev.pid} ev.tid={ev.tid} etype={etype}"
            )
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
                    accept_ns=ev.timestamp_ns,
                    accept_dur_ns=ev.duration_ns,
                    pid=ev.pid,
                    fd=int(ev.value1),
                )
                emit(
                    NormalizedEvent.now(
                        probe="nginx.connection.accept",
                        trace_id=ckey,
                        service="nginx",
                        name="accept",
                        pid=ev.pid,
                        fd=int(ev.value1),
                        client_ip=cip,
                        client_port=cport,
                        duration_ns=ev.duration_ns,
                        source="kprobe",
                    )
                )
            elif etype == "epoll":
                emit(
                    NormalizedEvent.now(
                        probe="nginx.epoll.tick",
                        trace_id=ckey,
                        service="nginx",
                        name="epoll_wait",
                        pid=ev.pid,
                        n_events=int(ev.value1),
                        duration_ns=ev.duration_ns,
                        source="kprobe",
                    )
                )
            elif etype == "data_out":
                emit(
                    NormalizedEvent.now(
                        probe="nginx.connection.data_out",
                        trace_id=ckey,
                        service="nginx",
                        name="sendmsg",
                        pid=ev.pid,
                        fd=int(ev.value2),
                        bytes_sent=int(ev.value1),
                        source="kprobe",
                    )
                )
            elif etype == "data_in":
                emit(
                    NormalizedEvent.now(
                        probe="nginx.connection.data_in",
                        trace_id=ckey,
                        service="nginx",
                        name="recvmsg",
                        pid=ev.pid,
                        bytes_received=int(ev.value1),
                        source="kprobe",
                    )
                )
            elif event_type_full == "epoll.wait":
                # asyncio epoll event — dispatch to asyncio handler
                trace_id = (
                    bytes(ev.trace_id)
                    .decode("ascii", errors="replace")
                    .rstrip("\x00")
                )
                if trace_id:
                    service = ev.service.decode(
                        "ascii", errors="replace"
                    ).rstrip("\x00")
                    emit(
                        NormalizedEvent.now(
                            probe="asyncio.loop.epoll_wait",
                            trace_id=trace_id,
                            service=service or "asyncio",
                            name="epoll_wait",
                            pid=ev.pid,
                            tid=ev.tid,
                            duration_ns=ev.duration_ns,
                            n_events=int(ev.value1),
                            ready_fds=[],
                            source="kprobe",
                        )
                    )
        except Exception as e:
            logger.debug("nginx kprobe event: %s", e)


# ------------- Log tail fallback --------------------------------


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
        from origintracer.sdk.emitter import emit_direct

        emit(
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


# ----------------- NginxProbe ---------------------------------


class NginxProbe(BaseProbe):
    """
        nginx observation — three complementary layers:

        kprobe  >> kernel timing, connection identity, epoll state
        Lua     >> HTTP semantics, upstream timing, request_id
        merged  >> nginx.request.enriched when both fire for same connection

        Pre-fork topology events:
            On start(), the probe discovers nginx master + worker pids and parks
            nginx.main.start and nginx.worker.discovered events into _pre_fork_events.
            These are drained into the live engine after init() completes in each
            gunicorn worker, giving every worker's graph the correct nginx topology:

                nginx::master - spawned >> nginx::worker-{pid}
                nginx::worker-{pid} - handled >> nginx::{uri}

        Configure:
            nginx:
    <<<<<<< HEAD
              mode: auto # auto | kprobe | lua | log | combined
    =======
              mode: auto # auto | kprobe | log |
    >>>>>>> origin/deploy_stack_tracer
              log_path: /var/log/nginx/access.log
              lua_host: 127.0.0.1
              lua_port: 9119
    """

    name = "nginx"

    def __init__(
        self,
        log_path="/var/log/nginx/access.log",
        mode="kprobe",
    ):
        self._log_path = log_path
        self._mode = mode
        self._corr: Optional[_NginxCorrelator] = None
        self._kp: Optional[_NginxKprobeMode] = None
        self._log: Optional[_NginxLogMode] = None

    def start(self):

        # --------- Discover nginx topology --------------
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

        # Register drain callback - fires after init() completes in worker
        from origintracer import _register_post_init_callback

        _register_post_init_callback(_drain_pre_fork_events)

        # -------- Start the observation layer ---------------
        self._corr = _NginxCorrelator()
        wk = self._mode in ("auto", "kprobe")

        kp_ok = False
        if wk and sys.platform == "linux":
            bridge = get_bridge()
            bridge.start()
            self._kp = _NginxKprobeMode(bridge, self._corr)
            kp_ok = self._kp.start()
            if not kp_ok:
                self._kp = None

        if not kp_ok:
            self._log = _NginxLogMode(self._log_path)
            if not self._log.start():
                self._log = None

        active = (["kprobe"] if kp_ok else []) + (
            ["log"] if self._log else []
        )
        if active:
            logger.info("nginx probe: %s", "+".join(active))
        else:
            logger.warning("nginx probe: no layer started")

    def stop(self):
        for x in (self._kp, self._log, self._corr):
            if x:
                x.stop()
        self._kp = self._log = self._corr = None

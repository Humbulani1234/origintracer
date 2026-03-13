"""
probes/asyncio_probe.py  (final — epoll fd enrichment added)

All three layers from the previous version are preserved unchanged.
One addition: the epoll BPF program now reads sk_dport from each
ready fd's socket struct so we know what type of I/O caused the wakeup.

New field on asyncio.loop.epoll_wait events:
    ready_fds: [{"fd": 7, "events": 1, "dst_port": 5432}, ...]
    dst_port 5432  → postgres response received
    dst_port 6379  → redis response received
    dst_port 0     → could not read (pipe, signal fd, non-socket)
    dst_port other → client HTTP connection, etc.

This lets the causal graph distinguish:
    epoll woke because postgres responded (I/O-bound wait — expected)
    epoll woke because nothing arrived yet (busy-poll — loop starvation)
"""

from __future__ import annotations

import asyncio, logging, os, sys, threading, time
from typing import Any, Callable, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..context.vars import get_trace_id, get_span_id
from ..core.kprobe_bridge import get_bridge

logger = logging.getLogger("stacktracer.probes.asyncio")

ProbeTypes.register_many(
    {
        "asyncio.loop.epoll_wait": "epoll_wait returned — fds ready, with socket type",
        "asyncio.loop.coro_call": "coroutine entered (sys.monitoring)",
        "asyncio.loop.coro_return": "coroutine returned (sys.monitoring)",
        "asyncio.task.create": "asyncio.create_task() called",
    }
)

_originals: dict = {}
_patched = False
_MONITORING_ID = None

# ── Enriched epoll BPF: reads dst_port from each ready fd ─────────────

_EPOLL_BPF = r"""
#include <uapi/linux/ptrace.h>
#include <net/sock.h>
#include <linux/fdtable.h>

struct trace_entry_t {
    char trace_id[36]; u64 start_ns; char service[32]; u32 pid; u32 tid;
};
BPF_HASH(trace_context, u64, struct trace_entry_t, 65536);

struct epoll_entry_t { u64 events_ptr; u64 entry_ns; };
BPF_HASH(epoll_entry, u64, struct epoll_entry_t);

struct epoll_ev_t { u32 events; u64 data; };

struct ready_fd_t { u32 fd; u32 epoll_events; u16 dst_port; };

struct epoll_result_t {
    u64 ts_ns; u32 pid; u32 tid; char trace_id[36]; char service[32];
    int n_events; u64 dur_ns;
    struct ready_fd_t fds[8]; int fd_count;
};
BPF_PERF_OUTPUT(epoll_events);

// Read destination port for a given fd by walking task → files → sock
static inline u16 fd_dst_port(u32 fd) {
    struct task_struct *t = (struct task_struct *)bpf_get_current_task();
    if (!t) return 0;
    struct files_struct *files = NULL;
    bpf_probe_read_kernel(&files, sizeof(files), &t->files);
    if (!files) return 0;
    struct fdtable *fdt = NULL;
    bpf_probe_read_kernel(&fdt, sizeof(fdt), &files->fdt);
    if (!fdt) return 0;
    struct file **farr = NULL;
    bpf_probe_read_kernel(&farr, sizeof(farr), &fdt->fd);
    if (!farr) return 0;
    struct file *fp = NULL;
    bpf_probe_read_kernel(&fp, sizeof(fp), &farr[fd]);
    if (!fp) return 0;
    struct socket *sp = NULL;
    bpf_probe_read_kernel(&sp, sizeof(sp), &fp->private_data);
    if (!sp) return 0;
    struct sock *sk = NULL;
    bpf_probe_read_kernel(&sk, sizeof(sk), &sp->sk);
    if (!sk) return 0;
    u16 dp = 0;
    bpf_probe_read_kernel(&dp, sizeof(dp), &sk->sk_dport);
    return (dp >> 8) | ((dp & 0xff) << 8);  // network→host
}

TRACEPOINT_PROBE(syscalls, sys_enter_epoll_wait) {
    u64 tid = bpf_get_current_pid_tgid();
    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (!ctx) return 0;
    struct epoll_entry_t e = { .events_ptr = (u64)args->events,
                               .entry_ns   = bpf_ktime_get_ns() };
    epoll_entry.update(&tid, &e);
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_exit_epoll_wait) {
    u64 tid = bpf_get_current_pid_tgid();
    int n = args->ret; if (n <= 0) return 0;
    struct epoll_entry_t *e = epoll_entry.lookup(&tid);
    if (!e) return 0; epoll_entry.delete(&tid);
    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (!ctx) return 0;

    struct epoll_result_t r = {};
    r.ts_ns = bpf_ktime_get_ns();
    r.pid = tid >> 32; r.tid = (u32)tid;
    r.n_events = n; r.dur_ns = r.ts_ns - e->entry_ns;
    __builtin_memcpy(r.trace_id, ctx->trace_id, 36);
    __builtin_memcpy(r.service,  ctx->service,  32);

    int cnt = n < 8 ? n : 8; r.fd_count = cnt;
    struct epoll_ev_t ev;
    struct epoll_ev_t __user *ep = (struct epoll_ev_t __user *)e->events_ptr;

    #pragma unroll
    for (int i = 0; i < 8; i++) {
        if (i >= cnt) break;
        if (bpf_probe_read_user(&ev, sizeof(ev), ep + i) == 0) {
            r.fds[i].fd           = (u32)ev.data;
            r.fds[i].epoll_events = ev.events;
            // NEW: read destination port for this fd
            r.fds[i].dst_port     = fd_dst_port((u32)ev.data);
        }
    }
    epoll_events.perf_submit(args, &r, sizeof(r)); return 0;
}
"""


class _EpollKprobe:
    def __init__(self, bridge):
        self._bridge = bridge
        self._bpf = None
        self._thread = None
        self._running = False
        self._pid = os.getpid()

    def start(self) -> bool:
        if not self._bridge.available:
            return False
        try:
            from bcc import BPF

            self._bpf = BPF(text=_EPOLL_BPF)
        except Exception as e:
            logger.warning("asyncio epoll BPF: %s", e)
            return False
        self._bpf["epoll_events"].open_perf_buffer(
            self._on_event
        )
        self._running = True
        self._thread = threading.Thread(
            target=self._poll,
            daemon=True,
            name="stacktracer-asyncio-epoll",
        )
        self._thread.start()
        logger.info(
            "asyncio epoll kprobe active (with fd socket-type enrichment)"
        )
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
        try:
            ev = self._bpf["epoll_events"].event(data)
            if ev.pid != self._pid:
                return
            trace_id = ev.trace_id.decode(
                "ascii", "replace"
            ).rstrip("\x00")
            if not trace_id:
                return
            service = ev.service.decode(
                "ascii", "replace"
            ).rstrip("\x00")

            ready = []
            for i in range(min(ev.fd_count, 8)):
                fd_info = {
                    "fd": ev.fds[i].fd,
                    "events": ev.fds[i].epoll_events,
                    "dst_port": ev.fds[i].dst_port,
                }
                # Human-readable service type from port
                p = ev.fds[i].dst_port
                if p == 5432:
                    fd_info["io_type"] = "postgres"
                elif p == 6379:
                    fd_info["io_type"] = "redis"
                elif p == 0:
                    fd_info["io_type"] = "pipe_or_signal"
                else:
                    fd_info["io_type"] = "tcp"
                ready.append(fd_info)

            emit(
                NormalizedEvent.now(
                    probe="asyncio.loop.epoll_wait",
                    trace_id=trace_id,
                    service=service or "asyncio",
                    name="epoll_wait",
                    pid=ev.pid,
                    tid=ev.tid,
                    duration_ns=ev.dur_ns,
                    n_events=ev.n_events,
                    ready_fds=ready,
                    source="kprobe",
                )
            )
        except Exception as e:
            logger.debug("asyncio epoll event: %s", e)


# ── sys.monitoring layer (unchanged from previous version) ────────────


def _is_coro(code) -> bool:
    return bool(code.co_flags & 0x100)


def _setup_monitoring_312() -> bool:
    global _MONITORING_ID
    if not hasattr(sys, "monitoring"):
        return False
    tid = sys.monitoring.PROFILER_ID
    _MONITORING_ID = tid
    try:
        sys.monitoring.set_events(
            tid,
            sys.monitoring.events.CALL
            | sys.monitoring.events.PY_RETURN,
        )
    except Exception as e:
        logger.warning("sys.monitoring set_events: %s", e)
        return False

    def on_call(code, offset, callable_, arg0):
        if not _is_coro(code):
            return sys.monitoring.DISABLE
        tid = get_trace_id()
        if tid:
            emit(
                NormalizedEvent.now(
                    probe="asyncio.loop.coro_call",
                    trace_id=tid,
                    service="asyncio",
                    name=code.co_qualname,
                    parent_span_id=get_span_id(),
                    source="sys.monitoring",
                )
            )

    def on_ret(code, offset, retval):
        if not _is_coro(code):
            return sys.monitoring.DISABLE
        tid = get_trace_id()
        if tid:
            emit(
                NormalizedEvent.now(
                    probe="asyncio.loop.coro_return",
                    trace_id=tid,
                    service="asyncio",
                    name=code.co_qualname,
                    parent_span_id=get_span_id(),
                    source="sys.monitoring",
                )
            )

    try:
        sys.monitoring.register_callback(
            tid, sys.monitoring.events.CALL, on_call
        )
        sys.monitoring.register_callback(
            tid, sys.monitoring.events.PY_RETURN, on_ret
        )
        logger.info("asyncio: sys.monitoring installed")
        return True
    except Exception as e:
        logger.warning("sys.monitoring register: %s", e)
        return False


def _teardown_monitoring_312():
    global _MONITORING_ID
    if _MONITORING_ID is None or not hasattr(sys, "monitoring"):
        return
    sys.monitoring.set_events(
        _MONITORING_ID, sys.monitoring.events.NO_EVENTS
    )
    sys.monitoring.register_callback(
        _MONITORING_ID, sys.monitoring.events.CALL, None
    )
    sys.monitoring.register_callback(
        _MONITORING_ID, sys.monitoring.events.PY_RETURN, None
    )
    _MONITORING_ID = None


def _setup_setprofile_311() -> bool:
    orig = sys.getprofile()
    _originals["sys_profile"] = orig

    def _cb(frame, event, arg):
        if event in ("call", "return") and _is_coro(
            frame.f_code
        ):
            tid = get_trace_id()
            if tid:
                p = (
                    "asyncio.loop.coro_call"
                    if event == "call"
                    else "asyncio.loop.coro_return"
                )
                emit(
                    NormalizedEvent.now(
                        probe=p,
                        trace_id=tid,
                        service="asyncio",
                        name=frame.f_code.co_qualname,
                        source="sys.setprofile",
                    )
                )
        if orig:
            orig(frame, event, arg)

    sys.setprofile(_cb)
    logger.info("asyncio: sys.setprofile installed")
    return True


def _teardown_setprofile_311():
    sys.setprofile(_originals.pop("sys_profile", None))


def _create_task_wrapper(orig: Callable) -> Callable:
    def _wrapped(coro, *, name=None, context=None):
        tid = get_trace_id()
        if tid:
            emit(
                NormalizedEvent.now(
                    probe="asyncio.task.create",
                    trace_id=tid,
                    service="asyncio",
                    name=getattr(
                        coro, "__qualname__", type(coro).__name__
                    ),
                    parent_span_id=get_span_id(),
                    task_name=name,
                )
            )
        return (
            orig(coro, name=name)
            if context is None
            else orig(coro, name=name, context=context)
        )

    return _wrapped


# ── AsyncioProbe ──────────────────────────────────────────────────────


class AsyncioProbe(BaseProbe):
    """
    Three layers, all preserved. One enhancement to the epoll layer:

    epoll BPF now reads sk_dport for each ready fd, so events include
    io_type: "postgres" | "redis" | "tcp" | "pipe_or_signal"

    This enriches the causal graph:
        asyncio::epoll_wait {n_events:3, ready:[
            {fd:7, io_type:"postgres"},   ← loop woke for DB response
            {fd:9, io_type:"redis"},       ← and cache response
            {fd:11, io_type:"tcp"},        ← and a new client connection
        ]}
    The causal rule engine can now distinguish I/O-bound waits
    (expected) from busy-poll (loop starvation symptom).
    """

    name = "asyncio"

    def __init__(self):
        self._epoll: Optional[_EpollKprobe] = None

    def start(self):
        global _patched
        if _patched:
            logger.warning("asyncio probe already installed")
            return

        major, minor = sys.version_info[:2]
        if (major, minor) not in ((3, 11), (3, 12), (3, 13)):
            logger.warning(
                "asyncio probe: unsupported Python %d.%d",
                major,
                minor,
            )
            return

        bridge = get_bridge()
        if bridge.available and sys.platform == "linux":
            self._epoll = _EpollKprobe(bridge)
            if not self._epoll.start():
                self._epoll = None

        if minor >= 12:
            _setup_monitoring_312()
        else:
            _setup_setprofile_311()

        _originals["create_task"] = asyncio.create_task
        asyncio.create_task = _create_task_wrapper(
            asyncio.create_task
        )
        _patched = True

    def stop(self):
        global _patched
        if not _patched:
            return
        if self._epoll:
            self._epoll.stop()
            self._epoll = None
        if sys.version_info[1] >= 12:
            _teardown_monitoring_312()
        else:
            _teardown_setprofile_311()
        if "create_task" in _originals:
            asyncio.create_task = _originals.pop("create_task")
        _patched = False
        logger.info("asyncio probe removed")

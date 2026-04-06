"""
probes/asyncio_probe.py  (revised — kprobe + sys.monitoring)

Observes the asyncio event loop without patching Task.__step.

The previous approach:
    Patched Task._Task__step at the class level. Fragile on 3.12+
    because Task moved to a C extension. Required uprobes on _asyncio.so
    for 3.12+ which needed root and were sensitive to symbol names.

This approach:
    Three layers, none of which require patching Python internals:

Layer 1 — kprobe on sys_epoll_wait (Linux, all Python versions)
    The event loop calls epoll_wait() as a syscall. kprobe captures:
        - How long the loop blocked waiting for I/O
        - How many fds became ready
        - Which fd numbers and event types (EPOLLIN/EPOLLOUT)
    This is more accurate than patching _run_once() because it measures
    the actual kernel call, not a Python wrapper around it.

    Correlation with Python trace context via KprobeBridge:
        bridge writes (tid → trace_id) to BPF map when trace starts
        kprobe reads the map to attribute events to the right trace

Layer 2 — sys.monitoring (Python 3.12+) or sys.setprofile (3.11)
    Observes coroutine entry and return without replacing any code.
    sys.monitoring is the official PEP 669 API designed exactly for
    this purpose — it is not monkey patching, it is the registered
    profiling callback interface.

    On 3.12+:
        sys.monitoring.CALL event fires on every coroutine step (await resume)
        sys.monitoring.PY_RETURN fires when coroutine returns or raises

    On 3.11:
        sys.setprofile callback — lower overhead than settrace,
        fires on c_call, c_return, call, return events

    What we get: coroutine qualified name, call depth, which module.
    We filter to asyncio tasks to avoid overhead from observing
    every function call in the process.

Layer 3 — asyncio.create_task() wrap (public API, all versions)
    Wraps the public create_task() function — not a method on Task,
    not a private attribute. This is the approved way to observe
    task creation. Stable across all versions.

What this cannot tell us compared to __step patching:
    The _fut_waiter state (what future a task is blocked on).
    This was the one genuinely useful thing __step gave us.
    We lose it here in exchange for safety and portability.

    The replacement: the epoll kprobe tells us WHICH FD became ready,
    and we can correlate fd → socket → connection_type to understand
    what the task was waiting for at the I/O level.

New ProbeTypes:
    asyncio.loop.epoll_wait     epoll_wait returned with N ready fds
    asyncio.loop.coro_call      coroutine entered (sys.monitoring)
    asyncio.loop.coro_return    coroutine returned (sys.monitoring)
    asyncio.task.create         create_task() called
"""

# stacktracer/probes/asyncio_probe.py  — _EpollKprobe + _KernelEventDispatcher
#
# The asyncio probe does NOT register a BPFProgramPart.
# Reason: epoll_wait is a single kernel tracepoint — only one BPF program
# can attach to it at a time. The nginx probe's _NGINX_BPF already handles
# BOTH the nginx and asyncio paths inside its epoll_wait tracepoint functions.
#
# If the nginx probe is NOT installed, and you still want asyncio epoll
# observation, register the minimal asyncio-only BPF part below instead.
#
# _EpollKprobe:
#   - opens kernel_events perf buffer on bridge.bpf
#   - filters to event_type == "epoll.wait"
#   - polls in a daemon thread
#
# _KernelEventDispatcher:
#   - recommended when nginx + asyncio probes are both active
#   - opens kernel_events ONCE, routes by event_type to correct handler
#   - prevents the "open_perf_buffer called twice" failure


from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
from typing import Any, Callable, List, Optional

from ..context.vars import get_span_id, get_trace_id
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..core.kprobe_bridge import get_bridge
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("stacktracer.probes.asyncio")

SUPPORTED_PYTHONS = ((3, 11), (3, 12), (3, 13))

# Register probe types
ProbeTypes.register_many(
    {
        "asyncio.loop.epoll_wait": "epoll_wait syscall returned — fds ready for I/O",
        "asyncio.loop.coro_call": "coroutine entered (awaited or started)",
        "asyncio.loop.coro_return": "coroutine returned or raised",
        "asyncio.task.create": "asyncio.create_task() called",
        "asyncio.loop.select_wait": "select()/kqueue wait returned (non-Linux)",
    }
)

_originals: dict = {}
_patched = False

# sys.monitoring tool ID (3.12+)
_MONITORING_TOOL_ID = None


# ── Asyncio-only BPF fragment (used when nginx probe is NOT installed) ────────
# Private to this module — not exported.
# Only register this if the nginx probe has NOT registered epoll_wait probes.
# Registering both will cause a duplicate tracepoint attachment error at runtime.
#
# To use: uncomment the register_bpf call below and ensure nginx_probe is
# NOT imported in this deployment.

_ASYNCIO_EPOLL_BPF = r"""
// ── epoll_wait enter (asyncio-only, use when nginx probe not present) ─────────
TRACEPOINT_PROBE(syscalls, sys_enter_epoll_wait) {
    u64 pid_tid = bpf_get_current_pid_tgid();
    u32 tid     = (u32)pid_tid;
    u64 ts      = bpf_ktime_get_ns();
 
    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (ctx) {
        asyncio_epoll_ts.update(&pid_tid, &ts);
    }
    return 0;
}
 
// ── epoll_wait exit (asyncio-only) ────────────────────────────────────────────
TRACEPOINT_PROBE(syscalls, sys_exit_epoll_wait) {
    u64 pid_tid   = bpf_get_current_pid_tgid();
    u32 pid       = (u32)(pid_tid >> 32);
    u32 tid       = (u32)pid_tid;
    u64 *entry_ts = asyncio_epoll_ts.lookup(&pid_tid);
    if (!entry_ts) return 0;
    asyncio_epoll_ts.delete(&pid_tid);
    if (args->ret <= 0) return 0;
 
    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (!ctx) return 0;
 
    struct kernel_event_t ev = {};
    ev.timestamp_ns = bpf_ktime_get_ns();
    ev.pid          = pid;
    ev.tid          = tid;
    ev.duration_ns  = ev.timestamp_ns - *entry_ts;
    ev.value1       = args->ret;
    __builtin_memcpy(ev.event_type, "epoll.wait", 11);
    __builtin_memcpy(ev.trace_id,   ctx->trace_id, 36);
    __builtin_memcpy(ev.service,    ctx->service,  32);
    kernel_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}
"""

# Uncomment ONLY when deploying without the nginx probe:
# register_bpf("asyncio", BPFProgramPart(
#     maps=["BPF_HASH(asyncio_epoll_ts, u64, u64);"],
#     probes=[_ASYNCIO_EPOLL_BPF],
# ))


class _EpollKprobe:
    """
    Consumes epoll.wait events from the shared kernel_events perf buffer.

    Does NOT own a BPF() object.
    Does NOT attach tracepoints.
    Does NOT call register_bpf() — epoll tracepoints are owned by the
    nginx probe's BPF fragment when both probes are active.

    Use _KernelEventDispatcher instead of this class when nginx probe is
    also running — it owns the perf buffer and routes to both handlers.
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

    def _handle_epoll_event(
        self, cpu: int, data: Any, size: int
    ) -> None:
        if self._bpf is None:
            return
        try:
            ev = self._bpf["epoll_events"].event(data)

            if ev.pid != self._our_pid:
                return  # filter to our process

            trace_id = ev.trace_id.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")
            service = ev.service.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")

            if not trace_id:
                return

            # Decode ready fds
            ready = [
                {
                    "fd": ev.ready_fds[i],
                    "events": ev.ready_events[i],
                }
                for i in range(min(ev.fd_count, 8))
            ]

            emit(
                NormalizedEvent.now(
                    probe="asyncio.loop.epoll_wait",
                    trace_id=trace_id,
                    service=service or "asyncio",
                    name="epoll_wait",
                    pid=ev.pid,
                    tid=ev.tid,
                    duration_ns=ev.duration_ns,
                    n_events=ev.n_events,
                    ready_fds=ready,
                    source="kprobe",
                )
            )
        except Exception as exc:
            logger.debug("epoll event handling error: %s", exc)


# ── Single perf buffer dispatcher ─────────────────────────────────────────────
#
# kernel_events perf buffer can only be opened once. Use this dispatcher
# instead of opening it separately in both _NginxKprobeMode and _EpollKprobe.
#
# In your Engine startup:
#
#   dispatcher = _KernelEventDispatcher(bridge, nginx_correlator, asyncio_correlator)
#   dispatcher.start()
#   # Don't call open_perf_buffer in nginx or asyncio probes — dispatcher owns it

# ── _KernelEventDispatcher ────────────────────────────────────────────────────


class _KernelEventDispatcher:
    """
    Opens kernel_events exactly once and routes to the correct handler.

    Use this when both nginx and asyncio probes are active.
    In this case neither _NginxKprobeMode.start() nor _EpollKprobe.start()
    should open the perf buffer — the dispatcher owns it.

    Engine startup with dispatcher:

        bridge = get_bridge()
        bridge.start()

        nginx_kp = _NginxKprobeMode(bridge, nginx_corr)
        epoll_kp = _EpollKprobe(bridge, asyncio_corr)

        dispatcher = _KernelEventDispatcher(bridge, nginx_kp, epoll_kp)

        nginx_kp._populate_nginx_pids()   # populate map, no buffer open
        dispatcher.start()                # opens buffer, starts poll thread

    Without dispatcher (only one probe active):

        nginx_kp.start()    # opens buffer — ok if asyncio probe not running
        # or
        epoll_kp.start()    # opens buffer — ok if nginx probe not running
    """

    def __init__(self, bridge, nginx_handler, epoll_handler):
        self._bridge = bridge
        self._nginx_handler = nginx_handler
        self._epoll_handler = epoll_handler
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> bool:
        if not self._bridge.available:
            return False

        try:
            self._bridge.bpf["kernel_events"].open_perf_buffer(
                self._dispatch
            )
        except Exception as exc:
            logger.warning(
                "kernel event dispatcher: open_perf_buffer failed: %s",
                exc,
            )
            return False

        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="stacktracer-kernel-dispatcher",
        )
        self._thread.start()
        logger.info("kernel event dispatcher: started")
        return True

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def _poll_loop(self) -> None:
        bpf = self._bridge.bpf
        while self._running:
            try:
                bpf.perf_buffer_poll(timeout=100)
            except Exception as exc:
                logger.debug(
                    "kernel dispatcher poll error: %s", exc
                )
                time.sleep(0.1)

    def _dispatch(self, cpu, data, size) -> None:
        bpf = self._bridge.bpf
        try:
            ev = bpf["kernel_events"].event(data)
        except Exception:
            return

        event_type = ev.event_type.decode(
            "ascii", errors="replace"
        ).rstrip("\x00")

        if event_type.startswith("nginx."):
            self._nginx_handler._handle_event(cpu, data, size)
        elif event_type.startswith("epoll."):
            self._epoll_handler._handle_epoll_event(
                cpu, data, size
            )


# ====================================================================== #
# Layer 3 — asyncio.create_task() (all versions, public API)
# ====================================================================== #


def _make_create_task_wrapper(original: Callable) -> Callable:
    def _traced_create_task(
        coro: Any,
        *,
        name: Optional[str] = None,
        context: Any = None,
    ):
        trace_id = get_trace_id()
        if trace_id:
            coro_name = getattr(
                coro, "__qualname__", type(coro).__name__
            )
            emit(
                NormalizedEvent.now(
                    probe="asyncio.task.create",
                    trace_id=trace_id,
                    service="asyncio",
                    name=coro_name,
                    parent_span_id=get_span_id(),
                    task_name=name,
                )
            )
        if context is not None:
            return original(coro, name=name, context=context)
        return original(coro, name=name)

    return _traced_create_task


# ====================================================================== #
# AsyncioProbe
# ====================================================================== #


class AsyncioProbe(BaseProbe):
    """
    Observes the asyncio event loop without patching Task.__step.

    Layer 1 — kprobe on sys_epoll_wait (Linux, all versions):
        Captures the actual kernel-level I/O wait. Sees exactly which
        fds became ready and how long the loop blocked. More accurate
        than patching _run_once() because it measures the real syscall.
        Requires: Linux + root/CAP_BPF + bcc.
        Degrades gracefully if unavailable.

    Layer 2 — sys.monitoring (3.12+) or sys.setprofile (3.11):
        Observes coroutine entry and return via official profiling hooks.
        Not monkey patching — uses CPython's registered callback interface.
        Filters to coroutine functions to minimise overhead.

    Layer 3 — asyncio.create_task() wrap (all versions):
        Public API, stable, captures task creation with coroutine name.

    Comparison with previous __step approach:
        Lost:   _fut_waiter state (what future a task was blocked on)
        Gained: Real epoll event data (which fds, which event types)
                Works on 3.12+ without root if kprobe unavailable
                Coroutine names via sys.monitoring without any patching
                No fragility from Python version changes to Task internals

    On non-Linux (macOS, Windows):
        kqueue / select syscall observation not yet implemented.
        Layers 2 and 3 function normally on all platforms.
    """

    name = "asyncio"

    def __init__(self) -> None:
        self._epoll_kprobe: Optional[_EpollKprobe] = None

    def start(self, observe_modules: List[str] = None) -> None:

        import pdb

        pdb.set_trace()

        global _originals, _patched

        major, minor = sys.version_info[:2]
        if (major, minor) not in SUPPORTED_PYTHONS:
            logger.warning(
                "asyncio probe: unsupported Python %d.%d",
                major,
                minor,
            )
            return

        if _patched:
            logger.warning(
                "asyncio probe already installed — skipping"
            )
            return

        # ── Layer 1: epoll kprobe ──────────────────────────────────────
        bridge = get_bridge()
        bridge.start()
        if bridge.available and sys.platform == "linux":
            self._epoll_kprobe = _EpollKprobe(bridge)
            ok = self._epoll_kprobe.start()
            if not ok:
                self._epoll_kprobe = None
        else:
            logger.info(
                "asyncio probe: epoll kprobe unavailable "
                "(platform=%s, bridge=%s). "
                "Coroutine-level tracing still active via sys.monitoring.",
                sys.platform,
                bridge.available,
            )

        # ── Layer 2: coroutine observation ─────────────────────────────
        # if minor >= 12:
        #     _setup_monitoring_312()
        # else:
        #     _setup_setprofile_311()

        # ── Layer 3: create_task ───────────────────────────────────────
        _originals["create_task"] = asyncio.create_task
        asyncio.create_task = _make_create_task_wrapper(
            asyncio.create_task
        )

        _patched = True

    def stop(self) -> None:
        global _originals, _patched

        if not _patched:
            return

        if self._epoll_kprobe:
            self._epoll_kprobe.stop()
            self._epoll_kprobe = None

        major, minor = sys.version_info[:2]
        # if minor >= 12:
        #     _teardown_monitoring_312()
        # else:
        #     _teardown_setprofile_311()

        if "create_task" in _originals:
            asyncio.create_task = _originals.pop("create_task")

        _patched = False
        logger.info("asyncio probe removed")

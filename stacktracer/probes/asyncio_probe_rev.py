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

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
from typing import Any, Callable, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..context.vars import get_trace_id, get_span_id
from ..core.kprobe_bridge import get_bridge

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


# ====================================================================== #
# Layer 1 — kprobe on sys_epoll_wait
# ====================================================================== #

# BPF program for epoll_wait observation.
# Uses the shared trace_context map from KprobeBridge.
# Compiled together so the maps are shared by name.

_EPOLL_BPF_PROGRAM = r"""
// sys_epoll_wait signature:
//   int epoll_wait(int epfd, struct epoll_event *events,
//                  int maxevents, int timeout)
//
// We probe at entry to save the events pointer (needed at return),
// and at return to read what the kernel wrote into the events[] array.

#include <uapi/linux/ptrace.h>

// Include shared bridge header — trace_context map and kernel_events
// perf buffer are defined there.
// (In the compiled version, KprobeBridge.bpf is passed to BPF() and
//  the programs share the same BPF object, so the map is shared.)

struct epoll_event_t {
    __u32 events;
    __u64 data;     // data.fd for most asyncio uses
};

// Save entry args so we can read events[] at return time
struct epoll_entry_t {
    u64  events_ptr;     // pointer to the events array in userspace
    int  maxevents;
    u64  entry_ns;
};

BPF_HASH(epoll_entry, u64, struct epoll_entry_t);   // tid → entry state

struct epoll_result_t {
    u64  timestamp_ns;
    u32  pid;
    u32  tid;
    char trace_id[36];
    char service[32];
    int  n_events;
    u64  duration_ns;
    // First 8 ready fds (bounded for BPF stack safety)
    u32  ready_fds[8];
    u32  ready_events[8];   // EPOLLIN/EPOLLOUT bitmasks
    int  fd_count;          // actual count, capped at 8
};

BPF_PERF_OUTPUT(epoll_events);

// Entry: save the events pointer for use at return
TRACEPOINT_PROBE(syscalls, sys_enter_epoll_wait) {
    u64 tid = bpf_get_current_pid_tgid();

    // Only trace if this thread is in a Python trace context
    struct trace_entry_t *ctx_entry = trace_context.lookup(&tid);
    if (!ctx_entry) return 0;

    struct epoll_entry_t entry = {
        .events_ptr = (u64)args->events,
        .maxevents  = args->maxevents,
        .entry_ns   = bpf_ktime_get_ns(),
    };
    epoll_entry.update(&tid, &entry);
    return 0;
}

// Return: read what the kernel wrote into events[]
TRACEPOINT_PROBE(syscalls, sys_exit_epoll_wait) {
    u64 tid = bpf_get_current_pid_tgid();
    int n_events = args->ret;

    struct epoll_entry_t *entry = epoll_entry.lookup(&tid);
    if (!entry) return 0;
    epoll_entry.delete(&tid);

    if (n_events <= 0) return 0;   // timeout or error — skip

    struct trace_entry_t *ctx_entry = trace_context.lookup(&tid);
    if (!ctx_entry) return 0;

    struct epoll_result_t result = {};
    result.timestamp_ns = bpf_ktime_get_ns();
    result.pid          = tid >> 32;
    result.tid          = (u32)tid;
    result.n_events     = n_events;
    result.duration_ns  = result.timestamp_ns - entry->entry_ns;
    __builtin_memcpy(result.trace_id, ctx_entry->trace_id, 36);
    __builtin_memcpy(result.service,  ctx_entry->service,  32);

    // Read up to 8 ready fd/event pairs from userspace
    int count = n_events < 8 ? n_events : 8;
    result.fd_count = count;

    struct epoll_event_t ev;
    struct epoll_event_t __user *events_ptr = (struct epoll_event_t __user *)entry->events_ptr;

    #pragma unroll
    for (int i = 0; i < 8; i++) {
        if (i >= count) break;
        if (bpf_probe_read_user(&ev, sizeof(ev), events_ptr + i) == 0) {
            result.ready_fds[i]    = (u32)ev.data;
            result.ready_events[i] = ev.events;
        }
    }

    epoll_events.perf_submit(args, &result, sizeof(result));
    return 0;
}
"""


class _EpollKprobe:
    """
    kprobe-based observer for epoll_wait syscall.
    Captures when the asyncio event loop returns from kernel I/O wait
    and what file descriptors became ready.
    """

    def __init__(self, bridge) -> None:
        self._bridge = bridge
        self._bpf = None
        self._thread = None
        self._running = False
        self._our_pid = os.getpid()

    def start(self) -> bool:
        if not self._bridge.available:
            return False

        try:
            from bcc import BPF
        except ImportError:
            return False

        # Compile the epoll program together with the bridge so they
        # share the trace_context map by name
        full_program = _EPOLL_BPF_PROGRAM
        try:
            self._bpf = BPF(text=full_program)
            logger.info(
                "asyncio epoll kprobe loaded — observing sys_epoll_wait "
                "via tracepoints syscalls:sys_enter/exit_epoll_wait"
            )
        except Exception as exc:
            logger.warning(
                "asyncio epoll BPF compile failed: %s", exc
            )
            return False

        self._bpf["epoll_events"].open_perf_buffer(
            self._handle_epoll_event
        )
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="stacktracer-epoll-kprobe",
        )
        self._thread.start()
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
                logger.debug("epoll kprobe poll error: %s", exc)

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


# ====================================================================== #
# Layer 2 — sys.monitoring (Python 3.12+) / sys.setprofile (3.11)
# ====================================================================== #

# We only observe coroutines to keep overhead low.
# Non-coroutine function calls are not interesting for asyncio tracing.


def _is_coroutine_code(code) -> bool:
    """True if the code object belongs to a coroutine function."""
    import inspect

    # CO_COROUTINE flag = 0x100
    return bool(code.co_flags & 0x100)


def _setup_monitoring_312() -> bool:
    """
    Set up sys.monitoring for Python 3.12+.
    Uses PROFILER_ID slot — reserved for profilers, does not conflict
    with coverage tools or debuggers that use DEBUGGER_ID.
    """
    global _MONITORING_TOOL_ID

    if not hasattr(sys, "monitoring"):
        return False

    TOOL_ID = sys.monitoring.PROFILER_ID
    _MONITORING_TOOL_ID = TOOL_ID

    # Register for CALL and PY_RETURN on coroutine functions only
    try:
        sys.monitoring.set_events(
            TOOL_ID,
            sys.monitoring.events.CALL
            | sys.monitoring.events.PY_RETURN,
        )
    except Exception as exc:
        logger.warning(
            "sys.monitoring set_events failed: %s", exc
        )
        return False

    def on_call(code, offset: int, callable_: Any, arg0: Any):
        if not _is_coroutine_code(code):
            # Tell monitoring to disable this specific code object
            # so we don't pay callback overhead for non-coroutines again.
            return sys.monitoring.DISABLE

        trace_id = get_trace_id()
        if not trace_id:
            return

        emit(
            NormalizedEvent.now(
                probe="asyncio.loop.coro_call",
                trace_id=trace_id,
                service="asyncio",
                name=code.co_qualname,
                parent_span_id=get_span_id(),
                module=code.co_filename,
                source="sys.monitoring",
            )
        )

    def on_return(code, offset: int, retval: Any):
        if not _is_coroutine_code(code):
            return sys.monitoring.DISABLE

        trace_id = get_trace_id()
        if not trace_id:
            return

        emit(
            NormalizedEvent.now(
                probe="asyncio.loop.coro_return",
                trace_id=trace_id,
                service="asyncio",
                name=code.co_qualname,
                parent_span_id=get_span_id(),
                source="sys.monitoring",
            )
        )

    try:
        sys.monitoring.register_callback(
            TOOL_ID, sys.monitoring.events.CALL, on_call
        )
        sys.monitoring.register_callback(
            TOOL_ID, sys.monitoring.events.PY_RETURN, on_return
        )
        logger.info(
            "asyncio probe: sys.monitoring installed (Python 3.12+ CALL/PY_RETURN)"
        )
        return True
    except Exception as exc:
        logger.warning(
            "sys.monitoring register_callback failed: %s", exc
        )
        return False


def _teardown_monitoring_312() -> None:
    global _MONITORING_TOOL_ID
    if _MONITORING_TOOL_ID is None:
        return
    if not hasattr(sys, "monitoring"):
        return
    try:
        sys.monitoring.set_events(
            _MONITORING_TOOL_ID, sys.monitoring.events.NO_EVENTS
        )
        sys.monitoring.register_callback(
            _MONITORING_TOOL_ID, sys.monitoring.events.CALL, None
        )
        sys.monitoring.register_callback(
            _MONITORING_TOOL_ID,
            sys.monitoring.events.PY_RETURN,
            None,
        )
    except Exception:
        pass
    _MONITORING_TOOL_ID = None


def _setup_setprofile_311() -> bool:
    """
    sys.setprofile for Python 3.11.
    Lower overhead than settrace — fires only on call/return, not every line.
    Filters to coroutine functions only to minimise overhead.
    """
    original_profile = sys.getprofile()
    _originals["sys_profile"] = original_profile

    def _profile_callback(frame, event: str, arg: Any):
        if event not in ("call", "return"):
            return

        code = frame.f_code
        if not _is_coroutine_code(code):
            return

        trace_id = get_trace_id()
        if not trace_id:
            return

        probe_type = (
            "asyncio.loop.coro_call"
            if event == "call"
            else "asyncio.loop.coro_return"
        )
        emit(
            NormalizedEvent.now(
                probe=probe_type,
                trace_id=trace_id,
                service="asyncio",
                name=code.co_qualname,
                parent_span_id=get_span_id(),
                source="sys.setprofile",
            )
        )

        # Chain to existing profiler if present
        if original_profile:
            original_profile(frame, event, arg)

    sys.setprofile(_profile_callback)
    logger.info(
        "asyncio probe: sys.setprofile installed (Python 3.11)"
    )
    return True


def _teardown_setprofile_311() -> None:
    original = _originals.pop("sys_profile", None)
    sys.setprofile(original)


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

    def start(self) -> None:
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
        if minor >= 12:
            _setup_monitoring_312()
        else:
            _setup_setprofile_311()

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
        if minor >= 12:
            _teardown_monitoring_312()
        else:
            _teardown_setprofile_311()

        if "create_task" in _originals:
            asyncio.create_task = _originals.pop("create_task")

        _patched = False
        logger.info("asyncio probe removed")

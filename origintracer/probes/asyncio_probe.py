"""
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

Layer 2 - Patched Task._Task__step at the class level. Fragile on 3.12+
    because Task moved to a C extension. The _fut_waiter state -
    what future a task is blocked on. This is a genuinely useful
    thing, but it is also  not stable.

    This approach is not yet stable because it accesses private methods
    and attributes. It will be improved using sys.monitoring

Layer 3 — asyncio.create_task() wrap (public API, all versions)
    Wraps the public create_task() function — not a method on Task,
    not a private attribute. This is the approved way to observe
    task creation. Stable across all versions.


The epoll kprobe tells us WHICH FD became ready, and we can correlate
fd → socket → connection_type to understand what the task was waiting for
at the I/O level.

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


_original_step = None
_patched: bool = False


def _format_fut_waiter(task) -> str | None:
    fut = getattr(task, "_fut_waiter", None)
    if fut is None:
        return None
    return repr(fut)


def _patched_step(self, exc=None):
    trace_id = get_trace_id()

    if trace_id and _original_step:
        try:
            coro = self.get_coro()
            coro_name = getattr(
                coro, "__qualname__", type(coro).__name__
            )
            task_name = self.get_name()
            fut_waiter = _format_fut_waiter(self)

            loop = asyncio.get_event_loop()
            ready_count = len(getattr(loop, "_ready", []))
            sched_count = len(getattr(loop, "_scheduled", []))

            t0 = time.perf_counter()
            result = _original_step(self, exc)
            duration_ns = int((time.perf_counter() - t0) * 1e9)

            emit(
                NormalizedEvent.now(
                    probe="asyncio.loop.tick",
                    trace_id=trace_id,
                    service="asyncio",
                    name="loop.tick",
                    coro_name=coro_name,
                    parent_span_id=get_span_id(),
                    task_name=task_name,
                    fut_waiter=fut_waiter,
                    ready_queue_depth=ready_count,
                    scheduled_count=sched_count,
                    had_exception=exc is not None,
                    duration_ns=duration_ns,
                )
            )

            return result

        except Exception as probe_exc:
            logger.debug(
                "asyncio probe error in _step: %s", probe_exc
            )

    # fallback
    return _original_step(self, exc) if _original_step else None


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
    Observes the asyncio event loop.
    """

    name = "asyncio"

    def __init__(self) -> None:
        self._epoll_kprobe: Optional[_EpollKprobe] = None

    def start(
        self, observe_modules: Optional[List[str]] = None
    ) -> None:
        global _originals, _patched, _original_step
        if _patched:
            logger.warning(
                "asyncio probe already installed - skipping"
            )
            return

        # Layer 1: epoll kprobe
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
                "Coroutine-level tracing still active.",
                sys.platform,
                bridge.available,
            )

        # Layer 2: coroutine observation
        step = getattr(asyncio.Task, "_step", None)

        if step is None:
            # Cannot patch on this Python version
            logger.debug(
                "asyncio probe: Task._step not available, skipping patch"
            )
            return

        _original_step = step

        def wrapper(self, exc=None):
            return _patched_step(self, exc)

        asyncio.Task._step = wrapper

        # Layer 3: create_task
        _originals["create_task"] = asyncio.create_task
        asyncio.create_task = _make_create_task_wrapper(
            asyncio.create_task
        )

        _patched = True

    def stop(self) -> None:
        global _originals, _patched, _original_step

        if not _patched:
            return

        if _original_step:
            asyncio.Task._step = _original_step

        _original_step = None

        if self._epoll_kprobe:
            self._epoll_kprobe.stop()
            self._epoll_kprobe = None

        if "create_task" in _originals:
            asyncio.create_task = _originals.pop("create_task")

        _patched = False
        logger.info("asyncio probe removed")

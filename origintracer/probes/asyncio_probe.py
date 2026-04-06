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
fd >> socket >> connection_type to understand what the task was waiting for
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
from ..core.bpf_programs import BPFProgramPart, register_bpf
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

# ----- Asyncio-only BPF fragment -------------------

_ASYNCIO_EPOLL_BPF = r"""
// ********** epoll_wait enter **********
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

// *********** epoll_wait exit (asyncio-only) *****************
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
_originals: dict = {}


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

"""
probes/asyncio_probe.py

Observes the asyncio event loop internals across all Python versions.

Three patching layers, applied in combination:

Layer 1 — BaseEventLoop._run_once() patch (ALL Python versions)
    _run_once() is the core of the event loop. It is pure Python in
    ALL versions including 3.12 and 3.13 — only Task moved to C.

    Inside _run_once(), the critical line is:
        event_list = self._selector.select(timeout)

    This is where select()/epoll_wait() blocks until the kernel says
    "these file descriptors are ready". When it returns, event_list
    contains (SelectorKey, events_bitmask) pairs — one per ready fd.

    By wrapping _run_once() we observe:
        - How long select() blocked (loop idle time — was there work?)
        - How many I/O events the kernel returned per tick
        - Which fds became ready (and what futures they are attached to)
        - Ready queue depth before and after the tick
        - Time spent in _process_events() — callback dispatch overhead

    This is the most complete view of the event loop available from
    Python userspace on any version.

    New ProbeTypes:
        asyncio.loop.select         select()/epoll_wait() blocked for N ms,
                                    returned M events. The core I/O wait.
        asyncio.loop.run_once       full _run_once() cycle with all components

Layer 2 — Task.__step patch (Python 3.11 only, pure-Python Task)
    The name-mangled _Task__step method on the pure-Python Task class.
    Gives per-coroutine step events with coro name and fut_waiter state.
    Not available on 3.12+ because Task is a C extension there.

    New ProbeType:
        asyncio.loop.tick           coroutine stepped (one await resumed)

Layer 3 — eBPF uprobe on _asyncio.cpython-3XX.so (Python 3.12+, Linux only)
    Python 3.12+ ships Task as a C extension in _asyncio.so.
    The C function that implements Task.__step is:
        _asyncio_Task__step_impl   (the actual work function)
        _asyncio_Task__step        (the Python-callable wrapper around it)

    We attach uprobes to the .so to observe task stepping at the C level.
    We cannot read Python object fields (coro name, fut_waiter) from eBPF
    because they require GIL-aware Python object traversal. Instead we
    capture: pid, tid, timing (entry → return = step duration).

    Python-level context (coro name etc.) is correlated from the
    _run_once() ready queue snapshot taken just before task stepping begins.

    New ProbeType (same as layer 2, different source):
        asyncio.loop.tick           emitted per task step at C level

Layer 4 — asyncio.create_task() wrap (ALL Python versions)
    Public API, stable, cheap. Captures task creation events.
    New ProbeType:
        asyncio.task.create

Architecture of _run_once() internals (from asyncio/base_events.py):

    def _run_once(self):
        # 1. Compute how long to block in select()
        timeout = ...

        # 2. ← WE OBSERVE THIS:
        event_list = self._selector.select(timeout)
        #    event_list: List[(SelectorKey, events)]
        #    SelectorKey.fd    = file descriptor number
        #    SelectorKey.data  = the Handle/Future registered for this fd
        #    events            = selectors.EVENT_READ | selectors.EVENT_WRITE

        # 3. ← WE OBSERVE THIS:
        self._process_events(event_list)
        #    Iterates event_list, calls reader/writer callbacks
        #    Each callback schedules a Handle into self._ready

        # 4. Execute ready callbacks (tasks step here)
        ntodo = len(self._ready)
        for i in range(ntodo):
            handle = self._ready.popleft()
            handle._run()   # ← Task.__step fires from here on 3.11
                            # ← _asyncio_Task__step fires from here on 3.12+

The eBPF uprobe fires at point 4 from inside the C extension.
"""

from __future__ import annotations

import asyncio
import ctypes
import glob
import logging
import os
import selectors
import sys
import time
import threading
from asyncio import base_events, tasks
from typing import Any, Dict, List, Optional, Tuple

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent
from ..context.vars import get_trace_id, get_span_id

logger = logging.getLogger("stacktracer.probes.asyncio")

SUPPORTED_PYTHONS = ((3, 11), (3, 12), (3, 13))

# Module-level originals
_originals: dict = {}
_patched = False


# ====================================================================== #
# Utility
# ====================================================================== #

def _check_python_version() -> bool:
    major, minor = sys.version_info[:2]
    if (major, minor) not in SUPPORTED_PYTHONS:
        logger.warning("asyncio probe: unsupported Python %d.%d", major, minor)
        return False
    return True


def _has_pure_python_task() -> bool:
    """True on Python 3.11 where Task.__step is accessible."""
    return hasattr(tasks.Task, "_Task__step")


def _find_asyncio_so() -> Optional[str]:
    """
    Locate the _asyncio C extension shared library.
    Required for eBPF uprobe attachment on Python 3.12+.

    Returns the absolute path to something like:
        /usr/lib/python3.12/lib-dynload/_asyncio.cpython-312-x86_64-linux-gnu.so
    """
    # Try sysconfig first (most reliable)
    try:
        import sysconfig
        lib_dir = sysconfig.get_path("platstdlib")
        dynload = os.path.join(lib_dir, "lib-dynload")
        patterns = [
            os.path.join(dynload, "_asyncio*.so"),
            os.path.join(lib_dir, "_asyncio*.so"),
        ]
        for pattern in patterns:
            matches = glob.glob(pattern)
            if matches:
                return matches[0]
    except Exception:
        pass

    # Fallback: find via the _asyncio module itself
    try:
        import _asyncio
        path = getattr(_asyncio, "__file__", None)
        if path and os.path.exists(path):
            return path
    except ImportError:
        pass

    return None


def _event_list_summary(event_list) -> dict:
    """
    Summarize the (SelectorKey, events) list returned by selector.select().

    Returns a dict suitable for NormalizedEvent metadata:
        events_count        total I/O events returned by kernel
        read_events         count of READ-ready fds
        write_events        count of WRITE-ready fds
        fd_list             list of ready file descriptor numbers (bounded to 20)
    """
    read_count  = 0
    write_count = 0
    fd_list     = []

    for key, events in event_list:
        fd = key.fd
        if events & selectors.EVENT_READ:
            read_count += 1
        if events & selectors.EVENT_WRITE:
            write_count += 1
        if len(fd_list) < 20:
            fd_list.append(fd)

    return {
        "events_count": len(event_list),
        "read_events":  read_count,
        "write_events": write_count,
        "fd_sample":    fd_list,   # sample — not exhaustive for large event lists
    }


# ====================================================================== #
# Layer 1 — BaseEventLoop._run_once() patch (ALL versions)
# ====================================================================== #

def _make_run_once_wrapper(original_run_once):
    """
    Wraps BaseEventLoop._run_once() to observe the full event loop tick.

    The inner wrapper replaces self._selector.select() temporarily so we
    can measure how long the kernel took to return events and capture the
    event_list contents — without modifying the selector itself.

    This is the probe that surfaces:
        "loop blocked for 45ms in epoll_wait, kernel returned 3 ready fds,
         process_events dispatched 3 callbacks in 0.2ms,
         then 12 handles were in the ready queue"
    """

    def _traced_run_once(self):
        trace_id = get_trace_id()

        if not trace_id:
            return original_run_once(self)

        # Snapshot state BEFORE the tick
        ready_before    = len(getattr(self, "_ready", []))
        scheduled_count = len(getattr(self, "_scheduled", []))

        # We intercept self._selector.select() by wrapping the selector
        # temporarily. The wrapper records the select() duration and
        # captures the event_list returned by the kernel.
        original_select = self._selector.select
        select_duration_ns = [0]
        kernel_events     = [None]   # [event_list] written by wrapper

        def _capturing_select(timeout=None):
            t0 = time.perf_counter()
            result = original_select(timeout)
            select_duration_ns[0] = int((time.perf_counter() - t0) * 1e9)
            kernel_events[0] = result
            return result

        self._selector.select = _capturing_select

        t_run_once_start = time.perf_counter()
        try:
            original_run_once(self)
        finally:
            self._selector.select = original_select   # always restore

        run_once_duration_ns = int((time.perf_counter() - t_run_once_start) * 1e9)

        ready_after = len(getattr(self, "_ready", []))

        # ── Emit asyncio.loop.select — the kernel I/O wait ──
        # This is the event that answers: "what did epoll_wait return?"
        event_list = kernel_events[0] or []
        ev_summary = _event_list_summary(event_list)

        emit(NormalizedEvent.now(
            probe="asyncio.loop.select",
            trace_id=trace_id,
            service="asyncio",
            name="select",
            parent_span_id=get_span_id(),
            duration_ns=select_duration_ns[0],
            # How long the loop blocked waiting for I/O
            select_duration_ms=round(select_duration_ns[0] / 1e6, 3),
            # What the kernel returned
            **ev_summary,
        ))

        # ── Emit asyncio.loop.run_once — full tick summary ──
        emit(NormalizedEvent.now(
            probe="asyncio.loop.run_once",
            trace_id=trace_id,
            service="asyncio",
            name="run_once",
            parent_span_id=get_span_id(),
            duration_ns=run_once_duration_ns,
            select_duration_ns=select_duration_ns[0],
            process_events_ns=run_once_duration_ns - select_duration_ns[0],
            ready_before=ready_before,
            ready_after=ready_after,
            scheduled_count=scheduled_count,
            io_events_count=ev_summary["events_count"],
        ))

    return _traced_run_once


# ====================================================================== #
# Layer 2 — Task.__step patch (Python 3.11 pure-Python Task)
# ====================================================================== #

def _make_step_wrapper(original_step):
    """
    Closure-based wrapper for Task.__step (Python 3.11 only).
    Captures original_step in a closure — safe against stop() setting
    the global to None mid-execution (race condition in original code).
    """

    def _traced_step(self: asyncio.Task, exc: Optional[BaseException] = None):
        trace_id = get_trace_id()

        if trace_id:
            try:
                coro      = self.get_coro()
                coro_name = getattr(coro, "__qualname__", type(coro).__name__)
                task_name = self.get_name()

                # What this task was blocked on before this step
                waiter = getattr(self, "_fut_waiter", None)
                fut_waiter_repr = repr(waiter)[:120] if waiter is not None else None

                loop = self.get_loop()
                ready_count     = len(getattr(loop, "_ready", []))
                scheduled_count = len(getattr(loop, "_scheduled", []))

                emit(NormalizedEvent.now(
                    probe="asyncio.loop.tick",
                    trace_id=trace_id,
                    service="asyncio",
                    name=coro_name,
                    parent_span_id=get_span_id(),
                    task_name=task_name,
                    fut_waiter=fut_waiter_repr,
                    ready_queue_depth=ready_count,
                    scheduled_count=scheduled_count,
                    had_exception=exc is not None,
                    source="python_patch",
                ))
            except Exception as probe_exc:
                logger.debug("asyncio step probe error: %s", probe_exc)

        return original_step(self, exc)   # closure — never touches global

    return _traced_step


# ====================================================================== #
# Layer 3 — eBPF uprobe on _asyncio.so (Python 3.12+, Linux only)
# ====================================================================== #

# BPF program to attach to the C-level task step function.
#
# What we capture:
#   - pid, tid of the worker executing the step
#   - Entry timestamp (from bpf_ktime_get_ns)
#   - Return timestamp → duration of one task step at C level
#
# What we CANNOT capture from eBPF:
#   - Coroutine name (Python object, requires GIL traversal)
#   - fut_waiter state (same)
# These are correlated at the Python level via _run_once ready queue.

_ASYNCIO_BPF_PROGRAM = r"""
#include <uapi/linux/ptrace.h>

BPF_HASH(ts_task_step, u64, u64);   // tid → entry timestamp
BPF_PERF_OUTPUT(asyncio_events);

struct asyncio_event_t {
    u32 pid;
    u32 tid;
    u64 duration_ns;
    u8  had_exc;        // 1 if exc argument was not NULL
};

// Probe: _asyncio_Task__step entry
// Signature (from CPython _asynciomodule.c):
//   static PyObject *
//   task_step(TaskObj *task, PyObject *exc)
//
// arg0 = self (TaskObj*)
// arg1 = exc  (PyObject*) — NULL means no exception
int probe_task_step_entry(struct pt_regs *ctx) {
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts  = bpf_ktime_get_ns();
    ts_task_step.update(&tid, &ts);
    return 0;
}

int probe_task_step_return(struct pt_regs *ctx) {
    u64 tid    = bpf_get_current_pid_tgid();
    u64 *start = ts_task_step.lookup(&tid);
    if (!start) return 0;

    struct asyncio_event_t evt = {};
    evt.pid         = tid >> 32;
    evt.tid         = (u32)tid;
    evt.duration_ns = bpf_ktime_get_ns() - *start;
    ts_task_step.delete(&tid);
    asyncio_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}
"""

# Candidate symbol names for the C-level task step function.
# The exact name varies between CPython versions and build configurations.
# We try each in order and attach to the first one that exists in the .so.
_TASK_STEP_SYMBOLS = [
    "task_step",                          # most CPython builds
    "_asyncio_Task__step_impl",           # some builds expose the impl directly
    "_asyncio_Task__step",                # Python-callable wrapper name
    "TaskObj_step",                       # rare alternative
]


class _EBPFTaskStepProbe:
    """
    eBPF-backed observer for Task.__step on Python 3.12+.
    Attaches uprobes to the _asyncio.cpython-3XX.so shared library.
    """

    def __init__(self, asyncio_so_path: str, pid: int) -> None:
        self._so_path   = asyncio_so_path
        self._pid       = pid
        self._bpf       = None
        self._thread    = None
        self._running   = False

    def start(self) -> bool:
        """Returns True if successfully attached."""
        try:
            from bcc import BPF
        except ImportError:
            logger.info(
                "bcc not installed — eBPF task step probe unavailable. "
                "pip install bcc (Linux only, requires kernel headers)"
            )
            return False

        if os.geteuid() != 0:
            logger.info(
                "asyncio eBPF probe requires root or CAP_BPF. "
                "Running without task-level step visibility on Python 3.12+."
            )
            return False

        try:
            self._bpf = BPF(text=_ASYNCIO_BPF_PROGRAM)
        except Exception as exc:
            logger.warning("asyncio eBPF compile failed: %s", exc)
            return False

        attached = False
        for symbol in _TASK_STEP_SYMBOLS:
            try:
                self._bpf.attach_uprobe(
                    name=self._so_path,
                    sym=symbol,
                    fn_name="probe_task_step_entry",
                )
                self._bpf.attach_uretprobe(
                    name=self._so_path,
                    sym=symbol,
                    fn_name="probe_task_step_return",
                )
                logger.info(
                    "asyncio eBPF probe attached to %s::%s",
                    os.path.basename(self._so_path), symbol,
                )
                attached = True
                break
            except Exception:
                continue

        if not attached:
            logger.warning(
                "asyncio eBPF: could not attach to any task_step symbol in %s. "
                "Symbols tried: %s. "
                "Run: nm -D %s | grep -i task",
                self._so_path,
                _TASK_STEP_SYMBOLS,
                self._so_path,
            )
            return False

        self._bpf["asyncio_events"].open_perf_buffer(self._handle_event)
        self._running = True
        self._thread  = threading.Thread(
            target=self._poll_loop, daemon=True, name="stacktracer-asyncio-ebpf"
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
                logger.debug("asyncio eBPF poll error: %s", exc)

    def _handle_event(self, cpu: int, data: Any, size: int) -> None:
        if self._bpf is None:
            return
        try:
            event = self._bpf["asyncio_events"].event(data)
            if event.pid != self._pid:
                return   # filter to our process only

            trace_id = get_trace_id()
            if not trace_id:
                return

            emit(NormalizedEvent.now(
                probe="asyncio.loop.tick",
                trace_id=trace_id,
                service="asyncio",
                name="task_step_c",
                pid=event.pid,
                tid=event.tid,
                duration_ns=event.duration_ns,
                source="ebpf",
                # Coro name is NOT available here — it lives in the Python heap.
                # Correlate with asyncio.loop.run_once ready_queue snapshot
                # which fires just before this task is stepped.
            ))
        except Exception as exc:
            logger.debug("asyncio eBPF event handling error: %s", exc)


# ====================================================================== #
# Layer 4 — create_task wrap (ALL versions)
# ====================================================================== #

def _make_create_task_wrapper(original):
    def _traced_create_task(coro: Any, *, name: Optional[str] = None, context: Any = None):
        trace_id = get_trace_id()
        if trace_id:
            coro_name = getattr(coro, "__qualname__", type(coro).__name__)
            emit(NormalizedEvent.now(
                probe="asyncio.task.create",
                trace_id=trace_id,
                service="asyncio",
                name=coro_name,
                parent_span_id=get_span_id(),
                task_name=name,
            ))
        if context is not None:
            return original(coro, name=name, context=context)
        return original(coro, name=name)
    return _traced_create_task


# ====================================================================== #
# AsyncioProbe — unified entry point
# ====================================================================== #

class AsyncioProbe(BaseProbe):
    """
    Observes the asyncio event loop across all Python versions.

    Applied layers per version:

    Python 3.11:
        Layer 1 — _run_once() patch          (select visibility)
        Layer 2 — Task.__step patch           (per-coroutine steps)
        Layer 4 — create_task wrap            (task creation)

    Python 3.12+ without root/bcc:
        Layer 1 — _run_once() patch           (select visibility — still works)
        Layer 4 — create_task wrap            (task creation)
        [ Layer 3 skipped — no eBPF access ]

    Python 3.12+ with root + bcc (Linux):
        Layer 1 — _run_once() patch           (select visibility)
        Layer 3 — eBPF uprobe on _asyncio.so  (per-step timing at C level)
        Layer 4 — create_task wrap            (task creation)

    Layer 1 is the most valuable in all cases because it captures the
    exact line where the kernel returns I/O events to the event loop —
    the fundamental observable that shows whether the loop is healthy
    (select() returning quickly with work) or stalled (select() blocked
    for too long because all coroutines are stuck on blocking calls).
    """
    name = "asyncio"

    def __init__(self) -> None:
        self._ebpf_probe: Optional[_EBPFTaskStepProbe] = None

    def start(self) -> None:
        global _originals, _patched

        if not _check_python_version():
            return
        if _patched:
            logger.warning("asyncio probe already patched — skipping")
            return

        # ── Layer 1: _run_once (ALL versions) ──────────────────────────
        # BaseEventLoop._run_once is pure Python in all versions.
        # This is where select()/epoll_wait() lives.
        try:
            original_run_once = base_events.BaseEventLoop._run_once
            _originals["run_once"] = original_run_once
            base_events.BaseEventLoop._run_once = _make_run_once_wrapper(original_run_once)
            logger.info("asyncio probe: _run_once patched (select visibility active)")
        except AttributeError as exc:
            logger.warning("asyncio probe: could not patch _run_once: %s", exc)

        # ── Layer 2 or 3: Task stepping ────────────────────────────────
        if _has_pure_python_task():
            # Python 3.11 — Task.__step is accessible
            original_step = getattr(tasks.Task, "_Task__step")
            _originals["task_step"] = original_step
            setattr(tasks.Task, "_Task__step", _make_step_wrapper(original_step))
            logger.info(
                "asyncio probe: Task.__step patched (Python %d.%d pure-Python Task)",
                *sys.version_info[:2],
            )
        else:
            # Python 3.12+ — Task is a C extension
            # Try eBPF uprobe on _asyncio.so
            so_path = _find_asyncio_so()
            if so_path:
                logger.info("asyncio probe: found _asyncio.so at %s", so_path)
                self._ebpf_probe = _EBPFTaskStepProbe(so_path, os.getpid())
                attached = self._ebpf_probe.start()
                if not attached:
                    self._ebpf_probe = None
                    logger.info(
                        "asyncio probe: eBPF unavailable on Python %d.%d. "
                        "_run_once patch still provides loop-level visibility "
                        "(select duration, event count, ready queue depth).",
                        *sys.version_info[:2],
                    )
            else:
                logger.warning(
                    "asyncio probe: _asyncio.so not found. "
                    "Cannot attach eBPF uprobe for task-step visibility on Python %d.%d.",
                    *sys.version_info[:2],
                )

        # ── Layer 4: create_task (ALL versions) ────────────────────────
        _originals["create_task"] = asyncio.create_task
        asyncio.create_task = _make_create_task_wrapper(asyncio.create_task)

        _patched = True

    def stop(self) -> None:
        global _originals, _patched

        if not _patched:
            return

        # Restore _run_once
        if "run_once" in _originals:
            base_events.BaseEventLoop._run_once = _originals.pop("run_once")

        # Restore Task.__step (3.11 only)
        if "task_step" in _originals:
            setattr(tasks.Task, "_Task__step", _originals.pop("task_step"))

        # Stop eBPF probe (3.12+)
        if self._ebpf_probe is not None:
            self._ebpf_probe.stop()
            self._ebpf_probe = None

        # Restore create_task
        if "create_task" in _originals:
            asyncio.create_task = _originals.pop("create_task")

        _patched = False
        logger.info("asyncio probe removed")
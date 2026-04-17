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

    1. uname -r >>  kernel >= 4.9
    2. apt install linux-headers-$(uname -r) >>  headers present
    3. apt install python3-bpfcc >>  BCC installed at OS level
    4. .pth file written into venv >>  BCC visible inside venv
    5. sudo python -c "from bcc import BPF" >>  compile test passes
    6. gunicorn run as sudo >>  kprobes can attach

**************************** INSTALLATION COMPLETE ************************

Layer 1 - kprobe on sys_epoll_wait (Linux, all Python versions)
    The event loop calls epoll_wait() as a syscall. kprobe captures:
        - How long the loop blocked waiting for I/O
        - How many fds became ready
        - Which fd numbers and event types (EPOLLIN/EPOLLOUT)
    This is more accurate than patching _run_once() because it measures
    the actual kernel call, not a Python wrapper around it.

    Correlation with Python trace context via KprobeBridge:
        bridge writes (tid:trace_id) to BPF map when trace starts
        kprobe reads the map to attribute events to the right trace

Layer 2 - Patched Task._Task__step at the class level. Fragile on 3.12+
    because Task moved to a C extension. The _fut_waiter state -
    what future a task is blocked on. This is a genuinely useful
    thing, but it is also  not stable.

    This approach is not yet stable because it accesses private methods
    and attributes. It will be improved using sys.monitoring

Layer 3 - asyncio.create_task() wrap (public API, all versions)
    Wraps the public create_task() function - not a method on Task,
    not a private attribute. This is the approved way to observe
    task creation. Stable across all versions.


The epoll kprobe tells us WHICH FD became ready, and we can correlate
fd >> socket >> connection_type to understand what the task was waiting for
at the I/O level.

ProbeTypes:
    asyncio.loop.epoll_wait - epoll_wait returned with N ready fds
    asyncio.loop.tick - coroutine entered via Task.__step
    asyncio.task.create - create_task() called
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
from asyncio import base_events, tasks
from typing import Any, Callable, List, Optional

from ..context.vars import get_span_id, get_trace_id
from ..core.bpf_programs import BPFProgramPart, register_bpf
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..core.kprobe_bridge import get_bridge
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("origintracer.probes.asyncio")

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

# ---------------- Asyncio-only BPF fragment ----------------------

_ASYNCIO_EPOLL_BPF = r"""
/* --------------- epoll_wait enter ------------- */
// ── epoll_pwait enter (what uvicorn/gunicorn actually uses) ───────────────────
TRACEPOINT_PROBE(syscalls, sys_enter_epoll_pwait) {
    u64 pid_tid = bpf_get_current_pid_tgid();
    u64 ts = bpf_ktime_get_ns();
    asyncio_epoll_ts.update(&pid_tid, &ts);
    return 0;
}

// epoll_pwait exit (mirrors epoll_wait exit exactly)
TRACEPOINT_PROBE(syscalls, sys_exit_epoll_pwait) {
    u64 pid_tid = bpf_get_current_pid_tgid();
    u32 pid = (u32)(pid_tid >> 32);
    u32 tid = (u32)pid_tid;

    u64 *entry_ts = asyncio_epoll_ts.lookup(&pid_tid);
    if (!entry_ts) return 0;

    u64 now  = bpf_ktime_get_ns();
    u64 diff = now - *entry_ts;
    asyncio_epoll_ts.delete(&pid_tid);

    struct trace_entry_t *ctx = trace_context.lookup(&tid);
    if (!ctx) return 0;

    struct kernel_event_t ev = {};
    ev.timestamp_ns = now;
    ev.pid = pid;
    ev.tid = tid;
    ev.duration_ns = diff;
    ev.value1 = args->ret;
    __builtin_memcpy(ev.event_type, "epoll.wait", 11);
    __builtin_memcpy(ev.trace_id, ctx->trace_id, 36);
    __builtin_memcpy(ev.service, ctx->service,  32);
    kernel_events.perf_submit(args, &ev, sizeof(ev));
    return 0;
}
"""

# Uncomment ONLY when deploying without the nginx probe:
register_bpf(
    "asyncio",
    BPFProgramPart(
        maps=["BPF_HASH(asyncio_epoll_ts, u64, u64);"],
        probes=[_ASYNCIO_EPOLL_BPF],
    ),
)


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
                "asyncio epoll kprobe: bridge unavailable - skipping"
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
            name="origintracer-epoll-kprobe",
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
        if self._bridge.bpf is None:
            return
        try:
            ev = self._bridge.bpf["kernel_events"].event(data)

            event_type = ev.event_type.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")
            if event_type != "epoll.wait":
                return
            trace_id = ev.trace_id.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")
            service = ev.service.decode(
                "ascii", errors="replace"
            ).rstrip("\x00")

            if not trace_id:
                return

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
        except Exception as exc:
            logger.debug("epoll event handling error: %s", exc)


_patched: bool = False
_originals: dict = {}
_original_step: dict = {}

# -------------- Layer 2 — coroutine tracing ------------------------


def _has_pure_python_task() -> bool:
    """
    True on Python 3.11 where Task.__step is accessible.
    """
    return hasattr(tasks.Task, "_Task__step")


def _make_step_wrapper(original_step):
    """
    Closure-based wrapper for Task.__step that instruments every coroutine
    step taken by the asyncio event loop.

    asyncio.Task._step() is the internal method the event loop calls each
    time a coroutine is resumed (i.e. after an await completes). By wrapping
    it we get a hook into every coroutine scheduling decision without
    modifying user code.

    Python 3.11 and earlier ship asyncio.Task as a pure-Python class,
    so Task.__step is directly accessable and this layer activates
    automatically.

    Python 3.12 and later ship asyncio.Task as a C extension
    (_asyncio.Task) by default. Task.__step does not exist on the C class
    and _has_pure_python_task() returns False, so this layer is skipped
    unless you explicitly force the pure-Python implementation.

    To force the pure-Python Task before the event loop creates any tasks,
    modify the gunicorn ot_post_fork hook, which runs in each
    worker process before any async code starts:

        # gunicorn_probe.py
        def ot_post_fork(server, worker):
            import asyncio
            import asyncio.tasks as tasks
            if hasattr(tasks, "_PyTask"):
                tasks.Task = tasks._PyTask
                asyncio.Task = tasks._PyTask
            ...
            Existing code...

    This is a development configuration. For production deployments
    rely on the kprobe layer - it provides event-loop-level timing
    without CPython internals dependency.
    """

    def _traced_step(
        self: asyncio.Task, exc: Optional[BaseException] = None
    ):
        trace_id = get_trace_id()

        if trace_id:
            try:
                coro = self.get_coro()
                coro_name = getattr(
                    coro, "__qualname__", type(coro).__name__
                )
                task_name = self.get_name()

                # What this task was blocked on before this step
                waiter = getattr(self, "_fut_waiter", None)
                fut_waiter_repr = (
                    repr(waiter)[:120]
                    if waiter is not None
                    else None
                )

                loop = self.get_loop()
                ready_count = len(getattr(loop, "_ready", []))
                scheduled_count = len(
                    getattr(loop, "_scheduled", [])
                )

                emit(
                    NormalizedEvent.now(
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
                    )
                )
            except Exception as probe_exc:
                logger.debug(
                    "asyncio step probe error: %s", probe_exc
                )

        return original_step(self, exc)

    return _traced_step


# ------------------------ Layer 3 — asyncio.create_task() --------------


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


# -------------------------- AsyncioProbe ---------------------------


class AsyncioProbe(BaseProbe):
    """
    Observes the asyncio event loop.
    """

    name = "asyncio"

    def __init__(self) -> None:
        self._epoll_kprobe: Optional[_EpollKprobe] = None

    def start(self) -> None:

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
        if _has_pure_python_task():
            original_step = getattr(tasks.Task, "_Task__step")
            _original_step["task_step"] = original_step
            setattr(
                tasks.Task,
                "_Task__step",
                _make_step_wrapper(original_step),
            )
            logger.info(
                "asyncio probe: Task.__step patched (Python %d.%d pure-Python Task)",
                *sys.version_info[:2],
            )

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

        if "task_step" in _original_step:
            setattr(
                tasks.Task,
                "_Task__step",
                _original_step.pop("task_step"),
            )

        if "create_task" in _originals:
            asyncio.create_task = _originals.pop("create_task")

        if self._epoll_kprobe:
            self._epoll_kprobe.stop()
            self._epoll_kprobe = None

        _patched = False
        logger.info("asyncio probe removed")

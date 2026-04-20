"""
The correlation layer between kernel-level observations and Python trace context.

The Problem
-----------
kprobes fire inside the kernel — they know ``pid``, ``tid``, and kernel state,
but nothing about Python trace IDs, coroutine names, or Django view names.
Python code knows all of those but has no visibility into what the kernel is doing.

The Solution
------------
A shared BPF hash map bridges the two worlds. Python writes
``(pid, tid) -> trace_id`` into the map when a traced request starts.
Every kprobe handler reads from the same map - if it finds an entry for the
current ``(pid, tid)``, the kernel event is attributed to that trace.

This is the only connection between Python and the kernel. No patching.
No uprobe on Python internals. The boundary is a 36-byte string in a
kernel hash map.

BPF map structure::

    BPF_HASH(trace_context, u64, struct trace_entry)

    key = tid -- unique per thread
    value = { trace_id: char[36],
      start_ns: u64,
      service:  char[32]
    }

Python Usage
------------
::

    from origintracer.core.kprobe_bridge import KprobeBridge

    bridge = KprobeBridge()
    bridge.start() # loads the BPF program, creates the map

    # Called by context/vars.py set_trace():
    bridge.register_trace(trace_id="abc-123", service="django")

    # Called by context/vars.py reset_trace():
    bridge.unregister_trace()

    bridge.stop()

BPF Program Usage
-----------------
Include in every kprobe that needs Python context::

    struct trace_entry {
        char trace_id[36];
        u64  start_ns;
        char service[32];
    };

    BPF_HASH(trace_context, u64, struct trace_entry);

    // In any kprobe handler:
    u64 pid_tid = bpf_get_current_pid_tgid();
    u32 tid = pid_tid; // lower 32 bits is TID

    struct trace_entry *entry = trace_context.lookup(&tid);
    if (!entry) return 0; // not a traced request, skip

    // entry->trace_id is now the Python trace ID for attribution

Permissions
-----------
kprobes require ``CAP_BPF`` or root on Linux. On systems without this,
``bridge.start()`` returns ``False`` and all kprobe-based probes degrade
gracefully - Python-side observations (``sys.monitoring``, middleware)
still function normally.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from typing import Any, Optional

from ..core.bpf_programs import (
    BRIDGE_BPF_HEADER,
    build_bpf_program,
)

logger = logging.getLogger("origintracer.kprobe_bridge")


def bridge_bpf_header() -> str:
    """
    Returns the BPF header string to prepend to every kprobe BPF program.
    Ensures all probes share the same trace_context map layout and
    kernel_events perf output.
    """
    return BRIDGE_BPF_HEADER


class KprobeBridge:
    """
    Compiles and owns the single shared ``BPF()`` object.

    No probe-specific knowledge lives here. The bridge assembles all registered
    BPF fragments into one program, compiles it once, and exposes the result
    to probes.

    Attributes
    ----------
    bpf : BPF
        The compiled BPF program. Probes use this to attach tracepoints
        and open perf buffers.
    trace_map : BPF hash map
        Shared map for Python middleware to write ``(tid, trace_id)`` pairs.
        kprobe handlers read from this map to attribute kernel events to
        the correct Python trace.

    Startup Order
    -------------
    The following order is required:

    1. Import all probe modules - each calls ``register_bpf()`` at module level
    2. Call ``bridge.start()`` - compiles everything registered so far
    3. Call ``probe.start(bridge, ...)`` - attaches tracepoints, opens perf buffers

    Reversing steps 2 and 3 will compile an incomplete program missing any
    probes imported after ``start()``.
    """

    def __init__(self) -> None:
        self._bpf: Optional[Any] = None
        self._map: Optional[Any] = None
        self._available: bool = False
        self._lock = threading.Lock()

    @property
    def available(self) -> bool:
        return self._available

    @property
    def bpf(self):
        """
        The compiled BPF object - all probes use this to attach and poll.
        """
        return self._bpf

    @property
    def trace_map(self):
        """
        The trace_context BPF hash map — Python writes trace IDs here.
        """
        return self._map

    @contextlib.contextmanager
    def log_ebpf_compilation(self):
        # To view eBPF compilation warnings and erros
        log = open("/tmp/bpf_warnings.log", "w")
        old_stderr = os.dup(2)
        os.dup2(log.fileno(), 2)
        try:
            yield
        finally:
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
            log.close()

    def start(self) -> bool:
        """
        Assemble and compile the combined BPF program.

        Must be called **after** all probe modules have been imported and
        **before** any probe's ``start()`` method. See startup order in the
        class docstring.

        Returns
        -------
        bool
            ``True`` on successful compilation. ``False`` — without raising —
            in any of these cases:

            - Not running on Linux
            - ``bcc`` not installed
            - Insufficient permissions (no ``CAP_BPF`` or root)
            - BPF compilation error (logged as warning)

            Callers should check the return value. A ``False`` result means all
            kprobe-based probes will degrade gracefully - Python-side observations
            still function normally.
        """
        if os.name != "posix":
            logger.info(
                "kprobe bridge: not on Linux — kernel probes unavailable"
            )
            return False

        try:
            from bcc import BPF
        except ImportError:
            logger.info(
                "kprobe bridge: bcc not installed — kernel probes unavailable"
            )
            return False

        if os.geteuid() != 0:
            logger.info(
                "kprobe bridge: requires root or CAP_BPF — "
                "kernel probes unavailable, Python-side observations still active"
            )
            return False

        program = build_bpf_program()

        print("Compiling eBPF probes...", flush=True)
        try:
            with self.log_ebpf_compilation():
                self._bpf = BPF(text=program)
            self._map = self._bpf["trace_context"]
            self._available = True
            logger.info("kprobe bridge: compiled successfully")
            return True
        except Exception as exc:
            print(
                "eBPF compilation failed - see /tmp/bpf_warnings.log for details",
                flush=True,
            )
            logger.warning(
                "kprobe bridge: BPF compilation failed: %s", exc
            )
            return False

    def stop(self) -> None:
        self._available = False
        self._bpf = None
        self._map = None

    # Called by Django/uvicorn middleware at request boundaries.
    # Writes into the BPF trace_context map so kernel probes can attribute
    # syscalls to the correct application trace.
    def register_trace(
        self,
        tid: int,
        trace_id: str,
        service: str,
        start_ns: int,
        pid: int,
    ) -> None:
        if not self._available or self._map is None:
            return
        try:
            entry = self._map.Leaf(
                trace_id=trace_id.encode()[:36],
                start_ns=start_ns,
                service=service.encode()[:32],
                pid=pid,
                tid=tid,
            )
            self._map[self._map.Key(tid)] = entry
        except Exception as exc:
            logger.debug(
                "kprobe bridge: register_trace failed: %s", exc
            )

    def unregister_trace(self, tid: int) -> None:
        if not self._available or self._map is None:
            return
        try:
            del self._map[self._map.Key(tid)]
        except Exception:
            pass


# Created by Engine, used by all kprobe probes
_bridge: Optional[KprobeBridge] = None
_bridge_lock = threading.Lock()


def get_bridge() -> "KprobeBridge":
    """
    Return the process-wide singleton KprobeBridge instance.
    """
    global _bridge
    if _bridge is None:
        with _bridge_lock:
            if _bridge is None:
                _bridge = KprobeBridge()
    return _bridge

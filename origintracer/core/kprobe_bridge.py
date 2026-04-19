"""
The correlation layer between kernel-level observations and Python trace context.

Issue description:
    kprobes fire inside the kernel. They know pid, tid, and kernel state.
    They do not know Python trace IDs, coroutine names, or Django view names.
    Python code knows all of those but nothing about what the kernel is doing.

Proposed solution - a shared BPF hash map:
    Python writes (pid, tid):trace_id into a BPF hash map when a traced
    request starts. Every kprobe handler reads from the same map. If it finds
    an entry for the current (pid, tid), the kernel event is attributed to
    that trace.

    This is the only connection between Python and the kernel. No patching.
    No uprobe on Python internals. The boundary is a 36-byte string in a
    kernel hash map.

    Structure:
        BPF_HASH(trace_context, u64, struct trace_entry)
        key = tid - unique per thread
        value = { trace_id: char[36], start_ns: u64, service: char[32] }

Usage - Python side:
    from origintracer.core.kprobe_bridge import KprobeBridge

    bridge = KprobeBridge()
    bridge.start() # loads the BPF program, creates the map

    # Called by context/vars.py set_trace():
    bridge.register_trace(trace_id="abc-123", service="django")

    # Called by context/vars.py reset_trace():
    bridge.unregister_trace()

    bridge.stop()

Usage (BPF program side) - include in every kprobe that needs Python context:

    // shared map defined once in kprobe_bridge BPF program
    // other BPF programs reference it by map name via BPF map pinning
    // or we compile them together.

    struct trace_entry {
        char trace_id[36];
        u64  start_ns;
        char service[32];
    };

    BPF_HASH(trace_context, u64, struct trace_entry);

    // In any kprobe handler:
    u64 pid_tid = bpf_get_current_pid_tgid();
    u32 tid = pid_tid; // Lower 32 bits is TID
    struct trace_entry *entry = trace_context.lookup(&tid);
    if (!entry) return 0;   // not a traced request, skip
    // entry->trace_id is now the Python trace ID for attribution

Permissions:
    kprobes require CAP_BPF or root on Linux.
    On systems without this, the bridge.start() returns False and
    all kprobe-based probes degrade gracefully - the Python-side
    observations (sys.monitoring, middleware) still function normally.
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
    Compiles and owns the single shared BPF() object.

    No probe-specific knowledge lives here. The bridge simply:
      - calls build_bpf_program() to get the assembled C source
      - compiles it with BPF()
      - exposes bridge.bpf so probes can attach tracepoints/open buffers
      - exposes bridge.trace_map so Python middleware can write trace IDs

    Startup contract:
      1. Import all probe modules  >> each calls register_bpf() at module level
      2. bridge.start() >> compiles everything registered so far
      3. probe.start(bridge, ...)  >> attaches tracepoints, opens perf buffers
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

        Returns True on success, False (without raising) if:
          - not on POSIX / not Linux
          - bcc not installed
          - not root / no CAP_BPF
          - BPF compilation fails (logged as warning)

        Must be called AFTER all probe modules have been imported,
        and BEFORE any probe's start() method.
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

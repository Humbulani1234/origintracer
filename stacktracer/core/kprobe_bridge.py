"""
The correlation layer between kernel-level observations and Python trace context.

Problem:
    kprobes fire inside the kernel. They know pid, tid, and kernel state.
    They do not know Python trace IDs, coroutine names, or Django view names.
    Python code knows all of those but nothing about what the kernel is doing.

Solution - a shared BPF hash map:
    Python writes (pid, tid) → trace_id into a BPF hash map when a traced
    request starts. Every kprobe handler reads from the same map. If it finds
    an entry for the current (pid, tid), the kernel event is attributed to
    that trace.

    This is the only connection between Python and the kernel. No patching.
    No uprobe on Python internals. The boundary is a 36-byte string in a
    kernel hash map.

    Structure:
        BPF_HASH(trace_context, u64, struct trace_entry)
        key   = tid — unique per thread
        value = { trace_id: char[36], start_ns: u64, service: char[32] }

Usage - Python side:
    from stacktracer.core.kprobe_bridge import KprobeBridge

    bridge = KprobeBridge()
    bridge.start()           # loads the BPF program, creates the map

    # Called by context/vars.py set_trace():
    bridge.register_trace(trace_id="abc-123", service="django")

    # Called by context/vars.py reset_trace():
    bridge.unregister_trace()

    bridge.stop()

Usage (BPF program side) — include in every kprobe that needs Python context:

    // shared map defined once in kprobe_bridge BPF program
    // other BPF programs reference it by map name via BPF map pinning
    // or we compile them together (preferred for simplicity)

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

Shared map approach:
    We compile one BPF program that contains the trace_context map.
    The epoll probe, TCP probe, and any other kprobe are compiled as
    part of the same BPF object so they share the map by name.
    This avoids map pinning complexity for the MVP.

Permissions:
    kprobes require CAP_BPF or root on Linux.
    On systems without this, the bridge.start() returns False and
    all kprobe-based probes degrade gracefully — the Python-side
    observations (sys.monitoring, middleware) still function normally.
"""

from __future__ import annotations

import ctypes
import logging
import os
import threading
from typing import Optional

from stacktracer.core.bpf_programs import (
    BRIDGE_BPF_HEADER,
    FULL_BPF_PROGRAM,
)

logger = logging.getLogger("stacktracer.kprobe_bridge")

# The BPF program that creates the shared trace_context map.
# This program itself does not attach to any probe — it only
# defines the map. Individual probe BPF programs reference it.
#
# We also define common helper structs here so all probes
# compiled with bridge_bpf_header() share the same layout.


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

    All kprobe-based probes (nginx, asyncio, tcp, ...) receive this bridge
    and interact with the kernel via bridge.bpf — they do NOT create their
    own BPF() objects.

    Lifecycle:
        bridge = get_bridge()
        bridge.start()           # compile + load BPF, must be called before any probe
        nginx_probe.start()      # attaches accept tracepoints, opens nginx buffer
        asyncio_probe.start()    # opens epoll buffer — no attachment needed
        ...
        bridge.stop()            # cleanup
    """

    def __init__(self) -> None:
        self._bpf: Optional[object] = None
        self._map = None  # trace_context BPF hash map
        self._available: bool = False
        self._lock = threading.Lock()

    # ── public API ────────────────────────────────────────────────────────────

    @property
    def available(self) -> bool:
        return self._available

    @property
    def bpf(self):
        """The compiled BPF object. All probes use this to attach and open buffers."""
        return self._bpf

    @property
    def trace_map(self):
        """The trace_context BPF hash map. Python writes trace IDs here."""
        return self._map

    def start(self) -> bool:
        """
        Compile and load the combined BPF program.

        Returns True if loaded successfully, False if unavailable
        (no bcc, not root, unsupported kernel).

        Must be called before any probe's start().
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

        try:
            self._bpf = BPF(text=FULL_BPF_PROGRAM)
            self._map = self._bpf["trace_context"]
            self._available = True
            logger.info(
                "kprobe bridge loaded — single BPF object ready"
            )
            return True
        except Exception as exc:
            logger.warning(
                "kprobe bridge failed to load: %s", exc
            )
            return False

    def stop(self) -> None:
        self._available = False
        self._bpf = None
        self._map = None

    # ── trace context helpers (called from Python middleware) ─────────────────

    def register_trace(
        self,
        tid: int,
        trace_id: str,
        service: str,
        start_ns: int,
        pid: int,
    ) -> None:
        """
        Write a trace context entry into the BPF map so kernel probes
        can attribute syscalls to this trace.

        Called by Django/uvicorn middleware at request start.
        """
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
        """
        Remove a trace context entry from the BPF map.

        Called by Django/uvicorn middleware at request end.
        """
        if not self._available or self._map is None:
            return
        try:
            del self._map[self._map.Key(tid)]
        except Exception:
            pass


# Global singleton — created by Engine, used by all kprobe probes
_bridge: Optional[KprobeBridge] = None
_bridge_lock = threading.Lock()


def get_bridge() -> "KprobeBridge":
    """Return the process-wide singleton KprobeBridge instance."""
    global _bridge
    if _bridge is None:
        with _bridge_lock:
            if _bridge is None:
                _bridge = KprobeBridge()
    return _bridge

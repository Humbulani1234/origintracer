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

logger = logging.getLogger("stacktracer.kprobe_bridge")

# The BPF program that creates the shared trace_context map.
# This program itself does not attach to any probe — it only
# defines the map. Individual probe BPF programs reference it.
#
# We also define common helper structs here so all probes
# compiled with bridge_bpf_header() share the same layout.

BRIDGE_BPF_HEADER = r"""
#include <uapi/linux/ptrace.h>
#include <linux/sched.h>
#include <net/sock.h>

// ── Shared trace context map ──────────────────────────────────────────
// Written by Python via BPF map update fd.
// Read by every kprobe handler to attribute kernel events to traces.

struct trace_entry_t {
    char trace_id[36];    // UUID string: "550e8400-e29b-41d4-a716-446655440000"
    u64  start_ns;        // bpf_ktime_get_ns() when trace started
    char service[32];     // "django", "celery", etc.
    u32  pid;
    u32  tid;
};

// Map pinned to /sys/fs/bpf/trace_context for sharing across processes
BPF_HASH(trace_context, u32, struct trace_entry_t, 65536);

// Convenience macro: look up current thread's trace context
// Usage: LOOKUP_TRACE(entry);  if (!entry) return 0;
#define LOOKUP_TRACE(entry_var) \
    u64 _pid_tid = bpf_get_current_pid_tgid(); \
    u32 _tid = _pid_tid; \
    struct trace_entry_t *entry_var = trace_context.lookup(&_tid);

// ── Shared output events ──────────────────────────────────────────────

struct kernel_event_t {
    u64  timestamp_ns;    // bpf_ktime_get_ns()
    u32  pid;
    u32  tid;
    char trace_id[36];
    char service[32];
    char event_type[32];  // "epoll.wait", "tcp.send", "connect", etc.

    // Numeric payload — interpretation depends on event_type
    s64  value1;          // epoll: n_events returned; tcp: bytes; etc.
    s64  value2;          // epoll: fd count; tcp: 0; etc.
    u64  duration_ns;     // entry→return duration

    // Socket info (populated for TCP events)
    u32  saddr;           // source IP (network byte order)
    u32  daddr;           // dest IP
    u16  sport;
    u16  dport;
};

BPF_PERF_OUTPUT(kernel_events);
"""


def bridge_bpf_header() -> str:
    """
    Returns the BPF header string to prepend to every kprobe BPF program.
    Ensures all probes share the same trace_context map layout and
    kernel_events perf output.
    """
    return BRIDGE_BPF_HEADER


class KprobeBridge:
    """
    Manages the shared BPF trace context map and provides the
    Python-side API for registering and unregistering traces.

    One instance is created by Engine at startup and shared with
    all kprobe-based probes (asyncio_kprobe, tcp_kprobe, etc.).
    """

    def __init__(self) -> None:
        self._bpf: Optional[object] = None
        self._map = None
        self._available: bool = False
        self._lock = threading.Lock()

    def start(self) -> bool:
        """
        Load the bridge BPF program and get a reference to the
        trace_context map for Python-side updates.

        Returns True if successfully loaded, False if unavailable
        (no bcc, no root, unsupported kernel).
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
                "kprobe bridge: bcc not installed — kernel probes unavailable. "
                "pip install bcc  (Linux only, requires kernel headers)"
            )
            return False

        if os.geteuid() != 0:
            logger.info(
                "kprobe bridge: requires root or CAP_BPF — kernel probes unavailable. "
                "Python-side observations (sys.monitoring, middleware) still active."
            )
            return False

        # The bridge program defines the shared map but attaches no probes
        bridge_program = (
            BRIDGE_BPF_HEADER
            + """
// Dummy kprobe so bcc compiles and loads the program
// We need the map to be live; this probe does nothing useful
int _bridge_noop(struct pt_regs *ctx) { return 0; }
"""
        )
        try:
            self._bpf = BPF(text=bridge_program)
            self._map = self._bpf["trace_context"]

            # PINNING: Pin the map to a shared filesystem location
            pin_path = "/sys/fs/bpf/trace_context"
            try:
                self._map.pin(pin_path)
            except Exception:
                # If already pinned, just load it
                self._map = BPF.get_table(
                    "trace_context", path=pin_path
                )

            self._available = True
            logger.info(
                "kprobe bridge loaded — trace_context map ready and pinned"
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

    @property
    def available(self) -> bool:
        return self._available

    @property
    def bpf(self):
        """The BPF object — passed to kprobe programs that share this map."""
        return self._bpf

    def register_trace(
        self,
        trace_id: str,
        service: str = "python",
    ) -> None:
        """
        Write the current thread's trace context into the BPF map.
        Called by context/vars.py set_trace() on every traced request.

        Non-blocking. If the bridge is unavailable, this is a no-op.
        """
        if not self._available or self._map is None:
            return

        try:
            import ctypes

            # Robustness: Get actual OS Thread ID to match BPF bpf_get_current_pid_tgid()
            if hasattr(os, "gettid"):
                tid = os.gettid()
            else:
                libc = ctypes.CDLL("libc.so.6")
                tid = libc.syscall(186)  # __NR_gettid

            key = ctypes.c_uint32(tid)

            # Build the C struct layout matching struct trace_entry_t
            class TraceEntry(ctypes.Structure):
                _fields_ = [
                    ("trace_id", ctypes.c_char * 36),
                    ("start_ns", ctypes.c_uint64),
                    ("service", ctypes.c_char * 32),
                    ("pid", ctypes.c_uint32),
                    ("tid", ctypes.c_uint32),
                ]

            import time

            entry = TraceEntry(
                trace_id=trace_id.encode("ascii")[:35],
                start_ns=int(time.perf_counter() * 1e9),
                service=service.encode("ascii")[:31],
                pid=os.getpid(),
                tid=tid,
            )

            self._map[key] = entry

        except Exception as exc:
            logger.debug(
                "kprobe bridge register_trace error: %s", exc
            )

    def unregister_trace(self) -> None:
        """
        Remove the current thread's trace context from the BPF map.
        Called by context/vars.py reset_trace().
        """
        if not self._available or self._map is None:
            return

        try:
            # Robustness: Get actual OS Thread ID to match BPF bpf_get_current_pid_tgid()
            if hasattr(os, "gettid"):
                tid = os.gettid()
            else:
                libc = ctypes.CDLL("libc.so.6")
                tid = libc.syscall(186)  # __NR_gettid

            key = ctypes.c_uint32(tid)
            self._map.__delitem__(key)
        except Exception:
            pass  # key already gone — fine


# Global singleton — created by Engine, used by all kprobe probes
_bridge: Optional[KprobeBridge] = None


def get_bridge() -> KprobeBridge:
    global _bridge
    if _bridge is None:
        _bridge = KprobeBridge()
    return _bridge

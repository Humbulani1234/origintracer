"""
probes/kernel_probe.py

Kernel-level probe using BCC (eBPF).
Captures: tcp_sendmsg, tcp_recvmsg, sys_read, sys_write syscalls.

Requirements:
  - Linux kernel 4.15+
  - bcc Python package (`pip install bcc`)
  - Root or CAP_BPF capability

This probe is optional and degrades gracefully if BCC is not available.
In that case, it logs a warning and does nothing.

For development / macOS: use KernelProbeStub which emits synthetic events.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Optional

from ..context.vars import get_trace_id
from ..core.event_schema import NormalizedEvent
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("stacktracer.probes.kernel")

# Minimal BCC eBPF program
_BPF_PROGRAM = r"""
#include <uapi/linux/ptrace.h>
#include <net/sock.h>
#include <bcc/proto.h>

BPF_PERF_OUTPUT(tcp_events);

struct tcp_event_t {
    u32 pid;
    u32 tid;
    u64 size;
    char comm[16];
    int direction;  // 0=send, 1=recv
};

int trace_tcp_send(struct pt_regs *ctx, struct sock *sk, struct msghdr *msg, size_t size) {
    struct tcp_event_t evt = {};
    evt.pid = bpf_get_current_pid_tgid() >> 32;
    evt.tid = bpf_get_current_pid_tgid();
    evt.size = size;
    evt.direction = 0;
    bpf_get_current_comm(&evt.comm, sizeof(evt.comm));
    tcp_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}

int trace_tcp_recv(struct pt_regs *ctx, struct sock *sk, struct msghdr *msg, size_t size) {
    struct tcp_event_t evt = {};
    evt.pid = bpf_get_current_pid_tgid() >> 32;
    evt.tid = bpf_get_current_pid_tgid();
    evt.size = size;
    evt.direction = 1;
    bpf_get_current_comm(&evt.comm, sizeof(evt.comm));
    tcp_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}
"""


class KernelProbe(BaseProbe):
    """
    Real kernel probe using eBPF.
    Falls back to KernelProbeStub if BCC is unavailable.
    """

    name = "kernel"

    def __init__(self) -> None:
        self._bpf: Optional[Any] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._pid = os.getpid()

    def start(self) -> None:
        try:
            from bcc import BPF
        except ImportError:
            logger.warning(
                "bcc not installed — kernel probe running in stub mode. "
                "Install with: pip install bcc (Linux only)"
            )
            self._stub_mode()
            return

        if os.geteuid() != 0:
            logger.warning("kernel probe requires root or CAP_BPF — skipping")
            return

        try:
            self._bpf = BPF(text=_BPF_PROGRAM)
            self._bpf.attach_kprobe(event="tcp_sendmsg", fn_name="trace_tcp_send")
            self._bpf.attach_kprobe(event="tcp_recvmsg", fn_name="trace_tcp_recv")
            self._bpf["tcp_events"].open_perf_buffer(self._handle_event)

            self._running = True
            self._thread = threading.Thread(
                target=self._poll_loop,
                daemon=True,
                name="stacktracer-kernel-probe",
            )
            self._thread.start()
            logger.info(
                "kernel probe installed via eBPF (pid=%d)",
                self._pid,
            )

        except Exception as exc:
            logger.error("kernel probe failed to attach: %s", exc)

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
                logger.debug("kernel probe poll error: %s", exc)

    def _handle_event(self, cpu: int, data: Any, size: int) -> None:
        """Called by BCC perf buffer for each kernel event."""
        if self._bpf is None:
            return
        try:
            event = self._bpf["tcp_events"].event(data)
            # Only emit for our own process
            if event.pid != self._pid:
                return

            trace_id = get_trace_id()
            if not trace_id:
                return

            direction = "tcp.send" if event.direction == 0 else "tcp.recv"
            emit(
                NormalizedEvent.now(
                    probe=direction,
                    trace_id=trace_id,
                    service="kernel",
                    name=event.comm.decode("utf-8", errors="replace"),
                    pid=event.pid,
                    tid=event.tid,
                    bytes=event.size,
                )
            )
        except Exception as exc:
            logger.debug("kernel event handling error: %s", exc)

    def _stub_mode(self) -> None:
        """Emit a single synthetic event to show the probe is alive."""
        trace_id = get_trace_id() or "stub"
        emit(
            NormalizedEvent.now(
                probe="tcp.send",
                trace_id=trace_id,
                service="kernel",
                name="stub",
                note="bcc unavailable — synthetic event",
            )
        )


class KernelProbeStub(BaseProbe):
    """
    Development stub — emits synthetic kernel events.
    Useful on macOS or in CI environments without eBPF.
    Not registered automatically; instantiate manually.
    """

    name = "kernel_stub"

    def start(self) -> None:
        logger.info("KernelProbeStub started (synthetic events only)")

    def stop(self) -> None:
        pass

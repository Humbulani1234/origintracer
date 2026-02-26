# myapp/probes/psycopg2_ebpf_probe.py

from stacktracer.sdk.base_probe import BaseProbe
from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.context.vars import get_trace_id
import os, threading, time, logging

logger = logging.getLogger(__name__)

# eBPF program: attach to psycopg2's libpq PQexec (the blocking query call)
_BPF_PROGRAM = r"""
#include <uapi/linux/ptrace.h>

BPF_PERF_OUTPUT(pg_events);
BPF_HASH(query_start, u64, u64);    // tid → start time

struct pg_event_t {
    u32 pid;
    u32 tid;
    u64 duration_ns;
    char query[128];
};

// Fires when PQexec is entered — record start time
int trace_pg_enter(struct pt_regs *ctx) {
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts = bpf_ktime_get_ns();
    query_start.update(&tid, &ts);
    return 0;
}

// Fires when PQexec returns — compute duration
int trace_pg_return(struct pt_regs *ctx) {
    u64 tid = bpf_get_current_pid_tgid();
    u64 *start = query_start.lookup(&tid);
    if (!start) return 0;

    struct pg_event_t evt = {};
    evt.pid = tid >> 32;
    evt.tid = (u32)tid;
    evt.duration_ns = bpf_ktime_get_ns() - *start;
    query_start.delete(&tid);

    pg_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}
"""


class Psycopg2EBPFProbe(BaseProbe):
    """
    Attaches uprobes to psycopg2's libpq PQexec.
    Captures: which thread, how long the blocking call took.
    Works regardless of async framework — eBPF sees the kernel level.
    """
    name = "psycopg2_ebpf"

    def __init__(self) -> None:
        self._bpf = None
        self._thread = None
        self._running = False
        self._pid = os.getpid()

    def start(self) -> None:
        try:
            from bcc import BPF
        except ImportError:
            logger.warning("bcc not available — psycopg2_ebpf probe inactive")
            return

        import ctypes, subprocess

        # Find libpq path
        try:
            result = subprocess.run(
                ["python3", "-c", "import ctypes.util; print(ctypes.util.find_library('pq'))"],
                capture_output=True, text=True
            )
            libpq_path = f"/usr/lib/{result.stdout.strip()}"
        except Exception:
            libpq_path = "/usr/lib/x86_64-linux-gnu/libpq.so.5"

        try:
            self._bpf = BPF(text=_BPF_PROGRAM)
            self._bpf.attach_uprobe(
                name=libpq_path, sym="PQexec", fn_name="trace_pg_enter"
            )
            self._bpf.attach_uretprobe(
                name=libpq_path, sym="PQexec", fn_name="trace_pg_return"
            )
            self._bpf["pg_events"].open_perf_buffer(self._handle_event)

            self._running = True
            self._thread = threading.Thread(
                target=self._poll, daemon=True, name="psycopg2-ebpf"
            )
            self._thread.start()
            logger.info("psycopg2_ebpf probe attached to PQexec in %s", libpq_path)

        except Exception as exc:
            logger.error("psycopg2_ebpf probe failed: %s", exc)

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _poll(self) -> None:
        while self._running and self._bpf:
            self._bpf.perf_buffer_poll(timeout=100)

    def _handle_event(self, cpu, data, size) -> None:
        if not self._bpf:
            return
        try:
            evt = self._bpf["pg_events"].event(data)
            if evt.pid != self._pid:
                return

            trace_id = get_trace_id()
            if not trace_id:
                return

            duration_ns = evt.duration_ns

            emit(NormalizedEvent.now(
                probe="db.query.start",
                trace_id=trace_id,
                service="postgres",
                name="PQexec",              # kernel-level — no query text at this layer
                duration_ns=duration_ns,
                pid=evt.pid,
                tid=evt.tid,
                # The key signal: was this called from a Celery worker thread
                # inside what should be an async context?
                blocking_call=True,
                duration_ms=round(duration_ns / 1e6, 3),
            ))

        except Exception as exc:
            logger.debug("psycopg2_ebpf event error: %s", exc)
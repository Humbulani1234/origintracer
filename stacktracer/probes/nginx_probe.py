"""
probes/nginx_probe.py

Nginx probe with two modes:

Mode A — eBPF uprobes (preferred, requires bcc + root + Linux)
    Attaches directly to nginx worker process symbols.
    Gives sub-millisecond visibility into the nginx request lifecycle
    without touching nginx config or adding log overhead.

    Symbols probed:
        ngx_http_wait_request_handler   connection accepted, waiting for first byte
        ngx_http_init_request           first byte received, request parsing begins
        ngx_http_handler                request parsed, routing to location block
        ngx_recv                        reading client socket data
        ngx_add_event                   registering upstream write with epoll
        ngx_epoll_process_events        nginx epoll tick — events per loop iteration

Mode B — JSON access log tail (fallback, no privileges needed)
    Tails /var/log/nginx/access.log expecting JSON format.
    Gives per-request visibility with no kernel access required.
    Coarser than Mode A — one event per request, not per lifecycle stage.

    Enable JSON logging in nginx:
        log_format json_combined escape=json '{'
            '"time":"$time_iso8601",'
            '"method":"$request_method",'
            '"uri":"$uri",'
            '"status":$status,'
            '"request_time":$request_time,'
            '"upstream_response_time":"$upstream_response_time"'
        '}';
        access_log /var/log/nginx/access.log json_combined;

New ProbeTypes introduced (add to core/event_schema.py):
    nginx.connection.accept     — connection accepted by worker, waiting first byte
    nginx.request.parse         — HTTP request parsing started
    nginx.request.route         — request routed to location block
    nginx.recv                  — nginx reading bytes from client socket
    nginx.upstream.dispatch     — nginx dispatching to upstream (your Django/FastAPI)
    nginx.epoll.tick            — nginx epoll loop iteration
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent

logger = logging.getLogger("stacktracer.probes.nginx")

_SYNTHETIC_TRACE_PREFIX = "nginx-"


# ====================================================================== #
# Mode A — eBPF uprobes on nginx worker symbols
# ====================================================================== #

_BPF_PROGRAM = r"""
#include <uapi/linux/ptrace.h>

// ---------------------------------------------------------------------------
// Shared timing infrastructure
// ---------------------------------------------------------------------------

BPF_HASH(ts_wait_request,   u64, u64);
BPF_HASH(ts_init_request,   u64, u64);
BPF_HASH(ts_handler,        u64, u64);
BPF_HASH(ts_recv,           u64, u64);
BPF_HASH(ts_epoll,          u64, u64);

BPF_PERF_OUTPUT(nginx_events);

#define EV_WAIT_REQUEST     1
#define EV_INIT_REQUEST     2
#define EV_HANDLER          3
#define EV_RECV             4
#define EV_ADD_EVENT        5
#define EV_EPOLL_TICK       6

struct nginx_event_t {
    u32  pid;
    u32  tid;
    u64  duration_ns;
    u8   ev_type;
    u32  extra;
};

// ---------------------------------------------------------------------------
// ngx_http_wait_request_handler
// Connection accepted. nginx is blocked in epoll_wait waiting for the client
// to send the first byte. Duration = client network RTT + connection setup.
// High duration here = slow clients or high-latency client networks.
// ---------------------------------------------------------------------------

int probe_wait_request_enter(struct pt_regs *ctx) {
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts  = bpf_ktime_get_ns();
    ts_wait_request.update(&tid, &ts);
    return 0;
}

int probe_wait_request_return(struct pt_regs *ctx) {
    u64 tid    = bpf_get_current_pid_tgid();
    u64 *start = ts_wait_request.lookup(&tid);
    if (!start) return 0;
    struct nginx_event_t evt = {};
    evt.pid         = tid >> 32;
    evt.tid         = (u32)tid;
    evt.duration_ns = bpf_ktime_get_ns() - *start;
    evt.ev_type     = EV_WAIT_REQUEST;
    ts_wait_request.delete(&tid);
    nginx_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}

// ---------------------------------------------------------------------------
// ngx_http_init_request
// First byte arrived. nginx allocates the ngx_http_request_t struct and
// begins parsing the HTTP request line and headers.
// Duration = HTTP parse time. High here = very large headers or slow parsers.
// ---------------------------------------------------------------------------

int probe_init_request_enter(struct pt_regs *ctx) {
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts  = bpf_ktime_get_ns();
    ts_init_request.update(&tid, &ts);
    return 0;
}

int probe_init_request_return(struct pt_regs *ctx) {
    u64 tid    = bpf_get_current_pid_tgid();
    u64 *start = ts_init_request.lookup(&tid);
    if (!start) return 0;
    struct nginx_event_t evt = {};
    evt.pid         = tid >> 32;
    evt.tid         = (u32)tid;
    evt.duration_ns = bpf_ktime_get_ns() - *start;
    evt.ev_type     = EV_INIT_REQUEST;
    ts_init_request.delete(&tid);
    nginx_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}

// ---------------------------------------------------------------------------
// ngx_http_handler
// Request parsed. nginx matches URI against location blocks and invokes
// the handler (proxy_pass, try_files, etc).
// Duration = location matching + handler setup, before upstream I/O.
// ---------------------------------------------------------------------------

int probe_handler_enter(struct pt_regs *ctx) {
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts  = bpf_ktime_get_ns();
    ts_handler.update(&tid, &ts);
    return 0;
}

int probe_handler_return(struct pt_regs *ctx) {
    u64 tid    = bpf_get_current_pid_tgid();
    u64 *start = ts_handler.lookup(&tid);
    if (!start) return 0;
    struct nginx_event_t evt = {};
    evt.pid         = tid >> 32;
    evt.tid         = (u32)tid;
    evt.duration_ns = bpf_ktime_get_ns() - *start;
    evt.ev_type     = EV_HANDLER;
    ts_handler.delete(&tid);
    nginx_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}

// ---------------------------------------------------------------------------
// ngx_recv
// nginx reading bytes from a client or upstream socket.
// Called multiple times per request for chunked or large bodies.
// extra = bytes returned by this read.
// High duration = network I/O wait, not nginx CPU time.
// ---------------------------------------------------------------------------

int probe_recv_enter(struct pt_regs *ctx) {
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts  = bpf_ktime_get_ns();
    ts_recv.update(&tid, &ts);
    return 0;
}

int probe_recv_return(struct pt_regs *ctx) {
    u64 tid    = bpf_get_current_pid_tgid();
    u64 *start = ts_recv.lookup(&tid);
    if (!start) return 0;
    struct nginx_event_t evt = {};
    evt.pid         = tid >> 32;
    evt.tid         = (u32)tid;
    evt.duration_ns = bpf_ktime_get_ns() - *start;
    evt.ev_type     = EV_RECV;
    evt.extra       = (u32)PT_REGS_RC(ctx);
    ts_recv.delete(&tid);
    nginx_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}

// ---------------------------------------------------------------------------
// ngx_add_event
// nginx registering a file descriptor with epoll — the exact moment nginx
// has dispatched to upstream and is yielding control back to the event loop.
// This is the upstream handoff point. No duration — point-in-time event.
// ---------------------------------------------------------------------------

int probe_add_event(struct pt_regs *ctx) {
    struct nginx_event_t evt = {};
    u64 tid  = bpf_get_current_pid_tgid();
    evt.pid  = tid >> 32;
    evt.tid  = (u32)tid;
    evt.ev_type = EV_ADD_EVENT;
    nginx_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}

// ---------------------------------------------------------------------------
// ngx_epoll_process_events
// One iteration of the nginx event loop — one epoll_wait() call.
// Duration = total time nginx spent processing events in this tick.
// extra = number of events returned by epoll_wait (event backlog).
// High duration + high extra = nginx event loop is saturated.
// ---------------------------------------------------------------------------

int probe_epoll_enter(struct pt_regs *ctx) {
    u64 tid = bpf_get_current_pid_tgid();
    u64 ts  = bpf_ktime_get_ns();
    ts_epoll.update(&tid, &ts);
    return 0;
}

int probe_epoll_return(struct pt_regs *ctx) {
    u64 tid    = bpf_get_current_pid_tgid();
    u64 *start = ts_epoll.lookup(&tid);
    if (!start) return 0;
    struct nginx_event_t evt = {};
    evt.pid         = tid >> 32;
    evt.tid         = (u32)tid;
    evt.duration_ns = bpf_ktime_get_ns() - *start;
    evt.ev_type     = EV_EPOLL_TICK;
    ts_epoll.delete(&tid);
    nginx_events.perf_submit(ctx, &evt, sizeof(evt));
    return 0;
}
"""

# Maps BPF event type integer → (ProbeType string, name label)
_EV_MAP = {
    1: ("nginx.connection.accept",  "wait_request"),
    2: ("nginx.request.parse",      "init_request"),
    3: ("nginx.request.route",      "handler"),
    4: ("nginx.recv",               "recv"),
    5: ("nginx.upstream.dispatch",  "add_event"),
    6: ("nginx.epoll.tick",         "epoll"),
}


def _find_nginx_binary() -> Optional[str]:
    """Locate the nginx worker binary for uprobe attachment."""
    candidates = [
        "/usr/sbin/nginx",
        "/usr/local/sbin/nginx",
        "/opt/nginx/sbin/nginx",
        "/usr/local/nginx/sbin/nginx",
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    try:
        import subprocess
        result = subprocess.run(
            ["which", "nginx"], capture_output=True, text=True, timeout=2
        )
        path = result.stdout.strip()
        if path and os.path.isfile(path):
            return path
    except Exception:
        pass
    return None


class NginxEBPFProbe(BaseProbe):
    """
    Mode A: eBPF uprobes on nginx worker process symbols.

    What each attached symbol reveals:
        ngx_http_wait_request_handler  client RTT + connection overhead
        ngx_http_init_request          HTTP parse time
        ngx_http_handler               location routing time
        ngx_recv                       socket read time + bytes per call
        ngx_add_event                  exact moment of upstream handoff
        ngx_epoll_process_events       event loop utilisation per tick

    Together these reconstruct the nginx critical path:
        [accept] → [parse] → [route] → [upstream dispatch]
                                              ↓
                                   [upstream response time]  ← your Django probe covers this
    """
    name = "nginx_ebpf"

    def __init__(self, nginx_binary: Optional[str] = None) -> None:
        self._nginx_binary = nginx_binary or _find_nginx_binary()
        self._bpf: Optional[Any] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        if not self._nginx_binary:
            logger.warning(
                "nginx binary not found — nginx eBPF probe inactive. "
                "Pass nginx_binary='/path/to/nginx' if non-standard location."
            )
            return

        try:
            from bcc import BPF
        except ImportError:
            logger.warning("bcc not installed — nginx eBPF probe inactive.")
            return

        if os.geteuid() != 0:
            logger.warning("nginx eBPF probe requires root or CAP_BPF.")
            return

        try:
            self._bpf = BPF(text=_BPF_PROGRAM)

            # Each symbol gets an entry uprobe and a return uretprobe
            # so we can measure duration between them
            symbol_pairs = [
                ("ngx_http_wait_request_handler", "probe_wait_request_enter", "probe_wait_request_return"),
                ("ngx_http_init_request",         "probe_init_request_enter", "probe_init_request_return"),
                ("ngx_http_handler",              "probe_handler_enter",      "probe_handler_return"),
                ("ngx_recv",                      "probe_recv_enter",         "probe_recv_return"),
                ("ngx_epoll_process_events",      "probe_epoll_enter",        "probe_epoll_return"),
            ]

            for symbol, entry_fn, return_fn in symbol_pairs:
                try:
                    self._bpf.attach_uprobe(
                        name=self._nginx_binary, sym=symbol, fn_name=entry_fn
                    )
                    self._bpf.attach_uretprobe(
                        name=self._nginx_binary, sym=symbol, fn_name=return_fn
                    )
                    logger.debug("attached uprobe: %s", symbol)
                except Exception as exc:
                    logger.warning("uprobe attach failed for %s: %s", symbol, exc)

            # ngx_add_event is a point-in-time event — entry only, no duration
            try:
                self._bpf.attach_uprobe(
                    name=self._nginx_binary, sym="ngx_add_event", fn_name="probe_add_event"
                )
                logger.debug("attached uprobe: ngx_add_event")
            except Exception as exc:
                logger.warning("uprobe attach failed for ngx_add_event: %s", exc)

            self._bpf["nginx_events"].open_perf_buffer(self._handle_event)

            self._running = True
            self._thread = threading.Thread(
                target=self._poll_loop, daemon=True, name="stacktracer-nginx-ebpf"
            )
            self._thread.start()
            logger.info("nginx eBPF probe attached to %s", self._nginx_binary)

        except Exception as exc:
            logger.error("nginx eBPF probe failed to start: %s", exc)

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
                logger.debug("nginx eBPF poll error: %s", exc)

    def _handle_event(self, cpu: int, data: Any, size: int) -> None:
        if self._bpf is None:
            return
        try:
            event  = self._bpf["nginx_events"].event(data)
            ev_type = event.ev_type
            probe_str, name = _EV_MAP.get(ev_type, ("custom", "unknown"))

            # nginx does not propagate trace IDs natively.
            # Synthetic ID per connection tid — correlate with Django via
            # X-Request-ID header if you add it in nginx and forward it.
            trace_id = f"{_SYNTHETIC_TRACE_PREFIX}{event.tid}"

            kwargs: dict = dict(
                probe=probe_str,
                trace_id=trace_id,
                service="nginx",
                name=name,
                pid=event.pid,
                tid=event.tid,
            )
            if event.duration_ns:
                kwargs["duration_ns"] = event.duration_ns
            if ev_type == 4:    # recv — attach byte count
                kwargs["bytes_read"] = event.extra
            if ev_type == 6:    # epoll tick — attach event backlog count
                kwargs["events_processed"] = event.extra

            emit(NormalizedEvent.now(**kwargs))

        except Exception as exc:
            logger.debug("nginx eBPF event handling error: %s", exc)


# ====================================================================== #
# Mode B — JSON access log tail
# ====================================================================== #

class NginxLogProbe(BaseProbe):
    """
    Mode B: tail the nginx JSON access log.

    Per-request visibility only — one request.entry + nginx.upstream.dispatch
    + request.exit per request. No intra-request lifecycle stages.

    Use when:
      - No root access for eBPF
      - macOS / non-Linux development
      - Production with restricted permissions
    """
    name = "nginx_log"

    def __init__(self, log_path: str = "/var/log/nginx/access.log") -> None:
        self._log_path = log_path
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        if not os.path.exists(self._log_path):
            logger.warning(
                "nginx log not found at %s — nginx log probe inactive.",
                self._log_path,
            )
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._tail_loop, daemon=True, name="stacktracer-nginx-log"
        )
        self._thread.start()
        logger.info("nginx log probe tailing %s", self._log_path)

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _tail_loop(self) -> None:
        try:
            with open(self._log_path, "r") as f:
                f.seek(0, 2)
                while self._running:
                    line = f.readline()
                    if line:
                        self._handle_line(line.strip())
                    else:
                        time.sleep(0.05)
        except Exception as exc:
            logger.error("nginx log probe tail error: %s", exc)

    def _handle_line(self, line: str) -> None:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            record = self._parse_common_log(line)
            if not record:
                return

        uri            = record.get("uri", record.get("request", "unknown"))
        method         = record.get("method", "")
        status         = int(record.get("status", 0))
        request_time_s = float(record.get("request_time", 0))
        upstream_raw   = record.get("upstream_response_time", "")
        trace_id       = f"{_SYNTHETIC_TRACE_PREFIX}{time.time_ns()}"

        emit(NormalizedEvent.now(
            probe="request.entry",
            trace_id=trace_id,
            service="nginx",
            name=uri,
            method=method,
            status_code=status,
            duration_ns=int(request_time_s * 1e9),
        ))

        if upstream_raw and upstream_raw not in ("-", ""):
            try:
                upstream_s = float(upstream_raw)
                emit(NormalizedEvent.now(
                    probe="nginx.upstream.dispatch",
                    trace_id=trace_id,
                    service="nginx",
                    name=uri,
                    upstream_duration_ns=int(upstream_s * 1e9),
                    # nginx overhead = total - upstream = time nginx spent on its own
                    nginx_overhead_ns=int((request_time_s - upstream_s) * 1e9),
                ))
            except ValueError:
                pass

        emit(NormalizedEvent.now(
            probe="request.exit",
            trace_id=trace_id,
            service="nginx",
            name=uri,
            status_code=status,
        ))

    @staticmethod
    def _parse_common_log(line: str) -> Optional[dict]:
        try:
            parts = line.split('"')
            if len(parts) < 5:
                return None
            request    = parts[1]
            status_raw = parts[2].strip().split()
            method, uri = request.split()[:2] if " " in request else ("", request)
            return {"uri": uri, "method": method, "status": status_raw[0] if status_raw else "0"}
        except Exception:
            return None


# ====================================================================== #
# NginxProbe — unified entry point, auto-selects mode
# ====================================================================== #

class NginxProbe(BaseProbe):
    """
    Unified nginx probe — selects the best available mode automatically.

    auto (default):
        Tries eBPF first. Falls back to log tail if bcc unavailable or not root.

    force mode:
        NginxProbe(mode="ebpf")   — eBPF only, fail if unavailable
        NginxProbe(mode="log")    — log tail only
    """
    name = "nginx"

    def __init__(
        self,
        log_path: str = "/var/log/nginx/access.log",
        nginx_binary: Optional[str] = None,
        mode: str = "auto",
    ) -> None:
        self._log_path     = log_path
        self._nginx_binary = nginx_binary
        self._mode         = mode
        self._active: Optional[BaseProbe] = None

    def start(self) -> None:
        if self._mode == "ebpf":
            self._active = NginxEBPFProbe(self._nginx_binary)
        elif self._mode == "log":
            self._active = NginxLogProbe(self._log_path)
        else:
            # auto: try eBPF, degrade gracefully
            try:
                import bcc  # noqa: F401
                if os.geteuid() == 0 and _find_nginx_binary():
                    self._active = NginxEBPFProbe(self._nginx_binary)
                    logger.info("nginx probe: eBPF mode selected")
                else:
                    raise RuntimeError("not root or nginx not found")
            except Exception:
                self._active = NginxLogProbe(self._log_path)
                logger.info("nginx probe: log mode selected (eBPF unavailable)")

        self._active.start()

    def stop(self) -> None:
        if self._active:
            self._active.stop()
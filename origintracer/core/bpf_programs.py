from dataclasses import dataclass, field
from typing import List


@dataclass
class BPFProgramPart:
    """
    A structured BPF fragment contributed by one probe package.

    Each field maps to a section in the final compiled program.
    Keeping sections separate lets the builder deduplicate #include headers
    and struct definitions safely, while preserving probe function order.

    Map naming convention (enforced by code review, not runtime):
        <probe_name>_<map_name>
        e.g.  nginx_accept_ts,  nginx_epoll_ts
    This prevents symbol collisions when multiple probe fragments are merged.

    Fields:
        headers: #include directives the probe needs beyond the bridge header.
                 e.g. ["#include <linux/in.h>", "#include <linux/socket.h>"]

        structs: Any extra C struct definitions the probe needs.
                 The shared kernel_event_t and trace_entry_t are already
                 defined in BRIDGE_BPF_HEADER — do not redefine them here.

        maps : BPF map declarations (BPF_HASH, BPF_ARRAY, etc.).
               kernel_events perf output is already declared in
               BRIDGE_BPF_HEADER — do not redeclare it here.

        probes: TRACEPOINT_PROBE and kprobe function bodies.
                Order within a probe's list is preserved.
                Order across probes follows registration order.
    """

    headers: List[str] = field(default_factory=list)
    structs: List[str] = field(default_factory=list)
    maps: List[str] = field(default_factory=list)
    probes: List[str] = field(default_factory=list)


# ── Registry ──────────────────────────────────────────────────────────────────

_registry: List[tuple] = (
    []
)  # [(probe_name, BPFProgramPart), ...]


def register_bpf(name: str, part: BPFProgramPart) -> None:
    """
    Register a BPF program fragment.

    Called by each probe module at import time, before KprobeBridge.start().
    The name is used for logging and deduplication diagnostics only.

    Example (inside nginx_probe.py at module level):
        from stacktracer.core.bpf_programs import BPFProgramPart, register_bpf
        register_bpf("nginx", BPFProgramPart(
            headers=[...],
            maps=[...],
            probes=[_NGINX_BPF],   # _NGINX_BPF defined privately in nginx_probe.py
        ))
    """
    _registry.append((name, part))


def get_registered_names() -> List[str]:
    """Return names of all registered probe fragments — useful for diagnostics."""
    return [name for name, _ in _registry]


def clear_bpf_registry() -> None:
    """Clear the registry. Used in tests only."""
    _registry.clear()


# --------------- Shared bridge header ---------------------------------
# Defines structs and maps that ALL probes depend on.
# This is the ONLY BPF C string that lives in this file.
#
# Design rules enforced here:
#   1. net/sock.h is NOT included — it causes bpf_wq incomplete-type errors
#      on kernel 6.x. Probes that need socket structs include it themselves.
#   2. kernel_events perf buffer is declared ONCE here. No probe may redeclare it.
#   3. kernel_event_t is the single shared event struct. No probe may define
#      its own event struct — doing so breaks the Python-side dispatcher.
#   4. trace_context map key is u32 tid (lower 32 bits of pid_tgid).
#      All probes must cast to u32 before calling trace_context.lookup().

BRIDGE_BPF_HEADER = r"""
#include <uapi/linux/ptrace.h>
#include <linux/sched.h>

// **** Trace context *******
// Written by Python (KprobeBridge.register_trace / unregister_trace).
// Read by probe functions to attribute kernel events to application traces.
//
// Key: u32 tid — always cast before lookup:
//   u64 pid_tid = bpf_get_current_pid_tgid();
//   u32 tid = (u32)pid_tid;
//   struct trace_entry_t *ctx = trace_context.lookup(&tid);

struct trace_entry_t {
    char trace_id[36];   // UUID: "550e8400-e29b-41d4-a716-446655440000"
    u64  start_ns;       // bpf_ktime_get_ns() at trace start
    char service[32];    // "django", "celery", etc.
    u32  pid;
    u32  tid;
};

BPF_HASH(trace_context, u32, struct trace_entry_t, 65536);

// ********** Shared event struct *********
// ALL probes emit this struct into kernel_events.
// The Python dispatcher demultiplexes by event_type string.
//
// event_type naming convention:  "<probe>.<event>"
//   nginx.accept    nginx.epoll    nginx.data_out    nginx.data_in
//   epoll.wait      tcp.connect    tcp.send          tcp.recv
//
// value1 / value2 interpretation depends on event_type — document in probe.

struct kernel_event_t {
    u64  timestamp_ns;
    u32  pid;
    u32  tid;
    char trace_id[36];
    char service[32];
    char event_type[32];
    s64  value1;          // primary numeric payload (fd, n_events, bytes, ...)
    s64  value2;          // secondary payload (e.g. fd for sendmsg)
    u64  duration_ns;
    u32  saddr;           // source IP (network byte order) — 0 if unused
    u32  daddr;           // dest IP   (network byte order) — 0 if unused
    u16  sport;
    u16  dport;
    u16  _pad;
    u32  client_ip;       // populated by accept probes — 0 otherwise
    u16  client_port;
};

// Declared ONCE here. Probes call kernel_events.perf_submit(...).
// Never declare BPF_PERF_OUTPUT in a probe fragment.
BPF_PERF_OUTPUT(kernel_events);
"""

# ---------- Bridge noop --------------------
# BCC requires at least one callable probe function to compile and load a
# program. This sentinel satisfies that requirement even when no probes are
# registered (e.g. in tests or stripped-down deployments).

_BRIDGE_NOOP = r"""
int _bridge_noop(struct pt_regs *ctx) { return 0; }
"""


def build_bpf_program() -> str:
    """
    Assemble all registered BPFProgramPart fragments into one BPF C string.
    Called by KprobeBridge.start() immediately before BPF(text=...).

    Sections in the output:
      1. BRIDGE_BPF_HEADER - shared structs + maps
      2. #include headers        — deduplicated union of all probe headers
      3. struct definitions      — deduplicated union of all probe structs
      4. map declarations        — deduplicated union of all probe maps
      5. probe functions         — in registration order, NOT deduplicated
      6. _BRIDGE_NOOP            — sentinel (last)

    Deduplication: exact string match after stripping whitespace.
    Map naming convention (probe_mapname) prevents false deduplication of
    distinct maps that happen to have similar declarations.
    """
    import logging

    logger = logging.getLogger(__name__)

    parts = [part for _, part in _registry]

    if not parts:
        logger.warning(
            "bpf_programs: build_bpf_program() called with no registered parts — "
            "only the bridge header and noop will be compiled"
        )

    logger.debug(
        "bpf_programs: building program from probes: %s",
        get_registered_names(),
    )

    def dedup(items: List[str]) -> List[str]:
        seen: set = set()
        out: List[str] = []
        for item in items:
            key = item.strip()
            if key and key not in seen:
                seen.add(key)
                out.append(item)
        return out

    all_headers: List[str] = []
    all_structs: List[str] = []
    all_maps: List[str] = []
    all_probes: List[str] = []

    for part in parts:
        all_headers.extend(part.headers)
        all_structs.extend(part.structs)
        all_maps.extend(part.maps)
        all_probes.extend(part.probes)

    sections = [
        BRIDGE_BPF_HEADER,
        "\n".join(dedup(all_headers)),
        "\n".join(dedup(all_structs)),
        "\n".join(dedup(all_maps)),
        "\n\n".join(all_probes),
        _BRIDGE_NOOP,
    ]

    return "\n\n".join(s for s in sections if s.strip())

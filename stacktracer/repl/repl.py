#!/usr/bin/env python3
"""
scripts/repl.py

StackTracer REPL — connects to a running agent via Unix socket.

Run from any terminal while gunicorn is running:
    python scripts/repl.py

The REPL auto-discovers /tmp/stacktracer-{pid}.sock and connects.
All queries run against the live engine in the worker process.
No engine is bootstrapped here — this file has zero stacktracer imports.

Protocol: newline-delimited JSON over Unix domain socket.
    Send:    {"id": "1", "query": "SHOW NODES"}\\n
    Receive: {"id": "1", "ok": true, "data": {...}}\\n

DSL queries (forwarded verbatim to engine.query()):
    SHOW latency WHERE service = "django"
    SHOW latency WHERE system = "export"
    SHOW graph
    SHOW graph WHERE system = "worker"
    SHOW events WHERE probe = "db.query.start" LIMIT 20
    HOTSPOT TOP 10
    CAUSAL
    CAUSAL WHERE tags = "blocking,celery"
    BLAME WHERE system = "export"
    DIFF SINCE deployment
    TRACE <trace_id>

REPL meta-commands (prefixed with \\):
    \\status                    engine stats (graph size, uptime, etc.)
    \\probes                    registered probe adapters
    \\rules                     registered causal rules
    \\semantic                  semantic aliases
    \\emit <probe> <svc> <n>    inject a synthetic event
    \\snapshot [label]          capture a temporal diff snapshot
    \\active                    in-flight requests
    \\reconnect                 pick a different worker socket
    \\help                      this message
    \\quit  or  Ctrl+C          exit
"""

from __future__ import annotations

import glob
import json
import os
import socket
import sys
import textwrap
import time
import uuid

# Make sure the project root is on the path (harmless if already there)
sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
)


# ------------------------------------------------------------------ #
# Colour helpers
# ------------------------------------------------------------------ #

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
WHITE = "\033[37m"


def c(text, *codes):
    return "".join(codes) + str(text) + RESET


def header(text):
    print(c(f"\n  {text}", BOLD, CYAN))


def ok(text):
    print(c(f"  ✓ {text}", GREEN))


def warn(text):
    print(c(f"  ⚠ {text}", YELLOW))


def err(text):
    print(c(f"  ✗ {text}", RED))


def dim(text):
    print(c(f"  {text}", DIM))


# ------------------------------------------------------------------ #
# Unix socket client
# ------------------------------------------------------------------ #

_SOCKET_PREFIX = "/tmp/stacktracer-"
_SOCKET_SUFFIX = ".sock"


def discover_sockets() -> list[str]:
    live = []
    for path in sorted(
        glob.glob(f"{_SOCKET_PREFIX}*{_SOCKET_SUFFIX}")
    ):
        pid = path.replace(_SOCKET_PREFIX, "").replace(
            _SOCKET_SUFFIX, ""
        )
        try:
            # Check if the process is actually alive
            os.kill(int(pid), 0)
            live.append(path)
        except (ProcessLookupError, ValueError):
            # Process is dead — remove the stale socket
            try:
                os.unlink(path)
            except OSError:
                pass
    return live


def pick_socket() -> str:
    """
    Auto-discover sockets. If exactly one exists, use it silently.
    If multiple exist, prompt the user to pick one.
    If none exist, print instructions and exit.
    """
    sockets = discover_sockets()

    if not sockets:
        err("No StackTracer agent found.")
        dim("Start your Django app with gunicorn first:")
        dim(
            "  gunicorn -c gunicorn.conf.py config.asgi:application \\"
        )
        dim(
            "           --worker-class uvicorn.workers.UvicornWorker"
        )
        dim("Then run this REPL in a separate terminal.")
        sys.exit(1)

    if len(sockets) == 1:
        pid = (
            sockets[0]
            .replace(_SOCKET_PREFIX, "")
            .replace(_SOCKET_SUFFIX, "")
        )
        ok(f"Connected to worker pid={pid}")
        return sockets[0]

    print(c("\n  Multiple workers found:", BOLD))
    for i, s in enumerate(sockets):
        pid = s.replace(_SOCKET_PREFIX, "").replace(
            _SOCKET_SUFFIX, ""
        )
        print(f"  [{i}] pid={pid}  ({s})")
    print()
    while True:
        try:
            choice = (
                input(
                    c("  Pick worker [0]: ", BOLD, BLUE)
                ).strip()
                or "0"
            )
            return sockets[int(choice)]
        except (ValueError, IndexError):
            err(f"Enter a number 0–{len(sockets) - 1}")


def query(sock_path: str, query_str: str) -> dict:
    """
    Send one query to the agent. Opens and closes a fresh connection
    per call — matches the server's one-query-per-connection protocol.
    Returns the full parsed response dict (always has 'ok' key).
    """
    msg = (
        json.dumps(
            {"id": str(uuid.uuid4())[:8], "query": query_str}
        ).encode()
        + b"\n"
    )
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(10.0)
        s.connect(sock_path)
        s.sendall(msg)
        raw = b""
        while b"\n" not in raw:
            chunk = s.recv(65536)
            if not chunk:
                break
            raw += chunk
        s.close()
        return json.loads(raw.split(b"\n")[0].decode())
    except FileNotFoundError:
        return {
            "ok": False,
            "error": f"Socket gone: {sock_path} — did the worker restart?",
        }
    except socket.timeout:
        return {"ok": False, "error": "Query timed out (>10 s)"}
    except ConnectionRefusedError:
        return {
            "ok": False,
            "error": "Connection refused — worker may have restarted",
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


# ------------------------------------------------------------------ #
# Result rendering  (all original logic preserved)
# ------------------------------------------------------------------ #


def render(result: dict) -> None:
    """Pretty-print a query result returned by the agent."""

    # Socket-level failure
    if not result.get("ok"):
        err(result.get("error", "Unknown error"))
        return

    data = result.get("data")

    # Unwrap executor envelope — executor returns {"metric": "...", "data": <payload>}
    # local_server wraps that in {"ok": True, "data": <executor_result>}
    # So result["data"] is the executor result, and the real payload is inside that.
    if isinstance(data, dict) and "data" in data:
        verb = data.get("verb", "")
        metric = data.get("metric", "")
        data = data["data"]  # ← unwrap to actual payload
    else:
        verb = result.get("verb", "")
        metric = result.get("metric", "")

    # error inside executor result
    if (
        isinstance(data, dict)
        and "error" in data
        and len(data) == 1
    ):
        err(data["error"])
        return

    # ── CAUSAL ────────────────────────────────────────────────────
    if verb == "CAUSAL":
        matches = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        if not matches:
            dim("No causal patterns matched.")
            return
        print()
        for m in matches:
            pct = int(m["confidence"] * 100)
            bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
            colour = RED if pct >= 80 else YELLOW
            print(
                c(f"  [{bar}] {pct}%  {m['rule']}", colour, BOLD)
            )
            wrapped = textwrap.fill(
                m["explanation"],
                width=72,
                initial_indent="        ",
                subsequent_indent="        ",
            )
            print(c(wrapped, DIM))
            evidence = m.get("evidence", {})
            if evidence:
                print(
                    c(
                        f"        Evidence: {json.dumps(evidence)}",
                        DIM,
                    )
                )
            print()
        return

    # ── DIFF ──────────────────────────────────────────────────────
    if verb == "DIFF":
        d = data if isinstance(data, dict) else {}
        new = d.get("new_edges", [])
        gone = d.get("removed_edges", [])
        if not new and not gone:
            dim("No graph changes detected.")
            return
        if new:
            print(
                c(f"\n  + {len(new)} new edge(s):", GREEN, BOLD)
            )
            for e in new:
                print(c(f"      {e}", GREEN))
        if gone:
            print(
                c(
                    f"\n  - {len(gone)} removed edge(s):",
                    RED,
                    BOLD,
                )
            )
            for e in gone:
                print(c(f"      {e}", RED))
        print()
        return

    # ── HOTSPOT / generic tabular list ────────────────────────────
    if verb == "HOTSPOT" or (
        isinstance(data, list)
        and data
        and isinstance(data[0], dict)
    ):
        rows = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        if not rows:
            dim("No results.")
            return
        _render_table(rows)
        return

    # ── TRACE / critical path ─────────────────────────────────────
    if verb == "TRACE":
        path = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        if not path:
            dim("Trace not found in event log.")
            return
        print()
        total = 0
        for stage in path:
            dur = stage.get("duration_ms")
            dur_str = (
                f"{dur:8.2f}ms"
                if dur is not None
                else "        —"
            )
            bar_len = int(min((dur or 0) / 5, 40))
            bar = "▓" * bar_len
            colour = (
                RED
                if (dur or 0) > 100
                else YELLOW if (dur or 0) > 20 else GREEN
            )
            print(
                c(f"  {dur_str}  ", WHITE)
                + c(f"{bar:<40}", colour)
                + c(f"  {stage['probe']}", BOLD)
                + c(
                    f"  {stage['service']}::{stage['name']}", DIM
                )
            )
            total += dur or 0
        print(
            c(
                f"\n  Total: {total:.2f}ms  ·  {len(path)} stages",
                BOLD,
                CYAN,
            )
        )
        print()
        return

    # ── BLAME ─────────────────────────────────────────────────────
    if verb == "BLAME":
        d = data if isinstance(data, dict) else {}
        rows = (
            d.get("data", data)
            if isinstance(data, list)
            else d.get("data", [])
        )
        resolved = d.get("resolved_nodes", [])
        if resolved:
            print(
                c(
                    f"\n  System nodes: {', '.join(resolved)}",
                    DIM,
                )
            )
        if not rows:
            dim("No upstream callers found.")
            return
        print()
        for row in rows:
            print(
                c(f"  {row['call_count']:6}x  ", YELLOW, BOLD)
                + c(row["caller"], WHITE)
            )
        print()
        return

    # ── SHOW STATUS ───────────────────────────────────────────────
    if verb == "STATUS" or (
        isinstance(data, dict) and "graph_nodes" in data
    ):
        header("Engine Status")
        for k, v in data.items():
            print(f"  {c(k, DIM):<30} {c(v, WHITE)}")
        print()
        return

    # ── SHOW GRAPH ────────────────────────────────────────────────
    if metric == "graph" or (
        isinstance(data, dict)
        and "nodes" in data
        and "edges" in data
    ):
        g = data if isinstance(data, dict) else {}
        nodes = g.get("nodes", [])
        edges = g.get("edges", [])
        print(
            c(
                f"\n  {len(nodes)} nodes  ·  {len(edges)} edges\n",
                BOLD,
            )
        )
        for n in nodes:
            ntype = n.get(
                "type", n.get("node_type", n.get("service", ""))
            )
            print(
                c(f"  [{ntype:12}]  ", DIM)
                + c(n["id"], WHITE)
                + c(f"  ×{n['call_count']}", CYAN)
            )
        if edges:
            print()
            for e in edges:
                src = e.get("source", e.get("from", "?"))
                tgt = e.get("target", e.get("to", "?"))
                etype = e.get("type", "")
                cnt = e.get("call_count", e.get("weight", ""))
                suffix = (
                    f" [{etype}"
                    + (f" ×{cnt}" if cnt else "")
                    + "]"
                    if etype or cnt
                    else ""
                )
                print(
                    c(f"  {src}", YELLOW)
                    + c(f"  →  ", DIM)
                    + c(tgt, YELLOW)
                    + c(suffix, DIM)
                )
        print()
        return

    # ── SNAPSHOT result ───────────────────────────────────────────
    if isinstance(data, dict) and "added_nodes" in data:
        new_n = len(data.get("added_nodes", []))
        new_e = len(data.get("added_edges", []))
        label = data.get("label", "")
        header(f"Snapshot {'(' + label + ')' if label else ''}")
        print(
            f"  New nodes: {c(new_n, CYAN)}  New edges: {c(new_e, CYAN)}"
        )
        print()
        return

    # ── Plain list of strings (e.g. SHOW PROBES, SHOW RULES) ──────
    if (
        isinstance(data, list)
        and data
        and isinstance(data[0], str)
    ):
        print()
        for item in data:
            print(f"  {c(item, WHITE)}")
        print()
        return

    # ── Empty list ────────────────────────────────────────────────
    if isinstance(data, list) and not data:
        dim("No results.")
        return

    # ── Fallback: raw JSON ────────────────────────────────────────
    print(
        json.dumps(
            data if data is not None else result,
            indent=2,
            default=str,
        )
    )


def _render_table(rows: list) -> None:
    if not rows:
        return
    keys = list(rows[0].keys())
    widths = {
        k: max(len(k), max(len(str(r.get(k, ""))) for r in rows))
        for k in keys
    }

    print()
    header_line = "  " + "  ".join(
        c(k.upper().replace("_", " "), BOLD, DIM).ljust(
            widths[k] + 11
        )
        for k in keys
    )
    print(header_line)
    print(
        c(
            "  "
            + "─" * (sum(widths.values()) + len(keys) * 2 + 2),
            DIM,
        )
    )

    for row in rows:
        parts = []
        for k in keys:
            val = str(row.get(k, "—"))
            try:
                f = float(val)
                colour = (
                    RED
                    if f > 100
                    else YELLOW if f > 20 else GREEN
                )
                parts.append(c(val.ljust(widths[k]), colour))
            except ValueError:
                parts.append(val.ljust(widths[k]))
        print("  " + "  ".join(parts))
    print()


# ------------------------------------------------------------------ #
# Meta-command handlers  (all go through the socket now)
# ------------------------------------------------------------------ #


def cmd_status(sock_path: str) -> None:
    render(query(sock_path, "SHOW STATUS"))


def cmd_probes(sock_path: str) -> None:
    result = query(sock_path, "SHOW PROBES")
    if not result.get("ok"):
        warn(
            "SHOW PROBES not yet supported by this server version."
        )
        dim("Add it to local_server.py _evaluate() when ready.")
        return
    header("Registered Probes")
    data = result.get("data") or []
    if not data:
        dim("None registered")
        return
    for name in (data if isinstance(data, list) else [data]):
        ok(str(name))
    print()


def cmd_rules(sock_path: str) -> None:
    result = query(sock_path, "SHOW RULES")
    if not result.get("ok"):
        warn(
            "SHOW RULES not yet supported by this server version."
        )
        dim("Add it to local_server.py _evaluate() when ready.")
        return
    header("Causal Rules")
    data = result.get("data") or []
    if not data:
        dim("None registered")
        return
    for name in (data if isinstance(data, list) else [data]):
        ok(str(name))
    print()


def cmd_semantic(sock_path: str) -> None:
    result = query(sock_path, "SHOW SEMANTIC")
    if not result.get("ok"):
        warn(
            "SHOW SEMANTIC not yet supported by this server version."
        )
        dim("Add it to local_server.py _evaluate() when ready.")
        return
    header("Semantic Aliases")
    data = result.get("data") or []
    if not data:
        dim("None registered")
        return
    for entry in (data if isinstance(data, list) else [data]):
        if isinstance(entry, dict):
            label = entry.get("label", "")
            desc = entry.get("description", "")
            print(f"  {c(label, BOLD, CYAN):<20} {c(desc, DIM)}")
        else:
            print(f"  {c(entry, WHITE)}")
    print()


def cmd_emit(sock_path: str, args: str) -> None:
    """
    Inject a synthetic event into the live engine.
    Usage: \\emit <probe> <service> <name>
    Mirrors the original cmd_emit signature exactly — same three-part syntax.
    """
    parts = args.split()
    if len(parts) < 3:
        err("Usage: \\emit <probe> <service> <name>")
        return
    probe, service, name = parts[0], parts[1], parts[2]
    trace_id = str(uuid.uuid4())
    payload = json.dumps(
        {
            "probe": probe,
            "service": service,
            "name": name,
            "trace_id": trace_id,
        }
    )
    result = query(sock_path, f"INJECT {payload}")
    if result.get("ok"):
        ok(
            f"Emitted  {probe}  {service}::{name}  trace={trace_id[:8]}"
        )
    else:
        warn(
            "INJECT not yet supported by this server version. Event NOT processed."
        )
        dim(
            f"  probe={probe}  service={service}  name={name}  trace={trace_id[:8]}"
        )
        dim(
            "Add INJECT to local_server.py _evaluate() when ready."
        )


def cmd_snapshot(sock_path: str, args: str) -> None:
    """Ask the live engine to capture a temporal diff snapshot."""
    label = args.strip()
    result = query(
        sock_path, f"SNAPSHOT {label}" if label else "SNAPSHOT"
    )
    if not result.get("ok"):
        warn(
            "SNAPSHOT not yet supported by this server version."
        )
        dim("Add it to local_server.py _evaluate() when ready.")
        return
    render(result)


def cmd_help() -> None:
    header("DSL Commands")
    queries = [
        (
            'SHOW latency WHERE service = "django"',
            "Latency by node, filtered",
        ),
        (
            'SHOW latency WHERE system = "export"',
            "Latency scoped to semantic alias",
        ),
        ("SHOW graph", "Full runtime graph"),
        (
            'SHOW graph WHERE system = "worker"',
            "Subgraph for a system",
        ),
        (
            'SHOW events WHERE probe = "db.query.start" LIMIT 20',
            "Raw event log",
        ),
        ("HOTSPOT TOP 10", "Busiest nodes by call count"),
        ("CAUSAL", "Run all causal rules"),
        (
            'CAUSAL WHERE tags = "blocking,celery"',
            "Filtered causal rules",
        ),
        (
            'BLAME WHERE system = "export"',
            "Upstream callers of a system",
        ),
        (
            "DIFF SINCE deployment",
            "Graph changes after named event",
        ),
        ("TRACE <trace_id>", "Reconstruct critical path"),
        (
            "\\stitch <trace_id>",
            "Stitch trace across ALL worker processes",
        ),
    ]
    for q, desc in queries:
        print(f"  {c(q, CYAN)}")
        print(c(f"    {desc}", DIM))
    print()
    header("REPL Commands")
    cmds = [
        ("\\status", "Engine stats (graph size, uptime, etc.)"),
        ("\\probes", "List registered probe adapters"),
        ("\\rules", "List registered causal rules"),
        ("\\semantic", "List semantic aliases"),
        (
            "\\emit <probe> <svc> <name>",
            "Inject a synthetic event into the live engine",
        ),
        (
            "\\snapshot [label]",
            "Capture a temporal diff snapshot",
        ),
        (
            "\\active",
            "In-flight requests currently being traced",
        ),
        ("\\reconnect", "Pick a different worker socket"),
        ("\\help", "This message"),
        ("\\quit  or  Ctrl+C", "Exit"),
    ]
    for cmd, desc in cmds:
        print(f"  {c(cmd, YELLOW):<45} {c(desc, DIM)}")
    print()


# ------------------------------------------------------------------ #
# Main loop
# ------------------------------------------------------------------ #


def cmd_stitch(trace_id: str) -> None:
    """
    Query every live worker socket for the same trace_id and print
    a unified timeline across all processes, ordered by timestamp.

    This is the cross-process story: one HTTP request, one Celery task,
    two processes, one trace. The trace_id is the thread connecting them.

    Usage:
        \\stitch abc123-...
    """
    if not trace_id:
        err("Usage: \\stitch <trace_id>")
        return

    sockets = discover_sockets()
    if not sockets:
        err("No workers found.")
        return

    # Collect stages from every socket that knows this trace_id
    all_stages: list[dict] = []
    found_in: list[str] = []

    for sock in sockets:
        pid = sock.replace(_SOCKET_PREFIX, "").replace(
            _SOCKET_SUFFIX, ""
        )
        try:
            result = query(sock, f"TRACE {trace_id}")
            payload = result.get(
                "data", {}
            )  # the inner dict with verb/trace_id/stages/data
            stages = payload.get(
                "data", []
            )  # the actual list of stage dicts
            if isinstance(stages, list) and stages:
                for s in stages:
                    s["_pid"] = pid
                    s["_socket"] = sock
                all_stages.extend(stages)
                found_in.append(pid)
        except Exception:
            pass  # worker may have restarted — skip silently

    if not all_stages:
        dim(f"  Trace {trace_id[:16]}… not found in any worker.")
        dim(f"  Searched {len(sockets)} socket(s).")
        return

    # Sort by timestamp if present, otherwise keep original order
    all_stages.sort(key=lambda s: s.get("timestamp", 0))

    # Print unified timeline
    print()
    print(
        c(f"  Trace  ", BOLD)
        + c(trace_id[:24] + "…", CYAN)
        + c(
            f"  across {len(found_in)} process(es): pid {', '.join(found_in)}",
            DIM,
        )
    )
    print()

    # Group label per pid so the reader sees process boundaries clearly
    prev_pid = None
    total = 0

    for stage in all_stages:
        pid = stage.get("_pid", "?")
        probe = stage.get("probe", "?")
        service = stage.get("service", "?")
        name = stage.get("name", "?")
        dur = stage.get("duration_ms")

        # Print process boundary header when we cross into a new process
        if pid != prev_pid:
            process_label = (
                "gunicorn/django worker"
                if "gunicorn" not in probe
                else "gunicorn worker"
            )
            print(
                c(
                    f"  ── pid={pid} ({'celery worker' if 'celery' in probe else 'gunicorn worker'}) ──",
                    DIM,
                )
            )
            prev_pid = pid

        dur_str = (
            f"{dur:8.2f}ms" if dur is not None else "        —"
        )
        bar_len = int(min((dur or 0) / 5, 40))
        bar = "▓" * bar_len
        colour = (
            RED
            if (dur or 0) > 100
            else YELLOW if (dur or 0) > 20 else GREEN
        )

        print(
            c(f"  {dur_str}  ", WHITE)
            + c(f"{bar:<40}", colour)
            + c(f"  {probe}", BOLD)
            + c(f"  {service}::{name}", DIM)
        )
        total += dur or 0

    print(
        c(
            f"\n  End-to-end: {total:.2f}ms  ·  "
            f"{len(all_stages)} stages  ·  "
            f"{len(found_in)} process(es)\n",
            BOLD,
            CYAN,
        )
    )


HISTORY_FILE = os.path.expanduser("~/.stacktracer_history")


def main():
    try:
        import readline

        try:
            readline.read_history_file(HISTORY_FILE)
        except FileNotFoundError:
            pass
        readline.set_history_length(200)
    except ImportError:
        pass  # Windows — fine without it

    print(
        c("\n  StackTracer REPL", BOLD, CYAN)
        + c("  live agent mode", DIM)
    )
    print(c("  Type \\help for commands, Ctrl+C to exit\n", DIM))

    sock_path = pick_socket()

    # Quick health check on connect
    status = query(sock_path, "SHOW STATUS")
    if status.get("ok"):
        d = status["data"]
        dim(
            f"  graph: {d.get('graph_nodes', '?')} nodes  ·  "
            f"{d.get('graph_edges', '?')} edges  ·  "
            f"pid={d.get('pid', '?')}"
        )
    print()

    while True:
        try:
            raw = input(c("› ", BOLD, BLUE)).strip()
        except (KeyboardInterrupt, EOFError):
            print()
            ok("Bye.")
            break

        if not raw:
            continue

        try:
            import readline

            readline.write_history_file(HISTORY_FILE)
        except Exception:
            pass

        # ── REPL meta-commands ─────────────────────────────────────
        if raw.startswith("\\"):
            parts = raw[1:].split(None, 1)
            cmd = parts[0].lower()
            args = parts[1] if len(parts) > 1 else ""

            if cmd in ("quit", "exit", "q"):
                ok("Bye.")
                break
            elif cmd == "help":
                cmd_help()
            elif cmd == "status":
                cmd_status(sock_path)
            elif cmd == "probes":
                cmd_probes(sock_path)
            elif cmd == "rules":
                cmd_rules(sock_path)
            elif cmd == "semantic":
                cmd_semantic(sock_path)
            elif cmd == "emit":
                cmd_emit(sock_path, args)
            elif cmd == "snapshot":
                cmd_snapshot(sock_path, args)
            elif cmd == "active":
                render(query(sock_path, "SHOW ACTIVE"))
            elif cmd == "reconnect":
                sock_path = pick_socket()
                st = query(sock_path, "SHOW STATUS")
                if st.get("ok"):
                    d = st["data"]
                    ok(
                        f"Reconnected  pid={d.get('pid', '?')}  "
                        f"nodes={d.get('graph_nodes', '?')}"
                    )
            elif cmd == "stitch":
                cmd_stitch(args.strip())
            else:
                err(f"Unknown command: \\{cmd}  (try \\help)")
            continue

        # ── DSL query — forwarded verbatim to the live engine ──────
        t0 = time.perf_counter()
        result = query(sock_path, raw)
        print(f"DEBUG raw: {result}")  # add this temporarily
        elapsed = (time.perf_counter() - t0) * 1000
        render(result)
        if result.get("ok"):
            dim(f"  {elapsed:.1f}ms")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
scripts/repl.py

StackTracer development REPL.
Run: python scripts/repl.py

No React. No API server. Direct access to the engine.
Useful for:
  - Testing probes locally
  - Writing and validating causal rules
  - Exploring the runtime graph during development
  - Running DSL queries against live or replayed data
"""

from __future__ import annotations

import json
import os
import sys
import time
import textwrap

# Make sure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ------------------------------------------------------------------ #
# Colour helpers (no dependencies)
# ------------------------------------------------------------------ #

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
BLUE   = "\033[34m"
CYAN   = "\033[36m"
WHITE  = "\033[37m"

def c(text, *codes): return "".join(codes) + str(text) + RESET
def header(text):    print(c(f"\n  {text}", BOLD, CYAN))
def ok(text):        print(c(f"  ✓ {text}", GREEN))
def warn(text):      print(c(f"  ⚠ {text}", YELLOW))
def err(text):       print(c(f"  ✗ {text}", RED))
def dim(text):       print(c(f"  {text}", DIM))


# ------------------------------------------------------------------ #
# Result rendering
# ------------------------------------------------------------------ #

def render(result: dict) -> None:
    """Pretty-print a DSL query result to the terminal."""

    if "error" in result:
        err(result["error"])
        return

    verb = result.get("verb", "")
    metric = result.get("metric", "")

    # ---- CAUSAL ----
    if verb == "CAUSAL":
        matches = result.get("data", [])
        if not matches:
            dim("No causal patterns matched.")
            return
        print()
        for m in matches:
            pct = int(m["confidence"] * 100)
            bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
            colour = RED if pct >= 80 else YELLOW
            print(c(f"  [{bar}] {pct}%  {m['rule']}", colour, BOLD))
            wrapped = textwrap.fill(m["explanation"], width=72, initial_indent="        ", subsequent_indent="        ")
            print(c(wrapped, DIM))
            evidence = m.get("evidence", {})
            if evidence:
                print(c(f"        Evidence: {json.dumps(evidence, indent=None)}", DIM))
            print()
        return

    # ---- DIFF ----
    if verb == "DIFF":
        new  = result.get("new_edges", [])
        gone = result.get("removed_edges", [])
        if not new and not gone:
            dim("No graph changes detected.")
            return
        if new:
            print(c(f"\n  + {len(new)} new edge(s):", GREEN, BOLD))
            for e in new:
                print(c(f"      {e}", GREEN))
        if gone:
            print(c(f"\n  - {len(gone)} removed edge(s):", RED, BOLD))
            for e in gone:
                print(c(f"      {e}", RED))
        print()
        return

    # ---- HOTSPOT / tabular data ----
    if verb in ("HOTSPOT",) or isinstance(result.get("data"), list):
        rows = result.get("data", [])
        if not rows:
            dim("No results.")
            return
        _render_table(rows)
        return

    # ---- TRACE / critical path ----
    if verb == "TRACE":
        path = result.get("data", [])
        if not path:
            dim("Trace not found in event log.")
            return
        print()
        total = 0
        for stage in path:
            dur = stage.get("duration_ms")
            dur_str = f"{dur:8.2f}ms" if dur is not None else "        —"
            bar_len = int(min((dur or 0) / 5, 40))  # 5ms per block, max 40
            bar = "▓" * bar_len
            colour = RED if (dur or 0) > 100 else YELLOW if (dur or 0) > 20 else GREEN
            print(
                c(f"  {dur_str}  ", WHITE) +
                c(f"{bar:<40}", colour) +
                c(f"  {stage['probe']}", BOLD) +
                c(f"  {stage['service']}::{stage['name']}", DIM)
            )
            total += dur or 0
        print(c(f"\n  Total: {total:.2f}ms  ·  {len(path)} stages", BOLD, CYAN))
        print()
        return

    # ---- BLAME ----
    if verb == "BLAME":
        data = result.get("data", [])
        resolved = result.get("resolved_nodes", [])
        print(c(f"\n  System nodes: {', '.join(resolved)}", DIM))
        if not data:
            dim("No upstream callers found.")
            return
        print()
        for row in data:
            print(c(f"  {row['call_count']:6}x  ", YELLOW, BOLD) + c(row["caller"], WHITE))
        print()
        return

    # ---- SHOW graph ----
    if metric == "graph":
        g = result.get("data", {})
        nodes = g.get("nodes", [])
        edges = g.get("edges", [])
        print(c(f"\n  {len(nodes)} nodes  ·  {len(edges)} edges\n", BOLD))
        for n in nodes:
            print(c(f"  [{n['type']:12}]  ", DIM) + c(n["id"], WHITE) +
                  c(f"  ×{n['call_count']}", CYAN))
        if edges:
            print()
            for e in edges:
                print(c(f"  {e['source']}", YELLOW) +
                      c(f"  →  ", DIM) +
                      c(e["target"], YELLOW) +
                      c(f"  [{e['type']} ×{e['call_count']}]", DIM))
        print()
        return

    # ---- Fallback: raw JSON ----
    print(json.dumps(result, indent=2, default=str))


def _render_table(rows: list) -> None:
    if not rows:
        return
    keys = list(rows[0].keys())
    widths = {k: max(len(k), max(len(str(r.get(k, ""))) for r in rows)) for k in keys}

    # Header
    header_line = "  " + "  ".join(c(k.upper().replace("_", " "), BOLD, DIM).ljust(widths[k] + 11) for k in keys)
    print()
    print(header_line)
    print(c("  " + "─" * (sum(widths.values()) + len(keys) * 2 + 2), DIM))

    # Rows
    for row in rows:
        parts = []
        for k in keys:
            val = str(row.get(k, "—"))
            # Colour numbers
            try:
                f = float(val)
                colour = RED if f > 100 else YELLOW if f > 20 else GREEN
                parts.append(c(val.ljust(widths[k]), colour))
            except ValueError:
                parts.append(val.ljust(widths[k]))
        print("  " + "  ".join(parts))
    print()


# ------------------------------------------------------------------ #
# Built-in REPL commands (not DSL — prefixed with \)
# ------------------------------------------------------------------ #

def cmd_status(engine):
    s = engine.status()
    header("Engine Status")
    for k, v in s.items():
        print(f"  {c(k, DIM):<30} {c(v, WHITE)}")
    print()

def cmd_probes():
    from stacktracer.sdk.base_probe import ProbeRegistry
    header("Registered Probes")
    available = ProbeRegistry.available()
    if not available:
        dim("None registered (no probes loaded yet)")
        return
    for name in available:
        ok(name)
    print()

def cmd_rules(engine):
    header("Causal Rules")
    for name in engine.causal.rule_names():
        ok(name)
    print()

def cmd_semantic(engine):
    header("Semantic Aliases")
    labels = engine.semantic.all_labels()
    if not labels:
        dim("None registered")
        return
    for label in labels:
        desc = engine.semantic.describe(label)
        print(f"  {c(label, BOLD, CYAN):<20} {c(desc or '', DIM)}")
    print()

def cmd_emit(engine, args: str):
    """
    Manually inject a synthetic event for testing.
    Usage: \emit request.entry django /api/test
    """
    parts = args.split()
    if len(parts) < 3:
        err("Usage: \\emit <probe> <service> <name>")
        return
    from stacktracer.core.event_schema import NormalizedEvent
    import uuid
    probe, service, name = parts[0], parts[1], parts[2]
    trace_id = str(uuid.uuid4())
    event = NormalizedEvent.now(probe=probe, trace_id=trace_id, service=service, name=name)
    engine.process(event)
    ok(f"Emitted  {probe}  {service}::{name}  trace={trace_id[:8]}")

def cmd_snapshot(engine, args: str):
    label = args.strip() or None
    diff = engine.snapshot(label=label)
    header(f"Snapshot {'(' + label + ')' if label else ''}")
    new_n = len(diff.get("added_nodes", []))
    new_e = len(diff.get("added_edges", []))
    print(f"  New nodes: {c(new_n, CYAN)}  New edges: {c(new_e, CYAN)}")
    print()

def cmd_help():
    header("DSL Commands")
    queries = [
        ('SHOW latency WHERE service = "django"',   'Latency by node, filtered'),
        ('SHOW latency WHERE system = "export"',    'Latency scoped to semantic alias'),
        ('SHOW graph',                              'Full runtime graph'),
        ('SHOW graph WHERE system = "worker"',      'Subgraph for a system'),
        ('SHOW events WHERE probe = "db.query.start" LIMIT 20', 'Raw event log'),
        ('HOTSPOT TOP 10',                          'Busiest nodes by call count'),
        ('CAUSAL',                                  'Run all causal rules'),
        ('CAUSAL WHERE tags = "blocking,celery"',   'Filtered causal rules'),
        ('BLAME WHERE system = "export"',           'Upstream callers of a system'),
        ('DIFF SINCE deployment',                   'Graph changes after named event'),
        ('TRACE <trace_id>',                        'Reconstruct critical path'),
    ]
    for q, desc in queries:
        print(f"  {c(q, CYAN)}")
        print(c(f"    {desc}", DIM))
    print()
    header("REPL Commands")
    cmds = [
        ('\\status',               'Engine stats (graph size, rules, etc.)'),
        ('\\probes',               'List registered probe adapters'),
        ('\\rules',                'List registered causal rules'),
        ('\\semantic',             'List semantic aliases'),
        ('\\emit <probe> <svc> <name>', 'Inject a synthetic event'),
        ('\\snapshot [label]',     'Capture a temporal diff snapshot'),
        ('\\help',                 'This message'),
        ('\\quit  or  Ctrl+C',     'Exit'),
    ]
    for cmd, desc in cmds:
        print(f"  {c(cmd, YELLOW):<40} {c(desc, DIM)}")
    print()


# ------------------------------------------------------------------ #
# Bootstrap: init engine with no probes so REPL starts fast
# ------------------------------------------------------------------ #

def bootstrap(sample_rate: float = 1.0) -> object:
    """Start a minimal engine — no probes, InMemoryRepository."""
    from stacktracer.core.engine import Engine
    from stacktracer.core.causal import build_default_registry
    from stacktracer.core.active_requests import ActiveRequestTracker
    from stacktracer.storage.repository import InMemoryRepository
    from stacktracer.sdk.emitter import bind_engine

    tracker = ActiveRequestTracker()
    engine  = Engine(
        causal_registry=build_default_registry(tracker=tracker),
        snapshot_interval_s=9999,   # manual snapshots only in REPL
    )
    engine.tracker = tracker
    engine.repository = InMemoryRepository()
    bind_engine(engine)
    return engine


# ------------------------------------------------------------------ #
# Main loop
# ------------------------------------------------------------------ #

HISTORY_FILE = os.path.expanduser("~/.stacktracer_history")

def main():
    # readline for history + arrow keys
    try:
        import readline
        try:
            readline.read_history_file(HISTORY_FILE)
        except FileNotFoundError:
            pass
        readline.set_history_length(200)
    except ImportError:
        pass  # Windows — fine without it

    print(c("\n  StackTracer REPL", BOLD, CYAN) + c("  development mode", DIM))
    print(c("  Type \\help for commands, Ctrl+C to exit\n", DIM))

    engine = bootstrap()
    ok("Engine ready  (InMemoryRepository, no probes)")
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

        # Save to readline history
        try:
            import readline
            readline.write_history_file(HISTORY_FILE)
        except Exception:
            pass

        # ---- REPL meta-commands ----
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
                cmd_status(engine)
            elif cmd == "probes":
                cmd_probes()
            elif cmd == "rules":
                cmd_rules(engine)
            elif cmd == "semantic":
                cmd_semantic(engine)
            elif cmd == "emit":
                cmd_emit(engine, args)
            elif cmd == "snapshot":
                cmd_snapshot(engine, args)
            else:
                err(f"Unknown command: \\{cmd}  (try \\help)")
            continue

        # ---- DSL query ----
        try:
            from stacktracer.query.parser import parse, execute
            t0 = time.perf_counter()
            parsed = parse(raw)
            result = execute(parsed, engine)
            elapsed = (time.perf_counter() - t0) * 1000
            render(result)
            dim(f"  {elapsed:.1f}ms")
        except ValueError as exc:
            err(f"Parse error: {exc}")
        except Exception as exc:
            err(f"Error: {exc}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
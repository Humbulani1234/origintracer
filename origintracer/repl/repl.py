#!/usr/bin/env python3
r"""Interactive terminal client for a running OriginTracer worker.

Start the application with OriginTracer initialized, then run the REPL in a
second terminal:

    python -m origintracer.repl.repl

The REPL discovers live worker sockets under ``/tmp/origintracer-<pid>.sock``.
If more than one worker is available, it asks which one to query. Structured
results are shown as colored tables with measurements, process identity, and
probe details where available.

Query examples (see ``\help`` for the grouped command list):

    SHOW latency WHERE service = "django"
    SHOW latency WHERE system = "database"
    SHOW nodes WHERE service = "nginx" LIMIT 20
    SHOW edges
    SHOW graph WHERE system = "worker"
    SHOW events WHERE probe = "django.db.query" LIMIT 20
    SHOW path WHERE trace_id = "<trace-id>"
    SHOW llm_content WHERE trace_id = "<trace-id>"
    SHOW tool_content WHERE trace_id = "<trace-id>"
    SHOW status
    SHOW active
    SHOW probes
    SHOW rules
    SHOW semantic
    TRACE <trace-id>
    HOTSPOT TOP 10
    CAUSAL
    CAUSAL WHERE tags = "blocking,celery"
    BLAME WHERE system = "database"
    DIFF SINCE deployment
    SHOW changes

``SHOW llm_content`` and ``SHOW tool_content`` return captured previews only
when their respective ``ORIGINTRACER_CAPTURE_LLM_CONTENT=1`` and
``ORIGINTRACER_CAPTURE_TOOL_CONTENT=1`` settings were enabled before the
worker started. The capture settings are off by default.

``TRACE`` shows the trace's ordered events and any reported timing. The gap
column is time between events; recorded duration is a separate measurement.
``\stitch <trace-id>`` queries every live local worker socket for that ID and
shows a cross-process event timeline. It does not calculate a combined
cross-worker wall duration.

Commands beginning with ``SHOW``, ``TRACE``, ``HOTSPOT``, ``CAUSAL``,
``BLAME``, and ``DIFF`` are query DSL commands. Backslash commands control the
REPL session. ``\emit`` and ``\snapshot`` depend on optional socket-server
support and may be reported as unavailable.
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import socket
import sys
import textwrap
import time
import uuid

# Colour helpers
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


def readline_prompt(text, *codes):
    """Return a coloured prompt whose ANSI escapes have zero display width.

    GNU readline redraws a line for cursor movement and command history.  Its
    ``\001``/``\002`` delimiters mark terminal control bytes as non-printing;
    without them, the blue/bold prompt shifts the cursor when an arrow key is
    used.  Do not use this for normal printed output: those delimiters are
    meaningful only while readline is processing an input prompt.
    """
    return (
        "\001"
        + "".join(codes)
        + "\002"
        + str(text)
        + "\001"
        + RESET
        + "\002"
    )


def header(text):
    print(c(f"\n  {text}", BOLD, CYAN))


def ok(text):
    print(c(f"  OK {text}", GREEN))


def warn(text):
    print(c(f"  WARN {text}", YELLOW))


def err(text):
    print(c(f"  FAIL {text}", RED))


def dim(text):
    print(c(f"  {text}", DIM))


# Unix socket client
_SOCKET_PREFIX = "/tmp/origintracer-"
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
            # Process is dead - remove the stale socket
            try:
                os.unlink(path)
            except OSError:
                pass
        except PermissionError:
            # Process is alive but owned by a privileged user (e.g. gunicorn run with sudo for eBPF access)
            # The REPL must also be run with elevated privileges to connect to it.
            print(
                f"[origintracer] Found socket {path} but cannot access it - "
                "gunicorn appears to be running with elevated privileges (sudo) "
                "likely for eBPF access.\n"
                "Please restart the REPL with sudo:\n\n"
                "    sudo python -m origintracer.repl.repl\n"
            )
            sys.exit(1)
    return live


def pick_socket() -> str:
    """
    Auto-discover sockets.
    If multiple exist, prompt the user to pick one.
    If none exist, print instructions and exit.
    """
    sockets = discover_sockets()

    if not sockets:
        err("No OriginTracer agent found.")
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
    Send one query to OriginTracer. Opens and closes a fresh connection
    per call. Returns the full parsed response dict.
    """
    if sys.platform != "linux":
        raise RuntimeError("Unix sockets not supported")
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
            "error": f"Socket gone: {sock_path} - did the worker restart?",
        }
    except socket.timeout:
        return {"ok": False, "error": "Query timed out (>10 s)"}
    except ConnectionRefusedError:
        return {
            "ok": False,
            "error": "Connection refused - worker may have restarted",
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


# Result rendering
def render(result: dict) -> None:
    """
    Pretty-print a query result returned by the agent.
    """

    # Socket-level failure
    if not result.get("ok"):
        err(result.get("error", "Unknown error"))
        return

    data = result.get("data")

    # TODO: look into this nested nature and simplify
    # Unwrap executor - executor returns {"metric": "...", "data": <payload>}
    # local_server wraps that in {"ok": True, "data": <executor_result>}
    # So result["data"] is the executor result, and the real payload is inside that.
    if isinstance(data, dict) and "data" in data:
        verb = data.get("verb", "")
        metric = data.get("metric", "")
        data = data["data"]  # unwrap to actual payload
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

    # CAUSAL
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
            if isinstance(evidence, dict) and evidence:
                print(c("        Evidence:", DIM))
                _render_table(
                    [
                        {
                            "measurement": key.replace("_", " "),
                            "value": _format_value(value),
                        }
                        for key, value in evidence.items()
                    ],
                    indent="          ",
                    column_colors={
                        "measurement": CYAN,
                        "value": WHITE,
                    },
                    prefer_table=True,
                )
            print()
        return

    # DIFF
    if verb == "DIFF":
        d = data if isinstance(data, dict) else {}
        changes = [
            {"change": "added", "edge": edge}
            for edge in d.get("new_edges", [])
        ] + [
            {"change": "removed", "edge": edge}
            for edge in d.get("removed_edges", [])
        ]
        if not changes:
            dim("No graph changes detected.")
            return
        _render_table(
            changes,
            column_colors={"change": CYAN, "edge": WHITE},
            prefer_table=True,
        )
        return

    # SHOW EVENTS
    if metric == "events":
        rows = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        _render_events(rows)
        return

    # TRACE and event timelines share the REPL's measured table style.
    if verb == "TRACE":
        path = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        if not path:
            dim("Trace not found in event log.")
            return
        rows = []
        total = 0.0
        for stage in path:
            gap = stage.get("duration_ms")
            recorded = stage.get("duration_ns")
            context = " · ".join(
                value
                for value in (
                    (
                        f"pid={stage['pid']}"
                        if stage.get("pid") is not None
                        else ""
                    ),
                    (
                        f"tid={stage['tid']}"
                        if stage.get("tid") is not None
                        else ""
                    ),
                    (
                        f"span={stage['span_id']}"
                        if stage.get("span_id")
                        else ""
                    ),
                    (
                        f"parent={stage['parent_span_id']}"
                        if stage.get("parent_span_id")
                        else ""
                    ),
                )
                if value
            )
            rows.append(
                {
                    "gap": (
                        f"{gap:.2f} ms"
                        if gap is not None
                        else "—"
                    ),
                    "stage": f"{stage.get('probe', '?')} · {stage.get('service', '?')}::{stage.get('name', '?')}",
                    "recorded": (
                        f"{recorded / 1e6:.3f} ms"
                        if recorded is not None
                        else "—"
                    ),
                    "context": context or "—",
                    "details": (
                        _format_value(stage.get("metadata"))
                        if stage.get("metadata")
                        else "—"
                    ),
                }
            )
            total += gap or 0
        _render_table(
            rows,
            column_colors={
                "stage": CYAN,
                "context": YELLOW,
                "details": DIM,
            },
            latency_columns={"gap", "recorded"},
            prefer_table=True,
        )
        print(
            c(
                f"  Sum of reported inter-event gaps: {total:.2f} ms · {len(path)} stages",
                BOLD,
                CYAN,
            )
        )
        print()
        return

    # BLAME
    if verb == "BLAME":
        d = data if isinstance(data, dict) else {}
        rows = (
            d.get("data", data)
            if isinstance(data, list)
            else d.get("data", [])
        )
        resolved = d.get("resolved_nodes", [])
        if resolved:
            dim(f"  System nodes: {', '.join(resolved)}")
        if not rows:
            dim("No upstream callers found.")
            return
        _render_table(
            [
                {
                    "caller": row.get("caller", "?"),
                    "calls": row.get("call_count", 0),
                }
                for row in rows
            ],
            column_colors={"caller": WHITE, "calls": CYAN},
            prefer_table=True,
        )
        return

    # SHOW STATUS
    if verb == "STATUS":
        assert isinstance(data, dict)
        _render_engine_status(data)
        return

    # SHOW GRAPH
    if metric == "graph":
        g = data if isinstance(data, dict) else {}
        nodes, edges = g.get("nodes", []), g.get("edges", [])
        print(
            c(
                f"\n  {len(nodes)} nodes · {len(edges)} edges\n",
                BOLD,
            )
        )
        _render_node_rows(nodes)
        if edges:
            _render_table(
                [
                    {
                        "source": edge.get(
                            "source", edge.get("from", "?")
                        ),
                        "target": edge.get(
                            "target", edge.get("to", "?")
                        ),
                        "relationship": edge.get("type", "—"),
                        "calls": edge.get(
                            "call_count", edge.get("weight", "—")
                        ),
                    }
                    for edge in edges
                ],
                column_colors={
                    "source": YELLOW,
                    "target": YELLOW,
                    "relationship": DIM,
                    "calls": CYAN,
                },
                prefer_table=True,
            )
        return

    # LLM content is rendered separately so prompt/response text does not make
    # the normal event table unreadably wide. It is only populated when the
    # LangGraph probe's opt-in capture setting is enabled.
    if metric == "llm_content":
        rows = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        _render_llm_content(rows)
        return

    if metric == "tool_content":
        rows = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        _render_tool_content(rows)
        return

    # SHOW NODES
    if metric == "nodes":
        rows = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        _render_node_rows(rows)
        return

    # HOTSPOT or edges
    if verb == "HOTSPOT" or metric == "edges":
        rows = (
            data
            if isinstance(data, list)
            else (data or {}).get("data", [])
        )
        if not rows:
            dim("No results.")
            return
        _render_table(rows, prefer_table=True)
        return

    # SNAPSHOT result
    if isinstance(data, dict) and "added_nodes" in data:
        new_n = len(data.get("added_nodes", []))
        new_e = len(data.get("added_edges", []))
        label = data.get("label", "")
        header(f"Snapshot {'(' + label + ')' if label else ''}")
        _render_table(
            [
                {"measurement": "new nodes", "count": new_n},
                {"measurement": "new edges", "count": new_e},
            ],
            column_colors={"measurement": CYAN, "count": GREEN},
            prefer_table=True,
        )
        return

    # Plain list of strings (e.g. SHOW PROBES, SHOW RULES)
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

    # Empty list
    if isinstance(data, list) and not data:
        dim("No results.")
        return

    # Render unfamiliar structured responses as field tables instead of raw JSON.
    fallback = data if data is not None else result
    if isinstance(fallback, dict):
        _render_mapping(fallback)
    elif (
        isinstance(fallback, list)
        and fallback
        and all(isinstance(item, dict) for item in fallback)
    ):
        _render_table(fallback, prefer_table=True)
    else:
        print(_format_value(fallback))


def _render_node_rows(rows: list) -> None:
    """Display graph nodes in measurement columns with probe details."""
    if not rows:
        dim("No results.")
        return
    table = []
    for row in rows:
        metadata = row.get("metadata")
        details = (
            " · ".join(
                f"{key.replace('_', ' ')}={_format_value(value)}"
                for key, value in metadata.items()
            )
            if isinstance(metadata, dict)
            else "—"
        )
        avg = row.get("avg_ms")
        table.append(
            {
                "service": row.get(
                    "service", row.get("type", "?")
                ),
                "node": row.get("id", "?"),
                "calls": row.get("call_count", 0),
                "avg": (
                    f"{avg:.2f} ms"
                    if isinstance(avg, (int, float))
                    else "—"
                ),
                "details": details or "—",
            }
        )
    _render_table(
        table,
        column_colors={
            "service": CYAN,
            "node": WHITE,
            "calls": CYAN,
            "details": DIM,
        },
        latency_columns={"avg"},
        prefer_table=True,
    )


def _render_engine_status(
    data: dict, *, connected: bool = False
) -> None:
    """Render worker health as a compact, readable status summary."""
    print()
    if connected:
        ok("Engine Status")
    else:
        header("Engine Status")

    active_probes = data.get("probes_active", [])
    active_count: int | str
    if isinstance(active_probes, (list, tuple)):
        probe_names = (
            ", ".join(str(name) for name in active_probes)
            or "none"
        )
        active_count = len(active_probes)
    else:
        probe_names = str(active_probes)
        active_count = "?"
    probe_total = data.get("probes_total", "?")

    rows = [
        {
            "area": "Process",
            "details": (
                f"PID {data.get('pid', '?')} · "
                f"uptime {data.get('uptime', 'unknown')}"
            ),
        },
        {
            "area": "Socket",
            "details": str(data.get("socket", "?")),
        },
        {
            "area": "Graph",
            "details": (
                f"{data.get('graph_nodes', '?')} nodes · "
                f"{data.get('graph_edges', '?')} edges · "
                f"{data.get('semantic_labels', '?')} semantic labels"
            ),
        },
        {
            "area": "Probes",
            "details": (
                f"{active_count} active / {probe_total} total · "
                f"{probe_names}"
            ),
        },
        {
            "area": "Requests",
            "details": f"{data.get('active_requests', '?')} active",
        },
        {
            "area": "Events",
            "details": f"{data.get('event_log_size', '?')} retained",
        },
    ]
    _render_table(
        rows,
        column_colors={"area": CYAN, "details": WHITE},
        prefer_table=True,
    )
    print()


def _render_observation_metadata(
    metadata, indent: str = "  "
) -> None:
    """Render nested observation metadata as readable field/value tables."""
    if isinstance(metadata, dict) and metadata:
        _render_mapping(metadata, indent=indent)


def _format_value(value) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dict):
        return ", ".join(
            f"{key}: {_format_value(item)}"
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set)):
        return (
            ", ".join(_format_value(item) for item in value)
            or "—"
        )
    return str(value)


def _render_mapping(mapping: dict, indent: str = "  ") -> None:
    rows: list[dict[str, str]] = []
    for key, value in mapping.items():
        label = str(key).replace("_", " ")
        if isinstance(value, dict):
            if rows:
                _render_table(
                    rows,
                    indent=indent,
                    column_colors={
                        "field": CYAN,
                        "value": WHITE,
                    },
                    prefer_table=True,
                )
                rows = []
            print(c(f"{indent}{label}", BOLD, DIM))
            _render_mapping(value, indent=indent + "  ")
        elif (
            isinstance(value, list)
            and value
            and all(isinstance(item, dict) for item in value)
        ):
            if rows:
                _render_table(
                    rows,
                    indent=indent,
                    column_colors={
                        "field": CYAN,
                        "value": WHITE,
                    },
                    prefer_table=True,
                )
                rows = []
            print(c(f"{indent}{label}", BOLD, DIM))
            _render_table(
                value, indent=indent + "  ", prefer_table=True
            )
        else:
            rows.append(
                {"field": label, "value": _format_value(value)}
            )
    if rows:
        _render_table(
            rows,
            indent=indent,
            column_colors={"field": CYAN, "value": WHITE},
            prefer_table=True,
        )


def _render_events(rows: list) -> None:
    """Display event rows with measurements and trace/process context."""
    if not rows:
        dim("No results.")
        return
    table = []
    for row in rows:
        ts = row.get("ts") or row.get("wall_time")
        context = " · ".join(
            value
            for value in (
                (
                    f"pid={row['pid']}"
                    if row.get("pid") is not None
                    else ""
                ),
                (
                    f"tid={row['tid']}"
                    if row.get("tid") is not None
                    else ""
                ),
                (
                    f"trace={row['trace_id']}"
                    if row.get("trace_id")
                    else ""
                ),
                (
                    f"span={row['span_id']}"
                    if row.get("span_id")
                    else ""
                ),
                (
                    f"parent={row['parent_span_id']}"
                    if row.get("parent_span_id")
                    else ""
                ),
            )
            if value
        )
        duration = row.get("duration_ns")
        table.append(
            {
                "time": (
                    time.strftime("%H:%M:%S", time.localtime(ts))
                    if ts
                    else "—"
                ),
                "event": f"{row.get('probe', '?')} · {row.get('service', '?')}::{row.get('name', '?')}",
                "duration": (
                    f"{duration / 1e6:.3f} ms"
                    if duration is not None
                    else "—"
                ),
                "context": context or "—",
                "details": (
                    _format_value(row.get("metadata"))
                    if row.get("metadata")
                    else "—"
                ),
            }
        )
    _render_table(
        table,
        column_colors={
            "time": DIM,
            "event": CYAN,
            "context": YELLOW,
            "details": DIM,
        },
        latency_columns={"duration"},
        prefer_table=True,
    )


def _render_table(
    rows: list,
    indent: str = "  ",
    column_colors: dict | None = None,
    latency_columns: set[str] | None = None,
    prefer_table: bool = False,
) -> None:
    """Render terminal-width-aware aligned columns with colored measurements."""
    if not rows:
        return
    keys = list(
        dict.fromkeys(key for row in rows for key in row)
    )
    cells = {
        key: [_format_value(row.get(key)) for row in rows]
        for key in keys
    }
    if not prefer_table and any(
        len(value) > 36
        for values in cells.values()
        for value in values
    ):
        for index, row in enumerate(rows, start=1):
            if len(rows) > 1:
                print(c(f"{indent}row {index}", DIM))
            _render_mapping(
                row,
                indent=indent + ("  " if len(rows) > 1 else ""),
            )
        return
    headings = {
        key: str(key).replace("_", " ").upper() for key in keys
    }
    minimums = {
        "time": 8,
        "event": 22,
        "stage": 22,
        "probe": 18,
        "operation": 16,
        "duration": 10,
        "gap": 10,
        "recorded": 10,
        "service": 8,
        "node": 18,
        "calls": 5,
        "avg": 8,
        "context": 18,
        "details": 7,
    }
    widths = {
        key: max(
            len(headings[key]),
            minimums.get(key, 0),
            min(
                36,
                max(
                    (len(value) for value in cells[key]),
                    default=0,
                ),
            ),
        )
        for key in keys
    }
    width_limit = max(
        sum(len(value) for value in headings.values()),
        shutil.get_terminal_size(fallback=(120, 24)).columns
        - len(indent)
        - 2 * max(len(keys) - 1, 0),
    )
    while sum(widths.values()) > width_limit:
        candidates = [
            key
            for key in keys
            if widths[key]
            > max(len(headings[key]), minimums.get(key, 0))
        ]
        if not candidates:
            break
        widest = max(
            candidates,
            key=lambda key: widths[key] - len(headings[key]),
        )
        widths[widest] -= 1

    def wrap_cell(value: str, width: int) -> list[str]:
        return textwrap.wrap(
            value,
            width=width,
            break_long_words=True,
            break_on_hyphens=False,
        ) or [""]

    column_colors = column_colors or {}
    latency_columns = latency_columns or set()
    print()
    print(
        indent
        + "  ".join(
            c(headings[key].ljust(widths[key]), BOLD, DIM)
            for key in keys
        )
    )
    print(indent + "  ".join("─" * widths[key] for key in keys))
    for row_index in range(len(rows)):
        lines = [
            wrap_cell(cells[key][row_index], widths[key])
            for key in keys
        ]
        for line_index in range(
            max(len(value) for value in lines)
        ):
            rendered = []
            for key, wrapped in zip(keys, lines):
                value = (
                    wrapped[line_index]
                    if line_index < len(wrapped)
                    else ""
                )
                padded = value.ljust(widths[key])
                if key in latency_columns:
                    try:
                        number = float(
                            cells[key][row_index].split()[0]
                        )
                        color = (
                            RED
                            if number > 100
                            else YELLOW if number > 20 else GREEN
                        )
                        rendered.append(c(padded, color))
                    except (TypeError, ValueError):
                        rendered.append(c(padded, DIM))
                elif key in column_colors:
                    rendered.append(
                        c(padded, column_colors[key])
                    )
                else:
                    try:
                        number = float(value)
                        color = (
                            RED
                            if number > 100
                            else YELLOW if number > 20 else GREEN
                        )
                        rendered.append(c(padded, color))
                    except ValueError:
                        rendered.append(padded)
            print(indent + "  ".join(rendered))
    print()


def _render_llm_content(rows: list) -> None:
    """Display captured LLM previews as readable per-event blocks."""
    if not rows:
        dim(
            "No captured LLM content. Enable capture before the next run."
        )
        return
    print()
    for row in rows:
        truncated = (
            " (truncated)" if row.get("truncated") else ""
        )
        print(
            c(
                f"  {row.get('probe', '?')}  {row.get('name', '?')}"
                f"  trace={str(row.get('trace_id', ''))[:16]}…{truncated}",
                BOLD,
                CYAN,
            )
        )
        content = str(row.get("content", ""))
        for line in content.splitlines() or [""]:
            print(f"    {line}")
        print()


def _render_tool_content(rows: list) -> None:
    """Display captured tool inputs, outputs, and errors as readable blocks."""
    if not rows:
        dim(
            "No captured tool content. Enable capture before the next run."
        )
        return
    print()
    for row in rows:
        truncated = (
            " (truncated)" if row.get("truncated") else ""
        )
        kind = row.get("kind", "content")
        print(
            c(
                f"  {kind}  {row.get('name', '?')}"
                f"  trace={str(row.get('trace_id', ''))[:16]}…{truncated}",
                BOLD,
                CYAN,
            )
        )
        content = str(row.get("content", ""))
        for line in content.splitlines() or [""]:
            print(f"    {line}")
        print()


# Meta-command handlers
def cmd_status(sock_path: str) -> None:
    """Display live worker identity, uptime, graph size, and activity."""
    render(query(sock_path, "SHOW STATUS"))


def cmd_probes(sock_path: str) -> None:
    """List probe adapters registered with the selected worker."""
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
    entries = data if isinstance(data, list) else [data]
    _render_table(
        [{"probe": str(item)} for item in entries],
        column_colors={"probe": CYAN},
        prefer_table=True,
    )


def cmd_rules(sock_path: str) -> None:
    """List causal rules registered with the selected worker."""
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
    entries = data if isinstance(data, list) else [data]
    _render_table(
        [{"rule": str(item)} for item in entries],
        column_colors={"rule": CYAN},
        prefer_table=True,
    )


def cmd_semantic(sock_path: str) -> None:
    """List semantic aliases and their descriptions."""
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
    entries = data if isinstance(data, list) else [data]
    _render_table(
        [
            (
                {
                    "alias": item.get("label", ""),
                    "description": item.get("description", ""),
                }
                if isinstance(item, dict)
                else {"alias": str(item), "description": "—"}
            )
            for item in entries
        ],
        column_colors={"alias": CYAN, "description": DIM},
        prefer_table=True,
    )


def cmd_emit(sock_path: str, args: str) -> None:
    r"""Request a synthetic event injection when supported by the socket server.

    Usage: ``\emit <probe> <service> <name>``. If the server does not support
    ``INJECT``, the command reports that no event was emitted.
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
    """Request a temporal graph snapshot if the socket server supports it.

    The current local query server may report SNAPSHOT as unavailable.
    """
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
    header("Query DSL")
    groups = [
        (
            "Graph and measurements",
            [
                (
                    'SHOW latency WHERE service = "django"',
                    "Node call counts and average durations",
                ),
                (
                    'SHOW nodes WHERE service = "nginx" LIMIT 20',
                    "Node inventory, including probe details",
                ),
                (
                    "SHOW edges",
                    "Relationships and edge measurements",
                ),
                (
                    'SHOW graph WHERE system = "worker"',
                    "Nodes and edges in a semantic scope",
                ),
                (
                    "HOTSPOT TOP 10",
                    "Highest call-count operations",
                ),
            ],
        ),
        (
            "Events and traces",
            [
                (
                    'SHOW events WHERE probe = "django.db.query" LIMIT 20',
                    "Recent matching events with timing and trace context",
                ),
                (
                    'SHOW path WHERE trace_id = "<trace-id>"',
                    "Ordered event path for one trace",
                ),
                (
                    "TRACE <trace-id>",
                    "Trace event timeline, gap and recorded durations",
                ),
                (
                    'SHOW llm_content WHERE trace_id = "<trace-id>"',
                    "Captured LLM prompt and response previews",
                ),
                (
                    'SHOW tool_content WHERE trace_id = "<trace-id>"',
                    "Captured tool arguments, results, and errors",
                ),
            ],
        ),
        (
            "Diagnosis and change history",
            [
                (
                    "CAUSAL",
                    "Evaluate all registered causal rules",
                ),
                (
                    'CAUSAL WHERE tags = "blocking,celery"',
                    "Evaluate rules matching the supplied tags",
                ),
                (
                    'BLAME WHERE system = "database"',
                    "Find upstream callers of a system or component",
                ),
                (
                    "DIFF SINCE deployment",
                    "Show graph changes since a label or timestamp",
                ),
                (
                    "SHOW changes",
                    "Show the latest graph changes",
                ),
            ],
        ),
        (
            "Engine inventory",
            [
                (
                    "SHOW status",
                    "Worker identity, uptime, graph, probes, and activity",
                ),
                (
                    "SHOW active",
                    "Requests currently tracked as in flight",
                ),
                ("SHOW probes", "Registered probe adapters"),
                ("SHOW rules", "Registered causal rules"),
                (
                    "SHOW semantic",
                    "Semantic aliases and descriptions",
                ),
            ],
        ),
    ]
    for title, queries in groups:
        print(c(f"\n  {title}", BOLD, CYAN))
        for query_text, description in queries:
            print(f"  {c(query_text, CYAN)}")
            print(c(f"    {description}", DIM))

    print(c("\n  Filters", BOLD, CYAN))
    print(
        c(
            "    SHOW filters: service, system, node, probe, trace_id",
            DIM,
        )
    )
    print(
        c(
            "    Use LIMIT N on row-based SHOW queries; combine filters with AND.",
            DIM,
        )
    )

    header("REPL Commands")
    commands = [
        (r"\status", "Show live engine and worker statistics"),
        (
            r"\active",
            "Show in-flight requests (same data as SHOW active)",
        ),
        (
            r"\probes / \rules / \semantic",
            "List registered instrumentation and aliases",
        ),
        (
            r"\stitch <trace-id>",
            "Query all local workers for a trace and combine their event rows",
        ),
        (r"\reconnect", "Choose a worker socket again"),
        (r"\help", "Show this command guide"),
        (r"\quit or Ctrl+C", "Exit the REPL"),
    ]
    for command, description in commands:
        print(f"  {c(command, YELLOW)}")
        print(c(f"    {description}", DIM))

    print(c("\n  Optional socket commands", BOLD, CYAN))
    print(
        c(
            r"    \emit <probe> <service> <name>   Inject an event if the server supports it.",
            DIM,
        )
    )
    print(
        c(
            r"    \snapshot [label]                Capture a snapshot if the server supports it.",
            DIM,
        )
    )

    print(c("\n  Notes", BOLD, CYAN))
    print(
        c(
            "    TRACE gap is elapsed time between events; recorded duration is separate.",
            DIM,
        )
    )
    print(
        c(
            "    STITCH does not calculate one combined wall duration across workers.",
            DIM,
        )
    )
    print(
        c(
            "    LLM/tool content appears only when capture was enabled before worker startup.",
            DIM,
        )
    )
    print()


def cmd_stitch(trace_id: str) -> None:
    r"""Query all live local worker sockets for one trace and show their events.

    Rows are ordered by timestamp. Recorded durations and inter-event gaps
    remain separate measurements; the command does not infer a combined
    cross-worker wall duration.

    Usage: ``\stitch <trace-id>``
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
            pass  # worker may have restarted - skip silently

    if not all_stages:
        dim(f"  Trace {trace_id[:16]}… not found in any worker.")
        dim(f"  Searched {len(sockets)} socket(s).")
        return

    # Sort by timestamp if present, otherwise keep original order
    all_stages.sort(key=lambda s: s.get("timestamp", 0))

    # Print unified timeline
    print()
    print(
        c("  Trace  ", BOLD)
        + c(trace_id[:24] + "…", CYAN)
        + c(
            f"  across {len(found_in)} process(es): pid {', '.join(found_in)}",
            DIM,
        )
    )
    print()

    stitch_rows = []
    total = 0.0
    for stage in all_stages:
        gap = stage.get("duration_ms")
        recorded = stage.get("duration_ns")
        probe = stage.get("probe", "?")
        timestamp = stage.get("timestamp")
        context = " · ".join(
            value
            for value in (
                (
                    f"span={stage['span_id']}"
                    if stage.get("span_id")
                    else ""
                ),
                (
                    f"parent={stage['parent_span_id']}"
                    if stage.get("parent_span_id")
                    else ""
                ),
                (
                    _format_value(stage.get("metadata"))
                    if stage.get("metadata")
                    else ""
                ),
            )
            if value
        )
        stitch_rows.append(
            {
                "time": (
                    time.strftime(
                        "%H:%M:%S", time.localtime(timestamp)
                    )
                    if timestamp
                    else "—"
                ),
                "worker": stage.get("_pid", "?"),
                "gap": (
                    f"{gap:.2f} ms" if gap is not None else "—"
                ),
                "event": f"{probe} · {stage.get('service', '?')}::{stage.get('name', '?')}",
                "recorded": (
                    f"{recorded / 1e6:.3f} ms"
                    if recorded is not None
                    else "—"
                ),
                "context": context or "—",
            }
        )
        total += gap or 0
    _render_table(
        stitch_rows,
        column_colors={
            "time": DIM,
            "worker": CYAN,
            "event": CYAN,
            "context": YELLOW,
        },
        latency_columns={"gap", "recorded"},
        prefer_table=True,
    )
    print(
        c(
            f"  Sum of reported inter-event gaps: {total:.2f} ms · {len(all_stages)} events · {len(found_in)} process(es)",
            BOLD,
            CYAN,
        )
    )


# -------------------- Main loop ------------------------------

HISTORY_FILE = os.path.expanduser("~/.origintracer_history")


def main():

    try:
        import readline

        readline.read_history_file(HISTORY_FILE)
        readline.set_history_length(200)
    except FileNotFoundError:
        pass  # history file doesn't exist yet, fine
    except ImportError:
        pass  # Windows, fine without it

    print(
        c("\n  OriginTracer REPL", BOLD, CYAN)
        + c("  live agent mode", DIM)
    )
    print(c("  Type \\help for commands, Ctrl+C to exit\n", DIM))

    sock_path = pick_socket()

    # Quick health check on connect
    status = query(sock_path, "SHOW STATUS")
    if status.get("ok"):
        _render_engine_status(
            status["data"]["data"], connected=True
        )

    while True:
        try:
            raw = input(
                readline_prompt("› ", BOLD, BLUE)
            ).strip()
        except (KeyboardInterrupt, EOFError):
            print()
            ok("Finished.")
            break

        if not raw:
            continue

        try:
            readline.write_history_file(HISTORY_FILE)
        except Exception:
            pass

        # REPL meta-commands
        if raw.startswith("\\"):
            parts = raw[1:].split(None, 1)
            cmd = parts[0].lower()
            args = parts[1] if len(parts) > 1 else ""

            if cmd in ("quit", "exit", "q"):
                ok("Finished.")
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
                        f"Reconnected  pid={d.get('pid', '?')}  nodes={d.get('graph_nodes', '?')}"
                    )
            elif cmd == "stitch":
                cmd_stitch(args.strip())
            else:
                err(f"Unknown command: \\{cmd}  (try \\help)")
            continue

        # DSL query - forwarded to the live engine
        t0 = time.perf_counter()
        result = query(sock_path, raw)
        elapsed = (time.perf_counter() - t0) * 1000
        render(result)
        if result.get("ok"):
            dim(f"  {elapsed:.1f}ms")


if __name__ == "__main__":
    main()

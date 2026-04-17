"""
A simple but graph-aware query DSL.

Grammar:
    QUERY := VERB METRIC [WHERE FILTERS] [LIMIT N] [AS system LABEL]
    VERB := SHOW | TRACE | BLAME | HOTSPOT | DIFF | CAUSAL
    METRIC := latency | events | path | graph | nodes | edges |
            status | active | probes | rules | semantic |
    FILTERS := FILTER [AND FILTER]*
    FILTER := FIELD OP VALUE
    OP := = | > | < | >= | <= | LIKE # currently supports only: =
    FIELD := service | probe | system | trace_id | node | name | tags
    VALUE := quoted string | number

Examples:
    SHOW latency WHERE service = "django"
    SHOW latency WHERE system = "database"
    SHOW graph
    SHOW graph WHERE system = "worker"
    SHOW nodes WHERE service = "gunicorn"
    SHOW edges
    SHOW events WHERE probe = "django.db.query" LIMIT 20
    SHOW status
    SHOW active
    SHOW probes
    SHOW rules
    SHOW semantic
    TRACE abc123def456
    SHOW path WHERE trace_id = "abc123"
    BLAME WHERE system = "db"
    HOTSPOT TOP 10
    DIFF SINCE deployment
    CAUSAL
    CAUSAL WHERE tags = "blocking,n+1"

The executor traverses the RuntimeGraph and TemporalStore, not raw DB rows,
so queries reflect the live, structured model — not flat event logs.
For historical queries, the executor falls back to the repository.
"""

from __future__ import annotations

import os
import re
import shlex
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ParsedQuery:
    verb: str  # SHOW | TRACE | BLAME | HOTSPOT | DIFF | CAUSAL
    metric: str  # latency | events | path | graph | changes | …
    filters: Dict[str, Any] = field(default_factory=dict)
    limit: int = 100
    raw: str = ""


def parse(query_str: str) -> ParsedQuery:
    """
    Parse a DSL query string into a ParsedQuery.
    Raises ValueError on syntax errors.
    """
    original = query_str.strip()
    # TODO: fix when extending supported operators
    normalized = original.replace("=", " = ")
    tokens = shlex.split(
        normalized
    )  # shlex handles quoted strings correctly

    if not tokens:
        raise ValueError("Empty query")

    verb = tokens[0].upper()

    # TRACE <trace_id>
    if verb == "TRACE":
        if len(tokens) < 2:
            raise ValueError(
                "TRACE requires a trace_id argument"
            )
        return ParsedQuery(
            verb="TRACE",
            metric="path",
            filters={"trace_id": tokens[1]},
            raw=original,
        )

    # BLAME WHERE system = "..."
    if verb == "BLAME":
        filters = _parse_filters(tokens[1:])
        return ParsedQuery(
            verb="BLAME",
            metric="upstream",
            filters=filters,
            raw=original,
        )

    # HOTSPOT [TOP N]
    if verb == "HOTSPOT":
        limit = 10
        if len(tokens) >= 3 and tokens[1].upper() == "TOP":
            try:
                limit = int(tokens[2])
            except ValueError:
                pass
        return ParsedQuery(
            verb="HOTSPOT",
            metric="nodes",
            limit=limit,
            raw=original,
        )

    # DIFF SINCE <label|timestamp>
    if verb == "DIFF":
        if len(tokens) >= 3 and tokens[1].upper() == "SINCE":
            return ParsedQuery(
                verb="DIFF",
                metric="edges",
                filters={"since": tokens[2]},
                raw=original,
            )
        return ParsedQuery(
            verb="DIFF", metric="edges", raw=original
        )

    # CAUSAL [WHERE tags = "..."]
    if verb == "CAUSAL":
        filters = _parse_filters(tokens[1:])
        return ParsedQuery(
            verb="CAUSAL",
            metric="matches",
            filters=filters,
            raw=original,
        )

    # SHOW <metric> [WHERE ...] [LIMIT N]
    if verb == "SHOW":
        if len(tokens) < 2:
            raise ValueError(
                "SHOW requires a metric (latency | events | path | graph | changes)"
            )

        metric = tokens[1].lower()
        remaining = tokens[2:]
        filters, limit = _parse_where_limit(remaining)
        return ParsedQuery(
            verb="SHOW",
            metric=metric,
            filters=filters,
            limit=limit,
            raw=original,
        )

    raise ValueError(
        f"Unknown verb '{verb}'. Expected: SHOW | TRACE | BLAME | HOTSPOT | DIFF | CAUSAL"
    )


def _parse_where_limit(
    tokens: List[str],
) -> Tuple[Dict[str, Any], int]:
    filters: Dict[str, Any] = {}
    limit = 100
    i = 0

    while i < len(tokens):
        tok = tokens[i].upper()

        if tok == "WHERE":
            i += 1
            while (
                i < len(tokens) and tokens[i].upper() != "LIMIT"
            ):
                if tokens[i].upper() == "AND":
                    i += 1
                    continue

                if i + 2 < len(tokens):
                    key = tokens[i]
                    op = tokens[i + 1]
                    val = tokens[i + 2]

                    if op != "=":
                        raise ValueError(
                            f"Unsupported operator: {op}"
                        )

                    # Type coercion
                    try:
                        if val.isdigit():
                            val = int(val)
                        else:
                            val = float(val)
                    except ValueError:
                        val = val.strip("'\"")

                    filters[key] = val
                    i += 3
                else:
                    raise ValueError(
                        f"Incomplete WHERE clause near '{tokens[i]}'"
                    )

        elif tok == "LIMIT":
            i += 1
            if i < len(tokens):
                try:
                    limit = int(tokens[i])
                    i += 1
                except ValueError:
                    raise ValueError(
                        f"LIMIT must be an integer, got: {tokens[i]}"
                    )
            else:
                raise ValueError(
                    "LIMIT keyword found but no value provided"
                )

        else:
            i += 1

    return filters, limit


def _parse_filters(tokens: List[str]) -> Dict[str, Any]:
    filters, _ = _parse_where_limit(tokens)
    return filters


def execute(query: ParsedQuery, engine: Any) -> Dict[str, Any]:
    dispatch = {
        "SHOW": _exec_show,
        "TRACE": _exec_trace,
        "BLAME": _exec_blame,
        "HOTSPOT": _exec_hotspot,
        "DIFF": _exec_diff,
        "CAUSAL": _exec_causal,
    }
    handler = dispatch.get(query.verb)
    if not handler:
        return {"error": f"No executor for verb '{query.verb}'"}
    try:
        return handler(query, engine)
    except Exception as exc:
        return {"error": str(exc), "query": query.raw}


# TODO: must be made scalable for more filters support
SEMANTIC_FILTER_KEYS = {
    "system",
    "service",
    "node",
    "probe",
}
# These are already meaningful — skip semantic resolution
DIRECT_FILTER_KEYS = {"tags", "trace_id"}


# SHOW - dispatch by metric
def _exec_show(
    query: ParsedQuery, engine: Any
) -> Dict[str, Any]:

    metric = query.metric
    filters = query.filters
    unknown = set(filters.keys()) - SEMANTIC_FILTER_KEYS
    if unknown:
        return {
            "error": f"Unknown filter key(s): {', '.join(sorted(unknown))}",
            "hint": f"Valid filters are: {', '.join(sorted(KNOWN_FILTER_KEYS))}",
        }
    # Handles system=, service=, node=, etc all through the same semantic layer
    node_scope = None
    # TODO: must be made scalable for more filters support
    semantic_candidate = (
        filters.get("system")
        or filters.get("service")
        or filters.get("node")
        or filters.get("probe")
    )

    if semantic_candidate:
        resolved = engine.semantic.resolve_nodes(
            semantic_candidate, engine.graph
        )
        if resolved:
            node_scope = resolved
        else:
            # If a candidate was provided but couldn't be resolved, it's an error
            return {
                "error": f"Could not find any nodes matching '{semantic_candidate}'",
                "hint": "Check your spelling or use 'show graph' without a"
                "WHERE clause to see everything.",
                "available_labels": engine.semantic.all_labels(),
            }

    handlers = {
        "latency": lambda: _show_latency(
            engine, filters, node_scope, query.limit
        ),
        "events": lambda: _show_events(
            engine, filters, query.limit
        ),
        "path": lambda: _show_path(engine, filters),
        "graph": lambda: _show_graph(engine, node_scope),
        "nodes": lambda: _show_nodes(
            engine, filters, node_scope, query.limit
        ),
        "edges": lambda: _show_edges(engine, node_scope),
        "changes": lambda: _exec_diff(query, engine),
        "status": lambda: _show_status(engine),
        "active": lambda: _show_active(engine),
        "probes": lambda: _show_probes(engine),
        "rules": lambda: _show_rules(engine),
        "semantic": lambda: _show_semantic(engine),
    }

    handler = handlers.get(metric)
    if not handler:
        return {"error": f"Unknown metric '{metric}'"}
    return handler()


def _show_latency(
    engine: Any,
    filters: Dict,
    node_scope: Optional[set],
    limit: int,
) -> Dict:
    nodes = list(engine.graph.all_nodes())

    if node_scope:
        nodes = [n for n in nodes if n.id in node_scope]

    if service := filters.get("service"):
        nodes = [n for n in nodes if n.service == service]

    nodes.sort(
        key=lambda n: n.avg_duration_ns or 0, reverse=True
    )
    nodes = nodes[:limit]

    return {
        "metric": "latency",
        "filters": filters,
        "data": [
            {
                "node": n.id,
                "service": n.service,
                "type": n.node_type,
                "call_count": n.call_count,
                "avg_duration_ms": (
                    round(n.avg_duration_ns / 1e6, 3)
                    if n.avg_duration_ns
                    else None
                ),
                "total_duration_ms": round(
                    n.total_duration_ns / 1e6, 3
                ),
            }
            for n in nodes
        ],
    }


def _show_events(engine: Any, filters: Dict, limit: int) -> Dict:
    """
    Query historical events. Falls back to in-memory event log if
    no repository is attached.
    """
    events = getattr(engine, "_event_log", [])

    if trace_id := filters.get("trace_id"):
        events = [e for e in events if e.trace_id == trace_id]
    if probe := filters.get("probe"):
        events = [e for e in events if e.probe == probe]
    if service := filters.get("service"):
        events = [e for e in events if e.service == service]

    events = events[-limit:]  # most recent N

    return {
        "metric": "events",
        "filters": filters,
        "data": [
            {
                "probe": e.probe,
                "service": e.service,
                "name": e.name,
                "trace_id": e.trace_id,
                "ts": getattr(e, "timestamp", None),
            }
            for e in events
        ],
    }


def _show_path(engine: Any, filters: Dict) -> Dict:
    trace_id = filters.get("trace_id")
    if not trace_id:
        return {
            "error": "SHOW path requires WHERE trace_id = <id>"
        }
    return {
        "metric": "critical_path",
        "trace_id": trace_id,
        "data": engine.critical_path(trace_id),
    }


def _show_graph(engine: Any, node_scope: Optional[set]) -> Dict:
    nodes = list(engine.graph.all_nodes())
    edges = list(engine.graph.all_edges())

    if node_scope:
        # Only show what the user specifically asked for
        nodes = [n for n in nodes if n.id in node_scope]

        # Only show connections between nodes inside this system
        edges = [
            e
            for e in edges
            if e.source in node_scope and e.target in node_scope
        ]

    return {
        "metric": "graph",
        "data": {
            "nodes": [_node_dict(n) for n in nodes],
            "edges": [_edge_dict(e) for e in edges],
        },
    }


def _show_nodes(
    engine: Any,
    filters: Dict,
    node_scope: Optional[set],
    limit: int,
) -> Dict:
    """
    Full node listing - same fields as the old built-in SHOW NODES.
    Supports service filter and node_scope from semantic resolution.
    """
    nodes = list(engine.graph.all_nodes())
    if node_scope:
        nodes = [n for n in nodes if n.id in node_scope]

    if service := filters.get("service"):
        nodes = [n for n in nodes if n.service == service]

    nodes = nodes[:limit]

    return {
        "metric": "nodes",
        "data": [_node_dict(n) for n in nodes],
    }


def _show_edges(engine: Any, node_scope: Optional[set]) -> Dict:
    """
    Full edge listing - same fields as the old built-in SHOW EDGES.
    """
    edges = list(engine.graph.all_edges())

    if node_scope:
        edges = [
            e
            for e in edges
            if e.source in node_scope or e.target in node_scope
        ]

    return {
        "metric": "edges",
        "data": [_edge_dict(e) for e in edges],
    }


def _show_status(engine: Any) -> Dict:
    """
    Engine health snapshot.
    """
    graph = engine.graph
    tracker = getattr(engine, "tracker", None)
    started = getattr(engine, "_started_at", None)
    event_log = getattr(engine, "_event_log", [])

    # Uptime
    uptime_s = (
        round(time.monotonic() - started, 1) if started else None
    )
    uptime_human = (
        _format_uptime(uptime_s) if uptime_s else "unknown"
    )

    # Graph
    nodes = list(graph.all_nodes())
    edges = list(graph.all_edges())

    # Probe health
    probes = getattr(engine, "probes", [])
    active_probes = [str(probe.name) for probe in probes]
    active_probe_count = len(active_probes)

    # Semantic layer
    semantic = getattr(engine, "semantic", None)
    label_count = len(semantic.all_labels()) if semantic else 0

    return {
        "verb": "STATUS",
        "data": {
            # Process
            "pid": os.getpid(),
            "socket": f"/tmp/origintracer-{os.getpid()}.sock",
            "uptime_s": uptime_s,
            "uptime": uptime_human,
            # Graph
            "graph_nodes": len(nodes),
            "graph_edges": len(edges),
            "semantic_labels": label_count,
            # Probes
            "probes_total": active_probe_count,
            "probes_active": active_probes,
            # Activity
            "active_requests": (
                tracker.active_count() if tracker else 0
            ),
            "event_log_size": len(event_log),
        },
    }


def _format_uptime(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"


def _show_active(engine: Any) -> Dict:
    tracker = getattr(engine, "tracker", None)
    if tracker is None:
        return {"metric": "active", "data": []}

    active = [
        {"trace_id": t, "started_at": v}
        for t, v in tracker._active.items()
    ]
    return {"metric": "active", "data": active}


def _show_probes(engine: Any) -> Dict:
    """
    List registered probe adapters by name.
    """
    probes = []
    probe_mgr = getattr(engine, "probes", None)
    if probe_mgr:
        for p in probe_mgr:
            probes.append(getattr(p, "name", str(p)))

    return {"metric": "probes", "data": probes}


def _show_rules(engine: Any) -> Dict:
    """List registered causal rule names."""
    registry = getattr(engine, "causal", None)

    if registry:
        rules = list(registry._rules.keys())
    else:
        rules = []

    return {"metric": "rules", "data": rules}


def _show_semantic(engine: Any) -> Dict:
    """
    List all semantic labels with descriptions.
    """
    semantic = getattr(engine, "semantic", None)
    if semantic is None:
        return {"metric": "semantic", "data": []}

    labels = []
    for label in semantic.all_labels():
        desc = ""
        # SemanticLayer may store descriptions - try common attribute patterns
        defn = getattr(semantic, "_definitions", {}).get(
            label
        ) or getattr(semantic, "_labels", {}).get(label)
        if defn:
            desc = getattr(defn, "description", "") or (
                defn.get("description", "")
                if isinstance(defn, dict)
                else ""
            )
        labels.append({"label": label, "description": desc})

    return {"metric": "semantic", "data": labels}


#
def _exec_trace(query: ParsedQuery, engine: Any) -> Dict:
    trace_id = query.filters.get("trace_id")
    if not trace_id:
        return {"error": "TRACE requires a trace_id"}
    path = engine.critical_path(trace_id)
    return {
        "verb": "TRACE",
        "trace_id": trace_id,
        "stages": len(path),
        "data": path,
    }


def _exec_blame(query: ParsedQuery, engine: Any) -> Dict:
    """
    For a given system label, find upstream callers.
    """
    label = query.filters.get("system")
    if not label:
        return {
            "error": "BLAME requires WHERE system = '<label>'"
        }

    node_ids = engine.semantic.resolve_nodes(label, engine.graph)
    if not node_ids:
        return {"error": f"No nodes found for system '{label}'"}

    callers: Dict[str, int] = {}
    for nid in node_ids:
        for edge in engine.graph.callers(nid):
            callers[edge.source] = (
                callers.get(edge.source, 0) + edge.call_count
            )

    sorted_callers = sorted(
        callers.items(), key=lambda x: x[1], reverse=True
    )
    return {
        "verb": "BLAME",
        "system": label,
        "resolved_nodes": list(node_ids),
        "data": [
            {"caller": c, "call_count": n}
            for c, n in sorted_callers[:20]
        ],
    }


def _exec_hotspot(query: ParsedQuery, engine: Any) -> Dict:
    """
    Return the N busiest nodes by call_count.
    Falls back to a direct graph sort if engine.hotspots() doesn't exist.
    """
    top_n = query.limit

    # Prefer the engine method if it exists
    if hasattr(engine, "hotspots"):
        return {
            "verb": "HOTSPOT",
            "data": engine.hotspots(top_n=top_n),
        }

    # Fallback - sort all_nodes() by call_count directly
    nodes = sorted(
        engine.graph.all_nodes(),
        key=lambda n: n.call_count,
        reverse=True,
    )[:top_n]

    return {
        "verb": "HOTSPOT",
        "data": [
            {
                "node": n.id,
                "service": n.service,
                "call_count": n.call_count,
                "avg_duration_ms": (
                    round(n.avg_duration_ns / 1e6, 3)
                    if n.avg_duration_ns
                    else None
                ),
                "total_duration_ms": round(
                    n.total_duration_ns / 1e6, 3
                ),
            }
            for n in nodes
        ],
    }


def _exec_diff(query: ParsedQuery, engine: Any) -> Dict:
    since_label = query.filters.get("since")
    since_ts: Optional[float] = None

    if since_label:
        diff = engine.temporal.label_diff(since_label)
        if diff:
            since_ts = diff.timestamp
        else:
            try:
                since_ts = float(since_label)
            except ValueError:
                return {
                    "error": f"Cannot resolve SINCE '{since_label}' — no marker found"
                }
    else:
        since_ts = time.time() - 60  # default: last 60 seconds

    # temporal methods may not all exist yet - degrade gracefully
    new_edges = (
        list(engine.temporal.new_edges_since(since_ts))
        if hasattr(engine.temporal, "new_edges_since")
        else []
    )
    removed_edges = (
        list(engine.temporal.removed_edges_since(since_ts))
        if hasattr(engine.temporal, "removed_edges_since")
        else []
    )
    changes = (
        engine.temporal.changes_since(since_ts)
        if hasattr(engine.temporal, "changes_since")
        else []
    )

    return {
        "verb": "DIFF",
        "since": since_ts,
        "since_label": since_label,
        "new_edges": new_edges,
        "removed_edges": removed_edges,
        "diff_count": len(changes),
    }


def _exec_causal(query: ParsedQuery, engine: Any) -> Dict:
    tags = None
    if "tags" in query.filters:
        tags = [
            t.strip()
            for t in str(query.filters["tags"]).split(",")
        ]

    matches = engine.evaluate(tags=tags)
    return {
        "verb": "CAUSAL",
        "match_count": len(matches),
        "data": [m.to_dict() for m in matches],
    }


def _node_dict(n) -> Dict:
    """
    Full node representation - used by SHOW NODES and SHOW GRAPH.
    """
    return {
        "id": n.id,
        "service": n.service,
        "type": n.node_type,
        "call_count": n.call_count,
        "duration_ns": n.total_duration_ns,
        "avg_ms": (
            round(n.avg_duration_ns / 1e6, 3)
            if n.avg_duration_ns
            else None
        ),
        "first_seen": n.first_seen,
        "last_seen": n.last_seen,
    }


def _edge_dict(e) -> Dict:
    """
    Full edge representation - used by SHOW EDGES and SHOW GRAPH.
    """
    return {
        "source": e.source,
        "target": e.target,
        "type": e.edge_type,
        "call_count": e.call_count,
        "weight": e.call_count,
    }

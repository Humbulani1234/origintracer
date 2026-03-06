"""
query/parser.py + query/executor.py

A simple but graph-aware query DSL.

Grammar:
    QUERY := VERB METRIC [WHERE FILTERS] [LIMIT N] [AS system LABEL]

    VERB    := SHOW | TRACE | BLAME | HOTSPOT | DIFF | CAUSAL
    METRIC  := latency | events | path | graph | changes
    FILTERS := FILTER [AND FILTER]*
    FILTER  := FIELD OP VALUE
    OP      := = | > | < | >=| <=| LIKE
    FIELD   := service | probe | system | trace_id | node | name
    VALUE   := quoted string | number

Examples:
    SHOW latency WHERE service = "django"
    SHOW events WHERE probe = "asyncio.task.block" LIMIT 50
    TRACE abc123def456
    SHOW path WHERE trace_id = "abc123"
    BLAME WHERE system = "export"
    HOTSPOT TOP 10
    DIFF SINCE deployment
    CAUSAL

The executor traverses the RuntimeGraph and TemporalStore, not raw DB rows,
so queries reflect the live, structured model — not flat event logs.
For historical queries, the executor falls back to the repository.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ====================================================================== #
# Parser
# ====================================================================== #

@dataclass
class ParsedQuery:
    verb: str                           # SHOW | TRACE | BLAME | HOTSPOT | DIFF | CAUSAL
    metric: str                         # latency | events | path | graph | changes | …
    filters: Dict[str, Any] = field(default_factory=dict)
    limit: int = 100
    raw: str = ""


def parse(query_str: str) -> ParsedQuery:
    """
    Parse a DSL query string into a ParsedQuery.
    Raises ValueError on syntax errors.
    """
    original = query_str.strip()
    tokens = shlex.split(original)   # shlex handles quoted strings correctly

    if not tokens:
        raise ValueError("Empty query")

    verb = tokens[0].upper()

    # --- TRACE <trace_id> ---
    if verb == "TRACE":
        if len(tokens) < 2:
            raise ValueError("TRACE requires a trace_id argument")
        return ParsedQuery(verb="TRACE", metric="path", filters={"trace_id": tokens[1]}, raw=original)

    # --- BLAME WHERE system = "..." ---
    if verb == "BLAME":
        filters = _parse_filters(tokens[1:])
        return ParsedQuery(verb="BLAME", metric="upstream", filters=filters, raw=original)

    # --- HOTSPOT [TOP N] ---
    if verb == "HOTSPOT":
        limit = 10
        if len(tokens) >= 3 and tokens[1].upper() == "TOP":
            try:
                limit = int(tokens[2])
            except ValueError:
                pass
        return ParsedQuery(verb="HOTSPOT", metric="nodes", limit=limit, raw=original)

    # --- DIFF SINCE <label|timestamp> ---
    if verb == "DIFF":
        if len(tokens) >= 3 and tokens[1].upper() == "SINCE":
            return ParsedQuery(verb="DIFF", metric="edges", filters={"since": tokens[2]}, raw=original)
        return ParsedQuery(verb="DIFF", metric="edges", raw=original)

    # --- CAUSAL [WHERE tags = "..."] ---
    if verb == "CAUSAL":
        filters = _parse_filters(tokens[1:])
        return ParsedQuery(verb="CAUSAL", metric="matches", filters=filters, raw=original)

    # --- SHOW <metric> [WHERE ...] [LIMIT N] ---
    if verb == "SHOW":
        if len(tokens) < 2:
            raise ValueError("SHOW requires a metric (latency | events | path | graph | changes)")

        metric = tokens[1].lower()
        remaining = tokens[2:]
        filters, limit = _parse_where_limit(remaining)
        return ParsedQuery(verb="SHOW", metric=metric, filters=filters, limit=limit, raw=original)

    raise ValueError(f"Unknown verb '{verb}'. Expected: SHOW | TRACE | BLAME | HOTSPOT | DIFF | CAUSAL")


def _parse_where_limit(tokens: List[str]) -> Tuple[Dict[str, Any], int]:
    filters: Dict[str, Any] = {}
    limit = 100
    i = 0
    while i < len(tokens):
        tok = tokens[i].upper()
        if tok == "WHERE":
            i += 1
            while i < len(tokens) and tokens[i].upper() not in ("LIMIT", "AND"):
                if i + 2 < len(tokens):
                    key = tokens[i]
                    op = tokens[i + 1]
                    val = tokens[i + 2]
                    # Type coerce numbers
                    try:
                        val = int(val)
                    except ValueError:
                        try:
                            val = float(val)
                        except ValueError:
                            pass
                    filters[key] = val
                    i += 3
                else:
                    i += 1
        elif tok == "LIMIT":
            i += 1
            if i < len(tokens):
                try:
                    limit = int(tokens[i])
                except ValueError:
                    pass
                i += 1
        elif tok == "AND":
            i += 1
        else:
            i += 1
    return filters, limit


def _parse_filters(tokens: List[str]) -> Dict[str, Any]:
    filters, _ = _parse_where_limit(tokens)
    return filters


# ====================================================================== #
# Executor
# ====================================================================== #

def execute(query: ParsedQuery, engine: Any) -> Dict[str, Any]:
    """
    Execute a ParsedQuery against the Engine's live model.
    Returns a result dict with 'data' and 'meta'.
    """
    dispatch = {
        "SHOW":    _exec_show,
        "TRACE":   _exec_trace,
        "BLAME":   _exec_blame,
        "HOTSPOT": _exec_hotspot,
        "DIFF":    _exec_diff,
        "CAUSAL":  _exec_causal,
    }
    handler = dispatch.get(query.verb)
    if not handler:
        return {"error": f"No executor for verb '{query.verb}'"}

    try:
        return handler(query, engine)
    except Exception as exc:
        return {"error": str(exc), "query": query.raw}


# ------------------------------------------------------------------ #
# SHOW handlers
# ------------------------------------------------------------------ #

def _exec_show(query: ParsedQuery, engine: Any) -> Dict[str, Any]:
    metric = query.metric
    filters = query.filters

    # Resolve semantic alias if 'system' filter is present
    node_scope = None
    if "system" in filters:
        label = filters["system"]
        node_scope = engine.semantic.resolve_nodes(label, engine.graph)
        if not node_scope:
            return {"error": f"Unknown semantic label '{label}'", "available": engine.semantic.all_labels()}

    if metric == "latency":
        return _show_latency(engine, filters, node_scope, query.limit)
    if metric == "events":
        return _show_events(engine, filters, query.limit)
    if metric == "path":
        return _show_path(engine, filters)
    if metric == "graph":
        return _show_graph(engine, node_scope)
    if metric == "changes":
        return _exec_diff(query, engine)

    return {"error": f"Unknown metric '{metric}'"}


def _show_latency(engine: Any, filters: Dict, node_scope: Optional[set], limit: int) -> Dict:
    nodes = list(engine.graph.all_nodes())

    if node_scope:
        nodes = [n for n in nodes if n.id in node_scope]

    service_filter = filters.get("service")
    if service_filter:
        nodes = [n for n in nodes if n.service == service_filter]

    # Sort by avg duration descending
    nodes.sort(key=lambda n: n.avg_duration_ns or 0, reverse=True)
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
                "avg_duration_ms": round(n.avg_duration_ns / 1e6, 3) if n.avg_duration_ns else None,
                "total_duration_ms": round(n.total_duration_ns / 1e6, 3),
            }
            for n in nodes
        ],
    }


def _show_events(engine: Any, filters: Dict, limit: int) -> Dict:
    """Query the repository (historical events) using filters."""
    if engine.repository is None:
        return {"error": "No repository attached to engine"}

    return {
        "metric": "events",
        "filters": filters,
        "data": engine.repository.query_events(
            trace_id=filters.get("trace_id"),
            probe=filters.get("probe"),
            service=filters.get("service"),
            limit=limit,
        ),
    }


def _show_path(engine: Any, filters: Dict) -> Dict:
    trace_id = filters.get("trace_id")
    if not trace_id:
        return {"error": "SHOW path requires WHERE trace_id = <id>"}
    return {
        "metric": "critical_path",
        "trace_id": trace_id,
        "data": engine.critical_path(trace_id),
    }


def _show_graph(engine: Any, node_scope: Optional[set]) -> Dict:
    nodes = list(engine.graph.all_nodes())
    edges = list(engine.graph.all_edges())

    if node_scope:
        # Include the scoped nodes and their immediate neighbors
        extended = set(node_scope)
        for n in list(node_scope):
            for e in engine.graph.neighbors(n):
                extended.add(e.target)
            for e in engine.graph.callers(n):
                extended.add(e.source)
        nodes = [n for n in nodes if n.id in extended]
        edges = [e for e in edges if e.source in extended or e.target in extended]

    return {
        "metric": "graph",
        "data": {
            "nodes": [
                {"id": n.id, "service": n.service, "type": n.node_type,
                 "call_count": n.call_count}
                for n in nodes
            ],
            "edges": [
                {"source": e.source, "target": e.target, "type": e.edge_type,
                 "call_count": e.call_count}
                for e in edges
            ],
        },
    }


# ------------------------------------------------------------------ #
# Other verb handlers
# ------------------------------------------------------------------ #

def _exec_trace(query: ParsedQuery, engine: Any) -> Dict:
    trace_id = query.filters.get("trace_id")
    path = engine.critical_path(trace_id)
    return {
        "verb": "TRACE",
        "trace_id": trace_id,
        "stages": len(path),
        "data": path,
    }


def _exec_blame(query: ParsedQuery, engine: Any) -> Dict:
    """
    For a given system label, find the upstream callers — i.e. what calls into it.
    Useful for: "who is hammering the export system?"
    """
    label = query.filters.get("system")
    if not label:
        return {"error": "BLAME requires WHERE system = '<label>'"}

    node_ids = engine.semantic.resolve_nodes(label, engine.graph)
    if not node_ids:
        return {"error": f"No nodes found for system '{label}'"}

    callers = {}
    for nid in node_ids:
        for edge in engine.graph.callers(nid):
            src = edge.source
            callers[src] = callers.get(src, 0) + edge.call_count

    sorted_callers = sorted(callers.items(), key=lambda x: x[1], reverse=True)
    return {
        "verb": "BLAME",
        "system": label,
        "resolved_nodes": list(node_ids),
        "data": [{"caller": c, "call_count": n} for c, n in sorted_callers[:20]],
    }


def _exec_hotspot(query: ParsedQuery, engine: Any) -> Dict:
    return {
        "verb": "HOTSPOT",
        "data": engine.hotspots(top_n=query.limit),
    }


def _exec_diff(query: ParsedQuery, engine: Any) -> Dict:
    since_label = query.filters.get("since")
    since_ts: Optional[float] = None

    if since_label:
        # Try as a named event first
        diff = engine.temporal.label_diff(since_label)
        if diff:
            since_ts = diff.timestamp
        else:
            # Try as a raw float timestamp
            try:
                since_ts = float(since_label)
            except ValueError:
                return {"error": f"Cannot resolve SINCE '{since_label}' — no marker found"}
    else:
        # Default: last 60 seconds
        import time
        since_ts = time.time() - 60

    new_edges = engine.temporal.new_edges_since(since_ts)
    removed_edges = engine.temporal.removed_edges_since(since_ts)
    changes = engine.temporal.changes_since(since_ts)

    return {
        "verb": "DIFF",
        "since": since_ts,
        "since_label": since_label,
        "new_edges": list(new_edges),
        "removed_edges": list(removed_edges),
        "diff_count": len(changes),
    }


def _exec_causal(query: ParsedQuery, engine: Any) -> Dict:
    tags = None
    if "tags" in query.filters:
        tags = [t.strip() for t in str(query.filters["tags"]).split(",")]

    matches = engine.evaluate(tags=tags)
    return {
        "verb": "CAUSAL",
        "match_count": len(matches),
        "data": [m.to_dict() for m in matches],
    }
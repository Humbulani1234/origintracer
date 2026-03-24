"""
backend/main.py

FastAPI service — persistence, query, and UI surface for StackTracer.

Surfaces:
    POST /api/v1/graph/snapshot  — receive serialised graph from agent
    POST /api/v1/events          — receive raw events for persistence
    POST /api/v1/ingest          — alias for /events (backward compat)
    GET  /api/v1/graph           — current graph (from latest snapshot)
    GET  /api/v1/traces/{id}     — critical path from event store
    GET  /api/v1/causal          — causal rules on latest snapshot
    GET  /api/v1/hotspots        — top N busiest nodes
    GET  /api/v1/diff            — graph diff since marker
    POST /api/v1/deployment      — store deployment marker
    GET  /api/v1/status          — snapshot metadata + system info
    POST /api/v1/query           — raw DSL query on latest snapshot
    GET  /health                 — liveness probe

Architecture:
    The agent owns the authoritative graph and builds it locally.
    FastAPI receives serialised graph snapshots (msgpack, ~1MB) every 60s,
    deserialises them, and serves all queries from the deserialised graph.
    FastAPI never rebuilds a graph from raw events — that is the agent's job.

    On startup, the latest snapshot is loaded from the storage backend
    so queries work immediately after a FastAPI restart without waiting
    60s for the next agent snapshot.

    Raw events are stored in PostgreSQL or ClickHouse for historical
    trace queries (GET /api/v1/traces/{id}).

Run with:
    uvicorn stacktracer.backend.main:app --host 0.0.0.0 --port 8000

Environment variables:
    STACKTRACER_API_KEYS  — comma-separated key:customer pairs
                            e.g. "sk_prod_xxx:acme,sk_dev_yyy:dev_customer"
    STACKTRACER_DB_DSN    — PostgreSQL DSN for event + snapshot storage
    STACKTRACER_CH_HOST   — ClickHouse host (alternative to Postgres)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("stacktracer.backend")


class _BackendEngine:
    """
    Minimal engine shim for the backend.
    The real Engine lives in the agent process.
    The backend only has a deserialised RuntimeGraph and no live components.
    """

    def __init__(self, graph):
        from stacktracer.core.semantic import SemanticLayer
        from stacktracer.core.temporal import TemporalStore
        from stacktracer.core.causal import (
            build_default_registry,
        )

        self.graph = graph
        self.semantic = SemanticLayer()
        self.temporal = TemporalStore()
        self.causal = build_default_registry(tracker=None)
        self.repository = None


app = FastAPI(
    title="StackTracer API",
    description="Runtime observability backend for Python async services",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*"
    ],  # tighten in production with explicit origins
    allow_methods=["*"],
    allow_headers=["*"],
)


# ====================================================================== #
# State
# ====================================================================== #

# One deserialised RuntimeGraph per customer.
# This is the cache in front of st_snapshots in the database.
# Populated on startup (from DB) and updated on every POST /graph/snapshot.
_graphs: Dict[str, Any] = {}
_graphs_lock = threading.Lock()

# Storage repository — set in _init_repository() at startup.
# Implements insert_event(), insert_snapshot(), get_latest_snapshot(),
# query_events(), insert_marker().
_repository: Optional[Any] = None

# API key → customer_id mapping.
_valid_api_keys: Dict[str, str] = {}


# ====================================================================== #
# Startup
# ====================================================================== #


def _load_api_keys() -> None:
    global _valid_api_keys
    raw = os.getenv("STACKTRACER_API_KEYS", "")
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            key, customer = pair.split(":", 1)
            _valid_api_keys[key.strip()] = customer.strip()
    if not _valid_api_keys:
        _valid_api_keys["dev-key-00000000"] = "dev_customer"
        logger.warning(
            "STACKTRACER_API_KEYS not set — "
            "dev-key-00000000 accepted (not safe for production)"
        )


def _init_repository() -> None:
    """
    Connect to the configured storage backend.
    Tries PostgreSQL first (STACKTRACER_DB_DSN), then ClickHouse
    (STACKTRACER_CH_HOST), then falls back to InMemoryRepository for dev.
    """
    global _repository

    db_dsn = os.getenv("STACKTRACER_DB_DSN")
    ch_host = os.getenv("STACKTRACER_CH_HOST")

    if db_dsn:
        try:
            import psycopg2
            from stacktracer.storage.base import EventRepository

            conn = psycopg2.connect(db_dsn)
            _repository = EventRepository(conn)
            logger.info(
                "Storage: PostgreSQL (%s)", db_dsn.split("@")[-1]
            )
            return
        except Exception as exc:
            logger.warning(
                "PostgreSQL connect failed: %s — falling back",
                exc,
            )

    if ch_host:
        try:
            from stacktracer.storage.base import (
                ClickHouseRepository,
            )

            _repository = ClickHouseRepository(host=ch_host)
            logger.info("Storage: ClickHouse (%s)", ch_host)
            return
        except Exception as exc:
            logger.warning(
                "ClickHouse connect failed: %s — falling back",
                exc,
            )

    from stacktracer.storage.base import InMemoryRepository

    _repository = InMemoryRepository()
    logger.info(
        "Storage: InMemory (dev mode — data lost on restart)"
    )


def _load_snapshots_on_startup() -> None:
    """
    On FastAPI restart, reload the latest graph snapshot from storage
    for each known customer. Without this the first 60 seconds after
    restart return 404 from require_graph() for every query.
    """
    if _repository is None:
        return
    if not hasattr(_repository, "get_latest_snapshot"):
        return

    # Known customers come from the API key map.
    # For multi-tenant production: iterate a customers table instead.
    customer_ids = set(_valid_api_keys.values())
    for customer_id in customer_ids:
        try:
            row = _repository.get_latest_snapshot(customer_id)
            if row is None:
                continue
            from stacktracer.core.graph_serializer import (
                MsgpackSerializer,
                ProtobufSerializer,
            )

            serializer = (
                ProtobufSerializer()
                if row["content_type"]
                == "application/x-protobuf"
                else MsgpackSerializer()
            )
            graph = serializer.deserialize(row["data"])
            with _graphs_lock:
                _graphs[customer_id] = graph
            logger.info(
                "Startup snapshot loaded: customer=%s nodes=%d edges=%d age=%.0fs",
                customer_id,
                len(graph._nodes),
                len(graph._edge_index),
                time.time() - row["received_at"],
            )
        except Exception as exc:
            logger.warning(
                "Startup snapshot load failed for %s: %s",
                customer_id,
                exc,
            )


@app.on_event("startup")
async def _startup() -> None:
    _load_api_keys()
    _init_repository()
    _load_snapshots_on_startup()
    logger.info("StackTracer backend ready")


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _repository and hasattr(_repository, "close"):
        try:
            _repository.close()
        except Exception:
            pass


# ====================================================================== #
# Auth
# ====================================================================== #


def _authenticate(authorization: Optional[str]) -> str:
    """Validate Bearer token, return customer_id."""
    if not authorization or not authorization.startswith(
        "Bearer "
    ):
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header",
        )
    api_key = authorization[7:]
    customer_id = _valid_api_keys.get(api_key)
    if not customer_id:
        raise HTTPException(
            status_code=401, detail="Invalid API key"
        )
    return customer_id


# ====================================================================== #
# Graph helpers
# ====================================================================== #


def get_graph(customer_id: str) -> Optional[Any]:
    """Return the latest deserialised graph for this customer, or None."""
    with _graphs_lock:
        return _graphs.get(customer_id)


def require_graph(customer_id: str) -> Any:
    """Return graph or raise 404 — used by every query endpoint."""
    graph = get_graph(customer_id)
    if graph is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "No graph snapshot received yet for this account. "
                "Ensure the StackTracer agent is running — "
                "it ships a snapshot every 60 seconds after startup."
            ),
        )
    return graph


# ====================================================================== #
# Pydantic schemas
# ====================================================================== #


class IngestPayload(BaseModel):
    events: List[Dict[str, Any]]


class QueryRequest(BaseModel):
    query: str


class DeploymentMarkRequest(BaseModel):
    label: str = "deployment"


# ====================================================================== #
# Routes: Graph snapshot (agent → backend)
# ====================================================================== #


@app.post("/api/v1/graph/snapshot")
async def receive_snapshot(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Receive a serialised RuntimeGraph from the StackTracer agent.
    The agent calls this every 60 seconds via the uploader._flush_snapshot().
    Deserialises the graph into memory and persists the raw bytes to storage
    so FastAPI restarts can reload without waiting for the next agent snapshot.
    """
    print(">>>>>HERE")
    customer_id = _authenticate(authorization)
    body = await request.body()
    content_type = request.headers.get(
        "content-type", "application/msgpack"
    )

    if not body:
        raise HTTPException(
            status_code=400, detail="Empty snapshot body"
        )

    try:
        from stacktracer.core.graph_serializer import (
            MsgpackSerializer,
            ProtobufSerializer,
        )

        serializer = (
            ProtobufSerializer()
            if content_type == "application/x-protobuf"
            else MsgpackSerializer()
        )
        graph = serializer.deserialize(body)

        # 1. Store in memory — immediate query serving
        with _graphs_lock:
            _graphs[customer_id] = graph

        # 2. Persist to storage — survives FastAPI restarts
        if _repository and hasattr(
            _repository, "insert_snapshot"
        ):
            _repository.insert_snapshot(
                customer_id=customer_id,
                data=body,
                content_type=content_type,
                node_count=len(graph._nodes),
                edge_count=len(graph._edge_index),
            )

        node_count = len(graph._nodes)
        edge_count = len(graph._edge_index)

        logger.info(
            "Snapshot received: customer=%s nodes=%d edges=%d bytes=%d",
            customer_id,
            node_count,
            edge_count,
            len(body),
        )
        return {
            "status": "ok",
            "nodes": node_count,
            "edges": edge_count,
            "bytes": len(body),
        }

    except Exception as exc:
        logger.error(
            "Snapshot deserialise failed: %s", exc, exc_info=True
        )
        raise HTTPException(
            status_code=400,
            detail=f"Snapshot parse error: {exc}",
        )


@app.post("/api/v1/graph/diff")
async def receive_graph_diff(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Receive a serialised RuntimeGraph from the StackTracer agent.
    The agent calls this every 60 seconds via the uploader._flush_snapshot().
    Deserialises the graph into memory and persists the raw bytes to storage
    so FastAPI restarts can reload without waiting for the next agent snapshot.
    """
    customer_id = _authenticate(authorization)
    body = await request.json()
    if _repository is not None:
        _repository.insert_graph_diff(customer_id, body)
    return {"ok": True}


# ====================================================================== #
# Routes: Event ingest (agent → backend, persistence only)
# ====================================================================== #


@app.post("/api/v1/events")
async def ingest_events(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Receive raw probe events from the agent uploader for persistence.
    Accepts msgpack (application/msgpack) or JSON (application/json).
    Stores to repository for historical trace queries.
    Does NOT rebuild the graph — that arrives via POST /api/v1/graph/snapshot.
    """
    customer_id = _authenticate(authorization)

    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")

    content_type = request.headers.get("content-type", "")
    try:
        if "msgpack" in content_type:
            import msgpack

            payload = msgpack.unpackb(body, raw=False)
        else:
            payload = json.loads(body)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to deserialise body: {exc}",
        )

    stored = 0
    errors = 0
    for raw in payload.get("events", []):
        try:
            raw.setdefault("metadata", {})[
                "customer_id"
            ] = customer_id
            from stacktracer.core.event_schema import (
                NormalizedEvent,
            )

            event = NormalizedEvent.from_dict(raw)
            if _repository is not None:
                _repository.insert_event(event)
            stored += 1
        except Exception as exc:
            errors += 1
            logger.debug(
                "Event store error: %s | raw=%s", exc, raw
            )

    return {"status": "ok", "stored": stored, "errors": errors}


@app.post("/api/v1/ingest")
async def ingest_events_compat(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Backward-compatible alias for /api/v1/events."""
    return await ingest_events(request, authorization)


# ====================================================================== #
# Routes: Graph queries (all read from deserialised snapshot)
# ====================================================================== #


@app.post("/api/v1/query")
async def query(
    request: QueryRequest,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Execute a raw DSL query against the latest graph snapshot."""
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)

    try:
        from stacktracer.query.parser import (
            parse as parse_query,
            execute as execute_query,
        )

        parsed = parse_query(request.query)
        return execute_query(parsed, graph)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"Query parse error: {exc}"
        )


@app.get("/api/v1/graph")
async def get_graph_route(
    service: Optional[str] = None,
    system: Optional[str] = None,
    authorization: Optional[str] = Header(None),
) -> Dict:
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)
    engine = _BackendEngine(graph)

    from stacktracer.query.parser import ParsedQuery
    from stacktracer.query.parser import execute as execute_query

    if system:
        q = ParsedQuery(
            verb="SHOW",
            metric="graph",
            filters={"system": system},
        )
    elif service:
        q = ParsedQuery(
            verb="SHOW",
            metric="graph",
            filters={"service": service},
        )
    else:
        q = ParsedQuery(verb="SHOW", metric="graph")

    return execute_query(q, engine)


@app.get("/api/v1/causal")
async def causal(
    tags: Optional[str] = None,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Run all causal rules against the latest snapshot.
    No tracker on the backend — request_duration_anomaly will not fire here.
    That rule requires the live ActiveRequestTracker in the agent process.
    Use the REPL (connects to agent Unix socket) for live anomaly detection.
    """
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)
    tag_list = (
        [t.strip() for t in tags.split(",")] if tags else None
    )

    from stacktracer.core.causal import build_default_registry
    from stacktracer.core.temporal import TemporalStore

    # No tracker — backend has no live requests.
    # build_default_registry(tracker=None) registers the anomaly rule
    # but its predicate returns False immediately, which is correct.
    registry = build_default_registry(tracker=None)
    temporal = TemporalStore()  # empty — backend has no diffs
    matches = registry.evaluate(graph, temporal, tags=tag_list)

    return {
        "match_count": len(matches),
        "matches": [m.to_dict() for m in matches],
    }


@app.get("/api/v1/hotspots")
async def hotspots(
    top: int = 10,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Return the top N nodes by call count from the latest snapshot."""
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)

    return {
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
            }
            for n in graph.hottest_nodes(top_n=top)
        ]
    }


@app.get("/api/v1/diff")
async def diff(
    since: Optional[str] = None,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Graph diff since a named marker or timestamp.
    NOTE: TemporalStore diffs live in the agent process.
    The backend can only serve diffs if the agent embeds temporal
    metadata in the graph snapshot (future: include diffs in snapshot payload).
    For now, this endpoint returns what is available in the snapshot.
    """
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)

    from stacktracer.query.parser import (
        ParsedQuery,
        execute as execute_query,
    )

    q = ParsedQuery(
        verb="DIFF",
        metric="edges",
        filters={"since": since} if since else {},
    )
    return execute_query(q, graph)


@app.get("/api/v1/traces/{trace_id}")
async def get_trace(
    trace_id: str,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Reconstruct the critical path for a trace from the event store.
    Requires raw events to have been persisted via POST /api/v1/events.
    """
    customer_id = _authenticate(authorization)

    if _repository is None:
        raise HTTPException(
            status_code=503,
            detail="No storage backend configured — cannot retrieve traces.",
        )

    events = _repository.query_events(
        trace_id=trace_id, limit=500
    )
    if not events:
        raise HTTPException(
            status_code=404,
            detail=f"No events found for trace '{trace_id}'",
        )

    # Build critical path: sort by wall_time, compute durations
    events.sort(key=lambda e: e.get("wall_time", 0))
    path = []
    total_ms = 0.0
    for e in events:
        dur_ns = e.get("duration_ns")
        dur_ms = round(dur_ns / 1e6, 3) if dur_ns else None
        if dur_ms:
            total_ms += dur_ms
        path.append(
            {
                "probe": e.get("probe"),
                "service": e.get("service"),
                "name": e.get("name"),
                "wall_time": e.get("wall_time"),
                "duration_ms": dur_ms,
                "span_id": e.get("span_id"),
            }
        )

    return {
        "trace_id": trace_id,
        "stages": len(path),
        "total_ms": round(total_ms, 3),
        "path": path,
    }


# ====================================================================== #
# Routes: Deployment markers
# ====================================================================== #


@app.post("/api/v1/deployment")
async def mark_deployment_endpoint(
    body: DeploymentMarkRequest,
    authorization: Optional[str] = Header(None),
) -> Dict:
    customer_id = _authenticate(authorization)
    if _repository is not None:
        _repository.insert_deployment_marker(
            customer_id, body.label
        )
    logger.info(
        "Deployment marked: customer=%s label=%s",
        customer_id,
        body.label,
    )
    return {
        "ok": True,
        "label": body.label,
        "timestamp": time.time(),
    }


# ====================================================================== #
# Routes: Status and health
# ====================================================================== #


@app.get("/api/v1/status")
async def status(
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Return snapshot metadata and system state for this customer."""
    customer_id = _authenticate(authorization)
    graph = get_graph(
        customer_id
    )  # None is OK here — status always responds

    snapshot_info: Dict[str, Any] = {"available": False}
    if graph is not None:
        snapshot_info = {
            "available": True,
            "nodes": len(graph._nodes),
            "edges": len(graph._edge_index),
            "last_updated": getattr(graph, "last_updated", None),
        }

    storage_info = "none"
    if _repository is not None:
        storage_info = type(_repository).__name__

    return {
        "customer_id": customer_id,
        "snapshot": snapshot_info,
        "storage": storage_info,
        "timestamp": time.time(),
    }


@app.get("/health")
async def health() -> Dict:
    """Liveness probe — always returns 200 if the process is running."""
    return {"status": "healthy", "timestamp": time.time()}


@app.get("/api/v1/nodes")
async def get_nodes(
    service: Optional[str] = None,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Return all nodes from the latest graph snapshot, optionally filtered by service."""
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)

    nodes = []
    for node in graph.all_nodes():
        if service and node.service != service:
            continue
        nodes.append(
            {
                "id": node.id,
                "service": node.service,
                "node_type": node.node_type,
                "call_count": node.call_count,
                "avg_duration_ns": node.avg_duration_ns,
                "first_seen": node.first_seen,
                "last_seen": node.last_seen,
                "metadata": node.metadata,
            }
        )

    return {
        "ok": True,
        "data": {"metric": "nodes", "data": nodes},
    }


@app.get("/api/v1/edges")
async def get_edges(
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Return all edges from the latest graph snapshot."""
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)

    edges = []
    for edge in graph.all_edges():
        edges.append(
            {
                "source": edge.source,
                "target": edge.target,
                "type": edge.edge_type,
                "call_count": edge.call_count,
                "weight": edge.call_count,
            }
        )

    return {
        "ok": True,
        "data": {"metric": "edges", "data": edges},
    }


# ====================================================================== #
# Error handling
# ====================================================================== #


@app.exception_handler(Exception)
async def generic_error(
    request: Request, exc: Exception
) -> JSONResponse:
    logger.error(
        "Unhandled error on %s: %s",
        request.url.path,
        exc,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
        },
    )

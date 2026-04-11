"""
FastAPI service - persistence, query, and UI surface for OriginTracer.

Surfaces:
    POST /api/v1/graph/snapshot - receive serialised graph from agent
    POST /api/v1/events - receive raw events for persistence
    GET  /api/v1/graph - current graph (from latest snapshot)
    GET  /api/v1/traces/{id} - critical path from event store
    GET  /api/v1/causal - causal rules on latest snapshot
    GET  /api/v1/hotspots - top N busiest nodes
    GET  /api/v1/diff - graph diff since marker
    POST /api/v1/deployment - store deployment marker
    GET  /api/v1/status - snapshot metadata + system info
    POST /api/v1/query - raw DSL query on latest snapshot
    GET  /health - liveness probe

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

import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from origintracer.storage.base import InMemoryRepository

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
        from origintracer.core.semantic import SemanticLayer
        from origintracer.core.temporal import TemporalStore

        self.graph = graph
        self.semantic = SemanticLayer()
        self.temporal = TemporalStore()
        self.repository = None

    def critical_path(
        self, trace_id: str
    ) -> List[Dict[str, Any]]:
        from origintracer.core.event_schema import ProbeTypes

        # pull events from repository instead of _event_log
        events = get_repository().query_events(
            trace_id=trace_id, limit=1000
        )

        # repository returns dicts, sort by timestamp
        events.sort(key=lambda e: e.get("timestamp", 0))

        registered = list(ProbeTypes.all().keys())
        filtered = [
            e for e in events if e.get("probe") in registered
        ]

        path = []
        last_ts = None
        for e in filtered:
            ts = e.get("timestamp")
            duration_ms = (
                round((ts - last_ts) * 1000, 3)
                if last_ts and ts
                else None
            )
            path.append(
                {
                    "probe": e.get("probe"),
                    "service": e.get("service"),
                    "name": e.get("name"),
                    "timestamp": ts,
                    "wall_time": e.get("wall_time"),
                    "duration_ms": duration_ms,
                    "metadata": e.get("metadata"),
                }
            )
            last_ts = ts

        return path


@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP code
    _load_api_keys()
    _init_repository()
    _load_snapshots_on_startup()
    logger.info("StackTracer backend ready")
    try:
        yield
    finally:
        # SHUTDOWN code
        if _repository and hasattr(_repository, "close"):
            try:
                _repository.close()
            except Exception:
                pass


app = FastAPI(
    lifespan=lifespan,
    title="StackTracer API",
    description="Runtime observability backend for Python async services",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in production use explicit origins
    allow_methods=["*"],
    allow_headers=["*"],
)


# One deserialised RuntimeGraph per customer.
# This is the cache in front of st_snapshots in the database.
# Populated on startup (from DB) and updated on every POST /graph/snapshot.
_graphs: Dict[str, Any] = {}
_graphs_lock = threading.Lock()

# Storage repository — set in _init_repository() at startup.
# Implements insert_event(), insert_snapshot(), get_latest_snapshot(),
# query_events(), insert_marker().
_repository: Optional[Any] = None

# API key:customer_id mapping.
_valid_api_keys: Dict[str, str] = {}


def _load_api_keys() -> None:
    global _valid_api_keys
    raw = os.getenv("STACKTRACER_API_KEYS", "")
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            key, customer = pair.split(":", 1)
            _valid_api_keys[key.strip()] = customer.strip()
    if not _valid_api_keys:
        _valid_api_keys["test-key-123"] = "local_dev"
        logger.warning(
            "STACKTRACER_API_KEYS not set - "
            "test-key-123 accepted for development"
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

            from origintracer.storage.base import (
                PGEventRepository,
            )

            conn = psycopg2.connect(db_dsn)
            _repository = PGEventRepository(conn)
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
            from origintracer.storage.base import (
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

    from origintracer.storage.base import InMemoryRepository

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
    # Known customers come from the API key map.
    # For multi-tenant production: iterate a customers table instead.
    customer_ids = set(_valid_api_keys.values())
    for customer_id in customer_ids:
        try:
            row = _repository.get_latest_snapshot(customer_id)
            if row is None:
                continue
            from origintracer.core.graph_serializer import (
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


class IngestPayload(BaseModel):
    events: List[Dict[str, Any]]


class QueryRequest(BaseModel):
    query: str


class DeploymentMarkRequest(BaseModel):
    label: str = "deployment"


# Unix socket client
_SOCKET_PREFIX = "/tmp/stacktracer-"
_SOCKET_SUFFIX = ".sock"


def discover_sockets() -> list[str]:
    import glob

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


@app.get("/api/v1/workers")
def get_workers(authorization: Optional[str] = Header(None)):
    _authenticate(authorization)
    sockets = discover_sockets()  # reuse your existing function
    workers = []
    for path in sockets:
        pid = path.replace(_SOCKET_PREFIX, "").replace(
            _SOCKET_SUFFIX, ""
        )
        workers.append({"pid": pid, "socket": path})
    return {"data": workers}


# --------------- Routes: Graph snapshot -------------------------------


def get_repository() -> InMemoryRepository:
    return _repository


@app.post("/api/v1/graph/snapshot")
async def receive_snapshot(
    request: Request,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Receive a serialised RuntimeGraph from the OriginTracer agent.
    The agent calls this every 60 seconds via the uploader._flush_snapshot().
    Deserialises the graph into memory and persists the raw bytes to storage
    so FastAPI restarts can reload without waiting for the next agent snapshot.
    """
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
        from origintracer.core.graph_serializer import (
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
        repository.insert_snapshot(
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
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Receive a serialised RuntimeGraph from the StackTracer agent.
    The agent calls this every 60 seconds via the uploader._flush_snapshot().
    Deserialises the graph into memory and persists the raw bytes to storage
    so FastAPI restarts can reload without waiting for the next agent snapshot.
    """
    customer_id = _authenticate(authorization)
    body = await request.json()
    repository.insert_graph_diff(customer_id, body)
    print(">>>> GRAPH DIFF", body)
    logger.info(
        "Graph diff received: customer=%s nodes=%d edges=%d bytes=%d",
        customer_id,
        len(body.get("added_nodes") or []),
        len(body.get("added_edges") or []),
        len(body),
    )
    return {"ok": True}


# ====================================================================== #
# Routes: Event ingest (agent → backend, persistence only)
# ====================================================================== #


@app.post("/api/v1/events")
async def ingest_events(
    request: Request,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
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
    import pdb

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
            from origintracer.core.event_schema import (
                NormalizedEvent,
            )

            event = NormalizedEvent.from_dict(raw)
            if repository is not None:
                repository.insert_event(event)
                logger.info(
                    "Events received: customer=%s nodes=%d edges=%d bytes=%d",
                    customer_id,
                    event.timestamp,
                    event.wall_time,
                    len(body),
                )
            stored += 1
        except Exception as exc:
            errors += 1
            logger.debug(
                "Event store error: %s | raw=%s", exc, raw
            )

    return {"status": "ok", "stored": stored, "errors": errors}


@app.get("/api/v1/events")
def get_recent_events(
    limit: int,
    trace_id: Optional[str] = None,
    probe: Optional[str] = None,
    service: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
):
    _authenticate(authorization)

    if repository is None:
        raise HTTPException(
            status_code=503,
            detail="No storage backend configured — cannot retrieve traces.",
        )

    events = repository.query_events(
        trace_id=trace_id,
        probe=probe,
        service=service,
        limit=limit,
    )
    return {
        "metric": "events",
        "data": [
            {
                "probe": e.get("probe"),
                "service": e.get("service"),
                "name": e.get("name"),
                "trace_id": e.get("trace_id"),
                "wall_time": e.get("wall_time"),
                "duration_ns": e.get("duration_ns"),
                "ts": e.get("timestamp")
                or e.get("timestamp_ns"),
            }
            for e in events
        ],
    }


# ------------ Graph queries (all read from deserialised snapshot) --------


@app.post("/api/v1/query")
async def query(
    request: QueryRequest,
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Execute a raw DSL query against the latest graph snapshot."""
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)
    engine = _BackendEngine(graph)

    try:
        from origintracer.query.parser import (
            execute as execute_query,
        )
        from origintracer.query.parser import (
            parse as parse_query,
        )

        parsed = parse_query(request.query)
        print(">>>>IM PARSED:", parsed)
        return execute_query(parsed, engine)
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

    nodes = list(graph.all_nodes())
    edges = list(graph.all_edges())

    node_scope = {x for x in (service, system) if x is not None}
    if node_scope:
        # Only show what the user specifically required
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


@app.get("/api/v1/causal")
async def causal(
    since: str = "origintracer-snapshot",
    tags: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Run all causal rules against the latest snapshot.
    No tracker on the backend — request_duration_anomaly will not fire here.
    That rule requires the live ActiveRequestTracker in the agent process.
    Use the REPL (connects to agent Unix socket) for live anomaly detection.
    """
    customer_id = _authenticate(authorization)
    graph = require_graph(customer_id)
    temporal_raw = repository.label_diff(customer_id, since)
    tag_list = (
        [t.strip() for t in tags.split(",")] if tags else None
    )

    from origintracer.core.causal import (
        PatternRegistry,
        _is_db_node,
    )
    from origintracer.core.temporal import (
        GraphDiff,
        TemporalStore,
    )

    temporal = TemporalStore()
    if temporal_raw:
        diff = GraphDiff(
            added_node_ids=set(
                temporal_raw.get("added_nodes", [])
            ),
            removed_node_ids=set(
                temporal_raw.get("removed_nodes", [])
            ),
            added_edge_keys=set(
                temporal_raw.get("added_edges", [])
            ),
            removed_edge_keys=set(
                temporal_raw.get("removed_edges", [])
            ),
            timestamp=temporal_raw.get("timestamp", time.time()),
            label=temporal_raw.get("label"),
        )
        temporal._diffs.append(diff)

    def _register_causal_rules():
        """Import rule modules to trigger global registration."""
        import origintracer.rules.asyncio_rules  # noqa: F401
        import origintracer.rules.django_rules  # noqa: F401

        # add any other rule files here

    _register_causal_rules()

    # No tracker — backend has no live requests.
    # registry() registers the anomaly rule
    # but its predicate returns False immediately, which is correct.
    registry = PatternRegistry
    matches = registry.evaluate(graph, temporal, tags=tag_list)
    print(">>>> CAUSAL", matches)
    print(">>>> TEMPORAL", temporal)
    print(">>>> REGISTRY", registry._rules)
    print(">>>> GRAPH", graph)
    return {
        "match_count": len(matches),
        "data": [m.to_dict() for m in matches],
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


@app.get("/api/v1/graph/diff")
async def diff(
    since: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Graph diff since a named marker or timestamp.
    NOTE: TemporalStore diffs live in the agent process.
    The backend can only serve diffs if the agent embeds temporal
    metadata in the graph snapshot (future: include diffs in snapshot payload).
    For now, this endpoint returns what is available in the snapshot.
    """
    customer_id = _authenticate(authorization)
    results = repository.label_diff(customer_id, since)
    if results is None:
        raise HTTPException(
            status_code=404, detail="No graph diffs available"
        )
    print(">>>RESULTS", results)
    return {"data": results}


# tra
@app.get("/api/v1/traces/{trace_id}")
async def get_trace(
    trace_id: str,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Reconstruct the critical path for a trace from the event store.
    Requires raw events to have been persisted via POST /api/v1/events.
    """
    from origintracer.core.event_schema import ProbeTypes

    _authenticate(authorization)
    # pull events from repository instead of _event_log
    if repository is None:
        raise HTTPException(
            status_code=503,
            detail="No storage backend configured - cannot retrieve traces",
        )
    events = repository.query_events(
        trace_id=trace_id, limit=1000
    )

    # repository returns dicts, sort by timestamp
    events.sort(key=lambda e: e.get("timestamp", 0))
    if not events:
        raise HTTPException(
            status_code=404,
            detail=f"No events found for trace '{trace_id}'",
        )
    registered = list(ProbeTypes.all().keys())
    filtered = [
        e for e in events if e.get("probe") in registered
    ]

    path = []
    last_ts = None
    total_ms = 0.0
    for e in filtered:
        ts = e.get("timestamp")
        duration_ms = (
            round((ts - last_ts) * 1000, 3)
            if last_ts and ts
            else None
        )
        dur_ns = e.get("duration_ns")
        dur_ms = round(dur_ns / 1e6, 3) if dur_ns else None
        if dur_ms:
            total_ms += dur_ms
        path.append(
            {
                "probe": e.get("probe"),
                "service": e.get("service"),
                "name": e.get("name"),
                "timestamp": ts,
                "wall_time": e.get("wall_time"),
                "duration_ms": duration_ms,
                "metadata": e.get("metadata"),
            }
        )
        last_ts = ts
    print(">>>TRACE:", path)
    return {"data": path, "total_ms": round(total_ms, 3)}


# ====================================================================== #
# Routes: Deployment markers
# ====================================================================== #


@app.post("/api/v1/deployment")
async def mark_deployment_endpoint(
    request: Request,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    customer_id = _authenticate(authorization)
    body = await request.json()
    if repository is not None:
        repository.insert_deployment_marker(
            customer_id, body.get("label")
        )
    print(">>>> WE DEPLOYED")
    logger.info(
        "Deployment marked: customer=%s label=%s",
        customer_id,
        body.get("label"),
    )
    return {
        "ok": True,
        "label": body.get("label"),
        "timestamp": time.time(),
    }


# ====================================================================== #
# Routes: Status and health
# ====================================================================== #


@app.get("/api/v1/status")
async def status(
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Return snapshot metadata and system state for this customer.
    """
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

    storage_info = type(_repository).__name__ or "none"
    print(
        ">>>>> MY STATUS",
        {
            "customer_id": customer_id,
            "snapshot": snapshot_info,
            "storage": storage_info,
            "timestamp": time.time(),
        },
    )
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
        "weight": e.call_count,  # alias for REPL table renderer
    }

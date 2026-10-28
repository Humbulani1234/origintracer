"""
FastAPI service - persistence, query, and UI surface for OriginTracer.

Surfaces:
    POST /api/v1/graph/snapshot - receive serialised graph from agent
    POST /api/v1/events - receive raw events for persistence
    POST /api/v1/causal/evaluate - evaluate and store causal rule matches
    GET  /api/v1/graph - current graph (from latest snapshot)
    GET  /api/v1/traces/{id} - critical path from event store
    GET  /api/v1/causal - causal rules on latest snapshot
    GET  /api/v1/causal/history - return causal matches history
    GET  /api/v1/hotspots - top N busiest nodes
    GET  /api/v1/diff - graph diff since marker
    POST /api/v1/deployment - store deployment marker
    GET  /api/v1/status - snapshot metadata + system info
    GET  /health - liveness probe

Architecture:
    OriginTracer owns the graph and builds it locally. FastAPI receives
    serialised graph snapshots for every configured interval, deserialises them,
    and serves all queries from the deserialised graph. FastAPI does not
    rebuild a graph from raw events - that is the OriginTracer's job.

    On startup, the latest snapshot is loaded from the storage backend
    so queries work immediately after a FastAPI restart without waiting
    the configured interval period for the next snapshot.

Run with:
    uvicorn backend.main:app --host 0.0.0.0 --port 8001

Environment variables:
    ORIGINTRACER_API_KEYS - comma-separated key:customer pairs
                            e.g. "sk_dev_yyy:dev_customer"
    ORIGINTRACER_DB_DSN - PostgreSQL DSN for event + snapshot storage
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from origintracer.storage.base import InMemoryRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("origintracer.fastapi")


class DeploymentRequest(BaseModel):
    label: Optional[str] = None
    worker_pid: str


class CausalEvaluationRequest(BaseModel):
    """Rule tags to include when an explicit causal evaluation is requested."""

    tags: list[str] = Field(default_factory=list, max_length=20)
    worker_instance_id: str


class GraphDiffRequest(BaseModel):
    added_nodes: Optional[list] = None
    removed_nodes: Optional[list] = None
    added_edges: Optional[list] = None
    removed_edges: Optional[list] = None
    timestamp: Optional[float] = None
    label: Optional[str] = None


def _register_causal_rules():
    """
    Import rule modules to trigger global registration.
    """
    import origintracer.rules.asyncio_rules  # noqa: F401
    import origintracer.rules.django_rules  # noqa: F401


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_api_keys()
    _init_repository()
    _load_snapshots_on_startup()
    _register_causal_rules()
    logger.info("OriginTracer backend ready")
    try:
        yield
    finally:
        if _repository and hasattr(_repository, "close"):
            try:
                _repository.close()
            except Exception:
                pass


app = FastAPI(
    lifespan=lifespan,
    title="OriginTracer API",
    description="Runtime observability backend for async services",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# One deserialised RuntimeGraph per customer and process instance.
_graphs: Dict[tuple[str, str], Any] = {}
_snapshot_metadata: Dict[tuple[str, str], Dict[str, Any]] = {}
_graphs_lock = threading.Lock()
# PID remains the selector for event history; graph state is keyed by instance ID.
_active_pid: Dict[str, str] = {}
_active_pid_lock = threading.Lock()

# Storage repository - set in _init_repository() at startup.
# Implements insert_event(), insert_snapshot(), get_latest_snapshot(),
# query_events(), insert_marker().
_repository: Optional[Any] = None

# API key:customer_id mapping - for development
_valid_api_keys: Dict[str, str] = {}


def _load_api_keys() -> None:
    # Implement proper loading of APIs for real workflow
    global _valid_api_keys
    raw = os.getenv("ORIGINTRACER_API_KEYS", "")
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            key, customer = pair.split(":", 1)
            _valid_api_keys[key.strip()] = customer.strip()
    if not _valid_api_keys:
        _valid_api_keys["test-key-123"] = "local_dev"
        logger.warning(
            "ORIGINTRACER_API_KEYS not set - "
            "test-key-123 accepted for development"
        )


def _init_repository() -> None:
    """
    Connect to the configured storage backend.
    Tries to connect to PostgreSQL (ORIGINTRACER_DB_DSN), then falls back
    to InMemoryRepository for dev.
    """
    global _repository

    db_dsn = os.getenv("ORIGINTRACER_DB_DSN")

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

    _repository = InMemoryRepository()
    logger.info(
        "Storage: InMemory (dev mode - with data lost on restart)"
    )


def _load_snapshots_on_startup() -> None:
    """
    On FastAPI restart, reload the latest graph snapshot from storage
    for each known customer.
    """
    if _repository is None:
        return

    customer_ids = set(_valid_api_keys.values())
    for customer_id in customer_ids:
        try:
            rows = _repository.list_latest_snapshots(customer_id)
            from origintracer.core.graph_serializer import (
                MsgpackSerializer,
                ProtobufSerializer,
            )

            for row in rows:
                serializer = (
                    ProtobufSerializer()
                    if row["content_type"]
                    == "application/x-protobuf"
                    else MsgpackSerializer()
                )
                graph = serializer.deserialize(row["data"])
                key = (customer_id, row["worker_instance_id"])
                with _graphs_lock:
                    _graphs[key] = graph
                    _snapshot_metadata[key] = {
                        k: row.get(k)
                        for k in (
                            "worker_pid",
                            "received_at",
                            "node_count",
                            "edge_count",
                        )
                    }
                logger.info(
                    "Startup snapshot loaded: customer=%s instance=%s pid=%s",
                    customer_id,
                    row["worker_instance_id"],
                    row["worker_pid"],
                )
        except Exception as exc:
            logger.warning(
                "Startup snapshot load failed for %s: %s",
                customer_id,
                exc,
            )


def _authenticate(authorization: Optional[str]) -> str:
    """
    Validate Bearer token, return customer_id.
    """
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


def get_graph(
    customer_id: str, worker_instance_id: str
) -> Optional[Any]:
    """
    Return the latest deserialised graph for this customer, or None.
    """
    with _graphs_lock:
        return _graphs.get((customer_id, worker_instance_id))


def require_graph(
    customer_id: str, worker_instance_id: str
) -> Any:
    """
    Return graph or raise 404 - used by every query endpoint.
    """
    graph = get_graph(customer_id, worker_instance_id)
    if graph is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No graph snapshot exists for worker instance {worker_instance_id}."
            ),
        )
    return graph


# Unix socket client
_SOCKET_PREFIX = "/tmp/origintracer-"
_SOCKET_SUFFIX = ".sock"
_SOCKET_TIMEOUT_S = 10.0
_MAX_WORKER_QUERY_CHARS = 60_000
_MAX_WORKER_RESPONSE_BYTES = 16 * 1024 * 1024


class WorkerQueryRequest(BaseModel):
    """
    One DSL query to execute against a selected live worker.
    """

    query: str = Field(
        min_length=1, max_length=_MAX_WORKER_QUERY_CHARS
    )


class WorkerUnavailableError(RuntimeError):
    """
    Raised when a selected worker socket is no longer available.
    """


class WorkerProtocolError(RuntimeError):
    """
    Raised when a worker returns an invalid or unsafe response.
    """


def _pid_from_socket_path(path: str) -> str:
    """
    Extract the PID encoded by an OriginTracer socket path.
    """
    return path.removeprefix(_SOCKET_PREFIX).removesuffix(
        _SOCKET_SUFFIX
    )


def _socket_path_for_pid(pid: int) -> str:
    """
    Construct the only socket path a client-supplied PID may select.
    """
    return f"{_SOCKET_PREFIX}{pid}{_SOCKET_SUFFIX}"


def _require_instance_id(value: Optional[str]) -> str:
    """Validate the process-instance key required by snapshot operations."""
    if (
        not value
        or len(value) > 64
        or not all(c.isalnum() or c == "-" for c in value)
    ):
        raise HTTPException(
            status_code=400,
            detail="A valid worker_instance_id is required",
        )
    return value


def discover_sockets() -> list[str]:
    """
    Return sockets whose PID still identifies a live local process.

    Stale paths are removed. A permission failure from ``kill(pid, 0)`` still
    proves that the process exists; the later socket connection determines
    whether FastAPI can communicate with it.
    """
    import glob

    live = []
    for path in sorted(
        glob.glob(f"{_SOCKET_PREFIX}*{_SOCKET_SUFFIX}")
    ):
        pid = _pid_from_socket_path(path)
        try:
            # Check if the process is actually alive
            os.kill(int(pid), 0)
            live.append(path)
        except PermissionError:
            live.append(path)
        except (ProcessLookupError, ValueError):
            # Process is dead - remove the stale socket
            try:
                os.unlink(path)
            except OSError:
                pass
    return live


def query_worker_socket(pid: int, query: str) -> dict:
    """
    Execute a DSL query against one live OriginTracer process.

    The PID is resolved exclusively through ``discover_sockets``; callers never
    supply a filesystem path. A fresh Unix-socket connection is used for every
    request, matching the REPL client and the local server's one-request-per-
    connection protocol. Responses are bounded to prevent an unhealthy worker
    from consuming unlimited backend memory.
    """
    socket_path = _socket_path_for_pid(pid)
    if socket_path not in set(discover_sockets()):
        raise WorkerUnavailableError(
            f"OriginTracer worker {pid} is not available"
        )

    request_id = uuid.uuid4().hex[:12]
    request_bytes = (
        json.dumps({"id": request_id, "query": query}).encode(
            "utf-8"
        )
        + b"\n"
    )

    try:
        with socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM
        ) as client:
            client.settimeout(_SOCKET_TIMEOUT_S)
            client.connect(socket_path)
            client.sendall(request_bytes)

            response_bytes = bytearray()
            while b"\n" not in response_bytes:
                chunk = client.recv(65_536)
                if not chunk:
                    break
                response_bytes.extend(chunk)
                if (
                    len(response_bytes)
                    > _MAX_WORKER_RESPONSE_BYTES
                ):
                    raise WorkerProtocolError(
                        "Worker response exceeded the configured limit"
                    )
    except socket.timeout as exc:
        raise WorkerUnavailableError(
            f"OriginTracer worker {pid} timed out"
        ) from exc
    except (ConnectionRefusedError, FileNotFoundError) as exc:
        raise WorkerUnavailableError(
            f"OriginTracer worker {pid} stopped or restarted"
        ) from exc
    except OSError as exc:
        raise WorkerUnavailableError(
            f"Could not connect to OriginTracer worker {pid}: {exc}"
        ) from exc

    if not response_bytes or b"\n" not in response_bytes:
        raise WorkerProtocolError(
            f"OriginTracer worker {pid} returned an incomplete response"
        )

    try:
        response = json.loads(
            bytes(response_bytes)
            .split(b"\n", 1)[0]
            .decode("utf-8")
        )
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise WorkerProtocolError(
            f"OriginTracer worker {pid} returned invalid JSON"
        ) from exc

    if not isinstance(response, dict):
        raise WorkerProtocolError(
            f"OriginTracer worker {pid} returned a non-object response"
        )
    response_id = response.get("id")
    if response_id and response_id != request_id:
        raise WorkerProtocolError(
            f"OriginTracer worker {pid} returned a mismatched response"
        )
    return response


@app.get("/api/v1/workers")
def get_workers(authorization: Optional[str] = Header(None)):
    """List live local processes that expose an OriginTracer query socket."""
    _authenticate(authorization)
    sockets = discover_sockets()
    workers = []
    for path in sockets:
        pid = _pid_from_socket_path(path)
        try:
            status_result = query_worker_socket(
                int(pid), "SHOW STATUS"
            )
            status_data = status_result.get("data", {}).get(
                "data", {}
            )
        except (
            ValueError,
            WorkerUnavailableError,
            WorkerProtocolError,
        ):
            continue
        instance_id = status_data.get("worker_instance_id")
        if not instance_id:
            continue
        workers.append(
            {
                "pid": pid,
                "worker_instance_id": instance_id,
                "socket": path,
            }
        )
    return {"data": workers}


@app.post("/api/v1/workers/{pid}/query")
def query_worker(
    pid: int,
    body: WorkerQueryRequest,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """Proxy one authenticated DSL query to the selected live process."""
    _authenticate(authorization)
    query = body.query.strip()
    if not query:
        raise HTTPException(
            status_code=400,
            detail="Worker query cannot be blank",
        )

    try:
        result = query_worker_socket(pid, query)
    except WorkerUnavailableError as exc:
        raise HTTPException(
            status_code=404, detail=str(exc)
        ) from exc
    except WorkerProtocolError as exc:
        raise HTTPException(
            status_code=502, detail=str(exc)
        ) from exc

    return {
        **result,
        "worker": {
            "pid": str(pid),
            "socket": _socket_path_for_pid(pid),
        },
    }


@app.post("/api/v1/workers/select")
def select_worker(
    body: dict,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """Select a worker PID for event history and return its latest instance."""
    customer_id = _authenticate(authorization)
    worker_pid = str(body.get("worker_pid", ""))
    if not worker_pid.isdigit() or int(worker_pid) < 1:
        raise HTTPException(
            status_code=400,
            detail="A positive worker_pid is required",
        )

    matches = [
        (instance_id, metadata)
        for (
            owner,
            instance_id,
        ), metadata in _snapshot_metadata.items()
        if owner == customer_id
        and str(metadata.get("worker_pid")) == worker_pid
    ]
    if not matches:
        raise HTTPException(
            status_code=404,
            detail=f"No snapshot found for worker PID {worker_pid}",
        )
    instance_id, _ = max(
        matches, key=lambda item: item[1].get("received_at") or 0
    )
    with _active_pid_lock:
        _active_pid[customer_id] = worker_pid
    return {
        "selected": worker_pid,
        "worker_instance_id": instance_id,
    }


def get_repository() -> Any:
    return _repository


@app.post("/api/v1/graph/snapshot")
async def receive_snapshot(
    request: Request,
    authorization: Optional[str] = Header(None),
    worker_instance_id_header: Optional[str] = Header(
        None, alias="X-OriginTracer-Worker-Instance-ID"
    ),
    worker_pid_header: Optional[int] = Header(
        None, alias="X-OriginTracer-Worker-PID"
    ),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Receive a serialised RuntimeGraph from the OriginTracer agent.
    The agent calls this every interval seconds via the
    uploader._flush_snapshot(). Deserialises the graph into memory and
    persists the raw bytes to storage so FastAPI restarts can reload without
    waiting for the next snapshot.
    """
    customer_id = _authenticate(authorization)
    worker_instance_id = _require_instance_id(
        worker_instance_id_header
    )
    if not worker_pid_header or worker_pid_header < 1:
        raise HTTPException(
            status_code=400,
            detail="A positive X-OriginTracer-Worker-PID is required",
        )
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

        # Store in memory - immediate query serving
        with _graphs_lock:
            _graphs[(customer_id, worker_instance_id)] = graph
            _snapshot_metadata[
                (customer_id, worker_instance_id)
            ] = {
                "worker_pid": worker_pid_header,
                "received_at": time.time(),
                "node_count": len(graph._nodes),
                "edge_count": len(graph._edge_index),
            }
        with _active_pid_lock:
            _active_pid.setdefault(
                customer_id, str(worker_pid_header)
            )
        # Persist to storage - survives FastAPI restarts
        repository.insert_snapshot(
            customer_id=customer_id,
            worker_instance_id=worker_instance_id,
            worker_pid=worker_pid_header,
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
    body: GraphDiffRequest,
    authorization: Optional[str] = Header(None),
    worker_instance_id_header: Optional[str] = Header(
        None, alias="X-OriginTracer-Worker-Instance-ID"
    ),
    worker_pid_header: Optional[int] = Header(
        None, alias="X-OriginTracer-Worker-PID"
    ),
    repository: Any = Depends(get_repository),
) -> Dict:
    """
    Receive an incremental graph diff from the OriginTracer agent
    """
    customer_id = _authenticate(authorization)
    worker_instance_id = _require_instance_id(
        worker_instance_id_header
    )
    if not worker_pid_header or worker_pid_header < 1:
        raise HTTPException(
            status_code=400,
            detail="A positive X-OriginTracer-Worker-PID is required",
        )
    repository.insert_graph_diff(
        customer_id,
        worker_instance_id,
        body.model_dump(),
        str(worker_pid_header),
    )

    logger.info(
        "Graph diff received: customer=%s nodes=%d edges=%d bytes=%d",
        customer_id,
        len(body.added_nodes or []),
        len(body.added_edges or []),
        len(
            [
                v
                for v in body.model_dump().values()
                if v is not None
            ]
        ),
    )
    return {"ok": True}


@app.post("/api/v1/events")
async def ingest_events(
    request: Request,
    authorization: Optional[str] = Header(None),
    repository: Any = Depends(get_repository),
) -> Dict:
    """
    Receive raw probe events from the process uploader for persistence.
    Accepts msgpack (application/msgpack) or JSON (application/json).
    Stores to repository for historical trace queries.
    Does NOT rebuild the graph - that arrives via POST /api/v1/graph/snapshot.
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
    worker_pid = payload.get("worker_pid")
    if worker_pid is not None:
        with _active_pid_lock:
            _active_pid.setdefault(customer_id, str(worker_pid))
    for raw in payload.get("events", []):
        try:
            raw.setdefault("metadata", {})[
                "customer_id"
            ] = customer_id
            if worker_pid is not None:
                raw["metadata"].setdefault(
                    "worker_pid", str(worker_pid)
                )
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
    limit: int = Query(default=30, ge=1, le=100),
    trace_id: Optional[str] = None,
    probe: Optional[str] = None,
    service: Optional[str] = None,
    worker_pid: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
):
    customer_id = _authenticate(authorization)
    with _active_pid_lock:
        selected_pid = worker_pid or _active_pid.get(customer_id)

    if repository is None:
        raise HTTPException(
            status_code=503,
            detail="No storage backend configured - cannot retrieve traces.",
        )

    events = repository.query_events(
        customer_id=customer_id,
        worker_pid=selected_pid,
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


@app.get("/api/v1/graph")
async def get_graph_route(
    service: Optional[str] = None,
    system: Optional[str] = None,
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
) -> Dict:
    customer_id = _authenticate(authorization)
    graph = require_graph(
        customer_id, _require_instance_id(worker_instance_id)
    )

    nodes = list(graph.all_nodes())
    edges = list(graph.all_edges())

    node_scope = {x for x in (service, system) if x is not None}
    if node_scope:
        nodes = [n for n in nodes if n.id in node_scope]
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


@app.get("/api/v1/causal/history")
async def causal_history(
    limit: int = Query(50),
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
):
    customer_id = _authenticate(authorization)
    worker_instance_id = _require_instance_id(worker_instance_id)
    return {
        "data": repository.get_causal_history(
            customer_id, worker_instance_id, limit
        )
    }


@app.get("/api/v1/causal")
async def causal(
    tags: Optional[str] = None,
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Run all causal rules against the latest snapshot.
    """
    customer_id = _authenticate(authorization)
    tag_list = (
        [t.strip() for t in tags.split(",")] if tags else None
    )
    return _evaluate_causal_rules(
        customer_id,
        _require_instance_id(worker_instance_id),
        repository,
        tag_list,
    )


@app.post("/api/v1/causal/evaluate")
async def evaluate_causal_rules(
    body: CausalEvaluationRequest,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """Run causal rules and save any matches for an explicit agent action."""
    customer_id = _authenticate(authorization)
    worker_instance_id = _require_instance_id(
        body.worker_instance_id
    )
    return _evaluate_causal_rules(
        customer_id,
        worker_instance_id,
        repository,
        body.tags or None,
    )


def _evaluate_causal_rules(
    customer_id: str,
    worker_instance_id: str,
    repository: Any,
    tag_list: Optional[list[str]],
) -> Dict:
    """Evaluate rules for one customer and persist matches when found."""
    graph = require_graph(customer_id, worker_instance_id)
    from origintracer.core.causal import PatternRegistry
    from origintracer.core.temporal import (
        GraphDiff,
        TemporalStore,
    )

    temporal = TemporalStore()
    raw_diffs = repository.get_diffs(
        customer_id, worker_instance_id
    )

    for d in raw_diffs:
        temporal._diffs.append(
            GraphDiff(
                added_node_ids=set(d.get("added_nodes", [])),
                removed_node_ids=set(d.get("removed_nodes", [])),
                added_edge_keys=set(d.get("added_edges", [])),
                removed_edge_keys=set(
                    d.get("removed_edges", [])
                ),
                timestamp=d.get("timestamp", time.time()),
                label=d.get("label"),
                worker_pid=d.get("worker_pid"),
            )
        )
    # rules registered once at startup in lifespan
    registry = PatternRegistry
    # No tracker - backend has no live requests.
    # rules that depends on it won't be executed
    matches = registry.evaluate(graph, temporal, tags=tag_list)
    # persist to storage
    if matches:
        snapshot_meta = _snapshot_metadata.get(
            (customer_id, worker_instance_id), {}
        )
        repository.save_causal_matches(
            customer_id=customer_id,
            worker_instance_id=worker_instance_id,
            matches=[m.to_dict() for m in matches],
            timestamp=time.time(),
            worker_pid=str(
                snapshot_meta.get("worker_pid", "unknown")
            ),
        )
    return {
        "match_count": len(matches),
        "data": [m.to_dict() for m in matches],
    }


@app.get("/api/v1/hotspots")
async def hotspots(
    top: int = Query(10, ge=1, le=100),
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Return the top N nodes by call count from the latest snapshot.
    """
    customer_id = _authenticate(authorization)
    graph = require_graph(
        customer_id, _require_instance_id(worker_instance_id)
    )

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
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Graph diff since a named marker or timestamp.
    NOTE: TemporalStore diffs live in the OriginTracer process.
    The backend can only serve diffs if the Uploader embeds temporal
    metadata in the graph snapshot (TODO: include diffs in snapshot payload).
    """
    customer_id = _authenticate(authorization)
    results = repository.get_label_diff(
        customer_id,
        _require_instance_id(worker_instance_id),
        since,
    )
    if results is None:
        raise HTTPException(
            status_code=404, detail="No graph diffs available"
        )
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

    customer_id = _authenticate(authorization)
    with _active_pid_lock:
        selected_pid = _active_pid.get(customer_id)
    # pull events from repository instead of _event_log
    if repository is None:
        raise HTTPException(
            status_code=503,
            detail="No storage backend configured - cannot retrieve traces",
        )
    events = repository.query_events(
        customer_id=customer_id,
        worker_pid=selected_pid,
        trace_id=trace_id,
        limit=1000,
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
    if not filtered:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Events found for trace '{trace_id}' but none matched "
                f"registered probe types. Check ProbeTypes registration."
            ),
        )

    path = []
    last_ts = None
    total_ms = 0.0
    for e in filtered:
        ts = e.get("timestamp")
        duration_ms: Optional[float] = (
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
    return {"data": path, "total_ms": round(total_ms, 3)}


@app.post("/api/v1/deployment")
async def mark_deployment_endpoint(
    body: DeploymentRequest,
    authorization: Optional[str] = Header(None),
    worker_instance_id_header: Optional[str] = Header(
        None, alias="X-OriginTracer-Worker-Instance-ID"
    ),
    worker_pid_header: Optional[int] = Header(
        None, alias="X-OriginTracer-Worker-PID"
    ),
    repository: Any = Depends(get_repository),
) -> Dict:
    customer_id = _authenticate(authorization)
    worker_instance_id = _require_instance_id(
        worker_instance_id_header
    )
    if not worker_pid_header or worker_pid_header < 1:
        raise HTTPException(
            status_code=400,
            detail="A positive X-OriginTracer-Worker-PID is required",
        )
    if str(worker_pid_header) != str(body.worker_pid):
        raise HTTPException(
            status_code=400,
            detail="Deployment body worker_pid must match the process header",
        )
    if repository is not None:
        repository.insert_deployment_marker(
            customer_id,
            worker_instance_id,
            body.label,
            str(worker_pid_header),
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


@app.get("/api/v1/status")
async def status(
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Return snapshot metadata and system state for this customer.
    """
    customer_id = _authenticate(authorization)
    worker_instance_id = _require_instance_id(worker_instance_id)
    graph = get_graph(customer_id, worker_instance_id)

    snapshot_info: Dict[str, Any] = {"available": False}
    if graph is not None:
        snapshot_info = {
            "available": True,
            "nodes": len(graph._nodes),
            "edges": len(graph._edge_index),
            "last_updated": getattr(graph, "last_updated", None),
            "worker_instance_id": worker_instance_id,
            **_snapshot_metadata.get(
                (customer_id, worker_instance_id), {}
            ),
        }

    storage_info = type(_repository).__name__ or "none"
    return {
        "customer_id": customer_id,
        "snapshot": snapshot_info,
        "storage": storage_info,
        "timestamp": time.time(),
    }


@app.get("/health")
async def health() -> Dict:
    """
    Liveness probe - always returns 200 if the process is running.
    """
    return {"status": "healthy", "timestamp": time.time()}


@app.get("/api/v1/nodes")
async def get_nodes(
    service: Optional[str] = None,
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Return all nodes from the latest graph snapshot, optionally filtered
    by service.
    """
    customer_id = _authenticate(authorization)
    graph = require_graph(
        customer_id, _require_instance_id(worker_instance_id)
    )

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
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
) -> Dict:
    """
    Return all edges from the latest graph snapshot.
    """
    customer_id = _authenticate(authorization)
    graph = require_graph(
        customer_id, _require_instance_id(worker_instance_id)
    )

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


@app.get("/api/v1/snapshots")
async def list_snapshots(
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """List the latest stored snapshot metadata for each worker instance."""
    customer_id = _authenticate(authorization)
    with _graphs_lock:
        rows = [
            {
                "worker_instance_id": instance_id,
                "worker_pid": meta.get("worker_pid"),
                "received_at": meta.get("received_at"),
                "nodes": meta.get("node_count"),
                "edges": meta.get("edge_count"),
            }
            for (
                owner,
                instance_id,
            ), meta in _snapshot_metadata.items()
            if owner == customer_id
        ]
    return {
        "data": sorted(
            rows,
            key=lambda row: row.get("received_at") or 0,
            reverse=True,
        )
    }


@app.get("/api/v1/snapshots/{worker_instance_id}")
async def get_process_snapshot(
    worker_instance_id: str,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """Return graph content and metadata for exactly one worker instance."""
    customer_id = _authenticate(authorization)
    instance_id = _require_instance_id(worker_instance_id)
    graph = require_graph(customer_id, instance_id)
    with _graphs_lock:
        metadata = dict(
            _snapshot_metadata.get(
                (customer_id, instance_id), {}
            )
        )
    return {
        "worker_instance_id": instance_id,
        **metadata,
        "data": {
            "nodes": [_node_dict(n) for n in graph.all_nodes()],
            "edges": [_edge_dict(e) for e in graph.all_edges()],
        },
    }


# Available in parser and repeated for backend standalone
def _node_dict(n) -> Dict:
    """
    Full node representation - used by SHOW GRAPH.
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


# Available in parser and repeated for backend standalone
def _edge_dict(e) -> Dict:
    """
    Full edge representation - used by SHOW GRAPH.
    """
    return {
        "source": e.source,
        "target": e.target,
        "type": e.edge_type,
        "call_count": e.call_count,
        "weight": e.call_count,  # alias for REPL table renderer
    }

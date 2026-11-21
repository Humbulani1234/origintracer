"""
FastAPI service - persistence, query, and UI surface for OriginTracer.

Surfaces:
    POST /api/v1/graph/snapshot - receive serialised graph from agent
    POST /api/v1/events - receive raw events for persistence
    GET  /api/v1/events/content - read captured LangGraph previews from stored events
    POST /api/v1/causal/evaluate - evaluate and store causal rule matches
    GET  /api/v1/graph - current graph (from latest snapshot)
    GET  /api/v1/traces/{id} - critical path from event store
    GET  /api/v1/traces/{id}/stitch - merge uploaded events across stored workers
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
import math
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

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
from pydantic import BaseModel, Field, field_validator

from backend.auth.service import (
    ApiKeyStore,
    OIDCVerifier,
    Principal,
    authenticate,
)
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


class CausalReportRequest(BaseModel):
    """Current causal matches evaluated by one instrumented process."""

    worker_instance_id: str
    worker_pid: str
    matches: list[Dict[str, Any]] = Field(
        default_factory=list, max_length=100
    )


class GraphDiffRequest(BaseModel):
    added_nodes: Optional[list] = None
    removed_nodes: Optional[list] = None
    added_edges: Optional[list] = None
    removed_edges: Optional[list] = None
    timestamp: Optional[float] = None
    label: Optional[str] = None


class WorkerRuntimeRequest(BaseModel):
    """Runtime-only status sent by an instrumented process heartbeat.

    ``status`` carries live process fields that are not part of the graph
    snapshot. ``observed_at`` is the uploader's Unix timestamp for that
    sample; the API uses it to calculate heartbeat age and reporting
    freshness.
    """

    status: Dict[str, Any]
    observed_at: float


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
    _init_authentication()
    _load_snapshots_on_startup()
    _register_causal_rules()
    logger.info("OriginTracer backend ready")
    try:
        yield
    finally:
        if _auth_connection is not None:
            try:
                _auth_connection.close()
            except Exception:
                logger.exception(
                    "Could not close API-key storage connection"
                )
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


# One deserialised RuntimeGraph per customer and process instance. Snapshot
# identity uses the stable instance ID rather than PID, since operating systems
# can reuse PIDs after a process exits.
_graphs: Dict[tuple[str, str], Any] = {}
# Receipt metadata is retained alongside each in-memory graph and enriched
# with the latest runtime heartbeat when one arrives.
_snapshot_metadata: Dict[tuple[str, str], Dict[str, Any]] = {}
# Recent cumulative Uvicorn request counters per process snapshot. Keeping a
# short backend-side window lets the API compare sibling Gunicorn workers even
# though each instrumented process uploads an independent RuntimeGraph.
_worker_request_samples: Dict[
    tuple[str, str], list[tuple[float, int]]
] = {}
# Latest app-local matches are reported by the process that loaded those rules.
_reported_causal_matches: Dict[
    tuple[str, str], list[dict[str, Any]]
] = {}
_reported_causal_signatures: Dict[tuple[str, str], str] = {}
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
_api_key_store: Optional[ApiKeyStore] = None
_oidc_verifier: Optional[OIDCVerifier] = None
_auth_connection: Optional[Any] = None


def _load_api_keys() -> None:
    global _valid_api_keys
    _valid_api_keys = {}
    raw = os.getenv("ORIGINTRACER_API_KEYS", "")
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            key, customer = pair.split(":", 1)
            _valid_api_keys[key.strip()] = customer.strip()
    if (
        not _valid_api_keys
        and os.getenv("ORIGINTRACER_ENV", "development").lower()
        != "production"
    ):
        _valid_api_keys["test-key-123"] = "local_dev"
        logger.warning(
            "ORIGINTRACER_API_KEYS not set - "
            "test-key-123 accepted in development mode"
        )
    if (
        not _valid_api_keys
        and os.getenv("ORIGINTRACER_ENV", "development").lower()
        == "production"
    ):
        logger.info(
            "No environment API keys configured; OIDC and issued keys remain available"
        )


def _init_authentication() -> None:
    """Initialize persistent API keys and optional OIDC verification."""
    global _api_key_store, _oidc_verifier, _auth_connection
    db_dsn = os.getenv("ORIGINTRACER_DB_DSN")
    production = (
        os.getenv("ORIGINTRACER_ENV", "development").lower()
        == "production"
    )
    if production and not db_dsn:
        raise RuntimeError(
            "ORIGINTRACER_DB_DSN is required in production for persistent API-key storage"
        )
    auth_connection = None
    if db_dsn:
        try:
            import psycopg2

            auth_connection = psycopg2.connect(db_dsn)
        except Exception as exc:
            logger.error(
                "Could not connect to PostgreSQL for API-key storage: %s",
                exc,
            )
            raise RuntimeError(
                "Persistent API-key storage is unavailable"
            ) from exc
    _auth_connection = auth_connection
    _api_key_store = ApiKeyStore(auth_connection)

    issuer = os.getenv("ORIGINTRACER_OIDC_ISSUER", "").strip()
    audience = os.getenv(
        "ORIGINTRACER_OIDC_AUDIENCE", ""
    ).strip()
    jwks_url = (
        os.getenv("ORIGINTRACER_OIDC_JWKS_URL", "").strip()
        or None
    )
    if bool(issuer) != bool(audience):
        raise RuntimeError(
            "Configure both ORIGINTRACER_OIDC_ISSUER and ORIGINTRACER_OIDC_AUDIENCE"
        )
    _oidc_verifier = (
        OIDCVerifier(issuer, audience, jwks_url)
        if issuer
        else None
    )
    if _oidc_verifier:
        logger.info(
            "OIDC authentication enabled for issuer %s", issuer
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
    if _api_key_store is not None:
        customer_ids.update(_api_key_store.customers())
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


def _authenticate_principal(
    authorization: Optional[str],
) -> Principal:
    """Validate a bearer credential and return its account identity."""
    if not authorization or not authorization.startswith(
        "Bearer "
    ):
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header",
        )
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(
            status_code=401, detail="Invalid credentials"
        )
    configured_customer = _valid_api_keys.get(token)
    if configured_customer:
        return Principal(
            customer_id=configured_customer,
            method="configured_api_key",
        )
    if _api_key_store is None:
        raise HTTPException(
            status_code=401, detail="Invalid credentials"
        )
    try:
        principal = authenticate(
            token,
            _valid_api_keys,
            _api_key_store,
            _oidc_verifier,
        )
    except Exception as exc:
        logger.info(
            "Bearer token validation failed: %s",
            type(exc).__name__,
        )
        principal = None
    if principal is None:
        raise HTTPException(
            status_code=401, detail="Invalid credentials"
        )
    return principal


def _authenticate(authorization: Optional[str]) -> str:
    """Validate a bearer credential and return its customer namespace."""
    return _authenticate_principal(authorization).customer_id


class ApiKeyCreateRequest(BaseModel):
    """Request to create a named API key."""

    name: str = Field(min_length=1, max_length=80)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        """Trim whitespace and reject a blank API-key label."""
        value = value.strip()
        if not value:
            raise ValueError(
                "name must contain a non-whitespace character"
            )
        return value


@app.get("/api/v1/auth/me")
def get_authenticated_identity(
    authorization: Optional[str] = Header(None),
) -> dict[str, str]:
    """Return the account identity represented by the bearer credential."""
    principal = _authenticate_principal(authorization)
    return {
        "customer_id": principal.customer_id,
        "auth_method": principal.method,
        **(
            {"subject": principal.subject}
            if principal.subject
            else {}
        ),
    }


@app.get("/api/v1/api-keys")
def list_api_keys(
    authorization: Optional[str] = Header(None),
) -> dict[str, Any]:
    """List safe API-key metadata for the signed-in OIDC account."""
    principal = _authenticate_principal(authorization)
    if principal.method != "oidc":
        raise HTTPException(
            status_code=403,
            detail="OIDC sign-in is required to manage API keys",
        )
    if _api_key_store is None:
        raise HTTPException(
            status_code=503,
            detail="API-key storage is unavailable",
        )
    return {
        "data": _api_key_store.list_for_customer(
            principal.customer_id
        )
    }


@app.post("/api/v1/api-keys", status_code=201)
def create_api_key(
    body: ApiKeyCreateRequest,
    authorization: Optional[str] = Header(None),
) -> dict[str, Any]:
    """Create a customer-scoped key and return its secret only once."""
    principal = _authenticate_principal(authorization)
    if principal.method != "oidc":
        raise HTTPException(
            status_code=403,
            detail="OIDC sign-in is required to manage API keys",
        )
    if _api_key_store is None:
        raise HTTPException(
            status_code=503,
            detail="API-key storage is unavailable",
        )
    secret, metadata = _api_key_store.create(
        principal.customer_id, body.name.strip()
    )
    return {"api_key": secret, "data": metadata}


@app.delete("/api/v1/api-keys/{key_id}", status_code=204)
def revoke_api_key(
    key_id: str, authorization: Optional[str] = Header(None)
) -> None:
    """Revoke one key owned by the signed-in OIDC account."""
    principal = _authenticate_principal(authorization)
    if principal.method != "oidc":
        raise HTTPException(
            status_code=403,
            detail="OIDC sign-in is required to manage API keys",
        )
    if _api_key_store is None:
        raise HTTPException(
            status_code=503,
            detail="API-key storage is unavailable",
        )
    if not _api_key_store.revoke(principal.customer_id, key_id):
        raise HTTPException(
            status_code=404, detail="API key not found"
        )


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


def get_repository() -> Any:
    return _repository


def _worker_runtime_view(
    runtime: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Normalize runtime data and determine whether its heartbeat is fresh.

    Freshness is based on the uploader's observation timestamp and configured snapshot
    interval. The accepted age is at least 30 seconds and at most 300 seconds, with three
    snapshot intervals as the normal window. This signals ingestion freshness; it is not an
    operating-system liveness check.

    Parameters
    ----------
    runtime : dict[str, Any] or None
        Stored heartbeat mapping, or ``None`` when no heartbeat is available.

    Returns
    -------
    dict[str, Any]
        Normalized ``online``, ``runtime_status``, ``last_seen``, and
        ``heartbeat_age_s`` values. Missing or invalid timestamps produce an
        offline signal and a null age.
    """
    if not runtime:
        return {
            "online": False,
            "runtime_status": None,
            "last_seen": None,
            "heartbeat_age_s": None,
        }
    observed_at = runtime.get("observed_at")
    status = runtime.get("status") or {}
    try:
        age = max(0.0, time.time() - float(observed_at))
    except (TypeError, ValueError):
        age = None
    try:
        interval = float(status.get("snapshot_interval_s", 15))
    except (TypeError, ValueError):
        interval = 15.0
    freshness_limit = max(30.0, min(interval * 3, 300.0))
    return {
        "online": age is not None and age <= freshness_limit,
        "runtime_status": status,
        "last_seen": observed_at,
        "heartbeat_age_s": (
            round(age, 1) if age is not None else None
        ),
    }


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

        # Store in memory - immediate query serving. Keep a short counter
        # history so worker_imbalance can compare sibling processes without
        # merging their full RuntimeGraphs.
        received_at = time.time()
        request_count = _uvicorn_request_counter(graph)
        with _graphs_lock:
            _graphs[(customer_id, worker_instance_id)] = graph
            _snapshot_metadata[
                (customer_id, worker_instance_id)
            ] = {
                "worker_pid": worker_pid_header,
                "received_at": received_at,
                "node_count": len(graph._nodes),
                "edge_count": len(graph._edge_index),
            }
            sample_key = (customer_id, worker_instance_id)
            samples = _worker_request_samples.setdefault(
                sample_key, []
            )
            # Compaction or a graph reset can reduce cumulative call_count.
            # Reset the baseline instead of treating topology churn as traffic.
            if samples and request_count < samples[-1][1]:
                samples.clear()
            samples.append((received_at, request_count))
            _worker_request_samples[sample_key] = [
                sample
                for sample in samples
                if received_at - sample[0] <= 120
            ]
            for old_key, old_samples in list(
                _worker_request_samples.items()
            ):
                if (
                    old_key[0] == customer_id
                    and old_key != sample_key
                    and old_samples
                    and received_at - old_samples[-1][0] > 120
                ):
                    _worker_request_samples.pop(old_key, None)
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


@app.post("/api/v1/worker/runtime")
async def receive_worker_runtime(
    body: WorkerRuntimeRequest,
    authorization: Optional[str] = Header(None),
    worker_instance_id_header: Optional[str] = Header(
        None, alias="X-OriginTracer-Worker-Instance-ID"
    ),
    worker_pid_header: Optional[int] = Header(
        None, alias="X-OriginTracer-Worker-PID"
    ),
    repository: Any = Depends(get_repository),
) -> Dict[str, Any]:
    """Store a worker's latest runtime heartbeat beside its graph snapshot.

    The uploader sends process-health fields that are not part of the serialized
    ``RuntimeGraph``, including uptime, active-request count, and active probes. The backend
    associates the heartbeat with the authenticated customer and exact worker instance, and
    accepts it only after the graph snapshot is stored and its PID matches the snapshot
    metadata.

    Parameters
    ----------
    body : WorkerRuntimeRequest
        Runtime status fields and the worker's observation timestamp.
    authorization : str, optional
        Customer authentication credential from the request header.
    worker_instance_id_header : str, optional
        Stable process-instance ID supplied by the uploader.
    worker_pid_header : int, optional
        PID that must match the stored graph snapshot.
    repository : Any
        Configured persistence layer for runtime status.

    Returns
    -------
    Dict[str, Any]
        Confirmation and the worker-instance ID whose heartbeat was updated.

    Raises
    ------
    HTTPException
        If worker identity, timestamp, status fields, or snapshot association is invalid.
    """
    customer_id = _authenticate(authorization)
    instance_id = _require_instance_id(worker_instance_id_header)
    if not worker_pid_header or worker_pid_header < 1:
        raise HTTPException(
            status_code=400,
            detail="A positive X-OriginTracer-Worker-PID is required",
        )
    now = time.time()
    if (
        not math.isfinite(body.observed_at)
        or body.observed_at <= 0
        or body.observed_at > now + 60
    ):
        raise HTTPException(
            status_code=400, detail="Invalid observed_at"
        )
    status = body.status
    for field in (
        "uptime_s",
        "active_requests",
        "graph_nodes",
        "graph_edges",
    ):
        value = status.get(field)
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Runtime status field {field} must be non-negative",
            )
    probes = status.get("probes_active", [])
    if not isinstance(probes, list) or any(
        not isinstance(item, str) for item in probes
    ):
        raise HTTPException(
            status_code=400,
            detail="Runtime status field probes_active must be a list of strings",
        )
    with _graphs_lock:
        metadata = _snapshot_metadata.get(
            (customer_id, instance_id)
        )
        if metadata is None:
            raise HTTPException(
                status_code=404,
                detail="A graph snapshot for this worker instance is required before its runtime heartbeat",
            )
        if str(metadata.get("worker_pid")) != str(
            worker_pid_header
        ):
            raise HTTPException(
                status_code=409,
                detail="Runtime heartbeat PID does not match the stored snapshot",
            )
        metadata["runtime_status"] = status
        metadata["runtime_observed_at"] = body.observed_at
    if repository is not None:
        repository.upsert_worker_runtime(
            customer_id,
            instance_id,
            str(worker_pid_header),
            status,
            body.observed_at,
        )
    return {"ok": True, "worker_instance_id": instance_id}


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
    diff_data = body.model_dump()
    # Use backend receipt time so diff rows and deployment markers share one
    # clock, even when the instrumented process runs on another host.
    diff_data["timestamp"] = time.time()
    repository.insert_graph_diff(
        customer_id,
        worker_instance_id,
        diff_data,
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
    worker_instance_id_header: Optional[str] = Header(
        None, alias="X-OriginTracer-Worker-Instance-ID"
    ),
    repository: Any = Depends(get_repository),
) -> Dict:
    """
    Receive raw probe events from the process uploader for persistence.
    Accepts msgpack (application/msgpack) or JSON (application/json).
    Stores to repository for historical trace queries. The uploader's worker
    instance ID header is copied into event metadata for process-safe filtering.
    Does NOT rebuild the graph - that arrives via POST /api/v1/graph/snapshot.
    Captured content remains opt-in and is exposed separately through an
    allowlisted response from `/api/v1/events/content`.
    """
    customer_id = _authenticate(authorization)
    worker_instance_id = (
        _require_instance_id(worker_instance_id_header)
        if worker_instance_id_header
        else None
    )
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
    errors_by_trace: Dict[str, int] = {}
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
            if worker_instance_id:
                raw["metadata"][
                    "worker_instance_id"
                ] = worker_instance_id
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
            trace_id = (
                str(raw.get("trace_id") or "")
                if isinstance(raw, dict)
                else ""
            )
            errors_by_trace[trace_id] = (
                errors_by_trace.get(trace_id, 0) + 1
            )
            logger.debug(
                "Event store error: %s | raw=%s", exc, raw
            )

    return {
        "status": "ok",
        "stored": stored,
        "errors": errors,
        "errors_by_trace": errors_by_trace,
    }


@app.get("/api/v1/events")
def get_recent_events(
    limit: int = Query(default=30, ge=1, le=100),
    trace_id: Optional[str] = None,
    probe: Optional[str] = None,
    service: Optional[str] = None,
    worker_pid: Optional[str] = None,
    worker_instance_id: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
):
    customer_id = _authenticate(authorization)
    selected_pid = worker_pid
    if not selected_pid and not worker_instance_id:
        with _active_pid_lock:
            selected_pid = _active_pid.get(customer_id)

    if repository is None:
        raise HTTPException(
            status_code=503,
            detail="No storage backend configured - cannot retrieve traces.",
        )

    events = repository.query_events(
        customer_id=customer_id,
        worker_pid=selected_pid,
        worker_instance_id=(
            _require_instance_id(worker_instance_id)
            if worker_instance_id
            else None
        ),
        trace_id=trace_id,
        probe=probe,
        service=service,
        limit=limit,
    )
    data = []
    for event in events:
        data.append(
            {
                "probe": event.get("probe"),
                "service": event.get("service"),
                "name": event.get("name"),
                "trace_id": event.get("trace_id"),
                "span_id": event.get("span_id"),
                "parent_span_id": event.get("parent_span_id"),
                "pid": event.get("pid"),
                "tid": event.get("tid"),
                "wall_time": event.get("wall_time"),
                "duration_ns": event.get("duration_ns"),
                "metadata": _ui_observation_metadata(
                    event.get("metadata") or {}
                ),
                "ts": event.get("timestamp")
                or event.get("timestamp_ns")
                or event.get("wall_time"),
            }
        )
    return {"metric": "events", "data": data}


@app.get("/api/v1/events/content")
def get_captured_content(
    worker_instance_id: str = Query(...),
    kind: str = Query(..., pattern="^(llm|tool)$"),
    limit: int = Query(default=100, ge=1, le=100),
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
):
    """Return allowlisted, opt-in LangGraph previews from stored events.

    Results are filtered by authenticated customer, worker instance, and
    content event probe. Arbitrary event metadata is never returned, and the
    endpoint does not query a live worker socket.
    """
    customer_id = _authenticate(authorization)
    instance_id = _require_instance_id(worker_instance_id)
    if repository is None:
        raise HTTPException(
            status_code=503,
            detail="No storage backend configured - cannot retrieve captured content.",
        )

    probes = (
        {
            "langgraph.llm.enter": ("prompt_preview", "prompt"),
            "langgraph.llm.exit": (
                "response_preview",
                "response",
            ),
        }
        if kind == "llm"
        else {
            "langgraph.tool.enter": (
                "tool_input_preview",
                "input",
            ),
            "langgraph.tool.exit": (
                "tool_output_preview",
                "output",
            ),
            "langgraph.error": ("tool_error_preview", "error"),
        }
    )
    # Query each relevant probe separately so unrelated LangGraph events do
    # not consume the result limit before captured previews are found.
    events = []
    for probe in probes:
        events.extend(
            repository.query_events(
                customer_id=customer_id,
                worker_instance_id=instance_id,
                probe=probe,
                limit=limit,
            )
        )

    data = []
    for event in sorted(
        events,
        key=lambda item: item.get("wall_time", 0),
        reverse=True,
    ):
        probe = event.get("probe")
        field, content_kind = probes[probe]
        metadata = event.get("metadata") or {}
        capture_flag = (
            "llm_content_captured"
            if kind == "llm"
            else "tool_content_captured"
        )
        if (
            not metadata.get(capture_flag)
            or field not in metadata
        ):
            continue
        data.append(
            {
                "probe": probe,
                "service": event.get("service"),
                "name": event.get("name"),
                "trace_id": event.get("trace_id"),
                "span_id": event.get("span_id"),
                "parent_span_id": event.get("parent_span_id"),
                "investigation_id": metadata.get(
                    "investigation_id"
                ),
                "kind": content_kind,
                "content": metadata[field],
                "truncated": bool(
                    metadata.get(f"{field}_truncated")
                ),
                "ts": event.get("wall_time"),
            }
        )
        if len(data) >= limit:
            break

    return {
        "metric": f"{kind}_content",
        "worker_instance_id": instance_id,
        "data": data,
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


@app.post("/api/v1/causal/report")
async def report_causal_matches(
    body: CausalReportRequest,
    authorization: Optional[str] = Header(None),
    worker_instance_id_header: Optional[str] = Header(
        None, alias="X-OriginTracer-Worker-Instance-ID"
    ),
    worker_pid_header: Optional[int] = Header(
        None, alias="X-OriginTracer-Worker-PID"
    ),
    repository: Any = Depends(get_repository),
) -> Dict:
    """Store the latest matches evaluated by the reporting process."""
    customer_id = _authenticate(authorization)
    worker_instance_id = _require_instance_id(
        worker_instance_id_header
    )
    if worker_instance_id != _require_instance_id(
        body.worker_instance_id
    ):
        raise HTTPException(
            status_code=400,
            detail="Causal report instance ID must match its header",
        )
    if not worker_pid_header or worker_pid_header < 1:
        raise HTTPException(
            status_code=400,
            detail="A positive X-OriginTracer-Worker-PID is required",
        )
    if str(worker_pid_header) != str(body.worker_pid):
        raise HTTPException(
            status_code=400,
            detail="Causal report PID must match its header",
        )

    matches = []
    for item in body.matches:
        if not isinstance(item, dict) or not item.get("rule"):
            continue
        try:
            confidence = float(item.get("confidence", 0))
        except (TypeError, ValueError):
            continue
        if (
            not math.isfinite(confidence)
            or not 0 <= confidence <= 1
        ):
            continue
        matches.append(
            {
                "rule": str(item["rule"]),
                "confidence": confidence,
                "explanation": str(item.get("explanation", "")),
                "evidence": (
                    item.get("evidence")
                    if isinstance(item.get("evidence"), dict)
                    else {}
                ),
                "timestamp": item.get("timestamp", time.time()),
            }
        )

    key = (customer_id, worker_instance_id)
    signature = json.dumps(
        [
            {
                "rule": item["rule"],
                "confidence": item["confidence"],
                "explanation": item["explanation"],
                "evidence": item["evidence"],
            }
            for item in matches
        ],
        sort_keys=True,
        default=str,
    )
    with _graphs_lock:
        changed = (
            _reported_causal_signatures.get(key) != signature
        )
        _reported_causal_matches[key] = matches
        _reported_causal_signatures[key] = signature

    if changed:
        repository.save_causal_matches(
            customer_id=customer_id,
            worker_instance_id=worker_instance_id,
            matches=matches,
            timestamp=time.time(),
            worker_pid=str(worker_pid_header),
        )
    return {"ok": True, "match_count": len(matches)}


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
        persist=False,
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
        persist=True,
    )


def _evaluate_causal_rules(
    customer_id: str,
    worker_instance_id: str,
    repository: Any,
    tag_list: Optional[list[str]],
    *,
    persist: bool,
) -> Dict:
    """Evaluate rules and optionally persist matches for one worker."""
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
    if persist and matches:
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
    data_by_rule = {}
    for match in matches:
        item = match.to_dict()
        rule_name = item.get("rule") or getattr(
            match, "rule_name", None
        )
        if rule_name:
            data_by_rule[str(rule_name)] = item
    if tag_list is None:
        with _graphs_lock:
            reported_matches = list(
                _reported_causal_matches.get(
                    (customer_id, worker_instance_id), []
                )
            )
        for reported in reported_matches:
            data_by_rule[reported["rule"]] = reported
    data = sorted(
        data_by_rule.values(),
        key=lambda item: item.get("confidence", 0),
        reverse=True,
    )
    fleet_match = _fleet_worker_imbalance(
        customer_id, worker_instance_id, tag_list
    )
    if fleet_match:
        data = [
            item
            for item in data
            if item.get("rule") != "worker_imbalance"
        ]
        data.append(fleet_match)
        data.sort(
            key=lambda item: item.get("confidence", 0),
            reverse=True,
        )
    return {
        "match_count": len(data),
        "data": data,
    }


def _fleet_worker_imbalance(
    customer_id: str,
    target_instance_id: str,
    tag_list: Optional[list[str]],
) -> Optional[Dict[str, Any]]:
    """Compare recent request rates for fresh workers of one Gunicorn master.

    Each process owns a separate graph, so the core graph rule cannot see its
    siblings. This API-level aggregation uses the same per-process snapshots
    and recent request-counter deltas already received by the backend. It
    deliberately requires fresh heartbeats and at least two measured workers.
    """
    if tag_list is not None:
        worker_tags = {"gunicorn", "concurrency", "blocking"}
        if not worker_tags.intersection(tag_list):
            return None

    now = time.time()
    with _graphs_lock:
        snapshots = []
        for (owner, instance_id), graph in _graphs.items():
            if owner != customer_id:
                continue
            metadata = dict(
                _snapshot_metadata.get((owner, instance_id), {})
            )
            samples = list(
                _worker_request_samples.get(
                    (owner, instance_id), []
                )
            )
            snapshots.append(
                (instance_id, graph, metadata, samples)
            )

    records: list[dict[str, Any]] = []
    for instance_id, graph, metadata, samples in snapshots:
        runtime = metadata.get("runtime_status") or {}
        heartbeat = _worker_runtime_view(
            {
                "status": runtime,
                "observed_at": metadata.get(
                    "runtime_observed_at"
                ),
            }
        )
        if not heartbeat["online"]:
            continue

        master_pid = None
        for node in graph.all_nodes():
            if (
                node.metadata.get("probe")
                == "gunicorn.master.start"
            ):
                master_pid = node.metadata.get("master_pid")
                if master_pid is not None:
                    break
        if master_pid is None:
            continue

        try:
            recent_samples = [
                sample
                for sample in samples
                if now - sample[0] <= 60
            ]
            if len(recent_samples) < 2:
                continue
            start_at, start_count = recent_samples[0]
            end_at, end_count = recent_samples[-1]
            window_s = end_at - start_at
            request_delta = end_count - start_count
        except (TypeError, ValueError, IndexError):
            continue
        if window_s < 10 or request_delta < 0:
            continue

        worker_pid = metadata.get("worker_pid")
        records.append(
            {
                "worker_instance_id": instance_id,
                "worker_pid": worker_pid,
                "gunicorn_master_pid": master_pid,
                "request_count": request_delta,
                "window_s": round(window_s, 1),
                "requests_per_second": round(
                    request_delta / window_s, 3
                ),
            }
        )

    target = next(
        (
            record
            for record in records
            if record["worker_instance_id"] == target_instance_id
        ),
        None,
    )
    if target is None:
        return None
    siblings = [
        record
        for record in records
        if str(record["gunicorn_master_pid"])
        == str(target["gunicorn_master_pid"])
    ]
    if len(siblings) < 2:
        return None

    rates = [
        record["requests_per_second"] for record in siblings
    ]
    total_requests = sum(
        record["request_count"] for record in siblings
    )
    zero_load = [
        record
        for record in siblings
        if record["request_count"] == 0
    ]
    if total_requests < 20:
        return None
    if zero_load:
        # A worker receiving no requests is a stronger signal, but require a
        # longer window and more traffic to avoid short-window scheduling noise.
        if min(record["window_s"] for record in siblings) < 30:
            return None
        ratio = None
        imbalance_kind = "zero_requests_on_online_worker"
    else:
        ratio = max(rates) / min(rates)
        if ratio < 2.0:
            return None
        imbalance_kind = "request_rate_ratio"

    busiest = max(
        siblings,
        key=lambda record: record["requests_per_second"],
    )
    return {
        "rule": "worker_imbalance",
        "confidence": 0.80,
        "explanation": (
            "Recent request traffic is uneven across fresh workers under the "
            "same Gunicorn master. This is an imbalance signal, not proof "
            "that a worker is blocked."
        ),
        "evidence": {
            "gunicorn_master_pid": target["gunicorn_master_pid"],
            "workers": siblings,
            "busiest_worker_pid": busiest["worker_pid"],
            "imbalance_kind": imbalance_kind,
            "rate_ratio": (
                round(ratio, 2) if ratio is not None else None
            ),
            "zero_request_worker_pids": [
                record["worker_pid"] for record in zero_load
            ],
            "comparison_basis": (
                "request-counter delta over recent snapshot window"
            ),
        },
        "timestamp": now,
    }


def _uvicorn_request_counter(graph: Any) -> int:
    """Count Uvicorn request-entry edges from Gunicorn workers once each."""
    worker_ids = {
        node.id
        for node in graph.all_nodes()
        if node.service == "gunicorn"
        and node.metadata.get("probe") == "gunicorn.worker.fork"
    }
    return sum(
        edge.call_count
        for edge in graph.all_edges()
        if edge.edge_type == "handled"
        and edge.source in worker_ids
    )


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


@app.get("/api/v1/latency")
async def latency(
    limit: int = Query(20, ge=1, le=100),
    service: Optional[str] = None,
    system: Optional[str] = None,
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
) -> Dict:
    """Rank uploaded snapshot nodes by average duration.

    ``system`` selects nodes tagged with that semantic alias by the uploading
    worker. This keeps semantic filtering tied to the process configuration,
    even when FastAPI runs on another host.
    """
    customer_id = _authenticate(authorization)
    graph = require_graph(
        customer_id, _require_instance_id(worker_instance_id)
    )
    nodes = list(graph.all_nodes())
    if service:
        nodes = [
            node for node in nodes if node.service == service
        ]
    if system:
        nodes = [
            node
            for node in nodes
            if system
            in (node.metadata or {}).get("semantic_labels", [])
        ]
    nodes.sort(
        key=lambda node: node.avg_duration_ns or 0, reverse=True
    )
    return {
        "metric": "latency",
        "worker_instance_id": worker_instance_id,
        "data": [
            {
                "node": node.id,
                "service": node.service,
                "type": node.node_type,
                "call_count": node.call_count,
                "avg_duration_ms": (
                    round(node.avg_duration_ns / 1e6, 3)
                    if node.avg_duration_ns
                    else None
                ),
                "total_duration_ms": round(
                    node.total_duration_ns / 1e6, 3
                ),
            }
            for node in nodes[:limit]
        ],
    }


@app.get("/api/v1/causal/rules")
async def causal_rules_catalog(
    authorization: Optional[str] = Header(None),
) -> Dict:
    """List rule definitions registered by this FastAPI service."""
    _authenticate(authorization)
    from origintracer.core.causal import PatternRegistry

    rules = getattr(PatternRegistry, "_rules", {})
    return {
        "metric": "rules",
        "scope": "backend registered rule catalog",
        "data": [
            {"name": rule.name, "description": rule.description}
            for rule in rules.values()
        ],
    }


@app.get("/api/v1/graph/diff")
async def diff(
    since: Optional[str] = None,
    worker_instance_id: str = Query(...),
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """Return net stored graph changes since a deployment marker or timestamp.

    Named markers and incremental diffs are stored separately. Resolve the
    marker's timestamp, then fold subsequent diff rows so later reversals
    cancel earlier additions/removals.
    """
    customer_id = _authenticate(authorization)
    instance_id = _require_instance_id(worker_instance_id)
    if not since:
        results = repository.get_label_diff(
            customer_id, instance_id, since
        )
        if results is not None:
            return {"data": results}
        raise HTTPException(
            status_code=404, detail="No graph diffs available"
        )

    marker_time = repository.get_deployment_marker_time(
        customer_id, instance_id, since
    )
    if marker_time is None:
        try:
            marker_time = float(since)
        except (TypeError, ValueError):
            # Support graph diffs written by older uploaders that attached a
            # deployment label directly to a diff row.
            legacy = repository.get_label_diff(
                customer_id, instance_id, since
            )
            if legacy is not None:
                return {"data": legacy}
            raise HTTPException(
                status_code=404,
                detail=f"No deployment marker or graph diff found for '{since}'",
            )

    diffs = [
        row
        for row in repository.get_diffs(customer_id, instance_id)
        if float(row.get("timestamp") or 0) > marker_time
    ]
    diffs.sort(key=lambda row: float(row.get("timestamp") or 0))
    net = {
        "added_nodes": set(),
        "removed_nodes": set(),
        "added_edges": set(),
        "removed_edges": set(),
    }
    for diff_row in diffs:
        for kind in ("nodes", "edges"):
            added = net[f"added_{kind}"]
            removed = net[f"removed_{kind}"]
            for item in diff_row.get(f"added_{kind}") or []:
                if item in removed:
                    removed.remove(item)
                else:
                    added.add(item)
            for item in diff_row.get(f"removed_{kind}") or []:
                if item in added:
                    added.remove(item)
                else:
                    removed.add(item)

    return {
        "data": {
            **{
                key: sorted(values)
                for key, values in net.items()
            },
            "timestamp": (
                float(diffs[-1]["timestamp"])
                if diffs
                else marker_time
            ),
            "label": since,
            "since_timestamp": marker_time,
        }
    }


# tra
def _derive_trace_spans(
    events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Derive span intervals and timing metrics from explicit event lineage.

    Parameters
    ----------
    events : list[dict[str, Any]]
        Chronologically ordered event rows for one trace.

    Returns
    -------
    dict[str, Any]
        Observed span intervals, request or trace wall duration when complete
        boundaries exist, estimated self-time after subtracting returned child
        intervals, and overlap pairs. Self-time can include unobserved child
        work. Parentless overlaps establish temporal co-occurrence in the trace
        only. Trace wall duration is withheld when the event sample may be
        truncated.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for event in events:
        span_id = event.get("span_id")
        if span_id:
            grouped.setdefault(str(span_id), []).append(event)

    spans: List[Dict[str, Any]] = []
    for span_id, rows in grouped.items():
        starts = [
            row
            for row in rows
            if str(row.get("probe") or "").endswith(
                (".enter", ".entry", ".start")
            )
        ]
        ends = [
            row
            for row in rows
            if str(row.get("probe") or "").endswith(
                (".exit", ".end", ".error", ".exception")
            )
        ]
        start_row = starts[0] if starts else None
        end_row = ends[-1] if ends else None
        terminal = end_row or (
            rows[-1] if len(rows) == 1 else None
        )
        if start_row is not None and end_row is not None:
            start = start_row.get("wall_time") or start_row.get(
                "timestamp"
            )
            end = end_row.get("wall_time") or end_row.get(
                "timestamp"
            )
        elif (
            terminal is not None
            and terminal.get("duration_ns") is not None
        ):
            end = terminal.get("wall_time") or terminal.get(
                "timestamp"
            )
            start = (
                end - terminal["duration_ns"] / 1e9
                if end is not None
                else None
            )
        else:
            continue
        if (
            not isinstance(start, (int, float))
            or not isinstance(end, (int, float))
            or end < start
        ):
            continue
        spans.append(
            {
                "span_id": span_id,
                "parent_span_id": (
                    end_row or start_row or terminal
                ).get("parent_span_id"),
                "service": (
                    end_row or start_row or terminal
                ).get("service"),
                "name": (end_row or start_row or terminal).get(
                    "name"
                ),
                "probe_start": (
                    start_row.get("probe") if start_row else None
                ),
                "probe_end": (
                    end_row.get("probe") if end_row else None
                ),
                "start_timestamp": start,
                "end_timestamp": end,
                "inclusive_duration_ms": round(
                    (end - start) * 1000, 3
                ),
                "timing_source": (
                    "paired_event_timestamps"
                    if start_row and end_row
                    else "recorded_duration_ns_and_end_timestamp"
                ),
            }
        )

    for span in spans:
        children = [
            child
            for child in spans
            if child.get("parent_span_id") == span["span_id"]
        ]
        intervals = sorted(
            (
                max(
                    span["start_timestamp"],
                    child["start_timestamp"],
                ),
                min(
                    span["end_timestamp"], child["end_timestamp"]
                ),
            )
            for child in children
            if child["start_timestamp"] < span["end_timestamp"]
            and child["end_timestamp"] > span["start_timestamp"]
        )
        covered = 0.0
        if intervals:
            left, right = intervals[0]
            for next_left, next_right in intervals[1:]:
                if next_left <= right:
                    right = max(right, next_right)
                else:
                    covered += right - left
                    left, right = next_left, next_right
            covered += right - left
        span["self_time_ms"] = round(
            max(
                0.0,
                (
                    span["end_timestamp"]
                    - span["start_timestamp"]
                    - covered
                )
                * 1000,
            ),
            3,
        )
        span["self_time_scope"] = (
            "observed exclusive interval after subtracting returned direct-child intervals; unobserved child work may remain"
        )
        span["self_time_child_count"] = sum(
            child["start_timestamp"] < span["end_timestamp"]
            and child["end_timestamp"] > span["start_timestamp"]
            for child in children
        )

    siblings: Dict[Optional[str], List[Dict[str, Any]]] = {}
    for span in spans:
        parent_id = span.get("parent_span_id")
        siblings.setdefault(parent_id, []).append(span)
    overlaps = []
    for parent_id, rows in siblings.items():
        rows.sort(key=lambda item: item["start_timestamp"])
        for index, left in enumerate(rows):
            for right in rows[index + 1 :]:
                overlap = min(
                    left["end_timestamp"], right["end_timestamp"]
                ) - max(
                    left["start_timestamp"],
                    right["start_timestamp"],
                )
                if overlap > 0:
                    overlaps.append(
                        {
                            "parent_span_id": parent_id,
                            "relationship": (
                                "shared_parent"
                                if parent_id is not None
                                else "trace_cooccurrence"
                            ),
                            "span_ids": [
                                left["span_id"],
                                right["span_id"],
                            ],
                            "overlap_ms": round(
                                overlap * 1000, 3
                            ),
                        }
                    )
    request_starts = [
        row
        for row in events
        if row.get("probe") == "request.entry"
    ]
    request_ends = [
        row
        for row in events
        if row.get("probe")
        in {
            "request.exit",
            "request.error",
            "request.exception",
            "django.exception",
        }
    ]
    events_may_be_truncated = len(events) >= 1000
    request_duration = None
    if (
        not events_may_be_truncated
        and len(request_starts) == 1
        and len(request_ends) == 1
    ):
        start = request_starts[0].get(
            "wall_time"
        ) or request_starts[0].get("timestamp")
        end = request_ends[-1].get("wall_time") or request_ends[
            -1
        ].get("timestamp")
        if (
            isinstance(start, (int, float))
            and isinstance(end, (int, float))
            and end >= start
        ):
            request_duration = round((end - start) * 1000, 3)
    complete_roots = [
        span
        for span in spans
        if span.get("parent_span_id") is None
        and span.get("probe_start")
        and span.get("probe_end")
    ]
    trace_wall_duration = request_duration
    trace_wall_source = (
        "request_entry_exit_boundaries"
        if request_duration is not None
        else None
    )
    if (
        trace_wall_duration is None
        and not events_may_be_truncated
        and len(complete_roots) == 1
    ):
        root = complete_roots[0]
        trace_wall_duration = root["inclusive_duration_ms"]
        trace_wall_source = "single_complete_root_span"
    return {
        "spans": spans,
        "request_wall_duration_ms": request_duration,
        "trace_wall_duration_ms": trace_wall_duration,
        "trace_wall_duration_source": trace_wall_source,
        "parallel_overlaps": overlaps,
        "parallel_overlap_scope": "interval overlaps among returned spans in this trace; parentless pairs establish trace co-occurrence only, and sibling intervals may be omitted if events were truncated",
        "events_may_be_truncated": events_may_be_truncated,
        "cumulative_inclusive_duration_ms": round(
            sum(span["inclusive_duration_ms"] for span in spans),
            3,
        ),
        "cumulative_scope": (
            "sum of observed span inclusive intervals; includes nested work and may include overlapping spans"
        ),
        "interval_basis": "event wall timestamps and recorded duration_ns; only spans with explicit paired identity or duration are included",
    }


def _trace_completeness(
    trace_id: str,
    events: List[Dict[str, Any]],
    timing: Dict[str, Any],
    runtime: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Summarize observed lifecycle and capture evidence for one trace.

    Missing boundaries are reported as missing observations. This function
    does not infer request failure, process death, or event loss from absence.

    Parameters
    ----------
    trace_id : str
        Trace identifier requested by the caller.
    events : list of dict[str, Any]
        Stored event rows returned for the trace.
    timing : dict[str, Any]
        Derived timing metadata for the returned event sample.
    runtime : dict[str, Any] or None
        Latest heartbeat for the selected worker instance, if available.

    Returns
    -------
    dict[str, Any]
        Bounded lifecycle, boundary, heartbeat, and capture-loss evidence.
    """
    probes = [str(event.get("probe") or "") for event in events]
    request_entry = any(
        probe == "request.entry" for probe in probes
    )
    request_exit = any(
        probe == "request.exit" for probe in probes
    )
    request_error_probes = sorted(
        {
            probe
            for probe in probes
            if probe
            in {
                "request.error",
                "request.exception",
                "django.exception",
            }
        }
    )
    activity_terminal_probes = sorted(
        {
            probe
            for probe in probes
            if probe.endswith(
                (".exit", ".end", ".error", ".exception")
            )
        }
    )

    runtime_status = (runtime or {}).get("status") or {}
    lifecycle_snapshot = (
        runtime_status.get("trace_lifecycle") or {}
    )
    lifecycle_record = None
    for collection in ("active", "recent"):
        rows = lifecycle_snapshot.get(collection, [])
        if not isinstance(rows, list):
            continue
        lifecycle_record = next(
            (
                row
                for row in reversed(rows)
                if isinstance(row, dict)
                and str(row.get("trace_id")) == trace_id
            ),
            None,
        )
        if lifecycle_record:
            break

    event_capture = runtime_status.get("event_capture") or {}
    loss_sources: Dict[str, int] = {}
    worker_wide_loss: Dict[str, int] = {}
    trace_counts_truncated = False
    for group_name, group in event_capture.items():
        if not isinstance(group, dict):
            continue
        trace_counts_truncated = trace_counts_truncated or bool(
            group.get("trace_counts_truncated")
        )
        for field, value in group.items():
            if field.endswith("_by_trace") and isinstance(
                value, dict
            ):
                try:
                    count = int(value.get(trace_id, 0))
                except (TypeError, ValueError):
                    count = 0
                if count > 0:
                    loss_sources[f"{group_name}.{field}"] = count
            elif field.endswith("_total"):
                try:
                    count = int(value)
                except (TypeError, ValueError):
                    continue
                if count > 0:
                    worker_wide_loss[f"{group_name}.{field}"] = (
                        count
                    )

    dropped_events = sum(loss_sources.values())
    heartbeat = _worker_runtime_view(runtime)
    heartbeat_fresh = heartbeat.get("online") is True
    if request_exit:
        status = "request_completed"
        status_basis = "request.exit event observed"
    elif request_error_probes:
        status = "request_error_observed"
        status_basis = "request error event observed"
    elif lifecycle_record:
        state = lifecycle_record.get("state")
        if state == "active":
            status = (
                "active_at_last_heartbeat"
                if heartbeat_fresh
                else "was_active_at_last_heartbeat"
            )
            status_basis = (
                "trace-scoped active-request tracker record"
            )
        elif state == "completed":
            status = "tracker_recorded_completion"
            status_basis = (
                "trace-scoped tracker lifecycle record"
            )
        elif state == "failed":
            status = "tracker_recorded_failure"
            status_basis = (
                "trace-scoped tracker lifecycle record"
            )
        elif state == "stale":
            status = "tracker_evicted_after_inactivity"
            status_basis = (
                "tracker TTL eviction; request outcome unknown"
            )
        elif state == "evicted":
            status = "tracker_evicted"
            status_basis = str(
                lifecycle_record.get("reason")
                or "tracker eviction"
            )
        else:
            status = "lifecycle_recorded"
            status_basis = (
                "trace-scoped tracker lifecycle record"
            )
    elif not events and dropped_events > 0:
        status = "capture_loss_observed_no_stored_events"
        status_basis = (
            "trace-correlated capture loss was recorded and no event rows "
            "were returned; this does not establish that every event was lost"
        )
    elif (
        timing.get("trace_wall_duration_source")
        == "single_complete_root_span"
    ):
        status = "complete_root_span_observed"
        status_basis = "one complete root span observed"
    elif activity_terminal_probes:
        status = "terminal_activity_observed"
        status_basis = "terminal span event(s) observed; request outcome not inferred"
    elif not events:
        status = "no_events_returned"
        status_basis = (
            "no stored event rows were returned; capture loss and request "
            "outcome are unknown without matching lifecycle evidence"
        )
    else:
        status = "completion_unknown"
        status_basis = (
            "no request completion boundary or matching lifecycle record "
            "observed"
        )

    return {
        "status": status,
        "status_basis": status_basis,
        "request_entry_observed": request_entry,
        "request_exit_observed": request_exit,
        "request_error_observed": bool(request_error_probes),
        "request_terminal_observed": (
            request_exit or bool(request_error_probes)
        ),
        "request_error_probes": request_error_probes,
        "terminal_activity_probes": activity_terminal_probes,
        "events_returned": len(events),
        "events_may_be_truncated": bool(
            timing.get("events_may_be_truncated")
        ),
        "lifecycle_record": lifecycle_record,
        "lifecycle_heartbeat": {
            "observed_at": (runtime or {}).get("observed_at"),
            "fresh": heartbeat_fresh,
            "heartbeat_age_s": heartbeat.get("heartbeat_age_s"),
        },
        "capture_loss": {
            "dropped_events": dropped_events,
            "sources": loss_sources,
            "worker_wide_loss_totals": worker_wide_loss,
            "trace_counts_truncated": trace_counts_truncated,
            "per_trace_loss_observed": dropped_events > 0,
        },
    }


@app.get("/api/v1/traces/{trace_id}")
async def get_trace(
    trace_id: str,
    worker_instance_id: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict:
    """
    Reconstruct the critical path for a trace from the event store.
    Requires raw events to have been persisted via POST /api/v1/events.
    """
    from origintracer.core.event_schema import ProbeTypes

    customer_id = _authenticate(authorization)
    selected_pid = None
    if not worker_instance_id:
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
        worker_instance_id=(
            _require_instance_id(worker_instance_id)
            if worker_instance_id
            else None
        ),
        trace_id=trace_id,
        limit=1000,
    )

    # repository returns dicts, sort by timestamp
    events.sort(
        key=lambda e: e.get("timestamp") or e.get("wall_time", 0)
    )
    registered = ProbeTypes.all()
    unregistered_probe_types = sorted(
        {
            str(event["probe"])
            for event in events
            if event.get("probe")
            and event.get("probe") not in registered
        }
    )
    missing_probe_name_count = sum(
        not event.get("probe") for event in events
    )
    runtime = (
        repository.get_worker_runtime(
            customer_id, worker_instance_id
        )
        if worker_instance_id and repository is not None
        else None
    )
    if not events:
        empty_completeness = _trace_completeness(
            trace_id,
            events,
            {"events_may_be_truncated": False},
            runtime,
        )
        has_trace_diagnostics = empty_completeness.get(
            "lifecycle_record"
        ) is not None or empty_completeness.get(
            "capture_loss", {}
        ).get(
            "per_trace_loss_observed"
        )
        if not has_trace_diagnostics:
            raise HTTPException(
                status_code=404,
                detail=f"No stored events found for trace '{trace_id}'",
            )

    warnings = []
    if not events:
        warnings.append(
            {
                "code": "no_stored_events",
                "message": (
                    "No stored event rows were returned for this trace "
                    "and worker instance. Lifecycle and capture evidence "
                    "are reported separately when available."
                ),
            }
        )
    if unregistered_probe_types:
        warnings.append(
            {
                "code": "unregistered_probe_types",
                "message": (
                    "Events with unregistered probe types are included "
                    "using their recorded names."
                ),
                "probe_types": unregistered_probe_types,
            }
        )
        logger.warning(
            "Trace %s includes unregistered probe types for worker %s: %s",
            trace_id,
            worker_instance_id or selected_pid or "unspecified",
            ", ".join(unregistered_probe_types),
        )
    if missing_probe_name_count:
        warnings.append(
            {
                "code": "missing_probe_name",
                "message": (
                    "Some stored events have no probe name; their events "
                    "are included with a null probe value."
                ),
                "event_count": missing_probe_name_count,
            }
        )

    path = []
    last_ts = None
    event_duration_sum_ms = 0.0
    for event in events:
        ts = event.get("timestamp") or event.get("wall_time")
        inter_event_gap_ms = (
            round((ts - last_ts) * 1000, 3)
            if last_ts is not None and ts is not None
            else None
        )
        duration_ns = event.get("duration_ns")
        recorded_duration_ms = (
            round(duration_ns / 1e6, 3)
            if duration_ns is not None
            else None
        )
        if recorded_duration_ms is not None:
            event_duration_sum_ms += recorded_duration_ms
        path.append(
            {
                "probe": event.get("probe"),
                "service": event.get("service"),
                "name": event.get("name"),
                "trace_id": event.get("trace_id"),
                "span_id": event.get("span_id"),
                "parent_span_id": event.get("parent_span_id"),
                "timestamp": ts,
                "wall_time": event.get("wall_time"),
                "duration_ns": duration_ns,
                "recorded_duration_ms": recorded_duration_ms,
                "inter_event_gap_ms": inter_event_gap_ms,
                "duration_ms": inter_event_gap_ms,
                "pid": event.get("pid"),
                "tid": event.get("tid"),
                "metadata": _ui_observation_metadata(
                    event.get("metadata") or {}
                ),
            }
        )
        last_ts = ts
    timing = _derive_trace_spans(events)
    trace_completeness = _trace_completeness(
        trace_id, events, timing, runtime
    )
    return {
        "data": path,
        "total_ms": round(event_duration_sum_ms, 3),
        "event_duration_sum_ms": round(event_duration_sum_ms, 3),
        "timing": timing,
        "trace_completeness": trace_completeness,
        "warnings": warnings,
    }


@app.get("/api/v1/traces/{trace_id}/stitch")
def stitch_stored_trace(
    trace_id: str,
    authorization: Optional[str] = Header(None),
    repository: InMemoryRepository = Depends(get_repository),
) -> Dict[str, Any]:
    """Merge uploaded event rows across workers without contacting their sockets."""
    customer_id = _authenticate(authorization)
    if not re.fullmatch(r"[A-Za-z0-9._:/-]{1,128}", trace_id):
        raise HTTPException(
            status_code=400,
            detail="trace_id contains unsupported characters",
        )

    if repository is None:
        raise HTTPException(
            status_code=503,
            detail="No storage backend configured - cannot retrieve traces.",
        )
    events = repository.query_events(
        customer_id=customer_id,
        trace_id=trace_id,
        limit=5000,
    )
    events.sort(key=lambda event: event.get("wall_time", 0))
    workers_found = sorted(
        {
            str(
                (event.get("metadata") or {}).get("worker_pid")
                or event.get("pid")
            )
            for event in events
            if (event.get("metadata") or {}).get("worker_pid")
            or event.get("pid")
        }
    )
    stages = [
        {
            "probe": event.get("probe"),
            "service": event.get("service"),
            "name": event.get("name"),
            "timestamp": event.get("wall_time"),
            "span_id": event.get("span_id"),
            "parent_span_id": event.get("parent_span_id"),
            "pid": event.get("pid"),
            "tid": event.get("tid"),
            "duration_ns": event.get("duration_ns"),
            "worker_instance_id": (
                event.get("metadata") or {}
            ).get("worker_instance_id"),
            "metadata": _ui_observation_metadata(
                event.get("metadata") or {}
            ),
            "duration_ms": (
                round(event["duration_ns"] / 1e6, 3)
                if event.get("duration_ns") is not None
                else None
            ),
            "worker_pid": str(
                (event.get("metadata") or {}).get("worker_pid")
                or event.get("pid")
                or "unknown"
            ),
        }
        for event in events
    ]
    return {
        "trace_id": trace_id,
        "source": "OriginTracer stitched stored event trace",
        "workers_searched": workers_found,
        "workers_found": workers_found,
        "stages": stages,
        "errors": [],
        "interpretation_limits": [
            "Only uploaded events are included; missing or delayed uploads can omit stages.",
            "duration_ms is the event's recorded duration when available; no cross-worker end-to-end duration is computed.",
        ],
    }


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
    """Return graph availability and runtime freshness for one process instance.

    The response combines the selected instance's in-memory graph metadata with its latest
    stored heartbeat. ``online`` reflects heartbeat freshness rather than an
    operating-system process check, so a missing or stale heartbeat cannot establish current
    process health or activity.

    Parameters
    ----------
    worker_instance_id : str
        Stable process-instance ID to inspect.
    authorization : str, optional
        Customer authentication credential from the request header.
    repository : InMemoryRepository
        Persistence layer used to load runtime status.

    Returns
    -------
    Dict
        Customer, snapshot, and runtime metadata, storage type, and response time.

    Raises
    ------
    HTTPException
        If authentication fails or the instance ID is invalid.
    """
    customer_id = _authenticate(authorization)
    worker_instance_id = _require_instance_id(worker_instance_id)
    graph = get_graph(customer_id, worker_instance_id)
    runtime = (
        repository.get_worker_runtime(
            customer_id, worker_instance_id
        )
        if repository is not None
        else None
    )
    runtime_view = _worker_runtime_view(runtime)

    snapshot_info: Dict[str, Any] = {"available": False}
    snapshot_info.update(runtime_view)
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
            **runtime_view,
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
                "metadata": _ui_observation_metadata(
                    node.metadata
                ),
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
    repository: Any = Depends(get_repository),
) -> Dict[str, Any]:
    """List stored process snapshots with their heartbeat freshness.

    The endpoint returns one row for each worker instance that uploaded a graph snapshot.
    Rows include snapshot receipt time and graph size, plus runtime status and an ``online``
    freshness signal when a heartbeat is available. Stale snapshots remain available for
    inspection, but clients must not count them as currently reporting workers.

    Parameters
    ----------
    authorization : str, optional
        Customer authentication credential from the request header.
    repository : Any
        Persistence layer used to load runtime heartbeats.

    Returns
    -------
    Dict[str, Any]
        Snapshot rows for the authenticated customer, ordered by newest snapshot.
    """
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
    runtime_rows = (
        repository.list_worker_runtime(customer_id)
        if repository is not None
        else []
    )
    runtime_by_instance = {
        row.get("worker_instance_id"): row
        for row in runtime_rows
        if isinstance(row, dict)
    }
    for row in rows:
        runtime = runtime_by_instance.get(
            row["worker_instance_id"]
        )
        row.update(_worker_runtime_view(runtime))
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
    repository: Any = Depends(get_repository),
) -> Dict[str, Any]:
    """Return stored graph content and metadata for one process instance.

    Requiring an instance ID prevents callers from mixing snapshots from process lifetimes
    that reused the same PID. The response includes graph nodes and edges, snapshot
    metadata, and the latest heartbeat-derived runtime status. A stale heartbeat does not
    prevent access to the saved graph.

    Parameters
    ----------
    worker_instance_id : str
        Stable process-instance ID to retrieve.
    authorization : str, optional
        Customer authentication credential from the request header.
    repository : Any
        Persistence layer used to load runtime status.

    Returns
    -------
    Dict[str, Any]
        Selected instance ID, snapshot metadata, runtime freshness, and serialized
        node and edge rows.

    Raises
    ------
    HTTPException
        If authentication fails, the ID is invalid, or no graph snapshot exists
        for that customer and process instance.
    """
    customer_id = _authenticate(authorization)
    instance_id = _require_instance_id(worker_instance_id)
    graph = require_graph(customer_id, instance_id)
    with _graphs_lock:
        metadata = dict(
            _snapshot_metadata.get(
                (customer_id, instance_id), {}
            )
        )
    runtime = (
        repository.get_worker_runtime(customer_id, instance_id)
        if repository is not None
        else None
    )
    return {
        "worker_instance_id": instance_id,
        **metadata,
        **_worker_runtime_view(runtime),
        "data": {
            "nodes": [_node_dict(n) for n in graph.all_nodes()],
            "edges": [_edge_dict(e) for e in graph.all_edges()],
        },
    }


_UI_OBSERVATION_METADATA_FIELDS = (
    "probe",
    "customer_id",
    "external",
    "dependency_host",
    "http_method",
    "method",
    "http_version",
    "status_code",
    "error_type",
    "duration_scope",
    "retry_count",
    "max_retry_count",
    "upstream_attempt_count",
    "upstream_duration_ns",
    "source",
    "task_id",
    "task_name",
    "state",
    "retries",
    "max_retries",
    "exception_type",
    "worker_pid",
    "master_pid",
    "worker_class",
    "exitcode",
    "response_bytes",
    "reason",
    "success",
    "result_type",
    "command_count",
    "num_workers",
    "bind",
    "investigation_id",
    "langgraph_kind",
    "is_langgraph_root",
    "langgraph_node",
    "langgraph_step",
    "langgraph_triggers",
    "langgraph_path",
    "langgraph_checkpoint_ns",
)
_UI_SAFE_LIFECYCLE_REASONS = {
    "normal_exit",
    "SIGINT",
    "SIGABRT_timeout",
}


def _ui_observation_metadata(metadata: Any) -> Dict[str, Any]:
    """Return useful probe details while excluding payloads and secrets.

    Event metadata can contain application values, request headers, SQL
    parameters, exception text, or model content. The UI receives only this
    explicit operational allowlist; new metadata fields stay private until
    they are reviewed for safe display.
    """
    if not isinstance(metadata, dict):
        return {}
    safe: Dict[str, Any] = {}
    for key in _UI_OBSERVATION_METADATA_FIELDS:
        value = metadata.get(key)
        if (
            key == "reason"
            and value not in _UI_SAFE_LIFECYCLE_REASONS
        ):
            continue
        if isinstance(value, bool) or value is None:
            if value is not None:
                safe[key] = value
        elif isinstance(value, (int, float)):
            safe[key] = value
        elif isinstance(value, str):
            safe[key] = value[:240]
        elif key in {
            "langgraph_triggers",
            "langgraph_path",
        } and isinstance(value, (list, tuple)):
            safe[key] = [
                item[:120] if isinstance(item, str) else item
                for item in value[:12]
                if isinstance(item, (str, int, float, bool))
            ]
    return safe


# Available in parser and repeated for backend standalone
def _node_dict(n) -> Dict:
    """Full graph node representation with reviewed operational metadata."""
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
        "metadata": _ui_observation_metadata(n.metadata),
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

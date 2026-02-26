"""
backend/main.py

FastAPI service with two surfaces:
  1. Ingest API  (/api/v1/ingest)     — receives probe events from agents
  2. Query API   (/api/v1/query)      — executes DSL queries over the model
  3. Graph API   (/api/v1/graph)      — returns current graph state for UI
  4. Causal API  (/api/v1/causal)     — evaluates causal patterns
  5. Health      (/health)            — load balancer / k8s liveness

Run with:
    uvicorn stacktracer.backend.main:app --host 0.0.0.0 --port 8000

Environment variables:
    STACKTRACER_SECRET_KEY   — shared HMAC secret for API key validation
    STACKTRACER_DB_DSN       — PostgreSQL DSN (optional)
    STACKTRACER_CH_HOST      — ClickHouse host (optional)
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..core.engine import Engine
from ..core.event_schema import NormalizedEvent
from ..core.causal import build_default_registry
from ..query.parser import parse as parse_query, ParsedQuery
from ..query.parser import execute as execute_query

logger = logging.getLogger("stacktracer.backend")

app = FastAPI(
    title="StackTracer API",
    description="Runtime world model for Python services",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------ #
# Application state (in-process for MVP; scale to Redis later)
# ------------------------------------------------------------------ #

_engine: Optional[Engine] = None
_valid_api_keys: Dict[str, str] = {}    # api_key → customer_id


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = Engine(causal_registry=build_default_registry())
        _engine.start_background_tasks()
    return _engine


def _load_api_keys() -> None:
    global _valid_api_keys
    # In production: load from database or environment
    raw = os.getenv("STACKTRACER_API_KEYS", "")
    for pair in raw.split(","):
        if ":" in pair:
            key, customer = pair.split(":", 1)
            _valid_api_keys[key.strip()] = customer.strip()
    if not _valid_api_keys:
        # Development fallback
        _valid_api_keys["dev-key-00000000"] = "dev_customer"
        logger.warning("No API keys configured — dev-key-00000000 accepted")


_load_api_keys()


# ------------------------------------------------------------------ #
# Auth middleware
# ------------------------------------------------------------------ #

def _authenticate(authorization: Optional[str]) -> str:
    """Validate Bearer token and return customer_id."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    api_key = authorization[7:]
    customer_id = _valid_api_keys.get(api_key)
    if not customer_id:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return customer_id


# ------------------------------------------------------------------ #
# Pydantic schemas
# ------------------------------------------------------------------ #

class IngestPayload(BaseModel):
    events: List[Dict[str, Any]]


class QueryRequest(BaseModel):
    query: str


class DeploymentMarkRequest(BaseModel):
    label: str = "deployment"


# ------------------------------------------------------------------ #
# Routes: Ingest
# ------------------------------------------------------------------ #

@app.post("/api/v1/ingest")
async def ingest(
    payload: IngestPayload,
    authorization: Optional[str] = Header(None),
) -> Dict:
    customer_id = _authenticate(authorization)
    engine = get_engine()

    accepted = 0
    errors = 0
    for raw in payload.events:
        try:
            raw["metadata"] = raw.get("metadata") or {}
            raw["metadata"]["customer_id"] = customer_id
            event = NormalizedEvent.from_dict(raw)
            engine.process(event)
            accepted += 1
        except Exception as exc:
            errors += 1
            logger.debug("Ingest parse error: %s | raw=%s", exc, raw)

    return {
        "status": "ok",
        "accepted": accepted,
        "errors": errors,
    }


# ------------------------------------------------------------------ #
# Routes: Query
# ------------------------------------------------------------------ #

@app.post("/api/v1/query")
async def query(
    request: QueryRequest,
    authorization: Optional[str] = Header(None),
) -> Dict:
    _authenticate(authorization)
    engine = get_engine()

    try:
        parsed = parse_query(request.query)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Query parse error: {exc}")

    result = execute_query(parsed, engine)
    return result


@app.get("/api/v1/graph")
async def get_graph(
    service: Optional[str] = None,
    system: Optional[str] = None,
    authorization: Optional[str] = Header(None),
) -> Dict:
    _authenticate(authorization)
    engine = get_engine()

    from ..query.parser import ParsedQuery
    if system:
        q = ParsedQuery(verb="SHOW", metric="graph", filters={"system": system})
    elif service:
        q = ParsedQuery(verb="SHOW", metric="graph", filters={"service": service})
    else:
        q = ParsedQuery(verb="SHOW", metric="graph")

    return execute_query(q, engine)


@app.get("/api/v1/traces/{trace_id}")
async def get_trace(
    trace_id: str,
    authorization: Optional[str] = Header(None),
) -> Dict:
    _authenticate(authorization)
    engine = get_engine()
    path = engine.critical_path(trace_id)
    if not path:
        raise HTTPException(status_code=404, detail=f"Trace '{trace_id}' not found")
    return {
        "trace_id": trace_id,
        "stages": len(path),
        "total_ms": sum(s.get("duration_ms") or 0 for s in path),
        "path": path,
    }


@app.get("/api/v1/causal")
async def causal(
    tags: Optional[str] = None,
    authorization: Optional[str] = Header(None),
) -> Dict:
    _authenticate(authorization)
    engine = get_engine()
    tag_list = [t.strip() for t in tags.split(",")] if tags else None
    matches = engine.evaluate(tags=tag_list)
    return {
        "match_count": len(matches),
        "matches": [m.to_dict() for m in matches],
    }


@app.get("/api/v1/hotspots")
async def hotspots(
    top: int = 10,
    authorization: Optional[str] = Header(None),
) -> Dict:
    _authenticate(authorization)
    return {"data": get_engine().hotspots(top_n=top)}


@app.post("/api/v1/deployment")
async def mark_deployment(
    body: DeploymentMarkRequest,
    authorization: Optional[str] = Header(None),
) -> Dict:
    _authenticate(authorization)
    get_engine().mark_deployment(body.label)
    return {"status": "ok", "label": body.label}


@app.get("/api/v1/diff")
async def diff(
    since: Optional[str] = None,
    authorization: Optional[str] = Header(None),
) -> Dict:
    _authenticate(authorization)
    engine = get_engine()
    q = ParsedQuery(verb="DIFF", metric="edges", filters={"since": since} if since else {})
    from ..query.parser import _exec_diff
    return _exec_diff(q, engine)


# ------------------------------------------------------------------ #
# Routes: Status
# ------------------------------------------------------------------ #

@app.get("/api/v1/status")
async def status(authorization: Optional[str] = Header(None)) -> Dict:
    _authenticate(authorization)
    return get_engine().status()


@app.get("/health")
async def health() -> Dict:
    return {"status": "healthy", "timestamp": time.time()}


# ------------------------------------------------------------------ #
# Error handling
# ------------------------------------------------------------------ #

@app.exception_handler(Exception)
async def generic_error(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled error: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)},
    )
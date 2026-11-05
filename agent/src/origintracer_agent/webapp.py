"""Initialize OriginTracer in the LangGraph Agent Server process."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import origintracer
from fastapi import FastAPI

from origintracer_agent.config import (
    backend_api_key,
    backend_url,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _PROJECT_ROOT / "origintracer.yaml"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start tracing and record the deployment baseline for this process.

    The explicit baseline lets the React dashboard compare this Agent Server
    worker's stored graph against its deployment start, even when the graph
    belongs to an otherwise idle worker.
    """
    origintracer.init(
        api_key=backend_api_key(),
        endpoint=backend_url(),
        config=str(_CONFIG_PATH),
        debug=True,
    )
    # React's graph diff view uses this baseline for every worker instance.
    # Mark it at process startup, as the Django integration does, so the
    # selected Agent Server worker can answer `since=deployment` requests.
    origintracer.mark_deployment("deployment")
    try:
        yield
    finally:
        origintracer.shutdown()


app = FastAPI(lifespan=lifespan)

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
    """Start the LangGraph probe for callbacks handled by this server."""
    origintracer.init(
        api_key=backend_api_key(),
        endpoint=backend_url(),
        config=str(_CONFIG_PATH),
        debug=True,
    )
    try:
        yield
    finally:
        origintracer.shutdown()


app = FastAPI(lifespan=lifespan)

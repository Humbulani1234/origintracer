"""FastAPI lifecycle integration for OriginTracer inside Agent Server.

Agent Server mounts this application alongside its built-in assistants, threads,
and runs API. The lifespan is the process boundary at which OriginTracer starts:
every Agent Server process therefore owns its own runtime graph and Unix socket.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI

import origintracer

_APP_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _APP_ROOT / "origintracer.yaml"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Start OriginTracer with the server process and release it
    on shutdown.
    """
    origintracer.init(
        config=str(_CONFIG_PATH),
        endpoint=os.getenv(
            "ORIGINTRACER_ENDPOINT", "http://localhost:8001"
        ),
    )
    origintracer.mark_deployment("deployment")
    try:
        yield
    finally:
        origintracer.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/origintracer-example")
def example_info() -> dict[str, str]:
    """
    Identify the custom application mounted into Agent Server.
    """
    return {
        "application": "OriginTracer LangGraph example",
        "graph": "personal_assistant",
        "observability": "origintracer",
    }

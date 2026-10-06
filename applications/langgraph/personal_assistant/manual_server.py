"""Minimal host-native FastAPI server for experimenting with the example graphs.

This module is intentionally not a replacement for LangGraph Agent Server. It
implements only the stateless ``POST /runs/wait`` request used by the example's
invocation scripts. Running it directly with Uvicorn keeps OriginTracer, Ollama,
the backend, and the React worker selector in the host PID namespace.
"""

from __future__ import annotations

import asyncio
import os
import sys
from asyncio import futures, tasks
from contextlib import asynccontextmanager
from contextvars import copy_context
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from personal_assistant.graph import supervisor_agent
from personal_assistant.scenarios import (
    agent_latency_hotspot_scenario,
    loop_runaway_scenario,
)
from personal_assistant.webapp import (
    lifespan as origintracer_lifespan,
)


class RunRequest(BaseModel):
    """Subset of an Agent Server stateless-run request used by this example."""

    assistant_id: str
    input: dict[str, Any] = Field(default_factory=dict)


_GRAPHS = {
    "personal_assistant": supervisor_agent,
    "loop_runaway_scenario": loop_runaway_scenario,
    "agent_latency_hotspot_scenario": agent_latency_hotspot_scenario,
}


def _max_jobs_per_worker() -> int:
    """Read the manual server's per-process graph concurrency limit."""
    raw_value = os.getenv("N_JOBS_PER_WORKER", "1")
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            "N_JOBS_PER_WORKER must be a positive integer"
        ) from exc
    if value < 1:
        raise RuntimeError(
            "N_JOBS_PER_WORKER must be a positive integer"
        )
    return value


# import pdb
# pdb.set_trace()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialize the concurrency gate and OriginTracer for this process."""
    app.state.run_slots = asyncio.Semaphore(
        _max_jobs_per_worker()
    )

    # import pdb
    # pdb.set_trace()

    async with origintracer_lifespan(app):
        # Uvicorn runs lifespan and HTTP requests in separate task contexts.
        # Preserve the context in which OriginTracer installed its LangGraph
        # callback so each manual-server run can inherit it below.
        app.state.origintracer_context = copy_context()
        yield


app = FastAPI(
    title="OriginTracer LangGraph experiment server",
    description=(
        "A host-native debugging adapter for the graphs declared in "
        "langgraph.json."
    ),
    lifespan=lifespan,
)


@app.post("/runs/wait")
async def run_and_wait(run: RunRequest, request: Request) -> Any:
    """Invoke one known graph when this worker has an execution slot."""
    graph = _GRAPHS.get(run.assistant_id)
    if graph is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown graph: {run.assistant_id}",
        )
    async with request.app.state.run_slots:
        run_context = (
            request.app.state.origintracer_context.copy()
        )
        run_task = asyncio.create_task(
            graph.ainvoke(run.input),
            context=run_context,
        )
        return await run_task

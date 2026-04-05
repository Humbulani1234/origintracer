"""
context/vars.py

ContextVar-based trace propagation.
Works correctly across asyncio coroutines, threads, and task boundaries.

Every probe reads current_trace_id to know which request it's in.
Django middleware sets and resets it per request.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Optional

# The single source of truth for "which request are we in?"
current_trace_id: ContextVar[Optional[str]] = ContextVar(
    "stacktracer_trace_id", default=None
)

# The span ID of the most recently emitted event — used to build parent_span chains
current_span_id: ContextVar[Optional[str]] = ContextVar(
    "stacktracer_span_id", default=None
)


def set_trace(
    trace_id: str, span_id: Optional[str] = None
) -> Token:
    """
    Set the current trace context. Returns a Token for reset().
    Use inside a try/finally to guarantee cleanup:

        token = set_trace(trace_id)
        try:
            ...
        finally:
            reset_trace(token)
    """
    token = current_trace_id.set(trace_id)
    if span_id:
        current_span_id.set(span_id)
    return token


def reset_trace(token: Token) -> None:
    current_trace_id.reset(token)
    # Reset span to default too
    current_span_id.set(None)


def get_trace_id() -> Optional[str]:
    return current_trace_id.get()


def get_span_id() -> Optional[str]:
    return current_span_id.get()

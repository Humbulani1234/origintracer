"""ContextVar-based trace and investigation grouping propagation.

Context values flow across asyncio tasks and callback boundaries. Probes use
``current_trace_id`` for one execution and ``current_investigation_id`` to
group related agent executions. Django middleware sets and resets trace IDs;
the agent sets and resets investigation IDs around graph nodes.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Optional

# The single source of truth for "which request are we in?"
current_trace_id: ContextVar[Optional[str]] = ContextVar(
    "origintracer_trace_id", default=None
)

# The span ID of the most recently emitted event used to
# build parent_span chains
current_span_id: ContextVar[Optional[str]] = ContextVar(
    "origintracer_span_id", default=None
)

# Groups multiple trace roots that belong to the same agent investigation.
# This is distinct from trace_id: one investigation can span several runs.
current_investigation_id: ContextVar[Optional[str]] = ContextVar(
    "origintracer_investigation_id", default=None
)


def set_trace(
    trace_id: str, span_id: Optional[str] = None
) -> Token:
    """
    Set the current trace context. Returns a Token for reset().

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


def set_investigation_id(investigation_id: str) -> Token:
    """Set the active investigation grouping context and return its token."""
    return current_investigation_id.set(investigation_id)


def reset_investigation_id(token: Token) -> None:
    """Restore the previous investigation grouping context."""
    current_investigation_id.reset(token)


def get_investigation_id() -> Optional[str]:
    """Return the active investigation grouping identifier, if any."""
    return current_investigation_id.get()

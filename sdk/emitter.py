"""
sdk/emitter.py

The emitter is the sole interface between probes and the Engine.
Probes never import Engine directly — they call emit().

This indirection lets the Engine be swapped, mocked in tests,
or run remotely without changing any probe code.

Architecture:
    Probe → emit(event) → EventBuffer → Engine.process(event)

The buffer absorbs bursts and decouples probe latency from Engine latency.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import List, Optional

from ..core.event_schema import NormalizedEvent

logger = logging.getLogger("stacktracer.emitter")

# ------------------------------------------------------------------ #
# In-process event buffer (absorbs micro-bursts)
# ------------------------------------------------------------------ #

class _EventBuffer:
    """
    Lock-free-ish bounded ring buffer for in-process use.
    Events are drained by the Engine on each call to process().
    For MVP: direct call-through. For production: batch drain thread.
    """

    def __init__(self, max_size: int = 50_000) -> None:
        self._q: deque[NormalizedEvent] = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._dropped = 0

    def push(self, event: NormalizedEvent) -> None:
        with self._lock:
            if len(self._q) >= self._q.maxlen:  # type: ignore[arg-type]
                self._dropped += 1
                return
            self._q.append(event)

    def drain(self, max_batch: int = 500) -> List[NormalizedEvent]:
        with self._lock:
            batch = []
            for _ in range(min(max_batch, len(self._q))):
                batch.append(self._q.popleft())
            return batch

    @property
    def dropped(self) -> int:
        return self._dropped


# ------------------------------------------------------------------ #
# Module-level state
# ------------------------------------------------------------------ #

_engine = None          # Set by bind_engine()
_buffer = _EventBuffer()
_direct_mode = True     # True = emit directly into Engine (MVP default)
                        # False = buffer + drain thread (high-throughput)


def bind_engine(engine: object) -> None:
    """Call once at startup with the Engine instance."""
    global _engine
    _engine = engine
    logger.info("StackTracer emitter bound to engine: %r", engine)


def emit(event: NormalizedEvent) -> None:
    """
    Emit one probe event.
    This is the ONLY function probes should call.
    """
    if _engine is None:
        return  # Silent drop if not initialised — probes must be safe to import early

    if _direct_mode:
        try:
            _engine.process(event)
        except Exception as exc:
            # NEVER let tracer errors crash the host application
            logger.debug("Engine.process error: %s", exc)
    else:
        _buffer.push(event)


def flush() -> None:
    """Drain buffer into engine (used in non-direct mode)."""
    if _engine is None or _direct_mode:
        return
    for event in _buffer.drain():
        try:
            _engine.process(event)
        except Exception as exc:
            logger.debug("Engine.process error during flush: %s", exc)


def stats() -> dict:
    return {
        "direct_mode": _direct_mode,
        "buffer_dropped": _buffer.dropped,
        "engine_bound": _engine is not None,
    }
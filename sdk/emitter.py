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
from time import time
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


class _DrainThread(threading.Thread):
    """
    Background thread that drains the EventBuffer into the Engine.
    Runs independently of the application thread.
    The application thread only ever calls _buffer.push() — one lock,
    one deque.append(), done. Cost: ~0.5 microseconds per emit().
    """

    def __init__(self, buffer: _EventBuffer, interval_s: float = 0.05) -> None:
        super().__init__(daemon=True, name="stacktracer-drain")
        self._buffer   = buffer
        self._interval = interval_s   # drain every 50ms
        self._running  = False

    def start_draining(self) -> None:
        self._running = True
        self.start()

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        while self._running:
            try:
                events = self._buffer.drain(max_batch=500)
                if events and _engine is not None:
                    for event in events:
                        try:
                            _engine.process(event)
                        except Exception as exc:
                            logger.debug("drain: process error: %s", exc)
            except Exception as exc:
                logger.debug("drain: loop error: %s", exc)
            time.sleep(self._interval)

# ------------------------------------------------------------------ #
# Module-level state
# ------------------------------------------------------------------ #

_engine = None          # Set by bind_engine()
_buffer = _EventBuffer()
_direct_mode = False     # True = emit directly into Engine (MVP default)
                        # False = buffer + drain thread (high-throughput)


_drain_thread: Optional[_DrainThread] = None   # add to module-level state

def bind_engine(engine: object) -> None:
    global _engine, _drain_thread
    _engine = engine
    _drain_thread = _DrainThread(_buffer, interval_s=0.05)
    _drain_thread.start_draining()
    logger.info("StackTracer emitter bound, drain thread started")


def emit(event: NormalizedEvent) -> None:
    """
    Emit one probe event.
    This is the ONLY function probes should call.
    """
    if _engine is None:
        return  # Silent drop if not initialised — probes must be safe to import early
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
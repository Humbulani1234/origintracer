"""
The emitter is the interface between probes and the Engine.
Probes do not import Engine directly - they call emit().

This design allows the Engine be swapped.

Flow:
    Probe >> emit(event) >> EventBuffer >> Engine.process(event)
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import List, Optional

from ..core.engine import Engine
from ..core.event_schema import NormalizedEvent

logger = logging.getLogger("origintracer.emitter")


class _DrainEventBuffer:
    """
    Bounded ring buffer for in-process use.
    Events are drained by the Engine on each call to process().
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

    def drain(
        self, max_batch: int = 500
    ) -> List[NormalizedEvent]:
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
    """

    def __init__(
        self, buffer: _DrainEventBuffer, interval_s: float = 0.05
    ) -> None:
        super().__init__(daemon=True, name="origintracer-drain")
        self._buffer = buffer
        self._interval = interval_s  # drain every 50ms
        self._running = False

    def start_draining(self) -> None:
        self._running = True
        self.start()

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        while self._running:
            try:
                events = self._buffer.drain(max_batch=500)
                if not events:
                    time.sleep(0.001)
                    continue
                if _engine is not None:
                    for event in events:
                        try:
                            _engine.process(event)
                        except Exception as exc:
                            logger.debug(
                                "drain: process error: %s", exc
                            )
            except Exception as exc:
                logger.debug("drain: loop error: %s", exc)


_engine: Optional[Engine] = None  # Set by bind_engine()
_buffer = _DrainEventBuffer()
_drain_thread: Optional[_DrainThread] = None


# For testing
_SYNC_MODE = False


def enable_sync_mode():
    global _SYNC_MODE
    _SYNC_MODE = True


def bind_engine(engine: Engine) -> None:
    global _engine, _drain_thread

    if _drain_thread:
        _drain_thread.stop()

    if _engine:
        flush()

    _engine = engine
    _drain_thread = _DrainThread(_buffer, interval_s=0.05)
    _drain_thread.start_draining()
    logger.info(
        "OriginTracer emitter bound, drain thread started"
    )


def unbind_engine() -> None:
    """
    Completely resets the emitter state.
    Stops the background thread and clears the buffer.
    """
    global _engine, _drain_thread

    if _drain_thread:
        _drain_thread.stop()
        # We don't necessarily need to .join() in tests unless,
        # but need to stop the loop.
        _drain_thread = None

    _engine = None

    # Clear the buffer
    with _buffer._lock:
        _buffer._q.clear()
        _buffer._dropped = 0


def emit(event: NormalizedEvent) -> None:
    """
    Emit one probe event.
    This is the ONLY function probes should call.
    """
    if _engine is None:
        return

    try:
        if _SYNC_MODE:
            _engine.process(event)
        else:
            _buffer.push(event)
    except Exception as exc:
        if (
            _SYNC_MODE
        ):  # Only log in sync mode to avoid spamming the buffer
            logger.warning(
                "OriginTracer failed to process event: %s", exc
            )


def emit_direct(event: NormalizedEvent) -> None:
    """
    Bypass buffer - process immediately. For lifecycle events.

    Use for gunicorn.worker.fork, celery.worker.fork, nginx.worker.discovered
    and any other topology event that must appear in the graph immediately
    at startup before the drain thread fires its first interval.

    And per-request events belong in the buffer.
    """
    import origintracer

    engine = (
        origintracer.get_engine()
    )  # always reads current live engine
    if engine is not None:
        engine.process(event)


def _restart_drain_thread() -> None:
    """
    Restart the drain thread after os.fork().

    Threads do not survive fork - the child process has the parent's
    buffer but no running drain thread. Call this in post_fork hooks
    (gunicorn st_post_fork, celery worker_process_init) before any
    emit() calls so the buffer drains correctly in the worker.
    """
    global _drain_thread
    if (
        _drain_thread is not None
        and not _drain_thread.is_alive()
    ):
        _drain_thread = _DrainThread(_buffer, interval_s=0.05)
        _drain_thread.start()
        logger.info("emitter: drain thread restarted after fork")


def flush() -> None:
    """
    Drain buffer into engine (used in non-direct mode).
    """
    if _engine is None:
        return
    for event in _buffer.drain():
        try:
            _engine.process(event)
        except Exception as exc:
            logger.debug(
                "Engine.process error during flush: %s", exc
            )


def stats() -> dict:
    return {
        "buffer_dropped": _buffer.dropped,
        "engine_bound": _engine is not None,
    }

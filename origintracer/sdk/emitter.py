"""
The interface between probes and the engine.

Probes never import ``Engine`` directly - they call ``emit()``. This
keeps probes decoupled from the engine implementation and allows the
engine to be swapped without touching any probe code.

Flow
----
::

    Probe
      └── emit(event)
            └── EventBuffer (thread-safe bounded deque)
                    └── Engine.process(event)

See Also
--------
``emit_direct()`` : Bypasses the buffer for lifecycle events that must
    appear in the graph immediately at startup before any requests arrive
    e.g. ``gunicorn.worker.fork``, ``celery.worker.fork``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict, deque
from typing import List, Optional

import origintracer

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
        self._dropped_by_trace: OrderedDict[str, int] = (
            OrderedDict()
        )
        self._trace_counts_truncated = False
        self._loss_by_reason: dict[
            str, OrderedDict[str, int]
        ] = {}

    def record_loss(
        self, event: NormalizedEvent, reason: str
    ) -> None:
        """Record one capture loss for the event's trace.

        Parameters
        ----------
        event : NormalizedEvent
            Event discarded before it could be processed.
        reason : str
            Capture stage and reason for the discard.

        Returns
        -------
        None
        """
        trace_id = str(event.trace_id or "")
        with self._lock:
            counters = self._loss_by_reason.setdefault(
                reason, OrderedDict()
            )
            if trace_id in counters:
                counters[trace_id] += 1
                counters.move_to_end(trace_id)
            else:
                if len(counters) >= 256:
                    counters.popitem(last=False)
                    self._trace_counts_truncated = True
                counters[trace_id] = 1

    def _record_drop_locked(self, trace_id: str) -> None:
        if trace_id in self._dropped_by_trace:
            self._dropped_by_trace[trace_id] += 1
            self._dropped_by_trace.move_to_end(trace_id)
            return
        if len(self._dropped_by_trace) >= 256:
            self._dropped_by_trace.popitem(last=False)
            self._trace_counts_truncated = True
        self._dropped_by_trace[trace_id] = 1

    def push(self, event: NormalizedEvent) -> None:
        with self._lock:
            if (
                self._q.maxlen is not None
                and len(self._q) >= self._q.maxlen
            ):
                self._dropped += 1
                self._record_drop_locked(
                    str(event.trace_id or "")
                )
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
        with self._lock:
            return self._dropped

    def diagnostics(self) -> dict:
        """Return bounded event-buffer loss counters.

        Returns
        -------
        dict
            Aggregate and trace-specific loss counts by capture stage.
        """
        with self._lock:
            return {
                "buffer_dropped_total": self._dropped,
                "buffer_dropped_by_trace": dict(
                    self._dropped_by_trace
                ),
                **{
                    f"{reason}_by_trace": dict(counters)
                    for reason, counters in self._loss_by_reason.items()
                },
                **{
                    f"{reason}_total": sum(counters.values())
                    for reason, counters in self._loss_by_reason.items()
                },
                "trace_counts_truncated": self._trace_counts_truncated,
            }


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
                            self._buffer.record_loss(
                                event, "engine_processing_failed"
                            )
                            logger.debug(
                                "drain: process error: %s", exc
                            )
                else:
                    for event in events:
                        self._buffer.record_loss(
                            event, "engine_unavailable"
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
        _buffer._dropped_by_trace.clear()
        _buffer._loss_by_reason.clear()
        _buffer._trace_counts_truncated = False


def emit(event: NormalizedEvent) -> None:
    """
    Emit one probe event.
    This is the ONLY function probes should call.
    """
    if _engine is None:
        _buffer.record_loss(event, "engine_unavailable")
        return

    try:
        if _SYNC_MODE:
            _engine.process(event)
        else:
            _buffer.push(event)
    except Exception as exc:
        _buffer.record_loss(event, "engine_processing_failed")
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
    engine = (
        origintracer.get_engine()
    )  # always reads current live engine
    if engine is None:
        _buffer.record_loss(event, "engine_unavailable")
        return
    try:
        engine.process(event)
    except Exception as exc:
        _buffer.record_loss(event, "engine_processing_failed")
        logger.debug("direct event processing failed: %s", exc)


def _restart_drain_thread() -> None:
    """
    Restart the drain thread after os.fork().

    Threads do not survive fork - the child process has the parent's
    buffer but no running drain thread. Call this in post_fork hooks
    (gunicorn post_fork, celery worker_process_init) before any
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
            if _engine is None:
                _buffer.record_loss(event, "engine_unavailable")
            else:
                _engine.process(event)
        except Exception as exc:
            _buffer.record_loss(
                event, "engine_processing_failed"
            )
            logger.debug(
                "Engine.process error during flush: %s", exc
            )


def stats() -> dict:
    return {
        **_buffer.diagnostics(),
        "buffer_dropped": _buffer.dropped,
        "engine_bound": _engine is not None,
    }

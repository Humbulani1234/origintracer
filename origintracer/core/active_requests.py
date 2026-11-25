from __future__ import annotations

import collections
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

logger = logging.getLogger("origintracer.active_requests")

_DEFAULT_TTL_S = 30.0
_DEFAULT_MAX = 10_000
_EVICT_PERIOD_S = 5.0
_COMPLETION_WINDOW = 200  # keep last N completions per pattern for rule evaluation
_LIFECYCLE_HISTORY = 500
_LIFECYCLE_UPLOAD_LIMIT = 100


@dataclass
class RequestSpan:
    """
    One in-flight or recently completed request.
    """

    trace_id: str
    service: str
    pattern: str  # normalized name, e.g. "GET /api/users/{id}/"
    start_time: float
    last_event: float = field(default_factory=time.monotonic)
    complete_time: Optional[float] = None
    probe_sequence: List[str] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    last_event_at: float = field(default_factory=time.time)
    event_count: int = 0

    @property
    def duration_ms(self) -> Optional[float]:
        if self.complete_time is None:
            return None
        return (self.complete_time - self.start_time) * 1000

    @property
    def in_flight_ms(self) -> float:
        return (time.monotonic() - self.start_time) * 1000

    @property
    def is_complete(self) -> bool:
        return self.complete_time is not None


class ActiveRequestTracker:
    """
    Tracks in-flight requests in a bounded dict with TTL eviction.

    Maintains a 30-second window of active ``trace_id`` entries, enabling
    causal rules to compare current request latency against stored historical
    averages.

    Attributes
    ----------
    active : dict[str, RequestSpan]
        Live requests keyed by ``trace_id``. Each ``RequestSpan`` holds
        ``service``, ``pattern`` (normalised name), ``start_time``, and
        ``last_event``.

    Lifecycle
    ---------
    .. list-table::
       :widths: 25 75

       * - ``start()``
         - Called when a traced request enters — ``TracerMiddleware``,
           Celery ``task_prerun``, etc.
       * - ``event()``
         - Called on every ``NormalizedEvent`` to update ``last_event``.
       * - ``complete()``
         - Called when the request exits. Returns the ``RequestSpan``
           with ``duration_ms`` set.
       * - TTL eviction
         - Entries not completed within 30s are evicted automatically.

    Causal Rule Integration
    -----------------------
    The ``request_duration_anomaly`` rule reads::

        tracker.recent_completions(pattern, window_s=60)

    and compares completion durations against the ``RuntimeGraph`` node's
    ``avg_duration_ns`` to detect live latency divergence.

    Usage
    -----
    ::

        tracker = ActiveRequestTracker()

        # In TracerMiddleware or Celery probe:
        tracker.start(trace_id="abc-123", service="django",
                      pattern="/api/users/{id}/")

        # In Engine.process():
        tracker.event(trace_id="abc-123", probe="django.db.query")

        # In TracerMiddleware exit or task_postrun:
        span = tracker.complete(trace_id="abc-123")
    """

    def __init__(
        self,
        ttl_s: float = _DEFAULT_TTL_S,
        max_size: int = _DEFAULT_MAX,
    ) -> None:
        self._ttl = ttl_s
        self._max = max_size
        self._active: Dict[str, RequestSpan] = {}
        self._recent_lifecycle: Deque[Dict[str, object]] = (
            collections.deque(maxlen=_LIFECYCLE_HISTORY)
        )
        self._lifecycle_history_evicted = 0
        self._lock = threading.Lock()
        self._alive = True

        # Ring buffer of recent completions keyed by pattern.
        # Used by causal rules to compute rolling P99.
        self._completions: Dict[str, Deque[float]] = (
            collections.defaultdict(
                lambda: collections.deque(
                    maxlen=_COMPLETION_WINDOW
                )
            )
        )

        # Background eviction thread
        self._evict_thread = threading.Thread(
            target=self._evict_loop,
            daemon=True,
            name="origintracer-active-req-evict",
        )
        self._evict_thread.start()

    def stop(self) -> None:
        self._alive = False

    def _record_lifecycle_locked(
        self,
        span: RequestSpan,
        state: str,
        reason: Optional[str] = None,
        exception_type: Optional[str] = None,
    ) -> None:
        """Append one bounded, serializable lifecycle observation.

        Parameters
        ----------
        span : RequestSpan
            Tracked request whose lifecycle changed.
        state : str
            Observed lifecycle state.
        reason : str, optional
            Reason the tracker recorded the state.
        exception_type : str, optional
            Exception class reported by the integration.

        Notes
        -----
        The caller must hold ``self._lock``.
        """
        if (
            len(self._recent_lifecycle)
            == self._recent_lifecycle.maxlen
        ):
            self._lifecycle_history_evicted += 1
        self._recent_lifecycle.append(
            {
                "trace_id": span.trace_id,
                "service": span.service,
                "pattern": span.pattern,
                "state": state,
                "reason": reason,
                "exception_type": exception_type,
                "started_at": span.started_at,
                "ended_at": (
                    time.time()
                    if span.complete_time is not None
                    else None
                ),
                "last_event_at": span.last_event_at,
                "event_count": span.event_count,
                "duration_ms": span.duration_ms,
            }
        )

    def start(
        self,
        trace_id: str,
        service: str,
        pattern: str,
    ) -> RequestSpan:
        """
        Register a new in-flight request.
        Called by ``TracerMiddleware._begin()``, Celery ``_on_task_start()``, etc.
        If the tracker is at capacity, the oldest entry is evicted to make room.
        This is a last-resort safety valve - normal eviction is TTL-based.
        """
        span = RequestSpan(
            trace_id=trace_id,
            service=service,
            pattern=pattern,
            start_time=time.monotonic(),
        )
        with self._lock:
            previous = self._active.pop(trace_id, None)
            if previous is not None:
                self._record_lifecycle_locked(
                    previous, "evicted", "trace_id_restarted"
                )
            if len(self._active) >= self._max:
                # Evict oldest entry - FIFO. Record why it stopped being tracked.
                oldest_key = next(iter(self._active))
                evicted = self._active.pop(oldest_key)
                self._record_lifecycle_locked(
                    evicted, "evicted", "tracker_capacity"
                )
                logger.debug(
                    "active_requests: cap eviction of %s",
                    oldest_key,
                )
            self._active[trace_id] = span
        return span

    def event(self, trace_id: str, probe: str) -> None:
        """
        Update last_event timestamp and append to probe_sequence.
        Called by ``Engine.process()`` for every ``NormalizedEvent``.
        """
        with self._lock:
            span = self._active.get(trace_id)
            if span is None:
                return
            span.last_event = time.monotonic()
            span.last_event_at = time.time()
            span.event_count += 1
            # Keep probe sequence bounded - first 20 probes only
            if len(span.probe_sequence) < 20:
                span.probe_sequence.append(probe)

    def complete(self, trace_id: str) -> Optional[RequestSpan]:
        """
        Mark a request as complete and move it to the completions buffer.
        Returns the completed span, or None if trace_id was not tracked.
        Called by ``TracerMiddleware exit``, Celery ``_on_task_end()``, etc.
        """
        with self._lock:
            span = self._active.pop(trace_id, None)
            if span is None:
                return None
            span.complete_time = time.monotonic()
            span.last_event_at = time.time()
            self._record_lifecycle_locked(span, "completed")
            if span.duration_ms is not None:
                self._completions[span.pattern].append(
                    span.duration_ms
                )
        return span

    def fail(
        self, trace_id: str, exception_type: Optional[str] = None
    ) -> Optional[RequestSpan]:
        """Record that a tracked operation ended with an observed failure.

        Parameters
        ----------
        trace_id : str
            Trace identifier for the tracked operation.
        exception_type : str, optional
            Exception class recorded by the integration, if available.

        Returns
        -------
        RequestSpan or None
            The failed span, or ``None`` when it was not tracked.
        """
        with self._lock:
            span = self._active.pop(trace_id, None)
            if span is None:
                return None
            span.complete_time = time.monotonic()
            span.last_event_at = time.time()
            self._record_lifecycle_locked(
                span,
                "failed",
                "integration_reported_exception",
                exception_type,
            )
            return span

    def diagnostics(
        self, limit: int = _LIFECYCLE_UPLOAD_LIMIT
    ) -> Dict[str, object]:
        """Return bounded lifecycle evidence for the next runtime heartbeat.

        Parameters
        ----------
        limit : int, optional
            Maximum number of active and recent lifecycle rows to include.

        Returns
        -------
        dict[str, object]
            Active rows, recent terminal states, and flags showing whether
            either list or the retained history was truncated.
        """
        limit = max(1, min(int(limit), _LIFECYCLE_UPLOAD_LIMIT))
        sampled_at = time.time()
        with self._lock:
            active = list(self._active.values())
            recent = list(self._recent_lifecycle)
            history_evicted = self._lifecycle_history_evicted
        active_rows = [
            {
                "trace_id": span.trace_id,
                "service": span.service,
                "pattern": span.pattern,
                "state": "active",
                "started_at": span.started_at,
                "last_event_at": span.last_event_at,
                "event_count": span.event_count,
                "in_flight_ms": round(span.in_flight_ms, 3),
                "sampled_at": sampled_at,
            }
            for span in active[-limit:]
        ]
        return {
            "active": active_rows,
            "active_truncated": len(active) > limit,
            "recent": recent[-limit:],
            "recent_truncated": (
                len(recent) > limit or history_evicted > 0
            ),
            "history_evicted_count": history_evicted,
            "sampled_at": sampled_at,
        }

    def active_count(self, service: Optional[str] = None) -> int:
        """
        Number of requests currently in-flight.
        """
        with self._lock:
            if service is None:
                return len(self._active)
            return sum(
                1
                for s in self._active.values()
                if s.service == service
            )

    def slow_in_flight(
        self, threshold_ms: float = 1000.0
    ) -> List[RequestSpan]:
        """
        Returns in-flight requests that have been running longer than threshold_ms.
        Used by _request_duration_anomaly to detect live slowness.
        """
        with self._lock:
            spans = list(self._active.values())
        now = time.monotonic()
        return [
            s
            for s in spans
            if (now - s.start_time) * 1000 > threshold_ms
        ]

    def recent_completions(
        self,
        pattern: str,
    ) -> List[float]:
        """
        Returns recent completion durations (ms) for the given pattern.
        Used by causal rules to compute rolling averages and percentiles.
        """
        with self._lock:
            return list(self._completions.get(pattern, []))

    def percentile(
        self, durations: List[float], p: float
    ) -> Optional[float]:
        """
        Helper for causal rules: p99, p95 from a duration list.
        """
        if not durations:
            return None
        sorted_d = sorted(durations)
        idx = int(len(sorted_d) * p / 100)
        return sorted_d[min(idx, len(sorted_d) - 1)]

    def all_patterns_summary(self) -> Dict[str, dict]:
        r"""
        Returns a summary of recent completion stats per pattern.
        Used by the REPL \status command to show live throughput.
        """
        with self._lock:
            completions = {
                k: list(v) for k, v in self._completions.items()
            }
        result = {}
        for pattern, durations in completions.items():
            if not durations:
                continue
            result[pattern] = {
                "count": len(durations),
                "avg_ms": round(
                    sum(durations) / len(durations), 1
                ),
                "p99_ms": round(
                    self.percentile(durations, 99) or 0, 1
                ),
                "p50_ms": round(
                    self.percentile(durations, 50) or 0, 1
                ),
            }
        return result

    def _normalize_path(self, path: str) -> str:
        # normalize /api/users/123/ >> /api/users/{id}/
        return re.sub(r"/\d+", "/{id}", path)

    def _evict_loop(self) -> None:
        while self._alive:
            time.sleep(_EVICT_PERIOD_S)
            self._evict()

    def _evict(self) -> None:
        cutoff = time.monotonic() - self._ttl
        with self._lock:
            stale = [
                tid
                for tid, span in self._active.items()
                if span.last_event < cutoff
            ]
            for tid in stale:
                span = self._active.pop(tid)
                self._record_lifecycle_locked(
                    span, "stale", "inactive_ttl"
                )
                logger.debug(
                    "active_requests: TTL eviction trace_id=%s service=%s "
                    "in_flight_ms=%.1f",
                    tid,
                    span.service,
                    span.in_flight_ms,
                )

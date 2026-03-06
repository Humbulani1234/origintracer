"""
core/engine.py

The Engine is the central, stack-agnostic coordinator.
It receives NormalizedEvents from any probe via emit(),
builds the RuntimeGraph, drives the TemporalStore,
and exposes causal/semantic query surfaces.

The Engine never imports from any probe module.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from .causal import PatternRegistry, CausalMatch, build_default_registry
from .event_schema import NormalizedEvent
from .runtime_graph import RuntimeGraph
from .semantic import SemanticLayer
from .temporal import TemporalStore

logger = logging.getLogger("stacktracer.engine")


class Engine:
    """
    Single-process engine instance.

    Lifecycle
    ---------
    1. Instantiate once at startup.
    2. Bind to the SDK emitter: `sdk.emitter.bind_engine(engine)`
    3. All probes emit via `sdk.emitter.emit(event)` — they never touch Engine directly.
    4. Call `engine.start_background_tasks()` to enable periodic snapshots.
    5. Query via `engine.query(...)` or the HTTP API.

    Thread safety
    -------------
    RuntimeGraph is internally thread-safe via RLock.
    Engine-level operations (snapshot, evaluate) acquire no additional locks.
    """

    def __init__(
        self,
        causal_registry: Optional[PatternRegistry] = None,
        semantic_layer: Optional[SemanticLayer] = None,
        snapshot_interval_s: float = 15.0,
        max_temporal_diffs: int = 500,
    ) -> None:
        self.graph = RuntimeGraph()
        self.temporal = TemporalStore(max_diffs=max_temporal_diffs)
        self.causal = causal_registry or build_default_registry()
        self.semantic = semantic_layer or SemanticLayer()

        self._snapshot_interval = snapshot_interval_s
        self._snapshot_thread: Optional[threading.Thread] = None
        self._running = False

        # Last event per trace — used to build graph edges between consecutive events.
        # Stored as (event, last_updated_timestamp) so stale entries can be evicted.
        # A trace_id not updated in _trace_ttl_s seconds is considered complete or
        # abandoned and is removed by _evict_stale_traces() in the snapshot loop.
        self._trace_ttl_s: float = 60.0
        self._last_event_per_trace: Dict[str, tuple] = {}   # trace_id → (NormalizedEvent, float)
        self._last_event_lock = threading.Lock()

        # In-order event log (bounded ring buffer for replay / timeline)
        self._event_log: List[NormalizedEvent] = []
        self._event_log_max = 10_000
        self._event_log_lock = threading.Lock()

        # Injected storage repository (optional — set after construction)
        self.repository: Optional[Any] = None

    # ------------------------------------------------------------------ #
    # Primary ingest path
    # ------------------------------------------------------------------ #

    def process(self, event: NormalizedEvent) -> None:
        """
        Called by sdk.emitter for every emitted probe event.
        Must be fast — this runs on the hot path.
        """
        # 1. Persist (async fire-and-forget; repository handles its own buffering)
        if self.repository:
            try:
                self.repository.insert_event(event)
            except Exception as exc:
                logger.debug("Repository insert failed: %s", exc)

        # 2. Update runtime graph — build edges between consecutive events in same trace
        with self._last_event_lock:
            entry = self._last_event_per_trace.get(event.trace_id)
            parent = entry[0] if entry else None
            self._last_event_per_trace[event.trace_id] = (event, time.monotonic())
        self.graph.add_from_event(event, parent_event=parent)

        # 3. Append to in-memory event log (bounded)
        with self._event_log_lock:
            self._event_log.append(event)
            if len(self._event_log) > self._event_log_max:
                self._event_log = self._event_log[-self._event_log_max:]

    # ------------------------------------------------------------------ #
    # Snapshot (called periodically or on deployment events)
    # ------------------------------------------------------------------ #

    def snapshot(self, label: Optional[str] = None) -> Dict[str, Any]:
        """Capture and store a graph diff. Returns the diff summary."""
        snap = self.graph.snapshot()
        diff = self.temporal.capture(snap, label=label)
        return diff.to_dict()

    def mark_deployment(self, label: str = "deployment") -> None:
        """
        Mark a deployment boundary in the temporal store.
        Call this from your CD pipeline or deployment hook.
        """
        self.temporal.mark_event(label)
        logger.info("Deployment marker set: %s", label)

    # ------------------------------------------------------------------ #
    # Causal evaluation
    # ------------------------------------------------------------------ #

    def evaluate(self, tags: Optional[List[str]] = None) -> List[CausalMatch]:
        """Run all registered causal rules against the current graph."""
        return self.causal.evaluate(self.graph, self.temporal, tags=tags)

    # ------------------------------------------------------------------ #
    # Query surface
    # ------------------------------------------------------------------ #

    def query(self, query_str: str) -> Dict[str, Any]:
        """
        Entry point for the DSL query layer.
        Delegates to query/executor.py.
        """
        from ..query.parser import parse
        from ..query.executor import execute
        parsed = parse(query_str)
        return execute(parsed, self)

    def critical_path(self, trace_id: str) -> List[Dict[str, Any]]:
        """
        Derive the critical path for a single trace_id from the event log.
        Returns events in chronological order, annotated with inter-stage durations.
        """
        with self._event_log_lock:
            events = [e for e in self._event_log if e.trace_id == trace_id]

        events.sort(key=lambda e: e.timestamp)

        CRITICAL_PROBES = {
            "request.entry",
            "django.middleware.enter",
            "django.url.resolve",
            "django.view.enter",
            "function.call",
            "function.return",
            "function.exception",
            "asyncio.task.create",
            "asyncio.task.block",
            "asyncio.task.wakeup",
            "db.query.start",
            "db.query.end",
            "tcp.send",
            "tcp.recv",
            "request.exit",
        }
        filtered = [e for e in events if e.probe in CRITICAL_PROBES]

        path = []
        last_ts = None
        for e in filtered:
            duration_ms = (e.timestamp - last_ts) * 1000 if last_ts else None
            path.append({
                "probe": e.probe,
                "service": e.service,
                "name": e.name,
                "timestamp": e.timestamp,
                "wall_time": e.wall_time,
                "duration_ms": round(duration_ms, 3) if duration_ms else None,
                "metadata": e.metadata,
            })
            last_ts = e.timestamp
        return path

    def traces_for_service(self, service: str, limit: int = 50) -> List[str]:
        """Return distinct trace_ids that touched a given service."""
        with self._event_log_lock:
            seen = []
            seen_set: set = set()
            for e in reversed(self._event_log):
                if e.service == service and e.trace_id not in seen_set:
                    seen.append(e.trace_id)
                    seen_set.add(e.trace_id)
                if len(seen) >= limit:
                    break
        return seen

    def hotspots(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """Return the N busiest nodes by call count."""
        return [
            {
                "node": n.id,
                "service": n.service,
                "type": n.node_type,
                "call_count": n.call_count,
                "avg_duration_ms": round(n.avg_duration_ns / 1e6, 3) if n.avg_duration_ns else None,
            }
            for n in self.graph.hottest_nodes(top_n=top_n)
        ]

    # ------------------------------------------------------------------ #
    # Background periodic snapshot thread
    # ------------------------------------------------------------------ #

    def start_background_tasks(self) -> None:
        if self._running:
            return
        self._running = True
        self._snapshot_thread = threading.Thread(
            target=self._snapshot_loop,
            daemon=True,
            name="stacktracer-snapshot",
        )
        self._snapshot_thread.start()
        logger.info("Background snapshot thread started (interval=%ss)", self._snapshot_interval)

    def stop(self) -> None:
        self._running = False
        if self._snapshot_thread:
            self._snapshot_thread.join(timeout=5)

    def _snapshot_loop(self) -> None:
        while self._running:
            time.sleep(self._snapshot_interval)
            try:
                self.snapshot()
                self._evict_stale_traces()
                if hasattr(self, "compactor") and self.compactor is not None:
                    self.compactor.compact(self.graph)
            except Exception as exc:
                logger.warning("Snapshot/compact failed: %s", exc)

    def _evict_stale_traces(self) -> None:
        """
        Remove trace_ids from _last_event_per_trace that have not received
        an event in _trace_ttl_s seconds (default 60s).

        Why this matters: every unique trace_id that ever passes through
        process() is inserted into this dict. Without eviction it grows
        forever — one entry per request for the lifetime of the process.
        At 100 req/s that is 360,000 entries per hour, each holding a
        reference to a NormalizedEvent object.

        Called from _snapshot_loop so it runs every snapshot_interval seconds
        (default 15s), which is well within the 60s TTL threshold.
        """
        now = time.monotonic()
        cutoff = now - self._trace_ttl_s
        with self._last_event_lock:
            stale = [tid for tid, (_, ts) in self._last_event_per_trace.items() if ts < cutoff]
            for tid in stale:
                del self._last_event_per_trace[tid]
        if stale:
            logger.debug("Evicted %d stale trace_ids from _last_event_per_trace", len(stale))

    # ------------------------------------------------------------------ #
    # State
    # ------------------------------------------------------------------ #

    def status(self) -> Dict[str, Any]:
        return {
            "graph_nodes": len(self.graph),
            "temporal_diffs": len(self.temporal),
            "event_log_size": len(self._event_log),
            "causal_rules": len(self.causal.rule_names()),
            "semantic_labels": self.semantic.all_labels(),
            "running": self._running,
        }

    def __repr__(self) -> str:
        return f"<Engine graph={self.graph} temporal={self.temporal}>"
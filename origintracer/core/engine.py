from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type

from ..query.parser import execute, parse
from ..sdk.base_probe import BaseProbe
from .active_requests import ActiveRequestTracker
from .causal import CausalMatch, PatternRegistry
from .event_schema import NormalizedEvent, ProbeTypes
from .graph_compactor import GraphCompactor
from .graph_normalizer import GraphNormalizer
from .runtime_graph import RuntimeGraph
from .semantic import SemanticLayer
from .temporal import TemporalStore

logger = logging.getLogger("origintracer.engine")


SpanKey = Tuple[str, str]


@dataclass
class _SpanState:
    """Engine-owned state for one logical operation in one trace."""

    anchor: NormalizedEvent
    last_seen: float
    duration_ns: int = 0
    parent_anchor: Optional[NormalizedEvent] = None


@dataclass(frozen=True)
class _PendingChild:
    """A child whose declared parent has not reached the engine yet."""

    span_key: SpanKey
    queued_at: float


class Engine:
    """
    Engine is the central, stack-agnostic coordinator.
    It receives `NormalizedEvents` from any probe via `emit()`,
    builds the `RuntimeGraph`, drives the `TemporalStore`,
    and exposes causal/semantic query surfaces.

    Lifecycle
    ---------
    1. Instantiate once at startup.
    2. Bind to the SDK emitter: `sdk.emitter.bind_engine(engine)`
    3. All probes emit via `sdk.emitter.emit(event)` - they never touch Engine directly.
    4. Call `engine.start_background_tasks()` to enable periodic snapshots.
    5. Query via `engine.query(...)` or the HTTP API.

    Lineage contract
    ----------------
    The engine understands only the framework-neutral lineage fields on
    ``NormalizedEvent``:

    * ``trace_id`` identifies an execution.
    * ``span_id`` identifies one logical operation within that execution.
    * ``parent_span_id`` identifies the direct operation that caused it.

    A producer that can observe real parentage should provide stable span IDs.
    That permits one parent to fan out to many children without making sibling
    relationships depend on arrival order. The lookup is scoped by trace ID so
    independently generated traces may safely reuse a span ID.

    Events can arrive out of order. A child whose parent is not yet known is
    retained temporarily and connected when the parent arrives. Both resolved
    spans and unresolved children are bounded by ``lineage_ttl_s``.

    Multiple events with the same ``(trace_id, span_id)`` are lifecycle
    observations of one logical operation, not separate invocations. The first
    observation creates and counts the graph node; later observations enrich its
    metadata and duration without incrementing its call count again.

    Events without explicit lineage form a sequential trace: the most recently
    observed event in a trace becomes the next event's parent. This mode suits
    serialized event streams but cannot infer causality between concurrent
    siblings. Events with explicit parentage are resolved exclusively through
    span lineage and do not advance the sequential cursor.

    The engine does not inspect probe-specific metadata or interpret any
    framework's scheduler, node model, or callback lifecycle. Those decisions
    belong to probes and causal rules.
    """

    def __init__(
        self,
        causal_registry: Optional[PatternRegistry] = None,
        semantic_layer: Optional[SemanticLayer] = None,
        snapshot_interval_s: float = 15.0,
        max_temporal_diffs: int = 500,
        lineage_ttl_s: float = 300.0,
    ) -> None:
        self.graph = RuntimeGraph()
        self.temporal = TemporalStore(
            max_diffs=max_temporal_diffs
        )
        self.tracker = (
            ActiveRequestTracker()
        )  # overwritten in init()
        self.normalizer = (
            GraphNormalizer()
        )  # overwritten in init()
        self.compactor = (
            GraphCompactor()
        )  # overwritten in init()
        self.semantic = semantic_layer or SemanticLayer()
        self.causal: Optional[Type[PatternRegistry]] = None
        # System active probes - overridden during init()
        self.probes: Optional[List[BaseProbe]] = None

        self._snapshot_interval = snapshot_interval_s
        self._snapshot_thread: Optional[threading.Thread] = None
        self._running = False
        self._started_at = time.monotonic()

        # Sequential cursor for events that do not declare explicit lineage.
        self._trace_ttl_s: float = 60.0
        self._last_event_per_trace: Dict[
            str, Tuple[NormalizedEvent, float]
        ] = {}
        self._last_event_lock = threading.Lock()

        # Explicit lineage is trace-scoped. The same lock protects the span index,
        # pending children, and the graph mutations derived from both so concurrent
        # calls to process() cannot resolve or count the same logical span twice.
        self._lineage_ttl_s = lineage_ttl_s
        self._event_by_span_id: Dict[SpanKey, _SpanState] = {}
        self._pending_children: Dict[
            SpanKey, List[_PendingChild]
        ] = {}
        self._lineage_lock = threading.RLock()
        self._span_lock = self._lineage_lock

        # GraphNormalizer owns mutable caches and cardinality state.
        self._normalizer_lock = threading.Lock()

        # In-order event log (bounded ring buffer for replay/timeline)
        self._event_log: List[NormalizedEvent] = []
        self._event_log_max = 10_000
        self._event_log_lock = threading.Lock()

        # The Uploader - set after engine construction
        self.repository: Optional[Any] = None

    def process(self, event: NormalizedEvent) -> None:
        """
        Consume one normalized observation and update engine state.

        A ``parent_span_id`` selects explicit lineage. Events without one use the
        sequential trace cursor. Explicit children whose parent has not arrived
        are retained for reconciliation; they are never attached to whichever
        sibling happened to arrive first.
        """
        with self._normalizer_lock:
            event.name = self.normalizer.normalize(
                event.service, event.name
            )

        self._apply_graph_observation(event)

        # Send normalized events to the Uploader
        if self.repository:
            try:
                self.repository.insert_event(event)
            except Exception as exc:
                logger.debug("Repository insert failed: %s", exc)

        # 3. Append to in-memory event log (bounded)
        with self._event_log_lock:
            self._event_log.append(event)
            if len(self._event_log) > self._event_log_max:
                self._event_log = self._event_log[
                    -self._event_log_max :
                ]

        # 4. Update Active Request Tracker
        self.tracker.event(
            trace_id=event.trace_id, probe=event.probe
        )

    def _apply_graph_observation(
        self,
        event: NormalizedEvent,
    ) -> None:
        """
        Apply one observation while holding the explicit-lineage lock.

        Explicit lineage is resolved strictly within the event's trace. If its
        parent is absent, the edge is deferred rather than guessed. Cursor and
        lineage decisions share this critical section so simultaneous lifecycle
        observations of one span cannot both be counted as new operations.
        """
        now = time.monotonic()
        span_key = self._span_key(event.trace_id, event.span_id)

        with self._lineage_lock:
            if event.probe == "request.exit":
                with self._last_event_lock:
                    self._last_event_per_trace.pop(
                        event.trace_id, None
                    )

            state = self._event_by_span_id.get(span_key)
            if state is not None:
                self._refresh_span_node(state, event, now)
                return

            parent: Optional[NormalizedEvent] = None
            parent_key: Optional[SpanKey] = None
            if event.parent_span_id is not None:
                parent_key = self._span_key(
                    event.trace_id, event.parent_span_id
                )
                parent_state = self._event_by_span_id.get(
                    parent_key
                )
                parent = (
                    parent_state.anchor if parent_state else None
                )
            elif event.probe != "request.exit":
                with self._last_event_lock:
                    entry = self._last_event_per_trace.get(
                        event.trace_id
                    )
                    parent = entry[0] if entry else None
                    self._last_event_per_trace[
                        event.trace_id
                    ] = (
                        event,
                        now,
                    )

            self.graph.add_from_event(event, parent_event=parent)
            state = _SpanState(
                anchor=event,
                last_seen=now,
                duration_ns=event.duration_ns or 0,
                parent_anchor=parent,
            )
            self._event_by_span_id[span_key] = state

            if (
                parent_key is not None
                and parent is None
                and parent_key != span_key
            ):
                self._pending_children.setdefault(
                    parent_key, []
                ).append(
                    _PendingChild(
                        span_key=span_key, queued_at=now
                    )
                )

            self._resolve_pending_children(span_key, state)

    @staticmethod
    def _span_key(trace_id: str, span_id: str) -> SpanKey:
        """Return the trace-scoped identity of a logical operation."""
        return trace_id, span_id

    def _refresh_span_node(
        self,
        state: _SpanState,
        event: NormalizedEvent,
        now: float,
    ) -> None:
        """
        Merge a later lifecycle observation into an already-counted span.

        The latest non-null duration replaces the earlier duration contribution
        for this span. Probe-specific metadata is merged, while ``probe`` remains
        the first observation's stable classification and ``last_probe`` records
        the newest lifecycle observation.
        """
        anchor = state.anchor
        anchor_id = self.graph.node_id(
            anchor.service, anchor.name
        )
        event_id = self.graph.node_id(event.service, event.name)
        if event_id != anchor_id:
            logger.warning(
                "Span %s in trace %s changed graph identity from %s to %s; "
                "keeping the first identity",
                event.span_id,
                event.trace_id,
                anchor_id,
                event_id,
            )

        duration_delta = (
            event.duration_ns - state.duration_ns
            if event.duration_ns is not None
            else None
        )
        node = self.graph.update_node_observation(
            anchor_id,
            duration_delta_ns=duration_delta,
            metadata={
                **event.metadata,
                "probe": anchor.probe,
                "last_probe": event.probe,
            },
        )
        if node is None:
            self.graph.upsert_node(
                node_id=anchor_id,
                node_type=anchor.service,
                service=anchor.service,
                duration_ns=event.duration_ns,
                metadata={
                    "probe": anchor.probe,
                    **event.metadata,
                },
            )
            state.duration_ns = event.duration_ns or 0
        elif event.duration_ns is not None:
            self._refresh_span_edge_duration(
                state,
                duration_delta or 0,
            )
            state.duration_ns = event.duration_ns

        state.last_seen = now

    def _resolve_pending_children(
        self, parent_key: SpanKey, parent_state: _SpanState
    ) -> None:
        """Attach every retained child that declared the registered parent."""
        pending = self._pending_children.pop(parent_key, [])
        for item in pending:
            child_state = self._event_by_span_id.get(
                item.span_key
            )
            if child_state is None:
                continue
            child_state.parent_anchor = parent_state.anchor
            self._connect_span_edge(parent_state, child_state)

    def _refresh_span_edge_duration(
        self, state: _SpanState, duration_delta_ns: int
    ) -> None:
        """Apply a lifecycle duration correction without recounting the edge."""
        parent = state.parent_anchor
        if parent is None or duration_delta_ns == 0:
            return
        parent_id = self.graph.node_id(
            parent.service, parent.name
        )
        child = state.anchor
        child_id = self.graph.node_id(child.service, child.name)
        self.graph.adjust_edge_duration(
            parent_id,
            child_id,
            "calls",
            duration_delta_ns,
        )

    def _connect_span_edge(
        self, parent_state: _SpanState, child_state: _SpanState
    ) -> None:
        """Create one aggregate calls edge between two resolved logical spans."""
        parent = parent_state.anchor
        child = child_state.anchor
        parent_id = self.graph.node_id(
            parent.service, parent.name
        )
        child_id = self.graph.node_id(child.service, child.name)
        if parent_id == child_id:
            return
        self.graph.upsert_edge(
            source=parent_id,
            target=child_id,
            edge_type="calls",
            duration_ns=child_state.duration_ns or None,
        )

    def snapshot(
        self, label: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Capture and store a graph diff. Returns the diff summary.
        """
        snap = self.graph.snapshot()
        diff = self.temporal.capture(snap, label=label)
        return diff.to_dict()

    def mark_deployment(self, label: str = "deployment") -> None:
        """
        Mark a deployment boundary in the temporal store.
        """
        self.temporal.mark_event(label)
        logger.info("Deployment marker set: %s", label)

    def evaluate(
        self, tags: Optional[List[str]] = None
    ) -> List[CausalMatch]:
        """
        Run all registered causal rules against the current graph.
        """
        if self.causal is not None:
            return self.causal.evaluate(
                self.graph,
                self.temporal,
                self.tracker,
                tags=tags,
            )
        return []

    def query(self, query_str: str) -> Dict[str, Any]:
        """
        Entry point for the DSL query layer.
        Delegates to query/executor.py.
        """
        parsed = parse(query_str)
        # The parser describes its coordinator protocol with the Engine class.
        return execute(parsed, self)  # type: ignore[arg-type]

    def critical_path(
        self, trace_id: str
    ) -> List[Dict[str, Any]]:
        """
        Derive the critical path for a single `trace_id` from the event log.
        Returns all registered probe events in chronological order,
        annotated with inter-stage durations.
        """
        with self._event_log_lock:
            events = [
                e
                for e in self._event_log
                if e.trace_id == trace_id
            ]
        events.sort(key=lambda e: e.timestamp)

        # All registered observations are meaningful. Events without a duration
        # still belong in the path because they may mark topology transitions.
        registered = list(ProbeTypes.all().keys())
        filtered = [e for e in events if e.probe in registered]

        path = []
        last_ts = None
        duration_ms: Optional[float] = None
        for e in filtered:
            duration_ms = (
                (e.timestamp - last_ts) * 1000
                if last_ts
                else None
            )
            path.append(
                {
                    "probe": e.probe,
                    "service": e.service,
                    "name": e.name,
                    "timestamp": e.timestamp,
                    "wall_time": e.wall_time,
                    "duration_ms": (
                        round(duration_ms, 3)
                        if duration_ms
                        else None
                    ),
                    "metadata": e.metadata,
                }
            )
            last_ts = e.timestamp
        return path

    def traces_for_service(
        self, service: str, limit: int = 50
    ) -> List[str]:
        """
        Return distinct trace_ids that touched a given service.
        """
        with self._event_log_lock:
            seen = []
            seen_set: set = set()
            for e in reversed(self._event_log):
                if (
                    e.service == service
                    and e.trace_id not in seen_set
                ):
                    seen.append(e.trace_id)
                    seen_set.add(e.trace_id)
                if len(seen) >= limit:
                    break
        return seen

    def hotspots(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """
        Return the N busiest nodes by call count.
        """
        return [
            {
                "node": n.id,
                "service": n.service,
                "type": n.node_type,
                "call_count": n.call_count,
                "avg_duration_ms": (
                    round(n.avg_duration_ns / 1e6, 3)
                    if n.avg_duration_ns
                    else None
                ),
            }
            for n in self.graph.hottest_nodes(top_n=top_n)
        ]

    def start_background_tasks(self) -> None:
        if self._running:
            return
        self._running = True
        self._snapshot_thread = threading.Thread(
            target=self._snapshot_loop,
            daemon=True,
            name="origintracer-snapshot",
        )
        self._snapshot_thread.start()
        logger.info(
            "Background snapshot thread started (interval=%ss)",
            self._snapshot_interval,
        )

    def stop(self) -> None:
        self._running = False
        if self._snapshot_thread:
            self._snapshot_thread.join(timeout=5)

    def _snapshot_loop(self) -> None:
        i = 0
        while self._running:
            time.sleep(self._snapshot_interval)
            i += 1
            try:
                self.snapshot(label=f"origintracer-snapshot-{i}")
                self._evict_stale_traces()
                self._evict_stale_spans()
                # Run graph compaction
                self.compactor.compact(self.graph)
            except Exception as exc:
                logger.warning(
                    "Snapshot/compact failed at iteration %d: %s",
                    i,
                    exc,
                    exc_info=True,
                )

    def _evict_stale_traces(self) -> None:
        """
        Remove trace_ids from _last_event_per_trace that have not received
        an event in _trace_ttl_s seconds (default 60s).
        Every unique trace_id that ever passes through process() is inserted
        into this dict. Without eviction it grows forever - one entry per request
        for the lifetime of the process. Called from _snapshot_loop so it runs every
        snapshot_interval seconds (default 15s), which is well within the 60s TTL
        threshold.
        """
        now = time.monotonic()
        cutoff = now - self._trace_ttl_s
        with self._last_event_lock:
            stale = [
                tid
                for tid, (
                    _,
                    ts,
                ) in self._last_event_per_trace.items()
                if ts < cutoff
            ]
            for tid in stale:
                del self._last_event_per_trace[tid]
        if stale:
            logger.debug(
                "Evicted %d stale trace_ids from _last_event_per_trace",
                len(stale),
            )

    def _evict_stale_spans(self) -> None:
        """
        Evict resolved lineage and unresolved children past the lineage TTL.

        Pending children deliberately share the same bound as the span index.
        Once either side is older than the reconciliation window, retaining an
        unresolved relationship can no longer produce a reliable edge.
        """
        now = time.monotonic()
        cutoff = now - self._lineage_ttl_s
        with self._lineage_lock:
            stale_spans = [
                key
                for key, state in self._event_by_span_id.items()
                if state.last_seen < cutoff
            ]
            for key in stale_spans:
                del self._event_by_span_id[key]

            stale_pending = 0
            for parent_key, children in list(
                self._pending_children.items()
            ):
                retained = [
                    child
                    for child in children
                    if child.queued_at >= cutoff
                    and child.span_key in self._event_by_span_id
                ]
                stale_pending += len(children) - len(retained)
                if retained:
                    self._pending_children[parent_key] = retained
                else:
                    del self._pending_children[parent_key]

        if stale_spans or stale_pending:
            logger.debug(
                "Evicted %d stale spans and %d unresolved lineage entries",
                len(stale_spans),
                stale_pending,
            )

    def status(self) -> Dict[str, Any]:
        with self._lineage_lock:
            lineage_spans = len(self._event_by_span_id)
            pending_lineage = sum(
                len(children)
                for children in self._pending_children.values()
            )
        return {
            "graph_nodes": len(self.graph),
            "temporal_diffs": len(self.temporal),
            "event_log_size": len(self._event_log),
            "lineage_spans": lineage_spans,
            "pending_lineage": pending_lineage,
            "causal_rules": (
                len(self.causal.rule_names())
                if self.causal is not None
                else 0
            ),
            "semantic_labels": self.semantic.all_labels(),
            "running": self._running,
        }

    def __repr__(self) -> str:
        return f"<Engine graph={self.graph} temporal={self.temporal}>"

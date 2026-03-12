"""
The NormalizedEvent is the only data interface between probes and the engine.
Probe-specific fields do not cross this boundary to the engine.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

# core/event_schema.py

class ProbeTypeRegistry:
    """
    Open registry of probe type strings.

    The core registers its built-in types at import time.
    Users register their own types from their application code.
    Third-party libraries register their types when installed.

    Registration is purely for:
        - IDE autocompletion via __contains__ and all()
        - REPL \probes command listing
        - Validation warnings (unknown types are warned, not rejected)

    The NormalizedEvent.probe field is typed as str — any string
    is accepted at runtime regardless of registration. Registration
    makes known types visible, not mandatory.
    """

    def __init__(self):
        self._types: dict[str, str] = {}   # name → description

    def register(self, probe_type: str, description: str = "") -> str:
        """
        Register a probe type string.
        Returns the string so it can be used as a constant:

            MY_PROBE = ProbeTypes.register("myapp.thing.start", "Thing started")
        """
        self._types[probe_type] = description
        return probe_type

    def register_many(self, types: dict[str, str]) -> None:
        """Register multiple types at once from a dict of name → description."""
        self._types.update(types)

    def all(self) -> dict[str, str]:
        return dict(self._types)

    def __contains__(self, probe_type: str) -> bool:
        return probe_type in self._types

    def __repr__(self):
        return f"<ProbeTypeRegistry {len(self._types)} types>"


ProbeTypes = ProbeTypeRegistry()

# Built-in types registered at import time
ProbeTypes.register_many({
    # HTTP lifecycle
    "request.entry":              "HTTP request received",
    "request.exit":               "HTTP response sent",
    # Django
    "django.middleware.enter":    "Django middleware chain entered",
    "django.middleware.exit":     "Django middleware chain exited",
    "django.url.resolve":         "URL resolved to view",
    "django.view.enter":          "View function entered",
    "django.view.exit":           "View function returned",
    # asyncio
    "asyncio.task.create":        "asyncio.create_task() called",
    "asyncio.task.block":         "Coroutine blocked on awaitable",
    "asyncio.task.wakeup":        "Coroutine resumed from await",
    "asyncio.loop.tick":          "Event loop Task.__step fired",
    "asyncio.timer.schedule":     "Timer scheduled on event loop",
    "asyncio.selector.event":     "I/O selector event fired",
    # Python function level
    "function.call":              "Traced function entered",
    "function.return":            "Traced function returned",
    "function.exception":         "Traced function raised exception",
    # Kernel
    "syscall.enter":              "Syscall entered",
    "syscall.exit":               "Syscall returned",
    "tcp.send":                   "TCP send (kernel level)",
    "tcp.recv":                   "TCP receive (kernel level)",
    # Database
    "db.query.start":             "Database query started",
    "db.query.end":               "Database query completed",
    # Uvicorn
    "uvicorn.request.receive":    "ASGI scope ready, app about to be called",
    "uvicorn.response.send":      "Response headers sent",
    "uvicorn.h11.cycle":          "H11 request/response cycle complete",
    "uvicorn.httptools.cycle":    "httptools request/response cycle complete",
    # Gunicorn
    "gunicorn.worker.spawn":      "Arbiter spawned a new worker",
    "gunicorn.worker.init":       "Worker process initialised",
    "gunicorn.worker.exit":       "Worker process exiting",
    "gunicorn.request.handle":    "Sync worker handling request",
    "gunicorn.worker.heartbeat":  "Worker heartbeat to master",
    # Nginx
    "nginx.connection.accept":    "Connection accepted, waiting first byte",
    "nginx.request.parse":        "HTTP request parsing started",
    "nginx.request.route":        "Request routed to location block",
    "nginx.recv":                 "nginx reading socket data",
    "nginx.upstream.dispatch":    "Dispatched to upstream",
    "nginx.epoll.tick":           "nginx epoll loop iteration",
    # Generic
    "custom":                     "User-defined event",
})

@dataclass
class NormalizedEvent:
    """
    The semantic observation from any probe.
    This crosses the probe↔engine boundary as the sole data contract.

    Fields
    ------
    probe:
        Which type of observation this is.
    service: 
        Logical service name (e.g. "django", "nginx", "postgres").
    name        : The specific entity (route, function, syscall, query text…).
    trace_id    : Ties all events in one request together.
    timestamp   : perf_counter() at moment of emission. Use wall clock for display.
    wall_time   : time.time() for human-readable timestamps.
    duration_ns : Optional measured duration (e.g. syscall wall time).
    span_id     : OpenTelemetry-compatible span identifier.
    parent_span_id : Parent span for distributed tracing linkage.
    pid         : OS process ID (populated by kernel-level probes).
    tid         : OS thread ID.
    metadata    : Probe-specific payload. Anything that doesn't fit above.
    """

    probe: str                           # ProbeType (kept as str for extensibility)
    service: str
    name: str
    trace_id: str

    timestamp: float = field(default_factory=time.perf_counter)
    wall_time: float = field(default_factory=time.time)

    duration_ns: Optional[int] = None
    span_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    parent_span_id: Optional[str] = None

    pid: Optional[int] = None
    tid: Optional[int] = None

    metadata: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def now(
        probe: str,
        trace_id: str,
        service: str,
        name: str,
        parent_span_id: Optional[str] = None,
        duration_ns: Optional[int] = None,   # ← extract explicitly
        pid: Optional[int] = None,           # ← same for pid/tid if probes pass them
        tid: Optional[int] = None,
        **metadata: Any,
    ) -> "NormalizedEvent":
        """
        Constructor which captures other metadata such as timestamps at call site.
        """
        return NormalizedEvent(
            probe          = probe,
            service        = service,
            name           = name,
            trace_id       = trace_id,
            parent_span_id = parent_span_id,
            duration_ns    = duration_ns,    # ← lands on the dataclass field
            pid            = pid,
            tid            = tid,
            metadata       = metadata,       # ← only genuinely unknown kwargs
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "probe": self.probe,
            "service": self.service,
            "name": self.name,
            "trace_id": self.trace_id,
            "timestamp": self.timestamp,
            "wall_time": self.wall_time,
            "duration_ns": self.duration_ns,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "pid": self.pid,
            "tid": self.tid,
            "metadata": self.metadata,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "NormalizedEvent":
        meta = {k: v for k, v in d.items() if k not in NormalizedEvent.__dataclass_fields__}
        return NormalizedEvent(
            probe=d["probe"],
            service=d["service"],
            name=d["name"],
            trace_id=d["trace_id"],
            timestamp=d.get("timestamp", time.perf_counter()),
            wall_time=d.get("wall_time", time.time()),
            duration_ns=d.get("duration_ns"),
            span_id=d.get("span_id", uuid.uuid4().hex[:16]),
            parent_span_id=d.get("parent_span_id"),
            pid=d.get("pid"),
            tid=d.get("tid"),
            metadata=d.get("metadata", meta),
        )

    def __repr__(self) -> str:
        dur = f" {self.duration_ns / 1_000:.1f}µs" if self.duration_ns else ""
        return f"<Event [{self.probe}] {self.service}::{self.name}{dur} trace={self.trace_id[:15]}>"

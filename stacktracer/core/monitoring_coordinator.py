"""
core/monitoring_coordinator.py

Central coordinator for sys.monitoring (Python 3.12+).

The problem this solves:
    sys.monitoring has a fixed set of tool slots (PROFILER_ID, DEBUGGER_ID etc).
    Each slot can only be claimed once per process. If two probes both call
    sys.monitoring.set_events(PROFILER_ID, ...) independently, the second
    call raises "tool N is not in use" because the first probe already
    owns the slot.

    Both the django probe (view function observation) and the asyncio probe
    (coroutine observation) need CALL and PY_RETURN events. Without a
    coordinator they fight over the same slot and one of them always loses.

The solution:
    One coordinator claims the tool slot once. Probes register named
    callback pairs (on_call, on_return) with the coordinator. The
    coordinator's own callbacks fan out to every registered handler.

    Only the coordinator ever touches sys.monitoring directly.
    Probes call coordinator.register() and coordinator.unregister().

Usage:
    # In probe.start():
    from stacktracer.core.monitoring_coordinator import get_coordinator
    coord = get_coordinator()
    coord.register("django", on_call=my_call_handler, on_return=my_return_handler)

    # In probe.stop():
    coord.unregister("django")

    # Coordinator claims the slot on first register(), releases when
    # last handler unregisters.

Thread safety:
    _handlers dict protected by RLock.
    sys.monitoring callbacks fire from the interpreter — no lock held
    during dispatch to avoid deadlock.
"""

from __future__ import annotations

import logging
import sys
import threading
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger("stacktracer.core.monitoring")

# Type for handler pairs
HandlerPair = Tuple[Optional[Callable], Optional[Callable]]


class MonitoringCoordinator:
    """
    Owns one sys.monitoring tool slot and fans out CALL / PY_RETURN
    events to all registered probe handlers.

    Lifecycle:
        First register()  → claims the tool slot, installs callbacks
        Last unregister() → releases the slot, removes callbacks
    """

    def __init__(self, tool_id: int) -> None:
        self._tool_id = tool_id
        self._lock = threading.RLock()
        # name → (on_call, on_return)  — both optional
        self._handlers: Dict[str, HandlerPair] = {}
        self._active = False

    # ------------------------------------------------------------------ #
    # Public API for probes
    # ------------------------------------------------------------------ #

    def register(
        self,
        name: str,
        on_call: Optional[Callable] = None,
        on_return: Optional[Callable] = None,
    ) -> None:
        """
        Register a named handler pair.
        Claims the sys.monitoring slot on first registration.
        """
        if not hasattr(sys, "monitoring"):
            logger.debug(
                "monitoring_coordinator: sys.monitoring not available — skipping %s",
                name,
            )
            return

        with self._lock:
            self._handlers[name] = (on_call, on_return)
            if not self._active:
                self._claim()

        logger.info(
            "monitoring_coordinator: registered handler '%s'",
            name,
        )

    def unregister(self, name: str) -> None:
        """
        Unregister a named handler.
        Releases the sys.monitoring slot when no handlers remain.
        """
        if not hasattr(sys, "monitoring"):
            return

        with self._lock:
            self._handlers.pop(name, None)
            if self._active and not self._handlers:
                self._release()

        logger.info(
            "monitoring_coordinator: unregistered handler '%s'",
            name,
        )

    def is_registered(self, name: str) -> bool:
        with self._lock:
            return name in self._handlers

    # ------------------------------------------------------------------ #
    # Internal — slot management
    # ------------------------------------------------------------------ #

    def _claim(self) -> None:
        """Claim the tool slot and install fan-out callbacks."""
        try:
            sys.monitoring.use_tool_id(self._tool_id, "stacktracer")
        except Exception as exc:
            logger.warning(
                "monitoring_coordinator: could not claim tool slot %d: %s",
                self._tool_id,
                exc,
            )
            return

        events = sys.monitoring.events
        try:
            sys.monitoring.set_events(
                self._tool_id,
                events.CALL | events.PY_RETURN,
            )
        except Exception as exc:
            logger.warning(
                "monitoring_coordinator: set_events failed: %s",
                exc,
            )
            try:
                sys.monitoring.free_tool_id(self._tool_id)
            except Exception:
                pass
            return

        try:
            sys.monitoring.register_callback(
                self._tool_id,
                events.CALL,
                self._dispatch_call,
            )
            sys.monitoring.register_callback(
                self._tool_id,
                events.PY_RETURN,
                self._dispatch_return,
            )
        except Exception as exc:
            logger.warning(
                "monitoring_coordinator: register_callback failed: %s",
                exc,
            )
            return

        self._active = True
        logger.info(
            "monitoring_coordinator: claimed sys.monitoring PROFILER_ID (tool %d)",
            self._tool_id,
        )

    def _release(self) -> None:
        """Release the tool slot and remove callbacks."""
        events = sys.monitoring.events
        try:
            sys.monitoring.set_events(self._tool_id, events.NO_EVENTS)
            sys.monitoring.register_callback(self._tool_id, events.CALL, None)
            sys.monitoring.register_callback(self._tool_id, events.PY_RETURN, None)
            sys.monitoring.free_tool_id(self._tool_id)
        except Exception as exc:
            logger.debug("monitoring_coordinator: release error: %s", exc)
        self._active = False
        logger.info("monitoring_coordinator: released sys.monitoring tool slot")

    # ------------------------------------------------------------------ #
    # Fan-out callbacks — called by sys.monitoring
    # ------------------------------------------------------------------ #

    def _dispatch_call(self, code: Any, offset: int, callable_: Any, arg0: Any) -> Any:
        """
        Fan out CALL events to all registered on_call handlers.
        Returns DISABLE only if ALL handlers returned DISABLE for this code object.
        If any handler wants the event, we keep delivering it.
        """
        disable_count = 0
        total = 0

        # Read handlers without lock — avoid deadlock in monitoring callback
        handlers = list(self._handlers.values())

        for on_call, _ in handlers:
            if on_call is None:
                continue
            total += 1
            try:
                result = on_call(code, offset, callable_, arg0)
                if result is sys.monitoring.DISABLE:
                    disable_count += 1
            except Exception as exc:
                logger.debug("monitoring on_call handler error: %s", exc)

        # Only DISABLE if every interested handler said DISABLE
        if total > 0 and disable_count == total:
            return sys.monitoring.DISABLE

    def _dispatch_return(self, code: Any, offset: int, retval: Any) -> Any:
        """Fan out PY_RETURN events to all registered on_return handlers."""
        disable_count = 0
        total = 0

        handlers = list(self._handlers.values())

        for _, on_return in handlers:
            if on_return is None:
                continue
            total += 1
            try:
                result = on_return(code, offset, retval)
                if result is sys.monitoring.DISABLE:
                    disable_count += 1
            except Exception as exc:
                logger.debug("monitoring on_return handler error: %s", exc)

        if total > 0 and disable_count == total:
            return sys.monitoring.DISABLE


# ====================================================================== #
# Module-level singleton
# ====================================================================== #

_coordinator: Optional[MonitoringCoordinator] = None
_coordinator_lock = threading.Lock()


def get_coordinator() -> MonitoringCoordinator:
    """
    Returns the process-wide MonitoringCoordinator singleton.
    Safe to call from multiple threads — initialises lazily on first call.
    """
    global _coordinator
    if _coordinator is None:
        with _coordinator_lock:
            if _coordinator is None:
                if hasattr(sys, "monitoring"):
                    tool_id = sys.monitoring.PROFILER_ID
                else:
                    tool_id = 2  # fallback — will no-op if monitoring unavailable
                _coordinator = MonitoringCoordinator(tool_id)
    return _coordinator

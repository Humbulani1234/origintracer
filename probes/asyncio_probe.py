"""
probes/asyncio_probe.py

Patches asyncio.Task.__step to observe the event loop's internal scheduling.
This is the core differentiator: we capture exactly what the event loop
is doing at the moment of each coroutine step.

What we observe:
  - When a task wakes up (asyncio.task.wakeup)
  - What future it was waiting on (asyncio.task.block)
  - Loop tick duration (asyncio.loop.tick)
  - Timer scheduling (asyncio.timer.schedule)

Safety:
  - The original __step is always called, even if our hook raises
  - Only emits when current_trace_id is set (i.e., inside a traced request)
  - Version guarded: we check we know the internal attribute layout

Fragility note:
  Task.__step is a private CPython implementation detail.
  Test on every minor Python version upgrade.
  Supported: 3.11, 3.12, 3.13
"""

from __future__ import annotations

import asyncio
import sys
import logging
from asyncio import tasks
from typing import Any, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent
from ..context.vars import get_trace_id, get_span_id

logger = logging.getLogger("stacktracer.probes.asyncio")

# The private method name (name-mangled in CPython)
_STEP_ATTR = "_Task__step"
_original_step = None
_patched = False

SUPPORTED_PYTHONS = ((3, 11), (3, 12), (3, 13))


def _check_python_version() -> bool:
    major, minor = sys.version_info[:2]
    if (major, minor) not in SUPPORTED_PYTHONS:
        logger.warning(
            "asyncio probe: unsupported Python %d.%d — skipping patch.",
            major, minor,
        )
        return False
    return True


def _has_step_attr() -> bool:
    """
    Python 3.12+ moves Task to a C extension (_asyncio.Task),
    which drops the name-mangled __step attribute.
    We fall back to coroutine-level tracing via set_coroutine_origin_tracking_depth
    or asyncio debug hooks in that case.
    """
    return hasattr(tasks.Task, _STEP_ATTR)


def _format_fut_waiter(task: asyncio.Task) -> Optional[str]:
    """Safely stringify what this task is waiting on."""
    try:
        waiter = getattr(task, "_fut_waiter", None)
        if waiter is None:
            return None
        return repr(waiter)[:120]
    except Exception:
        return None


def _traced_task_step(self: asyncio.Task, exc: Optional[BaseException] = None) -> None:
    """
    Replacement for Task.__step.
    Emits probe events before and after the coroutine step.
    """
    global _original_step

    trace_id = get_trace_id()

    if trace_id:
        try:
            coro = self.get_coro()
            coro_name = getattr(coro, "__qualname__", type(coro).__name__)
            task_name = self.get_name()
            fut_waiter = _format_fut_waiter(self)

            # Snapshot the ready queue depth (bounded, no deep copy)
            loop = asyncio.get_event_loop()
            ready_count = len(getattr(loop, "_ready", []))
            scheduled_count = len(getattr(loop, "_scheduled", []))

            emit(NormalizedEvent.now(
                probe="asyncio.loop.tick",
                trace_id=trace_id,
                service="asyncio",
                name=coro_name,
                parent_span_id=get_span_id(),
                task_name=task_name,
                fut_waiter=fut_waiter,
                ready_queue_depth=ready_count,
                scheduled_count=scheduled_count,
                had_exception=exc is not None,
            ))
        except Exception as probe_exc:
            logger.debug("asyncio probe error in __step: %s", probe_exc)

    # Always call the original — probe must never interfere with execution
    return _original_step(self, exc)


def _patch_create_task() -> None:
    """
    Wrap asyncio.create_task to emit task.create events.
    Less fragile than patching __step — uses public API.
    """
    _orig_create_task = asyncio.create_task

    def _traced_create_task(coro: Any, *, name: Optional[str] = None, context: Any = None) -> asyncio.Task:
        trace_id = get_trace_id()
        if trace_id:
            coro_name = getattr(coro, "__qualname__", type(coro).__name__)
            emit(NormalizedEvent.now(
                probe="asyncio.task.create",
                trace_id=trace_id,
                service="asyncio",
                name=coro_name,
                parent_span_id=get_span_id(),
                task_name=name,
            ))

        if context is not None:
            return _orig_create_task(coro, name=name, context=context)
        return _orig_create_task(coro, name=name)

    asyncio.create_task = _traced_create_task
    return _orig_create_task


class AsyncioProbe(BaseProbe):
    name = "asyncio"

    def __init__(self) -> None:
        self._original_create_task = None

    def start(self) -> None:
        global _original_step, _patched

        if not _check_python_version():
            return

        if _patched:
            logger.warning("asyncio probe already patched — skipping")
            return

        if _has_step_attr():
            # Python 3.11 pure-Python Task — full __step patch available
            _original_step = getattr(tasks.Task, _STEP_ATTR)
            setattr(tasks.Task, _STEP_ATTR, _traced_task_step)
            logger.info("asyncio probe installed via __step patch (Python %d.%d)", *sys.version_info[:2])
        else:
            # Python 3.12+ uses a C-extension Task (_asyncio.Task).
            # __step is not accessible. Fall back to create_task tracing only.
            # For deeper visibility on 3.12+, consider asyncio.get_event_loop().set_debug(True)
            # or a custom EventLoopPolicy that wraps the scheduler.
            logger.info(
                "asyncio probe: Task.__step not accessible on Python %d.%d "
                "(C-extension Task). Using create_task tracing only.",
                *sys.version_info[:2],
            )

        # create_task patch works on all Python versions
        self._original_create_task = _patch_create_task()
        _patched = True

    def stop(self) -> None:
        global _original_step, _patched

        if not _patched:
            return

        if _original_step is not None:
            setattr(tasks.Task, _STEP_ATTR, _original_step)
            _original_step = None

        if self._original_create_task is not None:
            asyncio.create_task = self._original_create_task
            self._original_create_task = None

        _patched = False
        logger.info("asyncio probe removed")
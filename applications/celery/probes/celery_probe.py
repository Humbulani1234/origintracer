"""
stacktracer/probes/celery_probe.py

Custom Celery probe for StackTracer.

Auto-discovered by StackTracer because:
  - This file is named *_probe.py
  - It lives in stacktracer/probes/ in the project root
  - It contains a class named CeleryProbe extending BaseProbe

Patching strategy:
    Celery provides a signal system (task_prerun, task_postrun,
    task_retry, task_failure, beat_init) that is stable public API.
    We connect to signals in start() and disconnect in stop().

    This is safer than patching Celery internals because:
        - Signals are documented and versioned
        - They survive Celery version upgrades
        - No class-level patching required

Celery worker architecture:
    Each Celery worker is a separate OS process (prefork pool by default).
    StackTracer is initialised in each worker via the post-fork mechanism
    in settings.py (or celery.py signals). Context variables (ContextVar)
    are process-local, so trace IDs do not leak between workers.

Trace ID for Celery tasks:
    Celery tasks do not carry HTTP trace IDs by default.
    We use the Celery task_id (a UUID generated per task invocation)
    as the trace_id. This means each task has its own trace.

    To correlate Celery tasks with the HTTP request that triggered them:
        1. Pass the HTTP trace_id as a task kwarg: task.delay(trace_id=tid)
        2. Read it in the signal handler: kwargs.get("trace_id") or task_id
    This is shown in the signal handlers below.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

from stacktracer.sdk.base_probe import BaseProbe
from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.context.vars import set_trace, reset_trace

# Import types — registration happens here as a side effect
from .celery_types import (
    TASK_START, TASK_END, TASK_RETRY, TASK_FAILURE, BEAT_TICK
)

logger = logging.getLogger("stacktracer.probes.celery")

# Per-task timing: task_id → start time (perf_counter)
# Thread-safe for prefork workers (one dict per process)
_task_start_times: dict[str, float] = {}


class CeleryProbe(BaseProbe):
    """
    Observes the Celery task lifecycle via Celery's signal system.

    Emits:
        celery.task.start    task_prerun signal
        celery.task.end      task_postrun signal (with duration)
        celery.task.retry    task_retry signal
        celery.task.failure  task_failure signal
        celery.beat.tick     beat_init signal (beat process only)
    """
    name = "celery"

    def start(self) -> None:
        try:
            from celery.signals import (
                task_prerun,
                task_postrun,
                task_retry,
                task_failure,
                celeryd_after_setup,
            )
        except ImportError:
            logger.warning(
                "celery not installed — celery probe inactive. "
                "(pip install celery)"
            )
            return

        task_prerun.connect(self._on_task_start,   weak=False)
        task_postrun.connect(self._on_task_end,     weak=False)
        task_retry.connect(self._on_task_retry,     weak=False)
        task_failure.connect(self._on_task_failure, weak=False)
        celeryd_after_setup.connect(self._on_worker_ready, weak=False)

        logger.info(
            "celery probe connected to signals "
            "(task_prerun, task_postrun, task_retry, task_failure)"
        )

    def stop(self) -> None:
        try:
            from celery.signals import (
                task_prerun, task_postrun, task_retry,
                task_failure, celeryd_after_setup,
            )
        except ImportError:
            return

        task_prerun.disconnect(self._on_task_start)
        task_postrun.disconnect(self._on_task_end)
        task_retry.disconnect(self._on_task_retry)
        task_failure.disconnect(self._on_task_failure)
        celeryd_after_setup.disconnect(self._on_worker_ready)

        logger.info("celery probe disconnected from signals")

    # ------------------------------------------------------------------ #
    # Signal handlers
    # ------------------------------------------------------------------ #

    def _on_task_start(
        self,
        task_id: str,
        task: Any,
        args: tuple,
        kwargs: dict,
        **signal_kwargs,
    ) -> None:
        """
        Fires at the start of every task execution.

        task_id  — unique ID for this task invocation (UUID)
        task     — the Task instance (has .name, .max_retries, .request)
        kwargs   — task keyword arguments (may contain forwarded trace_id)
        """
        # Allow HTTP trace ID to be forwarded through task kwargs
        trace_id = kwargs.get("_trace_id") or task_id
        token = set_trace(trace_id)

        _task_start_times[task_id] = time.perf_counter()

        # Store token on task request so _on_task_end can reset it
        task.request._stacktracer_token = token

        emit(NormalizedEvent.now(
            probe=TASK_START,
            trace_id=trace_id,
            service="celery",
            name=task.name,
            task_id=task_id,
            worker_pid=os.getpid(),
            retries=task.request.retries,
            max_retries=task.max_retries,
            args_count=len(args),
            kwargs_count=len(kwargs),
        ))

    def _on_task_end(
        self,
        task_id: str,
        task: Any,
        args: tuple,
        kwargs: dict,
        retval: Any,
        state: str,
        **signal_kwargs,
    ) -> None:
        """
        Fires after every task execution regardless of outcome.

        state — "SUCCESS", "FAILURE", "RETRY", etc.
        retval — return value (or exception on failure)
        """
        trace_id = kwargs.get("_trace_id") or task_id

        start = _task_start_times.pop(task_id, None)
        duration_ns = int((time.perf_counter() - start) * 1e9) if start else None

        emit(NormalizedEvent.now(
            probe=TASK_END,
            trace_id=trace_id,
            service="celery",
            name=task.name,
            task_id=task_id,
            worker_pid=os.getpid(),
            state=state,
            retries=task.request.retries,
            duration_ns=duration_ns,
        ))

        # Reset the trace context bound in _on_task_start
        token = getattr(task.request, "_stacktracer_token", None)
        if token is not None:
            reset_trace(token)

    def _on_task_retry(
        self,
        request: Any,
        reason: Any,
        einfo: Any,
        **signal_kwargs,
    ) -> None:
        """
        Fires when a task is about to be retried.
        `reason` is the exception that caused the retry.
        """
        task_id = request.id
        task_name = request.task
        trace_id = task_id

        emit(NormalizedEvent.now(
            probe=TASK_RETRY,
            trace_id=trace_id,
            service="celery",
            name=task_name,
            task_id=task_id,
            worker_pid=os.getpid(),
            reason=str(reason)[:200],
            retries=request.retries,
        ))

    def _on_task_failure(
        self,
        task_id: str,
        exception: Exception,
        traceback: Any,
        einfo: Any,
        args: tuple,
        kwargs: dict,
        **signal_kwargs,
    ) -> None:
        """
        Fires when a task fails permanently (all retries exhausted or
        exception not configured for retry).
        """
        trace_id = kwargs.get("_trace_id") or task_id

        emit(NormalizedEvent.now(
            probe=TASK_FAILURE,
            trace_id=trace_id,
            service="celery",
            name=getattr(exception, "__class__", type(exception)).__name__,
            task_id=task_id,
            worker_pid=os.getpid(),
            exception_type=type(exception).__name__,
            exception_msg=str(exception)[:200],
        ))

    def _on_worker_ready(self, sender: Any, **signal_kwargs) -> None:
        """
        Fires once when the worker process is fully initialised.
        Useful for confirming the probe is active in each worker.
        """
        logger.info(
            "celery probe active in worker (pid=%d hostname=%s)",
            os.getpid(),
            getattr(sender, "hostname", "unknown"),
        )
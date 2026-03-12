"""
stacktracer/probes/celery_probe.py

Custom Celery probe for StackTracer — user extension mechanism demo.

Auto-discovered by StackTracer because:
  - This file is named *_probe.py
  - It lives in stacktracer/probes/ in the project root
  - It contains a class named CeleryProbe extending BaseProbe

Patching strategy:
    Celery provides a signal system (task_prerun, task_postrun,
    task_retry, task_failure) that is stable public API.
    We connect to signals in start() and disconnect in stop().

Fork re-init:
    worker_process_init fires inside each forked prefork worker before
    it starts consuming tasks. We re-init StackTracer there so each
    worker gets its own engine + socket. Identical to gunicorn post_fork.

Trace continuity:
    Django views call dispatch() which passes _trace_id in task kwargs.
    _on_task_start reads it and continues the trace in the worker process.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from stacktracer.sdk.base_probe import BaseProbe
from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.context.vars import set_trace, reset_trace

# Inline constants — no relative imports (file loaded via spec_from_file_location)
TASK_START   = "celery.task.start"
TASK_END     = "celery.task.end"
TASK_RETRY   = "celery.task.retry"
TASK_FAILURE = "celery.task.failure"

logger = logging.getLogger("stacktracer.probes.celery")

# Single per-task state dict — keyed by task_id, one dict per process.
# Stores trace_id, token, task name, and start time together so every
# handler reads from the same source. _on_task_end owns cleanup.
_task_state: dict[str, dict] = {}


class CeleryProbe(BaseProbe):
    """
    Observes the Celery task lifecycle via Celery's signal system.

    Emits:
        celery.task.start    task_prerun signal
        celery.task.end      task_postrun signal (with duration)
        celery.task.retry    task_retry signal
        celery.task.failure  task_failure signal
        celery.worker.fork   worker_process_init (re-init confirmation)
    """
    name = "celery"

    def start(self, **kwargs) -> None:
        try:
            from celery.signals import (
                task_prerun,
                task_postrun,
                task_retry,
                task_failure,
                worker_process_init,
                worker_process_shutdown,
            )
        except ImportError:
            logger.warning(
                "celery not installed — celery probe inactive. "
                "(pip install celery)"
            )
            return

        task_prerun.connect(self._on_task_start,          weak=False)
        task_postrun.connect(self._on_task_end,           weak=False)
        task_retry.connect(self._on_task_retry,           weak=False)
        task_failure.connect(self._on_task_failure,       weak=False)
        worker_process_init.connect(self._on_worker_fork, weak=False)
        worker_process_shutdown.connect(self._on_worker_exit, weak=False)

        logger.info(
            "celery probe: signals connected "
            "(task_prerun, task_postrun, task_retry, task_failure, "
            "worker_process_init, worker_process_shutdown)"
        )

    def stop(self, **kwargs) -> None:
        try:
            from celery.signals import (
                task_prerun, task_postrun, task_retry, task_failure,
                worker_process_init, worker_process_shutdown,
            )
        except ImportError:
            return

        task_prerun.disconnect(self._on_task_start)
        task_postrun.disconnect(self._on_task_end)
        task_retry.disconnect(self._on_task_retry)
        task_failure.disconnect(self._on_task_failure)
        worker_process_init.disconnect(self._on_worker_fork)
        worker_process_shutdown.disconnect(self._on_worker_exit)

        logger.info("celery probe: signals disconnected")

    # ------------------------------------------------------------------ #
    # Signal handlers
    # ------------------------------------------------------------------ #

    def _on_task_start(self, task_id: str, task: Any, args: tuple,
                       kwargs: dict, **_) -> None:
        """
        Fires inside the worker before task execution.
        Reads _trace_id forwarded from the Django view, or falls back
        to task_id for tasks dispatched outside a request context.
        """
        trace_id = kwargs.get("_trace_id") or task_id
        token    = set_trace(trace_id)

        # Store everything in one place — all other handlers read from here
        _task_state[task_id] = {
            "trace_id": trace_id,
            "token":    token,
            "name":     task.name,
            "t0":       time.perf_counter(),
        }

        emit(NormalizedEvent.now(
            probe       = TASK_START,
            trace_id    = trace_id,
            service     = "celery",
            name        = task.name,
            task_id     = task_id,
            worker_pid  = os.getpid(),
            retries     = task.request.retries,
            max_retries = task.max_retries,
        ))

    def _on_task_end(self, task_id: str, task: Any, args: tuple,
                     kwargs: dict, retval: Any, state: str, **_) -> None:
        """
        Fires after every task execution regardless of outcome.
        This is the ONLY handler that resets the token and pops state.
        task_postrun always fires after task_failure, so cleanup is safe here.
        """
        state_data  = _task_state.pop(task_id, {})
        trace_id    = state_data.get("trace_id") or kwargs.get("_trace_id") or task_id
        t0          = state_data.get("t0")
        duration_ns = int((time.perf_counter() - t0) * 1e9) if t0 else None

        emit(NormalizedEvent.now(
            probe       = TASK_END,
            trace_id    = trace_id,
            service     = "celery",
            name        = task.name,
            task_id     = task_id,
            worker_pid  = os.getpid(),
            state       = state,
            retries     = task.request.retries,
            duration_ns = duration_ns,
        ))

        token = state_data.get("token")
        if token is not None:
            try:
                reset_trace(token)
            except RuntimeError:
                pass  # already reset — harmless

    def _on_task_retry(self, request: Any, reason: Any, einfo: Any,
                       **_) -> None:
        emit(NormalizedEvent.now(
            probe      = TASK_RETRY,
            trace_id   = request.id,
            service    = "celery",
            name       = request.task,
            task_id    = request.id,
            worker_pid = os.getpid(),
            reason     = str(reason)[:200],
            retries    = request.retries,
        ))

    def _on_task_failure(self, task_id: str, exception: Exception,
                         traceback: Any, einfo: Any, args: tuple,
                         kwargs: dict, **_) -> None:
        """
        Fires on permanent failure. Reads state but does NOT pop or
        reset token — _on_task_end (task_postrun) always fires after
        this and is the single owner of cleanup.
        """
        state_data = _task_state.get(task_id, {})
        trace_id   = state_data.get("trace_id") or kwargs.get("_trace_id") or task_id
        task_name  = state_data.get("name", task_id)

        emit(NormalizedEvent.now(
            probe          = TASK_FAILURE,
            trace_id       = trace_id,
            service        = "celery",
            name           = task_name,
            task_id        = task_id,
            worker_pid     = os.getpid(),
            exception_type = type(exception).__name__,
            exception_msg  = str(exception)[:200],
        ))
        # token reset intentionally omitted — _on_task_end owns it

    def _on_worker_fork(self, **_) -> None:
        """
        Fires inside each forked prefork worker before it starts
        consuming tasks. Re-inits StackTracer so this worker gets
        its own engine + /tmp/stacktracer-{pid}.sock.
        Identical role to gunicorn's st_post_fork.
        """
        import stacktracer

        worker_pid  = os.getpid()
        master_pid  = os.getppid()
        config_path = os.environ.get("STACKTRACER_CONFIG", "stacktracer.yaml")

        try:
            stacktracer.init(config=config_path)
        except Exception as exc:
            logger.warning(
                "celery probe: StackTracer re-init failed in worker "
                "pid=%d: %s", worker_pid, exc,
            )
            return

        emit(NormalizedEvent.now(
            probe      = "celery.worker.fork",
            trace_id   = f"celery-worker-{worker_pid}",
            service    = "celery",
            name       = "ForkPoolWorker",
            worker_pid = worker_pid,
            master_pid = master_pid,
        ))

        logger.info(
            "celery probe: worker re-initialised (pid=%d master=%d)",
            worker_pid, master_pid,
        )

    def _on_worker_exit(self, **_) -> None:
        logger.info("celery probe: worker exiting (pid=%d)", os.getpid())
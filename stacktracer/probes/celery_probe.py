"""
probes/celery_probe.py

Observes Celery using its documented signal system — the official,
stable extension point. No class patching, no private internals.

Signals used:
    task_prerun          task is about to execute in a worker
    task_postrun         task finished (success or failure)
    task_retry           task scheduled for retry
    task_failure         task raised permanently (all retries exhausted)
    worker_process_init  worker process forked and ready (prefork pool)
    worker_process_shutdown  worker process about to exit

Fork problem — same as gunicorn:
    Celery prefork pool forks the main process to create workers.
    Each worker inherits a stale copy of the engine from the parent.
    emit() in ForkPoolWorker-N writes into that worker's private copy —
    invisible to the REPL, which is connected to the main process socket.

    Fix: worker_process_init fires inside each forked worker before it
    starts consuming tasks. We re-init StackTracer there, giving each
    worker its own engine and /tmp/stacktracer-{pid}.sock.
    Identical to gunicorn's post_fork pattern.

Trace continuity across the broker:
    A Django view dispatches a task via .delay(). The task executes in
    a separate process with no ContextVar state. To keep the trace
    continuous, the view emits a celery.task.dispatch event and passes
    _trace_id in task kwargs. The probe reads it in task_prerun and
    continues the same trace in the worker.

    Graph topology produced:
        django::SomeView
            → celery::myapp.tasks.some_task    (dispatch edge)
                → celery::ForkPoolWorker        (fork edge)

Usage — add to stacktracer.yaml:
    probes:
      - celery

Dispatch from Django views:
    from stacktracer.probes.celery_probe import dispatch
    from stacktracer.context.vars import get_trace_id

    dispatch(my_task, get_trace_id(), kwarg1=value1)
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..context.vars import set_trace, reset_trace

logger = logging.getLogger("stacktracer.probes.celery")

ProbeTypes.register_many({
    "celery.task.dispatch":  "Django view dispatched a Celery task",
    "celery.task.start":     "Celery task execution started in worker",
    "celery.task.end":       "Celery task completed (success or failure)",
    "celery.task.retry":     "Celery task scheduled for retry",
    "celery.task.failure":   "Celery task raised permanently",
    "celery.worker.fork":    "Celery worker process forked and re-initialised",
})

# Per-task state — keyed by task_id, one dict per process (prefork safe)
_task_state: dict[str, dict] = {}


# ====================================================================== #
# Signal handlers — plain functions, connected in CeleryProbe.start()
# ====================================================================== #

def _on_task_start(task_id: str, task: Any, args: tuple,
                   kwargs: dict, **_) -> None:
    """
    Fires inside the worker process before task execution.
    Reads _trace_id forwarded from the Django view, or falls back
    to task_id so every task has a trace root even when dispatched
    outside a request (management commands, beat scheduler).
    """
    import time
    trace_id = kwargs.get("_trace_id") or task_id
    token    = set_trace(trace_id)

    _task_state[task_id] = {
        "trace_id": trace_id,
        "token":    token,
        "name":     task.name,
        "t0":       time.perf_counter(),
    }

    emit(NormalizedEvent.now(
        probe       = "celery.task.start",
        trace_id    = trace_id,
        service     = "celery",
        name        = task.name,
        task_id     = task_id,
        worker_pid  = os.getpid(),
        retries     = task.request.retries,
        max_retries = task.max_retries,
    ))


def _on_task_end(task_id: str, task: Any, args: tuple, kwargs: dict,
                 retval: Any, state: str, **_) -> None:
    """
    Fires inside the worker process after task execution regardless of outcome.
    """
    import time
    state_data  = _task_state.pop(task_id, {})
    trace_id    = state_data.get("trace_id") or kwargs.get("_trace_id") or task_id
    t0          = state_data.get("t0")
    duration_ns = int((time.perf_counter() - t0) * 1e9) if t0 else None

    emit(NormalizedEvent.now(
        probe       = "celery.task.end",
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
        reset_trace(token)


def _on_task_retry(request: Any, reason: Any, einfo: Any, **_) -> None:
    emit(NormalizedEvent.now(
        probe      = "celery.task.retry",
        trace_id   = request.id,
        service    = "celery",
        name       = request.task,
        task_id    = request.id,
        worker_pid = os.getpid(),
        reason     = str(reason)[:200],
        retries    = request.retries,
    ))


def _on_task_failure(task_id: str, exception: Exception, traceback: Any,
                     einfo: Any, args: tuple, kwargs: dict, **_) -> None:
    # task name stored at prerun — _on_task_failure has no Task instance
    state_data = _task_state.pop(task_id, {})
    trace_id   = state_data.get("trace_id") or kwargs.get("_trace_id") or task_id
    task_name  = state_data.get("name", task_id)

    emit(NormalizedEvent.now(
        probe          = "celery.task.failure",
        trace_id       = trace_id,
        service        = "celery",
        name           = task_name,
        task_id        = task_id,
        worker_pid     = os.getpid(),
        exception_type = type(exception).__name__,
        exception_msg  = str(exception)[:200],
    ))

    token = state_data.get("token")
    if token is not None:
        reset_trace(token)


def _on_worker_fork(**_) -> None:
    """
    Fires inside each forked prefork worker before it starts consuming tasks.
    Re-inits StackTracer so this worker gets its own engine + socket.
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
            "celery probe: StackTracer re-init failed in worker pid=%d: %s",
            worker_pid, exc,
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
        "celery probe: worker forked and re-initialised (pid=%d master=%d)",
        worker_pid, master_pid,
    )


def _on_worker_exit(**_) -> None:
    logger.info("celery probe: worker exiting (pid=%d)", os.getpid())


# ====================================================================== #
# Convenience helper for Django views
# ====================================================================== #

def dispatch(task_func: Any, trace_id: str, **task_kwargs) -> None:
    """
    Emit celery.task.dispatch then call task.delay().

    Creates the django::ViewName → celery::task_name edge in the graph
    because both the dispatch event and the task_start event share the
    same trace_id.

    Usage in a Django view:
        from stacktracer.probes.celery_probe import dispatch
        from stacktracer.context.vars import get_trace_id

        dispatch(send_notification, get_trace_id(), user_id=42)
    """
    emit(NormalizedEvent.now(
        probe     = "celery.task.dispatch",
        trace_id  = trace_id,
        service   = "django",
        name      = task_func.name,
        task_name = task_func.name,
    ))
    task_func.delay(**task_kwargs, _trace_id=trace_id)


# ====================================================================== #
# CeleryProbe
# ====================================================================== #

class CeleryProbe(BaseProbe):
    """
    Observes the Celery task lifecycle via Celery's signal system.

    Hooks installed at start():
        task_prerun             → celery.task.start
        task_postrun            → celery.task.end   (with duration)
        task_retry              → celery.task.retry
        task_failure            → celery.task.failure
        worker_process_init     → re-init StackTracer in each forked worker
        worker_process_shutdown → clean exit log

    Dispatch edges (django::view → celery::task) are created by calling
    dispatch() from Django views. Without this the view and task are
    disconnected in the graph because they run in separate processes.

    Add to stacktracer.yaml:
        probes:
          - celery
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
            logger.info(
                "celery not installed — celery probe inactive "
                "(pip install celery[redis])"
            )
            return

        task_prerun.connect(_on_task_start,          weak=False)
        task_postrun.connect(_on_task_end,           weak=False)
        task_retry.connect(_on_task_retry,           weak=False)
        task_failure.connect(_on_task_failure,       weak=False)
        worker_process_init.connect(_on_worker_fork, weak=False)
        worker_process_shutdown.connect(_on_worker_exit, weak=False)

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

        task_prerun.disconnect(_on_task_start)
        task_postrun.disconnect(_on_task_end)
        task_retry.disconnect(_on_task_retry)
        task_failure.disconnect(_on_task_failure)
        worker_process_init.disconnect(_on_worker_fork)
        worker_process_shutdown.disconnect(_on_worker_exit)

        logger.info("celery probe: signals disconnected")
from __future__ import annotations

import logging
import os
import time
from typing import Any

import origintracer
from origintracer.context.vars import reset_trace, set_trace
from origintracer.core.event_schema import (
    NormalizedEvent,
    ProbeTypes,
)
from origintracer.sdk.base_probe import BaseProbe
from origintracer.sdk.emitter import emit

logger = logging.getLogger("origintracer.probes.celery")

ProbeTypes.register_many(
    {
        "celery.main.start": "celeryd_after_setup (parked pre-fork, drained in worker)",
        "celery.worker.fork": "worker_process_init (re-init confirmation)",
        "celery.task.start": "task_prerun",
        "celery.task.end": "task_postrun (with duration)",
        "celery.task.retry": "task_retry",
        "celery.task.failure": "task_failure",
    }
)
_task_state: dict[str, dict] = {}

# Events emitted before fork - drained into each worker's engine after re-init.
# Same pattern as gunicorn's _pre_fork_events.
_pre_fork_events: list = []


def _drain_pre_fork_events() -> None:
    """
    Feed events captured in the main process into the worker's fresh engine.
    Called from _on_worker_fork after stacktracer.init() succeeds.
    fork() copies _pre_fork_events into every worker - we drain once then clear.
    """

    engine = origintracer.get_engine()
    if not _pre_fork_events or engine is None:
        return
    drained = 0
    for event in _pre_fork_events:
        try:
            engine.process(event)
            drained += 1
        except Exception as exc:
            logger.warning(
                "celery probe: pre-fork drain failed: %s", exc
            )
    _pre_fork_events.clear()
    logger.info(
        "celery probe: drained %d pre-fork event(s) into worker engine",
        drained,
    )


class CeleryProbe(BaseProbe):
    """
    Observes the Celery task lifecycle via Celery's signal system.
    """

    name = "celery"

    def start(self, **kwargs) -> None:
        try:
            from celery.signals import (
                celeryd_after_setup,
                task_failure,
                task_postrun,
                task_prerun,
                task_retry,
                worker_process_init,
                worker_process_shutdown,
            )
        except ImportError:
            logger.warning(
                "celery not installed — celery probe inactive."
            )
            return

        celeryd_after_setup.connect(
            self._on_main_ready, weak=False
        )
        task_prerun.connect(self._on_task_start, weak=False)
        task_postrun.connect(self._on_task_end, weak=False)
        task_retry.connect(self._on_task_retry, weak=False)
        task_failure.connect(self._on_task_failure, weak=False)
        worker_process_init.connect(
            self._on_worker_fork, weak=False
        )
        worker_process_shutdown.connect(
            self._on_worker_exit, weak=False
        )

        logger.info("celery probe: signals connected")

    def stop(self, **kwargs) -> None:
        try:
            from celery.signals import (
                celeryd_after_setup,
                task_failure,
                task_postrun,
                task_prerun,
                task_retry,
                worker_process_init,
                worker_process_shutdown,
            )
        except ImportError:
            return

        celeryd_after_setup.disconnect(self._on_main_ready)
        task_prerun.disconnect(self._on_task_start)
        task_postrun.disconnect(self._on_task_end)
        task_retry.disconnect(self._on_task_retry)
        task_failure.disconnect(self._on_task_failure)
        worker_process_init.disconnect(self._on_worker_fork)
        worker_process_shutdown.disconnect(self._on_worker_exit)

        logger.info("celery probe: signals disconnected")

    def _on_main_ready(self, sender, instance, **_) -> None:
        """
        Fires in the main process after Celery is set up, before any fork.
        DO NOT emit() here — no engine exists yet.
        Park the event in _pre_fork_events; fork() copies it into every
        worker and _on_worker_fork drains it after re-init.
        """
        pid = os.getpid()
        logger.warning(
            "celery probe: _on_main_ready fired pid=%d", pid
        )
        _pre_fork_events.append(
            NormalizedEvent.now(
                probe="celery.main.start",
                trace_id=f"celery-main-{pid}",
                service="celery",
                name="MainProcess",
                worker_pid=pid,
            )
        )

    def _on_worker_fork(self, **_) -> None:
        """
        Fires inside each forked prefork worker before it starts consuming tasks.
        Re-inits StackTracer, drains pre-fork events, then emits the fork event.
        """
        worker_pid = os.getpid()
        master_pid = os.getppid()
        config_path = os.environ.get(
            "ORIGINTRACER_CONFIG", "origintracer.yaml"
        )

        try:
            origintracer.init(config=config_path)
        except Exception as exc:
            logger.warning(
                "celery probe: re-init failed pid=%d: %s",
                worker_pid,
                exc,
            )
            return

        # Drain MainProcess event into this worker's fresh engine
        _drain_pre_fork_events()

        emit(
            NormalizedEvent.now(
                probe="celery.worker.fork",
                trace_id=f"celery-worker-{worker_pid}",
                service="celery",
                name="ForkPoolWorker",
                worker_pid=worker_pid,
                master_pid=master_pid,
            )
        )

        logger.info(
            "celery probe: worker re-initialised (pid=%d master=%d)",
            worker_pid,
            master_pid,
        )

    def _on_worker_exit(self, **_) -> None:
        logger.info(
            "celery probe: worker exiting (pid=%d)", os.getpid()
        )

    def _on_task_start(
        self,
        task_id: str,
        task: Any,
        args: tuple,
        kwargs: dict,
        **_,
    ) -> None:
        trace_id = kwargs.get("_trace_id") or task_id
        token = set_trace(trace_id)

        _task_state[task_id] = {
            "trace_id": trace_id,
            "token": token,
            "name": task.name,
            "t0": time.perf_counter(),
        }

        emit(
            NormalizedEvent.now(
                probe="celery.task.start",
                trace_id=trace_id,
                service="celery",
                name=task.name,
                task_id=task_id,
                worker_pid=os.getpid(),
                retries=task.request.retries,
                max_retries=task.max_retries,
            )
        )

    def _on_task_end(
        self,
        task_id: str,
        task: Any,
        args: tuple,
        kwargs: dict,
        retval: Any,
        state: str,
        **_,
    ) -> None:
        """
        Single owner of state cleanup and token reset.
        task_postrun always fires after task_failure so this is safe.
        """
        state_data = _task_state.pop(task_id, {})
        trace_id = (
            state_data.get("trace_id")
            or kwargs.get("_trace_id")
            or task_id
        )
        t0 = state_data.get("t0")
        duration_ns = (
            int((time.perf_counter() - t0) * 1e9) if t0 else None
        )

        emit(
            NormalizedEvent.now(
                probe="celery.task.end",
                trace_id=trace_id,
                service="celery",
                name=task.name,
                task_id=task_id,
                worker_pid=os.getpid(),
                state=state,
                retries=task.request.retries,
                duration_ns=duration_ns,
            )
        )

        token = state_data.get("token")
        if token is not None:
            try:
                reset_trace(token)
            except RuntimeError:
                pass

    def _on_task_retry(
        self, request: Any, reason: Any, einfo: Any, **_
    ) -> None:
        emit(
            NormalizedEvent.now(
                probe="celery.task.retry",
                trace_id=request.id,
                service="celery",
                name=request.task,
                task_id=request.id,
                worker_pid=os.getpid(),
                reason=str(reason)[:200],
                retries=request.retries,
            )
        )

    def _on_task_failure(
        self,
        task_id: str,
        exception: Exception,
        traceback: Any,
        einfo: Any,
        args: tuple,
        kwargs: dict,
        **_,
    ) -> None:
        """
        Reads state but does NOT pop or reset token.
        _on_task_end always fires after this and owns cleanup.
        """
        state_data = _task_state.get(task_id, {})
        trace_id = (
            state_data.get("trace_id")
            or kwargs.get("_trace_id")
            or task_id
        )
        task_name = state_data.get("name", task_id)

        emit(
            NormalizedEvent.now(
                probe="celery.task.failure",
                trace_id=trace_id,
                service="celery",
                name=task_name,
                task_id=task_id,
                worker_pid=os.getpid(),
                exception_type=type(exception).__name__,
                exception_msg=str(exception)[:200],
            )
        )

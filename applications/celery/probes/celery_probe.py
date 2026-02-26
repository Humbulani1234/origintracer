# myapp/probes/celery_probe.py

from stacktracer.sdk.base_probe import BaseProbe
from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.context.vars import get_trace_id, set_trace, reset_trace
import uuid, time, logging

logger = logging.getLogger(__name__)


class CeleryProbe(BaseProbe):
    name = "celery"

    def start(self) -> None:
        from celery.signals import (
            task_prerun, task_postrun, task_retry, task_failure
        )

        @task_prerun.connect
        def on_task_start(task_id, task, args, kwargs, **_):
            trace_id = str(uuid.uuid4())
            set_trace(trace_id)                     # propagate into this worker thread

            emit(NormalizedEvent.now(
                probe="function.call",              # reuses standard vocabulary
                trace_id=trace_id,
                service="celery",
                name=task.name,
                task_id=task_id,
                queue=task.request.delivery_info.get("routing_key", "default"),
                retries=task.request.retries,       # ← retry_amplification rule reads this
            ))

        @task_postrun.connect
        def on_task_end(task_id, task, retval, state, **_):
            trace_id = get_trace_id() or "unknown"
            emit(NormalizedEvent.now(
                probe="function.return",
                trace_id=trace_id,
                service="celery",
                name=task.name,
                state=state,            # "SUCCESS" | "FAILURE"
            ))
            reset_trace(None)

        @task_retry.connect
        def on_task_retry(request, reason, einfo, **_):
            trace_id = get_trace_id() or "unknown"
            emit(NormalizedEvent.now(
                probe="function.call",
                trace_id=trace_id,
                service="celery",
                name=request.task,
                retries=request.retries,            # increments each retry
                retry_reason=str(reason)[:200],
            ))

        @task_failure.connect
        def on_task_failure(task_id, exception, traceback, **_):
            trace_id = get_trace_id() or "unknown"
            emit(NormalizedEvent.now(
                probe="function.exception",
                trace_id=trace_id,
                service="celery",
                name=str(exception.__class__.__name__),
                exception_msg=str(exception)[:200],
            ))

        logger.info("CeleryProbe attached to task signals")

    def stop(self) -> None:
        from celery.signals import task_prerun, task_postrun, task_retry, task_failure
        task_prerun.disconnect()
        task_postrun.disconnect()
        task_retry.disconnect()
        task_failure.disconnect()
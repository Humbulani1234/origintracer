"""
Four Django views, each dispatching Celery tasks to exercise a specific
causal rule.  Every view:

    1. Reads the current trace_id from ContextVar (set by TracerMiddleware)
    2. Emits a celery.task.dispatch event — creates the
       django::ViewName → celery::task_name edge in the graph
    3. Passes _trace_id in task kwargs so celery_probe picks it up and
       continues the trace in the worker process

This gives full structural topology in the REPL graph:
    gunicorn::UvicornWorker
        → uvicorn::/tasks/report/
            → django::ReportView
                → celery::myapp.tasks.process_report   (dispatch edge)
                    → celery::ForkPoolWorker            (fork edge, from probe)

URLs:
    GET /tasks/report/<id>/  >> ReportView (baseline)
    POST /tasks/bulk-notify/ >> BulkNotifyView (fan-out amplification)
    GET /tasks/export/<id>/  >> ExportView (sync DB call)
    GET /tasks/failing/  >> FailingJobView (retry amplification)
    GET /tasks/status/  >> StatusView (REPL: how many tasks queued)
"""

import json

from django.http import JsonResponse
from django.views import View
from probes.redis_probe import TracedRedis

from origintracer.context.vars import get_trace_id
from origintracer.core.event_schema import NormalizedEvent
from origintracer.sdk.emitter import emit

from .tasks import (
    export_data,
    generate_report,
    process_report,
    risky_job,
    send_notification,
)


def _dispatch(task_func, trace_id: str, **task_kwargs):
    """
    Emit a celery.task.dispatch event then call task.delay().

    The event uses service="celery" so the graph node is
    celery::task_name, not django::task_name. This gives the correct
    cross-process edge in the gunicorn graph:

        django::/tasks/report/1/  →  celery::myapp.tasks.process_report

    Passing _trace_id in kwargs lets the celery probe continue the same
    trace in the worker process — same trace_id, two engines, one story.
    """
    task_name = task_func.name

    emit(
        NormalizedEvent.now(
            probe="celery.task.dispatch",
            trace_id=trace_id,
            service="celery",  # creates celery:: node, not django::
            name=task_name,
            task_name=task_name,
        )
    )

    task_func.delay(**task_kwargs, _trace_id=trace_id)


class ReportView(View):
    """
    GET /tasks/report/<id>/

    Dispatches one process_report task.
    Baseline — completes cleanly, establishes the normal latency floor.

    REPL after hitting this view several times:
        SHOW latency WHERE system = "celery"
        HOTSPOT TOP 10
    """

    def get(self, request, report_id: int):
        trace_id = get_trace_id()
        _dispatch(process_report, trace_id, report_id=report_id)
        return JsonResponse(
            {"queued": "process_report", "report_id": report_id}
        )


class BulkNotifyView(View):
    """
    POST /tasks/bulk-notify/
    Body: {"user_ids": [1, 2, ..., 10]}

    Dispatches one send_notification task per user_id.
    Fan-out pattern: one HTTP request → N tasks.

    When N ≥ 5 the call_count on the dispatch edge
    django::BulkNotifyView → celery::send_notification
    diverges significantly from the view call_count, which is what the
    celery_task_amplification rule detects.

    REPL after a few requests with 10 users each:
        BLAME WHERE system = "celery"
        CAUSAL WHERE tags = "celery"
    """

    def post(self, request):
        trace_id = get_trace_id()
        try:
            body = json.loads(request.body)
            user_ids = body.get("user_ids", list(range(1, 11)))
        except (json.JSONDecodeError, AttributeError):
            user_ids = list(range(1, 11))

        for uid in user_ids:
            _dispatch(
                send_notification,
                trace_id,
                user_id=uid,
                message=f"Hello user {uid}",
            )

        return JsonResponse(
            {
                "queued": len(user_ids),
                "task": "send_notification",
            }
        )


class ExportView(View):
    """
    GET /tasks/export/<id>/

    Dispatches one export_data task.
    The task makes a synchronous SQLite call on the worker thread (200-400ms).

    After a few requests, celery_sync_db_call fires because the graph
    shows a direct edge from celery::export_data → sqlite node with
    avg_duration_ns > 50ms.

    REPL after hitting this several times:
        CAUSAL WHERE tags = "celery"
        SHOW latency WHERE system = "worker"
    """

    def get(self, request, export_id: int):
        trace_id = get_trace_id()
        _dispatch(export_data, trace_id, export_id=export_id)
        return JsonResponse(
            {"queued": "export_data", "export_id": export_id}
        )


class StatusView(View):
    """
    GET /tasks/status/

    Simple health endpoint — confirms Django is up and shows how many
    tasks are in the Celery queue.  No task dispatched.

    REPL after hitting other views:
        SHOW graph
        HOTSPOT TOP 10
    """

    def get(self, request):
        try:
            from config.celery import app as celery_app

            inspect = celery_app.control.inspect(timeout=0.5)
            active = inspect.active() or {}
            queued = sum(len(v) for v in active.values())
        except Exception:
            queued = -1

        return JsonResponse(
            {
                "status": "ok",
                "active_tasks": queued,
                "repl_queries": [
                    "SHOW graph",
                    "HOTSPOT TOP 10",
                    'CAUSAL WHERE tags = "celery"',
                    'SHOW latency WHERE system = "celery"',
                    'BLAME WHERE system = "worker"',
                ],
            }
        )


# One shared TracedRedis instance — same as you'd do with redis.Redis
r = TracedRedis(host="localhost", port=6379, db=0)


class RedisCacheView(View):
    """
    GET /tasks/cache/<id>/

    1. Check Redis cache for the report          → redis::GET edge in graph
    2. Cache hit  → return immediately            (no celery edge)
    3. Cache miss → dispatch generate_report task → celery::generate_report edge
                 → store a "pending" marker in Redis → redis::SET edge

    REPL after several requests:
        SHOW graph
        HOTSPOT TOP 10
        SHOW latency WHERE system = "celery"
    """

    def get(self, request, report_id: int):
        trace_id = get_trace_id()
        cache_key = f"report:{report_id}"

        # Redis GET — traced, creates redis::GET node in graph
        cached = r.get(cache_key)

        if cached:
            return JsonResponse(
                {
                    "source": "cache",
                    "report_id": report_id,
                    "data": json.loads(cached),
                }
            )

        # Cache miss — dispatch task, store pending marker
        # Redis SET — traced, creates redis::SET node in graph
        r.set(
            cache_key, json.dumps({"status": "pending"}), ex=60
        )

        # dispatch() emits celery.task.dispatch then calls .delay()
        # creates the django::ReportView → celery::generate_report edge
        _dispatch(generate_report, trace_id, report_id=report_id)

        return JsonResponse(
            {
                "source": "queued",
                "report_id": report_id,
            }
        )

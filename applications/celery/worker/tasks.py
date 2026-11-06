"""
Tasks, each designed to exercise a specific causal rule:

    process_report >> baseline, completes cleanly
    send_notification >> dispatched N times per request (fan-out graph)
    export_data >> emits a measured slow SQLite operation

Each task accepts _trace_id as a kwarg. The celery probe reads it so
the trace is continuous from the Django view through to the task execution.
The graph edge  django::view >> celery::task is built from this
shared trace_id.
"""

import os
import random
import sqlite3
import time

from celery import shared_task

from origintracer.core.event_schema import NormalizedEvent
from origintracer.sdk.emitter import emit


@shared_task(name="myapp.tasks.process_report", bind=True)
def process_report(self, report_id: int, **kwargs):
    """
    Baseline task - completes cleanly in 50-150ms.
    Exercises: celery.task.start >> celery.task.end
    """
    time.sleep(random.uniform(0.05, 0.15))
    return {"status": "ok", "report_id": report_id}


@shared_task(name="myapp.tasks.send_notification", bind=True)
def send_notification(
    self, user_id: int, message: str = "", **kwargs
):
    """
    Individual notification task.
    BulkNotifyView dispatches one of these per user_id - fan-out pattern.
    BulkNotifyView dispatches multiple instances for one HTTP request,
    demonstrating task fan-out in the graph.
    """
    time.sleep(random.uniform(0.01, 0.05))
    return {"status": "sent", "user_id": user_id}


@shared_task(name="myapp.tasks.export_data", bind=True)
def export_data(self, export_id: int, **kwargs):
    """
    Slow task making a synchronous SQLite call on the worker thread.
    Blocks the Celery prefork worker thread for 500-700ms per call.
    Exercises: sync_db_in_celery causal rule
    REPL: CAUSAL
    """
    db_path = os.path.join(
        os.path.dirname(__file__), "..", "demo.db"
    )
    trace_id = kwargs.get("_trace_id") or self.request.id
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS exports "
            "(id INTEGER PRIMARY KEY, status TEXT)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO exports VALUES (?, ?)",
            (export_id, "pending"),
        )
        conn.commit()
        conn.execute(
            "UPDATE exports SET status = ? WHERE id = ?",
            ("done", export_id),
        )
        conn.commit()
        # Raw sqlite3 bypasses Django's execute_wrapper. Emit observations for
        # the repeated SQL explicitly so the graph's db_query_hotspot rule can
        # evaluate this workload. Model one slow synchronous read per task to
        # exercise the app-local sync_db_in_celery rule as well.
        sql = "SELECT status FROM exports WHERE id = ?"
        for query_index in range(6):
            started_ns = time.perf_counter_ns()
            conn.execute(sql, (export_id,)).fetchone()
            if query_index == 5:
                time.sleep(random.uniform(0.5, 0.7))
            duration_ns = time.perf_counter_ns() - started_ns
            emit(
                NormalizedEvent.now(
                    probe="celery.db.query",
                    trace_id=trace_id,
                    service="sqlite",
                    name="SELECT exports status",
                    duration_ns=duration_ns,
                    operation="SELECT",
                    sql=sql,
                    export_id=export_id,
                )
            )
    finally:
        conn.close()

    return {"status": "exported", "export_id": export_id}


@shared_task(name="myapp.tasks.generate_report", bind=True)
def generate_report(self, report_id: int, **kwargs):
    time.sleep(
        0.35
    )  # distinguish its task-duration baseline in the demo
    return {"report_id": report_id, "status": "done"}

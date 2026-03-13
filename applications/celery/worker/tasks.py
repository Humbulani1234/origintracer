"""
myapp/tasks.py

Four tasks, each designed to exercise a specific causal rule:

    process_report     → baseline, completes cleanly
    send_notifications → dispatched N times per request (fan-out amplification)
    export_data        → makes a synchronous DB call (celery_sync_db_call rule)
    risky_job          → fails and retries  (celery_retry_amplification rule)

Each task accepts _trace_id as a kwarg.  The celery probe reads it so
the trace is continuous from the Django view through to the task execution.
The graph edge  django::view → celery::task  is built from this shared trace_id.
"""

import os
import time
import random
import sqlite3
from celery import shared_task


@shared_task(name="myapp.tasks.process_report", bind=True)
def process_report(self, report_id: int, **kwargs):
    """
    Baseline task — completes cleanly in 50-150ms.
    Exercises: celery.task.start → celery.task.end
    REPL: SHOW latency WHERE system = "celery"
    """
    time.sleep(random.uniform(0.05, 0.15))
    return {"status": "ok", "report_id": report_id}


@shared_task(name="myapp.tasks.send_notification", bind=True)
def send_notification(self, user_id: int, message: str = "", **kwargs):
    """
    Individual notification task.
    BulkNotifyView dispatches one of these per user_id — fan-out pattern.
    When 10+ notifications are dispatched per HTTP request, the
    django::BulkNotifyView → celery::send_notification edge call_count
    diverges from the view call_count, surfacing celery_task_amplification.
    Exercises: fan-out amplification
    REPL: BLAME WHERE system = "celery"
    """
    time.sleep(random.uniform(0.01, 0.05))
    return {"status": "sent", "user_id": user_id}


@shared_task(name="myapp.tasks.export_data", bind=True)
def export_data(self, export_id: int, **kwargs):
    """
    Slow task making a synchronous SQLite call on the worker thread.
    Blocks the Celery prefork worker thread for 200-400ms per call.
    Exercises: celery_sync_db_call causal rule
    REPL: CAUSAL WHERE tags = "celery"
    """
    db_path = os.path.join(os.path.dirname(__file__), "..", "demo.db")
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
        time.sleep(random.uniform(0.2, 0.4))   # simulate slow query
        conn.execute(
            "UPDATE exports SET status = ? WHERE id = ?",
            ("done", export_id),
        )
        conn.commit()
    finally:
        conn.close()

    return {"status": "exported", "export_id": export_id}


@shared_task(
    name="myapp.tasks.risky_job",
    bind=True,
    max_retries=3,
    default_retry_delay=2,
)
def risky_job(self, should_fail: bool = True, **kwargs):
    """
    Task that fails and retries up to 3 times.
    After enough failures the celery_retry_amplification rule fires
    because retry node call_count >> start node call_count.
    Exercises: celery_retry_amplification causal rule
    REPL: CAUSAL WHERE tags = "celery"
    """
    if should_fail:
        try:
            raise ValueError(
                f"Simulated failure — attempt {self.request.retries + 1}"
            )
        except ValueError as exc:
            if self.request.retries < self.max_retries:
                raise self.retry(exc=exc)
            raise

    return {"status": "ok"}

import time

@shared_task(name="myapp.tasks.generate_report", bind=True)
def generate_report(self, report_id: int, **kwargs):
    time.sleep(0.2)   # simulate work
    return {"report_id": report_id, "status": "done"}
"""
myworker/tasks.py

Three tasks that exercise different probe paths.

process_report(report_id)
    Normal task. Completes cleanly. Baseline for comparison.
    Exercises: celery.task.start → celery.task.end

slow_task(item_id)
    Makes a deliberate synchronous database call.
    Exercises: celery_sync_db_call causal rule.
    The rule fires because the task has a postgres edge with high latency.

failing_task(should_fail)
    Fails and retries three times before giving up.
    Exercises: celery.task.retry × 3 → celery.task.failure
    After enough failures, triggers celery_retry_amplification rule.
"""

import time
import random
from celery import shared_task


@shared_task(name="myworker.tasks.process_report", bind=True)
def process_report(self, report_id: int, **kwargs):
    """
    Normal task — completes cleanly.

    Simulates lightweight work with a brief async-friendly sleep.
    In a real application this might generate a PDF, send an email,
    or call an external API with proper timeout handling.
    """
    time.sleep(random.uniform(0.05, 0.15))   # simulate work: 50-150ms
    return {"status": "ok", "report_id": report_id}


@shared_task(name="myworker.tasks.slow_task", bind=True)
def slow_task(self, item_id: int, **kwargs):
    """
    Slow task making a synchronous database call.

    sqlite3.connect() is a synchronous call. In a Celery prefork worker
    it blocks the worker process thread for the full query duration.
    This is the pattern the celery_sync_db_call rule detects.

    In a real application this pattern appears when:
        - Django ORM is called without sync_to_async
        - A library makes a blocking HTTP call without a timeout
        - An unindexed database query takes too long
    """
    import sqlite3
    import os

    # Simulate a slow synchronous database call (200-400ms)
    # This blocks the worker process thread — no yielding, no timeout.
    db_path = os.path.join(os.path.dirname(__file__), "..", "demo.db")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT OR IGNORE INTO items VALUES (?, ?)", (item_id, f"item_{item_id}"))
        conn.commit()

        # Simulate slow query
        time.sleep(random.uniform(0.2, 0.4))
        cursor = conn.execute("SELECT * FROM items WHERE id = ?", (item_id,))
        result = cursor.fetchone()
    finally:
        conn.close()

    return {"status": "ok", "item_id": item_id, "result": result}


@shared_task(
    name="myworker.tasks.failing_task",
    bind=True,
    max_retries=3,
    default_retry_delay=2,
)
def failing_task(self, should_fail: bool = True, **kwargs):
    """
    Task that fails and retries.

    should_fail=True  → fails all 3 retries then raises permanently
    should_fail=False → succeeds normally

    After sending enough failing tasks, the celery_retry_amplification
    rule fires because retries significantly outnumber completions.
    """
    if should_fail:
        try:
            raise ValueError(f"Simulated failure on attempt {self.request.retries + 1}")
        except ValueError as exc:
            if self.request.retries < self.max_retries:
                raise self.retry(exc=exc)
            raise

    return {"status": "ok"}
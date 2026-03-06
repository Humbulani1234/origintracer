"""
send_tasks.py

Helper script to send demo tasks to the Celery worker.
Run this after starting the worker to generate events for the REPL.

Usage:
    python send_tasks.py
"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myworker.settings")
django.setup()

from myworker.tasks import process_report, slow_task, failing_task

print("Sending tasks to Celery worker...")
print("Watch the StackTracer REPL for events.\n")

# Send 5 normal tasks — establishes baseline
print("Sending 5 normal tasks (process_report)...")
for i in range(1, 6):
    process_report.delay(report_id=i)

# Send 3 slow tasks — triggers celery_sync_db_call rule
print("Sending 3 slow tasks (slow_task)...")
for i in range(1, 4):
    slow_task.delay(item_id=i)

# Send 4 failing tasks — triggers celery_retry_amplification rule
print("Sending 4 failing tasks (failing_task)...")
for i in range(4):
    failing_task.delay(should_fail=True)

print("\nAll tasks dispatched.")
print("\nREPL queries to run:")
print("  HOTSPOT TOP 10")
print("  BLAME WHERE system = \"worker\"")
print("  CAUSAL WHERE tags = \"celery\"")
print("  SHOW latency WHERE system = \"celery\"")
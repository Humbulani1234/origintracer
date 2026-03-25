"""
stacktracer/probes/celery_types.py

Probe type constants for the Celery probe.

Import these instead of using string literals when emitting events.
This gives IDE autocompletion and ensures all types are registered
with the ProbeTypeRegistry before any events are emitted.

Registration happens at module import time — which occurs when
celery_probe.py is loaded by the auto-discovery system.
"""

from stacktracer.core.event_schema import ProbeTypes

# Register and bind to constants in one step.
# The return value of register() is the string itself,
# so these are plain str constants usable anywhere.

TASK_START = ProbeTypes.register(
    "celery.task.start",
    "Celery task execution started in a worker",
)
TASK_END = ProbeTypes.register("celery.task.end", "Celery task completed successfully")
TASK_RETRY = ProbeTypes.register(
    "celery.task.retry",
    "Celery task scheduled for retry after failure",
)
TASK_FAILURE = ProbeTypes.register(
    "celery.task.failure",
    "Celery task raised an unhandled exception",
)
BEAT_TICK = ProbeTypes.register(
    "celery.beat.tick",
    "Celery beat scheduler dispatched a periodic task",
)

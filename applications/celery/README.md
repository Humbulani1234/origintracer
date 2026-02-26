# StackTracer — Celery Demo Application

A minimal Celery application demonstrating how to write custom probes
and rules that extend StackTracer beyond the built-in probe set.

---

## What this demonstrates

This application shows the full custom probe and rule workflow:

```
1. Write a custom probe    stacktracer/probes/celery_probe.py
2. Register probe types    stacktracer/probes/celery_types.py
3. Write causal rules      stacktracer/rules/celery_rules.py
4. Configure               stacktracer.yaml
5. Run and query           REPL
```

After sending tasks to Celery, StackTracer fires these events:

```
celery.task.start          task execution started in worker
celery.task.end            task completed successfully
celery.task.retry          task scheduled for retry
celery.task.failure        task raised unhandled exception
celery.beat.tick           beat scheduler fired a periodic task
```

And the causal rule detects:

```
celery_sync_db_call        worker making synchronous database calls
                           blocking the Celery thread pool
celery_retry_amplification downstream failures causing cascading retries
                           across many tasks
```

Query the graph after sending tasks:

```
SHOW latency WHERE system = "worker"
BLAME WHERE system = "worker"
CAUSAL WHERE tags = "celery"
HOTSPOT TOP 10
DIFF SINCE deployment
```

---

## Prerequisites

```
Python 3.11+
Redis (used as Celery broker)
pip install stacktracer celery redis django
```

Start Redis:

```bash
# macOS
brew install redis && brew services start redis

# Linux
sudo apt install redis-server && sudo systemctl start redis
```

---

## Project layout

```
celery_app/
├── README.md
├── stacktracer.yaml              ← declares custom probe discovery
├── requirements.txt
├── myworker/
│   ├── celery.py                 ← Celery app instance
│   ├── tasks.py                  ← demo tasks (good, slow, failing)
│   └── models.py                 ← minimal model for DB probe demo
└── stacktracer/
    ├── probes/
    │   ├── celery_types.py       ← probe type constants (registered)
    │   └── celery_probe.py       ← custom probe
    └── rules/
        └── celery_rules.py       ← custom causal rules
```

The `stacktracer/probes/` and `stacktracer/rules/` directories are
auto-discovered by StackTracer. No YAML entry required for files
following the `*_probe.py` and `*_rules.py` naming convention.

---

## Step 1 — Install dependencies

```bash
cd applications/celery_app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Step 2 — Start the Celery worker

```bash
celery -A myworker worker --loglevel=info --concurrency=2
```

`--concurrency=2` creates two worker processes.
StackTracer initialises in each worker after fork.

For beat (periodic tasks):

```bash
celery -A myworker beat --loglevel=info
```

---

## Step 3 — Open the StackTracer REPL

```bash
python -m stacktracer.repl --config stacktracer.yaml
```

---

## Step 4 — Send tasks

In a separate terminal:

```bash
python send_tasks.py
```

Or from a Python shell:

```python
from myworker.tasks import process_report, slow_task, failing_task

# Normal task — completes cleanly
process_report.delay(report_id=1)

# Slow task — makes a synchronous database call
# triggers celery_sync_db_call causal rule
slow_task.delay(item_id=42)

# Failing task — retries three times then fails
# triggers celery_retry_amplification rule if enough tasks fail
failing_task.delay(should_fail=True)
```

---

## REPL queries to try

```
# Which tasks are taking the most time?
HOTSPOT TOP 10

# Who is calling the slow postgres node?
BLAME WHERE system = "worker"

# Run custom celery causal rules
CAUSAL WHERE tags = "celery"

# Was a new synchronous call introduced recently?
DIFF SINCE deployment

# Full latency breakdown of the worker layer
SHOW latency WHERE system = "worker"
```

---

## Understanding the custom probe

`stacktracer/probes/celery_probe.py` hooks into Celery's signal system:

```
task_prerun  → emits celery.task.start
task_postrun → emits celery.task.end
task_retry   → emits celery.task.retry
task_failure → emits celery.task.failure
```

Celery signals are the right patching point — they are stable public API,
fire reliably on every task lifecycle event, and do not require patching
private internals. Compare with the asyncio probe which must patch
`Task.__step` — a private method — because asyncio has no signal system.

## Understanding the custom rules

`stacktracer/rules/celery_rules.py` defines two predicates:

**`celery_sync_db_call`**
Traverses the runtime graph looking for celery nodes with a direct
edge to a postgres node where avg_duration_ns > 50ms. This pattern
means a Celery task is blocking its thread on a synchronous database
call, reducing worker pool throughput.

**`celery_retry_amplification`**
Looks for celery nodes where the retry count in the graph significantly
exceeds the call count — indicating tasks are being retried many times.
This often means a downstream failure (database down, API timeout) is
amplified into many retried tasks consuming the worker pool.

Both rules fire via `CAUSAL WHERE tags = "celery"` in the REPL.
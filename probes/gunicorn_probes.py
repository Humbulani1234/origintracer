"""
probes/gunicorn_probe.py

Gunicorn WSGI/ASGI process manager probe.

Gunicorn is a pre-fork worker server. The master process (Arbiter)
forks worker processes. Each worker handles requests independently.

Architecture:
    [master: Arbiter]
        ├── [worker 1: handles requests]
        ├── [worker 2: handles requests]
        └── [worker N: handles requests]

In production with async frameworks you run:
    gunicorn -k uvicorn.workers.UvicornWorker myapp:app

This means:
    - Gunicorn manages the process pool (spawning, killing, restarting)
    - Each worker runs a full uvicorn event loop
    - The uvicorn probe covers request handling inside each worker
    - This probe covers the worker lifecycle managed by gunicorn

What we observe here:
    - Worker spawning (Arbiter.spawn_worker)    — process pool events
    - Worker initialization (Worker.init_process) — per-worker startup cost
    - Request handling (sync workers only)       — per-request in sync workers
    - Worker exit                                — crash detection

Gunicorn worker types and probe coverage:
    sync            patched here — Worker.handle_request fires per request
    gthread         patched here — same handle_request path
    gevent          patched here — same path
    uvicorn.workers.UvicornWorker
                    process lifecycle patched here
                    request handling covered by UvicornProbe inside the worker

New ProbeTypes (add to event_schema.py):
    gunicorn.worker.spawn       Arbiter spawned a new worker process
    gunicorn.worker.init        Worker process initialized and ready
    gunicorn.worker.exit        Worker process exiting (normal or crash)
    gunicorn.request.handle     Sync worker handling one HTTP request
    gunicorn.worker.heartbeat   Worker sent heartbeat to master (alive signal)

Patching strategy:
    Arbiter methods are patched in the master process.
    Worker methods are patched in each worker process after fork.

    We must patch Worker.init_process (which runs post-fork) rather than
    trying to patch the worker instance from the master — the master's
    memory space and the worker's are separate after fork().

Note on PID tracking:
    After fork(), os.getpid() returns the worker's PID.
    The master PID is available via os.getppid() in the worker.
    We record both to correlate master-level and worker-level events.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent
from ..context.vars import get_trace_id, set_trace, reset_trace

import uuid

logger = logging.getLogger("stacktracer.probes.gunicorn")

_originals: dict = {}
_patched = False


# ====================================================================== #
# Arbiter patches — worker lifecycle in the master process
# ====================================================================== #

def _make_spawn_worker_wrapper(original):
    """
    Wraps Arbiter.spawn_worker to emit an event each time the master
    forks a new worker. Fires in the master process before the fork.
    """
    def _traced_spawn_worker(self):
        pid = original(self)   # returns the new worker PID

        emit(NormalizedEvent.now(
            probe="gunicorn.worker.spawn",
            trace_id=f"gunicorn-master-{os.getpid()}",
            service="gunicorn",
            name="arbiter",
            worker_pid=pid,
            master_pid=os.getpid(),
            worker_class=self.cfg.worker_class_str,
            num_workers=self.cfg.workers,
            worker_count=len(self.WORKERS),
        ))
        return pid

    return _traced_spawn_worker


def _make_kill_worker_wrapper(original):
    """
    Wraps Arbiter._kill_worker to detect intentional worker termination.
    This fires on graceful shutdown, rolling restart, and scale-down.
    """
    def _traced_kill_worker(self, pid, sig):
        import signal as signal_mod
        emit(NormalizedEvent.now(
            probe="gunicorn.worker.exit",
            trace_id=f"gunicorn-master-{os.getpid()}",
            service="gunicorn",
            name="arbiter",
            worker_pid=pid,
            master_pid=os.getpid(),
            signal=sig,
            signal_name=signal_mod.Signals(sig).name if hasattr(signal_mod, 'Signals') else str(sig),
            reason="killed_by_master",
        ))
        original(self, pid, sig)

    return _traced_kill_worker


# ====================================================================== #
# Worker patches — per-worker lifecycle and request handling
# ====================================================================== #

def _make_init_process_wrapper(original):
    """
    Wraps Worker.init_process — called in the worker process after fork.

    This is the right place to emit worker startup events because:
        1. We are now in the worker's own process (correct PID)
        2. The worker has completed its initialization
        3. The worker is about to enter its request loop

    Also re-applies the UvicornProbe if running as UvicornWorker,
    since the probe was applied in the parent process pre-fork and
    the uvicorn event loop is created fresh in each worker.
    """
    def _traced_init_process(self):
        worker_pid = os.getpid()
        master_pid = os.getppid()
        worker_class = type(self).__name__

        emit(NormalizedEvent.now(
            probe="gunicorn.worker.init",
            trace_id=f"gunicorn-worker-{worker_pid}",
            service="gunicorn",
            name=worker_class,
            worker_pid=worker_pid,
            master_pid=master_pid,
            worker_class=worker_class,
        ))

        try:
            original(self)
        except SystemExit as exc:
            # Worker exiting normally
            emit(NormalizedEvent.now(
                probe="gunicorn.worker.exit",
                trace_id=f"gunicorn-worker-{worker_pid}",
                service="gunicorn",
                name=worker_class,
                worker_pid=worker_pid,
                master_pid=master_pid,
                reason="system_exit",
                exit_code=exc.code,
            ))
            raise
        except Exception as exc:
            # Worker crashed
            emit(NormalizedEvent.now(
                probe="gunicorn.worker.exit",
                trace_id=f"gunicorn-worker-{worker_pid}",
                service="gunicorn",
                name=worker_class,
                worker_pid=worker_pid,
                master_pid=master_pid,
                reason="exception",
                exception=str(exc)[:200],
            ))
            raise

    return _traced_init_process


def _make_handle_request_wrapper(original):
    """
    Wraps Worker.handle_request for sync and gthread workers.

    This fires once per HTTP request in sync/gthread/gevent workers.
    For UvicornWorker, the UvicornProbe handles request-level events.

    environ is the WSGI environ dict — contains method, path, headers.
    """
    def _traced_handle_request(self, listener, req, client, addr):
        # Extract path and method from the request object
        # gunicorn's Request object has .path and .method
        path   = getattr(req, "path",   "/")
        method = getattr(req, "method", "")

        trace_id = str(uuid.uuid4())
        token = set_trace(trace_id)

        emit(NormalizedEvent.now(
            probe="gunicorn.request.handle",
            trace_id=trace_id,
            service="gunicorn",
            name=path,
            method=method,
            worker_pid=os.getpid(),
            client=str(addr),
        ))

        t0 = time.perf_counter()
        try:
            original(self, listener, req, client, addr)
        finally:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            emit(NormalizedEvent.now(
                probe="gunicorn.request.handle",
                trace_id=trace_id,
                service="gunicorn",
                name=path,
                method=method,
                worker_pid=os.getpid(),
                duration_ns=duration_ns,
            ))
            reset_trace(token)

    return _traced_handle_request


def _make_notify_wrapper(original):
    """
    Wraps Worker.notify — the heartbeat sent to the master process.

    Workers call notify() periodically to signal they are alive.
    If the master does not receive a heartbeat within timeout,
    it kills and restarts the worker.

    Emitting here lets us track worker liveness and detect
    workers that are stuck (heartbeats stop before kill event fires).

    We emit sparingly — only every 10th heartbeat to avoid noise.
    """
    _notify_count: dict = {"n": 0}

    def _traced_notify(self):
        _notify_count["n"] += 1
        original(self)

        # Emit every 10th heartbeat — enough signal without flooding the graph
        if _notify_count["n"] % 10 == 0:
            emit(NormalizedEvent.now(
                probe="gunicorn.worker.heartbeat",
                trace_id=f"gunicorn-worker-{os.getpid()}",
                service="gunicorn",
                name="heartbeat",
                worker_pid=os.getpid(),
                heartbeat_count=_notify_count["n"],
            ))

    return _traced_notify


class GunicornProbe(BaseProbe):
    """
    Patches gunicorn's Arbiter and Worker classes to observe the
    process pool lifecycle and per-request handling.

    What this probe tells you:
        How many workers are running and when they were spawned.
        Which workers crashed and why.
        Per-request timing in sync workers.
        Worker heartbeat health.

    What the UvicornProbe adds on top:
        Per-request ASGI timing inside each UvicornWorker.
        HTTP/1.1 cycle duration, status codes, protocol details.

    Combined, the two probes give you the full gunicorn+uvicorn picture:
        gunicorn.worker.spawn    → worker process created
        gunicorn.worker.init     → worker ready to serve
        uvicorn.request.receive  → request parsed inside worker
        uvicorn.h11.cycle        → request handled, response sent
        gunicorn.worker.heartbeat → worker alive

    Usage:
        Enable both probes together in stacktracer.yaml:
            probes:
              - gunicorn
              - uvicorn
              - asyncio
              - django
    """
    name = "gunicorn"

    def start(self) -> None:
        global _originals, _patched

        if _patched:
            logger.warning("gunicorn probe already patched — skipping")
            return

        patched_any = False

        # ---- Arbiter: worker lifecycle in master process ----
        try:
            from gunicorn.arbiter import Arbiter

            _originals["arbiter_spawn_worker"] = Arbiter.spawn_worker
            Arbiter.spawn_worker = _make_spawn_worker_wrapper(Arbiter.spawn_worker)

            _originals["arbiter_kill_worker"] = Arbiter._kill_worker
            Arbiter._kill_worker = _make_kill_worker_wrapper(Arbiter._kill_worker)

            logger.info("gunicorn probe patched Arbiter (spawn_worker, _kill_worker)")
            patched_any = True

        except ImportError:
            logger.warning(
                "gunicorn not installed — gunicorn probe inactive. "
                "(pip install gunicorn)"
            )
            return
        except AttributeError as exc:
            logger.warning("Arbiter patch failed: %s", exc)

        # ---- Worker base: init_process + notify (all worker types) ----
        try:
            from gunicorn.workers.base import Worker

            _originals["worker_init_process"] = Worker.init_process
            Worker.init_process = _make_init_process_wrapper(Worker.init_process)

            _originals["worker_notify"] = Worker.notify
            Worker.notify = _make_notify_wrapper(Worker.notify)

            logger.info("gunicorn probe patched Worker (init_process, notify)")
            patched_any = True

        except AttributeError as exc:
            logger.warning("Worker base patch failed: %s", exc)

        # ---- Sync worker: per-request handle_request ----
        # Only patch sync workers — UvicornWorker is covered by UvicornProbe
        try:
            from gunicorn.workers.sync import SyncWorker

            _originals["sync_handle_request"] = SyncWorker.handle_request
            SyncWorker.handle_request = _make_handle_request_wrapper(
                SyncWorker.handle_request
            )
            logger.info("gunicorn probe patched SyncWorker.handle_request")

        except (ImportError, AttributeError) as exc:
            logger.debug("SyncWorker patch skipped: %s", exc)

        # ---- GThread worker: per-request ----
        try:
            from gunicorn.workers.gthread import ThreadWorker

            _originals["gthread_handle_request"] = ThreadWorker.handle_request
            ThreadWorker.handle_request = _make_handle_request_wrapper(
                ThreadWorker.handle_request
            )
            logger.info("gunicorn probe patched ThreadWorker.handle_request")

        except (ImportError, AttributeError) as exc:
            logger.debug("ThreadWorker patch skipped: %s", exc)

        if patched_any:
            _patched = True

    def stop(self) -> None:
        global _originals, _patched

        if not _patched:
            return

        restore_map = {
            "arbiter_spawn_worker":   ("gunicorn.arbiter",         "Arbiter",      "spawn_worker"),
            "arbiter_kill_worker":    ("gunicorn.arbiter",         "Arbiter",      "_kill_worker"),
            "worker_init_process":    ("gunicorn.workers.base",    "Worker",       "init_process"),
            "worker_notify":          ("gunicorn.workers.base",    "Worker",       "notify"),
            "sync_handle_request":    ("gunicorn.workers.sync",    "SyncWorker",   "handle_request"),
            "gthread_handle_request": ("gunicorn.workers.gthread", "ThreadWorker", "handle_request"),
        }

        for key, (module_path, class_name, attr) in restore_map.items():
            if key in _originals:
                try:
                    import importlib
                    module = importlib.import_module(module_path)
                    cls = getattr(module, class_name)
                    setattr(cls, attr, _originals.pop(key))
                except Exception as exc:
                    logger.debug("Failed to restore %s: %s", key, exc)

        _patched = False
        logger.info("gunicorn probe removed")
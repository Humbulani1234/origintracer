"""
Observes gunicorn using only its documented configuration callbacks.

Approach:
    gunicorn has a documented server hooks system — callback functions you
    define in gunicorn.conf.py. These are the official, stable extension
    points for exactly this kind of observation. They are versioned public API.

    Hooks used:
        on_starting(server) - master process starting
        post_fork(server, worker) - worker process forked and running
        worker_exit(server, worker) - worker process about to exit
        worker_int(worker) - worker received SIGINT
        worker_abort(worker) - worker received SIGABRT (crash)
        pre_exec(server) - master about to exec() for graceful reload

Integration with gunicorn.conf.py:
    The user adds one line to their gunicorn config:

        # gunicorn.conf.py
        from stacktracer.probes.gunicorn_probe import install_hooks
        install_hooks()

    install_hooks() injects StackTracer callbacks into gunicorn's server
    hook system programmatically. The user does not need to manually
    define each callback function.

Alternatively, if the user prefers explicit configuration:

        # gunicorn.conf.py
        from stacktracer.probes.gunicorn_probe import (
            st_on_starting, st_post_fork, st_worker_exit
        )
        on_starting = st_on_starting
        post_fork   = st_post_fork
        worker_exit = st_worker_exit

ProbeTypes:
    gunicorn.master.start       master process started
    gunicorn.worker.fork        worker process forked (fires in master)
    gunicorn.worker.ready       worker initialised and ready to serve
    gunicorn.worker.exit        worker exiting normally
    gunicorn.worker.crash       worker aborting or receiving SIGINT
    gunicorn.master.reload      master about to exec() for reload
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable

from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit, emit_direct

logger = logging.getLogger("stacktracer.probes.gunicorn")

ProbeTypes.register_many(
    {
        "gunicorn.master.start": "gunicorn master process starting",
        "gunicorn.worker.fork": "gunicorn worker process forked",
        "gunicorn.worker.ready": "gunicorn worker ready to serve requests",
        "gunicorn.worker.exit": "gunicorn worker exiting normally",
        "gunicorn.worker.crash": "gunicorn worker aborting or interrupted",
        "gunicorn.master.reload": "gunicorn master reloading via exec()",
    }
)


# ====================================================================== #
# Hook functions — passed to gunicorn's server hook system
# These are plain functions, not class methods. gunicorn calls them
# directly as documented in its configuration reference.
# ====================================================================== #

# gunicorn_probe.py — module level
_pre_fork_events: list = []


def st_on_starting(server: Any) -> None:
    master_pid = os.getpid()
    bind = getattr(server, "address", [])
    cfg = getattr(server, "cfg", None)
    worker_class = (
        getattr(cfg, "worker_class_str", "unknown")
        if cfg
        else "unknown"
    )
    num_workers = getattr(cfg, "workers", 0) if cfg else 0

    # Do NOT call init() or emit() here — no engine exists yet and
    # the buffer will be orphaned after fork.
    # Instead, park the event in a module-level list.
    # fork() copies all process memory including this list into every
    # worker. st_post_fork then drains it into the worker's real engine.
    _pre_fork_events.append(
        NormalizedEvent.now(
            probe="gunicorn.master.start",
            trace_id=f"gunicorn-master-{master_pid}",
            service="gunicorn",
            name="master",
            master_pid=master_pid,
            bind=str(bind),
            worker_class=worker_class,
            num_workers=num_workers,
        )
    )

    logger.info(
        "gunicorn master starting (pid=%d, workers=%d)",
        master_pid,
        num_workers,
    )


def ot_post_fork(server: Any, worker: Any) -> None:
    worker_pid = os.getpid()
    master_pid = os.getppid()
    worker_class = type(worker).__name__
    trace_id = f"gunicorn-worker-{worker_pid}"

    try:
        from origintracer.sdk.emitter import (
            _restart_drain_thread,
        )

        _restart_drain_thread()
    except Exception as exc:
        logger.warning(
            "gunicorn probe: drain thread restart failed: %s",
            exc,
        )

    import stacktracer

    engine = stacktracer.get_engine()
    if engine is not None:
        _emit_worker_fork(
            trace_id, worker_class, worker_pid, master_pid
        )
        _drain_pre_fork_events(engine)
    else:
        stacktracer._register_post_init_callback(
            lambda: _emit_worker_fork(
                trace_id, worker_class, worker_pid, master_pid
            )
        )
        stacktracer._register_post_init_callback(
            lambda: _drain_pre_fork_events(
                stacktracer.get_engine()
            )
        )


def _drain_pre_fork_events(engine) -> None:
    """
    Feed events that were captured in the master process (before fork)
    into the worker's engine after AppConfig.ready() has run.

    fork() copies the entire parent address space into the child.
    _pre_fork_events is a module-level list - it is copied verbatim
    into every worker. AppConfig.ready() creates a new engine in the
    worker, but _pre_fork_events still holds the master events.
    We feed them into the new engine here.

    Every worker gets the same copy of _pre_fork_events. We only
    want each worker to process them once. Clearing after drain
    prevents double-processing if somehow this is called twice.
    """
    if not _pre_fork_events or engine is None:
        return

    for event in _pre_fork_events:
        try:
            engine.process(event)
        except Exception as exc:
            logger.warning(
                "gunicorn probe: pre-fork event drain failed: %s",
                exc,
            )

    _pre_fork_events.clear()
    logger.info(
        "gunicorn probe: drained pre-fork events into worker engine"
    )


def _emit_worker_fork(
    trace_id, worker_class, worker_pid, master_pid
):
    emit(
        NormalizedEvent.now(
            probe="gunicorn.worker.fork",
            trace_id=trace_id,
            service="gunicorn",
            name=f"{worker_class}-{os.getpid()}",
            worker_pid=worker_pid,
            master_pid=master_pid,
            worker_class=worker_class,
        )
    )


def st_worker_exit(server: Any, worker: Any) -> None:
    """
    Fires in the MASTER process when a worker exits normally.
    This is a post-exit notification — the worker is already gone.
    """
    worker_pid = getattr(worker, "pid", 0)
    master_pid = os.getpid()
    worker_class = type(worker).__name__
    exitcode = getattr(worker, "exitcode", None)

    # Emit from the master process — trace_id is worker-scoped
    trace_id = f"gunicorn-worker-{worker_pid}"

    emit(
        NormalizedEvent.now(
            probe="gunicorn.worker.exit",
            trace_id=trace_id,
            service="gunicorn",
            name=f"{worker_class}-{os.getpid()}",
            worker_pid=worker_pid,
            master_pid=master_pid,
            exitcode=exitcode,
            reason="normal_exit",
        )
    )


def st_worker_int(worker: Any) -> None:
    """
    Fires in the WORKER process when it receives SIGINT.
    Usually triggered by Ctrl+C or gunicorn graceful shutdown.
    """
    worker_pid = os.getpid()
    worker_class = type(worker).__name__
    trace_id = f"gunicorn-worker-{worker_pid}"

    # TODO: blocks the app, find a better way
    emit_direct(
        NormalizedEvent.now(
            probe="gunicorn.worker.crash",
            trace_id=trace_id,
            service="gunicorn",
            name=f"{worker_class}-{os.getpid()}",
            worker_pid=worker_pid,
            reason="SIGINT",
        )
    )


def st_worker_abort(worker: Any) -> None:
    """
    Fires in the WORKER process when it receives SIGABRT.
    This usually means the master killed the worker due to a timeout
    (worker failed to send heartbeat within the timeout period).
    This is a crash, not a graceful shutdown.
    """
    worker_pid = os.getpid()
    worker_class = type(worker).__name__
    trace_id = f"gunicorn-worker-{worker_pid}"

    emit_direct(
        NormalizedEvent.now(
            probe="gunicorn.worker.crash",
            trace_id=trace_id,
            service="gunicorn",
            name=f"{worker_class}-{os.getpid()}",
            worker_pid=worker_pid,
            reason="SIGABRT_timeout",
        )
    )


def st_pre_exec(server: Any) -> None:
    """
    Fires in the master process just before exec() for a graceful reload.
    The master is about to replace itself with a new process.
    """
    master_pid = os.getpid()
    trace_id = f"gunicorn-master-{master_pid}"

    emit_direct(
        NormalizedEvent.now(
            probe="gunicorn.master.reload",
            trace_id=trace_id,
            service="gunicorn",
            name="master",
            master_pid=master_pid,
        )
    )


# ====================================================================== #
# install_hooks — programmatic injection into gunicorn server hooks
# ====================================================================== #


def install_hooks() -> None:
    """
    Injects StackTracer callbacks into the gunicorn server hook system.

    Call from gunicorn.conf.py:
        from stacktracer.probes.gunicorn_probe import install_hooks
        install_hooks()

    This chains StackTracer callbacks with any hooks the user has already
    defined. It does not replace existing hooks — it wraps them.

    How it works:
        gunicorn reads the module-level names on_starting, post_fork, etc.
        from the config module (gunicorn.conf.py) after importing it.
        We inject into that module's namespace so gunicorn finds our callbacks.
    """
    import sys

    # The config module is the module that called install_hooks()
    # We walk the call stack to find it.
    import traceback

    frame = sys._getframe(1)
    config_module = frame.f_globals

    _chain(config_module, "on_starting", st_on_starting)
    _chain(config_module, "post_fork", ot_post_fork)
    _chain(config_module, "worker_exit", st_worker_exit)
    _chain(config_module, "worker_int", st_worker_int)
    _chain(config_module, "worker_abort", st_worker_abort)
    _chain(config_module, "pre_exec", st_pre_exec)

    logger.info("gunicorn StackTracer hooks installed")


def _chain(
    module_globals: dict, hook_name: str, st_fn: Callable
) -> None:
    """
    Chain our hook with an existing hook of the same name in the config module.
    If no existing hook exists, just set ours.
    """
    existing = module_globals.get(hook_name)

    if existing is None:
        module_globals[hook_name] = st_fn
        return

    # Wrap: call existing hook first, then ours
    def _chained(*args, **kwargs):
        existing(*args, **kwargs)
        st_fn(*args, **kwargs)

    module_globals[hook_name] = _chained


# ====================================================================== #
# GunicornProbe — for use with StackTracer probe registry
# ====================================================================== #


class GunicornProbe(BaseProbe):
    """
    gunicorn probe via official server hooks.

    Unlike other probes, GunicornProbe cannot self-install at runtime
    because gunicorn reads its hook configuration before starting workers.
    The hooks must be declared in gunicorn.conf.py.

    Two usage patterns:

    Pattern 1 — one-line setup in gunicorn.conf.py (recommended):
        from stacktracer.probes.gunicorn_probe import install_hooks
        install_hooks()

    Pattern 2 — explicit hook assignment in gunicorn.conf.py:
        from stacktracer.probes.gunicorn_probe import (
            st_on_starting, st_post_fork, st_worker_exit,
            st_worker_int, st_worker_abort, st_pre_exec,
        )
        on_starting  = st_on_starting
        post_fork    = st_post_fork
        worker_exit  = st_worker_exit
        worker_int   = st_worker_int
        worker_abort = st_worker_abort
        pre_exec     = st_pre_exec

    The GunicornProbe.start() method emits a warning if gunicorn is
    installed but the hooks are not yet configured, reminding the user
    to update their gunicorn.conf.py.
    """

    name = "gunicorn"

    def start(self) -> None:

        # import pdb
        # pdb.set_trace()

        try:
            import gunicorn  # noqa: F401
        except ImportError:
            logger.info(
                "gunicorn not installed — gunicorn probe inactive"
            )
            return

        logger.info(
            "gunicorn probe: hooks must be declared in gunicorn.conf.py.\n"
            "Add this line:\n"
            "    from stacktracer.probes.gunicorn_probe import install_hooks\n"
            "    install_hooks()\n"
            "This installs all lifecycle hooks without patching any classes."
        )

    def stop(self) -> None:
        # Hooks are config-time declarations — nothing to undo at runtime.
        # On process exit they simply stop being called.
        pass

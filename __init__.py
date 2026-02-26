"""
stacktracer/__init__.py

Public API for the StackTracer agent library.

Customer usage in Django settings.py:

    import stacktracer

    stacktracer.init(
        api_key=os.getenv('STACKTRACER_API_KEY'),
        sample_rate=0.05,       # 5% of requests
        probes=['django', 'asyncio'],
        endpoint='https://api.stacktracer.io',
    )

    MIDDLEWARE = [
        'stacktracer.probes.django_probe.TracerMiddleware',
        ...
    ]

All public symbols that customers should import are defined here.
Nothing else is part of the stable public API.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("stacktracer")


# ====================================================================== #
# Configuration
# ====================================================================== #

@dataclass
class Config:
    api_key: str
    endpoint: str = "https://api.stacktracer.io"
    sample_rate: float = 0.01       # 1% default
    buffer_size: int = 10_000
    flush_interval: int = 10        # seconds
    redact_fields: List[str] = field(default_factory=lambda: ["password", "token", "secret", "authorization"])
    probes: List[str] = field(default_factory=lambda: ["django", "asyncio"])
    snapshot_interval: float = 15.0
    enabled: bool = True
    debug: bool = False

    def __post_init__(self) -> None:
        # Auto-disable in debug/dev environments
        if os.getenv("DJANGO_DEBUG", "false").lower() == "true" and not self.debug:
            logger.info("StackTracer: DJANGO_DEBUG=true detected — disabling (set debug=True to override)")
            self.enabled = False


_config: Optional[Config] = None
_active_probes: List[Any] = []
_engine: Optional[Any] = None


def get_config() -> Config:
    if _config is None:
        raise RuntimeError("stacktracer.init() has not been called")
    return _config


def get_engine() -> Any:
    return _engine


# ====================================================================== #
# Initialisation
# ====================================================================== #

def init(
    api_key: str,
    endpoint: str = "https://api.stacktracer.io",
    sample_rate: float = 0.01,
    probes: Optional[List[str]] = None,
    snapshot_interval: float = 15.0,
    debug: bool = False,
    repository: Optional[Any] = None,
    semantic_config: Optional[List[Dict]] = None,
    **kwargs: Any,
) -> None:
    """
    Initialise StackTracer.

    Parameters
    ----------
    api_key          : Your StackTracer API key.
    endpoint         : Where to send trace batches.
    sample_rate      : Fraction of requests to trace (0.0–1.0).
    probes           : List of probe names to activate.
    snapshot_interval: Seconds between temporal graph snapshots.
    debug            : If True, enables in dev/DEBUG environments.
    repository       : Optional pre-built repository instance.
    semantic_config  : List of semantic alias dicts (see SemanticLayer).
    """
    global _config, _active_probes, _engine

    _config = Config(
        api_key=api_key,
        endpoint=endpoint,
        sample_rate=sample_rate,
        probes=probes or ["django", "asyncio"],
        snapshot_interval=snapshot_interval,
        debug=debug,
        **{k: v for k, v in kwargs.items() if k in Config.__dataclass_fields__},
    )

    if not _config.enabled:
        logger.info("StackTracer disabled (not initialised)")
        return

    _setup_engine(repository, semantic_config, snapshot_interval)
    _setup_probes(_config.probes)
    _setup_uploader()

    logger.info(
        "StackTracer initialised | sample_rate=%.1f%% probes=%s",
        _config.sample_rate * 100,
        _config.probes,
    )


def _setup_engine(repository: Optional[Any], semantic_config: Optional[List[Dict]], snapshot_interval: float) -> None:
    global _engine
    from .core.engine import Engine
    from .core.causal import build_default_registry
    from .core.semantic import load_from_dict, SemanticLayer
    from .sdk.emitter import bind_engine

    semantic = load_from_dict(semantic_config) if semantic_config else SemanticLayer()
    _engine = Engine(
        causal_registry=build_default_registry(),
        semantic_layer=semantic,
        snapshot_interval_s=snapshot_interval,
    )

    if repository:
        _engine.repository = repository

    bind_engine(_engine)
    _engine.start_background_tasks()


def _setup_probes(probe_names: List[str]) -> None:
    global _active_probes
    from .sdk.base_probe import ProbeRegistry

    # Ensure probe modules are imported so they register themselves
    _import_probe_modules()

    _active_probes = ProbeRegistry.load_from_config({"probes": probe_names})
    for probe in _active_probes:
        try:
            probe.start()
        except Exception as exc:
            logger.warning("Probe %s failed to start: %s", probe.name, exc)


def _import_probe_modules() -> None:
    """Force-import all probe modules so they register with ProbeRegistry."""
    # These imports are side-effecting (they call BaseProbe.__init_subclass__)
    try:
        from .probes import asyncio_probe  # noqa: F401
        from .probes import django_probe   # noqa: F401
        from .probes import kernel_probe   # noqa: F401
        from .probes import nginx_probe    # noqa: F401
    except ImportError as exc:
        logger.debug("Probe import warning: %s", exc)


def _setup_uploader() -> None:
    """Start background batch uploader (sends events to SaaS backend)."""
    from .buffer.uploader import Uploader
    config = get_config()
    uploader = Uploader(
        endpoint=config.endpoint,
        api_key=config.api_key,
        flush_interval=config.flush_interval,
    )
    uploader.start()


def shutdown() -> None:
    """Gracefully stop all probes and flush buffers. Call at Django shutdown."""
    global _active_probes, _engine
    for probe in _active_probes:
        try:
            probe.stop()
        except Exception as exc:
            logger.debug("Probe %s stop error: %s", probe.name, exc)
    _active_probes = []

    if _engine:
        _engine.stop()

    logger.info("StackTracer shut down")


# ====================================================================== #
# Convenience decorators for manual instrumentation
# ====================================================================== #

def trace(name: Optional[str] = None):
    """
    Decorator for explicit function-level tracing.

        @stacktracer.trace("my_expensive_function")
        def my_fn():
            ...
    """
    import functools
    import time

    def decorator(fn: Any) -> Any:
        fn_name = name or fn.__qualname__

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            from .context.vars import get_trace_id
            from .sdk.emitter import emit
            from .core.event_schema import NormalizedEvent

            trace_id = get_trace_id()
            if not trace_id:
                return fn(*args, **kwargs)

            start = time.perf_counter()
            emit(NormalizedEvent.now(
                probe="function.call",
                trace_id=trace_id,
                service="user",
                name=fn_name,
            ))
            try:
                result = fn(*args, **kwargs)
                return result
            except Exception as exc:
                emit(NormalizedEvent.now(
                    probe="function.exception",
                    trace_id=trace_id,
                    service="user",
                    name=fn_name,
                    exception_type=type(exc).__name__,
                ))
                raise
            finally:
                duration_ns = int((time.perf_counter() - start) * 1e9)
                emit(NormalizedEvent.now(
                    probe="function.return",
                    trace_id=trace_id,
                    service="user",
                    name=fn_name,
                    duration_ns=duration_ns,
                ))

        return wrapper
    return decorator


def mark_deployment(label: str = "deployment") -> None:
    """Signal a deployment boundary to the temporal engine."""
    if _engine:
        _engine.mark_deployment(label)


# ====================================================================== #
# Public re-exports
# ====================================================================== #

from .core.event_schema import NormalizedEvent  # noqa: F401
from .sdk.emitter import emit                    # noqa: F401
from .context.vars import get_trace_id           # noqa: F401

__version__ = "0.1.0"
__all__ = [
    "init",
    "shutdown",
    "get_config",
    "get_engine",
    "trace",
    "mark_deployment",
    "emit",
    "NormalizedEvent",
    "get_trace_id",
]
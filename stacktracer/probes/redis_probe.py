"""
stacktracer/probes/redis_probe.py

Observes Redis commands without patching the global Redis class.

Approach — TracedRedis subclass:
    We provide a TracedRedis class that subclasses redis.client.Redis
    and overrides execute_command() — the single method that all
    redis-py commands funnel through.

    The user constructs TracedRedis instead of redis.Redis:
        from stacktracer.probes.redis_probe import TracedRedis
        r = TracedRedis(host="localhost", port=6379, db=0)

    This is a documented pattern. redis-py is designed for subclassing —
    the library itself ships StrictRedis as a subclass of Redis.
    No global patching. Other code using redis.Redis directly is unaffected.

    For connection pools:
        from stacktracer.probes.redis_probe import make_traced_pool
        pool = make_traced_pool(host="localhost", port=6379)
        r    = TracedRedis(connection_pool=pool)

What we observe:
    Every Redis command: GET, SET, HGET, LPUSH, ZADD, etc.
    Captured via execute_command() which is the universal dispatch point.

    For each command:
        command name    (GET, SET, HGET, ...)
        key             (first argument after command name, if string)
        duration_ns     (entry to return)
        success         (True or False)
        error           (exception message if failed)

    We do NOT capture the full value — Redis values can be large
    (serialized objects, binary blobs) and are not useful for causal
    reasoning. The command + key is sufficient for graph building.

ProbeTypes:
    redis.command.execute   any Redis command

Graph node naming:
    We use the command name as the node name:
        redis::GET      "all GET operations on this redis instance"
        redis::HGET     "all HGET operations"
    This is intentionally coarse — we care about command patterns,
    not individual keys. GraphNormalizer handles any key-level
    cardinality if a probe emits keys as names.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from stacktracer.sdk.emitter import emit
from stacktracer.core.event_schema import NormalizedEvent, ProbeTypes
from stacktracer.context.vars import get_trace_id, get_span_id

logger = logging.getLogger("stacktracer.probes.redis")

ProbeTypes.register_many({
    "redis.command.execute": "Redis command executed (any command type)",
    "redis.pipeline.execute": "Redis pipeline flushed (batch of commands)",
})


def _get_redis():
    try:
        import redis
        return redis
    except ImportError:
        raise ImportError("redis not installed. pip install redis")


# ====================================================================== #
# TracedRedis
# ====================================================================== #

class TracedRedis:
    """
    Wraps redis.Redis and emits NormalizedEvents on every command.

    Subclassing redis.Redis directly caused issues with some redis-py
    versions that inspect __class__ during connection pool management.
    This composition wrapper avoids that entirely.

    Usage:
        from stacktracer.probes.redis_probe import TracedRedis

        r = TracedRedis(host="localhost", port=6379, db=0)
        r.set("key", "value")   # emits redis.command.execute
        r.get("key")            # emits redis.command.execute

    All redis-py attributes and methods are accessible — only
    execute_command is intercepted.
    """

    def __init__(self, *args, **kwargs) -> None:
        redis_lib = _get_redis()
        self._redis = redis_lib.Redis(*args, **kwargs)

    def execute_command(self, *args, **options) -> Any:
        """
        Intercept all Redis commands. All redis-py high-level methods
        (get, set, hget, lpush, etc.) call this method internally.

        args[0] is the command name (GET, SET, HGET, ...).
        args[1] is usually the key.
        """
        trace_id = get_trace_id()
        command  = args[0] if args else "UNKNOWN"
        key      = str(args[1])[:100] if len(args) > 1 else ""

        t0 = time.perf_counter()
        try:
            result = self._redis.execute_command(*args, **options)
        except Exception as exc:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe="redis.command.execute",
                    trace_id=trace_id,
                    service="redis",
                    name=command,
                    parent_span_id=get_span_id(),
                    duration_ns=duration_ns,
                    key=key,
                    error=str(exc)[:200],
                    success=False,
                ))
            raise
        else:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe="redis.command.execute",
                    trace_id=trace_id,
                    service="redis",
                    name=command,
                    parent_span_id=get_span_id(),
                    duration_ns=duration_ns,
                    key=key,
                    success=True,
                    # result_type gives shape info without value size risk
                    result_type=type(result).__name__,
                ))
            return result

    def pipeline(self, transaction: bool = True, shard_hint=None):
        """Return a TracedPipeline."""
        raw_pipe = self._redis.pipeline(transaction=transaction, shard_hint=shard_hint)
        return TracedPipeline(raw_pipe)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._redis, name)


# ====================================================================== #
# TracedPipeline
# ====================================================================== #

class TracedPipeline:
    """
    Wraps a redis Pipeline and emits one event when the pipeline executes.

    Pipelines batch multiple commands into a single round-trip.
    We emit one event when execute() is called with the count of
    commands in the batch, not one event per command.
    """

    def __init__(self, pipeline: Any) -> None:
        self._pipeline = pipeline

    def execute(self, raise_on_error: bool = True) -> Any:
        trace_id      = get_trace_id()
        command_count = len(self._pipeline.command_stack)

        t0 = time.perf_counter()
        try:
            result = self._pipeline.execute(raise_on_error=raise_on_error)
        except Exception as exc:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe="redis.pipeline.execute",
                    trace_id=trace_id,
                    service="redis",
                    name="pipeline",
                    duration_ns=duration_ns,
                    command_count=command_count,
                    error=str(exc)[:200],
                    success=False,
                ))
            raise
        else:
            duration_ns = int((time.perf_counter() - t0) * 1e9)
            if trace_id:
                emit(NormalizedEvent.now(
                    probe="redis.pipeline.execute",
                    trace_id=trace_id,
                    service="redis",
                    name="pipeline",
                    duration_ns=duration_ns,
                    command_count=command_count,
                    success=True,
                ))
            return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pipeline, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pipeline.__exit__(exc_type, exc_val, exc_tb)
        return False


# ====================================================================== #
# Connection pool helper
# ====================================================================== #

def make_traced_pool(**kwargs) -> Any:
    """
    Create a ConnectionPool for use with TracedRedis.

    Usage:
        pool = make_traced_pool(host="localhost", port=6379, db=0, max_connections=20)
        r    = TracedRedis(connection_pool=pool)
    """
    redis_lib = _get_redis()
    return redis_lib.ConnectionPool(**kwargs)
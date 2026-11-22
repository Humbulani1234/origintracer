import importlib

import pytest

from origintracer.context.vars import reset_trace, set_trace

redis = pytest.importorskip("redis")
redis_probe = importlib.import_module(
    "applications.celery.probes.redis_probe"
)


def test_redis_command_probe_records_success_without_connecting(
    monkeypatch,
):
    events = []
    monkeypatch.setattr(redis_probe, "emit", events.append)
    monkeypatch.setattr(
        redis.Redis,
        "execute_command",
        lambda self, *args, **kwargs: b"value",
    )
    token = set_trace("redis-trace")
    try:
        result = redis_probe.TracedRedis().execute_command(
            "GET", "session-key"
        )
    finally:
        reset_trace(token)

    assert result == b"value"
    assert events[0].probe == "redis.command.execute"
    assert events[0].trace_id == "redis-trace"
    assert events[0].metadata["key"] == "session-key"
    assert events[0].metadata["success"] is True


def test_redis_command_probe_emits_failure_and_reraises(
    monkeypatch,
):
    events = []
    monkeypatch.setattr(redis_probe, "emit", events.append)

    def fail(self, *args, **kwargs):
        raise redis.ConnectionError("offline")

    monkeypatch.setattr(redis.Redis, "execute_command", fail)
    token = set_trace("redis-trace")
    try:
        with pytest.raises(redis.ConnectionError):
            redis_probe.TracedRedis().execute_command(
                "GET", "session-key"
            )
    finally:
        reset_trace(token)

    assert events[0].metadata["success"] is False
    assert events[0].metadata["error"] == "offline"


def test_redis_pipeline_probe_reports_batch_size(monkeypatch):
    events = []
    monkeypatch.setattr(redis_probe, "emit", events.append)

    class Pipeline:
        command_stack = [("GET", "a"), ("SET", "b", "v")]

        def execute(self, raise_on_error=True):
            return [b"a", True]

    token = set_trace("pipeline-trace")
    try:
        result = redis_probe.TracedPipeline(Pipeline()).execute()
    finally:
        reset_trace(token)

    assert result == [b"a", True]
    assert events[0].probe == "redis.pipeline.execute"
    assert events[0].metadata["command_count"] == 2
    assert events[0].trace_id == "pipeline-trace"

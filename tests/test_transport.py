"""
tests/test_transport.py

Tests for the transport layer:
    EventBuffer / emit()   — SDK interface between probes and engine
    Uploader               — batching, repository interface, snapshot serialisation

Uploader HTTP tests use httpx.MockTransport so no real server is needed.
"""

from __future__ import annotations

import threading
import time
import pytest

from conftest import evt
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.buffer.uploader import Uploader, _EventBuffer


# ====================================================================== #
# EventBuffer
# ====================================================================== #

class TestEventBuffer:

    def test_push_and_drain(self):
        buf = _EventBuffer(max_size=100)
        buf.push({"a": 1})
        buf.push({"b": 2})
        batch = buf.drain(max_batch=10)
        assert len(batch) == 2
        assert batch[0] == {"a": 1}

    def test_drain_respects_max_batch(self):
        buf = _EventBuffer(max_size=100)
        for i in range(20):
            buf.push({"i": i})
        batch = buf.drain(max_batch=5)
        assert len(batch) == 5
        assert len(buf) == 15

    def test_drain_empties_buffer(self):
        buf = _EventBuffer(max_size=100)
        for i in range(5):
            buf.push({"i": i})
        buf.drain(max_batch=100)
        assert len(buf) == 0

    def test_maxlen_drops_oldest_on_overflow(self):
        buf = _EventBuffer(max_size=3)
        for i in range(5):
            buf.push({"i": i})
        batch = buf.drain(max_batch=10)
        # deque(maxlen=3) keeps the last 3 pushed
        assert len(batch) == 3
        assert batch[-1]["i"] == 4

    def test_thread_safe_push_drain(self):
        buf = _EventBuffer(max_size=10_000)
        pushed = []
        errors = []

        def pusher():
            try:
                for i in range(500):
                    buf.push({"i": i})
                    pushed.append(1)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=pusher) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert sum(pushed) == 2000


# ====================================================================== #
# emit() and bind_engine
# ====================================================================== #

class TestEmitter:

    def setup_method(self):
        # Reset emitter module state between tests
        import stacktracer.sdk.emitter as em
        em._engine = None

    def test_emit_before_bind_is_silent(self):
        from stacktracer.sdk.emitter import emit
        # Must not raise
        emit(evt())

    def test_emit_after_bind_calls_engine_process(self):
        from stacktracer.sdk.emitter import emit, bind_engine

        processed = []

        class FakeEngine:
            def process(self, event):
                processed.append(event)

        bind_engine(FakeEngine())
        emit(evt())
        assert len(processed) == 1

    def test_bind_engine_twice_replaces_reference(self):
        from stacktracer.sdk.emitter import emit, bind_engine

        calls_a, calls_b = [], []

        class EngineA:
            def process(self, e): calls_a.append(e)

        class EngineB:
            def process(self, e): calls_b.append(e)

        bind_engine(EngineA())
        emit(evt())
        bind_engine(EngineB())
        emit(evt())

        assert len(calls_a) == 1
        assert len(calls_b) == 1

    def test_engine_exception_does_not_propagate(self):
        from stacktracer.sdk.emitter import emit, bind_engine

        class CrashingEngine:
            def process(self, e):
                raise RuntimeError("engine is broken")

        bind_engine(CrashingEngine())
        # Must not raise — probes must never crash the host app
        emit(evt())


# ====================================================================== #
# Uploader — insert_event interface
# ====================================================================== #

class TestUploaderRepositoryInterface:
    """
    Uploader implements BaseRepository.insert_event() so Engine.process()
    can call it directly. Tests verify the interface contract without
    starting the background thread or making HTTP calls.
    """

    def _uploader(self) -> Uploader:
        return Uploader(
            endpoint       = "http://localhost:9999",
            api_key        = "test-key",
            flush_interval = 9999,   # never flushes automatically
        )

    def test_insert_event_queues_to_buffer(self):
        u = self._uploader()
        e = evt()
        u.insert_event(e)
        assert len(u._event_buffer) == 1

    def test_insert_event_converts_to_dict(self):
        u = self._uploader()
        e = evt(service="django", name="view", trace_id="t-1")
        u.insert_event(e)
        batch = u._event_buffer.drain(max_batch=1)
        assert batch[0]["service"] == "django"
        assert batch[0]["trace_id"] == "t-1"

    def test_insert_event_silent_on_exception(self):
        u = self._uploader()
        # Pass a non-NormalizedEvent object — must not raise
        u.insert_event(None)   # type: ignore

    def test_bind_engine_stores_reference(self):
        u = self._uploader()
        u.bind_engine(object())   # any object
        assert u._engine is not None

    def test_stats_returns_all_fields(self):
        u = self._uploader()
        s = u.stats()
        assert "events_sent_total"  in s
        assert "snapshots_sent"     in s
        assert "event_buffer_size"  in s
        assert "engine_bound"       in s
        assert "running"            in s

    def test_start_and_stop_lifecycle(self):
        u = self._uploader()
        u.start()
        assert u._running is True
        assert u._thread is not None and u._thread.is_alive()
        u.stop()
        assert u._running is False


# ====================================================================== #
# Uploader — HTTP flush (mocked)
# ====================================================================== #

class TestUploaderHTTPFlush:
    """
    Test that _flush_events() sends the correct payload to the correct
    endpoint. Uses a mock HTTP client to avoid needing a real server.
    """

    def setup_method(self):
        pytest.importorskip("httpx")

    def test_flush_events_posts_to_correct_endpoint(self, monkeypatch):
        import httpx
        from stacktracer.buffer.uploader import Uploader

        requests_made = []

        def mock_post(url, **kwargs):
            requests_made.append({"url": url, "kwargs": kwargs})
            return httpx.Response(200, json={"status": "ok", "stored": 1})

        monkeypatch.setattr(httpx, "post", mock_post)

        u = Uploader(endpoint="http://backend:8000", api_key="sk_test")
        u.insert_event(evt())
        u._flush_events()

        assert len(requests_made) == 1
        assert requests_made[0]["url"] == "http://backend:8000/api/v1/events"
        headers = requests_made[0]["kwargs"]["headers"]
        assert headers["Authorization"] == "Bearer sk_test"

    def test_flush_snapshot_posts_to_correct_endpoint(self, monkeypatch):
        import httpx
        from stacktracer.buffer.uploader import Uploader
        from stacktracer.core.runtime_graph import RuntimeGraph

        requests_made = []

        def mock_post(url, **kwargs):
            requests_made.append(url)
            return httpx.Response(200, json={"status": "ok", "nodes": 1, "edges": 0})

        monkeypatch.setattr(httpx, "post", mock_post)

        u = Uploader(endpoint="http://backend:8000", api_key="sk_test")

        class FakeEngine:
            graph = RuntimeGraph()

        u.bind_engine(FakeEngine())
        u._last_snapshot_flush_s = 0  # force immediate flush
        u._flush_snapshot()

        assert any("/api/v1/graph/snapshot" in url for url in requests_made)

    def test_flush_events_skips_when_buffer_empty(self, monkeypatch):
        import httpx
        from stacktracer.buffer.uploader import Uploader

        calls = []
        monkeypatch.setattr(httpx, "post", lambda *a, **k: calls.append(1))

        u = Uploader(endpoint="http://backend:8000", api_key="sk_test")
        u._flush_events()  # nothing in buffer

        assert calls == []

    def test_failed_upload_increments_failed_counter(self, monkeypatch):
        import httpx
        from stacktracer.buffer.uploader import Uploader

        def mock_post(*a, **k):
            return httpx.Response(500, text="Internal Server Error")

        monkeypatch.setattr(httpx, "post", mock_post)

        u = Uploader(endpoint="http://backend:8000", api_key="sk_test")
        u.insert_event(evt())
        u._flush_events()

        assert u._failed_event_sends == 1
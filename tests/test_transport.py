from __future__ import annotations

import json
import threading

import httpx
import pytest

from origintracer.core.runtime_graph import RuntimeGraph
from origintracer.sdk.uploader import (
    Uploader,
    _UploaderEventBuffer,
)

from .conftest import evt


class MockBackend:
    """Records every request the uploader makes and returns a canned reply."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.status = 200
        self.body = {"status": "ok", "nodes": 1, "edges": 0}

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status, json=self.body)

    def find(self, path: str) -> httpx.Request:
        return next(
            r for r in self.requests if r.url.path == path
        )


@pytest.fixture
def backend(monkeypatch):
    b = MockBackend()
    real_client = httpx.Client

    def client_factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(b.handler)
        return real_client(*args, **kwargs)

    # Uploader._http() calls httpx.Client(headers=...) at request time,
    # so it picks this up and keeps its real auth-header logic.
    monkeypatch.setattr(httpx, "Client", client_factory)
    return b


class TestEventBuffer:

    def test_push_and_drain(self):
        buf = _UploaderEventBuffer(maxlen=100)
        buf.push({"a": 1})
        buf.push({"b": 2})
        batch = buf.drain(max_batch=10)
        assert len(batch) == 2
        assert batch[0] == {"a": 1}

    def test_drain_respects_max_batch(self):
        buf = _UploaderEventBuffer(maxlen=100)
        for i in range(20):
            buf.push({"i": i})
        batch = buf.drain(max_batch=5)
        assert len(batch) == 5
        assert len(buf) == 15

    def test_drain_empties_buffer(self):
        buf = _UploaderEventBuffer(maxlen=100)
        for i in range(5):
            buf.push({"i": i})
        buf.drain(max_batch=100)
        assert len(buf) == 0

    def test_maxlen_drops_oldest_on_overflow(self):
        buf = _UploaderEventBuffer(maxlen=3)
        for i in range(5):
            buf.push({"i": i})
        batch = buf.drain(max_batch=10)
        # deque(maxlen=3) keeps the last 3 pushed
        assert len(batch) == 3
        assert batch[-1]["i"] == 4

    def test_thread_safe_push_drain(self):
        buf = _UploaderEventBuffer(maxlen=10_000)
        pushed = []
        errors = []

        def pusher():
            try:
                for i in range(500):
                    buf.push({"i": i})
                    pushed.append(1)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=pusher) for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert sum(pushed) == 2000


class TestEmitter:

    def setup_method(self):
        # Reset emitter module state between tests
        import origintracer.sdk.emitter as em

        em._engine = None

    def test_emit_before_bind_is_silent(self):
        from origintracer.sdk.emitter import emit

        # Must not raise
        emit(evt())

    def test_emit_after_bind_calls_engine_process(self):
        from origintracer.sdk.emitter import (
            bind_engine,
            emit,
            flush,
        )

        processed = []

        class FakeEngine:
            def process(self, event):
                processed.append(event)

        bind_engine(FakeEngine())
        emit(evt())

        # NEW: Force the buffer to empty into the engine right now
        # instead of waiting for the 50ms background thread.
        flush()

        assert len(processed) == 1

    def test_bind_engine_twice_replaces_reference(self):
        from origintracer.sdk.emitter import (
            bind_engine,
            emit,
            flush,
            unbind_engine,
        )

        calls_a, calls_b = [], []

        class EngineA:
            def process(self, e):
                calls_a.append(e)

        class EngineB:
            def process(self, e):
                calls_b.append(e)

        # Setup Engine A
        bind_engine(EngineA())
        emit(evt())
        flush()  # Drains into A
        assert len(calls_a) == 1

        # KEY FIX: Full reset before binding B
        unbind_engine()

        # Setup Engine B
        bind_engine(EngineB())
        emit(evt())
        flush()  # Drains into B
        assert len(calls_b) == 1

    def test_engine_exception_does_not_propagate(self):
        from origintracer.sdk.emitter import bind_engine, emit

        class CrashingEngine:
            def process(self, e):
                raise RuntimeError("engine is broken")

        bind_engine(CrashingEngine())
        # Must not raise — probes must never crash the host app
        emit(evt())


class TestUploaderRepositoryInterface:
    """
    Uploader implements BaseRepository.insert_event() so Engine.process()
    can call it directly. Tests verify the interface contract without
    starting the background thread or making HTTP calls.
    """

    def _uploader(self) -> Uploader:
        return Uploader(
            endpoint="http://localhost:9999",
            api_key="test-key",
            flush_interval=9999,  # never flushes automatically
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
        u.insert_event(None)  # type: ignore

    def test_bind_engine_stores_reference(self):
        u = self._uploader()
        u.bind_engine(object())  # any object
        assert u._engine is not None

    def test_stats_returns_all_fields(self):
        u = self._uploader()
        s = u.stats()
        assert "events_sent_total" in s
        assert "snapshots_sent" in s
        assert "event_buffer_size" in s
        assert "engine_bound" in s
        assert "running" in s

    def test_start_and_stop_lifecycle(self):
        u = self._uploader()
        u.start()
        assert u._running is True
        assert u._thread is not None and u._thread.is_alive()
        u.stop()
        assert u._running is False


class TestUploaderHTTPFlush:
    """
    Test that _flush_events() sends the correct payload to the correct
    endpoint. Requests are intercepted at the httpx transport layer.
    """

    def setup_method(self):
        pytest.importorskip("httpx")

    def test_flush_events_posts_to_correct_endpoint(
        self, backend
    ):
        u = Uploader(
            endpoint="http://backend:8000", api_key="sk_test"
        )
        u.insert_event(evt())
        u._flush_events()

        assert len(backend.requests) == 1
        req = backend.requests[0]
        assert (
            str(req.url) == "http://backend:8000/api/v1/events"
        )
        assert req.headers["Authorization"] == "Bearer sk_test"
        assert req.headers["X-OriginTracer-Worker-PID"].isdigit()
        assert req.headers["X-OriginTracer-Worker-Instance-ID"]

    def test_flush_snapshot_posts_to_correct_endpoint(
        self, backend
    ):
        u = Uploader(
            endpoint="http://backend:8000", api_key="sk_test"
        )

        class FakeTemporal:
            def latest_diff(self):
                return {
                    "nodes": [],
                    "edges": [],
                    "label": "test-sync",
                }

        class FakeEngine:
            graph = RuntimeGraph()
            temporal = FakeTemporal()

            @staticmethod
            def evaluate():
                class Match:
                    @staticmethod
                    def to_dict():
                        return {
                            "rule": "app_local_rule",
                            "confidence": 0.8,
                            "explanation": "test match",
                            "evidence": {"count": 6},
                            "timestamp": 1.0,
                        }

                return [Match()]

        u.bind_engine(FakeEngine())
        u._flush_snapshot()

        paths = [r.url.path for r in backend.requests]
        assert "/api/v1/graph/snapshot" in paths
        assert "/api/v1/causal/report" in paths
        assert "/api/v1/graph/diff" in paths

        snapshot_req = backend.find("/api/v1/graph/snapshot")
        causal_req = backend.find("/api/v1/causal/report")
        diff_req = backend.find("/api/v1/graph/diff")

        assert json.loads(causal_req.content)["matches"] == [
            {
                "rule": "app_local_rule",
                "confidence": 0.8,
                "explanation": "test match",
                "evidence": {"count": 6},
                "timestamp": 1.0,
            }
        ]

        assert (
            causal_req.headers["Authorization"]
            == "Bearer sk_test"
        )
        for req in (snapshot_req, causal_req, diff_req):
            assert req.headers[
                "X-OriginTracer-Worker-PID"
            ].isdigit()
            assert req.headers[
                "X-OriginTracer-Worker-Instance-ID"
            ]

        # Same worker identity on every request in one flush
        for name in (
            "X-OriginTracer-Worker-PID",
            "X-OriginTracer-Worker-Instance-ID",
        ):
            assert (
                diff_req.headers[name]
                == snapshot_req.headers[name]
            )

    def test_flush_events_skips_when_buffer_empty(self, backend):
        u = Uploader(
            endpoint="http://backend:8000", api_key="sk_test"
        )
        u._flush_events()  # nothing in buffer

        assert backend.requests == []

    def test_failed_upload_increments_failed_counter(
        self, backend
    ):
        backend.status = 500
        backend.body = {"error": "Internal Server Error"}

        u = Uploader(
            endpoint="http://backend:8000", api_key="sk_test"
        )
        u.insert_event(evt())
        u._flush_events()

        assert u._failed_event_sends == 1

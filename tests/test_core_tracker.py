from __future__ import annotations

import time

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)


class TestActiveRequestTracker:

    def setup_method(self):
        self.t = ActiveRequestTracker(ttl_s=5.0, max_size=100)

    def test_start_creates_span(self):
        self.t.start(
            trace_id="abc",
            service="django",
            pattern="/api/users/",
        )
        assert "abc" in self.t._active

    def test_complete_removes_from_active(self):
        self.t.start(
            trace_id="abc",
            service="django",
            pattern="/api/users/",
        )
        span = self.t.complete(trace_id="abc")
        assert "abc" not in self.t._active
        assert span is not None
        assert span.is_complete

    def test_complete_returns_duration(self):
        self.t.start(
            trace_id="abc",
            service="django",
            pattern="/api/users/",
        )
        # In test_core_tracker.py
        time.sleep(0.02)
        span = self.t.complete(trace_id="abc")
        # Use a lower bound to account for OS scheduling jitter
        assert span.duration_ms >= 14.5

    def test_complete_unknown_trace_returns_none(self):
        assert self.t.complete("nonexistent") is None

    def test_event_appends_probe_to_sequence(self):
        self.t.start(
            trace_id="abc", service="django", pattern="/api/"
        )
        self.t.event(trace_id="abc", probe="db.query.start")
        self.t.event(trace_id="abc", probe="db.query.end")
        span = self.t._active["abc"]
        assert span.probe_sequence == [
            "db.query.start",
            "db.query.end",
        ]

    def test_event_ignores_unknown_trace(self):
        """event() on unknown trace_id must not raise."""
        self.t.event(
            trace_id="nonexistent", probe="db.query.start"
        )

    def test_active_count(self):
        self.t.start("a", "django", "/api/a/")
        self.t.start("b", "django", "/api/b/")
        assert self.t.active_count() == 2

    def test_active_count_by_service(self):
        self.t.start("a", "django", "/api/")
        self.t.start("b", "celery", "process_order")
        assert self.t.active_count(service="django") == 1
        assert self.t.active_count(service="celery") == 1

    def test_slow_in_flight_filters_by_threshold(self):
        self.t.start("fast", "django", "/api/")
        self.t.start("slow", "django", "/api/")
        # Backdate slow entry to simulate 500ms in-flight
        self.t._active["slow"].start_time = (
            time.monotonic() - 0.5
        )
        slow = self.t.slow_in_flight(threshold_ms=200)
        ids = [s.trace_id for s in slow]
        assert "slow" in ids
        assert "fast" not in ids

    def test_all_patterns_summary_after_completions(self):
        for i in range(15):
            self.t.start(f"t{i}", "django", "/api/orders/")
            self.t._active[f"t{i}"].start_time = (
                time.monotonic() - 0.1
            )
            self.t.complete(f"t{i}")
        summary = self.t.all_patterns_summary()
        assert "/api/orders/" in summary
        assert summary["/api/orders/"]["count"] == 15

    def test_ttl_eviction_removes_stale_entries(self):
        tracker = ActiveRequestTracker(ttl_s=0.05, max_size=100)
        tracker.start("stale", "django", "/api/")
        # Backdate to trigger TTL
        tracker._active["stale"].last_event = (
            time.monotonic() - 1.0
        )
        tracker._evict()
        assert "stale" not in tracker._active

    def test_max_size_cap_evicts_oldest(self):
        tracker = ActiveRequestTracker(ttl_s=999, max_size=5)
        for i in range(10):
            tracker.start(f"t{i}", "django", "/api/")
        assert len(tracker._active) <= 5

    def test_start_complete_is_thread_safe(self):
        """Concurrent start/complete from multiple threads must not corrupt state."""
        import threading

        errors = []
        tracker = ActiveRequestTracker(
            ttl_s=30.0, max_size=10_000
        )

        def worker(thread_id):
            try:
                for i in range(100):
                    tid = f"t-{thread_id}-{i}"
                    tracker.start(tid, "django", "/api/")
                    tracker.event(tid, "db.query.start")
                    tracker.complete(tid)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=worker, args=(i,))
            for i in range(5)
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert not errors

"""
tests/test_event_schema.py

Dedicated tests for NormalizedEvent and the event_schema module.

Focus areas:
    1. Construction — direct and via .now() factory
    2. Field extraction — duration_ns, pid, tid must land on the dataclass
       field, not be swallowed into metadata (**kwargs)
    3. Serialisation — to_dict() / from_dict() round-trip preserves every field
    4. Metadata isolation — only genuinely unknown kwargs go into metadata
    5. Timestamps — wall_time and timestamp are captured at construction time
    6. Span identity — span_id generated, parent_span_id propagated correctly
"""

from __future__ import annotations

import time
import uuid

import pytest

from stacktracer.core.event_schema import NormalizedEvent

# ====================================================================== #
# Direct construction
# ====================================================================== #


class TestNormalizedEventConstruction:

    def test_required_fields_set(self):
        e = NormalizedEvent(
            probe="request.entry",
            service="django",
            name="/api/orders/",
            trace_id="t-001",
        )
        assert e.probe == "request.entry"
        assert e.service == "django"
        assert e.name == "/api/orders/"
        assert e.trace_id == "t-001"

    def test_span_id_auto_generated(self):
        e = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        assert e.span_id is not None
        assert len(e.span_id) == 16  # uuid4().hex[:16]

    def test_span_ids_unique_across_instances(self):
        a = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        b = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        assert a.span_id != b.span_id

    def test_metadata_defaults_to_empty_dict(self):
        e = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        assert e.metadata == {}

    def test_duration_ns_defaults_to_none(self):
        e = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        assert e.duration_ns is None

    def test_pid_tid_default_to_none(self):
        e = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        assert e.pid is None
        assert e.tid is None

    def test_wall_time_captured(self):
        before = time.time()
        e = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        after = time.time()
        assert before <= e.wall_time <= after

    def test_parent_span_id_none_by_default(self):
        e = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        assert e.parent_span_id is None


# ====================================================================== #
# .now() factory — field extraction from kwargs
# ====================================================================== #


class TestNormalizedEventNow:
    """
    .now() accepts **kwargs for metadata, but named fields like
    duration_ns, pid, tid must be extracted explicitly and placed on
    the dataclass field — NOT swallowed into metadata.

    This was the bug: duration_ns was going into event.metadata["duration_ns"]
    instead of event.duration_ns, so graph nodes never accumulated duration.
    """

    def test_duration_ns_lands_on_field(self):
        e = NormalizedEvent.now(
            "django.view.exit",
            "t1",
            "django",
            "view",
            duration_ns=10_000_000,
        )
        assert e.duration_ns == 10_000_000, (
            "duration_ns was swallowed into metadata — "
            "NormalizedEvent.now() must explicitly extract it before **kwargs"
        )

    def test_duration_ns_not_in_metadata(self):
        e = NormalizedEvent.now(
            "django.view.exit",
            "t1",
            "django",
            "view",
            duration_ns=10_000_000,
        )
        assert (
            "duration_ns" not in e.metadata
        ), "duration_ns must NOT appear in metadata — it has a dedicated field"

    def test_pid_lands_on_field(self):
        e = NormalizedEvent.now(
            "gunicorn.worker.fork",
            "t1",
            "gunicorn",
            "worker",
            pid=12345,
        )
        assert e.pid == 12345
        assert "pid" not in e.metadata

    def test_tid_lands_on_field(self):
        e = NormalizedEvent.now(
            "asyncio.task.start",
            "t1",
            "asyncio",
            "task",
            tid=99,
        )
        assert e.tid == 99
        assert "tid" not in e.metadata

    def test_parent_span_id_lands_on_field(self):
        e = NormalizedEvent.now(
            "function.call",
            "t1",
            "django",
            "view",
            parent_span_id="abc123",
        )
        assert e.parent_span_id == "abc123"
        assert "parent_span_id" not in e.metadata

    def test_unknown_kwargs_go_to_metadata(self):
        e = NormalizedEvent.now(
            "request.entry",
            "t1",
            "nginx",
            "upstream",
            method="GET",
            http_host="example.com",
        )
        assert e.metadata["method"] == "GET"
        assert e.metadata["http_host"] == "example.com"

    def test_mixed_known_and_unknown_kwargs(self):
        """Known fields go to their field, unknowns go to metadata."""
        e = NormalizedEvent.now(
            "db.query.end",
            "t1",
            "postgres",
            "SELECT",
            duration_ns=5_000_000,  # known field
            rows_returned=42,  # unknown → metadata
            pid=1234,  # known field
        )
        assert e.duration_ns == 5_000_000
        assert e.pid == 1234
        assert e.metadata["rows_returned"] == 42
        assert "duration_ns" not in e.metadata
        assert "pid" not in e.metadata

    def test_duration_none_when_not_passed(self):
        e = NormalizedEvent.now(
            "request.entry", "t1", "nginx", "accept"
        )
        assert e.duration_ns is None

    def test_wall_time_captured_at_call_site(self):
        before = time.time()
        e = NormalizedEvent.now("p", "t", "s", "n")
        after = time.time()
        assert before <= e.wall_time <= after

    def test_span_id_auto_generated_by_now(self):
        e = NormalizedEvent.now("p", "t", "s", "n")
        assert e.span_id is not None
        assert len(e.span_id) == 16


# ====================================================================== #
# Serialisation — to_dict() / from_dict()
# ====================================================================== #


class TestNormalizedEventSerialisation:

    def _make(self, **kwargs) -> NormalizedEvent:
        defaults = dict(
            probe="django.view.exit",
            trace_id="t-serial",
            service="django",
            name="OrderView.get",
            duration_ns=7_000_000,
            pid=1234,
            tid=5678,
        )
        defaults.update(kwargs)
        return NormalizedEvent.now(**defaults)

    def test_to_dict_contains_all_top_level_fields(self):
        e = self._make()
        d = e.to_dict()
        for field in (
            "probe",
            "service",
            "name",
            "trace_id",
            "span_id",
            "duration_ns",
            "pid",
            "tid",
            "wall_time",
            "timestamp",
            "metadata",
        ):
            assert (
                field in d
            ), f"to_dict() missing field: {field}"

    def test_from_dict_restores_probe_service_name(self):
        e = self._make()
        restored = NormalizedEvent.from_dict(e.to_dict())
        assert restored.probe == e.probe
        assert restored.service == e.service
        assert restored.name == e.name

    def test_from_dict_restores_trace_and_span_ids(self):
        e = self._make()
        restored = NormalizedEvent.from_dict(e.to_dict())
        assert restored.trace_id == e.trace_id
        assert restored.span_id == e.span_id

    def test_from_dict_restores_duration_ns(self):
        e = self._make(duration_ns=9_000_000)
        restored = NormalizedEvent.from_dict(e.to_dict())
        assert restored.duration_ns == 9_000_000

    def test_from_dict_restores_pid_tid(self):
        e = self._make(pid=111, tid=222)
        restored = NormalizedEvent.from_dict(e.to_dict())
        assert restored.pid == 111
        assert restored.tid == 222

    def test_from_dict_restores_parent_span_id(self):
        parent_id = uuid.uuid4().hex[:16]
        e = NormalizedEvent.now(
            "function.call",
            "t1",
            "django",
            "view",
            parent_span_id=parent_id,
        )
        restored = NormalizedEvent.from_dict(e.to_dict())
        assert restored.parent_span_id == parent_id

    def test_from_dict_restores_metadata(self):
        e = NormalizedEvent.now(
            "request.entry",
            "t1",
            "nginx",
            "upstream",
            method="POST",
            status_code=201,
        )
        restored = NormalizedEvent.from_dict(e.to_dict())
        assert restored.metadata["method"] == "POST"
        assert restored.metadata["status_code"] == 201

    def test_round_trip_is_idempotent(self):
        """Two round-trips must produce identical dicts."""
        e = self._make(method="GET", path="/api/")
        d1 = e.to_dict()
        d2 = NormalizedEvent.from_dict(d1).to_dict()
        assert d1 == d2

    def test_duration_ns_not_duplicated_in_metadata_after_round_trip(
        self,
    ):
        """
        After serialise → deserialise, duration_ns must still be only on
        the top-level field. If to_dict() accidentally writes it into
        metadata and from_dict() reads it back, we'd get double-counting.
        """
        e = self._make(duration_ns=3_000_000)
        restored = NormalizedEvent.from_dict(e.to_dict())
        assert restored.duration_ns == 3_000_000
        assert "duration_ns" not in restored.metadata


# ====================================================================== #
# repr and string formatting
# ====================================================================== #


class TestNormalizedEventRepr:

    def test_repr_contains_probe(self):
        e = NormalizedEvent(
            probe="request.entry",
            service="django",
            name="/api",
            trace_id="abc123xyz",
        )
        assert "request.entry" in repr(e)

    def test_repr_contains_partial_trace_id(self):
        e = NormalizedEvent(
            probe="p",
            service="s",
            name="n",
            trace_id="abc123xyz",
        )
        assert "abc123" in repr(e)

    def test_repr_does_not_raise_on_none_fields(self):
        e = NormalizedEvent(
            probe="p", service="s", name="n", trace_id="t"
        )
        # Must not raise even with all optional fields at defaults
        _ = repr(e)


# ====================================================================== #
# Edge cases
# ====================================================================== #


class TestNormalizedEventEdgeCases:

    def test_empty_trace_id_allowed(self):
        """Probes that don't have a trace context yet emit with trace_id=''."""
        e = NormalizedEvent.now(
            "gunicorn.master.start", "", "gunicorn", "master"
        )
        assert e.trace_id == ""

    def test_zero_duration_ns_stored_as_zero_not_none(self):
        """duration_ns=0 is a valid value (e.g. sub-microsecond ops), not None."""
        e = NormalizedEvent.now(
            "p", "t", "s", "n", duration_ns=0
        )
        assert e.duration_ns == 0
        assert e.duration_ns is not None

    def test_large_duration_ns_stored_correctly(self):
        """30-second request = 30_000_000_000 ns — must not overflow."""
        e = NormalizedEvent.now(
            "p", "t", "s", "n", duration_ns=30_000_000_000
        )
        assert e.duration_ns == 30_000_000_000

    def test_metadata_is_independent_per_instance(self):
        """Two events must not share the same metadata dict."""
        a = NormalizedEvent.now("p", "t1", "s", "n", key="a")
        b = NormalizedEvent.now("p", "t2", "s", "n", key="b")
        assert a.metadata is not b.metadata
        assert a.metadata["key"] == "a"
        assert b.metadata["key"] == "b"

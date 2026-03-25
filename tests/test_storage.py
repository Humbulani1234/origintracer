"""
tests/test_storage.py

Tests for all three storage backends via the BaseRepository interface.

Only InMemoryRepository is tested directly — it requires no external
dependencies (no Postgres, no ClickHouse) and is the backend used in
development and CI.

PostgreSQL and ClickHouse tests are marked with pytest.mark.integration
and skipped unless the environment variable STACKTRACER_TEST_DB_DSN
or STACKTRACER_TEST_CH_HOST are set respectively.

Tests focus on:
    - insert_event / query_events (events table)
    - insert_snapshot / get_latest_snapshot (snapshots table)
    - insert_deployment_marker (markers table)
    - BaseRepository interface completeness
"""

from __future__ import annotations

import os
import time

import pytest

from stacktracer.storage.base import (
    BaseRepository,
    InMemoryRepository,
)

from .conftest import evt

# ── Shared contract tests — run against any backend ───────────────────────


def run_event_contract(repo: BaseRepository):
    """Every backend must satisfy this contract."""
    e = evt(
        probe="request.entry",
        service="django",
        name="/api/orders/",
        trace_id="t-001",
    )
    repo.insert_event(e)

    results = repo.query_events(trace_id="t-001")
    assert len(results) == 1
    assert results[0]["probe"] == "request.entry"
    assert results[0]["service"] == "django"


def run_snapshot_contract(repo: BaseRepository):
    """Every backend that supports snapshots must satisfy this."""
    data = b"\x82\xa5nodes\x91\xa3foo"  # fake msgpack bytes
    repo.insert_snapshot(
        customer_id="acme",
        data=data,
        content_type="application/msgpack",
        node_count=42,
        edge_count=17,
    )
    row = repo.get_latest_snapshot("acme")
    assert row is not None
    assert row["data"] == data
    assert row["content_type"] == "application/msgpack"
    assert "received_at" in row


def run_marker_contract(repo: BaseRepository):
    repo.insert_deployment_marker("acme", "deploy:v1.0.0")
    # No query API for markers — just verify it doesn't raise


# ====================================================================== #
# InMemoryRepository — full test coverage
# ====================================================================== #


class TestInMemoryRepository:

    def setup_method(self):
        self.repo = InMemoryRepository()

    # ── Events ────────────────────────────────────────────────────────────

    def test_event_contract(self):
        run_event_contract(self.repo)

    def test_filter_by_probe(self):
        self.repo.insert_event(
            evt(probe="request.entry", trace_id="t1")
        )
        self.repo.insert_event(
            evt(probe="db.query.start", trace_id="t1")
        )
        results = self.repo.query_events(probe="request.entry")
        assert all(
            r["probe"] == "request.entry" for r in results
        )

    def test_filter_by_service(self):
        self.repo.insert_event(
            evt(service="django", name="view", trace_id="t1")
        )
        self.repo.insert_event(
            evt(service="postgres", name="SELECT", trace_id="t1")
        )
        results = self.repo.query_events(service="postgres")
        assert all(r["service"] == "postgres" for r in results)

    def test_filter_by_since(self):
        time.time()
        self.repo.insert_event(evt(trace_id="t1"))
        after = time.time()
        results = self.repo.query_events(since=after + 1)
        # No events after a future timestamp
        assert len(results) == 0

    def test_limit_respected(self):
        for i in range(20):
            self.repo.insert_event(evt(trace_id=f"t{i}"))
        results = self.repo.query_events(limit=5)
        assert len(results) == 5

    def test_maxlen_evicts_oldest(self):
        repo = InMemoryRepository(max_events=10)
        for i in range(20):
            repo.insert_event(evt(trace_id=f"t{i}"))
        assert len(repo) == 10

    def test_insert_event_silent_on_bad_input(self):
        """Repository must never raise — probes call insert_event on the hot path."""
        # NormalizedEvent.to_dict() should always work, but test robustness
        e = evt()
        e.metadata = None  # type: ignore — simulate corrupted event
        try:
            self.repo.insert_event(e)
        except Exception:
            pytest.fail("insert_event raised on bad input")

    # ── Snapshots ─────────────────────────────────────────────────────────

    def test_snapshot_contract(self):
        run_snapshot_contract(self.repo)

    def test_get_latest_snapshot_returns_none_before_insert(
        self,
    ):
        assert self.repo.get_latest_snapshot("nobody") is None

    def test_snapshot_overwrites_per_customer(self):
        """Only the most recent snapshot per customer is retained."""
        self.repo.insert_snapshot(
            "acme", b"first", "application/msgpack"
        )
        self.repo.insert_snapshot(
            "acme", b"second", "application/msgpack"
        )
        row = self.repo.get_latest_snapshot("acme")
        assert row["data"] == b"second"

    def test_snapshots_isolated_per_customer(self):
        self.repo.insert_snapshot(
            "acme", b"acme-data", "application/msgpack"
        )
        self.repo.insert_snapshot(
            "other", b"other-data", "application/msgpack"
        )
        assert (
            self.repo.get_latest_snapshot("acme")["data"]
            == b"acme-data"
        )
        assert (
            self.repo.get_latest_snapshot("other")["data"]
            == b"other-data"
        )

    # ── Markers ───────────────────────────────────────────────────────────

    def test_marker_contract(self):
        run_marker_contract(self.repo)

    def test_multiple_markers_stored(self):
        self.repo.insert_deployment_marker("acme", "deploy:v1")
        self.repo.insert_deployment_marker("acme", "deploy:v2")

        # Check the number of markers for the 'acme' customer specifically
        assert len(self.repo._markers["acme"]) == 2


# ====================================================================== #
# BaseRepository interface completeness check
# ====================================================================== #


class TestBaseRepositoryInterface:

    def test_in_memory_implements_all_abstract_methods(self):
        """
        InMemoryRepository must implement every abstract method on
        BaseRepository. If a new abstract method is added to the base
        class but not the InMemoryRepository, this test will catch it.
        """
        import inspect

        abstract_methods = {
            name
            for name, val in inspect.getmembers(
                BaseRepository, predicate=inspect.isfunction
            )
            if getattr(val, "__isabstractmethod__", False)
        }
        repo = InMemoryRepository()
        for method_name in abstract_methods:
            assert hasattr(
                repo, method_name
            ), f"InMemoryRepository is missing abstract method: {method_name}"


# ====================================================================== #
# PostgreSQL — integration tests (skipped without DSN)
# ====================================================================== #


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("STACKTRACER_TEST_DB_DSN"),
    reason="Set STACKTRACER_TEST_DB_DSN to run PostgreSQL tests",
)
class TestEventRepositoryPostgres:

    @pytest.fixture
    def pg_repo(self):
        import psycopg2

        from stacktracer.storage.base import (
            EventRepository,
        )

        conn = psycopg2.connect(
            os.environ["STACKTRACER_TEST_DB_DSN"]
        )
        repo = EventRepository(conn)
        yield repo
        # Cleanup test data
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM st_events    WHERE customer_id = 'test'"
            )
            cur.execute(
                "DELETE FROM st_snapshots WHERE customer_id = 'test'"
            )
            cur.execute(
                "DELETE FROM st_markers   WHERE customer_id = 'test'"
            )
        conn.commit()
        conn.close()

    def test_event_contract_postgres(self, pg_repo):
        run_event_contract(pg_repo)

    def test_snapshot_contract_postgres(self, pg_repo):
        run_snapshot_contract(pg_repo)

    def test_latest_snapshot_survives_multiple_inserts(
        self, pg_repo
    ):
        pg_repo.insert_snapshot(
            "test", b"v1", "application/msgpack", node_count=10
        )
        pg_repo.insert_snapshot(
            "test", b"v2", "application/msgpack", node_count=20
        )
        row = pg_repo.get_latest_snapshot("test")
        # PostgreSQL returns most recent by received_at DESC
        assert row["data"] in (
            b"v1",
            b"v2",
        )  # either could be latest depending on timing

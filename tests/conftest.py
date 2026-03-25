"""
tests/conftest.py

Shared fixtures used across all test modules.

Scope strategy:
    function  — default, fresh state per test (most fixtures)
    session   — expensive setup done once (none here yet — keep tests independent)

All fixtures are zero-dependency — no BPF, no Django, no Postgres required.
The test suite must run with: pytest tests/ -v
"""

from __future__ import annotations

import uuid

import pytest

from stacktracer.core.active_requests import ActiveRequestTracker
from stacktracer.core.causal import build_default_registry
from stacktracer.core.engine import Engine
from stacktracer.core.event_schema import NormalizedEvent
from stacktracer.core.semantic import (
    SemanticAlias,
    SemanticLayer,
)
from stacktracer.sdk.emitter import bind_engine, unbind_engine
from stacktracer.storage.base import InMemoryRepository

# ── Helpers ────────────────────────────────────────────────────────────────


def evt(
    probe: str = "function.call",
    service: str = "django",
    name: str = "view",
    trace_id: str = "trace-default",
    duration_ns: int | None = None,
    **meta,
) -> NormalizedEvent:
    """One-liner event factory used throughout tests."""
    return NormalizedEvent.now(
        probe=probe,
        trace_id=trace_id,
        service=service,
        name=name,
        **({"duration_ns": duration_ns} if duration_ns else {}),
        **meta,
    )


# ── Core fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def trace_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def tracker() -> ActiveRequestTracker:
    return ActiveRequestTracker(ttl_s=30.0, max_size=1000)


@pytest.fixture
def engine(tracker) -> Engine:
    """
    A fully wired Engine with tracker, registry, and semantic layer.
    Background tasks (snapshot thread) are disabled so tests are deterministic.
    """
    sem = SemanticLayer()
    sem.register(
        SemanticAlias(
            label="api",
            description="API surface",
            node_patterns=["django::/api/.*"],
            services=[],
        )
    )
    sem.register(
        SemanticAlias(
            label="db",
            description="Database layer",
            services=["postgres", "redis"],
            node_patterns=[],
        )
    )

    sem.register(
        SemanticAlias(
            label="export",
            description="The full export pipeline",
            services=["exporter"],
            node_patterns=["django::handle_export"],
        )
    )

    e = Engine(
        causal_registry=build_default_registry(tracker=tracker),
        semantic_layer=sem,
        snapshot_interval_s=9999,  # never fires during tests
    )
    e.tracker = tracker
    bind_engine(e)
    return e


@pytest.fixture
def graph(engine):
    """Convenience shortcut to engine.graph."""
    return engine.graph


@pytest.fixture
def repo():
    return InMemoryRepository()


@pytest.fixture(autouse=True)
def cleanup_emitter():
    """
    This runs automatically after EVERY test.
    It ensures no background threads are left running.
    """
    yield  # Run the test
    unbind_engine()  # Clean up after the test

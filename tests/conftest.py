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

from origintracer.core.active_requests import (
    ActiveRequestTracker,
)
from origintracer.core.causal import PatternRegistry
from origintracer.core.engine import Engine
from origintracer.core.event_schema import NormalizedEvent
from origintracer.core.semantic import (
    SemanticAlias,
    SemanticLayer,
)
from origintracer.sdk.emitter import bind_engine, unbind_engine
from origintracer.storage.base import InMemoryRepository

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


@pytest.fixture(autouse=True, scope="session")
def load_builtin_rules():
    import importlib
    import pkgutil

    import origintracer.rules

    for _, module_name, _ in pkgutil.iter_modules(
        origintracer.rules.__path__
    ):
        importlib.import_module(
            f"origintracer.rules.{module_name}"
        )
    # User rules that need to be available in tests
    from applications.django.rules import (
        gunicorn_rules,
        nginx_rules,
        uvicorn_rules,
    )

    gunicorn_rules.register(PatternRegistry)
    uvicorn_rules.register(PatternRegistry)
    nginx_rules.register(PatternRegistry)


@pytest.fixture(autouse=True, scope="function")
def reset_registry(load_builtin_rules):
    snapshot = dict(PatternRegistry._rules)
    yield
    PatternRegistry._rules = snapshot


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
        semantic_layer=sem,
        snapshot_interval_s=9999,  # never fires during tests
    )
    e.tracker = tracker
    e.causal = PatternRegistry
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

from __future__ import annotations

import uuid

import pytest

from backend import main
from backend.main import _active_pid, _active_pid_lock
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


def evt(
    probe: str = "function.call",
    service: str = "django",
    name: str = "view",
    trace_id: str = "trace-default",
    duration_ns: int | None = None,
    **meta,
) -> NormalizedEvent:
    """
    One-liner event factory used throughout tests.
    """
    return NormalizedEvent.now(
        probe=probe,
        trace_id=trace_id,
        service=service,
        name=name,
        **({"duration_ns": duration_ns} if duration_ns else {}),
        **meta,
    )


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
            label="django",
            description="Django application layer",
            services=["django"],
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
    e.probes = []
    bind_engine(e)
    return e


@pytest.fixture
def graph(engine):
    """
    Convenience shortcut to engine.graph.
    """
    return engine.graph


@pytest.fixture
def repo():
    return InMemoryRepository()


@pytest.fixture(autouse=True)
def cleanup_emitter():
    """
    This runs automatically after every test.
    It ensures no background threads are left running.
    """
    yield  # Run the test
    unbind_engine()  # Clean up after the test


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires_rule: skip test if rule not registered",
    )


@pytest.fixture(autouse=False)
def require_rule(request):
    marker = request.node.get_closest_marker("requires_rule")
    if marker:
        rule_name = marker.args[0]
        if rule_name not in PatternRegistry.rule_names():
            pytest.skip(
                f"Rule '{rule_name}' not registered - user rule not loaded"
            )


TEST_CUSTOMER = "dev_customer"
TEST_WORKER_PID = "12345"


@pytest.fixture()
def reset_global_state():
    """Reset all global state before every test."""
    with main._active_pid_lock:
        main._active_pid.clear()
    with main._graphs_lock:
        main._graphs.clear()
    yield
    with main._active_pid_lock:
        main._active_pid.clear()
    with main._graphs_lock:
        main._graphs.clear()


@pytest.fixture
def prepopulate_pid():
    """Pre-populate _active_pid for tests that need it already set."""
    with _active_pid_lock:
        _active_pid[TEST_CUSTOMER] = TEST_WORKER_PID

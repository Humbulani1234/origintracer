"""
tests/test_backend.py

Integration tests for the FastAPI backend using httpx.AsyncClient
with ASGITransport — no real server started, no ports bound.

Tests cover the full HTTP layer: auth, routing, request/response shapes,
snapshot deserialization, and the startup snapshot reload path.

Skipped if fastapi or httpx are not installed.
"""

from __future__ import annotations

import json
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from httpx import AsyncClient, ASGITransport
from stacktracer.backend.main import app
from stacktracer.storage.repository import InMemoryRepository

# ── Test client fixture ────────────────────────────────────────────────────

@pytest.fixture
async def client():
    """AsyncClient wired directly to the ASGI app — no TCP involved."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture(autouse=True)
def reset_backend_state():
    """
    Reset module-level state between tests so they are independent.
    Runs before each test in this module.
    """
    import stacktracer.backend.main as m
    m._graphs.clear()
    m._valid_api_keys = {"test-key-0000": "test_customer"}
    m._repository = InMemoryRepository()
    yield
    m._graphs.clear()


AUTH  = {"Authorization": "Bearer test-key-0000"}
NOAUTH: dict = {}


# ====================================================================== #
# Auth
# ====================================================================== #

@pytest.mark.anyio
class TestAuth:

    async def test_missing_auth_returns_401(self, client):
        r = await client.get("/api/v1/status")
        assert r.status_code == 401

    async def test_wrong_key_returns_401(self, client):
        r = await client.get("/api/v1/status", headers={"Authorization": "Bearer wrong-key"})
        assert r.status_code == 401

    async def test_valid_key_returns_200(self, client):
        r = await client.get("/api/v1/status", headers=AUTH)
        assert r.status_code == 200

    async def test_health_requires_no_auth(self, client):
        r = await client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"


# ====================================================================== #
# Event ingest
# ====================================================================== #

@pytest.mark.anyio
class TestEventIngest:

    async def test_ingest_events_stores_and_returns_count(self, client):
        payload = {
            "events": [
                {
                    "probe": "request.entry", "service": "django",
                    "name": "/api/orders/", "trace_id": "t-001",
                    "span_id": "abc", "wall_time": 1.0, "timestamp": 1.0,
                    "metadata": {},
                }
            ]
        }
        r = await client.post("/api/v1/events", json=payload, headers=AUTH)
        assert r.status_code == 200
        data = r.json()
        assert data["stored"] == 1
        assert data["errors"] == 0

    async def test_ingest_compat_alias_works(self, client):
        """POST /api/v1/ingest is a backward-compatible alias for /events."""
        payload = {"events": []}
        r = await client.post("/api/v1/ingest", json=payload, headers=AUTH)
        assert r.status_code == 200

    async def test_ingest_with_empty_events(self, client):
        r = await client.post("/api/v1/events", json={"events": []}, headers=AUTH)
        assert r.status_code == 200
        assert r.json()["stored"] == 0


# ====================================================================== #
# Graph snapshot
# ====================================================================== #

@pytest.mark.anyio
class TestGraphSnapshot:

    def _make_snapshot_bytes(self) -> bytes:
        """Build real msgpack snapshot bytes from a tiny RuntimeGraph."""
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.graph_serializer import MsgpackSerializer
        g = RuntimeGraph()
        g.upsert_node("django::view",   "fn", "django")
        g.upsert_node("postgres::SELECT", "db", "postgres")
        g.upsert_edge("django::view", "postgres::SELECT", "calls")
        return MsgpackSerializer().serialize(g)

    async def test_receive_snapshot_stores_in_memory(self, client):
        pytest.importorskip("msgpack")
        data = self._make_snapshot_bytes()
        r = await client.post(
            "/api/v1/graph/snapshot", content=data,
            headers={**AUTH, "content-type": "application/msgpack"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["nodes"] == 2
        assert body["edges"] == 1

    async def test_receive_snapshot_persists_to_repository(self, client):
        pytest.importorskip("msgpack")
        import stacktracer.backend.main as m
        data = self._make_snapshot_bytes()
        await client.post(
            "/api/v1/graph/snapshot", content=data,
            headers={**AUTH, "content-type": "application/msgpack"},
        )
        row = m._repository.get_latest_snapshot("test_customer")
        assert row is not None
        assert row["data"] == data

    async def test_empty_snapshot_returns_400(self, client):
        r = await client.post(
            "/api/v1/graph/snapshot", content=b"",
            headers={**AUTH, "content-type": "application/msgpack"},
        )
        assert r.status_code == 400

    async def test_invalid_msgpack_returns_400(self, client):
        r = await client.post(
            "/api/v1/graph/snapshot", content=b"not-valid-msgpack",
            headers={**AUTH, "content-type": "application/msgpack"},
        )
        assert r.status_code == 400


# ====================================================================== #
# Graph queries (require snapshot first)
# ====================================================================== #

@pytest.mark.anyio
class TestGraphQueries:

    @pytest.fixture(autouse=True)
    async def load_snapshot(self, client):
        """Load a real snapshot before each test in this class."""
        pytest.importorskip("msgpack")
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.graph_serializer import MsgpackSerializer
        g = RuntimeGraph()
        for _ in range(5):
            g.upsert_node("django::view",     "fn", "django")
        g.upsert_node("postgres::SELECT",     "db", "postgres")
        g.upsert_edge("django::view", "postgres::SELECT", "calls")
        data = MsgpackSerializer().serialize(g)
        await client.post(
            "/api/v1/graph/snapshot", content=data,
            headers={**AUTH, "content-type": "application/msgpack"},
        )

    async def test_get_graph_returns_nodes(self, client):
        r = await client.get("/api/v1/graph", headers=AUTH)
        assert r.status_code == 200
        body = r.json()
        assert "data" in body or "nodes" in body

    async def test_hotspots_returns_sorted_list(self, client):
        r = await client.get("/api/v1/hotspots?top=5", headers=AUTH)
        assert r.status_code == 200
        data = r.json()["data"]
        assert len(data) <= 5
        # First node should be django::view (called 5 times vs postgres once)
        assert data[0]["node"] == "django::view"

    async def test_causal_returns_matches_list(self, client):
        r = await client.get("/api/v1/causal", headers=AUTH)
        assert r.status_code == 200
        assert "matches" in r.json()

    async def test_get_graph_before_snapshot_returns_404(self, client):
        import stacktracer.backend.main as m
        m._graphs.clear()   # remove snapshot
        r = await client.get("/api/v1/graph", headers=AUTH)
        assert r.status_code == 404

    async def test_dsl_query_endpoint(self, client):
        r = await client.post(
            "/api/v1/query",
            json={"query": 'SHOW latency WHERE service = "django"'},
            headers=AUTH,
        )
        assert r.status_code == 200


# ====================================================================== #
# Status
# ====================================================================== #

@pytest.mark.anyio
class TestStatus:

    async def test_status_without_snapshot(self, client):
        r = await client.get("/api/v1/status", headers=AUTH)
        assert r.status_code == 200
        body = r.json()
        assert body["snapshot"]["available"] is False
        assert body["customer_id"] == "test_customer"

    async def test_status_with_snapshot(self, client):
        pytest.importorskip("msgpack")
        from stacktracer.core.runtime_graph import RuntimeGraph
        from stacktracer.core.graph_serializer import MsgpackSerializer
        g = RuntimeGraph()
        g.upsert_node("svc::fn", "fn", "svc")
        data = MsgpackSerializer().serialize(g)
        await client.post(
            "/api/v1/graph/snapshot", content=data,
            headers={**AUTH, "content-type": "application/msgpack"},
        )
        r = await client.get("/api/v1/status", headers=AUTH)
        body = r.json()
        assert body["snapshot"]["available"] is True
        assert body["snapshot"]["nodes"] == 1


# ====================================================================== #
# Deployment marker
# ====================================================================== #

@pytest.mark.anyio
class TestDeploymentMarker:

    async def test_post_deployment_returns_ok(self, client):
        r = await client.post(
            "/api/v1/deployment",
            json={"label": "v2.1.0"},
            headers=AUTH,
        )
        assert r.status_code == 200
        assert r.json()["label"] == "v2.1.0"
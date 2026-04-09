"""
Integration tests for the FastAPI backend using httpx.AsyncClient
with ASGITransport — no real server started, no ports bound.

Tests cover the full HTTP layer: auth, routing, request/response shapes,
snapshot deserialization, and the startup snapshot reload path.

Skipped if fastapi or httpx are not installed.
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from backend.main import app
from origintracer.storage.base import InMemoryRepository

# ── Test client fixture ────────────────────────────────────────────────────

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

AUTH = {"Authorization": "Bearer test-key-0000"}
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
        r = await client.get(
            "/api/v1/status",
            headers={"Authorization": "Bearer wrong-key"},
        )
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

    async def test_ingest_events_stores_and_returns_count(
        self, client
    ):
        payload = {
            "events": [
                {
                    "probe": "request.entry",
                    "service": "django",
                    "name": "/api/orders/",
                    "trace_id": "t-001",
                    "span_id": "abc",
                    "wall_time": 1.0,
                    "timestamp": 1.0,
                    "metadata": {},
                }
            ]
        }
        r = await client.post(
            "/api/v1/events", json=payload, headers=AUTH
        )
        assert r.status_code == 200
        data = r.json()
        assert data["stored"] == 1
        assert data["errors"] == 0

    async def test_ingest_with_empty_events(self, client):
        r = await client.post(
            "/api/v1/events", json={"events": []}, headers=AUTH
        )
        assert r.status_code == 200
        assert r.json()["stored"] == 0


# ====================================================================== #
# Graph snapshot
# ====================================================================== #


@pytest.mark.anyio
class TestGraphSnapshot:

    def _make_snapshot_bytes(self) -> bytes:
        """Build real msgpack snapshot bytes from a tiny RuntimeGraph."""
        from origintracer.core.graph_serializer import (
            MsgpackSerializer,
        )
        from origintracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        g.upsert_node("django::view", "fn", "django")
        g.upsert_node("postgres::SELECT", "db", "postgres")
        g.upsert_edge(
            "django::view", "postgres::SELECT", "calls"
        )
        return MsgpackSerializer().serialize(g)

    async def test_receive_snapshot_stores_in_memory(
        self, client
    ):
        pytest.importorskip("msgpack")
        data = self._make_snapshot_bytes()
        r = await client.post(
            "/api/v1/graph/snapshot",
            content=data,
            headers={
                **AUTH,
                "content-type": "application/msgpack",
            },
        )
        assert r.status_code == 200
        body = r.json()
        assert body["nodes"] == 2
        assert body["edges"] == 1

    async def test_receive_snapshot_persists_to_repository(
        self, client
    ):
        pytest.importorskip("msgpack")
        import backend.main as m

        data = self._make_snapshot_bytes()
        await client.post(
            "/api/v1/graph/snapshot",
            content=data,
            headers={
                **AUTH,
                "content-type": "application/msgpack",
            },
        )
        row = m._repository.get_latest_snapshot("test_customer")
        assert row is not None
        assert row["data"] == data

    async def test_empty_snapshot_returns_400(self, client):
        r = await client.post(
            "/api/v1/graph/snapshot",
            content=b"",
            headers={
                **AUTH,
                "content-type": "application/msgpack",
            },
        )
        assert r.status_code == 400

    async def test_invalid_msgpack_returns_400(self, client):
        r = await client.post(
            "/api/v1/graph/snapshot",
            content=b"not-valid-msgpack",
            headers={
                **AUTH,
                "content-type": "application/msgpack",
            },
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
        from origintracer.core.graph_serializer import (
            MsgpackSerializer,
        )
        from origintracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        for _ in range(5):
            g.upsert_node("django::view", "fn", "django")
        g.upsert_node("postgres::SELECT", "db", "postgres")
        g.upsert_edge(
            "django::view", "postgres::SELECT", "calls"
        )
        data = MsgpackSerializer().serialize(g)
        await client.post(
            "/api/v1/graph/snapshot",
            content=data,
            headers={
                **AUTH,
                "content-type": "application/msgpack",
            },
        )

    async def test_get_graph_returns_nodes(self, client):
        r = await client.get("/api/v1/graph", headers=AUTH)
        assert r.status_code == 200
        body = r.json()
        assert "data" in body or "nodes" in body

    async def test_hotspots_returns_sorted_list(self, client):
        r = await client.get(
            "/api/v1/hotspots?top=5", headers=AUTH
        )
        assert r.status_code == 200
        data = r.json()["data"]
        assert len(data) <= 5
        # First node should be django::view (called 5 times vs postgres once)
        assert data[0]["node"] == "django::view"

    async def test_causal_returns_matches_list(self, client):
        r = await client.get("/api/v1/causal", headers=AUTH)
        assert r.status_code == 200
        assert "matches" in r.json()

    async def test_get_graph_before_snapshot_returns_404(
        self, client
    ):
        import backend.main as m

        m._graphs.clear()  # remove snapshot
        r = await client.get("/api/v1/graph", headers=AUTH)
        assert r.status_code == 404

    async def test_dsl_query_endpoint(self, client):
        r = await client.post(
            "/api/v1/query",
            json={
                "query": 'SHOW latency WHERE service = "django"'
            },
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
        from origintracer.core.graph_serializer import (
            MsgpackSerializer,
        )
        from origintracer.core.runtime_graph import RuntimeGraph

        g = RuntimeGraph()
        g.upsert_node("svc::fn", "fn", "svc")
        data = MsgpackSerializer().serialize(g)
        await client.post(
            "/api/v1/graph/snapshot",
            content=data,
            headers={
                **AUTH,
                "content-type": "application/msgpack",
            },
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


@pytest.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture(autouse=True)
def reset_backend_state():
    import backend.main as m

    m._graphs.clear()
    m._valid_api_keys = {"test-key-0000": "test_customer"}
    m._repository = InMemoryRepository()
    yield
    m._graphs.clear()


AUTH = {"Authorization": "Bearer test-key-0000"}


def _make_snapshot_bytes(node_count: int = 2) -> bytes:
    """Build minimal msgpack snapshot bytes."""
    from origintracer.core.graph_serializer import (
        MsgpackSerializer,
    )
    from origintracer.core.runtime_graph import RuntimeGraph

    g = RuntimeGraph()
    g.upsert_node("django::view", "fn", "django")
    g.upsert_node("postgres::SELECT", "db", "postgres")
    if node_count > 2:
        g.upsert_node("redis::GET", "cache", "redis")
    g.upsert_edge("django::view", "postgres::SELECT", "calls")
    return MsgpackSerializer().serialize(g)


async def _post_snapshot(client, bytes_: bytes) -> None:
    await client.post(
        "/api/v1/graph/snapshot",
        content=bytes_,
        headers={**AUTH, "content-type": "application/msgpack"},
    )


# ====================================================================== #
# GET /api/v1/nodes
# ====================================================================== #


@pytest.mark.anyio
class TestNodes:

    @pytest.fixture(autouse=True)
    async def load_snapshot(self, client):
        pytest.importorskip("msgpack")
        await _post_snapshot(client, _make_snapshot_bytes())

    async def test_nodes_returns_all_nodes(self, client):
        r = await client.get("/api/v1/nodes", headers=AUTH)
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        nodes = body["data"]["data"]
        assert len(nodes) == 2
        node_ids = {n["id"] for n in nodes}
        assert "django::view" in node_ids
        assert "postgres::SELECT" in node_ids

    async def test_nodes_contains_expected_fields(self, client):
        r = await client.get("/api/v1/nodes", headers=AUTH)
        node = r.json()["data"]["data"][0]
        for field in (
            "id",
            "service",
            "node_type",
            "call_count",
            "avg_duration_ns",
            "first_seen",
            "last_seen",
            "metadata",
        ):
            assert field in node, f"Missing field: {field}"

    async def test_nodes_filter_by_service(self, client):
        r = await client.get(
            "/api/v1/nodes?service=django", headers=AUTH
        )
        assert r.status_code == 200
        nodes = r.json()["data"]["data"]
        assert all(n["service"] == "django" for n in nodes)
        assert len(nodes) == 1
        assert nodes[0]["id"] == "django::view"

    async def test_nodes_filter_by_nonexistent_service_returns_empty(
        self, client
    ):
        r = await client.get(
            "/api/v1/nodes?service=nonexistent", headers=AUTH
        )
        assert r.status_code == 200
        assert r.json()["data"]["data"] == []

    async def test_nodes_before_snapshot_returns_404(
        self, client
    ):
        import backend.main as m

        m._graphs.clear()
        r = await client.get("/api/v1/nodes", headers=AUTH)
        assert r.status_code == 404

    async def test_nodes_requires_auth(self, client):
        r = await client.get("/api/v1/nodes")
        assert r.status_code == 401


# ====================================================================== #
# GET /api/v1/edges
# ====================================================================== #


@pytest.mark.anyio
class TestEdges:

    @pytest.fixture(autouse=True)
    async def load_snapshot(self, client):
        pytest.importorskip("msgpack")
        await _post_snapshot(client, _make_snapshot_bytes())

    async def test_edges_returns_all_edges(self, client):
        r = await client.get("/api/v1/edges", headers=AUTH)
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        edges = body["data"]["data"]
        assert len(edges) == 1
        edge = edges[0]
        assert edge["source"] == "django::view"
        assert edge["target"] == "postgres::SELECT"

    async def test_edges_contains_expected_fields(self, client):
        r = await client.get("/api/v1/edges", headers=AUTH)
        edge = r.json()["data"]["data"][0]
        for field in (
            "source",
            "target",
            "type",
            "call_count",
            "weight",
        ):
            assert field in edge, f"Missing field: {field}"

    async def test_edges_weight_equals_call_count(self, client):
        r = await client.get("/api/v1/edges", headers=AUTH)
        edge = r.json()["data"]["data"][0]
        assert edge["weight"] == edge["call_count"]

    async def test_edges_before_snapshot_returns_404(
        self, client
    ):
        import backend.main as m

        m._graphs.clear()
        r = await client.get("/api/v1/edges", headers=AUTH)
        assert r.status_code == 404

    async def test_edges_requires_auth(self, client):
        r = await client.get("/api/v1/edges")
        assert r.status_code == 401


# ====================================================================== #
# GET /api/v1/traces/{trace_id}
# ====================================================================== #


@pytest.mark.anyio
class TestTraces:

    def _event(
        self,
        trace_id: str,
        probe: str,
        wall_time: float,
        duration_ns: int = 5_000_000,
    ) -> dict:
        return {
            "probe": probe,
            "service": "django",
            "name": f"/{probe}",
            "trace_id": trace_id,
            "span_id": f"span-{probe}",
            "wall_time": wall_time,
            "timestamp": wall_time,
            "duration_ns": duration_ns,
            "metadata": {},
        }

    async def test_trace_returns_critical_path(self, client):
        trace_id = "trace-abc-123"
        events = [
            self._event(
                trace_id, "request.entry", 1.0, 10_000_000
            ),
            self._event(trace_id, "db.query", 1.005, 4_000_000),
        ]
        await client.post(
            "/api/v1/events",
            json={"events": events},
            headers=AUTH,
        )
        r = await client.get(
            f"/api/v1/traces/{trace_id}", headers=AUTH
        )
        assert r.status_code == 200
        body = r.json()
        assert body["trace_id"] == trace_id
        assert body["stages"] == 2
        assert body["total_ms"] > 0

    async def test_trace_path_is_sorted_by_wall_time(
        self, client
    ):
        trace_id = "trace-sort-001"
        events = [
            self._event(trace_id, "db.query", 2.0),
            self._event(trace_id, "request.entry", 1.0),
        ]
        await client.post(
            "/api/v1/events",
            json={"events": events},
            headers=AUTH,
        )
        r = await client.get(
            f"/api/v1/traces/{trace_id}", headers=AUTH
        )
        path = r.json()["path"]
        wall_times = [s["wall_time"] for s in path]
        assert wall_times == sorted(wall_times)

    async def test_trace_path_contains_expected_fields(
        self, client
    ):
        trace_id = "trace-fields-001"
        await client.post(
            "/api/v1/events",
            json={
                "events": [self._event(trace_id, "view", 1.0)]
            },
            headers=AUTH,
        )
        r = await client.get(
            f"/api/v1/traces/{trace_id}", headers=AUTH
        )
        stage = r.json()["path"][0]
        for field in (
            "probe",
            "service",
            "name",
            "wall_time",
            "duration_ms",
            "span_id",
        ):
            assert field in stage, f"Missing field: {field}"

    async def test_trace_not_found_returns_404(self, client):
        r = await client.get(
            "/api/v1/traces/nonexistent-trace-id", headers=AUTH
        )
        assert r.status_code == 404

    async def test_trace_requires_auth(self, client):
        r = await client.get("/api/v1/traces/some-trace-id")
        assert r.status_code == 401

    async def test_trace_total_ms_sums_durations(self, client):
        trace_id = "trace-sum-001"
        # 10ms + 4ms = 14ms total
        events = [
            self._event(trace_id, "entry", 1.0, 10_000_000),
            self._event(trace_id, "db", 1.01, 4_000_000),
        ]
        await client.post(
            "/api/v1/events",
            json={"events": events},
            headers=AUTH,
        )
        r = await client.get(
            f"/api/v1/traces/{trace_id}", headers=AUTH
        )
        assert abs(r.json()["total_ms"] - 14.0) < 0.01


# ====================================================================== #
# GET /api/v1/diff
# ====================================================================== #


@pytest.mark.anyio
class TestDiff:

    @pytest.fixture(autouse=True)
    async def load_snapshot(self, client):
        pytest.importorskip("msgpack")
        await _post_snapshot(client, _make_snapshot_bytes())

    async def test_diff_returns_200_with_snapshot(self, client):
        r = await client.get("/api/v1/diff", headers=AUTH)
        assert r.status_code == 200

    async def test_diff_with_since_param(self, client):
        r = await client.get(
            "/api/v1/diff?since=v1.0.0", headers=AUTH
        )
        assert r.status_code == 200

    async def test_diff_before_snapshot_returns_404(
        self, client
    ):
        import backend.main as m

        m._graphs.clear()
        r = await client.get("/api/v1/diff", headers=AUTH)
        assert r.status_code == 404

    async def test_diff_requires_auth(self, client):
        r = await client.get("/api/v1/diff")
        assert r.status_code == 401


# ====================================================================== #
# POST /api/v1/graph/diff
# ====================================================================== #


@pytest.mark.anyio
class TestGraphDiff:

    async def test_post_graph_diff_returns_ok(self, client):
        payload = {
            "added_nodes": ["svc::fn"],
            "removed_nodes": [],
            "added_edges": [],
            "removed_edges": [],
        }
        r = await client.post(
            "/api/v1/graph/diff", json=payload, headers=AUTH
        )
        assert r.status_code == 200
        assert r.json()["ok"] is True

    async def test_post_graph_diff_stores_in_repository(
        self, client
    ):
        import backend.main as m

        payload = {
            "added_nodes": ["new::node"],
            "removed_nodes": [],
        }
        await client.post(
            "/api/v1/graph/diff", json=payload, headers=AUTH
        )
        # Verify repository received the diff via internal _diffs deque
        assert len(m._repository._diffs["test_customer"]) >= 1

    async def test_post_graph_diff_requires_auth(self, client):
        r = await client.post("/api/v1/graph/diff", json={})
        assert r.status_code == 401


# ====================================================================== #
# GET /api/v1/causal — tag filtering
# ====================================================================== #


@pytest.mark.anyio
class TestCausalTags:

    @pytest.fixture(autouse=True)
    async def load_snapshot(self, client):
        pytest.importorskip("msgpack")
        await _post_snapshot(client, _make_snapshot_bytes())

    async def test_causal_with_tags_param(self, client):
        r = await client.get(
            "/api/v1/causal?tags=latency,db", headers=AUTH
        )
        assert r.status_code == 200
        body = r.json()
        assert "matches" in body
        assert "match_count" in body

    async def test_causal_match_count_matches_list_length(
        self, client
    ):
        r = await client.get("/api/v1/causal", headers=AUTH)
        body = r.json()
        assert body["match_count"] == len(body["matches"])

    async def test_causal_before_snapshot_returns_404(
        self, client
    ):
        import backend.main as m

        m._graphs.clear()
        r = await client.get("/api/v1/causal", headers=AUTH)
        assert r.status_code == 404


# ====================================================================== #
# GET /api/v1/hotspots — edge cases
# ====================================================================== #


@pytest.mark.anyio
class TestHotspotsEdgeCases:

    async def test_hotspots_before_snapshot_returns_404(
        self, client
    ):
        r = await client.get("/api/v1/hotspots", headers=AUTH)
        assert r.status_code == 404

    async def test_hotspots_top_param_limits_results(
        self, client
    ):
        pytest.importorskip("msgpack")
        await _post_snapshot(
            client, _make_snapshot_bytes(node_count=3)
        )
        r = await client.get(
            "/api/v1/hotspots?top=1", headers=AUTH
        )
        assert r.status_code == 200
        assert len(r.json()["data"]) <= 1

    async def test_hotspots_requires_auth(self, client):
        r = await client.get("/api/v1/hotspots")
        assert r.status_code == 401


# ====================================================================== #
# POST /api/v1/query — error paths
# ====================================================================== #


@pytest.mark.anyio
class TestQueryErrorPaths:

    @pytest.fixture(autouse=True)
    async def load_snapshot(self, client):
        pytest.importorskip("msgpack")
        await _post_snapshot(client, _make_snapshot_bytes())

    async def test_invalid_dsl_returns_400(self, client):
        r = await client.post(
            "/api/v1/query",
            json={"query": "INVALID GARBAGE !!!"},
            headers=AUTH,
        )
        assert r.status_code == 400

    async def test_query_before_snapshot_returns_404(
        self, client
    ):
        import backend.main as m

        m._graphs.clear()
        r = await client.post(
            "/api/v1/query",
            json={
                "query": 'SHOW latency WHERE service = "django"'
            },
            headers=AUTH,
        )
        assert r.status_code == 404

    async def test_query_requires_auth(self, client):
        r = await client.post(
            "/api/v1/query",
            json={
                "query": 'SHOW latency WHERE service = "django"'
            },
        )
        assert r.status_code == 401

"""GAP-7 (#33) S2 — agent export/import 路由端到端 (真 ASGI, 非 mock)。

覆盖:
- POST /api/kv/export: 源节点产含张量 bundle
- POST /api/kv/import: 目标节点存入, 张量字节完整 round-trip
- 未鉴权 401 (Bearer 缺)
- export 不存在 cache_id → 404
- 预算超限 → stored=0 (skip)
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVCacheEntry, KVShard
from fusion_multi_node.distributed_mlx.kv_tensor_transport import SyntheticKVTransport
from fusion_multi_node.server.agent_server import AgentServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}
AGENT_PORT_A = 23457
AGENT_PORT_B = 23458


class PortRoutingTransport(AsyncBaseTransport):
    def __init__(self, port_to_app: dict[int, Any]):
        self._port_to_app = port_to_app
        self._clients: dict[int, AsyncClient] = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
            for p, app in port_to_app.items()
        }

    async def handle_async_request(self, request: Request) -> Response:
        port = request.url.port
        client = self._clients.get(port)
        if client is None:
            return Response(404, text=f"no agent for port {port}")
        return await client.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=dict(request.headers),
        )

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_entry_with_tensor(cache_id: str = "kv-export") -> KVCacheEntry:
    import time

    return KVCacheEntry(
        cache_id=cache_id,
        model_name="llama-1b",
        prompt_hash="hash-export",
        prompt_prefix="Hello",
        total_tokens=32,
        total_size_bytes=512,
        created_at=time.time(),
        ttl_seconds=3600.0,
        shards=[
            KVShard(
                shard_id="s0",
                model_name="llama-1b",
                layer_index=0,
                node_id="agent-a",
                token_count=32,
                size_bytes=512,
                created_at=time.time(),
                tensor=None,
                is_compressed=False,
            )
        ],
    )


def _make_agent_server(node_id: str, port: int, max_mb: float = 4096.0) -> AgentServer:
    agent = NodeAgent(
        config=AgentConfig(node_id=node_id, cluster_token=TEST_TOKEN, agent_port=port),
    )
    from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager

    kv = KVSharingManager(
        cluster_token=TEST_TOKEN,
        max_local_cache_mb=max_mb,
        transport=SyntheticKVTransport(tensor_size=256),
    )
    server = AgentServer(agent=agent, kv_manager=kv, shared_token=TEST_TOKEN)
    server._rate_limiter._max = 100000
    server._rate_limiter._window = 1.0
    server._host = "127.0.0.1"
    return server


@pytest.fixture
async def export_import_cluster():
    server_a = _make_agent_server("agent-a", AGENT_PORT_A)
    server_b = _make_agent_server("agent-b", AGENT_PORT_B)
    # 预存源缓存 (无张量 — export_bundle 经合成后端产张量)
    server_a.kv_manager.store_local(_make_entry_with_tensor())
    transport = PortRoutingTransport({AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app})
    client = AsyncClient(transport=transport, base_url="http://test")
    yield client, server_a, server_b
    await client.aclose()


class TestKVExportImportRoutes:
    async def test_export_returns_bundle_with_tensor(self, export_import_cluster):
        client, server_a, _ = export_import_cluster
        resp = await client.post(
            f"http://agent-a:{AGENT_PORT_A}/api/kv/export",
            json={"cache_id": "kv-export", "model_name": "llama-1b"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "ok"
        bundle = body["bundle"]
        assert "tensor" in bundle["shards"][0]
        assert bundle["shards"][0]["tensor_compress"] == "caveman"

    async def test_export_missing_cache_404(self, export_import_cluster):
        client, server_a, _ = export_import_cluster
        resp = await client.post(
            f"http://agent-a:{AGENT_PORT_A}/api/kv/export",
            json={"cache_id": "nope", "model_name": "llama-1b"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 404

    async def test_export_unauth_401(self, export_import_cluster):
        client, server_a, _ = export_import_cluster
        resp = await client.post(
            f"http://agent-a:{AGENT_PORT_A}/api/kv/export",
            json={"cache_id": "kv-export", "model_name": "llama-1b"},
        )
        assert resp.status_code == 401

    async def test_import_stores_tensor_round_trip(self, export_import_cluster):
        client, server_a, server_b = export_import_cluster
        # 源导出
        exp = await client.post(
            f"http://agent-a:{AGENT_PORT_A}/api/kv/export",
            json={"cache_id": "kv-export", "model_name": "llama-1b"},
            headers=AUTH_HEADERS,
        )
        bundle = exp.json()["bundle"]
        # 目标导入
        imp = await client.post(
            f"http://agent-b:{AGENT_PORT_B}/api/kv/import",
            json={"bundle": bundle},
            headers=AUTH_HEADERS,
        )
        assert imp.status_code == 200, imp.text
        assert imp.json()["stored"] == 1
        # 目标本地查回 — 张量字节存在
        restored = server_b.kv_manager.lookup_local_by_id("kv-export")
        assert restored is not None
        assert restored.shards[0].tensor is not None
        assert len(restored.shards[0].tensor) == 256

    async def test_import_unauth_401(self, export_import_cluster):
        client, _, server_b = export_import_cluster
        resp = await client.post(
            f"http://agent-b:{AGENT_PORT_B}/api/kv/import",
            json={"bundle": {"cache_id": "x", "shards": []}},
        )
        assert resp.status_code == 401

    async def test_import_budget_reject_oversize(self):
        # 目标预算 0 MB → 任何条目超限 → stored=0 (skip, 不静默存)。源端预算宽松能存。
        server_a = _make_agent_server("agent-a", AGENT_PORT_A, max_mb=4096.0)
        server_a.kv_manager.store_local(_make_entry_with_tensor())
        server_b = _make_agent_server("agent-b", AGENT_PORT_B, max_mb=0.0)
        transport = PortRoutingTransport({AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app})
        client = AsyncClient(transport=transport, base_url="http://test")
        try:
            exp = await client.post(
                f"http://agent-a:{AGENT_PORT_A}/api/kv/export",
                json={"cache_id": "kv-export", "model_name": "llama-1b"},
                headers=AUTH_HEADERS,
            )
            bundle = exp.json()["bundle"]
            imp = await client.post(
                f"http://agent-b:{AGENT_PORT_B}/api/kv/import",
                json={"bundle": bundle},
                headers=AUTH_HEADERS,
            )
            assert imp.status_code == 200
            assert imp.json()["stored"] == 0
        finally:
            await client.aclose()

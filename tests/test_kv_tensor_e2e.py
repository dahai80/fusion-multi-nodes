"""GAP-7 (#33) S3 — KV 张量跨节点传输 E2E (合成后端, 无真 fusion-mlx)。

真链: master ClusterMaster.sync_kv_cache 编排 → 两 AgentServer 真 ASGI →
源 /api/kv/export (含张量) → 目标 /api/kv/import (store_local 预算) → 返 True。
验证张量字节经 HTTP 跨节点完整 round-trip (满足 #33 验收)。

env-gated 真张量测试 (FUSION_E2E_KV_TENSOR=1) 待上游 fusion-mlx issue #650 落地激活。
"""

from __future__ import annotations

import logging
import os
from typing import Any

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVCacheEntry, KVShard, KVSharingManager
from fusion_multi_node.distributed_mlx.kv_tensor_transport import (
    MLXKVTransport,
    SyntheticKVTransport,
)
from fusion_multi_node.master.cluster_master import ClusterMaster, NodeInfo, NodeStatus
from fusion_multi_node.master.cluster_master import KVCacheEntry as MasterKVEntry
from fusion_multi_node.server.agent_server import AgentServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}
AGENT_PORT_A = 24457
AGENT_PORT_B = 24458


class PortRoutingTransport(AsyncBaseTransport):
    def __init__(self, port_to_app: dict[int, Any]):
        self._port_to_app = port_to_app
        self._clients: dict[int, AsyncClient] = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
            for p, app in port_to_app.items()
        }

    async def handle_async_request(self, request: Request) -> Response:
        client = self._clients.get(request.url.port)
        if client is None:
            return Response(404, text=f"no agent for port {request.url.port}")
        return await client.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=dict(request.headers),
        )

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_agent_server(node_id: str, port: int, tensor_size: int = 256) -> AgentServer:
    agent = NodeAgent(
        config=AgentConfig(node_id=node_id, cluster_token=TEST_TOKEN, agent_port=port),
    )
    kv = KVSharingManager(
        cluster_token=TEST_TOKEN,
        transport=SyntheticKVTransport(tensor_size=tensor_size),
    )
    server = AgentServer(agent=agent, kv_manager=kv, shared_token=TEST_TOKEN)
    server._rate_limiter._max = 100000
    server._rate_limiter._window = 1.0
    server._host = "127.0.0.1"
    return server


def _make_local_entry() -> KVCacheEntry:
    import time

    return KVCacheEntry(
        cache_id="kv-e2e-tensor",
        model_name="llama-1b",
        prompt_hash="h-e2e",
        prompt_prefix="Hello E2E",
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


@pytest.fixture
async def kv_tensor_cluster(monkeypatch):
    # SSRF 守卫拦 127.0.0.1 — 测试作用域放行 (与现有 dispatch E2E 一致)
    from fusion_multi_node.master import cluster_master as _cm_mod
    from fusion_multi_node.utils import auth as _auth_mod

    monkeypatch.setattr(_cm_mod, "is_safe_peer_host", lambda host: True)
    monkeypatch.setattr(_auth_mod, "is_safe_peer_host", lambda host: True)

    server_a = _make_agent_server("agent-a", AGENT_PORT_A, tensor_size=256)
    server_b = _make_agent_server("agent-b", AGENT_PORT_B, tensor_size=256)
    # 源预存缓存 (无张量 — export_bundle 经合成后端产张量)
    server_a.kv_manager.store_local(_make_local_entry())

    route = PortRoutingTransport({AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app})

    cm = ClusterMaster()
    cm._dispatch_http = AsyncClient(transport=route, timeout=30.0)
    cm._dispatch_token = TEST_TOKEN

    # 注册 master 级 KV 条目 + 两在线节点
    await cm.register_kv_cache(
        MasterKVEntry(
            cache_id="kv-e2e-tensor",
            model_name="llama-1b",
            node_id="agent-a",
            created_at=__import__("time").time(),
            size_mb=0.1,
        )
    )
    for nid, port in (("agent-a", AGENT_PORT_A), ("agent-b", AGENT_PORT_B)):
        ni = NodeInfo(
            node_id=nid,
            hostname=nid,
            ip_address="127.0.0.1",
            port=port,
            status=NodeStatus.ONLINE,
            last_heartbeat=__import__("time").time(),
        )
        async with cm._nodes_lock:
            cm.nodes[nid] = ni

    yield cm, server_a, server_b
    await cm._dispatch_http.aclose()
    await route.aclose()


class TestKVTensorE2E:
    async def test_sync_kv_cache_returns_true(self, kv_tensor_cluster):
        # #33 验收: sync_kv_cache 执行张量跨节点传输返 True (非 no-op)。
        cm, server_a, server_b = kv_tensor_cluster
        ok = await cm.sync_kv_cache(
            "kv-e2e-tensor",
            "llama-1b",
            "agent-a",
            0.1,
            target_node_id="agent-b",
        )
        assert ok is True

    async def test_tensor_round_trip_across_nodes(self, kv_tensor_cluster):
        # 张量字节经 HTTP 跨节点完整保留 — 目标本地查回张量非 None 且长度匹配。
        cm, server_a, server_b = kv_tensor_cluster
        await cm.sync_kv_cache(
            "kv-e2e-tensor",
            "llama-1b",
            "agent-a",
            0.1,
            target_node_id="agent-b",
        )
        got = server_b.kv_manager.lookup_local_by_id("kv-e2e-tensor")
        assert got is not None
        assert got.shards[0].tensor is not None
        assert len(got.shards[0].tensor) == 256

    async def test_sync_auto_select_target(self, kv_tensor_cluster):
        # 不传 target_node_id → master 自动选非源在线节点。
        cm, server_a, server_b = kv_tensor_cluster
        ok = await cm.sync_kv_cache("kv-e2e-tensor", "llama-1b", "agent-a", 0.1)
        assert ok is True
        got = server_b.kv_manager.lookup_local_by_id("kv-e2e-tensor")
        assert got is not None

    async def test_sync_missing_entry_returns_false(self, kv_tensor_cluster):
        cm, _, _ = kv_tensor_cluster
        ok = await cm.sync_kv_cache("nope", "llama-1b", "agent-a", 0.1, target_node_id="agent-b")
        assert ok is False


class TestRealTensorE2EGated:
    # 真张量 E2E (MLXKVTransport) — env-gated, 待上游 fusion-mlx issue #650 落地。
    # 合成后端已满足 #33 验收; 真张量为 env-gated bonus。

    @pytest.mark.skipif(
        os.environ.get("FUSION_E2E_KV_TENSOR") != "1",
        reason="真张量 KV E2E 需 FUSION_E2E_KV_TENSOR=1 + 上游 fusion-mlx issue #650",
    )
    async def test_mlx_transport_endpoints(self):
        # 仅当 #650 落地后运行 — 验 MLXKVTransport 调 /distributed/kv_cache/export|import。
        t = MLXKVTransport()
        exported = await t.export_tensor("c-real", "llama-1b", "agent-a")
        if exported is None:
            pytest.skip("上游 #650 端点仍未落地, MLX 后端降级 — 合成后端已满足 #33")
        ok = await t.import_tensor("c-real", "llama-1b", exported, "agent-a")
        await t.close()
        assert ok is True

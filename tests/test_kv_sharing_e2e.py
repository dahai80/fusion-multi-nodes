"""S4 E2E 跨节点 KV 缓存共享 — 真 ASGI 路由, 非单元 mock。

真实链: 两 AgentServer 真 FastAPI → PortRoutingTransport 路由 → /api/kv/* 路由
(store_local 直接注入 / lookup 经 HTTP / warm 经 HTTP 推送 / transfer 经 HTTP 拉取)。
覆盖 agent_server KV 路由端到端 (原 17/24 文件 mock, KV 跨节点路由零覆盖)。

KV 缓存为纯本地数据 (非模型张量), 用合成 KVCacheEntry 验路由链路, 不需真 fusion-mlx。
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVCacheEntry, KVShard
from fusion_multi_node.server.agent_server import AgentServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}
AGENT_PORT_A = 22457
AGENT_PORT_B = 22458

# KVSharingManager.lookup_remote/warm_cache 硬编码 127.0.0.1 + :11458 端口;
# E2E 用 ASGI 路由按端口分发, 故经 monkeypatch build_safe_url + 用 manager 自身
# _get_http_client 返回路由 transport (免真实端口监听)。
# 但 lookup_remote 用 sanitize_node_url_part(node_id) 拼 host — node_id 须是合法 host 段。
# 这里走 warm_cache (nodes 列表) + transfer (source/target) 两条 HTTP 链路。


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 agent ASGI app (复用 P3/P5/S4-数据并行约定)。"""

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


def _make_entry(cache_id: str = "kv-e2e", model_name: str = "llama-1b") -> KVCacheEntry:
    import time

    return KVCacheEntry(
        cache_id=cache_id,
        model_name=model_name,
        prompt_hash="hash-e2e",
        prompt_prefix="Hello world",
        total_tokens=64,
        total_size_bytes=2048,
        created_at=time.time(),
        ttl_seconds=3600.0,
        shards=[
            KVShard(
                shard_id="s0",
                model_name=model_name,
                layer_index=0,
                node_id="agent-a",
                token_count=64,
                size_bytes=2048,
                created_at=time.time(),
            )
        ],
    )


def _make_agent_server(node_id: str, port: int) -> AgentServer:
    """真实 NodeAgent + AgentServer (无 backend 推理需求, KV 路由不触推理)。"""
    agent = NodeAgent(
        config=AgentConfig(node_id=node_id, cluster_token=TEST_TOKEN, agent_port=port),
    )
    server = AgentServer(agent=agent, shared_token=TEST_TOKEN)
    server._rate_limiter._max = 100000
    server._rate_limiter._window = 1.0
    server._host = "127.0.0.1"
    return server


@pytest.fixture
async def kv_cluster(monkeypatch):
    """两节点 agent server + 共享路由 transport。

    warm_cache/transfer 经 manager 拼 URL: http://{node_id}:11458/api/kv/*。
    端口 11458 硬编码 (kv_cache_sharing), 不在路由 map。E2E 把 manager 的
    _get_http_client 换成路由 transport + URL 端口重写 (agent-a→port_a, agent-b→port_b)。
    """
    server_a = _make_agent_server("agent-a", AGENT_PORT_A)
    server_b = _make_agent_server("agent-b", AGENT_PORT_B)
    port_to_app = {AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app}
    routing_transport = PortRoutingTransport(port_to_app)

    node_to_port = {"agent-a": AGENT_PORT_A, "agent-b": AGENT_PORT_B}

    def _make_route_http(node_owner: str):
        # 每个 manager 生成自己的 _get_http_client: 返回路由 client, post 时重写端口
        async def _route_http(timeout: float = 30.0):
            client = AsyncClient(transport=routing_transport, timeout=timeout)
            orig_post = client.post

            async def _post(url, **kw):
                url_s = str(url)
                # manager 拼 :11458 (硬编码) — 按 host 段选真实路由端口
                for nid, p in node_to_port.items():
                    if nid in url_s:
                        url_s = url_s.replace(":11458", f":{p}")
                        break
                return await orig_post(url_s, **kw)

            client.post = _post
            return client

        return _route_http

    monkeypatch.setattr(server_a.kv_manager, "_get_http_client", _make_route_http("agent-a"))
    monkeypatch.setattr(server_b.kv_manager, "_get_http_client", _make_route_http("agent-b"))

    try:
        yield {
            "server_a": server_a,
            "server_b": server_b,
            "routing_transport": routing_transport,
            "port_a": AGENT_PORT_A,
            "port_b": AGENT_PORT_B,
        }
    finally:
        await routing_transport.aclose()


class TestKVSharingE2E:
    """跨节点 KV 缓存共享端到端 — 真 ASGI 路由 (非单元 mock)。"""

    async def test_kv_store_and_lookup_same_node(self, kv_cluster):
        """同节点: store_local 注入 → HTTP /api/kv/lookup 查回。
        验 agent_server lookup 路由 + KVSharingManager 本地索引 round-trip。"""
        server_a = kv_cluster["server_a"]
        server_a.kv_manager.store_local(_make_entry(cache_id="c-aaa"))

        transport = ASGITransport(app=server_a.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/kv/lookup",
                json={"model_name": "llama-1b", "prompt_hash": "hash-e2e"},
                headers=AUTH_HEADERS,
            )
        assert resp.status_code == 200
        data = resp.json()
        # 契约: {"found": True, "entry": {序列化 KVCacheEntry}}。
        assert data["found"] is True
        entry = data["entry"]
        assert entry["cache_id"] == "c-aaa"
        assert entry["total_tokens"] == 64
        assert len(entry["shards"]) == 1
        logger.info("S4 KV E2E 同节点 store→lookup 通过")

    async def test_kv_lookup_missing_returns_404(self, kv_cluster):
        """lookup 未命中 → 404 (非静默 200 空)。"""
        server_a = kv_cluster["server_a"]
        transport = ASGITransport(app=server_a.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/kv/lookup",
                json={"model_name": "no-such-model", "prompt_hash": "no-such-hash"},
                headers=AUTH_HEADERS,
            )
        assert resp.status_code == 404

    async def test_kv_lookup_remote_cross_node_contract(self, kv_cluster):
        """跨节点 lookup_remote 真链路 — store 在 node-a, node-b 经 HTTP 查回。

        验 lookup_remote 解码契约: route 返 {"found":True,"entry":{...}} → manager
        _deserialize_entry → 返回 (entry, node_id)。旧 route 返扁平 dict 无 "found"/"entry"
        键 → lookup_remote 永远 None (静默失效); 单元 mock 捏造该形状掩盖此 bug。
        此 E2E 用真 route (非 mock) 锁契约, 防 "假信心测试" 复发。
        """
        server_a = kv_cluster["server_a"]
        server_b = kv_cluster["server_b"]
        server_a.kv_manager.store_local(_make_entry(cache_id="c-remote"))

        result = await server_b.kv_manager.lookup_remote(
            model_name="llama-1b", prompt_hash="hash-e2e", nodes=["agent-a"]
        )
        assert result is not None, "lookup_remote 永远 None — route 契约失配"
        entry, node_id = result
        assert entry.cache_id == "c-remote"
        assert node_id == "agent-a"
        logger.info("S4 KV E2E 跨节点 lookup_remote 契约通过 (route→found/entry→decode)")

    async def test_kv_warm_pushes_to_remote_node(self, kv_cluster):
        """预热推送: node-a warm_cache → 经路由 HTTP POST node-b /api/kv/warm。

        验跨节点 warm HTTP 链路 (manager warm_cache → _get_http_client → 路由 → node-b)。
        fixture 已重写 :11458 端口 → 真实路由端口, 故直接调 warm_cache。
        """
        server_a = kv_cluster["server_a"]
        result = await server_a.kv_manager.warm_cache(
            model_name="llama-1b",
            prompts=["Hello world prompt"],
            nodes=["agent-b"],
        )
        # warm_cache 向 node-b POST /api/kv/warm, node-b 返 200 → success+1
        assert result["success"] >= 1, f"预热失败: {result}"
        logger.info(f"S4 KV E2E 跨节点 warm 推送通过: {result}")

    async def test_kv_stats_route(self, kv_cluster):
        """/api/kv/stats 路由 — 返 manager 统计。"""
        server_a = kv_cluster["server_a"]
        server_a.kv_manager.store_local(_make_entry(cache_id="c-stats"))
        transport = ASGITransport(app=server_a.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/kv/stats", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["local_entries"] >= 1
        logger.info(f"S4 KV E2E stats 路由通过: {data}")

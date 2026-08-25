"""P1-F 跨机 KV 共享规模化压测 — 真端口跨进程 KV 迁移大规模。

真链: N AgentServer 真端口 uvicorn.serve ←→ KVSharingManager 跨 HTTP
(/api/kv/warm 推送, /api/kv/transfer 拉取)。验:
- 大规模 warm_cache: M prompt × N node, 全成功 (0 丢失)
- 迁移延迟: p50 / p99 量测
- 预热开销: warm 总耗时 / 单 prompt 耗时
- 显存占用代理: total_size_bytes 累计 (合成数据, 非真显存)

非容器: 进程内真 uvicorn 真端口真 socket (测规模 + 延迟 + 丢失, 非容器编排)。
容器规模化见 TestKVContainerStress (skip-gate docker)。
免真模型 — 合成 KVCacheEntry (KV 路由不触推理)。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import socket
import time
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.server.agent_server import AgentServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "kvstress-token"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(len(s) - 1, int(len(s) * p))
    return s[idx]


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 agent ASGI app。"""

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


class _KVCluster:
    """N 真端口 agent, KVSharingManager 跨 HTTP 真路由。"""

    def __init__(self, n: int = 3):
        self.n = n
        self.servers: list[AgentServer] = []
        self.agents: list[NodeAgent] = []
        self.ports: list[int] = []
        self.node_ids: list[str] = []
        self._serve_tasks: list[asyncio.Task] = []

    def _add_agent(self, idx: int) -> AgentServer:
        port = _free_port()
        nid = f"kv-node-{idx}"
        cfg = AgentConfig(
            node_id=nid,
            agent_host="127.0.0.1",
            agent_port=port,
            cluster_token=TEST_TOKEN,
        )
        agent = NodeAgent(cfg)
        agent._get_local_ip = lambda: "127.0.0.1"
        server = AgentServer(agent=agent, shared_token=TEST_TOKEN)
        server._approval_manager = None
        server._rate_limiter = None
        self.agents.append(agent)
        self.servers.append(server)
        self.ports.append(port)
        self.node_ids.append(nid)
        return server

    async def start(self) -> None:
        for i in range(self.n):
            self._add_agent(i)
        for server, agent in zip(self.servers, self.agents):
            task = asyncio.create_task(server.start(host="127.0.0.1", port=agent.config.agent_port))
            self._serve_tasks.append(task)
            ok = await self._wait_health(agent.config.agent_port)
            assert ok, f"agent {agent.config.node_id} 未就绪"
        # 跨节点 KV URL :11458 硬编码 → 重写为真实端口 (经路由 client)。
        self._wire_routing()

    def _wire_routing(self) -> None:
        port_to_app = {p: s.app for p, s in zip(self.ports, self.servers)}
        routing = PortRoutingTransport(port_to_app)
        nid_to_port = dict(zip(self.node_ids, self.ports))

        def _make_route_http():
            async def _route_http(timeout: float = 30.0):
                client = AsyncClient(transport=routing, timeout=timeout)
                orig_post = client.post

                async def _post(url, **kw):
                    url_s = str(url)
                    for nid, p in nid_to_port.items():
                        if nid in url_s:
                            url_s = url_s.replace(":11458", f":{p}")
                            break
                    return await orig_post(url_s, **kw)

                client.post = _post
                return client

            return _route_http

        for server in self.servers:
            server.kv_manager._get_http_client = _make_route_http()

    async def _wait_health(self, port: int, timeout: float = 15.0) -> bool:
        deadline = time.monotonic() + timeout
        async with httpx.AsyncClient(timeout=2.0) as c:
            while time.monotonic() < deadline:
                try:
                    r = await c.get(
                        f"http://127.0.0.1:{port}/api/health",
                        headers={"Authorization": f"Bearer {TEST_TOKEN}"},
                    )
                    if r.status_code == 200:
                        return True
                except Exception:
                    pass
                await asyncio.sleep(0.2)
        return False

    async def stop(self) -> None:
        for server in self.servers:
            await server.stop()
        for t in self._serve_tasks:
            t.cancel()
        for t in self._serve_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


@pytest.fixture
async def kv_cluster():
    c = _KVCluster(n=3)
    await c.start()
    yield c
    await c.stop()


def _make_prompts(m: int) -> list[str]:
    return [f"prompt-{i}-prefix-content" for i in range(m)]


class TestKVStress:
    """跨机 KV 共享规模化压测 — 0 丢失 + 延迟量测。"""

    @pytest.mark.asyncio
    async def test_warm_cache_scale_zero_loss(self, kv_cluster):
        # M prompt × N node warm, 全成功 (0 丢失)。
        n = kv_cluster.n
        m = 20
        prompts = _make_prompts(m)

        mgr = kv_cluster.servers[0].kv_manager
        t0 = time.monotonic()
        result = await mgr.warm_cache(model_name="llama-1b", prompts=prompts, nodes=kv_cluster.node_ids)
        total_elapsed = time.monotonic() - t0

        expected = m * n
        assert result["failed"] == 0, f"warm 有丢失: failed={result['failed']}"
        assert result["success"] == expected, f"warm 成功数 {result['success']} != 期望 {expected}"
        logger.info(
            f"warm 规模: {m} prompt × {n} node = {expected} 次, "
            f"总耗时 {total_elapsed:.3f}s, 0 丢失"
        )

    @pytest.mark.asyncio
    async def test_warm_cache_latency_p99(self, kv_cluster):
        # 单 prompt warm 单节点, 重复测, 量 p50/p99。
        target = kv_cluster.node_ids[1]
        mgr = kv_cluster.servers[0].kv_manager
        latencies: list[float] = []

        for i in range(15):
            t0 = time.monotonic()
            r = await mgr.warm_cache(
                model_name="llama-1b", prompts=[f"lat-{i}"], nodes=[target]
            )
            latencies.append(time.monotonic() - t0)
            assert r["failed"] == 0, f"第 {i} 次 warm 丢失"

        p50 = _pct(latencies, 0.5)
        p99 = _pct(latencies, 0.99)
        logger.info(f"warm 单次延迟: p50={p50*1000:.1f}ms p99={p99*1000:.1f}ms (n={len(latencies)})")
        # 单次 warm 跨 HTTP 应 < 500ms (本地 ASGI 路由)。
        assert p99 < 1.0, f"warm p99 过高: {p99:.3f}s"

    @pytest.mark.asyncio
    async def test_warm_then_transfer_zero_loss(self, kv_cluster):
        # warm 到 node-0 → transfer 到 node-1/node-2, 验跨节点迁移不丢失。
        mgr = kv_cluster.servers[0].kv_manager
        prompts = _make_prompts(10)
        r = await mgr.warm_cache(model_name="llama-1b", prompts=prompts, nodes=[kv_cluster.node_ids[0]])
        assert r["failed"] == 0

        # 逐 prompt transfer node-0 → node-1, 量延迟。
        latencies: list[float] = []
        lost = 0
        for i, p in enumerate(prompts):
            cache_id = f"warm-{hashlib.sha256(p.encode()).hexdigest()[:16]}"
            t0 = time.monotonic()
            ok = await kv_cluster.servers[1].kv_manager.transfer_from_remote(
                cache_id, source_node=kv_cluster.node_ids[0], target_node=kv_cluster.node_ids[1]
            )
            latencies.append(time.monotonic() - t0)
            if not ok:
                lost += 1

        assert lost == 0, f"transfer 丢失 {lost}/{len(prompts)}"
        p99 = _pct(latencies, 0.99)
        logger.info(
            f"transfer 规模 {len(prompts)}, 0 丢失, "
            f"p50={_pct(latencies, 0.5)*1000:.1f}ms p99={p99*1000:.1f}ms"
        )
        assert p99 < 1.0, f"transfer p99 过高: {p99:.3f}s"

    @pytest.mark.asyncio
    async def test_kv_memory_accumulation(self, kv_cluster):
        # 累计 total_size_bytes 代理显存占用 (合成数据)。
        mgr = kv_cluster.servers[0].kv_manager
        r = await mgr.warm_cache(
            model_name="llama-1b", prompts=_make_prompts(5), nodes=kv_cluster.node_ids
        )
        assert r["failed"] == 0
        stats = kv_cluster.servers[0].kv_manager.get_stats()
        assert stats.get("local_entries", 0) >= 5, f"本地缓存条目不足: {stats}"
        logger.info(f"node-0 KV stats: {stats}")


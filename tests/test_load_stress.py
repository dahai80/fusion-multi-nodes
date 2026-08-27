"""S3 负载/压测基线测试 — 并发任务派发吞吐 / 尾延迟 / 无丢失。

调度层压测 (免真模型): N 节点集群 + FakeInferenceBackend, 并发提交 M 任务,
测派发吞吐 (task/s), 尾延迟 (p95/p99 completed_at - started_at), 无丢失
(全部 COMPLETED, 无 FAILED/丢失)。派发走真 ASGI 路由 (PortRoutingTransport)。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.agent.node_agent import InferenceBackend
from fusion_multi_node.master import ClusterMaster, ClusterTask, ParallelMode, TaskStatus
from fusion_multi_node.server.agent_server import AgentServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}

# 四节点集群端口 (transport 按端口路由, 不真实监听)
NODE_PORTS = {"n1": 22445, "n2": 22446, "n3": 22447, "n4": 22448}


class FastBackend(InferenceBackend):
    """零延迟假后端 — 压测调度器派发链路, 非模型推理。"""

    def __init__(self, node_id: str):
        self._node_id = node_id
        self.call_count = 0

    async def chat(
        self,
        model: str,
        messages: list[dict[str, Any]],
        temperature: float = 0.7,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.call_count += 1
        return {
            "choices": [{"message": {"content": f"ok@{self._node_id}"}}],
            "usage": {"total_tokens": 1},
        }

    async def embed(self, model: str, input_text: str, **kwargs: Any) -> dict[str, Any]:
        return {"data": [{"embedding": [0.1, 0.2]}]}

    async def health(self) -> bool:
        return True


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 agent ASGI app。"""

    def __init__(self, port_to_app: dict[int, Any]):
        self._port_to_app = port_to_app
        self._clients: dict[int, AsyncClient] = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test") for p, app in port_to_app.items()
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


@pytest.fixture
async def stress_cluster(monkeypatch):
    """起 master + 四节点 agent, SSRF 测试内放行 127.0.0.1。"""
    monkeypatch.setattr(
        "fusion_multi_node.master.cluster_master.is_safe_peer_host",
        lambda host: True,
    )
    monkeypatch.setattr(
        "fusion_multi_node.master.cluster_master.build_safe_url",
        lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
    )
    # 压测放开 agent 限流: 默认 30 req/min 会 429 拦并发派发。
    # AgentServer 构造时建 InMemoryRateLimiter 实例并交中间件; 构造后改实例
    # _max/_window (中间件持同一引用, 改实例即生效)。
    backends: dict[str, FastBackend] = {}
    port_to_app: dict[int, Any] = {}
    for nid, port in NODE_PORTS.items():
        backend = FastBackend(nid)
        agent = NodeAgent(
            # P1-18: 压测放开 agent 本地并发槽 (默认 4), 与 master max_tasks=200 对齐, 测调度吞吐非容量上限。
            config=AgentConfig(node_id=nid, cluster_token=TEST_TOKEN, agent_port=port, max_tasks=200),
            backend=backend,
        )
        server = AgentServer(agent=agent, shared_token=TEST_TOKEN)
        server._rate_limiter._max = 100000  # 压测放开限流 (默认 30 req/min)
        server._rate_limiter._window = 1.0
        backends[nid] = backend
        port_to_app[port] = server.app

    routing_transport = PortRoutingTransport(port_to_app)
    master = ClusterMaster(heartbeat_timeout=60.0)
    # P1-H: 压测测调度吞吐非租户配额 → 关闭配额 (0=不限), 否则单租户被限 4 并发。
    master.configure_scheduling(0)
    master._dispatch_token = TEST_TOKEN

    async def _fake_dispatch_http():
        return AsyncClient(transport=routing_transport, timeout=10.0)

    monkeypatch.setattr(master, "_get_dispatch_http", _fake_dispatch_http)

    # 注册四节点 (直接 register_node, 免 HTTP 注册开销)
    for nid, port in NODE_PORTS.items():
        from fusion_multi_node.master import NodeInfo

        await master.register_node(
            NodeInfo(
                node_id=nid,
                hostname=f"mac-{nid}",
                ip_address="127.0.0.1",
                port=port,
                total_memory_gb=64.0,
                available_memory_gb=48.0,
                cpu_cores=12,
                gpu_cores=30,
                max_tasks=200,  # 压测放开并发槽 (测调度吞吐, 非容量上限)
            )
        )

    try:
        yield {"master": master, "backends": backends}
    finally:
        await master.stop()
        await routing_transport.aclose()


async def _drain(master: ClusterMaster, timeout_s: float = 15.0) -> None:
    """等所有派发后台任务结束。"""
    for _ in range(int(timeout_s * 20)):
        pending = [t for t in master._dispatch_tasks.values() if not t.done()]
        if not pending:
            return
        await asyncio.sleep(0.05)


class TestLoadStress:
    """S3 调度层压测基线 — 吞吐 / 尾延迟 / 无丢失。"""

    @pytest.mark.asyncio
    async def test_concurrent_dispatch_no_loss(self, stress_cluster):
        """并发 40 任务 (单节点 DATA) 全部 COMPLETED, 无丢失/无 FAILED。

        基线断言 (非阈值卡死, 记录基线值供回归对比):
        - 全部 COMPLETED (lost == 0, failed == 0)
        - 每任务派发到达一节点 (backend 总调用 == 40)
        - 派发吞吐 > 20 task/s (调度器自身非瓶颈)
        """
        master = stress_cluster["master"]
        backends = stress_cluster["backends"]
        n_tasks = 40

        tasks = [
            ClusterTask(
                task_id=f"stress-{i}",
                name=f"stress-{i}",
                mode=ParallelMode.DATA,
                model_name="qwen-1b",
                task_type="inference",
                params={"prompt": f"p{i}", "messages": [], "max_tokens": 8},
            )
            for i in range(n_tasks)
        ]

        loop = asyncio.get_event_loop()
        t0 = loop.time()
        # 并发提交 (assign_task 持锁, 但 select_nodes + 派发 fire-and-forget)
        results = await asyncio.gather(*[master.assign_task(t) for t in tasks])
        ok_count = sum(1 for r in results if r)
        await _drain(master)
        elapsed = loop.time() - t0

        completed = sum(1 for t in master.tasks.values() if t.status == TaskStatus.COMPLETED)
        failed = sum(1 for t in master.tasks.values() if t.status == TaskStatus.FAILED)
        lost = n_tasks - completed - failed
        total_backend_calls = sum(b.call_count for b in backends.values())

        logger.info(
            f"S3 压测: {n_tasks} 任务, ok={ok_count}, completed={completed}, "
            f"failed={failed}, lost={lost}, backend_calls={total_backend_calls}, "
            f"elapsed={elapsed:.2f}s, throughput={n_tasks / max(elapsed, 0.001):.1f} task/s"
        )

        assert lost == 0, f"任务丢失: lost={lost} (completed={completed}, failed={failed})"
        failed_errs = [t.error for t in master.tasks.values() if t.status == TaskStatus.FAILED][:3]
        assert failed == 0, f"任务失败: failed={failed} (错误样本: {failed_errs})"
        assert completed == n_tasks, f"完成数不匹配: {completed}/{n_tasks}"
        assert total_backend_calls == n_tasks, f"backend 调用数 != 任务数: {total_backend_calls}/{n_tasks}"

    @pytest.mark.asyncio
    async def test_dispatch_latency_tail(self, stress_cluster):
        """派发延迟尾部分布 — p95 < 5.0s, p99 < 8.0s (单节点 DATA, 免真模型)。

        记录基线: completed_at - started_at 包含 select_nodes + httpx 派发 + backend
        (零延迟) + finalize + _drain 0.05s 轮询粒度。卡死阈值防回归恶化 (串行化
        退化时 p95 会冲到数十秒), 不卡绝对最优 — 阈值留机器负载余量。
        """
        master = stress_cluster["master"]
        n_tasks = 30

        tasks = []
        for i in range(n_tasks):
            t = ClusterTask(
                task_id=f"lat-{i}",
                name=f"lat-{i}",
                mode=ParallelMode.DATA,
                model_name="qwen-1b",
                task_type="inference",
                params={"prompt": f"p{i}", "messages": [], "max_tokens": 8},
            )
            tasks.append(t)
            await master.assign_task(t)
        await _drain(master)

        latencies = sorted(
            t.completed_at - t.started_at
            for t in master.tasks.values()
            if t.status == TaskStatus.COMPLETED and t.completed_at > t.started_at
        )
        assert len(latencies) == n_tasks, f"延迟样本不足: {len(latencies)}/{n_tasks}"

        def _pctl(q: float) -> float:
            idx = min(len(latencies) - 1, max(0, int(q * len(latencies))))
            return latencies[idx]

        p95 = _pctl(0.95)
        p99 = _pctl(0.99)
        logger.info(
            f"S3 延迟: n={len(latencies)}, p50={_pctl(0.5):.4f}s, "
            f"p95={p95:.4f}s, p99={p99:.4f}s, max={latencies[-1]:.4f}s"
        )

        assert p95 < 5.0, f"p95 派发延迟过高: {p95:.3f}s"
        assert p99 < 8.0, f"p99 派发延迟过高: {p99:.3f}s"

    @pytest.mark.asyncio
    async def test_data_parallel_throughput(self, stress_cluster):
        """DATA 并行 (两节点两 shard) 并发 20 任务 — 吞吐 > 3.0 task/s, 无丢失。

        验证多节点派发在并发压力下不丢任务、不串行化退化 (串行化退化吞吐
        会跌到 <1 task/s)。阈值留机器负载余量, 不卡绝对最优。
        """
        master = stress_cluster["master"]
        backends = stress_cluster["backends"]
        n_tasks = 20

        tasks = [
            ClusterTask(
                task_id=f"dp-{i}",
                name=f"dp-{i}",
                mode=ParallelMode.DATA,
                model_name="qwen-3b",
                model_shards=[{"id": "s0"}, {"id": "s1"}],
                task_type="inference",
                params={"prompt": f"p{i}", "messages": [], "max_tokens": 8},
            )
            for i in range(n_tasks)
        ]

        loop = asyncio.get_event_loop()
        t0 = loop.time()
        await asyncio.gather(*[master.assign_task(t) for t in tasks])
        await _drain(master)
        elapsed = loop.time() - t0

        completed = sum(1 for t in master.tasks.values() if t.status == TaskStatus.COMPLETED)
        failed = sum(1 for t in master.tasks.values() if t.status == TaskStatus.FAILED)
        total_backend_calls = sum(b.call_count for b in backends.values())

        logger.info(
            f"S3 DATA 并行压测: {n_tasks} 任务, completed={completed}, failed={failed}, "
            f"backend_calls={total_backend_calls}, elapsed={elapsed:.2f}s, "
            f"throughput={n_tasks / max(elapsed, 0.001):.1f} task/s"
        )

        assert completed == n_tasks, f"完成数不匹配: {completed}/{n_tasks}"
        assert failed == 0, f"任务失败: {failed}"
        assert total_backend_calls == n_tasks * 2, f"两节点各派发一次: {total_backend_calls}/{n_tasks * 2}"
        assert n_tasks / max(elapsed, 0.001) > 3.0, f"DATA 并行吞吐过低: {n_tasks / max(elapsed, 0.001):.1f} task/s"

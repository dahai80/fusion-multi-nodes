"""P1-12 (审计 §6.5): 真推理吞吐基准 — 单节点 vs 多节点 DATA 并行。

skip-gate: 真 fusion-mlx 运行 + 小模型可加载 (同 test_data_parallelism_e2e.py),
不满足则 skip (不假模型, 不 mock)。测单/多节点 DATA 并行吞吐 (task/s), 记基准值
(非断言绝对值 — 受机器负载波动), 断言多节点 > 单节点增益存在 (多节点不退化)。

运行: ~/claude-home/fusion-mlx/start.sh start; 模型默认 mlx-community-Llama-3.2-1B-Instruct-4bit, api_key dahai168。
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.agent.node_agent import FusionMLXBackend
from fusion_multi_node.master import ClusterMaster, ClusterTask, ParallelMode, TaskStatus
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "bench-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}

AGENT_PORT_A = 22561
AGENT_PORT_B = 22562

_MLX_URL = os.environ.get("FUSION_MLX_URL", "http://127.0.0.1:11434")
_MLX_API_KEY = os.environ.get("FUSION_MLX_TEST_API_KEY", "dahai168")
_MODEL_NAME = os.environ.get(
    "FUSION_BENCH_MODEL",
    "mlx-community-Llama-3.2-1B-Instruct-4bit",
)
_BENCH_PROMPTS = 6  # 每场景并发请求数 (小, 受真模型延迟, 控总时长)


def _mlx_alive() -> bool:
    try:
        r = httpx.get(
            f"{_MLX_URL}/v1/models",
            headers={"Authorization": f"Bearer {_MLX_API_KEY}"},
            timeout=3.0,
        )
        return r.status_code == 200
    except Exception:
        return False


def _model_available() -> bool:
    try:
        r = httpx.get(
            f"{_MLX_URL}/v1/models",
            headers={"Authorization": f"Bearer {_MLX_API_KEY}"},
            timeout=3.0,
        )
        if r.status_code != 200:
            return False
        ids = [m.get("id", "") for m in r.json().get("data", [])]
        return _MODEL_NAME in ids
    except Exception:
        return False


_skip_no_backend = pytest.mark.skipif(
    not (_mlx_alive() and _model_available()),
    reason="真 fusion-mlx 未运行 / 小模型不可用 (需 Llama-3.2-1B-Instruct-4bit)",
)


class PortRoutingTransport(AsyncBaseTransport):
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
            request.method, str(request.url), content=request.content, headers=dict(request.headers)
        )

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_real_agent_server(node_id: str, port: int) -> AgentServer:
    backend = FusionMLXBackend(base_url=_MLX_URL, api_key=_MLX_API_KEY)
    agent = NodeAgent(
        config=AgentConfig(
            node_id=node_id,
            cluster_token=TEST_TOKEN,
            agent_port=port,
            fusion_mlx_api_key=_MLX_API_KEY,
            max_tasks=64,
        ),
        backend=backend,
    )
    return AgentServer(agent=agent, shared_token=TEST_TOKEN)


def _bench_task(tid: str) -> ClusterTask:
    return ClusterTask(
        task_id=tid,
        name=tid,
        mode=ParallelMode.DATA,
        model_name=_MODEL_NAME,
        task_type="inference",
        params={
            "prompt": f"bench-{tid}",
            "messages": [{"role": "user", "content": "Say hello in one short sentence."}],
            "max_tokens": 16,
        },
    )


@_skip_no_backend
class TestRealInferenceBenchmark:
    """P1-12: 真模型单/多节点 DATA 并行吞吐基准 (记值, 断言多节点不退化)。"""

    @pytest.fixture
    async def cluster(self, monkeypatch, tmp_path):
        monkeypatch.setattr("fusion_multi_node.master.cluster_master.is_safe_peer_host", lambda host: True)
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.build_safe_url",
            lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
        )
        server_a = _make_real_agent_server("agent-a", AGENT_PORT_A)
        server_b = _make_real_agent_server("agent-b", AGENT_PORT_B)
        port_to_app = {AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app}
        routing = PortRoutingTransport(port_to_app)

        master = ClusterMaster(heartbeat_timeout=60.0)
        master._task_store_path = tmp_path / "tasks.json"
        master_server = MasterServer(master=master, shared_token=TEST_TOKEN)
        master_server._approval_manager = None

        async def _fake_http():
            return AsyncClient(transport=routing, timeout=120.0)

        monkeypatch.setattr(master, "_get_dispatch_http", _fake_http)
        master._dispatch_token = TEST_TOKEN
        master.configure_scheduling(0)

        try:
            yield {
                "master": master,
                "master_app": master_server.app,
                "routing": routing,
            }
        finally:
            await master.stop()
            await routing.aclose()

    async def _drain(self, master: ClusterMaster, timeout_s: float = 180.0) -> None:
        for _ in range(int(timeout_s * 20)):
            pending = [t for t in master._dispatch_tasks.values() if not t.done()]
            if not pending:
                return
            await asyncio.sleep(0.05)

    async def test_single_vs_multi_node_throughput(self, cluster):
        """单节点 vs 双节点 DATA 并行吞吐 — 多节点不退化 (增益 ≥0.9× 单节点)。

        真模型延迟抖动大, 不卡绝对增益比 (理想 >1×), 仅断言多节点吞吐不低于单节点 0.9×
        (多节点不该因调度开销退化), 记录两基准值供回归对比。
        """
        master = cluster["master"]
        transport = ASGITransport(app=cluster["master_app"])
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 单节点场景: 注册 1 节点, 本地强制派发到 agent-a
            await client.post(
                "/api/nodes/register",
                json={
                    "node_id": "agent-a",
                    "hostname": "mac-a",
                    "ip_address": "127.0.0.1",
                    "port": AGENT_PORT_A,
                    "total_memory_gb": 64.0,
                    "available_memory_gb": 48.0,
                    "cpu_cores": 12,
                    "gpu_cores": 30,
                    "max_tasks": 64,
                },
                headers=AUTH_HEADERS,
            )
            single_tasks = [_bench_task(f"s-{i}") for i in range(_BENCH_PROMPTS)]
            for t in single_tasks:
                t.preferred_node_id = "agent-a"
            t0 = time.time()
            await asyncio.gather(*[master.assign_task(t) for t in single_tasks])
            await self._drain(master)
            single_elapsed = time.time() - t0
            single_completed = sum(1 for t in master.tasks.values() if t.status == TaskStatus.COMPLETED)
            single_failed = sum(1 for t in master.tasks.values() if t.status == TaskStatus.FAILED)

            # 清场景任务, 注册第二节点
            master.tasks.clear()
            await client.post(
                "/api/nodes/register",
                json={
                    "node_id": "agent-b",
                    "hostname": "mac-b",
                    "ip_address": "127.0.0.1",
                    "port": AGENT_PORT_B,
                    "total_memory_gb": 64.0,
                    "available_memory_gb": 48.0,
                    "cpu_cores": 12,
                    "gpu_cores": 30,
                    "max_tasks": 64,
                },
                headers=AUTH_HEADERS,
            )
            multi_tasks = [_bench_task(f"m-{i}") for i in range(_BENCH_PROMPTS)]
            for t in multi_tasks:
                t.preferred_node_id = "agent-a"
            t0 = time.time()
            await asyncio.gather(*[master.assign_task(t) for t in multi_tasks])
            await self._drain(master)
            multi_elapsed = time.time() - t0
            multi_completed = sum(1 for t in master.tasks.values() if t.status == TaskStatus.COMPLETED)
            multi_failed = sum(1 for t in master.tasks.values() if t.status == TaskStatus.FAILED)

        single_tput = single_completed / max(single_elapsed, 0.001)
        multi_tput = multi_completed / max(multi_elapsed, 0.001)
        print(
            f"\n[P1-12 基准] 单节点: {single_completed} done / {single_elapsed:.2f}s = {single_tput:.2f} task/s "
            f"({single_failed} failed) | 双节点: {multi_completed} done / {multi_elapsed:.2f}s = "
            f"{multi_tput:.2f} task/s ({multi_failed} failed) | 增益 {multi_tput / max(single_tput, 0.001):.2f}×"
        )

        assert single_failed == 0, f"单节点有失败: {single_failed}"
        assert multi_failed == 0, f"双节点有失败: {multi_failed}"
        assert single_completed == _BENCH_PROMPTS, f"单节点完成数不匹配: {single_completed}/{_BENCH_PROMPTS}"
        assert multi_completed == _BENCH_PROMPTS, f"双节点完成数不匹配: {multi_completed}/{_BENCH_PROMPTS}"
        # 多节点不退化 (≥0.9× 单节点) — 真模型抖动留余量, 不卡绝对增益
        assert multi_tput >= single_tput * 0.9, (
            f"多节点吞吐退化: multi={multi_tput:.2f} < single*0.9={single_tput * 0.9:.2f}"
        )

"""S4 E2E DATA 并行真实模型推理 — 接上游 fusion-mlx /v1/chat/completions。

真实链: ClusterMaster DATA 派发 → PortRoutingTransport → 两 AgentServer 真 FastAPI
→ NodeAgent._execute_inference → FusionMLXBackend.chat → fusion-mlx /v1/chat/completions
(真模型 Llama-3.2-1B-Instruct-4bit)。两节点各跑同一 prompt, 各返回独立 content +
usage, 末节点 COMPLETED, result.outputs 长度 == 节点数。

DATA 并行语义: 同模型多副本并发服务批量请求, 各节点独立完整推理 (非层切分)。
对照 PIPELINE (test_pipeline_e2e.py 切层链传 hidden_states)。

依赖真 fusion-mlx 运行 + 小模型已下载; 不满足则 skip (不假模型, 不 mock)。
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.agent.node_agent import FusionMLXBackend
from fusion_multi_node.master import ClusterMaster, ClusterTask, ParallelMode, TaskStatus
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}
AGENT_PORT_A = 22455
AGENT_PORT_B = 22456

_MLX_URL = os.environ.get("FUSION_MLX_URL", "http://127.0.0.1:11434")
_MLX_API_KEY = os.environ.get("FUSION_MLX_TEST_API_KEY", "dahai168")
_MODEL_NAME = os.environ.get(
    "FUSION_DATA_MODEL",
    "mlx-community-Llama-3.2-1B-Instruct-4bit",
)


def _mlx_alive() -> bool:
    import httpx

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
    # 模型 id 在 fusion-mlx /v1/models 列表内即视为可加载 (非预加载检查)。
    import httpx

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
    """按 URL 端口路由到对应 agent ASGI app (复用 P5/P3 测试约定)。"""

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


def _make_real_agent_server(node_id: str, port: int) -> AgentServer:
    """真实 NodeAgent + 真 FusionMLXBackend (指向 live fusion-mlx)。"""
    backend = FusionMLXBackend(base_url=_MLX_URL, api_key=_MLX_API_KEY)
    agent = NodeAgent(
        config=AgentConfig(
            node_id=node_id,
            cluster_token=TEST_TOKEN,
            agent_port=port,
            fusion_mlx_api_key=_MLX_API_KEY,
        ),
        backend=backend,
    )
    return AgentServer(agent=agent, shared_token=TEST_TOKEN)


def _register_node(client: AsyncClient, node_id: str, port: int) -> Any:
    payload = {
        "node_id": node_id,
        "hostname": f"mac-{node_id}",
        "ip_address": "127.0.0.1",
        "port": port,
        "arch": "arm64",
        "total_memory_gb": 64.0,
        "available_memory_gb": 48.0,
        "cpu_cores": 12,
        "gpu_cores": 30,
    }
    return client.post("/api/nodes/register", json=payload, headers=AUTH_HEADERS)


@_skip_no_backend
class TestDataParallelismE2E:
    """真实 DATA 并行推理端到端 — 真 fusion-mlx + 真模型 chat。"""

    @pytest.fixture
    async def cluster(self, monkeypatch):
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.is_safe_peer_host",
            lambda host: True,
        )
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.build_safe_url",
            lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
        )
        server_a = _make_real_agent_server("agent-a", AGENT_PORT_A)
        server_b = _make_real_agent_server("agent-b", AGENT_PORT_B)
        port_to_app = {AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app}
        routing_transport = PortRoutingTransport(port_to_app)

        master = ClusterMaster(heartbeat_timeout=60.0)
        master_server = MasterServer(master=master, shared_token=TEST_TOKEN)
        master_server._approval_manager = None

        async def _fake_dispatch_http():
            return AsyncClient(transport=routing_transport, timeout=120.0)

        monkeypatch.setattr(master, "_get_dispatch_http", _fake_dispatch_http)
        master._dispatch_token = TEST_TOKEN

        try:
            yield {
                "master": master,
                "master_app": master_server.app,
                "server_a": server_a,
                "server_b": server_b,
                "routing_transport": routing_transport,
            }
        finally:
            await master.stop()
            await routing_transport.aclose()

    async def _drain_dispatch(self, master: ClusterMaster, timeout_s: float = 120.0) -> None:
        for _ in range(int(timeout_s * 20)):
            pending = [t for t in master._dispatch_tasks.values() if not t.done()]
            if not pending:
                return
            await asyncio.sleep(0.05)

    async def test_data_parallel_two_node_real_inference(self, cluster):
        """两节点 DATA 并行: 同 prompt 派发到两节点, 各独立推理, 合并两份 content。

        真 fusion-mlx /v1/chat/completions 真模型, 非退化非 mock。
        断言: COMPLETED, node_count==2, 两 outputs 各带 node_id + content。
        """
        master = cluster["master"]
        transport = ASGITransport(app=cluster["master_app"])
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r1 = await _register_node(client, "agent-a", AGENT_PORT_A)
            r2 = await _register_node(client, "agent-b", AGENT_PORT_B)
            assert r1.status_code == 200 and r2.status_code == 200

            task = ClusterTask(
                task_id="task-data-e2e",
                name="e2e-real-data",
                mode=ParallelMode.DATA,
                model_name=_MODEL_NAME,
                # DATA 并行节点数 = len(model_shards) (cluster_master:639 count=...or 1)。
                # 两 shard → 两节点各独立推理 (非层切分; shard 元数据仅定节点数)。
                model_shards=[{"id": "s0"}, {"id": "s1"}],
                task_type="inference",
                params={
                    "prompt": "用一词回答: 你好",
                    "messages": [],
                    "max_tokens": 16,
                    "temperature": 0.1,
                },
            )
            ok = await master.assign_task(task)
            assert ok, f"assign 失败: {task.error}"
            assert task.assigned_nodes == ["agent-a", "agent-b"], task.assigned_nodes

            await self._drain_dispatch(master)
            final = await master.get_task(task.task_id)

        assert final.status == TaskStatus.COMPLETED, f"期望 COMPLETED 实得 {final.status}: {final.error}"
        assert final.error == ""
        result = final.result
        assert result["node_count"] == 2
        outputs = result["outputs"]
        assert len(outputs) == 2, f"DATA 并行应有 2 份输出, 实得 {len(outputs)}: {result}"
        node_ids = {o.get("node_id") for o in outputs}
        assert node_ids == {"agent-a", "agent-b"}, node_ids
        for o in outputs:
            content = o.get("content", "")
            assert isinstance(content, str) and len(content) > 0, f"空 content: {o}"
            assert "usage" in o, f"缺 usage: {o}"
        logger.info(
            f"S4 DATA 并行 E2E 通过: {node_ids}, "
            f"content_a={outputs[0].get('content')[:30]!r} "
            f"content_b={outputs[1].get('content')[:30]!r}"
        )

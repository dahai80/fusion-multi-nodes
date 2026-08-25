"""P3 E2E 真实张量 PIPELINE — 接上游 fusion-mlx /distributed/* (#621)。

真实链: ClusterMaster PIPELINE 派发 → PortRoutingTransport → 两 AgentServer 真 FastAPI
→ NodeAgent._execute_pipeline_step → FusionMLXBackend → fusion-mlx /distributed/load_shard
+ /distributed/pipeline_step (真模型 Llama-3.2-1B-Instruct-4bit, 16 层, 切 [0,8]/[8,16])。
末节点返回最终 hidden_states (b64.npy, shape (1,4,2048))。

lm_head/解码超上游首版范围 (docs/distributed-pipeline.md line 151) — 本测试只验层前向链
数值张量 round-trip, 不验 token 生成。

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
AGENT_PORT_A = 22445
AGENT_PORT_B = 22446

# 真 fusion-mlx 地址 + api_key (与 ~/.fusion-mlx/settings.json auth.api_key 同源)。
_MLX_URL = os.environ.get("FUSION_MLX_URL", "http://127.0.0.1:11434")
_MLX_API_KEY = os.environ.get("FUSION_MLX_TEST_API_KEY", "dahai168")
_MODEL_PATH = os.environ.get(
    "FUSION_PIPELINE_MODEL",
    os.path.expanduser("~/.fusion-mlx/models/mlx-community-Llama-3.2-1B-Instruct-4bit"),
)


def _mlx_alive() -> bool:
    import httpx

    try:
        r = httpx.get(f"{_MLX_URL}/v1/models", headers={"Authorization": f"Bearer {_MLX_API_KEY}"}, timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


def _model_present() -> bool:
    return os.path.isdir(_MODEL_PATH) and any(
        f.endswith(".safetensors") for f in os.listdir(_MODEL_PATH)
    )


skip_no_backend = pytest.mark.skipif(
    not (_mlx_alive() and _model_present()),
    reason="真 fusion-mlx 未运行或小模型未下载 (需 Llama-3.2-1B-Instruct-4bit)",
)


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 agent ASGI app (复用 P5 测试约定)。"""

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


@pytest.mark.skipif(not (_mlx_alive() and _model_present()), reason="真 fusion-mlx 未运行或小模型未下载")
class TestPipelineE2E:
    """真实张量 PIPELINE 端到端 — 真 fusion-mlx + 真模型层切分。"""

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
            # 清理上游分片 (E2E 过程数据, 只留日志)
            await _cleanup_shards()

    async def _drain_dispatch(self, master: ClusterMaster, timeout_s: float = 60.0) -> None:
        for _ in range(int(timeout_s * 20)):
            pending = [t for t in master._dispatch_tasks.values() if not t.done()]
            if not pending:
                return
            await asyncio.sleep(0.05)

    async def test_pipeline_two_shard_real_tensor(self, cluster):
        """两节点 PIPELINE: shard0 [0,8] embed+layers → shard1 [8,16] layers
        → 末节点返回 hidden_states (1,4,2048) b64.npy。

        真 fusion-mlx /distributed/* 真模型层前向, 非退化非 mock。
        """
        master = cluster["master"]
        transport = ASGITransport(app=cluster["master_app"])
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r1 = await _register_node(client, "agent-a", AGENT_PORT_A)
            r2 = await _register_node(client, "agent-b", AGENT_PORT_B)
            assert r1.status_code == 200 and r2.status_code == 200

            task = ClusterTask(
                task_id="task-pipeline-e2e",
                name="e2e-real-pipeline",
                mode=ParallelMode.PIPELINE,
                model_name="Llama-3.2-1B-Instruct-4bit",
                model_shards=[
                    {"shard_index": 0, "layer_range": [0, 8]},
                    {"shard_index": 1, "layer_range": [8, 16]},
                ],
                task_type="pipeline_step",
                params={
                    "model_id": _MODEL_PATH,
                    "input_ids": [10, 20, 30, 40],
                },
            )
            ok = await master.assign_task(task)
            assert ok, f"assign 失败: {task.error}"
            assert task.assigned_nodes == ["agent-a", "agent-b"], task.assigned_nodes

            await self._drain_dispatch(master)
            final = await master.get_task(task.task_id)

        assert final.status == TaskStatus.COMPLETED, (
            f"期望 COMPLETED 实得 {final.status}: {final.error}"
        )
        assert final.error == ""
        result = final.result
        # 两段都跑通, 末段出口 hidden_states 非空 + 形状 (1,4,2048)
        assert result["node_count"] == 2
        assert len(result["steps"]) == 2
        assert result["shape"] == [1, 4, 2048], result["shape"]
        assert result["dtype"].startswith("mlx"), result["dtype"]
        assert isinstance(result["hidden_states"], str) and len(result["hidden_states"]) > 100
        # b64 能解回 .npy 且形状对 (张量 round-trip 完整)
        import base64
        import tempfile

        import mlx.core as mx

        raw = base64.b64decode(result["hidden_states"])
        with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as tf:
            tf.write(raw)
            npy_path = tf.name
        arr = mx.load(npy_path)
        os.unlink(npy_path)
        mx.eval(arr)
        assert list(arr.shape) == [1, 4, 2048], arr.shape
        assert str(arr.dtype).startswith("mlx"), arr.dtype
        logger.info(f"P3 E2E 通过: final shape {arr.shape} dtype {arr.dtype}")


async def _cleanup_shards() -> None:
    """E2E 后清 fusion-mlx 注册的分片 (过程数据, 不留)。"""
    import httpx

    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(
                f"{_MLX_URL}/distributed/shards",
                headers={"Authorization": f"Bearer {_MLX_API_KEY}"},
            )
            if r.status_code != 200:
                return
            for s in r.json().get("shards", []):
                sid = s.get("shard_id", "")
                if sid:
                    await c.delete(
                        f"{_MLX_URL}/distributed/shards/{sid}",
                        headers={"Authorization": f"Bearer {_MLX_API_KEY}"},
                    )
    except Exception as e:
        logger.debug(f"分片清理失败 (不影响测试): {e}")

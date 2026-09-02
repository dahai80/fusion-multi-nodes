"""#65: pipeline 模式门控 — pipeline_enabled=False 早拒; shard 角色过滤; 404 → upstream_missing → FAILED。"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.config import ClusterConfig
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeStatus, ParallelMode, TaskStatus
from fusion_multi_node.master.load_metrics import LoadMetrics
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


def _config(pipeline_enabled: bool = False) -> ClusterConfig:
    cfg = ClusterConfig()
    cfg.set("parallel.pipeline_enabled", pipeline_enabled)
    return cfg


@pytest.fixture
def master_server():
    master = ClusterMaster(heartbeat_timeout=60.0)
    master._cluster_config = _config(False)
    server = MasterServer(master=master, config=_config(False), shared_token=TEST_TOKEN)
    server._approval_manager = None
    return server


@pytest.fixture
def app(master_server):
    return master_server.app


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_pipeline_disabled_rejects_submit(client, master_server):
    # pipeline_enabled=False → submit mode=pipeline 返 400 明确报错
    resp = await client.post(
        "/api/tasks/submit",
        json={"name": "t", "mode": "pipeline", "model_name": "big-model"},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 400
    assert "pipeline 模式未启用" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_pipeline_enabled_passes_gate(client, master_server):
    master_server._cluster_config = _config(True)
    master_server.master._cluster_config = _config(True)
    # pipeline 开 + 有节点 → 不在 submit 拦 (落到 assign_task 调度; 无 ONLINE 节点 → 503)
    resp = await client.post(
        "/api/tasks/submit",
        json={"name": "t", "mode": "pipeline", "model_name": "big-model"},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code != 400


@pytest.mark.asyncio
async def test_pipeline_shard_role_filter():
    # #65: pipeline 模式 + pipeline_shard_roles=["heavy"] → 非 heavy 角色节点被排除
    master = ClusterMaster(heartbeat_timeout=60.0)
    master._cluster_config = _config(True)
    from fusion_multi_node.master import NodeInfo

    heavy = NodeInfo(
        node_id="heavy-1", hostname="h", ip_address="10.0.0.1", port=11458, role="heavy", status=NodeStatus.ONLINE
    )
    heavy.available_memory_gb = 64.0
    heavy.total_memory_gb = 64.0
    heavy.max_tasks = 4
    heavy.last_heartbeat = time.time()
    general = NodeInfo(
        node_id="general-1", hostname="g", ip_address="10.0.0.2", port=11458, role="general", status=NodeStatus.ONLINE
    )
    general.available_memory_gb = 64.0
    general.total_memory_gb = 64.0
    general.max_tasks = 4
    general.last_heartbeat = time.time()
    master.nodes["heavy-1"] = heavy
    master.nodes["general-1"] = general
    master.load_router.update_metrics("heavy-1", LoadMetrics(uma_used_ratio=0.5, cpu_percent=50.0, metal_util=0.5))
    master.load_router.update_metrics("general-1", LoadMetrics(uma_used_ratio=0.5, cpu_percent=50.0, metal_util=0.5))
    selected = await master.select_nodes(ParallelMode.PIPELINE, required_memory_gb=1.0, count=1, shard_roles=["heavy"])
    ids = [s.node_id for s in selected]
    assert "heavy-1" in ids
    assert "general-1" not in ids


@pytest.mark.asyncio
async def test_execute_pipeline_step_404_upstream_missing():
    # agent 侧: /distributed/pipeline_step 404 → 返 upstream_missing:True
    from fusion_multi_node.agent.node_agent import AgentConfig, FusionMLXBackend, NodeAgent

    config = AgentConfig(node_id="n1", fusion_mlx_port=11434)
    agent = NodeAgent(config)
    backend = FusionMLXBackend(base_url="http://127.0.0.1:11434", api_key="k")
    req = httpx.Request("POST", "http://x/distributed/pipeline_step")
    err_resp = httpx.Response(404, request=req)
    he = httpx.HTTPStatusError("404", request=req, response=err_resp)
    backend.load_shard = AsyncMock(return_value={"shard_id": "s0", "num_layers": 4})
    backend.pipeline_step = AsyncMock(side_effect=he)
    agent._backend = backend
    result = await agent._execute_pipeline_step(
        {"task_id": "t1", "params": {"model_id": "m", "shard_index": 0, "layer_range": [0, 4], "hidden_states": ""}}
    )
    assert result.get("upstream_missing") is True
    assert "未实现" in result.get("error", "")


@pytest.mark.asyncio
async def test_dispatch_pipeline_upstream_missing_finalizes_failed():
    # master 侧: _dispatch_to_node 返 upstream_missing → _dispatch_pipeline 映射 FAILED (不可重试)
    master = ClusterMaster(heartbeat_timeout=60.0)
    master._cluster_config = _config(True)
    from fusion_multi_node.master import NodeInfo

    node = NodeInfo(
        node_id="n1", hostname="n", ip_address="10.0.0.1", port=11458, role="heavy", status=NodeStatus.ONLINE
    )
    node.available_memory_gb = 64.0
    node.total_memory_gb = 64.0
    node.max_tasks = 4
    node.last_heartbeat = time.time()
    master.nodes["n1"] = node
    task = ClusterTask(
        task_id="t1",
        name="p",
        mode=ParallelMode.PIPELINE,
        model_name="m",
        model_shards=[{"shard_index": 0, "layer_range": [0, 4]}],
        assigned_nodes=["n1"],
        status=TaskStatus.RUNNING,
        created_at=0.0,
        started_at=0.0,
    )
    master.tasks["t1"] = task

    async def fake_dispatch(client, t, nid, snap, token, pipeline_step_params=None):
        return {"task_id": "t1", "error": "上游 /distributed/* 未实现 (fusion-mlx#621)", "upstream_missing": True}

    master._dispatch_to_node = fake_dispatch

    await master._dispatch_pipeline(task, ["n1"], dict(master.nodes), TEST_TOKEN)
    assert master.tasks["t1"].status == TaskStatus.FAILED
    assert "未实现" in master.tasks["t1"].error

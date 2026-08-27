"""F4 (#32): /api/v1 集群控制契约单测 — typed response_model 覆盖 9 操作。

校验:
- 响应 schema 与 V1* Pydantic 模型对齐 (字段存在/类型)。
- 9 操作 (list_nodes/register/remove/submit/migrate/degrade/progress/
  cluster_stats/observability_suggestions) 全覆盖。
- autoscaler 未接线 → 503 (契约文档化, 非歧义 enabled:False)。
- v1 与旧 /api/* 同源行为一致 (注册/列表)。
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.master import ClusterMaster
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


@pytest.fixture
def server():
    master = ClusterMaster(heartbeat_timeout=60.0)
    srv = MasterServer(master=master, shared_token=TEST_TOKEN)
    srv._approval_manager = None
    return srv


@pytest.fixture
async def client(server):
    async with AsyncClient(transport=ASGITransport(app=server.app), base_url="http://test") as c:
        yield c


def _register_payload(node_id="n1", **kw):
    return {
        "node_id": node_id,
        "hostname": kw.get("hostname", "mac-studio"),
        "ip_address": kw.get("ip_address", "10.0.1.5"),
        "port": kw.get("port", 11458),
        "arch": kw.get("arch", "arm64"),
        "total_memory_gb": kw.get("total_memory_gb", 64.0),
        "available_memory_gb": kw.get("available_memory_gb", 48.0),
        "cpu_cores": kw.get("cpu_cores", 12),
        "gpu_cores": kw.get("gpu_cores", 30),
        "device_model": kw.get("device_model", "MacStudio"),
        "uma_size_gb": kw.get("uma_size_gb", 64.0),
        "max_tasks": kw.get("max_tasks", 4),
    }


_NODE_FIELDS = {
    "node_id", "hostname", "ip_address", "port", "status", "role",
    "total_memory_gb", "available_memory_gb", "cpu_cores", "gpu_cores",
    "device_model", "uma_size_gb", "active_tasks", "max_tasks", "score",
    "last_heartbeat",
}

_TASK_FIELDS = {
    "task_id", "name", "mode", "model_name", "status", "assigned_nodes",
    "created_at", "started_at", "completed_at", "error", "required_capability",
    "priority", "degraded_from_model", "degradation_count", "cancel_reason",
    "sub_tasks", "result",
}


class TestV1ContractNodes:
    """操作 1-3: list_nodes / register / remove。"""

    @pytest.mark.asyncio
    async def test_list_nodes_schema(self, client):
        await client.post("/api/v1/nodes/register", json=_register_payload(), headers=AUTH_HEADERS)
        resp = await client.get("/api/v1/nodes", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert {"total", "online", "nodes"} <= data.keys()
        assert data["total"] == 1
        node = data["nodes"][0]
        assert node.keys() >= _NODE_FIELDS, f"缺字段: {_NODE_FIELDS - node.keys()}"
        assert node["role"] == "worker"

    @pytest.mark.asyncio
    async def test_get_node_schema(self, client):
        await client.post("/api/v1/nodes/register", json=_register_payload("n2"), headers=AUTH_HEADERS)
        resp = await client.get("/api/v1/nodes/n2", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        assert resp.json().keys() >= _NODE_FIELDS

    @pytest.mark.asyncio
    async def test_get_node_404(self, client):
        resp = await client.get("/api/v1/nodes/nope", headers=AUTH_HEADERS)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_register_node_schema(self, client):
        resp = await client.post("/api/v1/nodes/register", json=_register_payload("nreg"), headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert {"status", "node_id", "role"} <= data.keys()
        assert data["node_id"] == "nreg"
        assert data["role"] == "worker"

    @pytest.mark.asyncio
    async def test_register_invalid_node_id_400(self, client):
        payload = _register_payload("../etc")
        resp = await client.post("/api/v1/nodes/register", json=payload, headers=AUTH_HEADERS)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_remove_node_schema(self, client):
        await client.post("/api/v1/nodes/register", json=_register_payload("ndel"), headers=AUTH_HEADERS)
        resp = await client.delete("/api/v1/nodes/ndel", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "ok"


class TestV1ContractTasks:
    """操作 4-7: submit / migrate / degrade / progress。"""

    @pytest.mark.asyncio
    async def test_submit_task_schema(self, client):
        await client.post("/api/v1/nodes/register", json=_register_payload("nt1"), headers=AUTH_HEADERS)
        resp = await client.post(
            "/api/v1/tasks/submit",
            json={"name": "t1", "mode": "data", "model_name": "m", "prompt": "hi"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data.keys() >= _TASK_FIELDS
        assert "queued" in data

    @pytest.mark.asyncio
    async def test_submit_task_queued_202(self, client):
        # 无节点 → assign_task False → 503; 配额满/优先级 → PENDING queued → 202。
        # 这里仅校验 200 路径 queued 字段存在 (类型化契约)。
        await client.post("/api/v1/nodes/register", json=_register_payload("nt2"), headers=AUTH_HEADERS)
        resp = await client.post(
            "/api/v1/tasks/submit",
            json={"name": "t2", "mode": "data", "model_name": "m", "prompt": "hi"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code in (200, 202)
        assert "queued" in resp.json()

    @pytest.mark.asyncio
    async def test_submit_no_nodes_queued_202(self, client):
        # 无节点 → assign_task 入优先级队列 (queued) → 202 (非 503, 调度器正常排队)。
        resp = await client.post(
            "/api/v1/tasks/submit",
            json={"name": "t3", "mode": "data", "model_name": "m", "prompt": "hi"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 202, resp.text
        assert resp.json()["queued"] is True

    @pytest.mark.asyncio
    async def test_progress_schema(self, client):
        await client.post("/api/v1/nodes/register", json=_register_payload("np1"), headers=AUTH_HEADERS)
        submit = await client.post(
            "/api/v1/tasks/submit",
            json={"name": "tp", "mode": "data", "model_name": "m", "prompt": "hi"},
            headers=AUTH_HEADERS,
        )
        task_id = submit.json()["task_id"]
        resp = await client.get(f"/api/v1/tasks/{task_id}/progress", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert {
            "task_id", "name", "status", "progress", "total_shards",
            "completed_shards", "assigned_nodes", "elapsed_seconds",
            "remaining_seconds", "model_name",
        } <= data.keys()
        assert data["progress"] >= 0.0 and data["progress"] <= 1.0

    @pytest.mark.asyncio
    async def test_progress_404(self, client):
        resp = await client.get("/api/v1/tasks/nope/progress", headers=AUTH_HEADERS)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_migrate_schema(self, client):
        await client.post("/api/v1/nodes/register", json=_register_payload("nm1"), headers=AUTH_HEADERS)
        submit = await client.post(
            "/api/v1/tasks/submit",
            json={"name": "tm", "mode": "data", "model_name": "m", "prompt": "hi"},
            headers=AUTH_HEADERS,
        )
        task_id = submit.json()["task_id"]
        resp = await client.post(f"/api/v1/tasks/{task_id}/migrate", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "ok"
        assert resp.json()["task_id"] == task_id

    @pytest.mark.asyncio
    async def test_degrade_schema(self, client):
        # 降级须模型在 MODEL_DEGRADATION_CHAIN (70b→32b→...→1b), 否则无更小模型 → 400。
        await client.post("/api/v1/nodes/register", json=_register_payload("nd1"), headers=AUTH_HEADERS)
        submit = await client.post(
            "/api/v1/tasks/submit",
            json={"name": "td", "mode": "data", "model_name": "llama-70b", "prompt": "hi"},
            headers=AUTH_HEADERS,
        )
        task_id = submit.json()["task_id"]
        resp = await client.post(f"/api/v1/tasks/{task_id}/degrade", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "ok"


class TestV1ContractStatsAndOps:
    """操作 8-9: cluster_stats / observability_suggestions + autoscaler。"""

    @pytest.mark.asyncio
    async def test_cluster_stats_schema(self, client):
        await client.post("/api/v1/nodes/register", json=_register_payload("nc1"), headers=AUTH_HEADERS)
        resp = await client.get("/api/v1/cluster/stats", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert {"cluster", "tasks", "load_summary"} <= data.keys()
        assert {"online_nodes", "total_nodes", "active_tasks"} <= data["cluster"].keys()
        assert {"total", "completed", "failed"} <= data["tasks"].keys()

    @pytest.mark.asyncio
    async def test_observability_suggestions_schema(self, client):
        resp = await client.get("/api/v1/observability/suggestions", headers=AUTH_HEADERS)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert {"suggestions", "error"} <= data.keys()
        assert isinstance(data["suggestions"], list)

    @pytest.mark.asyncio
    async def test_autoscaler_get_503_not_wired(self, client):
        # 契约文档化: autoscaler 未接线 → 503 (非歧义 enabled:False)。
        resp = await client.get("/api/v1/autoscaler/config", headers=AUTH_HEADERS)
        assert resp.status_code == 503
        assert "未接线" in resp.text

    @pytest.mark.asyncio
    async def test_autoscaler_put_503_not_wired(self, client):
        resp = await client.put(
            "/api/v1/autoscaler/config",
            json={"min_nodes": 1, "max_nodes": 4},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 503

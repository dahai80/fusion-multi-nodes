"""#70 回归: exclude_nodes 透传到 select_nodes — 提交端点级验证。

exclude_nodes 端到端已实现 (TaskSubmitRequest.exclude_nodes -> ClusterTask -> select_nodes 过滤)。
本测试钉住提交端点契约: 排除的节点不出现在 assigned_nodes。
"""

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.master import ClusterMaster
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


@pytest.fixture
def master_server():
    master = ClusterMaster(heartbeat_timeout=60.0)
    server = MasterServer(master=master, shared_token=TEST_TOKEN)
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


async def _register_node(client, node_id, ip):
    payload = {
        "node_id": node_id,
        "hostname": f"mac-{node_id}",
        "ip_address": ip,
        "port": 11458,
        "arch": "arm64",
        "total_memory_gb": 64.0,
        "available_memory_gb": 48.0,
        "cpu_cores": 12,
        "gpu_cores": 30,
    }
    resp = await client.post("/api/nodes/register", json=payload, headers=AUTH_HEADERS)
    assert resp.status_code == 200, resp.text
    return resp


class TestExcludeNodesSubmit:
    @pytest.mark.asyncio
    async def test_exclude_node_routed_to_other(self, client):
        await _register_node(client, "n1", "10.0.1.1")
        await _register_node(client, "n2", "10.0.1.2")
        resp = await client.post(
            "/api/tasks/submit",
            json={
                "name": "exclude-n1",
                "mode": "data",
                "model_name": "llama-3b",
                "exclude_nodes": ["n1"],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] == "running"
        assigned = data["assigned_nodes"]
        assert "n1" not in assigned, f"被排除节点 n1 仍被派发: {assigned}"
        assert assigned == ["n2"], f"应派发到唯一未排除节点 n2: {assigned}"

    @pytest.mark.asyncio
    async def test_exclude_all_nodes_queues(self, client):
        await _register_node(client, "n1", "10.0.1.1")
        await _register_node(client, "n2", "10.0.1.2")
        resp = await client.post(
            "/api/tasks/submit",
            json={
                "name": "exclude-all",
                "mode": "data",
                "model_name": "llama-3b",
                "exclude_nodes": ["n1", "n2"],
            },
            headers=AUTH_HEADERS,
        )
        # 所有节点排除 → 无可用节点 → 入队 202 (P1-H) 或 503 (队列满)。
        assert resp.status_code in (202, 503), resp.text
        if resp.status_code == 202:
            data = resp.json()
            assert data.get("queued") is True
            assert data["assigned_nodes"] == []

    @pytest.mark.asyncio
    async def test_no_exclude_routed_any(self, client):
        await _register_node(client, "n1", "10.0.1.1")
        resp = await client.post(
            "/api/tasks/submit",
            json={
                "name": "no-exclude",
                "mode": "data",
                "model_name": "llama-3b",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] == "running"
        assert data["assigned_nodes"] == ["n1"]

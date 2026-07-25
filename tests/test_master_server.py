"""Master Server FastAPI 测试。"""

import pytest
from httpx import AsyncClient, ASGITransport

from fusion_multi_node.master import ClusterMaster, NodeInfo, NodeStatus
from fusion_multi_node.server.master_server import MasterServer


@pytest.fixture
def master_server():
    master = ClusterMaster()
    server = MasterServer(master=master)
    return server


@pytest.fixture
def app(master_server):
    return master_server.app


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestMasterServer:
    @pytest.mark.asyncio
    async def test_health(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["role"] == "master"

    @pytest.mark.asyncio
    async def test_register_node(self, client, master_server):
        resp = await client.post("/api/nodes/register", json={
            "node_id": "n1",
            "hostname": "mac-studio",
            "ip_address": "10.0.1.5",
            "port": 9755,
            "arch": "arm64",
            "total_memory_gb": 64.0,
            "available_memory_gb": 48.0,
            "cpu_cores": 12,
            "gpu_cores": 30,
        })
        assert resp.status_code == 200
        assert master_server.master.nodes["n1"].hostname == "mac-studio"

    @pytest.mark.asyncio
    async def test_heartbeat(self, client, master_server):
        await client.post("/api/nodes/register", json={
            "node_id": "n1",
            "hostname": "mac-studio",
            "ip_address": "10.0.1.5",
            "port": 9755,
        })
        resp = await client.post("/api/nodes/heartbeat", json={
            "node_id": "n1",
            "available_memory_gb": 40.0,
            "active_tasks": 2,
        })
        assert resp.status_code == 200
        node = master_server.master.nodes["n1"]
        assert node.available_memory_gb == 40.0
        assert node.active_tasks == 2

    @pytest.mark.asyncio
    async def test_heartbeat_unknown_node(self, client):
        resp = await client.post("/api/nodes/heartbeat", json={
            "node_id": "unknown",
            "available_memory_gb": 10.0,
        })
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_list_nodes(self, client, master_server):
        await client.post("/api/nodes/register", json={
            "node_id": "n1",
            "hostname": "node1",
            "ip_address": "10.0.1.1",
            "port": 9755,
        })
        await client.post("/api/nodes/register", json={
            "node_id": "n2",
            "hostname": "node2",
            "ip_address": "10.0.1.2",
            "port": 9755,
        })
        resp = await client.get("/api/nodes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

    @pytest.mark.asyncio
    async def test_get_node(self, client, master_server):
        await client.post("/api/nodes/register", json={
            "node_id": "n1",
            "hostname": "node1",
            "ip_address": "10.0.1.1",
            "port": 9755,
        })
        resp = await client.get("/api/nodes/n1")
        assert resp.status_code == 200
        assert resp.json()["hostname"] == "node1"

    @pytest.mark.asyncio
    async def test_delete_node(self, client, master_server):
        await client.post("/api/nodes/register", json={
            "node_id": "n1",
            "hostname": "node1",
            "ip_address": "10.0.1.1",
            "port": 9755,
        })
        resp = await client.delete("/api/nodes/n1")
        assert resp.status_code == 200
        assert "n1" not in master_server.master.nodes

    @pytest.mark.asyncio
    async def test_submit_task(self, client, master_server):
        await client.post("/api/nodes/register", json={
            "node_id": "n1",
            "hostname": "node1",
            "ip_address": "10.0.1.1",
            "port": 9755,
            "total_memory_gb": 64.0,
            "available_memory_gb": 48.0,
        })
        resp = await client.post("/api/tasks/submit", json={
            "name": "test-inference",
            "mode": "data",
            "model_name": "llama-3b",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"

    @pytest.mark.asyncio
    async def test_submit_task_no_nodes(self, client):
        resp = await client.post("/api/tasks/submit", json={
            "name": "test-inference",
            "mode": "data",
        })
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_cluster_stats(self, client, master_server):
        resp = await client.get("/api/cluster/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_nodes" in data
        assert "online_nodes" in data

    @pytest.mark.asyncio
    async def test_fault_report(self, client, master_server):
        await client.post("/api/nodes/register", json={
            "node_id": "n1",
            "hostname": "node1",
            "ip_address": "10.0.1.1",
            "port": 9755,
        })
        resp = await client.post("/api/nodes/fault", json={
            "node_id": "n1",
            "fault_type": "oom",
            "message": "Out of memory",
        })
        assert resp.status_code == 200
        assert master_server.master.nodes["n1"].status == NodeStatus.ERROR

    @pytest.mark.asyncio
    async def test_kv_register_and_find(self, client, master_server):
        await client.post("/api/nodes/register", json={"node_id": "n1", "hostname": "node1", "ip_address": "10.0.1.1", "port": 9755})
        await client.post("/api/kv/register", json={
            "cache_id": "kv1",
            "model_name": "llama-3b",
            "node_id": "n1",
            "size_mb": 256.0,
        })
        resp = await client.get("/api/kv/find/llama-3b")
        assert resp.status_code == 200
        assert resp.json()["cache_id"] == "kv1"

    @pytest.mark.asyncio
    async def test_kv_find_missing(self, client):
        resp = await client.get("/api/kv/find/unknown-model")
        assert resp.status_code == 404

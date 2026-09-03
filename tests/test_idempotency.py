"""#71 幂等键: X-Idempotency-Key 在 /api/tasks/submit 与 /api/v1/tasks/submit。

同键重复提交 (客户端重试) → 复用已存在任务, 不产生重复任务。
"""

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.master import ClusterMaster, TaskStatus
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


async def _register_node(client, node_id="n1"):
    payload = {
        "node_id": node_id,
        "hostname": "mac-studio",
        "ip_address": "10.0.1.5",
        "port": 11458,
        "arch": "arm64",
        "total_memory_gb": 64.0,
        "available_memory_gb": 48.0,
        "cpu_cores": 12,
        "gpu_cores": 30,
    }
    resp = await client.post("/api/nodes/register", json=payload, headers=AUTH_HEADERS)
    assert resp.status_code == 200, resp.text


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_same_key_returns_same_task(self, client, master_server):
        await _register_node(client)
        key = "idem-abc-123"
        headers = {**AUTH_HEADERS, "X-Idempotency-Key": key}
        r1 = await client.post(
            "/api/tasks/submit",
            json={"name": "t1", "mode": "data", "model_name": "llama-3b"},
            headers=headers,
        )
        assert r1.status_code == 200, r1.text
        tid1 = r1.json()["task_id"]

        r2 = await client.post(
            "/api/tasks/submit",
            json={"name": "t1-dup", "mode": "data", "model_name": "llama-3b"},
            headers=headers,
        )
        assert r2.status_code == 200, r2.text
        tid2 = r2.json()["task_id"]
        assert tid1 == tid2, f"同键应复用同一任务: {tid1} vs {tid2}"
        tasks = list(master_server.master.tasks.values())
        assert len(tasks) == 1, f"应只产生 1 个任务, 实际 {len(tasks)}"

    @pytest.mark.asyncio
    async def test_different_keys_create_separate_tasks(self, client, master_server):
        await _register_node(client)
        r1 = await client.post(
            "/api/tasks/submit",
            json={"name": "t1", "mode": "data", "model_name": "llama-3b"},
            headers={**AUTH_HEADERS, "X-Idempotency-Key": "key-A"},
        )
        r2 = await client.post(
            "/api/tasks/submit",
            json={"name": "t2", "mode": "data", "model_name": "llama-3b"},
            headers={**AUTH_HEADERS, "X-Idempotency-Key": "key-B"},
        )
        assert r1.status_code == 200 and r2.status_code == 200
        assert r1.json()["task_id"] != r2.json()["task_id"]
        assert len(master_server.master.tasks) == 2

    @pytest.mark.asyncio
    async def test_no_key_creates_separate_tasks(self, client, master_server):
        await _register_node(client)
        r1 = await client.post(
            "/api/tasks/submit",
            json={"name": "t1", "mode": "data", "model_name": "llama-3b"},
            headers=AUTH_HEADERS,
        )
        r2 = await client.post(
            "/api/tasks/submit",
            json={"name": "t2", "mode": "data", "model_name": "llama-3b"},
            headers=AUTH_HEADERS,
        )
        assert r1.json()["task_id"] != r2.json()["task_id"]
        assert len(master_server.master.tasks) == 2

    @pytest.mark.asyncio
    async def test_expired_key_creates_new_task(self, client, master_server):
        await _register_node(client)
        master_server.master._idempotency_ttl = 0.01
        key = "expiring-key"
        headers = {**AUTH_HEADERS, "X-Idempotency-Key": key}
        r1 = await client.post(
            "/api/tasks/submit",
            json={"name": "t1", "mode": "data", "model_name": "llama-3b"},
            headers=headers,
        )
        tid1 = r1.json()["task_id"]
        import asyncio

        await asyncio.sleep(0.05)
        r2 = await client.post(
            "/api/tasks/submit",
            json={"name": "t2", "mode": "data", "model_name": "llama-3b"},
            headers=headers,
        )
        assert r2.status_code == 200, r2.text
        assert r2.json()["task_id"] != tid1, "过期键应创建新任务"

    @pytest.mark.asyncio
    async def test_terminal_task_key_reusable(self, client, master_server):
        await _register_node(client)
        key = "terminal-key"
        headers = {**AUTH_HEADERS, "X-Idempotency-Key": key}
        r1 = await client.post(
            "/api/tasks/submit",
            json={"name": "t1", "mode": "data", "model_name": "llama-3b"},
            headers=headers,
        )
        tid1 = r1.json()["task_id"]
        task = master_server.master.tasks[tid1]
        task.status = TaskStatus.COMPLETED
        r2 = await client.post(
            "/api/tasks/submit",
            json={"name": "t2", "mode": "data", "model_name": "llama-3b"},
            headers=headers,
        )
        assert r2.status_code == 200, r2.text
        assert r2.json()["task_id"] != tid1, "终态任务键应允许复用"

    @pytest.mark.asyncio
    async def test_v1_submit_same_key(self, client, master_server):
        await _register_node(client)
        key = "v1-idem"
        headers = {**AUTH_HEADERS, "X-Idempotency-Key": key}
        r1 = await client.post(
            "/api/v1/tasks/submit",
            json={"name": "t1", "mode": "data", "model_name": "llama-3b"},
            headers=headers,
        )
        r2 = await client.post(
            "/api/v1/tasks/submit",
            json={"name": "t1-dup", "mode": "data", "model_name": "llama-3b"},
            headers=headers,
        )
        assert r1.status_code == 200 and r2.status_code == 200
        assert r1.json()["task_id"] == r2.json()["task_id"]

    @pytest.mark.asyncio
    async def test_ttl_purge_on_access(self, client, master_server):
        master = master_server.master
        master._idempotency_ttl = 0.01
        await master.register_idempotency("stale", "task_x")
        import asyncio

        await asyncio.sleep(0.05)
        hit = await master.try_idempotency("stale")
        assert hit is None, "过期键应被清除并返回 None"
        assert "stale" not in master._idempotency_keys

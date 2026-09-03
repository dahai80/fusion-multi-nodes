"""#69 cluster drain: drain-status 契约 + 排空判定 + 长任务阻塞。"""

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


async def _submit(client, name, exclude=None):
    body = {"name": name, "mode": "data", "model_name": "llama-3b"}
    if exclude:
        body["exclude_nodes"] = exclude
    return await client.post("/api/tasks/submit", json=body, headers=AUTH_HEADERS)


class TestClusterDrain:
    @pytest.mark.asyncio
    async def test_drain_status_not_draining(self, client):
        await _register_node(client)
        r = await client.get("/api/nodes/n1/drain-status", headers=AUTH_HEADERS)
        assert r.status_code == 200, r.text
        st = r.json()
        assert st["draining"] is False
        assert st["in_flight"] == 0
        assert st["ready"] is False
        assert st["long_task_active"] is False

    @pytest.mark.asyncio
    async def test_drain_redirects_new_submits(self, client, master_server):
        await _register_node(client, "n1")
        await _register_node(client, "n2")
        await client.post("/api/nodes/n1/drain", headers=AUTH_HEADERS)
        r = await _submit(client, "t1")
        assert r.status_code == 200, r.text
        assert r.json()["assigned_nodes"] == ["n2"]

    @pytest.mark.asyncio
    async def test_drain_ready_zero_inflight(self, client):
        await _register_node(client)
        await client.post("/api/nodes/n1/drain", headers=AUTH_HEADERS)
        r = await client.get("/api/nodes/n1/drain-status", headers=AUTH_HEADERS)
        assert r.status_code == 200
        st = r.json()
        assert st["draining"] is True
        assert st["in_flight"] == 0
        assert st["ready"] is True

    @pytest.mark.asyncio
    async def test_drain_not_ready_with_inflight(self, client, master_server):
        await _register_node(client)
        r = await _submit(client, "t1")
        tid = r.json()["task_id"]
        await client.post("/api/nodes/n1/drain", headers=AUTH_HEADERS)
        st_resp = await client.get("/api/nodes/n1/drain-status", headers=AUTH_HEADERS)
        st = st_resp.json()
        assert st["in_flight"] == 1
        assert st["ready"] is False
        # 模拟任务完成
        master_server.master.tasks[tid].status = TaskStatus.COMPLETED
        st2 = (await client.get("/api/nodes/n1/drain-status", headers=AUTH_HEADERS)).json()
        assert st2["in_flight"] == 0
        assert st2["ready"] is True

    @pytest.mark.asyncio
    async def test_drain_long_task_active(self, client, master_server):
        await _register_node(client)
        master_server.master._drain_long_task_threshold = 60.0
        r = await _submit(client, "t1")
        tid = r.json()["task_id"]
        task = master_server.master.tasks[tid]
        task.timeout_seconds = 600.0  # > 阈值 60 → 长任务
        await client.post("/api/nodes/n1/drain", headers=AUTH_HEADERS)
        st = (await client.get("/api/nodes/n1/drain-status", headers=AUTH_HEADERS)).json()
        assert st["long_task_active"] is True
        assert st["ready"] is False

    @pytest.mark.asyncio
    async def test_undrain_re_admits(self, client):
        await _register_node(client, "n1")
        await _register_node(client, "n2")
        await client.post("/api/nodes/n1/drain", headers=AUTH_HEADERS)
        r = await _submit(client, "t1")
        assert r.json()["assigned_nodes"] == ["n2"]
        await client.post("/api/nodes/n1/undrain", headers=AUTH_HEADERS)
        # n1 恢复可派发 (排除 n2 强制选 n1)
        r2 = await _submit(client, "t2", exclude=["n2"])
        assert r2.status_code == 200
        assert r2.json()["assigned_nodes"] == ["n1"]

    @pytest.mark.asyncio
    async def test_drain_status_node_not_found(self, client):
        r = await client.get("/api/nodes/ghost/drain-status", headers=AUTH_HEADERS)
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_drain_wait_cli_ready(self, client, master_server, monkeypatch):
        # CLI _drain_wait 逻辑: drain → ready → 退出 0
        from fusion_multi_node import cli

        await _register_node(client)
        calls = {"post": 0, "get": 0}

        async def fake_master_http(method, path, json_body=None):
            if method == "POST":
                calls["post"] += 1
                return {"status": "ok"}
            calls["get"] += 1
            return {"node_id": "n1", "draining": True, "in_flight": 0, "ready": True, "long_task_active": False}

        monkeypatch.setattr(cli, "_master_http", fake_master_http)
        await cli._drain_wait("n1", 10)
        assert calls["get"] >= 1

    @pytest.mark.asyncio
    async def test_drain_wait_cli_timeout(self, client, master_server, monkeypatch):
        from fusion_multi_node import cli

        await _register_node(client)

        async def fake_master_http(method, path, json_body=None):
            return {"node_id": "n1", "draining": True, "in_flight": 1, "ready": False, "long_task_active": True}

        monkeypatch.setattr(cli, "_master_http", fake_master_http)
        monkeypatch.setattr(cli.asyncio, "sleep", lambda *a, **kw: _noop_sleep())
        with pytest.raises(SystemExit) as exc:
            await cli._drain_wait("n1", 1)
        assert exc.value.code == 1


async def _noop_sleep():
    return None

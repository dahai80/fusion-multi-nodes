"""Master Server FastAPI coverage tests."""

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeStatus, ParallelMode, TaskStatus
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


def _register_node(client, node_id="n1", **kwargs):
    payload = {
        "node_id": node_id,
        "hostname": kwargs.get("hostname", "mac-studio"),
        "ip_address": kwargs.get("ip_address", "10.0.1.5"),
        "port": kwargs.get("port", 11458),
        "arch": kwargs.get("arch", "arm64"),
        "total_memory_gb": kwargs.get("total_memory_gb", 64.0),
        "available_memory_gb": kwargs.get("available_memory_gb", 48.0),
        "cpu_cores": kwargs.get("cpu_cores", 12),
        "gpu_cores": kwargs.get("gpu_cores", 30),
    }
    return client.post("/api/nodes/register", json=payload, headers=AUTH_HEADERS)


class TestMasterServerHealth:
    @pytest.mark.asyncio
    async def test_health(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["role"] == "master"


class TestMasterServerNodeManagement:
    @pytest.mark.asyncio
    async def test_register_node(self, client, master_server):
        resp = await _register_node(client)
        assert resp.status_code == 200
        assert master_server.master.nodes["n1"].hostname == "mac-studio"

    @pytest.mark.asyncio
    async def test_heartbeat(self, client, master_server):
        await _register_node(client)
        resp = await client.post(
            "/api/nodes/heartbeat",
            json={
                "node_id": "n1",
                "available_memory_gb": 40.0,
                "active_tasks": 2,
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        node = master_server.master.nodes["n1"]
        assert node.available_memory_gb == 40.0
        assert node.active_tasks == 2

    @pytest.mark.asyncio
    async def test_heartbeat_unknown_node(self, client):
        resp = await client.post(
            "/api/nodes/heartbeat",
            json={
                "node_id": "unknown",
                "available_memory_gb": 10.0,
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_heartbeat_revives_offline_node(self, client, master_server):
        await _register_node(client)
        master_server.master.nodes["n1"].status = NodeStatus.OFFLINE
        resp = await client.post(
            "/api/nodes/heartbeat",
            json={
                "node_id": "n1",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        assert master_server.master.nodes["n1"].status == NodeStatus.ONLINE

    @pytest.mark.asyncio
    async def test_list_nodes(self, client, master_server):
        await _register_node(client, node_id="n1")
        await _register_node(client, node_id="n2", hostname="mac2", ip_address="10.0.1.2")
        resp = await client.get("/api/nodes", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

    @pytest.mark.asyncio
    async def test_get_node(self, client, master_server):
        await _register_node(client)
        resp = await client.get("/api/nodes/n1", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert resp.json()["hostname"] == "mac-studio"

    @pytest.mark.asyncio
    async def test_get_node_missing(self, client):
        resp = await client.get("/api/nodes/nonexistent", headers=AUTH_HEADERS)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_node(self, client, master_server):
        await _register_node(client)
        resp = await client.delete("/api/nodes/n1", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert "n1" not in master_server.master.nodes

    @pytest.mark.asyncio
    async def test_fault_report(self, client, master_server):
        await _register_node(client)
        resp = await client.post(
            "/api/nodes/fault",
            json={
                "node_id": "n1",
                "fault_type": "oom",
                "message": "Out of memory",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        assert master_server.master.nodes["n1"].status == NodeStatus.FAULT

    @pytest.mark.asyncio
    async def test_fault_report_unknown_node(self, client):
        resp = await client.post(
            "/api/nodes/fault",
            json={
                "node_id": "unknown",
                "fault_type": "crash",
                "message": "Something went wrong",
            },
            headers=AUTH_HEADERS,
        )
        # 未知节点应 fail visibly 返回 404, 不再静默吞错返 200
        assert resp.status_code == 404


class TestMasterServerTaskManagement:
    @pytest.mark.asyncio
    async def test_submit_task(self, client, master_server):
        await _register_node(client)
        resp = await client.post(
            "/api/tasks/submit",
            json={
                "name": "test-inference",
                "mode": "data",
                "model_name": "llama-3b",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"

    @pytest.mark.asyncio
    async def test_submit_task_pipeline(self, client, master_server):
        await _register_node(client)
        resp = await client.post(
            "/api/tasks/submit",
            json={
                "name": "test-pipeline",
                "mode": "pipeline",
                "model_name": "llama-3b",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"

    @pytest.mark.asyncio
    async def test_submit_task_no_nodes(self, client):
        resp = await client.post(
            "/api/tasks/submit",
            json={
                "name": "test-inference",
                "mode": "data",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_list_tasks(self, client, master_server):
        await _register_node(client)
        await client.post(
            "/api/tasks/submit",
            json={"name": "task1", "mode": "data", "model_name": "m1"},
            headers=AUTH_HEADERS,
        )
        resp = await client.get("/api/tasks", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1

    @pytest.mark.asyncio
    async def test_get_task(self, client, master_server):
        await _register_node(client)
        submit_resp = await client.post(
            "/api/tasks/submit",
            json={"name": "task1", "mode": "data", "model_name": "m1"},
            headers=AUTH_HEADERS,
        )
        task_id = submit_resp.json()["task_id"]
        resp = await client.get(f"/api/tasks/{task_id}", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert resp.json()["name"] == "task1"

    @pytest.mark.asyncio
    async def test_get_task_missing(self, client):
        resp = await client.get("/api/tasks/nonexistent", headers=AUTH_HEADERS)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_cancel_task(self, client, master_server):
        await _register_node(client)
        submit_resp = await client.post(
            "/api/tasks/submit",
            json={"name": "task1", "mode": "data", "model_name": "m1"},
            headers=AUTH_HEADERS,
        )
        task_id = submit_resp.json()["task_id"]
        resp = await client.post(
            f"/api/tasks/{task_id}/cancel",
            json={"reason": "user request"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_cancel_task_missing(self, client):
        resp = await client.post(
            "/api/tasks/nonexistent/cancel",
            json={"reason": "test"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_cancel_completed_task(self, client, master_server):
        await _register_node(client)
        submit_resp = await client.post(
            "/api/tasks/submit",
            json={"name": "task1", "mode": "data", "model_name": "m1"},
            headers=AUTH_HEADERS,
        )
        task_id = submit_resp.json()["task_id"]
        await master_server.master.complete_task(task_id)
        resp = await client.post(
            f"/api/tasks/{task_id}/cancel",
            json={"reason": "too late"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_migrate_task(self, client, master_server):
        await _register_node(client, node_id="n1")
        await _register_node(client, node_id="n2", hostname="mac2", ip_address="10.0.1.2")
        submit_resp = await client.post(
            "/api/tasks/submit",
            json={"name": "task1", "mode": "data", "model_name": "m1"},
            headers=AUTH_HEADERS,
        )
        task_id = submit_resp.json()["task_id"]
        resp = await client.post(f"/api/tasks/{task_id}/migrate", headers=AUTH_HEADERS)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_migrate_task_missing(self, client):
        resp = await client.post("/api/tasks/nonexistent/migrate", headers=AUTH_HEADERS)
        assert resp.status_code == 500

    @pytest.mark.asyncio
    async def test_migrate_task_not_running(self, client, master_server):
        await _register_node(client)
        submit_resp = await client.post(
            "/api/tasks/submit",
            json={"name": "task1", "mode": "data", "model_name": "m1"},
            headers=AUTH_HEADERS,
        )
        task_id = submit_resp.json()["task_id"]
        await master_server.master.complete_task(task_id)
        resp = await client.post(f"/api/tasks/{task_id}/migrate", headers=AUTH_HEADERS)
        assert resp.status_code == 500


class TestMasterServerKVCache:
    @pytest.mark.asyncio
    async def test_kv_register_and_find(self, client, master_server):
        await _register_node(client)
        await client.post(
            "/api/kv/register",
            json={
                "cache_id": "kv1",
                "model_name": "llama-3b",
                "node_id": "n1",
                "size_mb": 256.0,
            },
            headers=AUTH_HEADERS,
        )
        resp = await client.get("/api/kv/find/llama-3b", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert resp.json()["cache_id"] == "kv1"

    @pytest.mark.asyncio
    async def test_kv_find_missing(self, client):
        resp = await client.get("/api/kv/find/unknown-model", headers=AUTH_HEADERS)
        assert resp.status_code == 404


class TestMasterServerStats:
    @pytest.mark.asyncio
    async def test_cluster_stats(self, client, master_server):
        resp = await client.get("/api/cluster/stats", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert "total_nodes" in data
        assert "online_nodes" in data


class TestMasterServerLifecycle:
    @pytest.mark.asyncio
    async def test_server_start_stop(self, master_server):
        assert master_server.master is not None
        assert master_server.app is not None
        master_server.master._running = True
        assert master_server.master._running is True
        master_server.master._running = False
        assert master_server.master._running is False

    @pytest.mark.asyncio
    async def test_stop(self, master_server):
        master_server._uvicorn_server = None
        await master_server.stop()
        assert master_server.master._running is False

    @pytest.mark.asyncio
    async def test_stop_with_server(self, master_server):
        mock_server = type("Server", (), {"should_exit": False})()
        master_server._uvicorn_server = mock_server
        await master_server.stop()
        assert mock_server.should_exit is True
        assert master_server.master._running is False

    @pytest.mark.asyncio
    async def test_start_mocked_uvicorn(self, master_server):
        from unittest.mock import AsyncMock, MagicMock, patch

        mock_config = MagicMock()
        mock_server_instance = MagicMock()
        mock_server_instance.serve = AsyncMock()
        with (
            patch("uvicorn.Config", return_value=mock_config),
            patch("uvicorn.Server", return_value=mock_server_instance),
        ):
            await master_server.start(host="127.0.0.1", port=9999)
        assert mock_server_instance.serve.called
        master_server.master._running = False


# ── 节点审批 API ──


@pytest.fixture
def approval_server():
    master = ClusterMaster(heartbeat_timeout=60.0)
    server = MasterServer(master=master, shared_token=TEST_TOKEN)
    # 保留默认 NodeApprovalManager（不置 None）
    return server


@pytest.fixture
def app2(approval_server):
    return approval_server.app


@pytest.fixture
async def client2(app2):
    transport = ASGITransport(app=app2)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _register_payload(node_id="apprv-1"):
    return {
        "node_id": node_id,
        "hostname": "mac-studio",
        "ip_address": "10.0.1.9",
        "port": 11458,
        "arch": "arm64",
        "total_memory_gb": 128.0,
        "available_memory_gb": 64.0,
        "cpu_cores": 12,
        "gpu_cores": 40,
        "role": "worker",
        "tags": [],
        "active_tasks": 0,
        "max_tasks": 4,
    }


class TestNodeApprovalAPI:
    @pytest.mark.asyncio
    async def test_register_pending_then_approve_then_register_ok(self, client2):
        # 1. 首次注册 -> 403 pending
        resp = await client2.post("/api/nodes/register", json=_register_payload(), headers=AUTH_HEADERS)
        assert resp.status_code == 403
        assert "pending" in resp.json()["detail"]

        # 2. 列出待审批
        resp = await client2.get("/api/nodes/pending", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        pending = resp.json()["pending"]
        assert any(p["node_id"] == "apprv-1" for p in pending)

        # 3. 审批通过
        resp = await client2.post(
            "/api/nodes/approve", json={"node_id": "apprv-1", "approved_by": "admin"}, headers=AUTH_HEADERS
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        # 4. 再次注册 -> 200
        resp = await client2.post("/api/nodes/register", json=_register_payload(), headers=AUTH_HEADERS)
        assert resp.status_code == 200

        # 5. 心跳 -> 200
        resp = await client2.post(
            "/api/nodes/heartbeat",
            json={"node_id": "apprv-1", "available_memory_gb": 60.0, "active_tasks": 0},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_approve_nonexistent_returns_404(self, client2):
        resp = await client2.post("/api/nodes/approve", json={"node_id": "ghost"}, headers=AUTH_HEADERS)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_reject_pending(self, client2):
        await client2.post("/api/nodes/register", json=_register_payload("rej-1"), headers=AUTH_HEADERS)
        resp = await client2.post(
            "/api/nodes/reject", json={"node_id": "rej-1", "reason": "test"}, headers=AUTH_HEADERS
        )
        assert resp.status_code == 200
        assert resp.json()["rejected"] is True

    @pytest.mark.asyncio
    async def test_approve_missing_node_id(self, client2):
        resp = await client2.post("/api/nodes/approve", json={}, headers=AUTH_HEADERS)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_pending_requires_auth(self, client2):
        resp = await client2.get("/api/nodes/pending")
        assert resp.status_code == 401


class TestPrometheusMetrics:
    """S2 /api/v1/metrics — Prometheus exposition 集群聚合指标。"""

    @pytest.mark.asyncio
    async def test_metrics_requires_auth(self, client):
        resp = await client.get("/api/v1/metrics")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_metrics_text_plain(self, client):
        resp = await client.get("/api/v1/metrics", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]

    @pytest.mark.asyncio
    async def test_metrics_exposition_shape(self, master_server, client):
        await _register_node(client, node_id="n1")
        # 植入一个已完成任务带派发延迟 + 一个重试计数
        master = master_server.master
        t = ClusterTask(
            task_id="m1",
            name="probe",
            mode=ParallelMode.DATA,
            model_name="m",
            assigned_nodes=["n1"],
            started_at=100.0,
            completed_at=100.5,
        )
        t.status = TaskStatus.COMPLETED
        t._retry_count = 2
        master.tasks["m1"] = t

        body = (await client.get("/api/v1/metrics", headers=AUTH_HEADERS)).text

        # 关键 metric 必须出现
        for metric in (
            "fusion_cluster_nodes_total",
            "fusion_cluster_nodes_online",
            "fusion_cluster_tasks_total",
            "fusion_cluster_tasks_completed",
            "fusion_cluster_tasks_failed",
            "fusion_cluster_task_retries_total",
            "fusion_cluster_kv_cache_entries",
            "fusion_cluster_dispatch_latency_seconds",
            'fusion_cluster_dispatch_latency_seconds{quantile="0.9"}',
        ):
            assert metric in body, f"metric 缺失: {metric}"
        # 重试计数 = 2
        assert "fusion_cluster_task_retries_total 2" in body
        # HELP/TYPE 注释对齐 exposition 0.0.4
        assert "# HELP fusion_cluster_nodes_total" in body
        assert "# TYPE fusion_cluster_nodes_total gauge" in body

    @pytest.mark.asyncio
    async def test_metrics_empty_cluster(self, client):
        body = (await client.get("/api/v1/metrics", headers=AUTH_HEADERS)).text
        # 空集群: 计数全 0, 延迟分位 0, count 0
        assert "fusion_cluster_nodes_total 0" in body
        assert "fusion_cluster_dispatch_latency_seconds_count 0" in body
        assert 'fusion_cluster_dispatch_latency_seconds{quantile="0.5"} 0.0000' in body

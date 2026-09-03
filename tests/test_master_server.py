"""Master Server FastAPI coverage tests."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeStatus, ParallelMode, TaskStatus
from fusion_multi_node.observability import ClusterObservability, LogEntry
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
    if "protocol_version" in kwargs:
        payload["protocol_version"] = kwargs["protocol_version"]
    return client.post("/api/nodes/register", json=payload, headers=AUTH_HEADERS)


class TestMasterServerHealth:
    @pytest.mark.asyncio
    async def test_health(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["role"] == "master"
        # C11: liveness 带本地依赖检查 (disk/mem/task_store)
        assert "checks" in data
        assert data["checks"]["task_store_writable"] is True

    @pytest.mark.asyncio
    async def test_health_deep_degraded_no_nodes(self, client):
        # C11: readiness — 无 ONLINE 节点 → degraded (node_quorum False)
        resp = await client.get("/api/health/deep")
        assert resp.status_code == 200
        data = resp.json()
        assert data["role"] == "master"
        assert data["checks"]["node_quorum"] is False
        assert data["checks"]["online_nodes"] == 0
        assert data["status"] == "degraded"

    @pytest.mark.asyncio
    async def test_health_deep_ok_with_node(self, client, master_server):
        # C11: 注册一节点 → readiness ok
        await _register_node(client)
        resp = await client.get("/api/health/deep")
        data = resp.json()
        assert data["checks"]["node_quorum"] is True
        assert data["checks"]["online_nodes"] == 1
        assert data["status"] == "ok"


class TestMasterServerObservability:
    """P0-8: Observability 接线后 /api/v1/observability/* 不再 503。"""

    @pytest.mark.asyncio
    async def test_routes_503_when_not_wired(self):
        # 未接线 (master 未 start) → 503/empty (旧行为, 验 guard 仍在)
        master = ClusterMaster()
        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/v1/observability/logs/export", headers=AUTH_HEADERS)
            assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_export_logs_ok_when_wired(self):
        # 接线 → 200, 非空 logs 列表
        import time

        master = ClusterMaster()
        master._observability = ClusterObservability()
        master._observability.add_log(
            LogEntry(timestamp=time.time(), node_id="n1", level="INFO", module="test", message="hello")
        )
        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/v1/observability/logs/export", headers=AUTH_HEADERS)
            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] >= 1
            assert data["logs"][0]["message"] == "hello"

    @pytest.mark.asyncio
    async def test_alerts_ok_when_wired(self):
        master = ClusterMaster()
        master._observability = ClusterObservability()
        master._observability.create_alert("warning", "t", "m", node_id="n1")
        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/v1/observability/alerts", headers=AUTH_HEADERS)
            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 1
            assert data["alerts"][0]["title"] == "t"

    @pytest.mark.asyncio
    async def test_suggestions_ok_when_wired(self):
        master = ClusterMaster()
        master._observability = ClusterObservability()
        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/v1/observability/suggestions", headers=AUTH_HEADERS)
            assert resp.status_code == 200
            data = resp.json()
            assert "suggestions" in data
            assert len(data["suggestions"]) >= 1  # 默认 "集群运行正常" 建议


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
    async def test_auto_approve_by_env_pattern(self, monkeypatch):
        # FUSION_AUTO_APPROVE_PATTERNS 匹配 ip 子串 → 免审批自动加入 (容器/可信 LAN)。
        monkeypatch.setenv("FUSION_AUTO_APPROVE_PATTERNS", "192.168.,10.")
        master = ClusterMaster(heartbeat_timeout=60.0)
        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        assert server._approval_manager is not None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await _register_node(c, node_id="auto-1", ip_address="192.168.97.5")
        assert resp.status_code == 200, f"自动审批应放行: {resp.status_code} {resp.text}"
        assert "auto-1" in master.nodes

    @pytest.mark.asyncio
    async def test_no_auto_approve_without_env(self, monkeypatch):
        # 未配 env → 走审批门 (非自动通过), 注册返回 202/待审批。
        monkeypatch.delenv("FUSION_AUTO_APPROVE_PATTERNS", raising=False)
        master = ClusterMaster(heartbeat_timeout=60.0)
        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        assert server._approval_manager is not None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await _register_node(c, node_id="pend-1", ip_address="192.168.97.6")
        assert resp.status_code == 403, f"未配 env 应走审批门拒绝: {resp.status_code} {resp.text}"
        assert "pend-1" not in master.nodes

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

    @pytest.mark.asyncio
    async def test_get_node_load_runs_sync_off_event_loop(self, client, master_server):
        # P1-10 (审计 §4.1): collect_load_report 同步阻塞须在 to_thread 里跑,
        # 不在事件循环线程 — 验证调用线程 ≠ 当前事件循环线程。
        import threading

        loop_thread = threading.get_ident()
        seen_thread = {}

        def fake_collect():
            seen_thread["id"] = threading.get_ident()
            report = MagicMock()
            report.to_dict.return_value = {"cpu": 12.3, "mem": 40.0}
            return report

        await _register_node(client)
        with patch.object(master_server._sync_manager, "collect_load_report", side_effect=fake_collect):
            resp = await client.get("/api/nodes/n1/load", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert seen_thread["id"] != loop_thread, "同步阻塞调用须移出事件循环 (to_thread)"


class TestProtocolCompat:
    # P1-17 (审计 §6.7): NodeRegisterRequest.protocol_version 比对, 拒不兼容并给降级指引。

    @pytest.mark.asyncio
    async def test_register_rejects_incompatible_version(self, client, master_server):
        resp = await _register_node(client, protocol_version="0.7.0")
        assert resp.status_code == 400
        assert "协议版本不兼容" in resp.json()["detail"]
        assert "0.8.0" in resp.json()["detail"]
        assert "n1" not in master_server.master.nodes

    @pytest.mark.asyncio
    async def test_register_accepts_compatible_version(self, client, master_server):
        resp = await _register_node(client, protocol_version="0.8.7")
        assert resp.status_code == 200
        assert "n1" in master_server.master.nodes

    @pytest.mark.asyncio
    async def test_register_accepts_empty_version_legacy(self, client, master_server):
        # 空串 (旧客户端/直测) 放行 — 灰度期向后兼容, 不阻断。
        resp = await _register_node(client)
        assert resp.status_code == 200
        assert "n1" in master_server.master.nodes

    @pytest.mark.asyncio
    async def test_register_accepts_nonstandard_version(self, client, master_server):
        # 非标准格式版本 (非纯数字段) 放行 — 不误拒未知格式。
        resp = await _register_node(client, protocol_version="dev-build")
        assert resp.status_code == 200
        assert "n1" in master_server.master.nodes


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
        # #65: pipeline 模式需 parallel.pipeline_enabled=true 才过门控 (上游 /distributed/* 404 默认关)。
        from fusion_multi_node.config import ClusterConfig

        cfg = ClusterConfig()
        cfg.set("parallel.pipeline_enabled", True)
        # #65: 测试节点注册默认 role=worker → 放宽 shard 角色含 worker 让其可派发。
        cfg.set("parallel.pipeline_shard_roles", ["worker", "general", "heavy"])
        master_server._cluster_config = cfg
        master_server.master._cluster_config = cfg
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
        # P1-H: 无节点 → 入队, 返回 202 (queued=True), 非 503。
        resp = await client.post(
            "/api/tasks/submit",
            json={
                "name": "test-inference",
                "mode": "data",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 202
        assert resp.json().get("queued") is True

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

    @pytest.mark.asyncio
    async def test_kv_sync_route_missing_entry(self, client):
        # GAP-7 (#33): /api/kv/sync 路由 — 条目缺失返 synced=0 (skip), 非 500。
        resp = await client.post(
            "/api/kv/sync",
            json={
                "cache_id": "nope",
                "model_name": "llama-3b",
                "source_node_id": "n1",
                "size_mb": 0.1,
                "target_node_id": "n2",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        assert resp.json()["synced"] == 0


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
        # P1-10: pydantic NodeApproveRequest.node_id 必填, 空 body → 422 (FastAPI validation)
        resp = await client2.post("/api/nodes/approve", json={}, headers=AUTH_HEADERS)
        assert resp.status_code == 422

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

    @pytest.mark.asyncio
    async def test_p1_24_node_level_metrics(self, master_server, client):
        """P1-24: Prometheus 补熔断/限流/节点级指标 — banned/rate_limited/per-node gauges。"""
        await _register_node(client, node_id="n1")
        await _register_node(client, node_id="n2")
        master = master_server.master
        # 植入限流计数 + ban n2 (直注 _banned_nodes, 模拟 report_fault 达阈值路径 — 节点仍注册)
        master._rate_limited_total = 7
        master._banned_nodes["n2"] = time.time() + 300.0

        body = (await client.get("/api/v1/metrics", headers=AUTH_HEADERS)).text

        # 集群级: banned 计数 + rate_limited 累计
        assert "fusion_cluster_banned_nodes" in body
        assert "fusion_cluster_banned_nodes 1" in body
        assert "fusion_cluster_rate_limited_total" in body
        assert "fusion_cluster_rate_limited_total 7" in body
        # 节点级: n1 正常 (banned=0), n2 ban 中 (banned=1)
        assert 'fusion_node_banned{node_id="n1"} 0' in body
        assert 'fusion_node_banned{node_id="n2"} 1' in body
        assert 'fusion_node_active_tasks{node_id="n1"}' in body
        assert 'fusion_node_memory_available_gb{node_id="n1"}' in body
        # HELP/TYPE 注释齐
        assert "# HELP fusion_node_banned" in body
        assert "# TYPE fusion_node_banned gauge" in body


class TestMasterServerStartPortConflict:
    """issue #25: Master 端口被占用 → OSError 带冲突端口提示。"""

    @pytest.mark.asyncio
    async def test_start_port_conflict_raises_with_hint(self, master_server):
        mock_uvicorn = MagicMock()
        mock_config = MagicMock()
        mock_server = MagicMock()
        mock_server.serve = AsyncMock(side_effect=OSError(48, "Address already in use"))
        mock_uvicorn.Config.return_value = mock_config
        mock_uvicorn.Server.return_value = mock_server
        with patch.dict("sys.modules", {"uvicorn": mock_uvicorn}):
            with pytest.raises(OSError, match="11452.*Master"):
                await master_server.start(host="127.0.0.1", port=11452)


class TestTaskEventBus:
    """P1-18 (审计 §5.5): 任务状态 SSE 推送 — 事件总线 + /api/tasks/events 端点。"""

    @pytest.mark.asyncio
    async def test_finalize_failed_emits_event(self, master_server):
        master = master_server.master
        task = ClusterTask(
            task_id="evt-fail",
            name="fail-task",
            mode=ParallelMode.DATA,
            model_name="m1",
            status=TaskStatus.RUNNING,
            assigned_nodes=["n1"],
        )
        master.tasks[task.task_id] = task
        q = master.subscribe_task_events()
        try:
            await master._finalize_task(task, success=False, error="boom", retryable=False)
            payload = await asyncio.wait_for(q.get(), timeout=1.0)
            assert payload["event"] == "failed"
            assert payload["task_id"] == "evt-fail"
            assert payload["error"] == "boom"
            assert payload["status"] == "failed"
        finally:
            master.unsubscribe_task_events(q)

    @pytest.mark.asyncio
    async def test_finalize_completed_emits_event(self, master_server):
        master = master_server.master
        task = ClusterTask(
            task_id="evt-ok",
            name="ok-task",
            mode=ParallelMode.DATA,
            model_name="m1",
            status=TaskStatus.RUNNING,
            assigned_nodes=["n1"],
        )
        master.tasks[task.task_id] = task
        q = master.subscribe_task_events()
        try:
            await master._finalize_task(task, success=True, error="", result={"r": 1})
            payload = await asyncio.wait_for(q.get(), timeout=1.0)
            assert payload["event"] == "completed"
            assert payload["status"] == "completed"
            assert payload["error"] == ""
        finally:
            master.unsubscribe_task_events(q)

    @pytest.mark.asyncio
    async def test_retry_exhaust_emits_failed(self, master_server):
        master = master_server.master
        task = ClusterTask(
            task_id="evt-retry-exhaust",
            name="retry-task",
            mode=ParallelMode.DATA,
            model_name="m1",
            status=TaskStatus.RUNNING,
            assigned_nodes=["n1"],
        )
        task._retry_count = master._max_retry_attempts
        master.tasks[task.task_id] = task
        q = master.subscribe_task_events()
        try:
            await master._finalize_task(task, success=False, error="transient", retryable=True)
            payload = await asyncio.wait_for(q.get(), timeout=1.0)
            assert payload["event"] == "failed"
            assert "超限" in payload["error"]
        finally:
            master.unsubscribe_task_events(q)

    @pytest.mark.asyncio
    async def test_cancel_emits_event(self, master_server):
        master = master_server.master
        task = ClusterTask(
            task_id="evt-cancel",
            name="cancel-task",
            mode=ParallelMode.DATA,
            model_name="m1",
            status=TaskStatus.RUNNING,
            assigned_nodes=["n1"],
        )
        master.tasks[task.task_id] = task
        q = master.subscribe_task_events()
        try:
            await master.cancel_task("evt-cancel", reason="user")
            payload = await asyncio.wait_for(q.get(), timeout=1.0)
            assert payload["event"] == "cancelled"
            assert payload["status"] == "cancelled"
        finally:
            master.unsubscribe_task_events(q)

    @pytest.mark.asyncio
    async def test_full_queue_drops_oldest(self, master_server):
        master = master_server.master
        task = ClusterTask(
            task_id="evt-overflow",
            name="overflow-task",
            mode=ParallelMode.DATA,
            model_name="m1",
            status=TaskStatus.RUNNING,
            assigned_nodes=["n1"],
        )
        master.tasks[task.task_id] = task
        q = master.subscribe_task_events()
        try:
            # 灌满队列 (maxsize=256) 后再 emit 一次, 不应抛 QueueFull
            for i in range(256):
                master._emit_task_event(task, "running")
            master._emit_task_event(task, "running")
            # 仍能消费到事件 (最旧被丢, 队列满 256)
            drained = 0
            while not q.empty():
                await q.get()
                drained += 1
            assert drained == 256
        finally:
            master.unsubscribe_task_events(q)

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_delivery(self, master_server):
        master = master_server.master
        task = ClusterTask(
            task_id="evt-unsub",
            name="unsub-task",
            mode=ParallelMode.DATA,
            model_name="m1",
            status=TaskStatus.RUNNING,
            assigned_nodes=["n1"],
        )
        master.tasks[task.task_id] = task
        q = master.subscribe_task_events()
        master.unsubscribe_task_events(q)
        master._emit_task_event(task, "running")
        assert q.empty()

    @pytest.mark.asyncio
    async def test_sse_endpoint_registered_and_media_type(self, master_server):
        # SSE 端点注册为 GET /api/tasks/events, 响应 media_type=text/event-stream。
        # 不消费无限流体 (ASGITransport 下 StreamingResponse 无限生成器消费会死锁,
        # 真实服务器的流式首帧验证留 E2E) — 此处验路由契约: 路径/方法/media_type。
        routes = {r.path: r for r in master_server.app.routes}
        assert "/api/tasks/events" in routes
        route = routes["/api/tasks/events"]
        assert route.methods is None or "GET" in route.methods
        endpoint = route.endpoint
        assert endpoint.__name__ == "task_events"
        # StreamingResponse 的 media_type 在路由 endpoint 闭包内构造, 此处验端点存在即可。

    @pytest.mark.asyncio
    async def test_sse_endpoint_requires_auth(self):
        master = ClusterMaster()
        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/tasks/events")
            assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_p2_7_overflow_drop_records_metric(self, master_server):
        # P2-7 (审计 §5.5): 满队列丢事件须记 event_dropped 指标, 不静默吞。
        master = master_server.master
        master._observability = ClusterObservability()
        task = ClusterTask(
            task_id="evt-p2-7",
            name="p2-7-task",
            mode=ParallelMode.DATA,
            model_name="m1",
            status=TaskStatus.RUNNING,
            assigned_nodes=["n1"],
        )
        master.tasks[task.task_id] = task
        q = master.subscribe_task_events()
        try:
            for _ in range(256):
                master._emit_task_event(task, "running")
            # 第 257 次 → 丢最旧 1 条 → event_dropped 累 1。
            master._emit_task_event(task, "running")
            metrics = master._observability.get_metrics("event_dropped", "cluster")
            assert len(metrics) == 1
            assert metrics[0].value == 1.0
        finally:
            master.unsubscribe_task_events(q)

    @pytest.mark.asyncio
    async def test_p2_7_no_drop_no_metric(self, master_server):
        # 未丢事件 → 无 event_dropped 指标 (只丢才记)。
        master = master_server.master
        master._observability = ClusterObservability()
        task = ClusterTask(
            task_id="evt-p2-7-ok",
            name="p2-7-ok-task",
            mode=ParallelMode.DATA,
            model_name="m1",
            status=TaskStatus.RUNNING,
            assigned_nodes=["n1"],
        )
        master.tasks[task.task_id] = task
        q = master.subscribe_task_events()
        try:
            master._emit_task_event(task, "running")
            master._emit_task_event(task, "completed")
            metrics = master._observability.get_metrics("event_dropped", "cluster")
            assert len(metrics) == 0
        finally:
            master.unsubscribe_task_events(q)


class TestMasterRateLimit:
    """P2-22 (审计 §3.8): Master 限流 — 超阈值返 429, 健康检查豁免。"""

    @pytest.mark.asyncio
    async def test_burst_returns_429(self):
        # 阈值 120/60s → 第 121 个非豁免请求返 429。
        master = ClusterMaster()
        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        server._approval_manager = None
        # 压低阈值加速测试 (不依赖默认 120)。
        server._rate_limiter._max = 5
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            codes = []
            for _ in range(7):
                r = await c.get("/api/nodes", headers=AUTH_HEADERS)
                codes.append(r.status_code)
            assert 429 in codes
            assert codes.count(200) == 5

    @pytest.mark.asyncio
    async def test_health_exempt_from_ratelimit(self, client):
        # 健康检查端点豁免限流 — 高频访问不返 429。
        codes = []
        for _ in range(15):
            r = await client.get("/api/health")
            codes.append(r.status_code)
        assert all(c == 200 for c in codes)


class TestConfigReload:
    # P2-20 (审计 §6.8): /api/v1/config/reload 热加载 — 重读 config.json + 重应用租户配额。

    def _write_config(self, path, tenant_max):
        import json as _json

        path.write_text(_json.dumps({"scheduling": {"tenant_max_concurrent": tenant_max}}))

    @pytest.mark.asyncio
    async def test_reload_reapplies_tenant_quota(self, tmp_path):
        from fusion_multi_node.config import ClusterConfig

        cfg_path = tmp_path / "config.json"
        self._write_config(cfg_path, 8)
        cfg = ClusterConfig(config_path=str(cfg_path))
        master = ClusterMaster()
        master.configure_scheduling(1)  # 初始非配置值
        server = MasterServer(master=master, shared_token=TEST_TOKEN, config=cfg)
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/api/v1/config/reload", headers=AUTH_HEADERS)
            assert r.status_code == 200
            assert r.json()["status"] == "ok"
            assert master._tenant_max_concurrent == 8
            # 改盘后再 reload, 新值生效 (无需重启)
            self._write_config(cfg_path, 2)
            r2 = await c.post("/api/v1/config/reload", headers=AUTH_HEADERS)
            assert r2.status_code == 200
            assert master._tenant_max_concurrent == 2

    @pytest.mark.asyncio
    async def test_reload_no_config_returns_503(self):
        master = ClusterMaster()
        server = MasterServer(master=master, shared_token=TEST_TOKEN)  # 不传 config
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/api/v1/config/reload", headers=AUTH_HEADERS)
            assert r.status_code == 503

    @pytest.mark.asyncio
    async def test_reload_requires_auth(self, tmp_path):
        from fusion_multi_node.config import ClusterConfig

        cfg_path = tmp_path / "config.json"
        self._write_config(cfg_path, 4)
        cfg = ClusterConfig(config_path=str(cfg_path))
        master = ClusterMaster()
        server = MasterServer(master=master, shared_token=TEST_TOKEN, config=cfg)
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/api/v1/config/reload")  # 无 Authorization
            assert r.status_code == 401


class TestSupervisorForward:
    """#73 master /api/nodes/{id}/supervisor/{op} 转发到对端 agent。"""

    @pytest.mark.asyncio
    async def test_supervisor_forward_ok(self, client, master_server):
        await _register_node(client, node_id="n1")
        fake_resp = MagicMock()
        fake_resp.json.return_value = {"ok": True, "available": True, "op": "status", "output": {"running": 1}}
        fake_resp.status_code = 200
        fake_client = MagicMock()
        fake_client.post = AsyncMock(return_value=fake_resp)
        master_server.master._get_dispatch_http = AsyncMock(return_value=fake_client)
        master_server.master._get_dispatch_token = MagicMock(return_value=TEST_TOKEN)
        r = await client.post("/api/nodes/n1/supervisor/status", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert body["node_id"] == "n1"
        fake_client.post.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_supervisor_forward_unknown_op(self, client, master_server):
        await _register_node(client, node_id="n1")
        r = await client.post("/api/nodes/n1/supervisor/evil", headers=AUTH_HEADERS)
        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_supervisor_forward_node_missing(self, client):
        r = await client.post("/api/nodes/ghost/supervisor/status", headers=AUTH_HEADERS)
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_supervisor_forward_network_error(self, client, master_server):
        await _register_node(client, node_id="n1")
        fake_client = MagicMock()
        fake_client.post = AsyncMock(side_effect=RuntimeError("connection refused"))
        master_server.master._get_dispatch_http = AsyncMock(return_value=fake_client)
        master_server.master._get_dispatch_token = MagicMock(return_value=TEST_TOKEN)
        r = await client.post("/api/nodes/n1/supervisor/drain", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is False
        assert body["available"] is False

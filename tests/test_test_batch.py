"""#79 测试批次编排协议测试。

验证:
(a) POST /api/test/batches 派发多个 test job → batch_id + assignments。
(b) GET /api/test/batches/{id} 状态派生 (pending/running/completed/partial/failed)。
(c) GET /api/test/batches/{id}/report 聚合 summary (total/passed/failed/running/pending)。
(d) NodeInfo.drivers 字段经注册 + /api/nodes 暴露。
(e) select_nodes driver 匹配 — required_capability 命中 drivers。
(f) test_exec_disabled 默认关 — agent 拒执行。
(g) 404 未知 batch。
(h) 持久化 — batch 元数据 snapshot/restore。

走 ASGITransport (无真 TCP), Bearer 鉴权 TEST_TOKEN。master._dispatch_to_node mock
避免真实 HTTP 派发, 让 _dispatch_data 自然 finalize (状态派生走真实路径)。
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.master import ClusterMaster, NodeInfo
from fusion_multi_node.master.test_batch import TestBatch, TestJob
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-batch-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


def _make_master(tmp_path, host="127.0.0.1", port=11452) -> ClusterMaster:
    m = ClusterMaster(host=host, port=port, heartbeat_timeout=60.0)
    m._task_store_path = tmp_path / "tasks.json"
    m._dispatch_token = TEST_TOKEN
    return m


def _make_server(master: ClusterMaster) -> MasterServer:
    server = MasterServer(master=master, shared_token=TEST_TOKEN)
    server._approval_manager = None
    return server


async def _register_node(master: ClusterMaster, node_id="n1", drivers=None):
    await master.register_node(
        NodeInfo(
            node_id=node_id,
            hostname=f"mac-{node_id}",
            ip_address="10.0.0.1",
            port=11458,
            total_memory_gb=64.0,
            available_memory_gb=48.0,
            cpu_cores=12,
            gpu_cores=30,
            max_tasks=4,
            drivers=drivers or [],
        )
    )


@pytest.fixture
async def env(tmp_path, monkeypatch):
    monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
    m = _make_master(tmp_path)
    s = _make_server(m)
    await _register_node(m, "n1", drivers=["test"])
    client = AsyncClient(transport=ASGITransport(app=s.app), base_url="http://test")
    try:
        yield {"m": m, "s": s, "client": client}
    finally:
        await client.aclose()
        await m.stop()


def _mock_dispatch(master: ClusterMaster, exit_code=0, error=""):
    """Mock _dispatch_to_node 返指定结果, _dispatch_data 自然 finalize。"""

    async def _fake(client, task, nid, nodes_snap, token, **kw):
        if error:
            return {"error": error, "task_id": task.task_id}
        return {"exit_code": exit_code, "success": exit_code == 0, "task_id": task.task_id}

    master._dispatch_to_node = _fake


async def _submit_batch(client, jobs):
    payload = {
        "jobs": jobs,
        "user": "tester",
        "priority": 0,
        "tier": "general",
    }
    return await client.post("/api/test/batches", json=payload, headers=AUTH_HEADERS)


class TestTestBatchSubmit:
    async def test_submit_batch_queues_jobs(self, env):
        _mock_dispatch(env["m"], exit_code=0)
        r = await _submit_batch(
            env["client"],
            [
                {"job_id": "j1", "command": ["echo", "ok"], "required_driver": "test", "timeout": 30},
                {"job_id": "j2", "command": ["echo", "bye"], "required_driver": "test", "timeout": 30},
            ],
        )
        assert r.status_code in (200, 202)
        body = r.json()
        assert "batch_id" in body
        assert body["status"] == "pending"
        assert len(body["assignments"]) == 2
        for a in body["assignments"]:
            assert a["node_id"] == "n1"

    async def test_submit_batch_no_driver_node_queued(self, env):
        # n1 只广告 "test", 要求 "pytest" → 选不到节点 → 入队 (P1-H 节点不足入队, 非 503)。
        r = await _submit_batch(
            env["client"],
            [{"job_id": "j1", "command": ["pytest"], "required_driver": "pytest", "timeout": 30}],
        )
        assert r.status_code == 202
        body = r.json()
        assert body["assignments"][0]["state"] == "pending"
        assert body["assignments"][0]["node_id"] == ""

    async def test_submit_batch_invalid_command_400(self, env):
        # 空命令 list → Pydantic 通过 (list[str] 允许空), 路由 400 校验拦截。
        r = await _submit_batch(
            env["client"],
            [{"job_id": "j1", "command": [], "required_driver": "test", "timeout": 30}],
        )
        assert r.status_code == 400


class TestTestBatchStatus:
    async def test_batch_status_completed(self, env):
        _mock_dispatch(env["m"], exit_code=0)
        r = await _submit_batch(
            env["client"],
            [{"job_id": "j1", "command": ["echo", "ok"], "required_driver": "test", "timeout": 30}],
        )
        batch_id = r.json()["batch_id"]
        # 派发后台 finalize, 轮询至终态。
        status = ""
        for _ in range(20):
            g = await env["client"].get(f"/api/test/batches/{batch_id}", headers=AUTH_HEADERS)
            assert g.status_code == 200
            status = g.json()["status"]
            if status in ("completed", "failed", "partial"):
                break
            import asyncio

            await asyncio.sleep(0.05)
        assert status == "completed"

    async def test_batch_status_failed(self, env):
        _mock_dispatch(env["m"], exit_code=1, error="exit 1")
        r = await _submit_batch(
            env["client"],
            [{"job_id": "j1", "command": ["false"], "required_driver": "test", "timeout": 30}],
        )
        batch_id = r.json()["batch_id"]
        status = ""
        for _ in range(20):
            g = await env["client"].get(f"/api/test/batches/{batch_id}", headers=AUTH_HEADERS)
            status = g.json()["status"]
            if status in ("completed", "failed", "partial"):
                break
            import asyncio

            await asyncio.sleep(0.05)
        assert status == "failed"

    async def test_batch_not_found(self, env):
        g = await env["client"].get("/api/test/batches/nope", headers=AUTH_HEADERS)
        assert g.status_code == 404
        g2 = await env["client"].get("/api/test/batches/nope/report", headers=AUTH_HEADERS)
        assert g2.status_code == 404


class TestTestBatchReport:
    async def test_report_aggregation(self, env):
        # 2 job: 一个 exit 0, 一个 exit 1 → partial, summary passed=1 failed=1。
        call = {"i": 0}

        async def _fake(client, task, nid, nodes_snap, token, **kw):
            call["i"] += 1
            ec = 0 if call["i"] == 1 else 1
            if ec != 0:
                return {"error": "exit 1", "exit_code": ec, "task_id": task.task_id}
            return {"exit_code": ec, "success": True, "task_id": task.task_id}

        env["m"]._dispatch_to_node = _fake
        r = await _submit_batch(
            env["client"],
            [
                {"job_id": "j1", "command": ["echo", "ok"], "required_driver": "test", "timeout": 30},
                {"job_id": "j2", "command": ["false"], "required_driver": "test", "timeout": 30},
            ],
        )
        batch_id = r.json()["batch_id"]
        status = ""
        rep = None
        for _ in range(20):
            g = await env["client"].get(f"/api/test/batches/{batch_id}/report", headers=AUTH_HEADERS)
            assert g.status_code == 200
            rep = g.json()
            status = rep["status"]
            if status in ("completed", "failed", "partial"):
                break
            import asyncio

            await asyncio.sleep(0.05)
        assert status == "partial"
        assert rep["summary"]["total"] == 2
        assert rep["summary"]["passed"] == 1
        assert rep["summary"]["failed"] == 1


class TestNodeDrivers:
    async def test_node_drivers_field_exposed(self, env):
        g = await env["client"].get("/api/nodes", headers=AUTH_HEADERS)
        assert g.status_code == 200
        nodes = g.json()["nodes"]
        n1 = [n for n in nodes if n["node_id"] == "n1"][0]
        assert "test" in n1["drivers"]


class TestSelectNodesDriverMatch:
    async def test_select_nodes_driver_match(self, env):
        # n1 drivers=["test"], job required_capability="test" → 命中。
        nodes = await env["m"].select_nodes(
            mode=None,
            required_memory_gb=1.0,
            count=1,
            required_capability="test",
        )
        assert len(nodes) == 1
        assert nodes[0].node_id == "n1"

    async def test_select_nodes_driver_miss(self, env):
        # required_capability="pytest" → n1 不命中 → 空。
        nodes = await env["m"].select_nodes(
            mode=None,
            required_memory_gb=1.0,
            count=1,
            required_capability="pytest",
        )
        assert len(nodes) == 0


class TestBatchPersistence:
    async def test_batch_snapshot_restore(self, tmp_path):
        m = _make_master(tmp_path)
        batch = TestBatch(
            batch_id="batch_persist1",
            created_at=1000.0,
            owner_master="",
            jobs=[TestJob(job_id="j1", task_id="tjob_1", required_driver="test", created_at=1000.0)],
        )
        snap = batch.to_snapshot()
        restored = TestBatch.from_snapshot(snap)
        assert restored.batch_id == "batch_persist1"
        assert len(restored.jobs) == 1
        assert restored.jobs[0].required_driver == "test"
        await m.stop()


class TestTestExecDisabled:
    async def test_execute_test_disabled_default(self, tmp_path):
        from fusion_multi_node.agent.node_agent import AgentConfig, NodeAgent

        # 默认 test_exec_enabled=False (无 FUSION_NODE_TEST_EXEC env)。
        cfg = AgentConfig(node_id="n_disabled", cluster_token=TEST_TOKEN)
        assert cfg.test_exec_enabled is False
        assert "test" not in cfg.drivers
        agent = NodeAgent(config=cfg)
        result = await agent._execute_test({"task_id": "t1", "params": {"command": ["echo", "ok"]}})
        assert result.get("test_exec_disabled") is True

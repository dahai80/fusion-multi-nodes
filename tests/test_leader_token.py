"""#77 per-leader token 过期写入拒绝测试 (opt-in env)。

验证:
(a) enforce OFF (默认): 即使发过期 X-Leader-Token 也不拒 (离线默认不变)。
(b) enforce ON + HA leader: 正确 token → 提交成功; 过期 token → 409 LeaderChanged。
(c) enforce ON + 单 Master: 永不拒 (enforce 仅 HA 生效)。
(d) enforce ON + HA + 缺 header → 放行 (灰度兼容)。
(e) /api/leader/credentials 返回的 token 与 leader_token() 一致。
(f) active-active: enforce 永关, 不拒。

走 ASGITransport (无真 TCP), Bearer 鉴权携带 TEST_TOKEN。
"""

from __future__ import annotations

import time

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.master import ClusterMaster, NodeInfo, ParallelMode
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-leader-tok"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}

SUBMIT_BODY = {
    "name": "tok-test",
    "mode": "data",
    "model_name": "qwen-1b",
    "prompt": "hi",
    "max_tokens": 8,
}


def _make_master(tmp_path, host="127.0.0.1", port=11452) -> ClusterMaster:
    m = ClusterMaster(host=host, port=port, heartbeat_timeout=60.0)
    m._task_store_path = tmp_path / "tasks.json"
    m._dispatch_token = TEST_TOKEN
    return m


def _make_server(master: ClusterMaster) -> MasterServer:
    server = MasterServer(master=master, shared_token=TEST_TOKEN)
    server._approval_manager = None
    return server


async def _register_node(master: ClusterMaster, node_id="n1"):
    await master.register_node(
        NodeInfo(
            node_id=node_id,
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            total_memory_gb=64.0,
            available_memory_gb=48.0,
            cpu_cores=12,
            gpu_cores=30,
            max_tasks=4,
        )
    )


@pytest.fixture
async def ha_leader(tmp_path, monkeypatch):
    """HA standby leader, enforce ON (env)。"""
    monkeypatch.setenv("FUSION_LEADER_TOKEN_ENFORCE", "1")
    m = _make_master(tmp_path)
    m.setup_election(node_id="master-1", priority=10, known_nodes=[{"node_id": "master-2", "priority": 1}])
    m._is_leader = True
    m._election._leader_id = "master-1"
    m._election.current_term = 2
    s = _make_server(m)
    await _register_node(m, "n1")
    client = AsyncClient(transport=ASGITransport(app=s.app), base_url="http://test")
    try:
        yield {"m": m, "s": s, "client": client}
    finally:
        await client.aclose()
        await m.stop()


class TestLeaderTokenEnforce:
    async def test_enforce_off_accepts_stale_token(self, tmp_path):
        """(a) 默认 enforce OFF: 过期 token 也不拒。"""
        m = _make_master(tmp_path)
        m.setup_election(node_id="master-1", priority=10, known_nodes=[{"node_id": "master-2", "priority": 1}])
        m._is_leader = True
        m._election._leader_id = "master-1"
        m._election.current_term = 2
        s = _make_server(m)
        await _register_node(m, "n1")
        client = AsyncClient(transport=ASGITransport(app=s.app), base_url="http://test")
        try:
            assert m.leader_token_enforce() is False
            hdrs = {**AUTH_HEADERS, "X-Leader-Token": "stale-deadbeef"}
            r = await client.post("/api/tasks/submit", json=SUBMIT_BODY, headers=hdrs)
            assert r.status_code != 409
        finally:
            await client.aclose()
            await m.stop()

    async def test_ha_correct_token_accepted(self, ha_leader):
        """(b) enforce ON + HA: 正确 token → 非 409。"""
        m = ha_leader["m"]
        client = ha_leader["client"]
        assert m.leader_token_enforce() is True
        tok = m.leader_token()
        hdrs = {**AUTH_HEADERS, "X-Leader-Token": tok}
        r = await client.post("/api/tasks/submit", json=SUBMIT_BODY, headers=hdrs)
        assert r.status_code != 409

    async def test_ha_stale_token_rejected_409(self, ha_leader):
        """(b) enforce ON + HA: 过期 token → 409 LeaderChanged。"""
        client = ha_leader["client"]
        hdrs = {**AUTH_HEADERS, "X-Leader-Token": "wrong-token-0000000000000000"}
        r = await client.post("/api/tasks/submit", json=SUBMIT_BODY, headers=hdrs)
        assert r.status_code == 409
        assert "LeaderChanged" in r.json()["detail"]

    async def test_ha_missing_header_accepted(self, ha_leader):
        """(d) enforce ON + 缺 header → 放行 (灰度兼容)。"""
        client = ha_leader["client"]
        r = await client.post("/api/tasks/submit", json=SUBMIT_BODY, headers=AUTH_HEADERS)
        assert r.status_code != 409

    async def test_single_master_enforce_never_rejects(self, tmp_path, monkeypatch):
        """(c) enforce ON env + 单 Master (无 _election): 永不拒。"""
        monkeypatch.setenv("FUSION_LEADER_TOKEN_ENFORCE", "1")
        m = _make_master(tmp_path)
        s = _make_server(m)
        await _register_node(m, "n1")
        client = AsyncClient(transport=ASGITransport(app=s.app), base_url="http://test")
        try:
            assert m.leader_token_enforce() is False
            hdrs = {**AUTH_HEADERS, "X-Leader-Token": "totally-wrong"}
            r = await client.post("/api/tasks/submit", json=SUBMIT_BODY, headers=hdrs)
            assert r.status_code != 409
        finally:
            await client.aclose()
            await m.stop()

    async def test_active_active_enforce_off(self, tmp_path, monkeypatch):
        """(f) active-active: enforce 永关 (无 _election)。"""
        monkeypatch.setenv("FUSION_LEADER_TOKEN_ENFORCE", "1")
        m = _make_master(tmp_path)
        m._ha_mode = "active-active"
        m._ha_node_id = "aa-master-1"
        s = _make_server(m)
        await _register_node(m, "n1")
        client = AsyncClient(transport=ASGITransport(app=s.app), base_url="http://test")
        try:
            assert m.leader_token_enforce() is False
            assert m.leader_epoch() == 0
            assert m.current_leader_id() == "aa-master-1"
        finally:
            await client.aclose()
            await m.stop()

    async def test_leader_credentials_matches_leader_token(self, ha_leader):
        """(e) /api/leader/credentials 返回 token 与 leader_token() 一致。"""
        m = ha_leader["m"]
        client = ha_leader["client"]
        r = await client.get("/api/leader/credentials", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["leader_token"] == m.leader_token()
        assert body["epoch"] == m.leader_epoch()
        assert body["leader_id"] == m.current_leader_id()
        assert body["enforce"] is True

    async def test_cancel_stale_token_rejected_409(self, tmp_path, monkeypatch):
        """cancel 路由亦接 per-leader token 拒绝。"""
        monkeypatch.setenv("FUSION_LEADER_TOKEN_ENFORCE", "1")
        m = _make_master(tmp_path)
        m.setup_election(node_id="master-1", priority=10, known_nodes=[{"node_id": "master-2", "priority": 1}])
        m._is_leader = True
        m._election._leader_id = "master-1"
        m._election.current_term = 2
        s = _make_server(m)
        await _register_node(m, "n1")
        client = AsyncClient(transport=ASGITransport(app=s.app), base_url="http://test")
        # 先放一个任务
        from fusion_multi_node.master import ClusterTask, TaskStatus

        async with m._tasks_lock:
            m.tasks["t-cancel"] = ClusterTask(
                task_id="t-cancel",
                name="tc",
                mode=ParallelMode.DATA,
                model_name="qwen-1b",
                status=TaskStatus.RUNNING,
                created_at=time.time(),
            )
        try:
            hdrs = {**AUTH_HEADERS, "X-Leader-Token": "wrong-token-0000000000000000"}
            r = await client.post("/api/tasks/t-cancel/cancel", json={}, headers=hdrs)
            assert r.status_code == 409
        finally:
            await client.aclose()
            await m.stop()

    async def test_v1_submit_stale_token_rejected_409(self, ha_leader):
        """v1 submit 路由亦接 per-leader token 拒绝。"""
        client = ha_leader["client"]
        hdrs = {**AUTH_HEADERS, "X-Leader-Token": "wrong-token-0000000000000000"}
        r = await client.post("/api/v1/tasks/submit", json=SUBMIT_BODY, headers=hdrs)
        assert r.status_code == 409

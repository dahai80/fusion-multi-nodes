"""#72 fencing token + 权威成员视图 (cluster_view/partitioned)。

验证:
(a) MasterElection.fencing_token 在 _become_leader 单调递增 + 持久化恢复。
(b) NodeAgent 拒过期 master 派发 (incoming token < last → fencing_rejected)。
(c) token 0 (单 master/active-active) 永不拒 (向后兼容)。
(d) /api/nodes partitioned 标志 — 少数派非 leader follower (leader 未知) = True。
(e) /api/nodes cluster_view — 单 master = True, leader = True, follower 近同步 = True。
(f) standby receive_synced_state 后 _last_leader_sync 更新 → cluster_view 转 True。
"""

import time

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.master import ClusterMaster, NodeInfo
from fusion_multi_node.master.election import MasterElection
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-fencing-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


def _make_master(tmp_path, port=11452):
    m = ClusterMaster(host="127.0.0.1", port=port, heartbeat_timeout=60.0)
    m._task_store_path = tmp_path / f"tasks-{port}.json"
    m._election_state_path = tmp_path / f"election-{port}.json"
    m._dispatch_token = TEST_TOKEN
    return m


def _make_server(master):
    s = MasterServer(master=master, shared_token=TEST_TOKEN)
    s._approval_manager = None
    return s


async def _register(master, node_id="n1"):
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


class TestFencingTokenElection:
    @pytest.mark.asyncio
    async def test_fencing_token_increments_on_become_leader(self, tmp_path):
        el = MasterElection(
            node_id="m1",
            priority=10,
            known_nodes=["m2"],
            state_path=tmp_path / "election.json",
        )
        assert el.fencing_token == 0
        await el._become_leader()
        assert el.fencing_token == 1
        await el._become_leader()
        assert el.fencing_token == 2

    @pytest.mark.asyncio
    async def test_fencing_token_persisted_and_restored(self, tmp_path):
        state_path = tmp_path / "election.json"
        el = MasterElection(node_id="m1", priority=10, known_nodes=["m2"], state_path=state_path)
        await el._become_leader()
        await el._become_leader()
        assert el.fencing_token == 2
        el2 = MasterElection(node_id="m1", priority=10, known_nodes=["m2"], state_path=state_path)
        assert el2.fencing_token == 2, "崩溃重启 fencing_token 不回退"

    @pytest.mark.asyncio
    async def test_get_state_includes_fencing_token(self, tmp_path):
        el = MasterElection(node_id="m1", priority=10, known_nodes=["m2"])
        await el._become_leader()
        state = el.get_state()
        assert state["fencing_token"] == 1


class TestAgentFencingReject:
    @pytest.mark.asyncio
    async def test_agent_rejects_lower_token(self):
        agent = NodeAgent(AgentConfig(node_id="n1", cluster_token=TEST_TOKEN))
        agent._last_fencing_token = 5
        task = {"task_id": "t1", "type": "inference", "fencing_token": 3, "model": "qwen-1b", "params": {}}
        r = await agent.execute_task(task)
        assert r.get("fencing_rejected") is True
        assert "stale master" in r.get("error", "")
        assert agent._last_fencing_token == 5, "拒后 last 不降"

    @pytest.mark.asyncio
    async def test_agent_accepts_higher_token(self):
        agent = NodeAgent(AgentConfig(node_id="n1", cluster_token=TEST_TOKEN))
        agent._last_fencing_token = 2
        task = {"task_id": "t2", "type": "inference", "fencing_token": 7, "model": "qwen-1b", "params": {}}
        # 接受更高 token, 进入任务派发 (后续 backend 调用会失败, 但 fencing 不拒)
        r = await agent.execute_task(task)
        assert r.get("fencing_rejected") is not True
        assert agent._last_fencing_token == 7

    @pytest.mark.asyncio
    async def test_token_zero_never_rejects(self):
        agent = NodeAgent(AgentConfig(node_id="n1", cluster_token=TEST_TOKEN))
        agent._last_fencing_token = 99
        # token 0 = 无选举 (单 master/active-active), 永不拒
        task = {"task_id": "t3", "type": "inference", "fencing_token": 0, "model": "qwen-1b", "params": {}}
        r = await agent.execute_task(task)
        assert r.get("fencing_rejected") is not True
        assert agent._last_fencing_token == 99, "token 0 不更新 last"

    @pytest.mark.asyncio
    async def test_token_equal_accepted(self):
        agent = NodeAgent(AgentConfig(node_id="n1", cluster_token=TEST_TOKEN))
        agent._last_fencing_token = 4
        task = {"task_id": "t4", "type": "inference", "fencing_token": 4, "model": "qwen-1b", "params": {}}
        r = await agent.execute_task(task)
        assert r.get("fencing_rejected") is not True


class TestMembershipView:
    @pytest.mark.asyncio
    async def test_single_master_cluster_view_true(self, tmp_path):
        m = _make_master(tmp_path)
        await _register(m)
        view = m.membership_view()
        assert view["cluster_view"] is True
        assert view["partitioned"] is False

    @pytest.mark.asyncio
    async def test_leader_cluster_view_true(self, tmp_path):
        m = _make_master(tmp_path)
        m.setup_election(node_id="m1", priority=10, known_nodes=[{"node_id": "m2"}])
        m._is_leader = True
        view = m.membership_view()
        assert view["cluster_view"] is True
        assert view["partitioned"] is False

    @pytest.mark.asyncio
    async def test_minority_non_leader_partitioned(self, tmp_path):
        m = _make_master(tmp_path)
        m.setup_election(node_id="m2", priority=1, known_nodes=[{"node_id": "m1"}])
        m._is_leader = False
        # leader 未知 (选举空窗 / 少数派) → partitioned
        assert m._election.leader_known is False
        view = m.membership_view()
        assert view["cluster_view"] is False
        assert view["partitioned"] is True

    @pytest.mark.asyncio
    async def test_follower_with_leader_sync_cluster_view_true(self, tmp_path):
        m = _make_master(tmp_path)
        m.setup_election(node_id="m2", priority=1, known_nodes=[{"node_id": "m1"}])
        m._is_leader = False
        # 模拟 leader 已知 + 刚收到状态同步
        m._election._leader_id = "m1"
        await m.receive_synced_state({"nodes": [], "kv_cache": [], "banned_nodes": {}})
        assert m._last_leader_sync > 0
        view = m.membership_view()
        assert view["cluster_view"] is True
        assert view["partitioned"] is False

    @pytest.mark.asyncio
    async def test_follower_stale_sync_partitioned(self, tmp_path):
        m = _make_master(tmp_path)
        m.setup_election(node_id="m2", priority=1, known_nodes=[{"node_id": "m1"}])
        m._is_leader = False
        m._election._leader_id = "m1"
        # 模拟很久没同步 → 超窗口
        m._last_leader_sync = time.time() - 3600
        view = m.membership_view()
        assert view["cluster_view"] is False
        assert view["partitioned"] is True


class TestNodesRoutePartitionedFlag:
    @pytest.mark.asyncio
    async def test_api_nodes_single_master_not_partitioned(self, tmp_path):
        m = _make_master(tmp_path)
        await _register(m)
        s = _make_server(m)
        transport = ASGITransport(app=s.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.get("/api/nodes", headers=AUTH_HEADERS)
            assert r.status_code == 200
            body = r.json()
            assert body["cluster_view"] is True
            assert body["partitioned"] is False

    @pytest.mark.asyncio
    async def test_api_nodes_partitioned_minority(self, tmp_path):
        m = _make_master(tmp_path, port=11453)
        await _register(m)
        m.setup_election(node_id="m2", priority=1, known_nodes=[{"node_id": "m1"}])
        m._is_leader = False
        assert m._election.leader_known is False
        s = _make_server(m)
        transport = ASGITransport(app=s.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.get("/api/nodes", headers=AUTH_HEADERS)
            assert r.status_code == 200
            body = r.json()
            assert body["partitioned"] is True
            assert body["cluster_view"] is False

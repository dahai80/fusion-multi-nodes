"""#76 epoch/leader_id 暴露测试。

验证:
(a) 单 Master: /api/nodes + /api/cluster/stats + /api/v1/nodes + /api/v1/cluster/stats
    返回 epoch=0, leader_id="", is_leader=True。
(b) HA standby leader: setup_election 后 epoch 来自 current_term, leader_id=已当选 leader。
(c) 每节点条目带 epoch/leader_id 与集群级同值。
(d) /api/leader/credentials 返回 epoch/leader_id/leader_token/is_leader/enforce。
(e) current_leader_id 单 Master 返 "" (无 HA 节点 id)。

走 ASGITransport (无真 TCP), Bearer 鉴权携带 TEST_TOKEN。
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.master import ClusterMaster, NodeInfo
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-epoch-token"
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
async def single_master(tmp_path):
    m = _make_master(tmp_path)
    s = _make_server(m)
    await _register_node(m, "n1")
    client = AsyncClient(transport=ASGITransport(app=s.app), base_url="http://test")
    try:
        yield {"m": m, "s": s, "client": client}
    finally:
        await client.aclose()
        await m.stop()


class TestEpochSingleMaster:
    async def test_nodes_returns_epoch_zero_empty_leader(self, single_master):
        r = await single_master["client"].get("/api/nodes", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["epoch"] == 0
        assert body["leader_id"] == ""
        assert body["is_leader"] is True
        # 每节点条目同值
        for entry in body["nodes"]:
            assert entry["epoch"] == 0
            assert entry["leader_id"] == ""

    async def test_cluster_stats_epoch(self, single_master):
        r = await single_master["client"].get("/api/cluster/stats", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["epoch"] == 0
        assert body["leader_id"] == ""
        assert body["is_leader"] is True

    async def test_v1_nodes_epoch(self, single_master):
        r = await single_master["client"].get("/api/v1/nodes", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["epoch"] == 0
        assert body["leader_id"] == ""
        assert body["is_leader"] is True
        for entry in body["nodes"]:
            assert entry["epoch"] == 0
            assert entry["leader_id"] == ""

    async def test_v1_cluster_stats_epoch(self, single_master):
        r = await single_master["client"].get("/api/v1/cluster/stats", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        cluster = body["cluster"]
        assert cluster["epoch"] == 0
        assert cluster["leader_id"] == ""
        assert cluster["is_leader"] is True

    async def test_get_single_node_epoch(self, single_master):
        r = await single_master["client"].get("/api/nodes/n1", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["epoch"] == 0
        assert body["leader_id"] == ""

    async def test_v1_get_single_node_epoch(self, single_master):
        r = await single_master["client"].get("/api/v1/nodes/n1", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["epoch"] == 0
        assert body["leader_id"] == ""

    async def test_leader_credentials_single(self, single_master):
        r = await single_master["client"].get("/api/leader/credentials", headers=AUTH_HEADERS)
        assert r.status_code == 200
        body = r.json()
        assert body["epoch"] == 0
        assert body["leader_id"] == ""
        assert body["is_leader"] is True
        assert body["enforce"] is False
        assert "leader_token" in body and len(body["leader_token"]) == 32


class TestEpochHAMode:
    async def test_ha_leader_epoch_and_leader_id(self, tmp_path):
        m = _make_master(tmp_path)
        m.setup_election(
            node_id="master-1",
            priority=10,
            known_nodes=[{"node_id": "master-2", "priority": 1}],
        )
        # 模拟当选: leader_id 置本节点, current_term > 0
        m._is_leader = True
        m._election._leader_id = "master-1"
        m._election.current_term = 3
        try:
            assert m.leader_epoch() == 3
            assert m.current_leader_id() == "master-1"
        finally:
            await m.stop()

    async def test_ha_standby_epoch_from_synced_term(self, tmp_path):
        m = _make_master(tmp_path)
        m.setup_election(
            node_id="master-2",
            priority=1,
            known_nodes=[{"node_id": "master-1", "priority": 10}],
        )
        # standby: 非 leader, 知晓 leader=master-1, term 同步
        m._is_leader = False
        m._election._leader_id = "master-1"
        m._election.current_term = 5
        try:
            assert m.leader_epoch() == 5
            assert m.current_leader_id() == "master-1"
        finally:
            await m.stop()

    async def test_leader_token_differs_across_epochs(self, tmp_path):
        m = _make_master(tmp_path)
        m.setup_election(node_id="master-1", priority=10, known_nodes=[{"node_id": "master-2", "priority": 1}])
        m._is_leader = True
        m._election._leader_id = "master-1"
        m._election.current_term = 1
        tok1 = m.leader_token()
        m._election.current_term = 2
        tok2 = m.leader_token()
        try:
            assert tok1 != tok2
            assert len(tok1) == 32 and len(tok2) == 32
            # 同 epoch 同 leader_id → 同 token (确定性)
            m._election.current_term = 1
            assert m.leader_token() == tok1
        finally:
            await m.stop()

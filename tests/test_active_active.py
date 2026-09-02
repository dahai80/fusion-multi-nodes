"""#63: Active-Active 双主模式测试 (离线, 无 Redis)。

验证:
(a) 两 master ha.mode=active-active 均接受 submit (无 503 standby 拒)。
(b) M1 提交任务 owner_master=M1, 经 _peer_sync_loop 同步到 M2 为镜像; M2 不重派 (owner-skip)。
(c) 跨节点派发: M1 select_nodes 可见 M2 同步来的节点。
(d) heavy tier 任务亲和 heavy 角色节点。
(e) drain M1 节点 → 新任务跳过该节点。
(f) 任务 ID 前缀含 node_id → 跨 master 唯一。
(g) 回归: standby 模式 (mode=standby) 仍 503 拒提交 (active-active 不破坏旧路径)。

走 PortRoutingTransport (无真 TCP), Bearer TEST_TOKEN, monkeypatch is_safe_peer_host/build_safe_url。
"""

from __future__ import annotations

import asyncio
import time

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.master import (
    ClusterMaster,
    ClusterTask,
    NodeInfo,
    NodeStatus,
    ParallelMode,
    TaskStatus,
)
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-aa-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}

M1_PORT = 11452
M2_PORT = 11453


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 master ASGI app。"""

    def __init__(self, port_to_app: dict[int, object]):
        self._port_to_app = port_to_app
        self._clients: dict[int, AsyncClient] = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test") for p, app in port_to_app.items()
        }

    async def handle_async_request(self, request: Request) -> Response:
        port = request.url.port
        client = self._clients.get(port)
        if client is None:
            return Response(404, text=f"no master for port {port}")
        return await client.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=dict(request.headers),
        )

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_master(tmp_path, port) -> ClusterMaster:
    m = ClusterMaster(host="127.0.0.1", port=port, heartbeat_timeout=60.0)
    m._task_store_path = tmp_path / f"tasks-{port}.json"
    m._election_state_path = tmp_path / f"election-{port}.json"
    m._dispatch_token = TEST_TOKEN
    return m


def _make_server(master: ClusterMaster) -> MasterServer:
    server = MasterServer(master=master, shared_token=TEST_TOKEN)
    server._approval_manager = None
    return server


def _node(node_id: str, role: str = "general", port: int = 11458) -> NodeInfo:
    n = NodeInfo(
        node_id=node_id,
        hostname=node_id,
        ip_address="10.0.0.1",
        port=port,
        total_memory_gb=64.0,
        available_memory_gb=48.0,
        cpu_cores=12,
        gpu_cores=30,
        max_tasks=4,
        role=role,
        status=NodeStatus.ONLINE,
    )
    n.last_heartbeat = time.time()
    return n


async def _register(master: ClusterMaster, node_id: str, role: str = "general", port: int = 11458) -> None:
    await master.register_node(_node(node_id, role, port))


@pytest.fixture
async def aa_pair(tmp_path, monkeypatch):
    """两 master active-active 互连, 跑 _peer_sync_loop 双向同步。"""
    import fusion_multi_node.master.cluster_master as cm_mod

    monkeypatch.setattr(cm_mod, "is_safe_peer_host", lambda host: True)
    monkeypatch.setattr(cm_mod, "build_safe_url", lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}")

    m1 = _make_master(tmp_path, M1_PORT)
    m2 = _make_master(tmp_path, M2_PORT)
    s1 = _make_server(m1)
    s2 = _make_server(m2)

    routing = PortRoutingTransport({M1_PORT: s1.app, M2_PORT: s2.app})

    async def _fake_http():
        return AsyncClient(transport=routing, timeout=10.0)

    monkeypatch.setattr(m1, "_get_dispatch_http", _fake_http)
    monkeypatch.setattr(m2, "_get_dispatch_http", _fake_http)

    await m1.start(
        with_server=False,
        with_mdns=False,
        ha_config={
            "mode": "active-active",
            "node_id": "master-1",
            "peers": [{"node_id": "master-2", "ip_address": "127.0.0.1", "port": M2_PORT}],
            "state_sync_interval": 0.2,
        },
    )
    await m2.start(
        with_server=False,
        with_mdns=False,
        ha_config={
            "mode": "active-active",
            "node_id": "master-2",
            "peers": [{"node_id": "master-1", "ip_address": "127.0.0.1", "port": M1_PORT}],
            "state_sync_interval": 0.2,
        },
    )

    try:
        yield {"m1": m1, "m2": m2, "s1": s1, "s2": s2, "routing": routing}
    finally:
        await m1.stop()
        await m2.stop()
        await routing.aclose()


class TestActiveActive:
    @pytest.mark.asyncio
    async def test_both_masters_accept_submit_no_503(self, aa_pair):
        """(a) 双主均非 standby (_election=None) → 提交不返 503 standby 拒。"""
        m1, m2 = aa_pair["m1"], aa_pair["m2"]
        await _register(m1, "n1")
        await _register(m2, "n2")
        assert m1._election is None
        assert m2._election is None
        assert m1._ha_mode == "active-active"
        assert m1._ha_node_id == "master-1"
        assert m2._ha_node_id == "master-2"
        # submit 到 m1 (有节点 → assign_task 成功, 非 503 standby)
        t = ClusterTask(
            task_id=f"{m1.aa_task_prefix()}-task_aaa",
            name="t",
            mode=ParallelMode.DATA,
            model_name="qwen-1b",
            owner_master=m1.aa_owner(),
            created_at=time.time(),
        )
        ok = await m1.assign_task(t)
        assert ok is True

    @pytest.mark.asyncio
    async def test_task_id_prefix_uniqueness(self, aa_pair):
        """(f) aa_task_prefix 返各自 node_id → 跨 master 任务 ID 不撞。"""
        m1, m2 = aa_pair["m1"], aa_pair["m2"]
        assert m1.aa_task_prefix() == "master-1"
        assert m2.aa_task_prefix() == "master-2"
        assert m1.aa_owner() == "master-1"
        assert m2.aa_owner() == "master-2"

    @pytest.mark.asyncio
    async def test_mirror_task_not_redispatched(self, aa_pair):
        """(b) M1 自有任务同步到 M2 为镜像; M2 assign_task 对非自有任务 owner-skip 返 False。"""
        m1, m2 = aa_pair["m1"], aa_pair["m2"]
        await _register(m1, "n1")
        await _register(m2, "n2")
        # M1 创建自有任务 (owner_master=master-1)
        t = ClusterTask(
            task_id="master-1-task_mirror",
            name="mirror",
            mode=ParallelMode.DATA,
            model_name="qwen-1b",
            owner_master="master-1",
            status=TaskStatus.PENDING,
            created_at=time.time(),
        )
        async with m1._tasks_lock:
            m1.tasks[t.task_id] = t
        # 模拟同步: M2 经 receive_synced_tasks 接收 M1 的任务镜像

        task_dict = m1._task_to_dict(t)
        await m2.receive_synced_tasks([task_dict])
        assert "master-1-task_mirror" in m2.tasks
        assert m2.tasks["master-1-task_mirror"].owner_master == "master-1"
        # M2 对该镜像任务派发 → owner-skip 返 False (非自有, 不重派)
        ok = await m2.assign_task(m2.tasks["master-1-task_mirror"])
        assert ok is False

    @pytest.mark.asyncio
    async def test_owner_wins_not_overwritten(self, aa_pair):
        """(b) owner-wins: M2 自有任务不被 M1 同步快照覆盖。"""
        m1, m2 = aa_pair["m1"], aa_pair["m2"]
        await _register(m1, "n1")
        await _register(m2, "n2")
        # M2 自有任务
        own = ClusterTask(
            task_id="master-2-task_own",
            name="own",
            mode=ParallelMode.DATA,
            model_name="qwen-1b",
            owner_master="master-2",
            status=TaskStatus.RUNNING,
            created_at=time.time(),
        )
        async with m2._tasks_lock:
            m2.tasks[own.task_id] = own
        # M1 推来同名任务 (owner=master-1, RUNNING) — 试图覆盖
        foreign = ClusterTask(
            task_id="master-2-task_own",
            name="foreign",
            mode=ParallelMode.DATA,
            model_name="qwen-1b",
            owner_master="master-1",
            status=TaskStatus.PENDING,
            created_at=time.time(),
        )
        await m2.receive_synced_tasks([m1._task_to_dict(foreign)])
        # M2 自有任务保留 (owner-wins), 不被 PENDING 覆盖
        assert m2.tasks["master-2-task_own"].owner_master == "master-2"
        assert m2.tasks["master-2-task_own"].status == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_peer_sync_propagates_nodes_and_tasks(self, aa_pair):
        """(b)(c) _peer_sync_loop 双向推 nodes+tasks → M2 可见 M1 节点, M1 可见 M2 任务。"""
        m1, m2 = aa_pair["m1"], aa_pair["m2"]
        await _register(m1, "n1")
        await _register(m2, "n2")
        t = ClusterTask(
            task_id="master-1-task_sync",
            name="sync",
            mode=ParallelMode.DATA,
            model_name="qwen-1b",
            owner_master="master-1",
            status=TaskStatus.PENDING,
            created_at=time.time(),
        )
        async with m1._tasks_lock:
            m1.tasks[t.task_id] = t
        # 等若干同步周期 (interval=0.2s)
        await asyncio.sleep(0.8)
        # M2 应已接收 M1 的节点 n1 + 任务
        assert "n1" in m2.nodes
        assert "master-1-task_sync" in m2.tasks
        # M1 应已接收 M2 的节点 n2
        assert "n2" in m1.nodes

    @pytest.mark.asyncio
    async def test_drain_excludes_node_from_new_tasks(self, aa_pair):
        """(e) drain M1 节点 n1 → select_nodes 不选 n1 (in-flight 继续)。"""
        m1 = aa_pair["m1"]
        await _register(m1, "n1")
        await _register(m1, "n2")
        ok = await m1.set_node_draining("n1", True)
        assert ok is True
        assert m1.nodes["n1"].draining is True
        selected = await m1.select_nodes(ParallelMode.DATA, required_memory_gb=1.0, count=1)
        ids = [n.node_id for n in selected]
        assert "n1" not in ids
        assert "n2" in ids

    @pytest.mark.asyncio
    async def test_heavy_tier_affinity(self, aa_pair):
        """(d) heavy tier 任务 → select_nodes 亲和 heavy 角色节点 (软加分, 优先选 heavy)。"""
        m1 = aa_pair["m1"]
        await _register(m1, "general-1", role="general")
        await _register(m1, "heavy-1", role="heavy")
        selected = await m1.select_nodes(ParallelMode.DATA, required_memory_gb=1.0, count=1, task_tier="heavy")
        assert len(selected) == 1
        assert selected[0].node_id == "heavy-1"


class TestStandbyRegression:
    """(g) 回归: standby 模式 (选举配置 + 非 leader) 仍 503 拒提交, active-active 不破坏旧路径。"""

    @pytest.mark.asyncio
    async def test_standby_still_rejects_submit(self, tmp_path):
        m = _make_master(tmp_path, M1_PORT)
        m.setup_election(
            node_id="master-1",
            priority=1,
            known_nodes=[{"node_id": "master-2", "priority": 10, "ip_address": "127.0.0.1", "port": M2_PORT}],
        )
        m._is_leader = False
        t = ClusterTask(
            task_id="t-standby",
            name="t",
            mode=ParallelMode.DATA,
            model_name="qwen-1b",
            created_at=time.time(),
        )
        ok = await m.assign_task(t)
        assert ok is False
        await m.stop()

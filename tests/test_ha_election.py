"""HA 双 Master 选举 + 任务同步测试。

验证:
(a) 两个 master 经 HTTP ASGI 路由完成投票往返。
(b) leader 任务同步到达 standby 本地存储 (receive_synced_tasks 落盘)。
(c) standby 拒绝 assign_task (非 leader 不调度)。
(d) 单 Master 模式 (无选举配置) 不受影响 — _is_leader 默认 True, assign_task 正常。

走 ASGITransport + PortRoutingTransport (无真 TCP), Bearer 鉴权携带 TEST_TOKEN。
"""

import time

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode, TaskStatus
from fusion_multi_node.master.cluster_master import VoteRequest
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-ha-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}

M1_PORT = 11452
M2_PORT = 11453


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 master ASGI app。"""

    def __init__(self, port_to_app: dict[int, object]):
        self._port_to_app = port_to_app
        self._clients: dict[int, AsyncClient] = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
            for p, app in port_to_app.items()
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


def _make_master(tmp_path, host="127.0.0.1", port=M1_PORT) -> ClusterMaster:
    m = ClusterMaster(host=host, port=port, heartbeat_timeout=60.0)
    m._task_store_path = tmp_path / f"tasks-{port}.json"
    m._dispatch_token = TEST_TOKEN
    return m


def _make_server(master: ClusterMaster) -> MasterServer:
    server = MasterServer(master=master, shared_token=TEST_TOKEN)
    server._approval_manager = None
    return server


async def _register_node(master: ClusterMaster, node_id="n1"):
    """经 register_node 注册 (同步 load_router 指标, 供 select_nodes 可见)。"""
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


def _task(tid="t-ha", status=TaskStatus.PENDING) -> ClusterTask:
    return ClusterTask(
        task_id=tid,
        name=f"task-{tid}",
        mode=ParallelMode.DATA,
        model_name="qwen-1b",
        status=status,
        created_at=time.time(),
    )


@pytest.fixture
async def ha_pair(tmp_path, monkeypatch):
    """起两个 master + 两个 server, 经 PortRoutingTransport 互连。"""
    monkeypatch.setattr(
        "fusion_multi_node.master.cluster_master.is_safe_peer_host",
        lambda host: True,
    )
    monkeypatch.setattr(
        "fusion_multi_node.master.cluster_master.build_safe_url",
        lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
    )

    m1 = _make_master(tmp_path, port=M1_PORT)
    m2 = _make_master(tmp_path, port=M2_PORT)
    s1 = _make_server(m1)
    s2 = _make_server(m2)

    port_to_app = {M1_PORT: s1.app, M2_PORT: s2.app}
    routing = PortRoutingTransport(port_to_app)

    async def _fake_http(master):
        return AsyncClient(transport=routing, timeout=10.0)

    monkeypatch.setattr(m1, "_get_dispatch_http", lambda: _fake_http(m1))
    monkeypatch.setattr(m2, "_get_dispatch_http", lambda: _fake_http(m2))

    # 配置选举: m1 高优先级 (leader), m2 低优先级 (standby), 互指对端
    m1.setup_election(
        node_id="master-1",
        priority=10,
        known_nodes=[
            {"node_id": "master-2", "priority": 1, "ip_address": "127.0.0.1", "port": M2_PORT}
        ],
    )
    m2.setup_election(
        node_id="master-2",
        priority=1,
        known_nodes=[
            {"node_id": "master-1", "priority": 10, "ip_address": "127.0.0.1", "port": M1_PORT}
        ],
    )

    try:
        yield {"m1": m1, "m2": m2, "s1": s1, "s2": s2, "routing": routing}
    finally:
        await m1.stop()
        await m2.stop()
        await routing.aclose()


class TestHAVoteRoundTrip:
    @pytest.mark.asyncio
    async def test_vote_http_round_trip(self, ha_pair):
        """(a) m1 (高优先级) 向 m2 (低优先级) 发起拉票 HTTP, m2 返回 vote_granted。"""
        m1 = ha_pair["m1"]
        # m1 高优先级向 m2 拉票: m2 priority=1, 候选人 priority=10 → 满足 >= 条件
        vote_req = VoteRequest(
            term=1,
            candidate_id="master-1",
            candidate_priority=10,
        )
        resp = await m1._send_vote_request_cb(vote_req, "master-2")
        assert resp.voter_id == "master-2"
        assert resp.vote_granted is True

    @pytest.mark.asyncio
    async def test_vote_no_address_peer_skipped(self, tmp_path):
        """无地址对端 (裸字符串 peer) 拉票跳过, 返回 vote_granted=False。"""
        m = _make_master(tmp_path)
        m.setup_election(node_id="master-1", priority=10, known_nodes=[{"node_id": "peer-x"}])
        vote_req = VoteRequest(term=1, candidate_id="master-1", candidate_priority=10)
        resp = await m._send_vote_request_cb(vote_req, "peer-x")
        assert resp.vote_granted is False
        await m.stop()


class TestHATaskSync:
    @pytest.mark.asyncio
    async def test_leader_sync_reaches_standby_store(self, ha_pair, tmp_path):
        """(b) leader (m1) 落盘任务 → _persist_tasks 推送到 standby (m2) → m2 本地存储含该任务。"""
        m1 = ha_pair["m1"]
        m2 = ha_pair["m2"]
        # m1 为 leader, m2 为 standby
        m1._is_leader = True
        m2._is_leader = False
        await _register_node(m1, "n1")
        # m1 放入一个 PENDING 任务并触发 _persist_tasks (含 HA 推送)
        async with m1._tasks_lock:
            m1.tasks["t-sync"] = _task("t-sync", TaskStatus.PENDING)
        await m1._persist_tasks()
        # 等异步推送完成 (best-effort, 同步 await 已在 _persist_tasks 内)
        assert m2._task_store_path.exists()
        # m2 应已合并接收
        assert "t-sync" in m2.tasks
        assert m2.tasks["t-sync"].status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_receive_synced_tasks_idempotent(self, tmp_path):
        """receive_synced_tasks 幂等: 重复推送同任务不重复计数, 终态不覆盖。"""
        m = _make_master(tmp_path)
        task_dicts = [
            {
                "task_id": "t-idem",
                "name": "task-t-idem",
                "mode": "data",
                "model_name": "qwen-1b",
                "status": "pending",
                "created_at": time.time(),
            }
        ]
        n1 = await m.receive_synced_tasks(task_dicts)
        assert n1 == 1
        n2 = await m.receive_synced_tasks(task_dicts)
        assert n2 == 1  # 覆盖同 key, 仍计 1
        assert len(m.tasks) == 1
        await m.stop()

    @pytest.mark.asyncio
    async def test_sync_does_not_overwrite_terminal(self, tmp_path):
        """standby 已有终态任务不被 leader 旧快照覆盖。"""
        m = _make_master(tmp_path)
        # standby 已有 COMPLETED 任务
        async with m._tasks_lock:
            m.tasks["t-done"] = _task("t-done", TaskStatus.COMPLETED)
        # leader 推送同 task_id 的 PENDING 快照
        task_dicts = [
            {
                "task_id": "t-done",
                "name": "task-t-done",
                "mode": "data",
                "model_name": "qwen-1b",
                "status": "pending",
                "created_at": time.time(),
            }
        ]
        await m.receive_synced_tasks(task_dicts)
        assert m.tasks["t-done"].status == TaskStatus.COMPLETED
        await m.stop()


class TestHAStandbyGuard:
    @pytest.mark.asyncio
    async def test_standby_refuses_assign_task(self, ha_pair):
        """(c) standby (非 leader) assign_task 返回 False。"""
        m2 = ha_pair["m2"]
        m2._is_leader = False
        await _register_node(m2, "n1")
        ok = await m2.assign_task(_task("t-refuse"))
        assert ok is False

    @pytest.mark.asyncio
    async def test_standby_submit_route_503(self, ha_pair):
        """standby 提交任务路由返回 503 standby mode。"""
        s2 = ha_pair["s2"]
        m2 = ha_pair["m2"]
        m2._is_leader = False
        transport = ASGITransport(app=s2.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t-route", "model_name": "qwen-1b"},
                headers=AUTH_HEADERS,
            )
            assert resp.status_code == 503
            assert "standby" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_leader_assign_task_works(self, ha_pair):
        """leader assign_task 正常 (不被 standby 守卫拦截)。"""
        m1 = ha_pair["m1"]
        m1._is_leader = True
        await _register_node(m1, "n1")
        # 轻量级模型 (qwen-1b) 走 M4-02 本地强制, 需 preferred_node_id
        task = _task("t-ok")
        task.preferred_node_id = "n1"
        ok = await m1.assign_task(task)
        assert ok is True


class TestSingleMasterUnaffected:
    @pytest.mark.asyncio
    async def test_single_master_assign_task(self, tmp_path):
        """(d) 单 Master 模式 (无选举) _is_leader=True, assign_task 正常。"""
        m = _make_master(tmp_path)
        assert m._election is None
        assert m._is_leader is True
        await _register_node(m, "n1")
        task = _task("t-single")
        task.preferred_node_id = "n1"
        ok = await m.assign_task(task)
        assert ok is True
        await m.stop()

    @pytest.mark.asyncio
    async def test_single_master_vote_rejected(self, tmp_path):
        """单 Master 模式收到拉票请求返回 vote_granted=False。"""
        m = _make_master(tmp_path)
        req = VoteRequest(term=1, candidate_id="other", candidate_priority=5)
        resp = await m.handle_vote_request(req)
        assert resp.vote_granted is False
        await m.stop()

    @pytest.mark.asyncio
    async def test_single_master_sync_no_targets(self, tmp_path):
        """单 Master 模式 _sync_tasks_to_standbys_locked 返回空 (无选举)。"""
        m = _make_master(tmp_path)
        async with m._tasks_lock:
            m.tasks["t1"] = _task("t1")
            targets = m._sync_tasks_to_standbys_locked()
        assert targets == []
        await m.stop()

    @pytest.mark.asyncio
    async def test_single_master_sync_tasks_endpoint(self, tmp_path):
        """单 Master 模式 /api/ha/sync-tasks 仍可接收 (无选举, receive_synced_tasks 直接合并)。"""
        m = _make_master(tmp_path)
        s = _make_server(m)
        transport = ASGITransport(app=s.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/ha/sync-tasks",
                json={"tasks": [{"task_id": "t-ep", "name": "n", "mode": "data", "status": "pending"}]},
                headers=AUTH_HEADERS,
            )
            assert resp.status_code == 200
            assert resp.json()["merged"] == 1
        assert "t-ep" in m.tasks
        await m.stop()


class TestHAVoteEndpoint:
    @pytest.mark.asyncio
    async def test_vote_endpoint_single_master_rejects(self, tmp_path):
        """单 Master /api/ha/vote 返回 vote_granted=False (无选举)。"""
        m = _make_master(tmp_path)
        s = _make_server(m)
        transport = ASGITransport(app=s.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/ha/vote",
                json={"term": 1, "candidate_id": "other", "candidate_priority": 5},
                headers=AUTH_HEADERS,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["vote_granted"] is False
        await m.stop()

    @pytest.mark.asyncio
    async def test_vote_endpoint_with_election(self, tmp_path):
        """配置选举的 master /api/ha/vote 透传到选举管理器。"""
        m = _make_master(tmp_path)
        m.setup_election(node_id="master-1", priority=3)
        s = _make_server(m)
        transport = ASGITransport(app=s.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/ha/vote",
                json={"term": 1, "candidate_id": "master-2", "candidate_priority": 5},
                headers=AUTH_HEADERS,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["vote_granted"] is True
            assert data["voter_id"] == "master-1"
        await m.stop()

    @pytest.mark.asyncio
    async def test_vote_endpoint_bad_payload(self, tmp_path):
        """/api/ha/vote 非法 payload 返回 400。"""
        m = _make_master(tmp_path)
        s = _make_server(m)
        transport = ASGITransport(app=s.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/ha/vote",
                json={"term": "not-an-int"},
                headers=AUTH_HEADERS,
            )
            assert resp.status_code == 400
        await m.stop()

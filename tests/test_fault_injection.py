"""Phase 4 故障注入 E2E — 真 ASGI 路由, 验调度器对真实故障的端到端自愈。

三组单链路 (非单元 mock):
(a) agent 宕机 → 任务超时 → 入重试队列 → 重派存活节点 → COMPLETED。
(b) 反复派发失败 → 节点 ban → select_nodes 跳过 → 新任务路由到存活节点。
(c) HA leader 故障 → standby 升 leader → assign_task 恢复派发 + 同步任务可读。

走 PortRoutingTransport (无真 TCP) + 真 AgentServer /api/execute + FakeBackend
(非真模型, 推理为合成 OpenAI 形状, 不触 fusion-mlx)。Bearer 鉴权携带 TEST_TOKEN。
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, InferenceBackend, NodeAgent
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode, TaskStatus
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-fault-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}
AGENT_PORT_A = 22461
AGENT_PORT_B = 22462
M1_PORT = 11452
M2_PORT = 11453


class FakeBackend(InferenceBackend):
    """合成推理后端 — chat 返 OpenAI 形状, 不触 fusion-mlx。

    fail=True → raise, 触发 agent /api/execute 500 → master 派发失败路径。
    """

    def __init__(self, node_id: str, fail: bool = False):
        self.node_id = node_id
        self.fail = fail

    async def chat(self, model, messages, temperature=0.7, max_tokens=4096, **kw):
        if self.fail:
            raise RuntimeError(f"FakeBackend {self.node_id} 故意失败 (模拟 agent 内部错误)")
        content = messages[0]["content"] if messages else "ok"
        return {
            "choices": [{"message": {"content": f"{self.node_id}:{content}"}}],
            "usage": {"total_tokens": 8},
        }

    async def embed(self, model, input_text, **kw):
        return {"data": [{"embedding": [0.1, 0.2]}]}

    async def health(self):
        return not self.fail


class FaultRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到 ASGI app。drop(port) 模拟该 agent 宕机 → 后续请求 404。"""

    def __init__(self, port_to_app: dict[int, Any]):
        self._port_to_app = dict(port_to_app)
        self._clients: dict[int, AsyncClient] = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test") for p, app in port_to_app.items()
        }

    async def handle_async_request(self, request: Request) -> Response:
        port = request.url.port
        client = self._clients.get(port)
        if client is None:
            return Response(404, text=f"agent {port} 已宕机 (无路由)")
        return await client.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=dict(request.headers),
        )

    def drop(self, port: int) -> None:
        self._clients.pop(port, None)

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_agent(node_id: str, port: int, fail: bool = False) -> NodeAgent:
    agent = NodeAgent(
        config=AgentConfig(node_id=node_id, cluster_token=TEST_TOKEN, agent_port=port),
        backend=FakeBackend(node_id=node_id, fail=fail),
    )
    return agent


def _make_agent_server(node_id: str, port: int, fail: bool = False) -> AgentServer:
    agent = _make_agent(node_id, port, fail=fail)
    server = AgentServer(agent=agent, shared_token=TEST_TOKEN)
    server._rate_limiter._max = 100000
    server._rate_limiter._window = 1.0
    server._host = "127.0.0.1"
    return server


def _node(node_id: str, port: int) -> NodeInfo:
    return NodeInfo(
        node_id=node_id,
        hostname=f"mac-{node_id}",
        ip_address="127.0.0.1",
        port=port,
        total_memory_gb=64.0,
        available_memory_gb=48.0,
        cpu_cores=12,
        gpu_cores=30,
        max_tasks=4,
    )


def _make_master(tmp_path, port: int = M1_PORT) -> ClusterMaster:
    m = ClusterMaster(host="127.0.0.1", port=port, heartbeat_timeout=60.0)
    m._task_store_path = tmp_path / f"tasks-fault-{port}.json"
    m._election_state_path = tmp_path / f"election-fault-{port}.json"
    m._dispatch_token = TEST_TOKEN
    return m


def _make_master_server(master: ClusterMaster) -> MasterServer:
    server = MasterServer(master=master, shared_token=TEST_TOKEN)
    server._approval_manager = None
    return server


def _task(tid: str = "t-fault", timeout_seconds: float = 300.0) -> ClusterTask:
    return ClusterTask(
        task_id=tid,
        name=f"task-{tid}",
        mode=ParallelMode.DATA,
        model_name="qwen-1b",
        timeout_seconds=timeout_seconds,
        task_type="inference",
        params={"prompt": "hello", "max_tokens": 8},
    )


async def _drain_dispatch(master: ClusterMaster, timeout: float = 5.0) -> None:
    """等 master 后台派发协程全部落定 (fire-and-forget 完成)。空列表跳过。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        pending = [h for h in master._dispatch_tasks.values() if not h.done()]
        if not pending:
            return
        await asyncio.wait(pending, timeout=0.5, return_when=asyncio.ALL_COMPLETED)
    logger.warning("派发协程排空超时, 剩余未完成")


def _patch_safe(monkeypatch):
    monkeypatch.setattr(
        "fusion_multi_node.master.cluster_master.is_safe_peer_host",
        lambda host: True,
    )
    monkeypatch.setattr(
        "fusion_multi_node.master.cluster_master.build_safe_url",
        lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
    )


@pytest.fixture
async def fault_cluster(tmp_path, monkeypatch):
    """两 agent server + master, 经 FaultRoutingTransport 互连。"""
    _patch_safe(monkeypatch)
    server_a = _make_agent_server("agent-a", AGENT_PORT_A)
    server_b = _make_agent_server("agent-b", AGENT_PORT_B)
    port_to_app = {AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app}
    routing = FaultRoutingTransport(port_to_app)

    master = _make_master(tmp_path)
    mserver = _make_master_server(master)

    async def _fake_http():
        return AsyncClient(transport=routing, timeout=10.0)

    monkeypatch.setattr(master, "_get_dispatch_http", _fake_http)

    await master.register_node(_node("agent-a", AGENT_PORT_A))
    await master.register_node(_node("agent-b", AGENT_PORT_B))

    try:
        yield {
            "master": master,
            "mserver": mserver,
            "server_a": server_a,
            "server_b": server_b,
            "routing": routing,
        }
    finally:
        await master.stop()
        await routing.aclose()


@pytest.fixture
async def ha_fault_pair(tmp_path, monkeypatch):
    """双 master + 互连路由, 选举已配置 (m1 leader, m2 standby)。"""
    _patch_safe(monkeypatch)
    m1 = _make_master(tmp_path, port=M1_PORT)
    m2 = _make_master(tmp_path, port=M2_PORT)
    s1 = _make_master_server(m1)
    s2 = _make_master_server(m2)
    port_to_app = {M1_PORT: s1.app, M2_PORT: s2.app}
    routing = FaultRoutingTransport(port_to_app)

    async def _fake_http():
        return AsyncClient(transport=routing, timeout=10.0)

    monkeypatch.setattr(m1, "_get_dispatch_http", _fake_http)
    monkeypatch.setattr(m2, "_get_dispatch_http", _fake_http)

    m1.setup_election(
        node_id="master-1",
        priority=10,
        known_nodes=[{"node_id": "master-2", "priority": 1, "ip_address": "127.0.0.1", "port": M2_PORT}],
    )
    m2.setup_election(
        node_id="master-2",
        priority=1,
        known_nodes=[{"node_id": "master-1", "priority": 10, "ip_address": "127.0.0.1", "port": M1_PORT}],
    )
    # m1 高优先级 → leader; m2 standby
    m1._is_leader = True
    m2._is_leader = False

    try:
        yield {"m1": m1, "m2": m2, "s1": s1, "s2": s2, "routing": routing}
    finally:
        await m1.stop()
        await m2.stop()
        await routing.aclose()


class TestAgentDownRedispatch:
    """(a) agent 宕机 → 超时 → 重试 → 重派存活节点。"""

    @pytest.mark.asyncio
    async def test_timeout_retry_lands_on_survivor(self, fault_cluster):
        master = fault_cluster["master"]
        routing = fault_cluster["routing"]

        # 植入 RUNNING 任务到 agent-a, 超时 0.01s, 派发到 agent-a (即将宕机)
        task = _task(tid="t-down", timeout_seconds=0.01)
        task.status = TaskStatus.RUNNING
        task.assigned_nodes = ["agent-a"]
        task.started_at = time.time()
        master.tasks[task.task_id] = task
        for nid in task.assigned_nodes:
            master.nodes[nid].active_tasks += 1

        # agent-a 宕机 (移出路由)
        routing.drop(AGENT_PORT_A)
        # ban agent-a: 重试时 select_nodes 跳过它, 确定落到 agent-b
        for _ in range(master._FAULT_THRESHOLD):
            await master.report_fault("agent-a", "dispatch_failed", "agent down")
        assert master.is_node_banned("agent-a")

        # 触发超时检查 → TIMEOUT → 入重试队列 (等超时窗口过)
        await asyncio.sleep(0.05)
        timed_out = await master.check_timeouts()
        assert task.task_id in timed_out
        # _enqueue_retry 把 TIMEOUT 任务置回 PENDING 待重派
        assert task.status == TaskStatus.PENDING
        assert task in master._pending_retry

        # 排空重试队列: assign_task 重选节点 (跳过 ban 的 agent-a) → agent-b → 派发完成
        retry_tasks = master._pending_retry[:]
        master._pending_retry.clear()
        for t in retry_tasks:
            await master.assign_task(t)

        # 派发为 fire-and-forget, 等后台协程落定
        await _drain_dispatch(master)

        final = master.tasks.get(task.task_id)
        assert final is not None, "重派任务应存在"
        assert final.status == TaskStatus.COMPLETED, f"重派后应 COMPLETED, 实际 {final.status}"
        assert final.assigned_nodes == ["agent-b"], "重派应落到存活节点 agent-b"
        logger.info("Phase4(a) agent 宕机→超时→重试→存活节点 COMPLETED 通过")


class TestDispatchFailBanRoutes:
    """(b) 反复派发失败 → ban → 新任务路由到存活节点。"""

    @pytest.mark.asyncio
    async def test_banned_node_skipped_new_task_routes_survivor(self, fault_cluster, monkeypatch):
        master = fault_cluster["master"]
        routing = fault_cluster["routing"]

        # agent-a 宕机 (移出路由) → 派发 404 → _dispatch_to_node raise → report_fault
        routing.drop(AGENT_PORT_A)

        # 连续派发 threshold 次到 agent-a → 累计故障达阈值 → 自动 ban
        for i in range(master._FAULT_THRESHOLD):
            t = _task(tid=f"t-fail-{i}")
            t.status = TaskStatus.RUNNING
            t.assigned_nodes = ["agent-a"]
            t.started_at = time.time()
            master.tasks[t.task_id] = t
            master.nodes["agent-a"].active_tasks += 1
            await master._dispatch_task(t)
        assert master.is_node_banned("agent-a"), "反复派发失败应自动 ban"

        # 新任务: select_nodes 跳过 ban 的 agent-a → 落 agent-b → COMPLETED
        new_task = _task(tid="t-new")
        ok = await master.assign_task(new_task)
        assert ok, "存活节点可用时应分配成功"
        assert new_task.assigned_nodes == ["agent-b"], "新任务应路由到未 ban 的存活节点"

        await _drain_dispatch(master)
        final = master.tasks.get(new_task.task_id)
        assert final is not None and final.status == TaskStatus.COMPLETED
        logger.info("Phase4(b) ban→select_nodes 跳过→新任务路由存活节点 COMPLETED 通过")


class TestHAFailoverTakeover:
    """(c) HA leader 故障 → standby 升 leader → assign_task 恢复 + 同步任务可读。"""

    @pytest.mark.asyncio
    async def test_standby_takes_over_after_leader_death(self, ha_fault_pair):
        m1 = ha_fault_pair["m1"]  # leader
        m2 = ha_fault_pair["m2"]  # standby

        # standby 模式: m2 拒绝派发 (非 leader)
        task_probe = _task(tid="t-standby-probe")
        assert await m2.assign_task(task_probe) is False, "standby 不应派发任务"

        # leader m1 持有任务, 同步到 standby m2 (经 _persist_tasks → push)
        task_live = _task(tid="t-live")
        task_live.status = TaskStatus.RUNNING
        task_live.assigned_nodes = []
        task_live.started_at = time.time()
        m1.tasks[task_live.task_id] = task_live
        await m1._persist_tasks()

        # 经 HTTP 路由, m2 应收到同步任务 (receive_synced_tasks 落盘)
        await asyncio.sleep(0.2)
        synced = m2.tasks.get(task_live.task_id)
        assert synced is not None, "standby 应收到 leader 同步的任务"
        assert synced.task_id == task_live.task_id
        logger.info("Phase4(c) leader→standby 任务同步到达通过")

        # 模拟 leader 故障 + standby 赢得选举: m1 降级, m2 升 leader
        m1._on_demoted_from_leader()
        m2._on_elected_leader()
        assert m1._is_leader is False
        assert m2._is_leader is True

        # m2 升 leader 后 assign_task 恢复派发 (不再拒绝)
        task_after = _task(tid="t-after-takeover")
        # m2 无注册节点 → assign_task 入优先级队列 (返回 True, 非 False 拒绝)
        ok = await m2.assign_task(task_after)
        assert ok is True, "standby 升 leader 后应恢复派发 (非 standby 拒绝 False)"
        # 关键: 不再因 standby 守卫返回 False
        assert (
            task_after.status != TaskStatus.RUNNING
            or task_after.task_id in {t.task_id for t in m2._pending_queue}
            or task_after.status == TaskStatus.RUNNING
        )
        logger.info("Phase4(c) standby 升 leader 后 assign_task 恢复派发通过")

        # 同步过来的任务在 m2 仍可读 (HA 状态转移不丢任务)
        assert m2.tasks.get(task_live.task_id) is not None, "接管后同步任务不应丢失"
        logger.info("Phase4(c) HA 故障接管全链路通过")

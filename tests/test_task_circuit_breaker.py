"""S1 任务级熔断器测试 — 派发失败累计报告故障达阈值自动 ban, select_nodes 跳过 ban 节点。"""

from __future__ import annotations

import pytest

from fusion_multi_node.master.cluster_master import (
    ClusterMaster,
    ClusterTask,
    NodeInfo,
    NodeStatus,
    ParallelMode,
    TaskStatus,
)


def _node(node_id: str = "n1", **kw) -> NodeInfo:
    base = {
        "node_id": node_id,
        "hostname": f"mac-{node_id}",
        "ip_address": "10.0.0.1",
        "port": 11458,
        "total_memory_gb": 64.0,
        "available_memory_gb": 48.0,
        "cpu_cores": 12,
        "gpu_cores": 30,
    }
    base.update(kw)
    return NodeInfo(**base)


def _task(node_id: str = "n1", task_id: str = "t1") -> ClusterTask:
    return ClusterTask(
        task_id=task_id,
        name="probe",
        mode=ParallelMode.DATA,
        model_name="test-model",
        assigned_nodes=[node_id],
        task_type="inference",
        params={"prompt": "hi", "max_tokens": 4},
    )


def _plant(master: ClusterMaster, task: ClusterTask) -> ClusterTask:
    """直接植入 master.tasks 为 RUNNING — _finalize_task 只回填 RUNNING 任务。"""
    task.status = TaskStatus.RUNNING
    master.tasks[task.task_id] = task
    return task


class _Resp:
    """假 httpx 响应 — 构造非 200 触发派发失败路径。"""

    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict:
        return self._payload


class _FailClient:
    """总是返回 503 的假 AsyncClient — _dispatch_to_node 应报 fault 并 raise。"""

    is_closed = False

    async def post(self, url, json=None, headers=None, timeout=None):
        return _Resp(503, {"status": "error", "detail": "agent down"})


class _OkClient:
    is_closed = False

    async def post(self, url, json=None, headers=None, timeout=None):
        return _Resp(200, {"status": "ok", "result": {"output": "ok"}})


class _ErrClient:
    """C9: agent 内部错误 — 200+ok, 但 result 含 error 键 (OOM/坏模型)。

    _dispatch_to_node 不 raise (status ok), _dispatch_data 识别 logic_fail
    → report_fault("agent_internal_error") + 不可重试 (FAILED 非 PENDING)。
    """

    is_closed = False

    async def post(self, url, json=None, headers=None, timeout=None):
        return _Resp(200, {"status": "ok", "result": {"error": "OOM: 内存不足"}})


class TestDispatchFaultReports:
    """S1 gap1: _dispatch_to_node 失败 → report_fault(node_id) 累计。"""

    @pytest.mark.asyncio
    async def test_dispatch_failure_reports_fault(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        master._dispatch_token = "test-token"
        master._dispatch_http = _FailClient()
        task = _plant(master, _task("n1"))

        await master._dispatch_task(task)

        # C8: 瞬时传输失败 (503) 可重试 → 入重试队列 PENDING (非直接 FAILED)
        assert task.status == TaskStatus.PENDING
        assert task in master._pending_retry, "瞬时派发失败应入重试队列"
        # 单次派发失败 = 1 次故障 (未达阈值 3, 仍可注册)
        assert not master.is_node_banned("n1")
        assert master._fault_counts["n1"], "派发失败应累计故障计数"

    @pytest.mark.asyncio
    async def test_repeated_dispatch_failure_bans_node(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        master._dispatch_token = "test-token"
        master._dispatch_http = _FailClient()
        threshold = master._FAULT_THRESHOLD

        # 连续派发 threshold 次, 每次失败报告一次故障
        for i in range(threshold):
            t = _plant(master, _task("n1", task_id=f"t{i}"))
            await master._dispatch_task(t)

        assert master.is_node_banned("n1"), "派发失败达阈值应自动 ban"

    @pytest.mark.asyncio
    async def test_ok_dispatch_no_fault(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        master._dispatch_token = "test-token"
        master._dispatch_http = _OkClient()

        await master._dispatch_task(_plant(master, _task("n1")))

        assert task_status_ok(master, "t1")
        assert not master._fault_counts.get("n1"), "成功派发不应累计故障"


class TestSelectNodesSkipsBanned:
    """S1 gap2: select_nodes 跳过 ban 期内节点。"""

    @pytest.mark.asyncio
    async def test_select_nodes_excludes_banned(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        await master.register_node(_node("n2", ip_address="10.0.0.2"))
        # ban n1 (达阈值)
        for _ in range(master._FAULT_THRESHOLD):
            await master.report_fault("n1", "dispatch_failed", "x")
        assert master.is_node_banned("n1")

        selected = await master.select_nodes(ParallelMode.DATA, count=1)

        assert all(n.node_id != "n1" for n in selected), "ban 节点不应被选中"
        assert any(n.node_id == "n2" for n in selected), "正常节点仍可被选"

    @pytest.mark.asyncio
    async def test_select_nodes_returns_empty_when_all_banned(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        for _ in range(master._FAULT_THRESHOLD):
            await master.report_fault("n1", "dispatch_failed", "x")

        selected = await master.select_nodes(ParallelMode.DATA, count=1)

        assert selected == [], "全部 ban 应返回空, 调度侧须降级/回退"

    @pytest.mark.asyncio
    async def test_unbanned_node_reselected(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        for _ in range(master._FAULT_THRESHOLD):
            await master.report_fault("n1", "dispatch_failed", "x")
        assert await master.select_nodes(ParallelMode.DATA, count=1) == []

        master.unban_node("n1")
        # 解封解除 ban, 但 report_fault 已将状态置 FAULT; 重新注册恢复 ONLINE (PATCH 语义)
        master.nodes["n1"].status = NodeStatus.ONLINE

        selected = await master.select_nodes(ParallelMode.DATA, count=1)
        assert any(n.node_id == "n1" for n in selected), "解封后应重新可选"


class TestAgentInternalErrorCircuit:
    """C9: agent 内部错误 (200+ok+result.error) 对熔断器可见, 且不可重试。"""

    @pytest.mark.asyncio
    async def test_agent_internal_error_reports_fault(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        master._dispatch_token = "test-token"
        master._dispatch_http = _ErrClient()
        task = _plant(master, _task("n1"))

        await master._dispatch_task(task)

        # C9: agent 逻辑错误 → report_fault(agent_internal_error) 计入故障窗口
        assert master._fault_counts.get("n1"), "agent 内部错误应累计故障"
        assert master.nodes["n1"].status == NodeStatus.FAULT, "agent 内部错误应置节点 FAULT (对熔断器可见)"
        # 逻辑错误不可重试 → 直接 FAILED (非 PENDING)
        assert task.status == TaskStatus.FAILED, "agent 内部错误应 FAILED 非 PENDING 重试"
        assert task not in master._pending_retry, "逻辑错误不应入重试队列"

    @pytest.mark.asyncio
    async def test_agent_internal_error_eventually_bans(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        master._dispatch_token = "test-token"
        master._dispatch_http = _ErrClient()
        threshold = master._FAULT_THRESHOLD

        for i in range(threshold):
            t = _plant(master, _task("n1", task_id=f"err{i}"))
            await master._dispatch_task(t)

        assert master.is_node_banned("n1"), "反复 agent 内部错误达阈值应 ban"

    @pytest.mark.asyncio
    async def test_retry_exhaustion_marks_failed(self):
        """C8 重试耗尽: 瞬时失败重试 _max_retry_attempts 次后 → FAILED (非无限重试)。"""
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        master._dispatch_token = "test-token"
        master._dispatch_http = _FailClient()
        task = _plant(master, _task("n1"))
        task._retry_count = master._max_retry_attempts  # 已耗尽重试预算

        await master._dispatch_task(task)

        assert task.status == TaskStatus.FAILED, "重试耗尽应 FAILED 非 PENDING"
        assert task not in master._pending_retry


def task_status_ok(master: ClusterMaster, task_id: str) -> bool:
    t = master.tasks.get(task_id)
    return t is not None and t.status == TaskStatus.COMPLETED

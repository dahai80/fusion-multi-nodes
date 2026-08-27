"""P1-H 多租户限流/配额/优先级调度测试 — 优先级队列 + 租户配额。

验收: 租户超配额入队 (非拒绝); 高优先级任务先得空闲节点;
任务完成/节点上线排空队列; 取消移除队列任务。

免真模型 — 用 FakeBackend + 真实 dispatch HTTP (PortRoutingTransport)。
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from fusion_multi_node.agent.node_agent import InferenceBackend
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode, TaskStatus

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-token-sched"


class FakeBackend(InferenceBackend):
    def __init__(self, delay: float = 0.0):
        self.delay = delay
        self.call_count = 0

    async def chat(self, model, messages, temperature=0.7, max_tokens=4096, **kwargs):
        self.call_count += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return {"choices": [{"message": {"content": "ok"}}], "usage": {"total_tokens": 1}}

    async def embed(self, model, input_text, **kwargs):
        self.call_count += 1
        return {"data": [{"embedding": [0.1]}]}

    async def health(self):
        return True


async def _drain(master: ClusterMaster) -> None:
    for _ in range(50):
        await asyncio.sleep(0.02)
        if (
            all(
                t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)
                for t in master.tasks.values()
            )
            and not master._pending_queue
        ):
            return
    await asyncio.sleep(0.1)


def _make_task(task_id: str, user: str = "", priority: int = 1) -> ClusterTask:
    return ClusterTask(
        task_id=task_id,
        name=task_id,
        mode=ParallelMode.DATA,
        model_name="qwen-1b",
        user=user,
        priority=priority,
        task_type="inference",
        params={"prompt": "hi", "messages": [], "max_tokens": 8},
    )


def _node(node_id: str, port: int) -> NodeInfo:
    return NodeInfo(
        node_id=node_id,
        hostname=node_id,
        ip_address="127.0.0.1",
        port=port,
        total_memory_gb=64.0,
        available_memory_gb=48.0,
        cpu_cores=12,
        max_tasks=4,
    )


class _HoldResp:
    status_code = 200

    def json(self):
        return {"status": "ok", "result": {"output": "ok"}}

    text = "ok"


class _HoldClient:
    # 派发 HTTP 挂起 (长 sleep 不返) — 保持任务 RUNNING, 避免后台派发失败 finalize 清空计数。
    is_closed = False

    async def post(self, url, json=None, headers=None, timeout=None):
        await asyncio.sleep(30.0)
        return _HoldResp()

    async def aclose(self):
        pass


class TestTenantQuota:
    """租户配额 — 超额入队, 非拒绝。"""

    @pytest.mark.asyncio
    async def test_over_quota_enqueues(self):
        master = ClusterMaster()
        master._dispatch_token = TEST_TOKEN
        master.configure_scheduling(2)  # 每租户 2 并发
        await master.register_node(_node("n1", 21500))

        # 同租户提交 4 个 — 前 2 个派发, 后 2 个入队 (无 backend mock 派发会失败,
        # 但 assign_task 标 RUNNING 后返回 True, 配额按 RUNNING 计)。
        tasks = [_make_task(f"q-{i}", user="tenant-A") for i in range(4)]
        results = []
        for t in tasks:
            results.append(await master.assign_task(t))

        assert all(results), "assign_task 须全部返回 True (入队非拒绝)"
        running = master._running_count_for_user("tenant-A")
        assert running == 2, f"租户 tenant-A RUNNING 应为 2, 实际 {running}"
        queued = [t for t in master._pending_queue if t.user == "tenant-A"]
        assert len(queued) == 2, f"队列应剩 2, 实际 {len(queued)}"

    @pytest.mark.asyncio
    async def test_different_tenants_independent_quota(self):
        master = ClusterMaster()
        master._dispatch_token = TEST_TOKEN
        master.configure_scheduling(1)
        await master.register_node(_node("n1", 21501))

        t_a = _make_task("a-1", user="tenant-A")
        t_b = _make_task("b-1", user="tenant-B")
        ok_a = await master.assign_task(t_a)
        ok_b = await master.assign_task(t_b)

        assert ok_a and ok_b, "不同租户各自配额, 互不阻塞"
        assert master._running_count_for_user("tenant-A") == 1
        assert master._running_count_for_user("tenant-B") == 1
        assert len(master._pending_queue) == 0

    @pytest.mark.asyncio
    async def test_quota_zero_unlimited(self, monkeypatch):
        # SSRF 守卫拦 127.0.0.1 — 测试作用域放行 (与 dispatch E2E 测试一致), 否则后台派发
        # 累 3 fault ban 节点 → select_nodes 返空 → 全入队 (非配额语义验证目标)。
        from fusion_multi_node.master import cluster_master as _cm_mod
        from fusion_multi_node.utils import auth as _auth_mod

        monkeypatch.setattr(_cm_mod, "is_safe_peer_host", lambda host: True)
        # build_safe_url 内部调 auth.is_safe_peer_host (本模块绑定), 须同放行否则 raise ValueError。
        monkeypatch.setattr(_auth_mod, "is_safe_peer_host", lambda host: True)
        master = ClusterMaster()
        master._dispatch_token = TEST_TOKEN
        master.configure_scheduling(0)  # 配额不限 (节点容量仍限)
        await master.register_node(_node("n1", 21502))  # max_tasks=4
        # 派发 HTTP 挂起 (不返) — 保持任务 RUNNING, 避免后台派发失败 finalize 清空计数
        # (assign_task 返 True 后后台 _dispatch_task 并发跑, 无 mock 会失败回填 FAILED →
        # running_count 随调度时序波动; 挂起 client 锁定 RUNNING 计数稳定)。
        master._dispatch_http = _HoldClient()

        tasks = [_make_task(f"u-{i}", user="tenant-Z") for i in range(10)]
        for t in tasks:
            await master.assign_task(t)

        # 配额不限 → 不因配额入队; 节点 max_tasks=4 → 4 RUNNING, 6 因满载入队。
        assert master._running_count_for_user("tenant-Z") == 4, "配额不限, 节点容量限 4 并发"
        assert len(master._pending_queue) == 6, "超节点容量的 6 个入队"


class TestPriorityQueue:
    """优先级队列 — 高优先级先得空闲节点。"""

    @pytest.mark.asyncio
    async def test_high_priority_dispatched_first(self):
        master = ClusterMaster()
        master._dispatch_token = TEST_TOKEN
        master.configure_scheduling(1)  # 单并发, 制造排队
        await master.register_node(_node("n1", 21503))

        # 低优先派发占满唯一并发槽 (priority=1)。
        low = _make_task("low-1", user="t", priority=1)
        await master.assign_task(low)
        assert master._running_count_for_user("t") == 1

        # 三个排队: low(1), high(3), mid(2)。入队顺序故意乱序。
        low2 = _make_task("low-2", user="t", priority=1)
        high = _make_task("high-1", user="t", priority=3)
        mid = _make_task("mid-1", user="t", priority=2)
        await master.assign_task(low2)
        await master.assign_task(high)
        await master.assign_task(mid)

        # 队列按 priority 降序: high(3) > mid(2) > low2(1)
        prios = [t.priority for t in master._pending_queue]
        assert prios == [3, 2, 1], f"队列应为降序 [3,2,1], 实际 {prios}"

        # 完成占槽任务 → 排空, 队首 (high) 应变 RUNNING。
        await master.complete_task(low.task_id)
        assert high.status == TaskStatus.RUNNING, f"高优先级应先派发, 实际 {high.status}"

    @pytest.mark.asyncio
    async def test_drain_on_node_register(self):
        master = ClusterMaster()
        master._dispatch_token = TEST_TOKEN
        master.configure_scheduling(0)

        # 无节点 → 入队。
        t = _make_task("reg-1", user="t", priority=1)
        await master.assign_task(t)
        assert t.task_id in {x.task_id for x in master._pending_queue}

        # 注册节点 → 排空, 任务变 RUNNING。
        await master.register_node(_node("n1", 21504))
        assert t.status == TaskStatus.RUNNING, f"节点上线应排空派发, 实际 {t.status}"
        assert len(master._pending_queue) == 0

    @pytest.mark.asyncio
    async def test_cancel_removes_from_queue(self):
        master = ClusterMaster()
        master._dispatch_token = TEST_TOKEN
        master.configure_scheduling(1)
        await master.register_node(_node("n1", 21505))

        holder = _make_task("hold-1", user="t", priority=1)
        await master.assign_task(holder)
        queued = _make_task("queued-1", user="t", priority=1)
        await master.assign_task(queued)
        assert queued.task_id in {x.task_id for x in master._pending_queue}

        ok = await master.cancel_task(queued.task_id)
        assert ok is True
        assert queued.status == TaskStatus.CANCELLED
        assert queued.task_id not in {x.task_id for x in master._pending_queue}

    @pytest.mark.asyncio
    async def test_cancel_running_drains_queue(self):
        master = ClusterMaster()
        master._dispatch_token = TEST_TOKEN
        master.configure_scheduling(1)
        await master.register_node(_node("n1", 21506))

        holder = _make_task("hold-2", user="t", priority=1)
        await master.assign_task(holder)
        nxt = _make_task("next-1", user="t", priority=2)
        await master.assign_task(nxt)
        assert nxt.task_id in {x.task_id for x in master._pending_queue}

        # 取消占槽任务 → 释放并发槽 → 排空, nxt 变 RUNNING。
        await master.cancel_task(holder.task_id)
        assert nxt.status == TaskStatus.RUNNING, f"取消后队列应排空, 实际 {nxt.status}"

"""F-A12 节点注册幂等 (PATCH 语义) + F-A13 故障黑名单测试 (#20)。"""

from __future__ import annotations

import asyncio

import pytest

from fusion_multi_node.master.cluster_master import ClusterMaster, NodeInfo, NodeStatus


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


class TestRegisterIdempotency:
    """F-A12: 再注册 = PATCH, 保留 Master 权威运行态字段, 更新硬件声明字段。"""

    @pytest.mark.asyncio
    async def test_reregister_preserves_runtime_fields(self):
        master = ClusterMaster()
        await master.register_node(_node("n1", available_memory_gb=48.0, cpu_cores=12, gpu_cores=30))
        # Master 运行态被外部改写 (如派发后 active_tasks 涨)
        master.nodes["n1"].active_tasks = 3
        master.nodes["n1"].max_tasks = 8
        master.nodes["n1"].network_rtt_ms = 12.5
        master.nodes["n1"].status = NodeStatus.BUSY

        # 再注册: 硬件声明字段更新 (cpu 翻倍/可用内存降), 运行态保留
        ok = await master.register_node(
            _node("n1", available_memory_gb=20.0, cpu_cores=24, gpu_cores=40, hostname="mac-renamed")
        )
        assert ok is True
        node = master.nodes["n1"]
        assert node.active_tasks == 3          # 运行态保留
        assert node.max_tasks == 8             # 运行态保留
        assert node.network_rtt_ms == 12.5     # 运行态保留
        assert node.status == NodeStatus.BUSY  # 非 OFFLINE 运行态保留
        assert node.cpu_cores == 24            # 硬件声明更新
        assert node.gpu_cores == 40            # 硬件声明更新
        assert node.available_memory_gb == 20.0
        assert node.hostname == "mac-renamed"

    @pytest.mark.asyncio
    async def test_reregiver_offline_recovers_online(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        master.nodes["n1"].status = NodeStatus.OFFLINE
        ok = await master.register_node(_node("n1"))
        assert ok is True
        assert master.nodes["n1"].status == NodeStatus.ONLINE

    @pytest.mark.asyncio
    async def test_new_register_returns_true(self):
        master = ClusterMaster()
        ok = await master.register_node(_node("n1"))
        assert ok is True
        assert "n1" in master.nodes


class TestFaultBlacklist:
    """F-A13: report_fault 窗口内达阈值 → ban; ban 内拒绝注册; 到期自动解封。"""

    @pytest.mark.asyncio
    async def test_report_fault_marks_fault(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        ok = await master.report_fault("n1", "oom", "out of memory")
        assert ok is True
        assert master.nodes["n1"].status == NodeStatus.FAULT

    @pytest.mark.asyncio
    async def test_fault_threshold_triggers_ban(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        threshold = master._FAULT_THRESHOLD
        for i in range(threshold - 1):
            await master.report_fault("n1", "oom", f"fault {i}")
            assert not master.is_node_banned("n1"), f"第 {i + 1} 次不应 ban"
        await master.report_fault("n1", "oom", "fatal")
        assert master.is_node_banned("n1")

    @pytest.mark.asyncio
    async def test_banned_node_register_rejected(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        for _ in range(master._FAULT_THRESHOLD):
            await master.report_fault("n1", "oom", "x")
        assert master.is_node_banned("n1")
        ok = await master.register_node(_node("n1", hostname="mac-reboot"))
        assert ok is False
        # ban 仍生效
        assert master.is_node_banned("n1")

    @pytest.mark.asyncio
    async def test_unban_node_manual(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        for _ in range(master._FAULT_THRESHOLD):
            await master.report_fault("n1", "oom", "x")
        assert master.is_node_banned("n1")
        removed = master.unban_node("n1")
        assert removed is True
        assert not master.is_node_banned("n1")
        ok = await master.register_node(_node("n1"))
        assert ok is True

    @pytest.mark.asyncio
    async def test_unregister_with_ban_reason(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        await master.unregister_node("n1", reason="banned")
        assert master.is_node_banned("n1")
        ok = await master.register_node(_node("n1"))
        assert ok is False

    @pytest.mark.asyncio
    async def test_unregister_no_ban_without_reason(self):
        master = ClusterMaster()
        await master.register_node(_node("n1"))
        await master.unregister_node("n1")
        assert not master.is_node_banned("n1")
        ok = await master.register_node(_node("n1"))
        assert ok is True

    @pytest.mark.asyncio
    async def test_fault_window_decay(self):
        master = ClusterMaster()
        # 窗口外旧故障应被惰性清理, 不累积到阈值
        master._FAULT_WINDOW_S = 0.05
        await master.register_node(_node("n1"))
        await master.report_fault("n1", "oom", "old1")
        await master.report_fault("n1", "oom", "old2")
        await asyncio.sleep(0.1)  # 窗口过期
        await master.report_fault("n1", "oom", "new1")
        assert not master.is_node_banned("n1"), "窗口外故障不应累积"

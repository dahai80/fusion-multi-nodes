"""Cluster Master 测试。

测试 ClusterMaster、NodeInfo、ClusterTask、KVCacheEntry 等。
用户指令：要求测试覆盖率90%+。
No external API callers — these are internal unit tests.
Data schemas: NodeInfo, ClusterTask, KVCacheEntry dataclasses from cluster_master module.
"""

import asyncio
import time

import pytest

from fusion_multi_node.master.cluster_master import (
    ClusterMaster,
    ClusterTask,
    KVCacheEntry,
    NodeInfo,
    NodeStatus,
    ParallelMode,
    TaskStatus,
)


class TestNodeStatus:
    def test_values(self):
        assert NodeStatus.ONLINE.value == "online"
        assert NodeStatus.OFFLINE.value == "offline"
        assert NodeStatus.BUSY.value == "busy"
        assert NodeStatus.ERROR.value == "error"


class TestTaskStatus:
    def test_values(self):
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
        assert TaskStatus.MIGRATED.value == "migrated"
        assert TaskStatus.TIMEOUT.value == "timeout"


class TestNodeInfo:
    def test_basic(self):
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        assert info.node_id == "n1"
        assert info.status == NodeStatus.OFFLINE
        assert info.active_tasks == 0
        assert info.max_tasks == 4

    def test_score_high_resources(self):
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            total_memory_gb=64.0,
            available_memory_gb=50.0,
            active_tasks=0,
            max_tasks=4,
            network_rtt_ms=5.0,
        )
        score = info.score
        assert 0 < score <= 1

    def test_score_low_resources(self):
        info = NodeInfo(
            node_id="n2",
            hostname="mac2",
            ip_address="10.0.0.2",
            port=11458,
            total_memory_gb=64.0,
            available_memory_gb=5.0,
            active_tasks=4,
            max_tasks=4,
            network_rtt_ms=50.0,
        )
        score = info.score
        assert 0 <= score <= 1


class TestClusterTask:
    def test_basic(self):
        task = ClusterTask(task_id="t1", name="inference", mode=ParallelMode.PIPELINE)
        assert task.status == TaskStatus.PENDING
        assert task.model_name == ""
        assert task.assigned_nodes == []

    def test_with_model(self):
        task = ClusterTask(
            task_id="t1",
            name="inference",
            mode=ParallelMode.DATA,
            model_name="llama",
            assigned_nodes=["n1", "n2"],
        )
        assert task.mode == ParallelMode.DATA
        assert len(task.assigned_nodes) == 2


class TestKVCacheEntry:
    def test_basic(self):
        entry = KVCacheEntry(
            cache_id="c1",
            model_name="test",
            node_id="n1",
            created_at=time.time(),
            size_mb=100.0,
        )
        assert entry.cache_id == "c1"
        assert entry.access_count == 0
        assert entry.ttl_seconds == 3600.0


class TestClusterMaster:
    def test_init(self):
        master = ClusterMaster()
        assert len(master.nodes) == 0
        assert len(master.tasks) == 0
        assert len(master.kv_cache) == 0

    @pytest.mark.asyncio
    async def test_start_stop_no_server(self):
        master = ClusterMaster()
        await master.start(with_server=False, with_mdns=False)
        assert master._running is True
        await master.stop()
        assert master._running is False

    @pytest.mark.asyncio
    async def test_register_node(self):
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        await master.register_node(info)
        assert "n1" in master.nodes
        assert master.nodes["n1"].status == NodeStatus.ONLINE

    @pytest.mark.asyncio
    async def test_unregister_node(self):
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        await master.register_node(info)
        await master.unregister_node("n1")
        assert "n1" not in master.nodes

    @pytest.mark.asyncio
    async def test_get_online_nodes_empty(self):
        master = ClusterMaster()
        nodes = await master.get_online_nodes()
        assert len(nodes) == 0

    @pytest.mark.asyncio
    async def test_get_online_nodes(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        nodes = await master.get_online_nodes()
        assert len(nodes) == 1

    @pytest.mark.asyncio
    async def test_assign_task(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        ok = await master.assign_task(task)
        assert ok is True
        assert "t1" in master.tasks

    @pytest.mark.asyncio
    async def test_assign_task_no_nodes(self):
        # P1-H: 无可用节点 → 入优先级队列 (非 503), 返回 True, PENDING 待节点上线。
        master = ClusterMaster()
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE)
        ok = await master.assign_task(task)
        assert ok is True
        assert task.status == TaskStatus.PENDING
        assert task.task_id in {t.task_id for t in master._pending_queue}

    @pytest.mark.asyncio
    async def test_assign_task_toctou_backfill(self):
        """select_nodes 锁外执行 → 并发抢占首选节点满载 → 锁内补选其它空闲节点。
        模拟: 首选 n1 被并发抢满 (active_tasks=max_tasks), n2/n3 仍空闲,
        assign_task 不应 503, 而补选到空闲节点。
        """
        master = ClusterMaster()
        # n1 满载 (模拟并发抢占后), n2/n3 空闲且评分相近
        for nid, active in (("n1", 4), ("n2", 0), ("n3", 0)):
            info = NodeInfo(
                node_id=nid,
                hostname=f"mac{nid}",
                ip_address=f"10.0.0.{nid[-1]}",
                port=11458,
                status=NodeStatus.ONLINE,
                available_memory_gb=50.0,
                total_memory_gb=64.0,
                active_tasks=active,
                max_tasks=4,
                last_heartbeat=time.time(),
            )
            await master.register_node(info)
        # PIPELINE 单分片 → select_nodes 按 score 排序选 n1 (score 最高, 因 active 不参与 PIPELINE 排序)
        # 锁内 reconfirm 发现 n1 满 → 补选 n2 或 n3
        task = ClusterTask(task_id="t-toctou", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        ok = await master.assign_task(task)
        assert ok is True
        # 派发到的不应是满载的 n1
        assert "n1" not in task.assigned_nodes
        assert len(task.assigned_nodes) == 1
        assert task.assigned_nodes[0] in ("n2", "n3")

    @pytest.mark.asyncio
    async def test_complete_task(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        await master.assign_task(task)
        await master.complete_task("t1")
        assert master.tasks["t1"].status == TaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_complete_task_with_error_sets_failed(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        await master.assign_task(task)
        await master.complete_task("t1", error="OOM")
        assert master.tasks["t1"].status == TaskStatus.FAILED
        assert master.tasks["t1"].error == "OOM"

    @pytest.mark.asyncio
    async def test_migrate_task(self):
        master = ClusterMaster()
        info1 = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        info2 = NodeInfo(
            node_id="n2",
            hostname="mac2",
            ip_address="10.0.0.2",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=60.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info1)
        await master.register_node(info2)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        await master.assign_task(task)
        ok = await master.migrate_task("t1")
        assert ok is True
        assert master.tasks["t1"].status == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_migrate_task_not_found(self):
        master = ClusterMaster()
        ok = await master.migrate_task("nope")
        assert ok is False

    @pytest.mark.asyncio
    async def test_check_heartbeat(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        ok = await master.check_heartbeat("n1")
        assert ok is True

    @pytest.mark.asyncio
    async def test_check_heartbeat_stale_sets_offline(self):
        master = ClusterMaster(heartbeat_timeout=5.0)
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        await master.register_node(info)
        master.nodes["n1"].last_heartbeat = time.time() - 100
        ok = await master.check_heartbeat("n1")
        assert ok is False
        assert master.nodes["n1"].status == NodeStatus.OFFLINE

    @pytest.mark.asyncio
    async def test_check_heartbeat_missing_node(self):
        master = ClusterMaster()
        ok = await master.check_heartbeat("nope")
        assert ok is False

    @pytest.mark.asyncio
    async def test_check_timeouts(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        task = ClusterTask(
            task_id="t1",
            name="infer",
            mode=ParallelMode.PIPELINE,
            model_name="test",
            timeout_seconds=0.01,
        )
        await master.assign_task(task)
        master.tasks["t1"].status = TaskStatus.RUNNING
        master.tasks["t1"].started_at = time.time() - 1
        timed_out = await master.check_timeouts()
        assert "t1" in timed_out
        assert master.tasks["t1"].status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_select_nodes(self):
        master = ClusterMaster()
        info1 = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        info2 = NodeInfo(
            node_id="n2",
            hostname="mac2",
            ip_address="10.0.0.2",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=60.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info1)
        await master.register_node(info2)
        selected = await master.select_nodes(ParallelMode.PIPELINE, required_memory_gb=10.0, count=2)
        assert len(selected) == 2

    @pytest.mark.asyncio
    async def test_select_nodes_insufficient(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=5.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        selected = await master.select_nodes(ParallelMode.PIPELINE, required_memory_gb=50.0, count=1)
        assert len(selected) == 0

    @pytest.mark.asyncio
    async def test_register_kv_cache(self):
        master = ClusterMaster()
        entry = KVCacheEntry(
            cache_id="c1",
            model_name="test",
            node_id="n1",
            created_at=time.time(),
            size_mb=100.0,
        )
        await master.register_kv_cache(entry)
        assert "c1" in master.kv_cache

    @pytest.mark.asyncio
    async def test_find_kv_cache(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        entry = KVCacheEntry(
            cache_id="c1",
            model_name="test",
            node_id="n1",
            created_at=time.time(),
            size_mb=100.0,
        )
        await master.register_kv_cache(entry)
        found = await master.find_kv_cache("test")
        assert found is not None
        assert found.cache_id == "c1"

    @pytest.mark.asyncio
    async def test_find_kv_cache_missing(self):
        master = ClusterMaster()
        found = await master.find_kv_cache("nope")
        assert found is None

    @pytest.mark.asyncio
    async def test_find_kv_cache_offline_node_skips(self):
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        await master.register_node(info)
        master.nodes["n1"].status = NodeStatus.OFFLINE
        entry = KVCacheEntry(
            cache_id="c1",
            model_name="test",
            node_id="n1",
            created_at=time.time(),
            size_mb=100.0,
        )
        await master.register_kv_cache(entry)
        found = await master.find_kv_cache("test")
        assert found is None
        assert "c1" in master.kv_cache

    @pytest.mark.asyncio
    async def test_find_kv_cache_expired(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        entry = KVCacheEntry(
            cache_id="c1",
            model_name="test",
            node_id="n1",
            created_at=time.time() - 5000,
            size_mb=100.0,
            ttl_seconds=1.0,
        )
        await master.register_kv_cache(entry)
        found = await master.find_kv_cache("test")
        assert found is None
        assert "c1" not in master.kv_cache

    @pytest.mark.asyncio
    async def test_get_stats(self):
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        await master.register_node(info)
        stats = await master.get_stats()
        assert "total_nodes" in stats
        assert "online_nodes" in stats

    @pytest.mark.asyncio
    async def test_start_mdns(self):
        from unittest.mock import MagicMock, patch

        master = ClusterMaster()
        # mock 真实 Zeroconf，避免后台线程事件循环与 close() 竞态
        # （Python 3.14 + zeroconf 0.150：_async_setup 未完成即 close → RuntimeError Event loop is closed）
        mock_zc = MagicMock()
        with (
            patch("zeroconf.Zeroconf", return_value=mock_zc),
            patch("zeroconf.ServiceInfo"),
        ):
            master._start_mdns()
            master._stop_mdns()
            mock_zc.register_service.assert_called_once()
            mock_zc.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_online_nodes_stale_excluded_pure_snapshot(self):
        # R6: get_online_nodes 纯快照, 不改状态。stale 节点被排除但状态保持 ONLINE。
        master = ClusterMaster(heartbeat_timeout=0.01)
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        await master.register_node(info)
        master.nodes["n1"].last_heartbeat = time.time() - 100
        online = await master.get_online_nodes()
        assert len(online) == 0
        # 读操作不应有副作用 — 状态仍 ONLINE, 跃迁交给 _refresh_node_statuses
        assert master.nodes["n1"].status == NodeStatus.ONLINE

    @pytest.mark.asyncio
    async def test_refresh_node_statuses_stale_goes_offline(self):
        # R6: 状态跃迁统一在 _refresh_node_statuses (health loop 调用), 非读路径。
        master = ClusterMaster(heartbeat_timeout=0.01)
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        await master.register_node(info)
        master.nodes["n1"].last_heartbeat = time.time() - 100
        await master._refresh_node_statuses()
        assert master.nodes["n1"].status == NodeStatus.OFFLINE

    @pytest.mark.asyncio
    async def test_refresh_node_statuses_busy_to_online(self):
        # R6: BUSY 节点 active_tasks 下降时恢复 ONLINE。
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        info.status = NodeStatus.BUSY
        info.active_tasks = 0
        info.max_tasks = 4
        await master.register_node(info)
        master.nodes["n1"].last_heartbeat = time.time()
        await master._refresh_node_statuses()
        assert master.nodes["n1"].status == NodeStatus.ONLINE

    @pytest.mark.asyncio
    async def test_complete_task_decrements_active(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="10.0.0.1",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        await master.assign_task(task)
        assert master.nodes["n1"].active_tasks >= 1
        await master.complete_task("t1")
        assert master.nodes["n1"].active_tasks == 0

    @pytest.mark.asyncio
    async def test_complete_task_missing(self):
        master = ClusterMaster()
        await master.complete_task("nope")

    @pytest.mark.asyncio
    async def test_health_check_loop(self):
        master = ClusterMaster()
        master._running = True
        task = asyncio.create_task(master._health_check_loop())
        await asyncio.sleep(0.05)
        master._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def test_estimate_memory_70b(self):
        master = ClusterMaster()
        task = ClusterTask(
            task_id="t1",
            name="infer",
            mode=ParallelMode.PIPELINE,
            model_name="llama-70b",
        )
        mem = master._estimate_memory(task)
        assert mem > 30.0

    def test_estimate_memory_13b(self):
        master = ClusterMaster()
        task = ClusterTask(
            task_id="t1",
            name="infer",
            mode=ParallelMode.PIPELINE,
            model_name="llama-13b",
        )
        mem = master._estimate_memory(task)
        assert mem > 10.0

    def test_estimate_memory_8b(self):
        master = ClusterMaster()
        task = ClusterTask(
            task_id="t1",
            name="infer",
            mode=ParallelMode.PIPELINE,
            model_name="llama-8b",
        )
        mem = master._estimate_memory(task)
        assert mem > 6.0

    def test_estimate_memory_3b(self):
        master = ClusterMaster()
        task = ClusterTask(
            task_id="t1",
            name="infer",
            mode=ParallelMode.PIPELINE,
            model_name="llama-3b",
        )
        mem = master._estimate_memory(task)
        assert mem > 4.0

    def test_estimate_memory(self):
        master = ClusterMaster()
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        mem = master._estimate_memory(task)
        assert isinstance(mem, float)
        assert mem >= 0

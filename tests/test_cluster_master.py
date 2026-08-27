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
    async def test_start_wires_observability(self):
        # P0-8: start() 接线 _observability (路由经此读, 原恒 None → 503)
        master = ClusterMaster()
        assert master._observability is None
        await master.start(with_server=False, with_mdns=False)
        assert master._observability is not None
        await master.stop()

    @pytest.mark.asyncio
    async def test_collect_observability_records_metrics(self):
        # P0-8: 注册一节点 → _collect_observability_locked 记录 mem_used_gb/active_tasks
        master = ClusterMaster()
        await master.start(with_server=False, with_mdns=False)
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458,
                        total_memory_gb=64.0, available_memory_gb=48.0)
        await master.register_node(info)
        await master._collect_observability_locked()
        obs = master._observability
        latest_mem = obs.get_latest_metric("mem_used_gb", node_id="n1")
        latest_tasks = obs.get_latest_metric("active_tasks", node_id="cluster")
        assert latest_mem is not None
        assert latest_mem.value == 16.0  # 64 - 48
        assert latest_tasks is not None
        assert latest_tasks.value == 0.0
        await master.stop()

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
    async def test_select_nodes_exclude_blacklist(self):
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
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info1)
        await master.register_node(info2)
        # #31 exclude_nodes 硬黑名单: n1 规避, 只剩 n2
        selected = await master.select_nodes(
            ParallelMode.DATA,
            required_memory_gb=10.0,
            count=1,
            exclude_nodes=["n1"],
        )
        assert len(selected) == 1
        assert selected[0].node_id == "n2"

    @pytest.mark.asyncio
    async def test_select_nodes_exclude_all_empty(self):
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
        await master.register_node(info1)
        # #31 全部节点规避 → 无候选, 返回空 (不回退到黑名单节点)
        selected = await master.select_nodes(
            ParallelMode.DATA,
            required_memory_gb=10.0,
            count=1,
            exclude_nodes=["n1"],
        )
        assert selected == []

    @pytest.mark.asyncio
    async def test_select_nodes_preferred_soft_hint(self):
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
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(info1)
        await master.register_node(info2)
        # preferred_node_id 软提示: 优先选 n1 (preferred_bonus)
        selected = await master.select_nodes(
            ParallelMode.DATA,
            required_memory_gb=10.0,
            count=1,
            preferred_node_id="n1",
        )
        assert len(selected) == 1
        assert selected[0].node_id == "n1"

    @pytest.mark.asyncio
    async def test_assign_task_exclude_nodes_passthrough(self):
        # #31 端到端: assign_task 透传 exclude_nodes, 被规避节点不派发
        master = ClusterMaster()
        bad = NodeInfo(
            node_id="bad",
            hostname="mac-bad",
            ip_address="10.0.0.9",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        good = NodeInfo(
            node_id="good",
            hostname="mac-good",
            ip_address="10.0.0.8",
            port=11458,
            status=NodeStatus.ONLINE,
            available_memory_gb=50.0,
            total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        await master.register_node(bad)
        await master.register_node(good)
        task = ClusterTask(
            task_id="t-exclude-1",
            name="retry-avoid",
            mode=ParallelMode.DATA,
            model_name="small",
            timeout_seconds=30.0,
            user="u1",
            created_at=time.time(),
            required_capability="",
            preferred_node_id="",
            exclude_nodes=["bad"],
        )
        ok = await master.assign_task(task)
        assert ok
        assert task.status == TaskStatus.RUNNING
        assert task.assigned_nodes == ["good"]

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
    async def test_find_kv_cache_lock_order_no_nested_cross_domain(self):
        # P1-12 (审计 §2.4/§4.4): find_kv_cache 须 nodes→kv 顺序, 两锁不嵌套持有。
        # _kv_lock 持有区不得 await 跨域 _nodes_lock。
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
        await master.register_kv_cache(
            KVCacheEntry(cache_id="c1", model_name="test", node_id="n1", created_at=time.time(), size_mb=100.0)
        )

        nested = {"kv_held_during_nodes_acquire": False}
        real_nodes_lock = master._nodes_lock
        real_kv_lock = master._kv_lock

        orig_nodes_acquire = real_nodes_lock.acquire
        orig_kv_release = real_kv_lock.release

        async def spy_nodes_acquire():
            if real_kv_lock.locked():
                nested["kv_held_during_nodes_acquire"] = True
            return await orig_nodes_acquire()

        async def spy_kv_release():
            return await orig_kv_release()

        master._nodes_lock.acquire = spy_nodes_acquire  # type: ignore[method-assign]
        master._kv_lock.release = spy_kv_release  # type: ignore[method-assign]
        try:
            found = await master.find_kv_cache("test")
        finally:
            master._nodes_lock.acquire = orig_nodes_acquire  # type: ignore[method-assign]
            master._kv_lock.release = orig_kv_release  # type: ignore[method-assign]
        assert found is not None
        assert found.cache_id == "c1"
        assert nested["kv_held_during_nodes_acquire"] is False, (
            "find_kv_cache 在 _kv_lock 持有区获取 _nodes_lock (kv→nodes 嵌套, 违反 nodes→kv 锁序)"
        )

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
            patch.object(
                ClusterMaster,
                "_collect_mdns_props",
                return_value=("MacBookPro", "32.0"),
            ),
        ):
            await master._start_mdns()
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
    async def test_dispatch_http_timeout_follows_task_timeout_seconds(self):
        # P1-13 (审计 §5.4): _dispatch_to_node HTTP 超时须随 task.timeout_seconds, 不固定 300s。
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
        nodes_snap = await master._snapshot_nodes(["n1"])
        token = master._get_dispatch_token()

        captured = {}

        class FakeResponse:
            status_code = 200

            def json(self):
                return {"status": "ok", "result": {"ok": True}}

        class FakeClient:
            async def post(self, url, json=None, headers=None, timeout=None):
                captured["timeout"] = timeout
                return FakeResponse()

        task = ClusterTask(
            task_id="t-long",
            name="infer",
            mode=ParallelMode.DATA,
            model_name="test",
            timeout_seconds=600.0,
        )
        result = await master._dispatch_to_node(FakeClient(), task, "n1", nodes_snap, token)
        assert result == {"ok": True, "node_id": "n1"}
        # 600s 任务 → HTTP 超时 = 600 + 30 缓冲 = 630 (>300, 不再被掐断)
        assert captured["timeout"] == 630.0

    @pytest.mark.asyncio
    async def test_dispatch_http_timeout_floor(self):
        # P1-13: 极小 timeout_seconds 不得让 HTTP 超时 < 30s 下限。
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
        nodes_snap = await master._snapshot_nodes(["n1"])
        token = master._get_dispatch_token()

        captured = {}

        class FakeResponse:
            status_code = 200

            def json(self):
                return {"status": "ok", "result": {"ok": True}}

        class FakeClient:
            async def post(self, url, json=None, headers=None, timeout=None):
                captured["timeout"] = timeout
                return FakeResponse()

        task = ClusterTask(
            task_id="t-tiny",
            name="infer",
            mode=ParallelMode.DATA,
            model_name="test",
            timeout_seconds=1.0,
        )
        await master._dispatch_to_node(FakeClient(), task, "n1", nodes_snap, token)
        # 1s + 30 缓冲 = 31, > 下限 30
        assert captured["timeout"] == 31.0

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


class TestP01LoopFaultTolerance:
    """P0-1: 背景循环逐次异常隔离 — 单轮抛非 CancelledError 不杀整个循环。"""

    @pytest.mark.asyncio
    async def test_persist_loop_survives_persist_failure(self):
        import fusion_multi_node.master.cluster_master as cm

        master = ClusterMaster()
        calls = {"n": 0}

        async def boom():
            calls["n"] += 1
            raise RuntimeError("模拟写盘失败")

        orig_persist = master._persist_tasks
        master._persist_tasks = boom
        orig_sleep = cm.asyncio.sleep

        async def fast_sleep(_d):
            await orig_sleep(0)

        cm.asyncio.sleep = fast_sleep
        try:
            await master.start(with_server=False, with_mdns=False)
            await orig_sleep(0.05)
            assert master._persist_task is not None
            assert not master._persist_task.done(), "持久化循环不应被异常杀死"
            assert calls["n"] >= 1, "循环体应已执行至少一次"
        finally:
            cm.asyncio.sleep = orig_sleep
            master._persist_tasks = orig_persist
            await master.stop()

    @pytest.mark.asyncio
    async def test_retry_loop_survives_assign_failure(self):
        import fusion_multi_node.master.cluster_master as cm

        master = ClusterMaster()
        task = ClusterTask(task_id="r1", name="infer", mode=ParallelMode.DATA, model_name="m")
        master._pending_retry.append(task)
        calls = {"n": 0}

        async def boom(_t):
            calls["n"] += 1
            raise RuntimeError("模拟派发失败")

        orig_assign = master.assign_task
        master.assign_task = boom
        orig_sleep = cm.asyncio.sleep

        async def fast_sleep(_d):
            await orig_sleep(0)

        cm.asyncio.sleep = fast_sleep
        try:
            await master.start(with_server=False, with_mdns=False)
            await orig_sleep(0.05)
            assert not master._retry_task.done(), "重试循环不应被异常杀死"
            assert calls["n"] >= 1, "循环体应已执行至少一次"
        finally:
            cm.asyncio.sleep = orig_sleep
            master.assign_task = orig_assign
            await master.stop()

    @pytest.mark.asyncio
    async def test_health_check_loop_survives_timeout_failure(self):
        import fusion_multi_node.master.cluster_master as cm

        master = ClusterMaster()

        async def boom():
            raise RuntimeError("模拟超时检查失败")

        master.check_timeouts = boom
        orig_sleep = cm.asyncio.sleep

        async def fast_sleep(_d):
            await orig_sleep(0)

        cm.asyncio.sleep = fast_sleep
        try:
            await master.start(with_server=False, with_mdns=False)
            await orig_sleep(0.05)
            assert not master._health_task.done(), "健康检查循环不应被异常杀死"
        finally:
            cm.asyncio.sleep = orig_sleep
            await master.stop()

    @pytest.mark.asyncio
    async def test_health_check_loop_survives_refresh_failure(self):
        import fusion_multi_node.master.cluster_master as cm

        master = ClusterMaster()

        async def ok():
            pass

        async def boom():
            raise RuntimeError("模拟刷新失败")

        master.check_timeouts = ok
        master._refresh_node_statuses = boom
        master._cleanup_completed_tasks = ok
        master._cleanup_offline_nodes = ok
        orig_sleep = cm.asyncio.sleep

        async def fast_sleep(_d):
            await orig_sleep(0)

        cm.asyncio.sleep = fast_sleep
        try:
            await master.start(with_server=False, with_mdns=False)
            await orig_sleep(0.05)
            assert not master._health_task.done(), "健康检查循环不应被刷新异常杀死"
        finally:
            cm.asyncio.sleep = orig_sleep
            await master.stop()


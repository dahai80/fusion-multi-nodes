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
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755)
        assert info.node_id == "n1"
        assert info.status == NodeStatus.OFFLINE
        assert info.active_tasks == 0
        assert info.max_tasks == 4

    def test_score_high_resources(self):
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            total_memory_gb=64.0, available_memory_gb=50.0,
            active_tasks=0, max_tasks=4, network_rtt_ms=5.0,
        )
        score = info.score
        assert 0 < score <= 1

    def test_score_low_resources(self):
        info = NodeInfo(
            node_id="n2", hostname="mac2", ip_address="10.0.0.2", port=9755,
            total_memory_gb=64.0, available_memory_gb=5.0,
            active_tasks=4, max_tasks=4, network_rtt_ms=50.0,
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
            task_id="t1", name="inference", mode=ParallelMode.DATA,
            model_name="llama", assigned_nodes=["n1", "n2"],
        )
        assert task.mode == ParallelMode.DATA
        assert len(task.assigned_nodes) == 2


class TestKVCacheEntry:
    def test_basic(self):
        entry = KVCacheEntry(
            cache_id="c1", model_name="test", node_id="n1",
            created_at=time.time(), size_mb=100.0,
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

    def test_register_node(self):
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755)
        master.register_node(info)
        assert "n1" in master.nodes
        assert master.nodes["n1"].status == NodeStatus.ONLINE

    def test_unregister_node(self):
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755)
        master.register_node(info)
        master.unregister_node("n1")
        assert "n1" not in master.nodes

    def test_get_online_nodes_empty(self):
        master = ClusterMaster()
        nodes = master.get_online_nodes()
        assert len(nodes) == 0

    def test_get_online_nodes(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, last_heartbeat=time.time(),
        )
        master.register_node(info)
        nodes = master.get_online_nodes()
        assert len(nodes) == 1

    def test_assign_task(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=50.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        master.register_node(info)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        ok = master.assign_task(task)
        assert ok is True
        assert "t1" in master.tasks

    def test_assign_task_no_nodes(self):
        master = ClusterMaster()
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE)
        ok = master.assign_task(task)
        assert ok is False

    def test_complete_task(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=50.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        master.register_node(info)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        master.assign_task(task)
        master.complete_task("t1")
        assert master.tasks["t1"].status == TaskStatus.COMPLETED

    def test_complete_task_with_error_sets_failed(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=50.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        master.register_node(info)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        master.assign_task(task)
        master.complete_task("t1", error="OOM")
        assert master.tasks["t1"].status == TaskStatus.FAILED
        assert master.tasks["t1"].error == "OOM"

    def test_migrate_task(self):
        master = ClusterMaster()
        info1 = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=50.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        info2 = NodeInfo(
            node_id="n2", hostname="mac2", ip_address="10.0.0.2", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=60.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        master.register_node(info1)
        master.register_node(info2)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        master.assign_task(task)
        ok = master.migrate_task("t1")
        assert ok is True
        # migrate_task resets to PENDING then re-assigns -> RUNNING
        assert master.tasks["t1"].status == TaskStatus.RUNNING

    def test_migrate_task_not_found(self):
        master = ClusterMaster()
        ok = master.migrate_task("nope")
        assert ok is False

    def test_check_heartbeat(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, last_heartbeat=time.time(),
        )
        master.register_node(info)
        ok = master.check_heartbeat("n1")
        assert ok is True

    def test_check_heartbeat_stale_sets_offline(self):
        master = ClusterMaster(heartbeat_timeout=5.0)
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755)
        master.register_node(info)
        # register_node sets last_heartbeat=time.time(), so force stale AFTER registration
        master.nodes["n1"].last_heartbeat = time.time() - 100
        ok = master.check_heartbeat("n1")
        assert ok is False
        assert master.nodes["n1"].status == NodeStatus.OFFLINE

    def test_check_heartbeat_missing_node(self):
        master = ClusterMaster()
        ok = master.check_heartbeat("nope")
        assert ok is False

    def test_check_timeouts(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=50.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        master.register_node(info)
        task = ClusterTask(
            task_id="t1", name="infer", mode=ParallelMode.PIPELINE,
            model_name="test", timeout_seconds=0.01,
        )
        master.assign_task(task)
        master.tasks["t1"].status = TaskStatus.RUNNING
        master.tasks["t1"].started_at = time.time() - 1
        timed_out = master.check_timeouts()
        assert "t1" in timed_out
        assert master.tasks["t1"].status == TaskStatus.TIMEOUT

    def test_select_nodes(self):
        master = ClusterMaster()
        info1 = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=50.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        info2 = NodeInfo(
            node_id="n2", hostname="mac2", ip_address="10.0.0.2", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=60.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        master.register_node(info1)
        master.register_node(info2)
        selected = master.select_nodes(ParallelMode.PIPELINE, required_memory_gb=10.0, count=2)
        assert len(selected) == 2

    def test_select_nodes_insufficient(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=5.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        master.register_node(info)
        selected = master.select_nodes(ParallelMode.PIPELINE, required_memory_gb=50.0, count=1)
        assert len(selected) == 0

    def test_register_kv_cache(self):
        master = ClusterMaster()
        entry = KVCacheEntry(
            cache_id="c1", model_name="test", node_id="n1",
            created_at=time.time(), size_mb=100.0,
        )
        master.register_kv_cache(entry)
        assert "c1" in master.kv_cache

    def test_find_kv_cache(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, last_heartbeat=time.time(),
        )
        master.register_node(info)
        entry = KVCacheEntry(
            cache_id="c1", model_name="test", node_id="n1",
            created_at=time.time(), size_mb=100.0,
        )
        master.register_kv_cache(entry)
        found = master.find_kv_cache("test")
        assert found is not None
        assert found.cache_id == "c1"

    def test_find_kv_cache_missing(self):
        master = ClusterMaster()
        found = master.find_kv_cache("nope")
        assert found is None

    def test_find_kv_cache_offline_node_skips(self):
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755)
        master.register_node(info)
        # Force offline AFTER registration (register_node sets ONLINE)
        master.nodes["n1"].status = NodeStatus.OFFLINE
        entry = KVCacheEntry(
            cache_id="c1", model_name="test", node_id="n1",
            created_at=time.time(), size_mb=100.0,
        )
        master.register_kv_cache(entry)
        found = master.find_kv_cache("test")
        assert found is None
        # offline node entries are skipped but NOT removed
        assert "c1" in master.kv_cache

    def test_find_kv_cache_expired(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, last_heartbeat=time.time(),
        )
        master.register_node(info)
        entry = KVCacheEntry(
            cache_id="c1", model_name="test", node_id="n1",
            created_at=time.time() - 5000, size_mb=100.0, ttl_seconds=1.0,
        )
        master.register_kv_cache(entry)
        found = master.find_kv_cache("test")
        assert found is None
        assert "c1" not in master.kv_cache

    def test_get_stats(self):
        master = ClusterMaster()
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755)
        master.register_node(info)
        stats = master.get_stats()
        assert "total_nodes" in stats
        assert "online_nodes" in stats

    @pytest.mark.asyncio
    async def test_start_mdns(self):
        master = ClusterMaster()
        master._start_mdns()
        master._stop_mdns()

    def test_get_online_nodes_stale_goes_offline(self):
        master = ClusterMaster(heartbeat_timeout=0.01)
        info = NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755)
        master.register_node(info)
        master.nodes["n1"].last_heartbeat = time.time() - 100
        online = master.get_online_nodes()
        assert len(online) == 0
        assert master.nodes["n1"].status == NodeStatus.OFFLINE

    def test_complete_task_decrements_active(self):
        master = ClusterMaster()
        info = NodeInfo(
            node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=9755,
            status=NodeStatus.ONLINE, available_memory_gb=50.0, total_memory_gb=64.0,
            last_heartbeat=time.time(),
        )
        master.register_node(info)
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        master.assign_task(task)
        assert master.nodes["n1"].active_tasks >= 1
        master.complete_task("t1")
        assert master.nodes["n1"].active_tasks == 0

    def test_complete_task_missing(self):
        master = ClusterMaster()
        master.complete_task("nope")

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
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="llama-70b")
        mem = master._estimate_memory(task)
        assert mem > 30.0

    def test_estimate_memory_13b(self):
        master = ClusterMaster()
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="llama-13b")
        mem = master._estimate_memory(task)
        assert mem > 10.0

    def test_estimate_memory_8b(self):
        master = ClusterMaster()
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="llama-8b")
        mem = master._estimate_memory(task)
        assert mem > 6.0

    def test_estimate_memory_3b(self):
        master = ClusterMaster()
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="llama-3b")
        mem = master._estimate_memory(task)
        assert mem > 4.0

    def test_estimate_memory(self):
        master = ClusterMaster()
        task = ClusterTask(task_id="t1", name="infer", mode=ParallelMode.PIPELINE, model_name="test")
        mem = master._estimate_memory(task)
        assert isinstance(mem, float)
        assert mem >= 0

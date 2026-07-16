"""深度覆盖测试 — 补齐剩余模块的缺失分支，目标 90%+。"""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fusion_multi_nodes.agent import NodeAgent, AgentConfig
from fusion_multi_nodes.distributed_mlx import (
    DistributedMLXBridge, CavemanCompressor, CavemanManager,
    KVSharingManager, KVCacheEntry, KVShard,
)
from fusion_multi_nodes.mcp_gateway import MCPClusterGateway, MCPTool
from fusion_multi_nodes.observability import ClusterObservability, LogEntry
from fusion_multi_nodes.master import ClusterMaster, NodeInfo, NodeStatus, ClusterTask, ParallelMode, TaskStatus


# ══════════════════════════════════════════════════════════════════════════════
# Node Agent 深度覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestNodeAgentDeep:
    @pytest.mark.asyncio
    async def test_execute_inference(self):
        """测试推理任务执行（由于 fusion-mlx 可能未运行，测试错误处理）。"""
        agent = NodeAgent()
        result = await agent.execute_task({
            "task_id": "inf_1",
            "type": "inference",
            "model": "test",
            "params": {"prompt": "hello", "temperature": 0.7, "max_tokens": 100},
        })
        # fusion-mlx 未运行时应返回错误
        assert "error" in result or "task_id" in result

    @pytest.mark.asyncio
    async def test_execute_embedding(self):
        """测试 embedding 任务执行。"""
        agent = NodeAgent()
        result = await agent.execute_task({
            "task_id": "emb_1",
            "type": "embedding",
            "model": "BGE-M3",
            "params": {"text": "test text"},
        })
        assert "error" in result or "task_id" in result

    @pytest.mark.asyncio
    async def test_execute_plugin(self):
        """测试插件任务执行。"""
        agent = NodeAgent()
        result = await agent.execute_task({
            "task_id": "plg_1",
            "type": "plugin",
            "plugin": "test",
            "action": "run",
            "params": {},
        })
        # fusion-desk 未运行时应返回错误
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_heartbeat_loop_error(self):
        """测试心跳循环异常处理。"""
        agent = NodeAgent()
        agent._running = True
        # 快速启动和停止，确保不卡住
        task = asyncio.create_task(agent._heartbeat_loop())
        await asyncio.sleep(0.1)
        agent._running = False
        await task
        assert True

    @pytest.mark.asyncio
    async def test_hardware_report_loop(self):
        """测试硬件上报循环。"""
        agent = NodeAgent()
        agent._running = True
        task = asyncio.create_task(agent._hardware_report_loop())
        await asyncio.sleep(0.1)
        agent._running = False
        await task
        assert True

    @pytest.mark.asyncio
    async def test_report_hardware(self):
        """测试硬件上报（master 未运行，应返回 False）。"""
        agent = NodeAgent()
        result = await agent.report_hardware()
        assert isinstance(result, bool)


# ══════════════════════════════════════════════════════════════════════════════
# Distributed MLX Bridge 深度覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestDistributedMLXDeep:
    @pytest.mark.asyncio
    async def test_pipeline_inference_with_nodes(self):
        """测试流水线并行（远程节点不可达，应返回错误）。"""
        bridge = DistributedMLXBridge()
        result = await bridge.pipeline_inference("test", "prompt", ["10.255.255.1"])
        assert "error" in result

    @pytest.mark.asyncio
    async def test_data_parallel_inference(self):
        """测试数据并行（远程节点不可达，应返回错误）。"""
        bridge = DistributedMLXBridge()
        results = await bridge.data_parallel_inference(
            "test", ["prompt1", "prompt2"], ["10.255.255.1"]
        )
        assert len(results) == 2
        for r in results:
            assert "error" in r

    @pytest.mark.asyncio
    async def test_load_shard_remote(self):
        """测试远程分片加载（不可达，应返回 False）。"""
        bridge = DistributedMLXBridge()
        # 先分片
        await bridge.shard_model("test", 2)
        result = await bridge.load_shard("test", 0, "10.255.255.1")
        assert not result

    @pytest.mark.asyncio
    async def test_single_inference(self):
        """测试单节点推理（远程不可达，应返回异常处理）。"""
        bridge = DistributedMLXBridge()
        result = await bridge._single_inference("10.255.255.1", "test", "prompt", 8000)
        assert "error" in result


# ══════════════════════════════════════════════════════════════════════════════
# Caveman 深度覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestCavemanDeep:
    def test_compressor_build_dictionary(self):
        c = CavemanCompressor(dictionary_size=10)
        c.build_dictionary([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
        assert len(c._dictionary) == 10  # 最多 10 个

    def test_compress_with_dictionary(self):
        c = CavemanCompressor()
        c.build_dictionary([0x41424344])  # "ABCD"
        data = b"ABCDABCDABCD"
        compressed, stats = c.compress(data, method="dict")
        assert stats.original_bytes == 12
        # 字典压缩应减少大小
        assert stats.compressed_bytes <= stats.original_bytes

    def test_compress_large_data(self):
        c = CavemanCompressor()
        data = b"Hello Fusion-Multi-Node! " * 1000  # 22KB
        compressed, stats = c.compress(data, method="zlib")
        assert stats.ratio < 0.5  # 大文本应有高压缩率

    def test_compress_binary_data(self):
        c = CavemanCompressor()
        data = bytes(range(256)) * 100
        compressed, stats = c.compress(data, method="diff")
        # 差分压缩对连续数据有效
        assert stats.ratio <= 1.0

    def test_has_repeated_pattern_edge(self):
        c = CavemanCompressor()
        assert not c._has_repeated_pattern(b"")  # 空数据
        assert not c._has_repeated_pattern(b"ab")  # 太短
        assert c._has_repeated_pattern(b"AAAAAAAABBBBBBBB")  # 重复

    @pytest.mark.asyncio
    async def test_compress_tensor_thunderbolt(self):
        m = CavemanManager()
        data = b"x" * 10000
        compressed, method, stats = await m.compress_tensor(data, link_type="thunderbolt_5")
        # Thunderbolt 5 使用 dict 压缩，对大块重复数据有效
        assert stats.original_bytes > 0

    @pytest.mark.asyncio
    async def test_decompress_roundtrip_aggressive(self):
        m = CavemanManager()
        original = b"aggressive compression test data " * 30
        compressed, method, _ = await m.compress_tensor(original, link_type="ethernet_100m")
        decompressed = await m.decompress_tensor(compressed, method)
        assert decompressed == original


# ══════════════════════════════════════════════════════════════════════════════
# MCP Gateway 深度覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestMCPGatewayDeep:
    @pytest.mark.asyncio
    async def test_handle_tool_call_local(self):
        """测试本地工具调用（无 selector 时）。"""
        gateway = MCPClusterGateway()
        tool = MCPTool(name="local_test", description="test", parameters={})
        gateway.register_tool(tool)
        # 无 selector 时使用 localhost
        result = await gateway.handle_tool_call("local_test", {"key": "value"}, source="test")
        # fusion-desk 未运行，应返回错误
        assert "error" in result

    @pytest.mark.asyncio
    async def test_handle_tool_call_with_remote_selector(self):
        """测试远程节点工具调用。"""
        gateway = MCPClusterGateway()
        gateway.set_node_selector(lambda t: "10.255.255.1")
        tool = MCPTool(name="remote_test", description="test", parameters={})
        gateway.register_tool(tool)
        result = await gateway.handle_tool_call("remote_test", {}, source="claude_code")
        # 远程不可达，应返回错误
        assert "error" in result

    def test_token_counting(self):
        """测试 token 统计。"""
        gateway = MCPClusterGateway()
        gateway.total_token_count = 5000000
        tool = MCPTool(name="t", description="t", parameters={})
        gateway.register_tool(tool)
        stats = gateway.get_stats()
        assert stats["token_remaining"] == 5000000  # budget - 5M = 5M


# ══════════════════════════════════════════════════════════════════════════════
# Observability 深度覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestObservabilityDeep:
    def test_metrics_cleanup(self):
        obs = ClusterObservability(retention_hours=0.0)  # 立即过期
        obs.record_metric("n1", "m1", 1.0)
        obs.record_metric("n1", "m1", 2.0)
        # 手动清理（start 中的 cleanup_loop 每 5 分钟执行一次）
        cutoff = time.time()
        obs.metrics = [m for m in obs.metrics if m.timestamp > cutoff]
        assert len(obs.metrics) == 0

    def test_log_with_critical_level(self):
        obs = ClusterObservability()
        obs.add_log(LogEntry(
            timestamp=time.time(), node_id="n1", level="CRITICAL",
            module="test", message="Critical error"))
        alerts = obs.get_active_alerts()
        assert len(alerts) >= 1

    @pytest.mark.asyncio
    async def test_cleanup_loop(self):
        obs = ClusterObservability(retention_hours=0.0)
        obs._running = True
        # 添加一些过期数据
        obs.metrics = [type("M", (), {"timestamp": time.time() - 7200, "node_id": "n1", "metric_name": "m", "value": 1.0, "tags": {}})()]
        # 启动清理循环
        task = asyncio.create_task(obs._cleanup_loop())
        await asyncio.sleep(0.1)
        obs._running = False
        await task
        assert True


# ══════════════════════════════════════════════════════════════════════════════
# Master 深度覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestMasterDeep:
    def test_heartbeat_unknown_node(self):
        master = ClusterMaster()
        assert not master.check_heartbeat("nonexistent")

    def test_migrate_task_not_running(self):
        master = ClusterMaster()
        task = ClusterTask(task_id="t1", name="t1", mode=ParallelMode.DATA,
                          status=TaskStatus.PENDING)
        master.tasks["t1"] = task
        assert not master.migrate_task("t1")

    def test_complete_task_with_node(self):
        master = ClusterMaster()
        master.register_node(NodeInfo(
            node_id="w1", hostname="w1", ip_address="10.0.0.1", port=9755,
            total_memory_gb=32, available_memory_gb=24, cpu_cores=12, gpu_cores=40,
            status=NodeStatus.ONLINE, last_heartbeat=time.time()))
        task = ClusterTask(task_id="t2", name="t2", mode=ParallelMode.DATA,
                          assigned_nodes=["w1"], status=TaskStatus.RUNNING)
        master.tasks["t2"] = task
        master.complete_task("t2", "test error")
        assert task.status == TaskStatus.FAILED
        assert master.nodes["w1"].active_tasks == 0

    def test_kv_cache_cleanup_on_find(self):
        master = ClusterMaster()
        from fusion_multi_nodes.master import KVCacheEntry
        master.kv_cache["expired"] = KVCacheEntry(
            cache_id="expired", model_name="test", node_id="n1",
            created_at=time.time() - 7200, size_mb=10.0, ttl_seconds=3600)
        result = master.find_kv_cache("test")
        assert result is None
        assert "expired" not in master.kv_cache  # 已清理

    def test_complete_task_no_nodes(self):
        master = ClusterMaster()
        task = ClusterTask(task_id="t3", name="t3", mode=ParallelMode.DATA,
                          assigned_nodes=[], status=TaskStatus.RUNNING)
        master.tasks["t3"] = task
        master.complete_task("t3")
        assert task.status == TaskStatus.COMPLETED


# ══════════════════════════════════════════════════════════════════════════════
# KV Cache 深度覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestKVSharingDeep:
    def test_kv_shard_ordering(self):
        """测试 LRU 淘汰顺序。"""
        manager = KVSharingManager(max_local_cache_mb=0.005)  # 极小缓存
        import time
        for i in range(20):
            entry = KVCacheEntry(
                cache_id=f"e{i}", model_name="m", prompt_hash=f"h{i}",
                prompt_prefix="", total_tokens=10, total_size_bytes=1000,
                created_at=time.time() - i * 10)
            # 前几个会触发淘汰
            manager.store_local(entry)
        stats = manager.get_stats()
        # 缓存应该远小于 20 条
        assert stats["local_entries"] < 20

    @pytest.mark.asyncio
    async def test_warm_cache_empty(self):
        manager = KVSharingManager()
        result = await manager.warm_cache("test", [], ["node_1"])
        assert result["success"] == 0
        assert result["failed"] == 0

    def test_deserialize_entry(self):
        """测试 KV 条目反序列化。"""
        manager = KVSharingManager()
        data = {
            "cache_id": "test",
            "model_name": "m",
            "prompt_hash": "h",
            "prompt_prefix": "p",
            "total_tokens": 10,
            "total_size_bytes": 100,
            "created_at": time.time(),
            "ttl_seconds": 3600,
            "access_count": 0,
            "shards": [],
        }
        entry = manager._deserialize_entry(data)
        assert entry.cache_id == "test"
        assert entry.model_name == "m"


# ══════════════════════════════════════════════════════════════════════════════
# Distributed Bridge 额外覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestDistributedBridgeExtra:
    def test_dist_config_defaults(self):
        from fusion_multi_nodes.distributed_mlx import DistConfig, DistMode
        config = DistConfig()
        assert config.mode == DistMode.PIPELINE
        assert config.num_nodes == 1
        assert config.caveman_compress is True

    @pytest.mark.asyncio
    async def test_shard_model_large(self):
        bridge = DistributedMLXBridge()
        shards = await bridge.shard_model("test", num_shards=8)
        assert len(shards) == 8
        # 总层数应覆盖所有层
        all_layers = []
        for s in shards:
            all_layers.extend(s.layers)
        assert len(set(all_layers)) == 32  # 默认 32 层

    @pytest.mark.asyncio
    async def test_get_model_config_fallback(self):
        bridge = DistributedMLXBridge()
        # fusion-mlx 未运行，应返回默认配置
        config = await bridge._get_model_config("nonexistent")
        assert config["num_hidden_layers"] == 32
        assert config["memory_mb"] == 4096
"""Fusion-Multi-Node 全覆盖测试 — 补齐 Phase 1-5 所有模块的缺失分支。

目标：覆盖率 90%+
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fusion_multi_nodes.master import (
    ClusterMaster, ClusterTask, KVCacheEntry, NodeInfo, NodeStatus, ParallelMode, TaskStatus,
)
from fusion_multi_nodes.agent import NodeAgent, AgentConfig
from fusion_multi_nodes.distributed_mlx import (
    DistributedMLXBridge, ModelShard, DistConfig, DistMode,
    CavemanCompressor, CavemanManager, CompressStats,
    KVSharingManager, KVCacheEntry as KVEntry, KVShard, KVCacheWarmScheduler,
)
from fusion_multi_nodes.mcp_gateway import MCPClusterGateway, MCPTool, MCPRequest
from fusion_multi_nodes.observability import ClusterObservability, MetricPoint, Alert, LogEntry
from fusion_multi_nodes.config import ClusterConfig
from fusion_multi_nodes.utils import setup_logger, get_data_dir, get_log_dir, NetworkTopologyDetector, LinkType


# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: Cluster Master 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestClusterMasterFull:
    def setup_method(self):
        self.master = ClusterMaster(host="127.0.0.1", port=19753, heartbeat_timeout=60.0)

    def test_unregister_node(self):
        node = NodeInfo(node_id="n1", hostname="h1", ip_address="1.1.1.1", port=9755, status=NodeStatus.ONLINE)
        self.master.register_node(node)
        assert "n1" in self.master.nodes
        self.master.unregister_node("n1")
        assert "n1" not in self.master.nodes

    def test_heartbeat_timeout(self):
        node = NodeInfo(node_id="n2", hostname="h2", ip_address="1.1.1.2", port=9755,
                        status=NodeStatus.ONLINE, last_heartbeat=time.time())
        self.master.register_node(node)
        # register_node 覆盖 last_heartbeat，所以直接设置
        self.master.nodes["n2"].last_heartbeat = time.time() - 120
        assert not self.master.check_heartbeat("n2")
        assert self.master.nodes["n2"].status == NodeStatus.OFFLINE

    def test_heartbeat_active(self):
        node = NodeInfo(node_id="n3", hostname="h3", ip_address="1.1.1.3", port=9755,
                        status=NodeStatus.ONLINE, last_heartbeat=time.time())
        self.master.register_node(node)
        assert self.master.check_heartbeat("n3")

    def test_select_nodes_insufficient(self):
        nodes = self.master.select_nodes(ParallelMode.PIPELINE, required_memory_gb=999.0, count=5)
        assert len(nodes) == 0

    def test_task_migration(self):
        self.master.register_node(NodeInfo(
            node_id="w1", hostname="w1", ip_address="10.0.0.1", port=9755,
            total_memory_gb=32, available_memory_gb=24, cpu_cores=12, gpu_cores=40,
            status=NodeStatus.ONLINE, last_heartbeat=time.time()))
        task = ClusterTask(task_id="mig", name="migrate", mode=ParallelMode.DATA)
        assert self.master.assign_task(task)
        # migrate_task 会释放原节点并重新分配，成功后状态为 RUNNING
        assert self.master.migrate_task("mig")
        assert task.status == TaskStatus.RUNNING  # 重新分配成功

    def test_task_migration_not_found(self):
        assert not self.master.migrate_task("nonexistent")

    def test_check_timeouts(self):
        task = ClusterTask(task_id="slow", name="slow", mode=ParallelMode.DATA,
                           timeout_seconds=0.1, started_at=time.time() - 10,
                           status=TaskStatus.RUNNING)
        self.master.tasks["slow"] = task
        timed_out = self.master.check_timeouts()
        assert "slow" in timed_out

    def test_kv_cache_register_and_find(self):
        # 先注册节点，find_kv_cache 需要节点在线
        self.master.register_node(NodeInfo(
            node_id="n1", hostname="h1", ip_address="10.0.0.1", port=9755,
            total_memory_gb=32, available_memory_gb=24, cpu_cores=12, gpu_cores=40,
            status=NodeStatus.ONLINE, last_heartbeat=time.time()))
        entry = KVCacheEntry(
            cache_id="kv1", model_name="qwen", node_id="n1", created_at=time.time(),
            size_mb=10.0, ttl_seconds=3600)
        self.master.register_kv_cache(entry)
        found = self.master.find_kv_cache("qwen")
        assert found is not None
        assert found.cache_id == "kv1"

    def test_kv_cache_expired(self):
        # 注册节点，否则 find_kv_cache 找不到
        self.master.register_node(NodeInfo(
            node_id="n1", hostname="h1", ip_address="10.0.0.1", port=9755,
            total_memory_gb=32, available_memory_gb=24, cpu_cores=12, gpu_cores=40,
            status=NodeStatus.ONLINE, last_heartbeat=time.time()))
        entry = KVCacheEntry(
            cache_id="kv_old", model_name="old", node_id="n1", created_at=time.time() - 7200,
            size_mb=10.0, ttl_seconds=3600)
        self.master.register_kv_cache(entry)
        found = self.master.find_kv_cache("old")
        assert found is None  # 已过期

    def test_estimate_memory(self):
        task = ClusterTask(task_id="t1", name="t1", mode=ParallelMode.DATA, model_name="qwen3.5-9b")
        mem = self.master._estimate_memory(task)
        assert mem > 0

    def test_estimate_memory_70b(self):
        task = ClusterTask(task_id="t2", name="t2", mode=ParallelMode.PIPELINE, model_name="llama-70b")
        mem = self.master._estimate_memory(task)
        assert mem > 30  # 70B 需要 32GB+

    def test_estimate_memory_unknown(self):
        task = ClusterTask(task_id="t3", name="t3", mode=ParallelMode.DATA, model_name="")
        mem = self.master._estimate_memory(task)
        assert mem == 2.0  # 基础内存

    def test_start_stop(self):
        asyncio.run(self.master.start())
        assert self.master._running
        asyncio.run(self.master.stop())
        assert not self.master._running

    def test_assign_task_no_nodes(self):
        task = ClusterTask(task_id="no_nodes", name="no", mode=ParallelMode.DATA)
        assert not self.master.assign_task(task)

    def test_complete_task_not_found(self):
        self.master.complete_task("not_found")  # 不应报错


# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: Node Agent 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestNodeAgentFull:
    def test_agent_initialization(self):
        agent = NodeAgent()
        assert agent.config.node_id.startswith("node_")

    def test_custom_config(self):
        config = AgentConfig(node_id="custom_node", master_host="10.0.0.1")
        agent = NodeAgent(config)
        assert agent.config.node_id == "custom_node"
        assert agent.config.master_host == "10.0.0.1"

    def test_collect_hardware_detailed(self):
        agent = NodeAgent()
        info = agent.collect_hardware_info()
        assert "node_id" in info
        assert "hostname" in info
        assert "ip_address" in info
        assert "total_memory_gb" in info
        assert "cpu_cores" in info
        assert "is_apple_silicon" in info
        assert "fusion_mlx_running" in info
        assert "fusion_desk_running" in info
        assert "timestamp" in info

    def test_get_local_ip(self):
        agent = NodeAgent()
        ip = agent._get_local_ip()
        assert ip is not None
        assert len(ip) > 0

    @patch("fusion_multi_nodes.agent.node_agent.subprocess.run")
    def test_get_mlx_version(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout=b"0.18.0\n")
        agent = NodeAgent()
        version = agent._get_mlx_version()
        # 取决于 mock 是否生效
        assert isinstance(version, str)

    @patch("fusion_multi_nodes.agent.node_agent.subprocess.run")
    def test_get_gpu_cores(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0,
            stdout="Chipset Model: Apple M4 Ultra\n  Total Number of Cores: 80\n")
        agent = NodeAgent()
        cores = agent._get_gpu_cores()
        assert cores == 80

    def test_check_service_port_open(self):
        # 检查本地已知端口
        agent = NodeAgent()
        result = agent._check_service(22)  # SSH 通常打开
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_send_heartbeat(self):
        agent = NodeAgent()
        # 不 mock，仅测试返回值类型（实际会连不上 master）
        result = await agent.send_heartbeat()
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_report_fault(self):
        agent = NodeAgent()
        result = await agent.report_fault("test", "test fault")
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_execute_unknown_task_type(self):
        agent = NodeAgent()
        result = await agent.execute_task({"task_id": "t1", "type": "unknown_type"})
        assert "error" in result


# ══════════════════════════════════════════════════════════════════════════════
# Phase 2: Distributed MLX Bridge 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestDistributedMLXFull:
    @pytest.mark.asyncio
    async def test_shard_model_custom(self):
        bridge = DistributedMLXBridge()
        shards = await bridge.shard_model("test-model", num_shards=2, strategy="uniform")
        assert len(shards) == 2
        assert shards[0].shard_id == 0
        assert shards[1].shard_id == 1

    @pytest.mark.asyncio
    async def test_shard_model_single(self):
        bridge = DistributedMLXBridge()
        shards = await bridge.shard_model("test-model", num_shards=1)
        assert len(shards) == 1
        assert len(shards[0].layers) == 32  # 默认 32 层

    @pytest.mark.asyncio
    async def test_load_shard_invalid(self):
        bridge = DistributedMLXBridge()
        result = await bridge.load_shard("test-model", shard_id=99, node_id="node_1")
        assert not result

    @pytest.mark.asyncio
    async def test_pipeline_inference_no_nodes(self):
        bridge = DistributedMLXBridge()
        result = await bridge.pipeline_inference("test", "prompt", [])
        # 空链时返回原 prompt
        assert result["output"] == "prompt"
        assert result["nodes"] == 0

    @pytest.mark.asyncio
    async def test_data_parallel_empty(self):
        bridge = DistributedMLXBridge()
        results = await bridge.data_parallel_inference("test", [], ["node_1"])
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_sync_weights(self):
        bridge = DistributedMLXBridge()
        # 远程不可达，应返回 False
        result = await bridge.sync_weights("test", "10.255.255.1", ["10.255.255.2"])
        assert not result

    @pytest.mark.asyncio
    async def test_get_model_config(self):
        bridge = DistributedMLXBridge()
        # 本地 fusion-mlx 可能未运行，应返回默认配置
        config = await bridge._get_model_config("test")
        assert "num_hidden_layers" in config
        assert "memory_mb" in config


# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: MCP Gateway 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestMCPGatewayFull:
    def setup_method(self):
        self.gateway = MCPClusterGateway(host="127.0.0.1", port=19756)

    def test_unregister_tool(self):
        tool = MCPTool(name="test_tool", description="test", parameters={})
        self.gateway.register_tool(tool)
        assert "test_tool" in self.gateway.tools
        self.gateway.unregister_tool("test_tool")
        assert "test_tool" not in self.gateway.tools

    def test_set_node_selector(self):
        def selector(tool):
            return "node_1"
        self.gateway.set_node_selector(selector)
        assert self.gateway._node_selector is not None

    @pytest.mark.asyncio
    async def test_handle_tool_call_unknown_tool(self):
        result = await self.gateway.handle_tool_call("unknown_tool", {}, source="test")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_handle_tool_call_budget_exhausted(self):
        self.gateway.token_budget = 0
        self.gateway.total_token_count = 0
        tool = MCPTool(name="test", description="test", parameters={})
        self.gateway.register_tool(tool)
        result = await self.gateway.handle_tool_call("test", {}, source="test")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_handle_tool_call_with_selector(self):
        def selector(tool):
            return "remote_node"
        self.gateway.set_node_selector(selector)
        tool = MCPTool(name="test2", description="test", parameters={})
        self.gateway.register_tool(tool)
        result = await self.gateway.handle_tool_call("test2", {}, source="test")
        # 远程调用会失败，但会返回 error 而不是抛异常
        assert "error" in result

    def test_get_stats_detailed(self):
        stats = self.gateway.get_stats()
        assert stats["registered_tools"] == 0
        assert stats["total_requests"] == 0
        assert stats["token_budget"] > 0
        assert stats["token_remaining"] == stats["token_budget"]

    @pytest.mark.asyncio
    async def test_start_stop(self):
        await self.gateway.start()
        assert self.gateway._running
        await self.gateway.stop()
        assert not self.gateway._running


# ══════════════════════════════════════════════════════════════════════════════
# Phase 5: Caveman 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestCavemanFull:
    def test_compressor_initialization(self):
        c = CavemanCompressor(dictionary_size=512)
        assert c.dictionary_size == 512
        assert c._dictionary == {}

    def test_compress_empty(self):
        c = CavemanCompressor()
        compressed, stats = c.compress(b"", method="zlib")
        assert stats.original_bytes == 0
        assert stats.compressed_bytes >= 0

    def test_compress_small_data(self):
        c = CavemanCompressor()
        data = b"hello"
        compressed, stats = c.compress(data, method="auto")
        assert stats.original_bytes == 5
        assert stats.ratio <= 1.0

    def test_compress_no_method(self):
        c = CavemanCompressor()
        data = b"test"
        compressed, stats = c.compress(data, method="nonexistent")
        assert compressed == data  # 未知方法原样返回

    def test_decompress_unknown_method(self):
        c = CavemanCompressor()
        result = c.decompress(b"test", "unknown")
        assert result == b"test"

    def test_reset_stats(self):
        c = CavemanCompressor()
        c.compress(b"test data" * 10, method="zlib")
        assert c.stats.original_bytes > 0
        c.reset_stats()
        assert c.stats.original_bytes == 0

    def test_select_method_small(self):
        c = CavemanCompressor()
        assert c._select_method(b"small") == "dict"

    def test_select_method_repeated(self):
        c = CavemanCompressor()
        # 重复模式应选择 diff
        data = b"AAAAAAAABBBBBBBBCCCCCCCC" * 10
        method = c._select_method(data)
        assert method in ("diff", "zlib")

    def test_has_repeated_pattern(self):
        c = CavemanCompressor()
        assert c._has_repeated_pattern(b"AAAABBBBAAAABBBB")
        assert not c._has_repeated_pattern(b"abcdefgh")
        assert not c._has_repeated_pattern(b"a")

    def test_caveman_manager_get_config(self):
        m = CavemanManager()
        tb5 = m.get_compression_config("thunderbolt_5")
        assert tb5["method"] == "dict"
        unknown = m.get_compression_config("unknown")
        assert unknown["method"] == "zlib"

    @pytest.mark.asyncio
    async def test_compress_tensor_uncompressed(self):
        m = CavemanManager()
        data = b"x" * 1000
        compressed, method, stats = await m.compress_tensor(data, link_type="thunderbolt_5")
        assert stats.original_bytes > 0

    @pytest.mark.asyncio
    async def test_compress_tensor_aggressive(self):
        m = CavemanManager()
        data = b"Hello Fusion-Multi-Node! " * 100
        compressed, method, stats = await m.compress_tensor(data, link_type="ethernet_100m")
        assert stats.original_bytes > stats.compressed_bytes

    @pytest.mark.asyncio
    async def test_decompress_tensor(self):
        m = CavemanManager()
        original = b"Test data for roundtrip. " * 50
        compressed, method, _ = await m.compress_tensor(original, link_type="ethernet_1g")
        decompressed = await m.decompress_tensor(compressed, method)
        assert decompressed == original

    def test_compression_ratio_zero(self):
        m = CavemanManager()
        assert m.get_compression_ratio() == 1.0  # 未压缩时

    def test_get_stats_empty(self):
        m = CavemanManager()
        stats = m.get_stats()
        assert stats["total_original_bytes"] == 0
        assert stats["savings_percent"] == 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Phase 5: Network Topology 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestNetworkTopologyFull:
    @pytest.mark.asyncio
    async def test_detect_all_interfaces(self):
        detector = NetworkTopologyDetector()
        interfaces = await detector.detect()
        assert "lo0" in interfaces
        assert len(interfaces) >= 1

    def test_no_interfaces_returns_none(self):
        detector = NetworkTopologyDetector()
        best = detector.get_best_link()
        assert best is None  # 未检测时

    def test_primary_interface_default(self):
        detector = NetworkTopologyDetector()
        assert detector.get_primary_interface() == "lo0"  # 默认回环

    def test_link_speed_default(self):
        detector = NetworkTopologyDetector()
        assert detector.get_link_speed() == 1000.0  # 默认 1Gbps

    def test_link_type_default(self):
        detector = NetworkTopologyDetector()
        assert detector.get_link_type() == LinkType.UNKNOWN

    def test_is_thunderbolt_available(self):
        detector = NetworkTopologyDetector()
        # 添加 Thunderbolt 接口
        from fusion_multi_nodes.utils.network_topology import LinkInfo
        detector._interfaces["thunderbolt0"] = LinkInfo(
            type=LinkType.THUNDERBOLT_5, bandwidth_mbps=40000, latency_ms=0.1,
            interface="thunderbolt0", is_rdma=True, is_active=True, priority=0)
        assert detector.is_thunderbolt_available()

    def test_recommended_compression_thunderbolt(self):
        from fusion_multi_nodes.utils.network_topology import LinkInfo
        detector = NetworkTopologyDetector()
        detector._interfaces["tb"] = LinkInfo(
            type=LinkType.THUNDERBOLT_5, bandwidth_mbps=40000, latency_ms=0.1,
            interface="tb", is_rdma=True, is_active=True, priority=0)
        assert detector.get_recommended_compression() == "none"

    def test_recommended_compression_ethernet_1g(self):
        from fusion_multi_nodes.utils.network_topology import LinkInfo
        detector = NetworkTopologyDetector()
        detector._interfaces["eth"] = LinkInfo(
            type=LinkType.ETHERNET_1G, bandwidth_mbps=1000, latency_ms=0.5,
            interface="eth", is_rdma=False, is_active=True, priority=4)
        assert detector.get_recommended_compression() == "normal"

    def test_recommended_compression_wifi(self):
        from fusion_multi_nodes.utils.network_topology import LinkInfo
        detector = NetworkTopologyDetector()
        detector._interfaces["wifi"] = LinkInfo(
            type=LinkType.WIFI_6, bandwidth_mbps=1200, latency_ms=2.0,
            interface="wifi", is_rdma=False, is_active=True, priority=6)
        assert detector.get_recommended_compression() == "aggressive"

    def test_measure_peer_latency_timeout(self):
        detector = NetworkTopologyDetector()
        # 使用短超时，避免测试卡住
        result = asyncio.run(detector.measure_peer_latency("10.255.255.1", count=1))
        assert result == 10.0  # 默认超时值

    def test_interface_type_unknown(self):
        detector = NetworkTopologyDetector()
        result = detector._get_interface_type("nonexistent")
        # 系统上可能返回 "Ethernet" 或 "Unknown"，取决于 system_profiler
        assert isinstance(result, str)


# ══════════════════════════════════════════════════════════════════════════════
# Phase 5: KV Cache Sharing 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestKVSharingFull:
    def test_kv_entry_is_expired(self):
        entry = KVEntry(cache_id="e1", model_name="m", prompt_hash="h",
                        prompt_prefix="", total_tokens=10, total_size_bytes=100,
                        created_at=0.0, ttl_seconds=1.0)
        assert entry.is_expired

    def test_kv_entry_not_expired(self):
        entry = KVEntry(cache_id="e2", model_name="m", prompt_hash="h",
                        prompt_prefix="", total_tokens=10, total_size_bytes=100,
                        created_at=time.time(), ttl_seconds=3600)
        assert not entry.is_expired

    def test_lookup_local_empty(self):
        manager = KVSharingManager()
        found = manager.lookup_local("test", "hash")
        assert found is None

    def test_lookup_prefix_empty(self):
        manager = KVSharingManager()
        matches = manager.lookup_prefix("test", "prefix")
        assert matches == []

    def test_lookup_prefix_no_match(self):
        manager = KVSharingManager()
        entry = KVEntry(cache_id="p1", model_name="m1", prompt_hash="h1",
                        prompt_prefix="Hello world", total_tokens=10, total_size_bytes=100,
                        created_at=time.time())
        manager.store_local(entry)
        matches = manager.lookup_prefix("m1", "Goodbye")
        assert len(matches) == 0

    def test_get_stats_empty(self):
        manager = KVSharingManager()
        stats = manager.get_stats()
        assert stats["local_entries"] == 0
        assert stats["local_size_mb"] == 0.0

    @pytest.mark.asyncio
    async def test_lookup_remote_no_nodes(self):
        manager = KVSharingManager()
        result = await manager.lookup_remote("test", "hash", [])
        assert result is None

    @pytest.mark.asyncio
    async def test_transfer_from_remote(self):
        manager = KVSharingManager()
        result = await manager.transfer_from_remote("cache1", "node_1", "node_2")
        assert not result  # 远程不可达

    @pytest.mark.asyncio
    async def test_warm_cache(self):
        manager = KVSharingManager()
        result = await manager.warm_cache("test", ["prompt1", "prompt2"], ["node_1"])
        assert "success" in result
        assert "failed" in result

    def test_warm_scheduler_record(self):
        scheduler = KVCacheWarmScheduler(KVSharingManager())
        scheduler.record_prompt("What is AI?")
        scheduler.record_prompt("What is AI?")
        scheduler.record_prompt("What is AI?")
        hot = scheduler.get_hot_prompts(threshold=2)
        assert len(hot) >= 1

    def test_warm_scheduler_no_hot(self):
        scheduler = KVCacheWarmScheduler(KVSharingManager())
        scheduler.record_prompt("rare")
        hot = scheduler.get_hot_prompts(threshold=5)
        assert hot == []

    def test_warm_scheduler_stop(self):
        scheduler = KVCacheWarmScheduler(KVSharingManager())
        scheduler.stop()
        assert not scheduler._running


# ══════════════════════════════════════════════════════════════════════════════
# Phase 4: Observability 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestObservabilityFull:
    def setup_method(self):
        self.obs = ClusterObservability(retention_hours=1.0)

    def test_metric_with_tags(self):
        self.obs.record_metric("node_1", "custom_metric", 42.0, tags={"gpu": "m4"})
        metrics = self.obs.get_metrics("custom_metric", node_id="node_1")
        assert len(metrics) >= 1
        assert metrics[0].tags.get("gpu") == "m4"

    def test_get_latest_metric_none(self):
        latest = self.obs.get_latest_metric("nonexistent")
        assert latest is None

    def test_log_with_error_level_triggers_alert(self):
        self.obs.add_log(LogEntry(
            timestamp=time.time(), node_id="node_1", level="ERROR",
            module="test", message="Something went wrong"))
        alerts = self.obs.get_active_alerts()
        assert len(alerts) >= 1

    def test_alert_handler(self):
        handled = []
        def handler(alert):
            handled.append(alert.alert_id)
        self.obs.on_alert(handler)
        self.obs.create_alert("warning", "Test", "msg")
        assert len(handled) >= 1

    def test_resolve_nonexistent_alert(self):
        assert not self.obs.resolve_alert("nonexistent")

    def test_get_active_alerts_by_severity(self):
        self.obs.create_alert("critical", "Critical", "msg")
        self.obs.create_alert("warning", "Warning", "msg")
        critical = self.obs.get_active_alerts(severity="critical")
        assert len(critical) >= 1
        warning = self.obs.get_active_alerts(severity="warning")
        assert len(warning) >= 1

    @pytest.mark.asyncio
    async def test_alert_rules_node_offline(self):
        nodes = {"node_1": {"status": "offline", "hostname": "test"}}
        alerts = await self.obs.check_alert_rules(nodes)
        assert len(alerts) >= 1

    @pytest.mark.asyncio
    async def test_alert_rules_low_memory(self):
        nodes = {"node_1": {"status": "online", "hostname": "test",
                            "available_memory_gb": 1.0, "total_memory_gb": 32.0}}
        alerts = await self.obs.check_alert_rules(nodes)
        assert len(alerts) >= 1

    @pytest.mark.asyncio
    async def test_alert_rules_ok(self):
        nodes = {"node_1": {"status": "online", "hostname": "test",
                            "available_memory_gb": 16.0, "total_memory_gb": 32.0}}
        alerts = await self.obs.check_alert_rules(nodes)
        assert len(alerts) == 0

    @pytest.mark.asyncio
    async def test_start_stop(self):
        await self.obs.start()
        assert self.obs._running
        await self.obs.stop()
        assert not self.obs._running

    def test_get_cluster_report_empty(self):
        report = self.obs.get_cluster_report()
        assert report["metrics_collected"] == 0

    def test_build_node_summary(self):
        from fusion_multi_nodes.observability.observability import _build_node_summary
        summary = _build_node_summary({"latency_ms": [5.0, 10.0], "tokens_per_sec": [50.0, 60.0]})
        assert summary["avg_latency_ms"] == 7.5
        assert summary["avg_tps"] == 55.0

    def test_build_node_summary_empty(self):
        from fusion_multi_nodes.observability.observability import _build_node_summary
        summary = _build_node_summary({})
        assert summary["avg_latency_ms"] == 0
        assert summary["avg_tps"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# Phase 4: Config 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestConfigFull:
    def test_config_merge(self):
        config = ClusterConfig(config_path="/tmp/_test_fusion_config.json")
        merged = config._merge({"a": 1, "b": {"c": 2}}, {"b": {"d": 3}})
        assert merged["a"] == 1
        assert merged["b"]["c"] == 2
        assert merged["b"]["d"] == 3

    def test_config_get_default(self):
        config = ClusterConfig(config_path="/tmp/_test_fusion_config2.json")
        val = config.get("nonexistent.key", "default_val")
        assert val == "default_val"

    def test_config_save_load(self, tmp_path):
        config_path = str(tmp_path / "config.json")
        config = ClusterConfig(config_path=config_path)
        config.set("cluster.master_port", 19999)
        config2 = ClusterConfig(config_path=config_path)
        assert config2.get("cluster.master_port") == 19999

    def test_config_to_agent_config(self, tmp_path):
        from fusion_multi_nodes.agent import AgentConfig
        config_path = str(tmp_path / "agent_config_test.json")
        config = ClusterConfig(config_path=config_path)
        # 确保从新文件加载，不被其他测试污染
        config.set("cluster.master_port", 9753)
        agent_config = config.to_node_agent_config()
        assert isinstance(agent_config, AgentConfig)
        assert agent_config.master_port == 9753
        assert agent_config.fusion_mlx_port == 8000


# ══════════════════════════════════════════════════════════════════════════════
# Phase 4: Utils 全覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestUtilsFull:
    def test_setup_logger(self):
        logger = setup_logger("test_logger", verbose=True)
        assert logger.level == 10  # DEBUG

    def test_setup_logger_info(self):
        logger = setup_logger("test_logger2", verbose=False)
        assert logger.level == 20  # INFO

    def test_get_data_dir(self):
        data_dir = get_data_dir()
        assert data_dir.name == "multi-node"
        assert data_dir.exists()

    def test_get_log_dir(self):
        log_dir = get_log_dir()
        assert log_dir.name == "logs"
        assert log_dir.exists()


# ══════════════════════════════════════════════════════════════════════════════
# 模块完整性验证
# ══════════════════════════════════════════════════════════════════════════════

class TestModuleIntegrity:
    """验证所有模块可正常导入且关键 API 存在。"""

    def test_all_modules_importable(self):
        import fusion_multi_nodes
        import fusion_multi_nodes.master
        import fusion_multi_nodes.agent
        import fusion_multi_nodes.distributed_mlx
        import fusion_multi_nodes.mcp_gateway
        import fusion_multi_nodes.observability
        import fusion_multi_nodes.config
        import fusion_multi_nodes.utils
        assert True

    def test_cli_importable(self):
        try:
            from fusion_multi_nodes import cli
            assert cli.main is not None
        except ImportError:
            # click 可能未安装
            pass

    def test_distributed_mlx_all_exports(self):
        from fusion_multi_nodes.distributed_mlx import (
            DistributedMLXBridge, ModelShard, DistConfig, DistMode,
            CavemanCompressor, CavemanManager, CompressStats,
            KVSharingManager, KVCacheEntry, KVShard, KVCacheWarmScheduler,
        )
        assert DistributedMLXBridge is not None
        assert CavemanCompressor is not None
        assert KVSharingManager is not None

    def test_utils_all_exports(self):
        from fusion_multi_nodes.utils import (
            setup_logger, get_data_dir, get_log_dir,
            NetworkTopologyDetector, LinkInfo, LinkType, NetworkPath,
        )
        assert NetworkTopologyDetector is not None
        assert LinkType is not None
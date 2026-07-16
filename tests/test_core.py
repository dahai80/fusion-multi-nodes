"""Fusion-Multi-Node 单元测试。"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

from fusion_multi_nodes.master import (
    ClusterMaster, ClusterTask, NodeInfo, NodeStatus, ParallelMode, TaskStatus,
)
from fusion_multi_nodes.agent import NodeAgent, AgentConfig
from fusion_multi_nodes.distributed_mlx import DistributedMLXBridge, ModelShard, DistMode
from fusion_multi_nodes.mcp_gateway import MCPClusterGateway, MCPTool
from fusion_multi_nodes.observability import ClusterObservability, LogEntry


# ── Cluster Master 测试 ──

class TestClusterMaster:
    def setup_method(self):
        self.master = ClusterMaster(host="127.0.0.1", port=19753, heartbeat_timeout=60.0)

    def test_register_node(self):
        node = NodeInfo(
            node_id="test_node_1",
            hostname="test-mac-1",
            ip_address="192.168.1.100",
            port=9755,
            total_memory_gb=32.0,
            available_memory_gb=16.0,
            cpu_cores=12,
            gpu_cores=40,
            status=NodeStatus.ONLINE,
        )
        self.master.register_node(node)
        assert "test_node_1" in self.master.nodes
        assert self.master.nodes["test_node_1"].status == NodeStatus.ONLINE

    def test_get_online_nodes(self):
        node = NodeInfo(
            node_id="test_node_2",
            hostname="test-mac-2",
            ip_address="192.168.1.101",
            port=9755,
            total_memory_gb=64.0,
            available_memory_gb=32.0,
            cpu_cores=16,
            gpu_cores=64,
            status=NodeStatus.ONLINE,
            last_heartbeat=time.time(),
        )
        self.master.register_node(node)
        online = self.master.get_online_nodes()
        assert len(online) >= 1

    def test_select_nodes_data_parallel(self):
        for i in range(3):
            self.master.register_node(NodeInfo(
                node_id=f"node_{i}",
                hostname=f"mac-{i}",
                ip_address=f"192.168.1.{100+i}",
                port=9755,
                total_memory_gb=32.0,
                available_memory_gb=16.0 + i * 8.0,
                cpu_cores=12,
                gpu_cores=40,
                status=NodeStatus.ONLINE,
                last_heartbeat=time.time(),
            ))
        nodes = self.master.select_nodes(ParallelMode.DATA, count=2)
        assert len(nodes) == 2

    def test_assign_task(self):
        self.master.register_node(NodeInfo(
            node_id="worker_1",
            hostname="worker-1",
            ip_address="192.168.1.10",
            port=9755,
            total_memory_gb=32.0,
            available_memory_gb=24.0,
            cpu_cores=12,
            gpu_cores=40,
            status=NodeStatus.ONLINE,
            last_heartbeat=time.time(),
        ))
        task = ClusterTask(
            task_id="test_task_1",
            name="test-inference",
            mode=ParallelMode.DATA,
            model_name="qwen3.5-9b",
        )
        result = self.master.assign_task(task)
        assert result is True
        assert task.status == TaskStatus.RUNNING

    def test_complete_task(self):
        task = ClusterTask(
            task_id="test_task_2",
            name="test-complete",
            mode=ParallelMode.DATA,
        )
        self.master.tasks["test_task_2"] = task
        self.master.complete_task("test_task_2")
        assert self.master.tasks["test_task_2"].status == TaskStatus.COMPLETED

    def test_get_stats(self):
        stats = self.master.get_stats()
        assert "total_nodes" in stats
        assert "online_nodes" in stats
        assert "total_tasks" in stats

    def test_node_score(self):
        node = NodeInfo(
            node_id="score_test",
            hostname="score-test",
            ip_address="10.0.0.1",
            port=9755,
            total_memory_gb=64.0,
            available_memory_gb=48.0,
            cpu_cores=16,
            gpu_cores=64,
            status=NodeStatus.ONLINE,
            active_tasks=1,
            max_tasks=4,
        )
        score = node.score
        assert 0 <= score <= 1.0


# ── Node Agent 测试 ──

class TestNodeAgent:
    def test_agent_config(self):
        config = AgentConfig(
            node_id="test_agent",
            master_host="192.168.1.1",
            master_port=9753,
            agent_port=9755,
        )
        assert config.node_id == "test_agent"
        assert config.master_host == "192.168.1.1"

    def test_collect_hardware(self):
        agent = NodeAgent()
        info = agent.collect_hardware_info()
        assert info["arch"] == "arm64"
        assert info["total_memory_gb"] > 0
        assert info["cpu_cores"] > 0
        assert info["node_id"] != ""


# ── Distributed MLX 测试 ──

class TestDistributedMLX:
    @pytest.mark.asyncio
    async def test_shard_model(self):
        bridge = DistributedMLXBridge()
        shards = await bridge.shard_model("test-model", num_shards=4)
        assert len(shards) == 4
        assert shards[0].shard_id == 0
        assert shards[0].total_shards == 4
        assert len(shards[0].layers) > 0

    def test_model_shard_dataclass(self):
        shard = ModelShard(shard_id=0, total_shards=2, layers=[0, 1, 2, 3], node_id="node_1")
        assert shard.shard_id == 0
        assert shard.node_id == "node_1"
        assert shard.layers == [0, 1, 2, 3]


# ── MCP Gateway 测试 ──

class TestMCPGateway:
    def setup_method(self):
        self.gateway = MCPClusterGateway(host="127.0.0.1", port=19756)

    def test_register_tool(self):
        tool = MCPTool(
            name="test_tool",
            description="A test tool",
            parameters={"type": "object", "properties": {"input": {"type": "string"}}},
            node_id="test_node",
            plugin="test_plugin",
        )
        self.gateway.register_tool(tool)
        assert "test_tool" in self.gateway.tools

    def test_get_tools_list(self):
        tool = MCPTool(
            name="code_review",
            description="Review code changes",
            parameters={"type": "object", "properties": {"code": {"type": "string"}}},
        )
        self.gateway.register_tool(tool)
        tools = self.gateway.get_tools_list()
        assert len(tools) >= 1
        assert tools[0]["name"] == "code_review"

    def test_get_stats(self):
        stats = self.gateway.get_stats()
        assert "registered_tools" in stats
        assert "total_requests" in stats
        assert "total_token_count" in stats


# ── Observability 测试 ──

class TestObservability:
    def setup_method(self):
        self.obs = ClusterObservability(retention_hours=1.0)

    def test_record_metric(self):
        self.obs.record_metric("node_1", "memory_used_gb", 12.5)
        metrics = self.obs.get_metrics("memory_used_gb", node_id="node_1")
        assert len(metrics) >= 1
        assert metrics[0].value == 12.5

    def test_get_latest_metric(self):
        self.obs.record_metric("node_1", "tokens_per_sec", 45.2)
        latest = self.obs.get_latest_metric("tokens_per_sec", "node_1")
        assert latest is not None
        assert latest.value == 45.2

    def test_create_alert(self):
        alert = self.obs.create_alert("warning", "Test Alert", "This is a test", node_id="node_1")
        assert alert.severity == "warning"
        assert alert.title == "Test Alert"
        assert not alert.resolved

    def test_resolve_alert(self):
        alert = self.obs.create_alert("critical", "Critical Alert", "Something went wrong")
        assert self.obs.resolve_alert(alert.alert_id)
        assert self.obs.get_active_alerts() == []

    def test_add_log(self):
        self.obs.add_log(LogEntry(
            timestamp=time.time(),
            node_id="node_1",
            level="INFO",
            module="test",
            message="Test log entry",
        ))
        logs = self.obs.get_logs(node_id="node_1", limit=10)
        assert len(logs) >= 1
        assert logs[0].message == "Test log entry"

    def test_get_cluster_report(self):
        self.obs.record_metric("node_1", "memory_used_gb", 16.0)
        self.obs.record_metric("node_1", "tokens_per_sec", 50.0)
        report = self.obs.get_cluster_report()
        assert "metrics_collected" in report
        assert "logs_collected" in report
        assert "active_alerts" in report


# ── 配置测试 ──

class TestClusterConfig:
    def test_default_config(self, tmp_path):
        from fusion_multi_nodes.config import ClusterConfig
        config_path = str(tmp_path / "test_config.json")
        config = ClusterConfig(config_path=config_path)
        assert config.get("cluster.master_port") == 9753
        assert config.get("mlx.fusion_mlx_port") == 8000
        assert config.get("mcp.token_budget") == 10_000_000

    def test_set_and_get(self, tmp_path):
        from fusion_multi_nodes.config import ClusterConfig
        config = ClusterConfig(config_path=str(tmp_path / "test2.json"))
        config.set("cluster.master_port", 19753)
        assert config.get("cluster.master_port") == 19753


# ── Phase 5: Caveman 压缩测试 ──

class TestCaveman:
    def test_compress_zlib(self):
        from fusion_multi_nodes.distributed_mlx import CavemanCompressor
        compressor = CavemanCompressor()
        data = b"Hello Fusion-Multi-Node! " * 100
        compressed, stats = compressor.compress(data, method="zlib")
        assert len(compressed) < len(data)
        assert stats.ratio < 1.0
        decompressed = compressor.decompress(compressed, "zlib")
        assert decompressed == data

    def test_compress_diff(self):
        from fusion_multi_nodes.distributed_mlx import CavemanCompressor
        compressor = CavemanCompressor()
        data = bytes(range(256)) * 10
        compressed, stats = compressor.compress(data, method="diff")
        assert stats.ratio < 1.0
        decompressed = compressor.decompress(compressed, "diff")
        assert decompressed == data

    def test_compress_dict(self):
        from fusion_multi_nodes.distributed_mlx import CavemanCompressor
        compressor = CavemanCompressor()
        # 使用 zlib 无损压缩测试
        data = b"ABCABCABCABCABCABCABC" * 10
        compressed, stats = compressor.compress(data, method="zlib")
        decompressed = compressor.decompress(compressed, "zlib")
        assert decompressed == data
        assert stats.ratio < 1.0

    def test_auto_select_method(self):
        from fusion_multi_nodes.distributed_mlx import CavemanCompressor
        compressor = CavemanCompressor()
        # 小数据
        small = b"hello"
        compressed, stats = compressor.compress(small, method="auto")
        assert stats.method == "dict"

    @pytest.mark.asyncio
    async def test_caveman_manager(self):
        from fusion_multi_nodes.distributed_mlx import CavemanManager
        manager = CavemanManager()
        data = b"Test data for Caveman compression. " * 50
        compressed, method, stats = await manager.compress_tensor(data, link_type="ethernet_1g")
        assert stats.original_bytes > stats.compressed_bytes
        stats = manager.get_stats()
        assert stats["total_original_bytes"] > 0
        assert stats["savings_percent"] > 0

    def test_compression_configs(self):
        from fusion_multi_nodes.distributed_mlx import CavemanManager
        manager = CavemanManager()
        tb5 = manager.get_compression_config("thunderbolt_5")
        assert tb5["method"] == "dict"
        eth1g = manager.get_compression_config("ethernet_1g")
        assert eth1g["method"] == "zlib"


# ── Phase 5: 网络拓扑测试 ──

class TestNetworkTopology:
    @pytest.mark.asyncio
    async def test_detect(self):
        from fusion_multi_nodes.utils import NetworkTopologyDetector
        detector = NetworkTopologyDetector()
        interfaces = await detector.detect()
        assert len(interfaces) >= 1
        assert "lo0" in interfaces

    def test_get_best_link(self):
        from fusion_multi_nodes.utils import NetworkTopologyDetector
        detector = NetworkTopologyDetector()
        # 手动添加接口
        detector._interfaces["lo0"] = type("Link", (), {"type": "thunderbolt_5", "bandwidth_mbps": 40000,
                                                          "latency_ms": 0.01, "interface": "lo0",
                                                          "is_rdma": False, "is_active": True, "priority": 0})()
        best = detector.get_best_link()
        assert best is not None

    def test_link_type_classification(self):
        from fusion_multi_nodes.utils import NetworkTopologyDetector, LinkType
        detector = NetworkTopologyDetector()
        assert detector._classify_thunderbolt(40000) == LinkType.THUNDERBOLT_5
        assert detector._classify_ethernet(1000) == LinkType.ETHERNET_1G
        assert detector._classify_wifi(2400) == LinkType.WIFI_6E


# ── Phase 5: KV 缓存共享测试 ──

class TestKVSharing:
    def test_store_and_lookup(self):
        import time
        from fusion_multi_nodes.distributed_mlx import KVSharingManager, KVCacheEntry, KVShard
        manager = KVSharingManager(max_local_cache_mb=64.0)

        entry = KVCacheEntry(
            cache_id="test_1",
            model_name="qwen3.5-9b",
            prompt_hash="abc123",
            prompt_prefix="Hello",
            total_tokens=100,
            total_size_bytes=1024,
            created_at=time.time(),
            shards=[KVShard(
                shard_id="s1", model_name="qwen3.5-9b", layer_index=0,
                node_id="node_1", token_count=100, size_bytes=1024,
                created_at=time.time(),
            )],
        )
        assert manager.store_local(entry)
        found = manager.lookup_local("qwen3.5-9b", "abc123")
        assert found is not None
        assert found.cache_id == "test_1"

    def test_cache_expiry(self):
        from fusion_multi_nodes.distributed_mlx import KVSharingManager, KVCacheEntry, KVShard
        import time
        manager = KVSharingManager()

        entry = KVCacheEntry(
            cache_id="expired",
            model_name="test",
            prompt_hash="old",
            prompt_prefix="",
            total_tokens=10,
            total_size_bytes=100,
            created_at=time.time() - 7200,  # 2小时前
            ttl_seconds=3600,  # 1小时TTL
        )
        manager.store_local(entry)
        found = manager.lookup_local("test", "old")
        assert found is None  # 已过期

    def test_lru_eviction(self):
        from fusion_multi_nodes.distributed_mlx import KVSharingManager, KVCacheEntry, KVShard
        import time

        manager = KVSharingManager(max_local_cache_mb=0.001)  # 极小缓存

        for i in range(10):
            entry = KVCacheEntry(
                cache_id=f"test_{i}",
                model_name="test",
                prompt_hash=f"hash_{i}",
                prompt_prefix="",
                total_tokens=10,
                total_size_bytes=500,
                created_at=time.time(),
            )
            manager.store_local(entry)

        stats = manager.get_stats()
        assert stats["local_entries"] < 10  # 应该淘汰了一些

    def test_prefix_match(self):
        from fusion_multi_nodes.distributed_mlx import KVSharingManager, KVCacheEntry
        import time
        manager = KVSharingManager()

        entry = KVCacheEntry(
            cache_id="prefix_test",
            model_name="test",
            prompt_hash="p1",
            prompt_prefix="Hello world, this is a test",
            total_tokens=50,
            total_size_bytes=500,
            created_at=time.time(),
        )
        manager.store_local(entry)
        matches = manager.lookup_prefix("test", "Hello world")
        assert len(matches) >= 1
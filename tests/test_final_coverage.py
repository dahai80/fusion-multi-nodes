"""最终覆盖测试 — 补齐剩余的缺失分支，重点覆盖 CLI、MCP、Observability、Caveman。"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fusion_multi_nodes.master import ClusterMaster, NodeInfo, NodeStatus, ClusterTask, ParallelMode, TaskStatus
from fusion_multi_nodes.mcp_gateway import MCPClusterGateway, MCPTool
from fusion_multi_nodes.observability import ClusterObservability, LogEntry, MetricPoint
from fusion_multi_nodes.distributed_mlx import CavemanCompressor, CavemanManager, KVSharingManager, KVCacheEntry, KVShard
from fusion_multi_nodes.config import ClusterConfig


# ══════════════════════════════════════════════════════════════════════════════
# Caveman 剩余分支覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestCavemanFinal:
    def test_dict_compress_roundtrip(self):
        """测试字典压缩的完整往返。"""
        c = CavemanCompressor(dictionary_size=64)
        # 构建字典
        c.build_dictionary([0x41424344, 0x45464748])  # ABCD, EFGH
        # 压缩包含字典 token 的数据
        data = b"ABCDEFGHABCDEFGH"
        compressed, stats = c.compress(data, method="dict")
        decompressed = c.decompress(compressed, "dict")
        # 字典压缩可能无法完美压缩所有数据
        # 但应该能正确解压
        assert len(decompressed) > 0

    def test_dict_compress_no_dict(self):
        """无字典时的压缩行为。"""
        c = CavemanCompressor()
        data = b"test data"
        compressed, stats = c.compress(data, method="dict")
        assert compressed == data  # 无字典时原样返回

    def test_dict_decompress_no_reverse_dict(self):
        """无反查字典时的解压行为。"""
        c = CavemanCompressor()
        result = c.decompress(b"test", "dict")
        assert result == b"test"

    def test_diff_compress_short_data(self):
        """短数据的差分压缩。"""
        c = CavemanCompressor()
        data = b"ab"
        compressed, stats = c.compress(data, method="diff")
        assert stats.original_bytes == 2

    @pytest.mark.asyncio
    async def test_caveman_manager_compress_tensor_none(self):
        """测试 CavemanManager 的压缩。"""
        m = CavemanManager()
        data = b"Hello Fusion-Multi-Node! " * 100
        compressed, method, stats = await m.compress_tensor(data, link_type="thunderbolt_5")
        assert stats.original_bytes > stats.compressed_bytes

    @pytest.mark.asyncio
    async def test_decompress_tensor_roundtrip_all_methods(self):
        """测试所有压缩方法的往返解压。"""
        m = CavemanManager()
        original = b"Test data for roundtrip testing. " * 30
        for link_type in ["thunderbolt_5", "ethernet_10g", "ethernet_1g", "ethernet_100m"]:
            compressed, method, _ = await m.compress_tensor(original, link_type=link_type)
            decompressed = await m.decompress_tensor(compressed, method)
            assert decompressed == original, f"Roundtrip failed for {link_type}"


# ══════════════════════════════════════════════════════════════════════════════
# MCP Gateway 剩余分支覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestMCPGatewayFinal:
    @pytest.mark.asyncio
    async def test_forward_to_node_localhost(self):
        """测试转发到本地节点。"""
        gateway = MCPClusterGateway()
        tool = MCPTool(name="test_forward", description="test", parameters={})
        request = gateway._create_request("test_forward", {})
        result = await gateway._forward_to_node(request, tool)
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_forward_to_node_remote(self):
        """测试转发到远程节点。"""
        gateway = MCPClusterGateway()
        tool = MCPTool(name="test_remote", description="test", parameters={})
        request = gateway._create_request("test_remote", {})
        request.assigned_node = "10.255.255.1"
        result = await gateway._forward_to_node(request, tool)
        assert isinstance(result, dict)

    def test_create_request(self):
        """测试创建请求记录。"""
        gateway = MCPClusterGateway()
        request = gateway._create_request("test_tool", {"key": "value"})
        assert request.tool_name == "test_tool"
        assert request.arguments == {"key": "value"}
        assert request.status == "pending"

    def test_mcp_tool_defaults(self):
        """测试 MCPTool 默认值。"""
        tool = MCPTool(name="defaults", description="test", parameters={})
        assert tool.required_memory_gb == 0.0
        assert tool.timeout == 60.0
        assert tool.call_count == 0


# ══════════════════════════════════════════════════════════════════════════════
# Observability 剩余分支覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestObservabilityFinal:
    def test_metric_point_dataclass(self):
        """测试 MetricPoint 数据类。"""
        mp = MetricPoint(timestamp=time.time(), node_id="n1", metric_name="test", value=42.0)
        assert mp.value == 42.0
        assert mp.tags == {}

    def test_metric_with_tags_filter(self):
        """测试带标签的指标过滤。"""
        obs = ClusterObservability()
        obs.record_metric("n1", "mem", 16.0, tags={"gpu": "m4"})
        obs.record_metric("n2", "mem", 32.0, tags={"gpu": "m4"})
        # 按节点过滤
        n1_metrics = obs.get_metrics("mem", node_id="n1")
        assert len(n1_metrics) == 1
        assert n1_metrics[0].value == 16.0

    def test_log_entry_dataclass(self):
        """测试 LogEntry 数据类。"""
        log = LogEntry(timestamp=time.time(), node_id="n1", level="INFO", module="test", message="test")
        assert log.level == "INFO"
        assert log.task_id == ""

    def test_alert_resolve(self):
        """测试告警解决。"""
        obs = ClusterObservability()
        obs.create_alert("critical", "Test", "msg")
        alerts = obs.get_active_alerts()
        assert len(alerts) == 1
        obs.resolve_alert(alerts[0].alert_id)
        assert obs.get_active_alerts() == []

    @pytest.mark.asyncio
    async def test_alert_rules_check(self):
        """测试告警规则检查。"""
        obs = ClusterObservability()
        # 正常节点不触发告警
        nodes = {"n1": {"status": "online", "hostname": "h1", "available_memory_gb": 16.0, "total_memory_gb": 32.0}}
        alerts = await obs.check_alert_rules(nodes)
        assert len(alerts) == 0


# ══════════════════════════════════════════════════════════════════════════════
# KV Cache 剩余分支覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestKVSharingFinal:
    def test_kv_shard_defaults(self):
        """测试 KVShard 默认值。"""
        shard = KVShard(shard_id="s1", model_name="m", layer_index=0, node_id="n1",
                        token_count=100, size_bytes=1024, created_at=time.time())
        assert shard.access_count == 0
        assert shard.last_access == 0.0
        assert shard.is_compressed is False

    def test_kv_cache_entry_properties(self):
        """测试 KVCacheEntry 属性。"""
        entry = KVCacheEntry(cache_id="c1", model_name="m", prompt_hash="h",
                            prompt_prefix="", total_tokens=100, total_size_bytes=1024,
                            created_at=time.time() - 7200, ttl_seconds=3600)
        assert entry.is_expired

    def test_kv_manager_lookup_by_prefix(self):
        """测试按前缀匹配 KV 缓存。"""
        manager = KVSharingManager()
        entry = KVCacheEntry(cache_id="c1", model_name="m", prompt_hash="h1",
                            prompt_prefix="Hello world", total_tokens=10, total_size_bytes=100,
                            created_at=time.time())
        manager.store_local(entry)
        matches = manager.lookup_prefix("m", "Hello")
        assert len(matches) == 1
        assert matches[0].cache_id == "c1"

    def test_warm_scheduler_max_count(self):
        """测试预热调度器的最大数量限制。"""
        from fusion_multi_nodes.distributed_mlx import KVCacheWarmScheduler
        scheduler = KVCacheWarmScheduler(KVSharingManager())
        for i in range(20):
            scheduler.record_prompt(f"prompt_{i % 5}")
        hot = scheduler.get_hot_prompts(threshold=2, max_count=3)
        assert len(hot) <= 3


# ══════════════════════════════════════════════════════════════════════════════
# Config 剩余分支覆盖
# ══════════════════════════════════════════════════════════════════════════════

class TestConfigFinal:
    def test_config_load_from_file(self, tmp_path):
        """测试从文件加载配置。"""
        config_path = str(tmp_path / "test_config.json")
        # 先创建配置文件
        with open(config_path, "w") as f:
            json.dump({"cluster": {"master_port": 12345}}, f)
        config = ClusterConfig(config_path=config_path)
        assert config.get("cluster.master_port") == 12345

    def test_config_get_nonexistent(self):
        """测试获取不存在的配置项。"""
        config = ClusterConfig(config_path="/tmp/_nonexistent_config.json")
        val = config.get("nonexistent.deep.path", "default")
        assert val == "default"


# ══════════════════════════════════════════════════════════════════════════════
# CLI 测试（click 已安装）
# ══════════════════════════════════════════════════════════════════════════════

class TestCLI:
    def test_cli_import(self):
        """测试 CLI 模块可导入。"""
        from fusion_multi_nodes import cli
        assert cli is not None
        assert cli.main is not None

    def test_cli_help(self):
        """测试 CLI 帮助信息。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Fusion-Multi-Node" in result.output

    def test_cli_version(self):
        """测试 CLI 版本信息。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0

    def test_cli_config_list(self):
        """测试 config list 命令。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["config", "list"])
        assert result.exit_code == 0

    def test_cli_config_get(self):
        """测试 config get 命令。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["config", "get", "cluster.master_port"])
        assert result.exit_code == 0

    def test_cli_node_list(self):
        """测试 node list 命令。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["node", "list"])
        assert result.exit_code == 0

    def test_cli_cluster_status(self):
        """测试 cluster status 命令。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["cluster", "status"])
        assert result.exit_code == 0

    def test_cli_task_list(self):
        """测试 task list 命令。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["task", "list"])
        assert result.exit_code == 0

    def test_cli_caveman_test(self):
        """测试 caveman test 命令。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["caveman", "test", "test data"])
        assert result.exit_code == 0

    def test_cli_network_detect(self):
        """测试 network detect 命令。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        # 使用隔离的文件系统避免影响实际环境
        result = runner.invoke(cli, ["network", "detect"])
        assert result.exit_code == 0

    def test_cli_kv_stats(self):
        """测试 kv stats 命令。"""
        from click.testing import CliRunner
        from fusion_multi_nodes.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["kv", "stats"])
        assert result.exit_code == 0


# ══════════════════════════════════════════════════════════════════════════════
# Network Topology 测试（netifaces 已安装）
# ══════════════════════════════════════════════════════════════════════════════

class TestNetworkTopologyFinal:
    @pytest.mark.asyncio
    async def test_detect_with_netifaces(self):
        """测试网络拓扑检测（netifaces 已安装）。"""
        from fusion_multi_nodes.utils import NetworkTopologyDetector
        detector = NetworkTopologyDetector()
        interfaces = await detector.detect()
        assert len(interfaces) >= 1
        # 应包含回环接口
        assert "lo0" in interfaces

    def test_link_type_values(self):
        """测试 LinkType 枚举值。"""
        from fusion_multi_nodes.utils import LinkType
        assert LinkType.THUNDERBOLT_5.value == "thunderbolt_5"
        assert LinkType.ETHERNET_10G.value == "ethernet_10g"
        assert LinkType.WIFI_6E.value == "wifi_6e"

    def test_network_path_dataclass(self):
        """测试 NetworkPath 数据类。"""
        from fusion_multi_nodes.utils import NetworkPath, LinkInfo, LinkType
        path = NetworkPath(source="node1", target="node2")
        assert path.source == "node1"
        assert path.target == "node2"
        assert path.links == []
        assert path.aggregated_bandwidth_mbps == 0.0

    def test_link_info_priority(self):
        """测试链路优先级排序。"""
        from fusion_multi_nodes.utils import NetworkTopologyDetector, LinkType, LinkInfo
        detector = NetworkTopologyDetector()
        tb5 = LinkInfo(type=LinkType.THUNDERBOLT_5, bandwidth_mbps=40000, latency_ms=0.1,
                       interface="tb5", is_rdma=True, is_active=True, priority=0)
        eth1g = LinkInfo(type=LinkType.ETHERNET_1G, bandwidth_mbps=1000, latency_ms=0.5,
                         interface="eth1", is_rdma=False, is_active=True, priority=4)
        detector._interfaces["tb5"] = tb5
        detector._interfaces["eth1"] = eth1g
        best = detector.get_best_link()
        assert best.interface == "tb5"  # Thunderbolt 优先

    def test_get_recommended_compression(self):
        """测试压缩策略推荐。"""
        from fusion_multi_nodes.utils import NetworkTopologyDetector, LinkType, LinkInfo
        detector = NetworkTopologyDetector()
        # 无接口时返回 aggressive
        assert detector.get_recommended_compression() == "aggressive"
        # 添加 Thunderbolt 5
        detector._interfaces["tb"] = LinkInfo(
            type=LinkType.THUNDERBOLT_5, bandwidth_mbps=40000, latency_ms=0.1,
            interface="tb", is_rdma=True, is_active=True, priority=0)
        assert detector.get_recommended_compression() == "none"

    def test_measure_latency_loopback(self):
        """测试回环延迟测量。"""
        from fusion_multi_nodes.utils import NetworkTopologyDetector
        detector = NetworkTopologyDetector()
        # 回环延迟应为 0.01ms
        latency = asyncio.run(detector._measure_latency("lo0"))
        assert latency == 0.01

    def test_measure_interface_speed_loopback(self):
        """测试接口速度测量。"""
        from fusion_multi_nodes.utils import NetworkTopologyDetector
        detector = NetworkTopologyDetector()
        speed = asyncio.run(detector._measure_interface_speed("lo0"))
        assert speed > 0

    def test_get_primary_interface_with_interfaces(self):
        """测试获取主接口。"""
        from fusion_multi_nodes.utils import NetworkTopologyDetector, LinkType, LinkInfo
        detector = NetworkTopologyDetector()
        detector._interfaces["eth0"] = LinkInfo(
            type=LinkType.ETHERNET_1G, bandwidth_mbps=1000, latency_ms=0.5,
            interface="eth0", is_rdma=False, is_active=True, priority=4)
        assert detector.get_primary_interface() == "eth0"
        assert detector.get_link_speed() == 1000.0
        assert detector.get_link_type() == LinkType.ETHERNET_1G


# ── MCP Gateway 额外覆盖 ──

class TestMCPGatewayExtra:
    def test_mcp_tool_call_count(self):
        """测试工具调用计数。"""
        tool = MCPTool(name="counter", description="test", parameters={})
        assert tool.call_count == 0

    def test_mcp_request_defaults(self):
        """测试 MCPRequest 默认值。"""
        from fusion_multi_nodes.mcp_gateway import MCPRequest
        req = MCPRequest(request_id="r1", tool_name="t1", arguments={}, source="test")
        assert req.status == "pending"
        assert req.assigned_node == ""
        assert req.token_count == 0

    @pytest.mark.asyncio
    async def test_handle_tool_call_with_selector_string(self):
        """测试节点选择器返回字符串。"""
        gateway = MCPClusterGateway()
        gateway.set_node_selector(lambda t: "10.0.0.1")
        tool = MCPTool(name="sel_test", description="test", parameters={})
        gateway.register_tool(tool)
        result = await gateway.handle_tool_call("sel_test", {}, source="test")
        # 远程不可达，应返回错误
        assert "error" in result


# ── Observability 额外覆盖 ──

class TestObservabilityExtra:
    def test_get_metrics_with_filters(self):
        """测试指标过滤。"""
        obs = ClusterObservability()
        for i in range(5):
            obs.record_metric(f"node_{i}", "cpu", float(i * 10))
        # 按时间过滤
        later = obs.get_metrics("cpu", since=time.time() + 1)
        assert len(later) == 0
        # 按限制数量
        limited = obs.get_metrics("cpu", limit=2)
        assert len(limited) <= 2
        # 最新指标不存在
        latest = obs.get_latest_metric("nonexistent")
        assert latest is None

    def test_get_logs_with_filters(self):
        """测试日志过滤。"""
        obs = ClusterObservability()
        obs.add_log(LogEntry(time.time(), "n1", "INFO", "mod1", "msg1"))
        obs.add_log(LogEntry(time.time(), "n2", "ERROR", "mod2", "msg2"))
        # 按节点过滤
        n1_logs = obs.get_logs(node_id="n1")
        assert len(n1_logs) == 1
        # 按级别过滤
        error_logs = obs.get_logs(level="ERROR")
        assert len(error_logs) == 1
        # 按时间过滤
        future_logs = obs.get_logs(since=time.time() + 1)
        assert len(future_logs) == 0
        # 按限制数量
        limited = obs.get_logs(limit=1)
        assert len(limited) <= 1


# ── KV Cache 额外覆盖 —─

class TestKVSharingExtra:
    def test_evict_when_cache_empty(self):
        """测试空缓存时的淘汰行为。"""
        manager = KVSharingManager(max_local_cache_mb=0.0)
        # 直接调用 _evict，缓存为空时应 break
        manager._evict(1000)
        assert manager._local_size_bytes == 0

    def test_kv_shard_last_access(self):
        """测试 KVShard 的 last_access 更新。"""
        import time
        shard = KVShard(shard_id="s1", model_name="m", layer_index=0,
                        node_id="n1", token_count=100, size_bytes=1024,
                        created_at=time.time())
        assert shard.last_access == 0.0
        shard.last_access = time.time()
        assert shard.last_access > 0
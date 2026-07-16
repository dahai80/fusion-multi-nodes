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
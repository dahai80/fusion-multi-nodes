# Fusion-Multi-Node API Reference

## Core Modules

### Master (`fusion_multi_nodes.master`)

Cluster scheduling — node discovery, resource allocation, task lifecycle, KV cache.

```python
from fusion_multi_nodes.master import ClusterMaster, ClusterTask, NodeInfo, KVCacheEntry

master = ClusterMaster(host="0.0.0.0", port=9753, heartbeat_timeout=15.0)

# Register a node
node = NodeInfo(node_id="node_1", hostname="mac-studio-1", ip_address="10.0.0.1",
                port=9755, total_memory_gb=64.0, available_memory_gb=48.0,
                cpu_cores=16, gpu_cores=64, status=NodeStatus.ONLINE)
master.register_node(node)

# Submit a task
task = ClusterTask(task_id="task_1", name="batch-inference", mode=ParallelMode.DATA)
master.assign_task(task)

# Query stats
stats = master.get_stats()
```

### Agent (`fusion_multi_nodes.agent`)

Per-machine daemon — hardware reporting, heartbeat, task execution.

```python
from fusion_multi_nodes.agent import NodeAgent, AgentConfig

config = AgentConfig(node_id="my_mac", master_host="10.0.0.1", master_port=9753)
agent = NodeAgent(config)
await agent.start()

# Collect hardware info
info = agent.collect_hardware_info()
```

### Distributed MLX (`fusion_multi_nodes.distributed_mlx`)

Pipeline/data parallelism, model sharding, Caveman compression, KV cache sharing.

```python
from fusion_multi_nodes.distributed_mlx import (
    DistributedMLXBridge, CavemanManager, KVSharingManager
)

# Model sharding
bridge = DistributedMLXBridge()
shards = await bridge.shard_model("llama-70b", num_shards=4)

# Token compression
manager = CavemanManager()
compressed, method, stats = await manager.compress_tensor(data, link_type="ethernet_1g")

# KV cache sharing
kv = KVSharingManager(max_local_cache_mb=4096.0)
kv.store_local(entry)
```

### MCP Gateway (`fusion_multi_nodes.mcp_gateway`)

Unified MCP endpoint for Claude Desktop/Code integration.

```python
from fusion_multi_nodes.mcp_gateway import MCPClusterGateway, MCPTool

gateway = MCPClusterGateway(host="0.0.0.0", port=9756)

tool = MCPTool(name="code_review", description="Review code changes",
               parameters={"type": "object", "properties": {"code": {"type": "string"}}})
gateway.register_tool(tool)

# Handle Claude tool call
result = await gateway.handle_tool_call("code_review", {"code": "..."}, source="claude_code")
```

### Observability (`fusion_multi_nodes.observability`)

Metrics, logs, alerts, and cluster health monitoring.

```python
from fusion_multi_nodes.observability import ClusterObservability, LogEntry

obs = ClusterObservability(retention_hours=24.0)
obs.record_metric("node_1", "memory_used_gb", 16.0, tags={"gpu": "m4_ultra"})
obs.add_log(LogEntry(timestamp=time.time(), node_id="node_1",
                     level="INFO", module="scheduler", message="Task completed"))
alert = obs.create_alert("warning", "High memory usage", f"Node node_1 at 90%")
```

### Config (`fusion_multi_nodes.config`)

Global cluster configuration management.

```python
from fusion_multi_nodes.config import ClusterConfig

config = ClusterConfig()
config.set("cluster.master_port", 9753)
port = config.get("cluster.master_port")
```

### Utils (`fusion_multi_nodes.utils`)

Network topology detection, logging, data directory management.

```python
from fusion_multi_nodes.utils import NetworkTopologyDetector, setup_logger

detector = NetworkTopologyDetector()
interfaces = await detector.detect()
best_link = detector.get_best_link()
```

---

## CLI Reference

```bash
fusion-multi-node [OPTIONS] COMMAND [ARGS]
```

### Global Options

| Option | Description |
|--------|-------------|
| `--verbose`, `-v` | Verbose output |
| `--version` | Show version |

### Commands

#### `cluster start/stop/status`

| Subcommand | Description |
|------------|-------------|
| `start --mode master\|agent\|both` | Start cluster services |
| `stop` | Stop all cluster services |
| `status` | Show cluster status summary |

#### `node list/info`

| Subcommand | Description |
|------------|-------------|
| `list [--online]` | List all nodes (or only online) |
| `info <node_id>` | Show detailed node information |

#### `task submit/list/cancel`

| Subcommand | Description |
|------------|-------------|
| `submit --name NAME --model MODEL [--mode pipeline\|data]` | Submit a task |
| `list` | List all tasks |
| `cancel <task_id>` | Cancel a task |

#### `config list/get/set`

| Subcommand | Description |
|------------|-------------|
| `list` | Show all configuration |
| `get <key>` | Get a config value |
| `set <key> <value>` | Set a config value |

#### `network detect`

| Subcommand | Description |
|------------|-------------|
| `detect` | Detect network topology and link types |

#### `caveman test`

| Subcommand | Description |
|------------|-------------|
| `test [data]` | Test Caveman compression with sample data |

#### `kv stats/warm`

| Subcommand | Description |
|------------|-------------|
| `stats` | Show KV cache statistics |
| `warm --prompt TEXT --nodes TEXT` | Warm KV cache with prompts |

---

## Data Models

### `NodeInfo`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `node_id` | `str` | required | Unique node identifier |
| `hostname` | `str` | required | Machine hostname |
| `ip_address` | `str` | required | IP address |
| `port` | `int` | required | Agent port |
| `total_memory_gb` | `float` | 0.0 | Total RAM in GB |
| `available_memory_gb` | `float` | 0.0 | Available RAM in GB |
| `cpu_cores` | `int` | 0 | Number of CPU cores |
| `gpu_cores` | `int` | 0 | Number of GPU cores |
| `mlx_version` | `str` | "" | Installed MLX version |
| `status` | `NodeStatus` | OFFLINE | Current node status |
| `active_tasks` | `int` | 0 | Currently running tasks |

### `ClusterTask`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `task_id` | `str` | required | Unique task identifier |
| `name` | `str` | required | Task name |
| `mode` | `ParallelMode` | required | Pipeline or data parallel |
| `model_name` | `str` | "" | Model name for inference |
| `assigned_nodes` | `List[str]` | [] | Assigned worker nodes |
| `status` | `TaskStatus` | PENDING | Current task status |
| `timeout_seconds` | `float` | 300.0 | Task timeout |

### `KVCacheEntry`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cache_id` | `str` | required | Cache identifier |
| `model_name` | `str` | required | Associated model |
| `node_id` | `str` | required | Hosting node |
| `created_at` | `float` | required | Creation timestamp |
| `size_mb` | `float` | required | Cache size in MB |
| `ttl_seconds` | `float` | 3600.0 | Time-to-live |

### `MCPTool`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Tool name |
| `description` | `str` | required | Tool description |
| `parameters` | `Dict` | required | JSON Schema parameters |
| `node_id` | `str` | "" | Assigned node |
| `required_memory_gb` | `float` | 0.0 | Memory requirement |
| `timeout` | `float` | 60.0 | Execution timeout |

---

## Enums

### `NodeStatus`

| Value | Description |
|-------|-------------|
| `ONLINE` | Node is active and healthy |
| `OFFLINE` | Node is unreachable |
| `BUSY` | Node is at maximum capacity |
| `ERROR` | Node reported an error |

### `ParallelMode`

| Value | Description |
|-------|-------------|
| `PIPELINE` | Pipeline parallelism (model sharding) |
| `DATA` | Data parallelism (batch distribution) |

### `TaskStatus`

| Value | Description |
|-------|-------------|
| `PENDING` | Awaiting assignment |
| `RUNNING` | Currently executing |
| `COMPLETED` | Finished successfully |
| `FAILED` | Execution failed |
| `MIGRATED` | Migrated to another node |
| `TIMEOUT` | Exceeded time limit |

### `LinkType`

| Value | Bandwidth | Description |
|-------|-----------|-------------|
| `THUNDERBOLT_5` | 40Gbps+ | Thunderbolt 5 |
| `THUNDERBOLT_4` | 20Gbps | Thunderbolt 4 |
| `THUNDERBOLT_3` | 10Gbps | Thunderbolt 3 |
| `ETHERNET_10G` | 10Gbps | 10 Gigabit Ethernet |
| `ETHERNET_1G` | 1Gbps | Gigabit Ethernet |
| `ETHERNET_100M` | 100Mbps | Fast Ethernet |
| `WIFI_6E` | 2.4Gbps+ | WiFi 6E |
| `WIFI_6` | 1.2Gbps | WiFi 6 |
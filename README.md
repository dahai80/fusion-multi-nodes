<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>Cluster scheduling core for distributed Apple Silicon MLX inference</strong></p>
  <p><em>Pool multiple Macs into a unified AI cluster — pipeline parallelism, data parallelism, MCP gateway.</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
  <img src="https://img.shields.io/badge/MLX-Distributed-orange" alt="MLX">
  <img src="https://img.shields.io/badge/status-beta-yellow" alt="Beta">
  <img src="https://img.shields.io/badge/tests-172%20passed-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/coverage-79%25-yellow" alt="Coverage">
</p>

---

## 📋 Overview

**Fusion-Multi-Node** is the cluster scheduling core for the [Fusion-MLX](https://github.com/dahai80) ecosystem. It enables pooling multiple Apple Silicon Macs (M4/M5 Studio/Max) into a distributed inference cluster supporting two parallel modes:

- **Pipeline Parallelism** — Split large models (70B+) across multiple Macs, each handling a subset of layers
- **Data Parallelism** — Load the same model on multiple Macs, distribute batch requests for higher throughput

Built on `mlx.distributed`, compatible with Thunderbolt 5 RDMA and Ethernet. All five core modules are fully implemented:

| Module | Responsibility | Coverage |
|--------|---------------|----------|
| **Cluster Master** | Node discovery, resource scheduler, task lifecycle, KV cache pool, fault tolerance | 90% |
| **Node Agent** | Per-machine daemon, hardware reporting, task execution, fault reporting | 49% |
| **Distributed MLX Bridge** | Pipeline/data parallelism, model sharding, Caveman compression, KV cache sharing | 93% |
| **MCP Cluster Gateway** | Unified MCP endpoint, tool routing, Claude Desktop/Code integration | 87% |
| **Cluster Observability** | Metrics, logs, alerts, cluster health dashboard | 92% |

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Claude Code / API / fusion-desk UI         │
│                           ↓                                  │
│              fusion-multi-node Cluster Master                 │
│     (Auto-discovery, Scheduler, KV Pool, Fault Tolerance)     │
│                           ↓                                  │
│     ┌──────────────┬──────────────┬──────────────┐           │
│     │  Node Agent   │  Node Agent  │  Node Agent  │           │
│     │  (Mac M4)     │  (Mac M4)    │  (Mac M4)    │           │
│     │  fusion-desk  │  fusion-desk │  fusion-desk │           │
│     │  fusion-mlx   │  fusion-mlx  │  fusion-mlx  │           │
│     └──────────────┴──────────────┴──────────────┘           │
│                           ↓                                  │
│              Distributed MLX (mlx.distributed)                │
│         Thunderbolt RDMA / Ethernet / P2P Bridge              │
└──────────────────────────────────────────────────────────────┘
```

### Ecosystem Position

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                          │
│   fusion-desk  │  fusion-code  │  fusion-ui  │  Claude App   │
└──────────────────────────┬──────────────────────────────────┘
                           │ MCP / HTTP
┌──────────────────────────▼──────────────────────────────────┐
│                    Control Layer                               │
│         fusion-multi-node (Cluster Master + Node Agent)        │
│         MCP Cluster Gateway                                   │
└──────────────────────────┬──────────────────────────────────┘
                           │ distributed API
┌──────────────────────────▼──────────────────────────────────┐
│                    Inference Layer                             │
│         fusion-mlx (MLX distributed, quantization, Metal)     │
│         Fusion-KB (vector search, RAG)                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Installation

```bash
# Clone
git clone https://github.com/dahai80/fusion-multi-nodes.git
cd fusion-multi-nodes

# Install with pip
pip install -e .

# Install with all dependencies
pip install -e ".[all]"

# Run tests
pip install -e ".[test]"
pytest tests/ -v
```

### Start Cluster Master

```bash
fusion-multi-node cluster start --mode master
```

### Start Node Agent

```bash
fusion-multi-node cluster start --mode agent
```

### Check Cluster Status

```bash
fusion-multi-node cluster status
fusion-multi-node node list
```

---

## 📖 Command Reference

### Global Options

| Option | Description |
|--------|-------------|
| `--verbose`, `-v` | Verbose debug output |
| `--version` | Show version and exit |

### Cluster Management

| Command | Description |
|---------|-------------|
| `cluster start --mode master` | Start Cluster Master (port 9753) |
| `cluster start --mode agent` | Start Node Agent (port 9755) |
| `cluster start --mode both` | Start both Master and Agent |
| `cluster stop` | Stop all cluster services |
| `cluster status` | Show cluster summary (nodes, tasks, memory) |

### Node Management

| Command | Description |
|---------|-------------|
| `node list` | List all registered nodes |
| `node list --online` | Show only online nodes |
| `node info <node_id>` | Show detailed node information |

### Task Management

| Command | Description |
|---------|-------------|
| `task submit --name <name> --mode pipeline --model <model>` | Submit pipeline parallel task |
| `task submit --name <name> --mode data --model <model>` | Submit data parallel task |
| `task list` | List all tasks with status |
| `task cancel <task_id>` | Cancel a running task |

### Configuration

| Command | Description |
|---------|-------------|
| `config list` | Show all configuration |
| `config get <key>` | Get a config value (e.g. `cluster.master_port`) |
| `config set <key> <value>` | Set a config value |

### Network & Compression

| Command | Description |
|---------|-------------|
| `network detect` | Detect network topology and link types |
| `caveman test [data]` | Test Caveman compression with sample data |
| `kv stats` | Show KV cache statistics |
| `kv warm --prompt <text> --nodes <id>` | Warm KV cache with prompts |

---

## 🏗️ Module Architecture

### 1. Cluster Master (`fusion_multi_nodes.master`)

**File**: `master/cluster_master.py` | **Coverage**: 90%

The Cluster Master is the single source of truth for the cluster. It manages node registration, health checks, task scheduling, and KV cache sharing.

```python
from fusion_multi_nodes.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode

master = ClusterMaster(host="0.0.0.0", port=9753)

# Node registers itself
node = NodeInfo(node_id="node_1", hostname="mac-studio-1", ip_address="10.0.0.1",
                port=9755, total_memory_gb=64.0, available_memory_gb=48.0,
                cpu_cores=16, gpu_cores=64)
master.register_node(node)

# Task scheduling
task = ClusterTask(task_id="task_1", name="batch-inference", mode=ParallelMode.DATA)
master.assign_task(task)  # Auto-selects best node

# Lifecycle
master.complete_task("task_1")  # Or migrate_task() for failover
```

**Key capabilities**:
- **Node discovery**: LAN P2P scanning, Thunderbolt bridge detection
- **Resource scheduler**: Score-based node selection (available memory, load, latency)
- **Pipeline parallelism**: Split large models across nodes
- **Data parallelism**: Load balance batch inference across nodes
- **KV cache pool**: Cross-node KV cache sharing and reuse
- **Fault tolerance**: Task timeout, migration, auto-failover

### 2. Node Agent (`fusion_multi_nodes.agent`)

**File**: `agent/node_agent.py` | **Coverage**: 49%

The Node Agent runs on every Mac in the cluster, reporting hardware metrics and executing tasks.

```python
from fusion_multi_nodes.agent import NodeAgent, AgentConfig

config = AgentConfig(node_id="my_mac", master_host="10.0.0.1")
agent = NodeAgent(config)
await agent.start()

# Hardware info
info = agent.collect_hardware_info()
# Returns: {node_id, hostname, total_memory_gb, cpu_cores, gpu_cores, mlx_version, ...}

# Execute inference task
result = await agent.execute_task({
    "task_id": "t1", "type": "inference",
    "model": "qwen3.5-9b",
    "params": {"prompt": "Hello, world!"}
})
```

**Key capabilities**:
- **Hardware collection**: Memory, CPU, GPU, MLX version, Apple Silicon detection
- **Heartbeat**: Periodic health reporting to Master
- **Task execution**: Inference, embedding, plugin tasks via fusion-mlx API
- **Fault reporting**: Automatic crash/OOM/network failure reporting

### 3. Distributed MLX Bridge (`fusion_multi_nodes.distributed_mlx`)

**File**: `distributed_mlx/distributed_bridge.py` | **Coverage**: 93% (Caveman)

Three sub-modules for distributed inference:

**a) Model Sharding & Pipeline**
```python
from fusion_multi_nodes.distributed_mlx import DistributedMLXBridge

bridge = DistributedMLXBridge()
shards = await bridge.shard_model("llama-70b", num_shards=4)
# 4 shards, each with ~8 layers

# Pipeline inference across nodes
result = await bridge.pipeline_inference(
    "llama-70b", "What is AI?", ["node_1", "node_2", "node_3", "node_4"]
)
```

**b) Caveman Token Compression**
```python
from fusion_multi_nodes.distributed_mlx import CavemanManager

manager = CavemanManager()
compressed, method, stats = await manager.compress_tensor(data, link_type="ethernet_1g")
# Methods: zlib (general), diff (repeated patterns), dict (dictionary-based)
# Overhead: 40-60% bandwidth reduction
```

**c) KV Cache Sharing**
```python
from fusion_multi_nodes.distributed_mlx import KVSharingManager, KVCacheEntry

kv = KVSharingManager(max_local_cache_mb=4096.0)
entry = KVCacheEntry(cache_id="c1", model_name="qwen", prompt_hash="abc123",
                     prompt_prefix="What is", total_tokens=100, total_size_bytes=1024,
                     created_at=time.time())
kv.store_local(entry)
found = kv.lookup_local("qwen", "abc123")
```

### 4. MCP Cluster Gateway (`fusion_multi_nodes.mcp_gateway`)

**File**: `mcp_gateway/mcp_gateway.py` | **Coverage**: 87%

The MCP Gateway provides a unified Model Context Protocol entry point for Claude Desktop and Claude Code, aggregating tools from all cluster nodes.

```python
from fusion_multi_nodes.mcp_gateway import MCPClusterGateway, MCPTool

gateway = MCPClusterGateway(host="0.0.0.0", port=9756)

# Register a tool
tool = MCPTool(
    name="code_review",
    description="Review code changes using distributed MLX",
    parameters={"type": "object", "properties": {"code": {"type": "string"}}},
    required_memory_gb=4.0,
)
gateway.register_tool(tool)

# Claude calls this tool
result = await gateway.handle_tool_call("code_review", {"code": "..."}, source="claude_code")
```

### 5. Cluster Observability (`fusion_multi_nodes.observability`)

**File**: `observability/observability.py` | **Coverage**: 92%

Comprehensive monitoring with metrics, logs, and alerts.

```python
from fusion_multi_nodes.observability import ClusterObservability, LogEntry

obs = ClusterObservability(retention_hours=24.0)

# Metrics
obs.record_metric("node_1", "memory_used_gb", 16.0, tags={"gpu": "m4_ultra"})
obs.record_metric("node_1", "tokens_per_sec", 52.3)

# Logs
obs.add_log(LogEntry(time.time(), "node_1", "INFO", "scheduler", "Task completed"))

# Alerts
alert = obs.create_alert("warning", "High memory", "node_1 at 90% utilization")

# Check alert rules
await obs.check_alert_rules(nodes)
```

---

## 🔧 Configuration

Default config at `~/.fusion/multi-node/config.json`:

```json
{
  "cluster": {
    "master_port": 9753,
    "discovery_port": 9754,
    "agent_port": 9755,
    "mcp_port": 9756,
    "heartbeat_timeout": 15.0,
    "heartbeat_interval": 5.0
  },
  "parallel": {
    "default_mode": "pipeline",
    "pipeline_timeout": 300.0,
    "caveman_compress": true
  },
  "mlx": {
    "fusion_mlx_port": 8000,
    "fusion_kb_port": 11434,
    "fusion_desk_port": 9000
  },
  "mcp": {
    "token_budget": 10000000,
    "tool_timeout": 60.0
  },
  "observability": {
    "retention_hours": 24.0,
    "alert_enabled": true
  }
}
```

---

## 🧪 Running Tests

```bash
# Install test dependencies
pip install -e ".[test,all]"

# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=fusion_multi_nodes --cov-report=html

# Run specific test file
pytest tests/test_core.py -v
```

---

## 🛣️ Roadmap

### Phase 1 ✅ Core Infrastructure
- [x] Cluster Master — node discovery, scheduler, task lifecycle
- [x] Node Agent — hardware reporting, heartbeat, task execution

### Phase 2 ✅ Distributed MLX
- [x] Model sharding (pipeline parallelism)
- [x] Data parallel inference
- [x] Weight synchronization across nodes

### Phase 3 ✅ MCP Integration
- [x] Unified MCP endpoint for Claude
- [x] Tool registration and routing
- [x] Token budget management

### Phase 4 ✅ Observability & CLI
- [x] Metrics, logs, alerts
- [x] Cluster reports
- [x] Full CLI (15+ commands)

### Phase 5 ✅ Advanced Optimizations
- [x] Caveman token compression (3 methods)
- [x] Thunderbolt RDMA detection
- [x] KV cache sharing (local LRU + remote)

### Future
- [ ] Distributed MLX operator bridge (mlx.distributed API)
- [ ] Plugin ecosystem cluster registration
- [ ] Cluster monitoring dashboard (fusion-ui)
- [ ] Thunderbolt RDMA acceleration
- [ ] Cross-node KV cache with Caveman compression

---

## 🔒 Security

- **100% local offline** — Zero external network dependencies
- **Node authentication** — All agents must register with Master
- **Sandbox isolation** — Each node runs in its own sandbox
- **No telemetry** — No analytics, no phoning home
- **File access whitelist** — Controlled via SecurityPolicy

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

Contributions are welcome! Please ensure:

1. Tests pass: `pytest tests/ -v`
2. Coverage maintained: `pytest --cov=fusion_multi_nodes`
3. Code style follows PEP 8

---

## Architecture Influences

| Pattern | Source |
|---------|--------|
| Tool Registry + type coercion | [Squish](https://github.com/nicepkg/squish) |
| Lazy import via `__getattr__` | [Squish](https://github.com/nicepkg/squish) |
| MCP protocol | [LibreChat](https://github.com/danny-avila/LibreChat) |

---

<p align="center">
  <strong>Fusion-Multi-Node — Pool Macs, Unify Inference, Scale Locally.</strong>
</p>
<p align="center">
  <sub>Built with ❤️ by Fusion-MLX Team</sub>
</p>

---

<br>

<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>分布式 Apple Silicon MLX 集群调度核心</strong></p>
  <p><em>将多台 Mac 组成统一 AI 集群 — 流水线并行、数据并行、MCP 网关</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="许可证">
  <img src="https://img.shields.io/badge/MLX-分布式-orange" alt="MLX">
  <img src="https://img.shields.io/badge/状态-beta-yellow" alt="Beta">
  <img src="https://img.shields.io/badge/测试-172%20通过-brightgreen" alt="测试">
  <img src="https://img.shields.io/badge/覆盖率-79%25-yellow" alt="覆盖率">
</p>

---

## 📋 产品简介

**Fusion-Multi-Node** 是 Fusion-MLX 生态的集群调度核心层，解决多台 Apple Silicon Mac（M4/M5 Studio/Max）组成分布式推理集群的问题。

### 两种分布式模式

| 模式 | 说明 | 场景 |
|------|------|------|
| **流水线并行** | 大模型分层拆分到多 Mac | 跑 70B+ 超大本地模型 |
| **数据并行** | 多节点完整加载同款模型 | 批量代码生成、高吞吐推理 |

### 五大核心模块

| 模块 | 职责 | 覆盖率 |
|------|------|--------|
| **Cluster Master** | 节点发现、资源调度、任务生命周期、KV 缓存池、容灾 | 90% |
| **Node Agent** | 每台 Mac 守护进程、硬件上报、任务执行、故障上报 | 49% |
| **Distributed MLX Bridge** | 流水线/数据并行、模型分片、Caveman 压缩、KV 缓存共享 | 93% |
| **MCP 集群网关** | 统一 MCP 入口、工具路由、Claude Desktop/Code 集成 | 87% |
| **集群可观测** | 指标、日志、告警、集群健康面板 | 92% |

### 架构

```
┌──────────────────────────────────────────────────────────────┐
│                    Claude Code / API / fusion-desk UI         │
│                           ↓                                  │
│              fusion-multi-node Cluster Master                 │
│     (自动发现、调度器、KV 池、容错)                            │
│                           ↓                                  │
│     ┌──────────────┬──────────────┬──────────────┐           │
│     │  Node Agent   │  Node Agent  │  Node Agent  │           │
│     │  (Mac M4)     │  (Mac M4)    │  (Mac M4)    │           │
│     │  fusion-desk  │  fusion-desk │  fusion-desk │           │
│     │  fusion-mlx   │  fusion-mlx  │  fusion-mlx  │           │
│     └──────────────┴──────────────┴──────────────┘           │
│                           ↓                                  │
│              Distributed MLX (mlx.distributed)                │
│         Thunderbolt RDMA / 以太网 / P2P 桥接                  │
└──────────────────────────────────────────────────────────────┘
```

---

## 🚀 快速开始

### 安装

```bash
git clone https://github.com/dahai80/fusion-multi-nodes.git
cd fusion-multi-nodes
pip install -e ".[all]"
```

### 启动集群

```bash
# 启动主调度节点
fusion-multi-node cluster start --mode master

# 启动节点代理（每台 Mac 都需要）
fusion-multi-node cluster start --mode agent

# 查看集群状态
fusion-multi-node cluster status
fusion-multi-node node list
```

### CLI 命令速查

```bash
fusion-multi-node cluster start/stop/status    # 集群管理
fusion-multi-node node list/info               # 节点管理
fusion-multi-node task submit/list/cancel      # 任务管理
fusion-multi-node config list/get/set          # 配置管理
fusion-multi-node network detect               # 网络拓扑检测
fusion-multi-node caveman test                 # Caveman 压缩测试
fusion-multi-node kv stats/warm                # KV 缓存管理
```

---

## 🏗️ 模块详解

### 1. Cluster Master — 主调度节点

**文件**: `master/cluster_master.py` | **覆盖率**: 90%

```python
from fusion_multi_nodes.master import ClusterMaster, NodeInfo, ClusterTask, ParallelMode

master = ClusterMaster(host="0.0.0.0", port=9753)

# 节点注册
node = NodeInfo(node_id="mac1", hostname="mac-studio-1", ip_address="10.0.0.1",
                port=9755, total_memory_gb=64.0, available_memory_gb=48.0)
master.register_node(node)

# 任务调度
task = ClusterTask(task_id="t1", name="推理任务", mode=ParallelMode.DATA)
master.assign_task(task)  # 自动选择最优节点
```

### 2. Node Agent — 节点代理

**文件**: `agent/node_agent.py` | **覆盖率**: 49%

```python
from fusion_multi_nodes.agent import NodeAgent, AgentConfig

agent = NodeAgent(AgentConfig(node_id="mac1", master_host="10.0.0.1"))
await agent.start()

# 收集硬件信息
info = agent.collect_hardware_info()
# {node_id, hostname, total_memory_gb, cpu_cores, gpu_cores, mlx_version, ...}
```

### 3. Distributed MLX — 分布式推理

**文件**: `distributed_mlx/` | **覆盖率**: 93%

```python
# 模型分片
bridge = DistributedMLXBridge()
shards = await bridge.shard_model("llama-70b", num_shards=4)

# Token 压缩
manager = CavemanManager()
compressed, method, stats = await manager.compress_tensor(data, link_type="ethernet_1g")
# 节省 40-60% 带宽

# KV 缓存共享
kv = KVSharingManager(max_local_cache_mb=4096.0)
kv.store_local(entry)
found = kv.lookup_local("qwen", "abc123")
```

### 4. MCP 集群网关

**文件**: `mcp_gateway/mcp_gateway.py` | **覆盖率**: 87%

```python
gateway = MCPClusterGateway(host="0.0.0.0", port=9756)

tool = MCPTool(name="code_review", description="代码审查",
               parameters={"type": "object", "properties": {"code": {"type": "string"}}})
gateway.register_tool(tool)

# Claude 调用
result = await gateway.handle_tool_call("code_review", {"code": "..."}, source="claude_code")
```

### 5. 集群可观测

**文件**: `observability/observability.py` | **覆盖率**: 92%

```python
obs = ClusterObservability(retention_hours=24.0)
obs.record_metric("node_1", "memory_used_gb", 16.0)
obs.add_log(LogEntry(time.time(), "node_1", "INFO", "scheduler", "任务完成"))
alert = obs.create_alert("warning", "内存过高", "node_1 使用率 90%")
```

---

## 🔧 配置

默认配置 `~/.fusion/multi-node/config.json`:

```json
{
  "cluster": { "master_port": 9753, "agent_port": 9755, "heartbeat_timeout": 15.0 },
  "parallel": { "default_mode": "pipeline", "caveman_compress": true },
  "mlx": { "fusion_mlx_port": 8000, "fusion_kb_port": 11434 },
  "mcp": { "token_budget": 10000000 },
  "observability": { "retention_hours": 24.0 }
}
```

---

## 🧪 测试

```bash
pip install -e ".[test,all]"
pytest tests/ -v
pytest tests/ --cov=fusion_multi_nodes --cov-report=html
```

---

## 📄 开源协议

MIT License

---

<p align="center">
  <strong>Fusion-Multi-Node — 汇聚多台 Mac，统一推理，本地扩展。</strong>
</p>
<p align="center">
  <sub>Built with ❤️ by Fusion-MLX Team</sub>
</p>
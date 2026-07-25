<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>Cluster scheduling core for distributed Apple Silicon MLX inference</strong></p>
  <p><em>Pool multiple Macs into a unified AI cluster — pipeline parallelism, data parallelism, MCP gateway.</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.1.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
  <img src="https://img.shields.io/badge/tests-585%20passed-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/coverage-96%25-brightgreen" alt="Coverage">
</p>

---

## 📋 Overview

**Fusion-Multi-Node** is the cluster scheduling core for the [Fusion-MLX](https://github.com/dahai80) ecosystem. It enables pooling multiple Apple Silicon Macs (M4/M5 Studio/Max) into a distributed inference cluster.

### Two Distributed Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Pipeline Parallelism** | Split large models (70B+) across multiple Macs, each handling a subset of layers | Run超大本地模型 |
| **Data Parallelism** | Load the same model on multiple Macs, distribute batch requests for higher throughput | High-throughput batch inference |

### Seven Core Modules

| Module | Responsibility | Coverage |
|--------|---------------|----------|
| **Cluster Master** | Node discovery, resource scheduler, task lifecycle, KV cache pool, fault tolerance | 95% |
| **Node Agent** | Per-machine daemon, hardware reporting, task execution, mDNS auto-discovery | 90% |
| **mDNS Discovery** | Bonjour/mDNS zero-config node discovery, service registration/browsing | 86% |
| **FMP Protocol** | Three-layer binary protocol, AES-GCM encryption, TCP long connection, circuit breaker | 95% |
| **Distributed MLX Bridge** | Pipeline/data parallelism, model sharding, Caveman compression, KV cache sharing | 97% |
| **MCP Cluster Gateway** | Unified MCP endpoint, tool routing, Claude Desktop/Code integration | 100% |
| **Cluster Observability** | Metrics, logs, alerts, cluster health dashboard | 100% |

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
git clone https://github.com/dahai80/fusion-multi-node.git
cd fusion-multi-node

pip install -e .            # Core install
pip install -e ".[all]"     # All optional deps
pip install -e ".[test]"    # Test deps
```

### Start Cluster

```bash
# Start Cluster Master
fusion-multi-node cluster start --mode master

# Start Node Agent (on each Mac)
fusion-multi-node cluster start --mode agent

# Check status
fusion-multi-node cluster status
fusion-multi-node node list
```

### CLI Quick Reference

```bash
fusion-multi-node cluster start/stop/status    # Cluster management
fusion-multi-node node list/info/discover      # Node management
fusion-multi-node task submit/list/cancel      # Task management
fusion-multi-node config list/get/set          # Configuration
fusion-multi-node network detect               # Network topology
fusion-multi-node caveman test                 # Caveman compression
fusion-multi-node kv stats/warm                # KV cache management
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
| `cluster status` | Show cluster summary |

### Node Management

| Command | Description |
|---------|-------------|
| `node list` | List all registered nodes |
| `node list --online` | Show only online nodes |
| `node info <node_id>` | Show detailed node info |
| `node start --role master` | Start as master node |
| `node start --role agent` | Start as agent node |
| `node discover` | mDNS discover LAN nodes |

### Task Management

| Command | Description |
|---------|-------------|
| `task submit -n <name> -m <model> --mode pipeline` | Submit pipeline task |
| `task submit -n <name> -m <model> --mode data` | Submit data-parallel task |
| `task list` | List all tasks |
| `task cancel <task_id>` | Cancel a task |

### Configuration

| Command | Description |
|---------|-------------|
| `config list` | Show all configuration |
| `config get <key>` | Get a config value |
| `config set <key> <value>` | Set a config value |

### Network & Compression

| Command | Description |
|---------|-------------|
| `network detect` | Detect network topology and link types |
| `caveman test [data]` | Test Caveman compression |
| `kv stats` | Show KV cache statistics |
| `kv warm --prompt <text> --nodes <id>` | Warm KV cache |

---

## 🏗️ Module Architecture

### 1. Cluster Master (`fusion_multi_node.master`)

The single source of truth for the cluster — node registration, health checks, task scheduling, KV cache.

```python
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode

master = ClusterMaster(host="0.0.0.0", port=9753)

node = NodeInfo(node_id="node_1", hostname="mac-studio-1", ip_address="10.0.0.1",
                port=9755, total_memory_gb=64.0, available_memory_gb=48.0)
master.register_node(node)

task = ClusterTask(task_id="task_1", name="batch-inference", mode=ParallelMode.DATA)
master.assign_task(task)
master.complete_task("task_1")
```

**Key capabilities**: Score-based node selection, task lifecycle (PENDING→RUNNING→COMPLETED/FAILED/TIMEOUT), migration, KV cache pool, heartbeat timeout.

### 2. Node Agent (`fusion_multi_node.agent`)

Runs on every Mac — hardware metrics, heartbeat, task execution via fusion-mlx API.

```python
from fusion_multi_node.agent import NodeAgent, AgentConfig

config = AgentConfig(node_id="my_mac", master_host="10.0.0.1")
agent = NodeAgent(config)
await agent.start()

info = agent.collect_hardware_info()
result = await agent.execute_task({"task_id": "t1", "type": "inference", "model": "qwen3.5-9b"})
```

### 3. mDNS Discovery (`fusion_multi_node.discovery`)

Zero-config Bonjour/mDNS node discovery. Master registers; Agents browse.

```python
from fusion_multi_node.discovery import MDNSDiscovery

mdns = MDNSDiscovery(node_id="fusion-master")
mdns.register(port=9753, properties={"role": "master"})

master = await mdns.find_master_async(timeout=5.0)
mdns.unregister()
```

### 4. FMP Protocol (`fusion_multi_node.protocol`)

Three-layer binary protocol with AES-GCM encryption and circuit breaker.

```python
from fusion_multi_node.protocol import (
    FMPMessage, PayloadType, FMPCrypto,
    FMPConnectionManager, FMPRouter, CircuitBreaker,
)

msg = FMPMessage.create("master", "node1", PayloadType.HEARTBEAT, {"status": "ok"})
data = msg.serialize()
msg2 = FMPMessage.deserialize(data)

key = FMPCrypto.generate_key()
crypto = FMPCrypto(key=key)
crypto.encrypt_message(msg)
crypto.decrypt_message(msg)

cb = CircuitBreaker(name="node1", failure_threshold=5)
if cb.allow_request():
    cb.record_success()
```

**Three layers**: LinkLayer (routing, hop_count), BusinessLayer (payload, rounds), ControlLayer (heartbeat, ACK, flow control).

### 5. Distributed MLX Bridge (`fusion_multi_node.distributed_mlx`)

Three sub-modules for distributed inference:

```python
from fusion_multi_node.distributed_mlx import DistributedMLXBridge, CavemanManager, KVSharingManager

# Model sharding & pipeline
bridge = DistributedMLXBridge()
shards = await bridge.shard_model("llama-70b", num_shards=4)
result = await bridge.pipeline_inference("llama-70b", "What is AI?", ["n1", "n2", "n3", "n4"])

# Caveman compression (40-60% bandwidth savings)
manager = CavemanManager()
compressed, method, stats = await manager.compress_tensor(data, link_type="ethernet_1g")

# KV cache sharing
kv = KVSharingManager(max_local_cache_mb=4096.0)
kv.store_local(entry)
found = kv.lookup_local("qwen", "abc123")
```

### 6. MCP Cluster Gateway (`fusion_multi_node.mcp_gateway`)

Unified MCP endpoint for Claude Desktop/Code, aggregating tools from all nodes.

```python
from fusion_multi_node.mcp_gateway import MCPClusterGateway, MCPTool

gateway = MCPClusterGateway(host="0.0.0.0", port=9756)
tool = MCPTool(name="code_review", description="Review code",
               parameters={"type": "object", "properties": {"code": {"type": "string"}}})
gateway.register_tool(tool)
result = await gateway.handle_tool_call("code_review", {"code": "..."}, source="claude_code")
```

### 7. Cluster Observability (`fusion_multi_node.observability`)

Metrics, logs, alerts with retention and auto-cleanup.

```python
from fusion_multi_node.observability import ClusterObservability, LogEntry

obs = ClusterObservability(retention_hours=24.0)
obs.record_metric("node_1", "memory_used_gb", 16.0, tags={"gpu": "m4_ultra"})
obs.add_log(LogEntry(time.time(), "node_1", "INFO", "scheduler", "Task completed"))
alert = obs.create_alert("warning", "High memory", "node_1 at 90% utilization")
await obs.check_alert_rules(nodes)
```

---

## 🔧 Configuration

Default config at `~/.fusion/multi-node/config.json`:

```json
{
  "cluster": {
    "master_host": "0.0.0.0",
    "master_port": 9753,
    "discovery_port": 9754,
    "agent_port": 9755,
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
    "fusion_desk_port": 9000
  },
  "mcp": {
    "token_budget": 10000000,
    "tool_timeout": 60.0
  },
  "observability": {
    "retention_hours": 24.0
  }
}
```

---

## 🧪 Testing

```bash
pip install -e ".[test]"

# Run all tests (585 tests)
pytest tests/ -v

# With coverage (96.1%)
pytest tests/ --cov=fusion_multi_node --cov-report=html

# Run specific module
pytest tests/test_cluster_master.py -v
pytest tests/test_protocol.py -v
```

---

## 📊 Key Constants

| Constant | Default | Purpose |
|----------|---------|---------|
| Master port | 9753 | Cluster Master service port |
| Discovery port | 9754 | mDNS discovery port |
| Agent port | 9755 | Node Agent port |
| MCP port | 9756 | MCP Gateway port |
| Heartbeat timeout | 15.0s | Stale node threshold |
| Task timeout | 300.0s | Default task timeout |
| KV cache TTL | 3600.0s | Default KV cache expiry |
| Token budget | 10,000,000 | MCP gateway token limit |

---

## 🛣️ Roadmap

### v0.1.0 ✅ (Current)
- [x] Cluster Master — node discovery, scheduler, task lifecycle, fault tolerance
- [x] Node Agent — hardware reporting, heartbeat, task execution, mDNS auto-discovery
- [x] mDNS Discovery — Bonjour zero-config service registration and browsing
- [x] FMP Protocol — three-layer binary protocol, AES-GCM encryption, circuit breaker
- [x] Distributed MLX — model sharding, pipeline/data parallelism, Caveman compression, KV cache sharing
- [x] MCP Gateway — unified MCP endpoint for Claude integration
- [x] Observability — metrics, logs, alerts, cluster reports
- [x] CLI — 15+ commands for cluster/node/task/config/network/caveman/kv management
- [x] 96.1% test coverage (585 tests)

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
- **AES-GCM encryption** — FMP protocol encrypted communication
- **Circuit breaker** — Automatic fault isolation for failing nodes
- **No telemetry** — No analytics, no phoning home

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

Contributions welcome! Please ensure:

1. Tests pass: `pytest tests/ -v`
2. Coverage ≥ 90%: `pytest --cov=fusion_multi_node`
3. 4-space indentation, no docstrings (self-documenting names)
4. All classes use `logging.getLogger(__name__)`

---

<p align="center">
  <strong>Fusion-Multi-Node — Pool Macs, Unify Inference, Scale Locally.</strong>
</p>
<p align="center">
  <sub>Built with ❤️ by Fusion-MLX Team</sub>
</p>

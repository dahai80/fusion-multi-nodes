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
  <img src="https://img.shields.io/badge/status-alpha-yellow" alt="Alpha">
</p>

---

## 📋 Overview

**Fusion-Multi-Node** is the cluster scheduling core for the Fusion-MLX ecosystem. It enables pooling multiple Apple Silicon Macs (M4/M5 Studio/Max) into a distributed inference cluster supporting both pipeline parallelism and data parallelism.

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

### Five Core Modules

| Module | Responsibility |
|--------|---------------|
| **Cluster Master** | Node discovery, resource scheduler, task lifecycle, KV cache pool, fault tolerance |
| **Node Agent** | Per-machine daemon, hardware reporting, task execution, fault reporting |
| **Distributed MLX Bridge** | Pipeline/data parallelism, model sharding, weight sync, Caveman compression |
| **MCP Cluster Gateway** | Unified MCP endpoint, tool routing, Claude Desktop/Code integration |
| **Cluster Observability** | Metrics, logs, alerts, cluster health dashboard |

---

## 🚀 Quick Start

### Installation

```bash
cd fusion-multi-nodes
pip install -e .
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

### Node Management

| Command | Description |
|---------|-------------|
| `fusion-multi-node node list` | List all nodes in the cluster |
| `fusion-multi-node node list --online` | Show only online nodes |
| `fusion-multi-node node info <id>` | Show detailed node information |

### Cluster Management

| Command | Description |
|---------|-------------|
| `fusion-multi-node cluster start --mode master` | Start Cluster Master |
| `fusion-multi-node cluster start --mode agent` | Start Node Agent |
| `fusion-multi-node cluster stop` | Stop all cluster services |
| `fusion-multi-node cluster status` | Show cluster status summary |

### Task Management

| Command | Description |
|---------|-------------|
| `fusion-multi-node task submit --name <name> --model <model>` | Submit a task |
| `fusion-multi-node task submit --mode pipeline` | Pipeline parallel task |
| `fusion-multi-node task submit --mode data` | Data parallel task |
| `fusion-multi-node task list` | List all tasks |
| `fusion-multi-node task cancel <id>` | Cancel a task |

### Configuration

| Command | Description |
|---------|-------------|
| `fusion-multi-node config list` | Show all configuration |
| `fusion-multi-node config get <key>` | Get a config value |
| `fusion-multi-node config set <key> <value>` | Set a config value |

---

## 🏗️ Module Architecture

### Cluster Master

```
fusion_multi_nodes/master/
├── __init__.py
└── cluster_master.py    # ClusterMaster, NodeInfo, ClusterTask, KVCacheEntry
```

Key capabilities:
- **Node discovery**: LAN P2P scanning, Thunderbolt bridge detection
- **Resource scheduler**: Score-based node selection (memory, load, latency)
- **Pipeline parallelism**: Split large models across multiple nodes
- **Data parallelism**: Load balance batch inference across nodes
- **KV cache pool**: Cross-node KV cache sharing
- **Fault tolerance**: Task timeout, migration, auto-failover

### Node Agent

```
fusion_multi_nodes/agent/
├── __init__.py
└── node_agent.py        # NodeAgent, AgentConfig
```

Key capabilities:
- **Hardware collection**: Memory, CPU, GPU, MLX version, Apple Silicon detection
- **Heartbeat**: Periodic health reporting to Master
- **Task execution**: Inference, embedding, plugin tasks via fusion-mlx API
- **Fault reporting**: Automatic crash/oome/network failure reporting

### Distributed MLX Bridge

```
fusion_multi_nodes/distributed_mlx/
├── __init__.py
└── distributed_bridge.py  # DistributedMLXBridge, ModelShard
```

Key capabilities:
- **Model sharding**: Auto-split model layers across nodes
- **Pipeline inference**: Chain multiple nodes for large model inference
- **Data parallel inference**: Distribute prompts across nodes
- **Weight sync**: Cross-node model weight synchronization
- **Caveman compression**: Token compression for reduced bandwidth

### MCP Cluster Gateway

```
fusion_multi_nodes/mcp_gateway/
├── __init__.py
└── mcp_gateway.py       # MCPClusterGateway, MCPTool, MCPRequest
```

Key capabilities:
- **Unified MCP endpoint**: Single entry point for all Claude tools
- **Tool routing**: Route to optimal node based on resource requirements
- **Token budget**: Cluster-wide Coding Plan token management
- **Plugin aggregation**: Combine tools from all nodes

### Cluster Observability

```
fusion_multi_nodes/observability/
├── __init__.py
└── observability.py     # ClusterObservability, MetricPoint, Alert, LogEntry
```

Key capabilities:
- **Metrics**: Memory, TPS, RTT, session duration
- **Logs**: Unified log aggregation from all nodes
- **Alerts**: Node offline, OOM, task timeout, high latency
- **Reports**: Cluster health summary, node statistics

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
pip install -e ".[test]"
pytest tests/ -v
```

---

## 🛣️ Roadmap

### Phase 1 (Current) ✅
- [x] Cluster Master + Node Agent
- [x] Distributed MLX Bridge (sharding, pipeline, data parallel)
- [x] MCP Cluster Gateway
- [x] Cluster Observability (metrics, logs, alerts)
- [x] CLI (node, cluster, task, config management)

### Phase 2 (Planned)
- [ ] Distributed MLX operator bridge (mlx.distributed API)
- [ ] Pipeline + data parallelism inference
- [ ] Cross-node KV cache sharing

### Phase 3 (Future)
- [ ] Plugin ecosystem cluster registration
- [ ] MCP gateway full Claude integration
- [ ] Cluster monitoring dashboard (fusion-ui)

### Phase 4
- [ ] Fault migration and auto-healing
- [ ] Coding Plan cluster token statistics
- [ ] Thunderbolt RDMA acceleration

---

## 🔒 Security

- **100% local offline** — Zero external network dependencies
- **Node authentication** — All agents must register with Master
- **Sandbox isolation** — Each node runs in its own sandbox
- **No telemetry** — No analytics, no phoning home

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.

---

<p align="center">
  <strong>Fusion-Multi-Node — Pool Macs, Unify Inference, Scale Locally.</strong>
</p>
<p align="center">
  <sub>Built with ❤️ by Fusion-MLX Team</sub>
</p>
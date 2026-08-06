<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>Cluster scheduling core for distributed Apple Silicon MLX inference</strong></p>
  <p><em>Pool multiple Macs into a unified AI cluster — pipeline parallelism, data parallelism, MCP gateway.</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.5.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License">
  <img src="https://img.shields.io/badge/tests-793%20passed-brightgreen" alt="Tests">
</p>

---

## 📋 Overview

**Fusion-Multi-Node** is the cluster scheduling core for the [Fusion-MLX](https://github.com/dahai80) ecosystem. It enables pooling multiple Apple Silicon Macs (M4/M5 Studio/Max) into a distributed inference cluster.

### Two Distributed Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Pipeline Parallelism** | Split large models (70B+) across multiple Macs, each handling a subset of layers | Run超大本地模型 |
| **Data Parallelism** | Load the same model on multiple Macs, distribute batch requests for higher throughput | High-throughput batch inference |

### Core Modules

| Module | Responsibility |
|--------|---------------|
| **Cluster Master** | Node discovery, resource scheduler, task lifecycle, KV cache pool, fault tolerance, master election, cloud fallback, task auto-degradation, load-aware routing, task sharding, AST diff, FMP KV sync |
| **Node Agent** | Per-machine daemon, hardware reporting, task execution, mDNS auto-discovery |
| **mDNS Discovery** | Bonjour/mDNS zero-config node discovery, manual IP join fallback |
| **FMP Protocol** | Three-layer binary protocol, AES-GCM encryption, TCP long connection, circuit breaker, hop_count, FMP inbound server |
| **Distributed MLX Bridge** | Pipeline/data parallelism, model sharding, Caveman compression, KV cache sharing |
| **MCP Cluster Gateway** | Unified MCP endpoint, tool routing, Claude Desktop/Code integration |
| **Security** | Node approval, Master/Worker permission isolation, Worker sandbox, OS-level sandbox-exec, data scrubbing, FMPCrypto (AES-256-GCM + ECDH), Metal AES-GCM acceleration |
| **Observability** | Metrics, logs, alerts, log store & export, intelligent fault diagnosis, optimization suggestions, 7-day retention |
| **Autoscaler** | Conservative/Balanced/Aggressive scale policies, auto scale-up/down/rebalance, hot-reload config |
| **Storage Volumes** | Volume abstraction, shard replication, checkpoint persistence, capacity monitoring, LRU eviction, shard distribution, distributed KV store, quorum read/write |

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Claude Code / API / fusion-desk UI         │
│                           ↓                                  │
│              fusion-multi-node Cluster Master                 │
│  (Discovery, Scheduler, KV Pool, Election, Autoscaler,       │
│   Cloud Fallback, Degradation, Security, Observability)      │
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

## 🏗️ Module Architecture

### 1. Cluster Master (`fusion_multi_node.master`)

The single source of truth for the cluster — node registration, health checks, task scheduling, KV cache, master election, cloud fallback, task auto-degradation.

```python
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode

master = ClusterMaster(host="127.0.0.1", port=11449)

node = NodeInfo(
    node_id="node_1",
    hostname="mac-studio-1",
    ip_address="10.0.0.1",
    port=11445,
    total_memory_gb=64.0,
    available_memory_gb=48.0,
)
master.register_node(node)

task = ClusterTask(
    task_id="task_1",
    name="batch-inference",
    mode=ParallelMode.DATA,
    required_capability="inference",
    preferred_node_id="node_1",
    priority=5,
)
master.assign_task(task)
await master.cancel_task("task_1", reason="user request", cancel_sub_tasks=True)
await master.degrade_task("task_1")  # 70b→32b→13b→8b→3b→1b
master.complete_task("task_1")
```

**Key capabilities**: Load-aware routing (BALANCED/VRAM_FIRST/LOCALITY_FIRST/LOW_LATENCY, thread-safe strategy switching), local-force gate (≤0.5B models), VRAM-first scheduling (≥13B), score-based node selection with capability filtering, task lifecycle (PENDING→RUNNING→COMPLETED/FAILED/TIMEOUT/MIGRATED), recursive cancel, model auto-degradation chain, migration, KV cache pool with FMP sync, AST diff-only transmission, task sharding (inference/AST/vectorize, shard timeout), heartbeat timeout, cloud fallback on retry exhaustion.

### Master Election (`fusion_multi_node.master.election`)

Raft-simplified leader election with priority-based voting:

```python
from fusion_multi_node.master.election import MasterElection, ElectionState

election = MasterElection(node_id="node-1", priority=5, known_nodes=["node-2", "node-3"])
await election.start()
resp = await election.handle_vote_request(req)
await election.receive_heartbeat("leader-id", term=2)
```

### Cloud API Fallback (`fusion_multi_node.master.cloud_fallback`)

OpenAI/Anthropic fallback with daily cost limits:

```python
from fusion_multi_node.master.cloud_fallback import CloudFallbackClient, CloudConfig

client = CloudFallbackClient(config=CloudConfig(provider="openai", api_key="sk-...", max_cost_per_day=10.0))
result = await client.chat(messages=[{"role": "user", "content": "Hello"}])
usage = client.get_usage()  # total_requests, daily_cost, etc.
```

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

Zero-config Bonjour/mDNS node discovery with manual IP join fallback.

```python
from fusion_multi_node.discovery import MDNSDiscovery
from fusion_multi_node.discovery.manual_join import ManualJoinClient, ManualJoinManager

# mDNS auto-discovery
mdns = MDNSDiscovery(node_id="fusion-master")
mdns.register(port=11449, properties={"role": "master"})
master = await mdns.find_master_async(timeout=5.0)

# Manual IP join (mDNS fallback)
client = ManualJoinClient()
resp = await client.join(master_host="10.0.0.1", master_port=11449, node_id="node-1")

mgr = ManualJoinManager(cluster_secret="my-secret", auto_approve=True)
result = mgr.handle_join_request({"node_id": "node-1", "cluster_secret": "my-secret"})
```

### 4. FMP Protocol (`fusion_multi_node.protocol`)

Three-layer binary protocol with AES-GCM encryption, circuit breaker, and hop_count broadcast limit.

```python
from fusion_multi_node.protocol import (
    FMPMessage,
    PayloadType,
    FMPCrypto,
    FMPConnectionManager,
    FMPRouter,
    CircuitBreaker,
    FMPServer,
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

**Three layers**: LinkLayer (routing, hop_count), BusinessLayer (payload, rounds), ControlLayer (heartbeat, ACK, flow control). **Unified interface**: FMPInterface wraps connection management, message construction, encryption, heartbeat. **Protobuf v2**: Structured .proto with Envelope/Control/Payload messages, auto-fallback to JSON/msgpack.

### 5. Security (`fusion_multi_node.security`)

Node approval, Master/Worker permission isolation, Worker sandbox, data scrubbing.

```python
from fusion_multi_node.security.permission import (
    PermissionManager,
    NodeRole,
    Permission,
)
from fusion_multi_node.security.node_approval import NodeApprovalManager
from fusion_multi_node.security.sandbox import (
    WorkerSandbox,
    SandboxConfig,
    SandboxExecutor,
)
from fusion_multi_node.security.data_scrubber import DataScrubber
from fusion_multi_node.security.crypto import FMPCrypto, MetalCryptoBackend

# Permission isolation
pm = PermissionManager()
pm.assign_role("master-1", NodeRole.MASTER)
pm.assign_role("worker-1", NodeRole.WORKER)
pm.has_permission("worker-1", Permission.TASK_EXECUTE)  # True
pm.has_permission("worker-1", Permission.TASK_SUBMIT)  # False
pm.check_path_access("worker-1", "/api/execute", "POST")  # True

# Node approval
mgr = NodeApprovalManager(auto_approve_patterns=["192.168."])
req = mgr.request_join(node_id="n1", hostname="mac-1", ip_address="192.168.1.10", port=11445)
mgr.approve("n1", approved_by="admin")

# Worker sandbox
sandbox = WorkerSandbox(
    config=SandboxConfig(
        allowed_paths=["/tmp", "/data"],
        allowed_network_hosts=["api.openai.com"],
    )
)
sandbox.check_path_access("/tmp/out", write=True)  # True
sandbox.check_network_access("api.openai.com")  # True
sandbox.filter_environment({"HOME": "/u", "SECRET": "x"})  # SECRET removed

# Data scrubbing (phone, email, API key, ID card, etc.)
scrubber = DataScrubber()
text, hits = scrubber.scrub_text("Call 13912345678, key=sk-abc123...")

# OS-level sandbox execution (macOS sandbox-exec / Linux unshare)
executor = SandboxExecutor()
result = await executor.execute_in_sandbox("task-1", ["python", "script.py"])

# Metal AES-GCM acceleration (Apple Silicon hardware)
metal = MetalCryptoBackend()
encrypted = metal.encrypt(key, plaintext)
decrypted = metal.decrypt(key, encrypted)

# Secure transfer pipeline (AST diff + PII scrubbing)
from fusion_multi_node.security.secure_transfer import SecureTransferPipeline

pipeline = SecureTransferPipeline()
transfer = pipeline.prepare_transfer(old_ast, new_ast)  # diff + scrub
restored = pipeline.apply_transfer(base_ast, transfer)  # rebuild
```

### 6. Observability (`fusion_multi_node.observability`)

Metrics, logs, alerts, log store with export, intelligent fault diagnosis.

```python
from fusion_multi_node.observability import ClusterObservability, LogEntry
from fusion_multi_node.observability.log_store import (
    LogStore,
    StoredLog,
    FaultDiagnoser,
)

# Metrics & alerts
obs = ClusterObservability(retention_hours=168.0)
obs.record_metric("node_1", "memory_used_gb", 16.0, tags={"gpu": "m4_ultra"})
obs.add_log(LogEntry(time.time(), "node_1", "INFO", "scheduler", "Task completed"))
logs = obs.export_logs(fmt="json")  # M8-02 log export
suggestions = obs.generate_optimization_suggestions()  # M8-03 smart suggestions

# Log store & export
store = LogStore()
store.store(
    StoredLog(
        timestamp=time.time(),
        level="error",
        source="master",
        message="heartbeat timeout",
    )
)
results = store.query(level="error")
json_data = store.export_json()
csv_data = store.export_csv()

# Fault diagnosis (pattern matching + root cause analysis)
diagnoser = FaultDiagnoser()
results = diagnoser.diagnose(logs)
freq = diagnoser.analyze_frequency(logs, group_by="source")
```

### 7. Autoscaler (`fusion_multi_node.autoscaler`)

Conservative/Balanced/Aggressive scale policies.

```python
from fusion_multi_node.autoscaler import (
    Autoscaler,
    AutoscalerConfig,
    ScalePolicy,
    ScaleAction,
)

scaler = Autoscaler(
    policy=ScalePolicy.BALANCED,
    on_scale_up=lambda n: print(f"scale up {n}"),
    on_scale_down=lambda n: print(f"scale down {n}"),
    get_cluster_state=lambda: {"nodes": [...], "tasks": [...]},
)
action = await scaler.evaluate()  # SCALE_UP, SCALE_DOWN, REBALANCE, NOOP
```

### 8. Storage Volumes (`fusion_multi_node.storage`)

Volume abstraction, shard replication, checkpoint persistence.

```python
from fusion_multi_node.storage import StorageVolume, VolumeSpec, VolumeType
from fusion_multi_node.storage import ShardReplicator, ReplicationConfig
from fusion_multi_node.storage import CheckpointManager, CheckpointEntry
from fusion_multi_node.storage import DistributedKVStore, KVEntry

# Volume management
sv = StorageVolume(base_dir="/data/volumes")
sv.create_volume(VolumeSpec(name="models", volume_type=VolumeType.LOCAL))
sv.write_file("models", "config.json", b'{"model": "llama-70b"}')
data = sv.read_file("models", "config.json")

# Shard replication
replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
replicas = replicator.assign_replicas("shard-1", "/models/llama.bin", 1024, nodes)
healthy = replicator.get_healthy_replica("shard-1")

# Checkpoint persistence
cp = CheckpointManager(checkpoint_dir="/data/checkpoints")
cp.save(CheckpointEntry(checkpoint_id="cp-1", task_id="t1", node_id="n1", step=5, state_data={...}))
latest = cp.load_latest("t1")

# Distributed KV Store with TTL, partitions, snapshot/restore
kv = DistributedKVStore(data_dir="/data/kv")
kv.put("config:model", {"name": "llama-70b"}, partition="config", ttl_seconds=3600)
val = kv.get("config:model")
kv.snapshot()  # M9-03: persist to disk
kv.restore("snapshot.json", merge=True)

# Quorum read/write for shard replication
qr = replicator.quorum_write("shard-1", data, storage_volume=sv)
qread = replicator.quorum_read("shard-1", storage_volume=sv)
```

### 9. MCP Cluster Gateway (`fusion_multi_node.mcp_gateway`)

Unified MCP endpoint for Claude Desktop/Code, aggregating tools from all nodes.

```python
from fusion_multi_node.mcp_gateway import MCPClusterGateway, MCPTool

gateway = MCPClusterGateway(host="127.0.0.1", port=11446)
tool = MCPTool(
    name="code_review",
    description="Review code",
    parameters={"type": "object", "properties": {"code": {"type": "string"}}},
)
gateway.register_tool(tool)
result = await gateway.handle_tool_call("code_review", {"code": "..."}, source="claude_code")
```

---

## 🔧 Configuration

Default config at `~/.fusion/multi-node/config.json`:

```json
{
  "cluster": {
    "master_host": "127.0.0.1",
    "master_port": 11449,
    "discovery_port": 11450,
    "agent_port": 11445,
    "heartbeat_timeout": 15.0,
    "heartbeat_interval": 3.0
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
    "retention_hours": 168.0
  }
}
```

---

## 🧪 Testing

```bash
pip install -e ".[test]"

# Run all tests (793 tests)
pytest tests/ -v

# With coverage
pytest tests/ --cov=fusion_multi_node --cov-report=html

# Run specific module
pytest tests/test_cluster_master.py -v
pytest tests/test_protocol.py -v
pytest tests/test_new_features.py -v
```

---

## 📊 Key Constants

| Constant | Default | Purpose |
|----------|---------|---------|
| Master port | 11449 | Cluster Master service port |
| Discovery port | 11450 | mDNS discovery port |
| Agent port | 11445 | Node Agent port |
| MCP port | 11446 | MCP Gateway port |
| Heartbeat timeout | 15.0s | Stale node threshold |
| Task timeout | 300.0s | Default task timeout |
| KV cache TTL | 3600.0s | Default KV cache expiry |
| Token budget | 10,000,000 | MCP gateway token limit |
| Degradation chain | 70b→32b→13b→8b→3b→1b | Model auto-degradation |

---

## 🛣️ Roadmap

### v0.1.0 ✅
- [x] Cluster Master — node discovery, scheduler, task lifecycle, fault tolerance
- [x] Node Agent — hardware reporting, heartbeat, task execution, mDNS auto-discovery
- [x] mDNS Discovery — Bonjour zero-config service registration and browsing
- [x] FMP Protocol — three-layer binary protocol, AES-GCM encryption, circuit breaker
- [x] Distributed MLX — model sharding, pipeline/data parallelism, Caveman compression, KV cache sharing
- [x] MCP Gateway — unified MCP endpoint for Claude integration
- [x] Observability — metrics, logs, alerts, cluster reports
- [x] CLI — 15+ commands for cluster/node/task/config/network/caveman/kv management

### v0.3.0 ✅
- [x] Full audit remediation (P0-P3), 585 tests, 0 ruff errors

### v0.5.0 ✅ (Current)
- [x] M1-02 device_model + UMA size in mDNS discovery & NodeInfo
- [x] M1-03 Heartbeat interval 5s→3s
- [x] M1-02/03 mDNS heartbeat_interval/timeout in broadcast properties, real device_model + uma_size_gb
- [x] M1-05 Manual IP join fallback (mDNS failure scenario)
- [x] M2-04 hop_count broadcast storm prevention
- [x] M2-01 Structured .proto with Envelope/Control/Payload messages
- [x] M2-03 FMP heartbeat sending (start_heartbeat/stop_heartbeat on connection)
- [x] M2-05 FMPInterface unified API (connect, send_heartbeat, send_task_assign, broadcast)
- [x] M3-01 Master/Worker permission isolation
- [x] M3-02 Node approval mechanism (integrated into /api/nodes/register)
- [x] M3-02 NodeInfo.role field (master/worker/standby)
- [x] M3-05 TaskSpec separation (task definition vs runtime state)
- [x] M3-02 NodeStatus.FAULT enum value
- [x] M3-03 Master election (Raft-simplified with priority)
- [x] M4-01 LoadMetrics + LoadRouter structured load-aware routing
- [x] M4-02 Local-force gate (≤0.5B models forced local)
- [x] M4-03 VRAM-first scheduling (≥13B models, thread-safe strategy switching)
- [x] M4-04 Task auto-degradation (70b→32b→13b→8b→3b→1b)
- [x] M4-05 Cloud API fallback (OpenAI/Anthropic, daily cost limits)
- [x] M5-01/02/05 Task sharding (inference/AST/vectorize, by_file/by_document/by_batch, result merge)
- [x] M5-03 Timeout task auto-retry queue (_enqueue_retry, max 1 attempt)
- [x] M5-03 TaskShard timeout field + is_timed_out property
- [x] M5-04 Task full-lifecycle cancel (recursive sub-task)
- [x] M6-01 Master data isolation enforcement
- [x] M6-01 Worker temp dir cleanup (auto mkdir/rmtree on task execute)
- [x] M6-02 Worker sandbox (resource limits, path/network filtering, usage monitoring, subprocess env)
- [x] M6-03 Node approval integrated into register endpoint
- [x] M6-04 AST diff-only transmission
- [x] M6-04 Data scrubbing (phone, email, API key, ID card, etc.)
- [x] M6-04 FMPCrypto (AES-256-GCM with ECDH-negotiated session keys)
- [x] M7-06 Monitoring API v1 (/api/v1/nodes/{id}/metrics, /api/v1/tasks/{id}/progress)
- [x] M7-06 /api/v1/cluster/stats + /api/v1/tasks/{id}/timeline endpoints
- [x] M8-01 LogLevel standard enum (INFO/WARN/ERROR/FATAL) + Master全节点日志汇总 (collect_node_logs)
- [x] M8 Log store & export (JSON/CSV/text)
- [x] M8 Intelligent fault diagnosis (pattern matching + root cause)
- [x] M9-02/03 Storage data transfer + capacity monitoring + LRU eviction
- [x] M9-01 Distributed KV Store (TTL, partitions, snapshot/restore, persistence)
- [x] M9-02 Quorum read/write for shard replication
- [x] M9-03 KV Store snapshot/restore
- [x] M9-04 FMP protocol KV cache sync
- [x] M9 Storage volumes (local/shared/distributed)
- [x] M9 Shard replication with health tracking
- [x] M9 Checkpoint persistence
- [x] M9 Model shard distribution
- [x] M10-02/03 Autoscaler builtin scale actions (standby activation + migrate-then-deactivate)
- [x] M10 Autoscaler (conservative/balanced/aggressive policies)
- [x] M10 Task migration on scale-down
- [x] protobuf>=5.0.0 dependency
- [x] P0: FMPServer inbound TCP server (cross-node shard/KV transport)
- [x] P0: Protobuf structured encoding (envelope/control/payload fields)
- [x] P0: Autoscaler hot-reload (update_config/update_policy)
- [x] P0: Cross-node FMP transport (ShardReplicator + DistributedKVStore remote ops)
- [x] P1: Log retention 7 days (168h default) + log export API
- [x] P1: Smart optimization suggestions (alert-driven + error pattern analysis)
- [x] P1: SandboxExecutor (macOS sandbox-exec / Linux unshare / python-resource fallback)
- [x] P2: Metal AES-GCM acceleration (Apple Silicon CommonCrypto bridge + auto-fallback)
- [x] P2: CLI --transport fmp wiring (FMPServer + FMPConnectionManager)
- [x] 793 tests, 0 ruff errors

### Future
- [ ] Distributed MLX operator bridge (mlx.distributed API)
- [ ] Plugin ecosystem cluster registration
- [ ] Cluster monitoring dashboard (fusion-studio)
- [ ] Thunderbolt RDMA acceleration
- [ ] Cross-node KV cache with Caveman compression

---

## 🔒 Security

- **100% local offline** — Zero external network dependencies
- **Node approval** — New nodes require approval or pattern-based auto-approval
- **Master/Worker isolation** — Role-based permission, API path access control
- **Worker sandbox** — CPU/memory/disk limits, path & network whitelisting
- **Data scrubbing** — Auto-detect and redact PII (phone, email, API keys, ID cards)
- **AES-GCM encryption** — FMP protocol encrypted communication
- **Circuit breaker** — Automatic fault isolation for failing nodes
- **No telemetry** — No analytics, no phoning home

---

## 📄 License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

Contributions welcome! Please ensure:

1. Tests pass: `pytest tests/ -v`
2. Lint passes: `ruff check fusion_multi_node/`
3. 4-space indentation, no docstrings (self-documenting names)
4. All classes use `logging.getLogger(__name__)`

---

<p align="center">
  <strong>Fusion-Multi-Node — Pool Macs, Unify Inference, Scale Locally.</strong>
</p>
<p align="center">
  <sub>Built with ❤️ by Fusion-MLX Team</sub>
</p>

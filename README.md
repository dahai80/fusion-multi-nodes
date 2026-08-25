<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>Cluster scheduling core for distributed Apple Silicon MLX inference</strong></p>
  <p><em>Pool multiple Macs into a unified AI cluster — pipeline parallelism, data parallelism, 100% local-first.</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.8.2-blue" alt="Version">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License">
  <img src="https://img.shields.io/badge/tests-882%20passed-brightgreen" alt="Tests">
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
| **Cluster Master** | Node discovery, resource scheduler, task lifecycle, KV cache pool, fault tolerance, task auto-degradation, load-aware routing, task sharding, AST diff, FMP KV sync, 真实张量 PIPELINE 层切分链 (接 fusion-mlx `/distributed/*`), master→agent 派发循环, **H3 任务持久化+崩溃恢复** (RUNNING/PENDING 原子落盘, 崩溃重启自动重派)。HA 选举接 `start(ha_config=)` (默认关闭单 Master)。cloud_fallback 调度路径 v0.8.2 已切断 (100% 本地) |
| **Node Agent** | Per-machine daemon, hardware reporting, task execution, mDNS auto-discovery, pipeline_step (上游 `/distributed/load_shard`+`pipeline_step`, b64.npy 激活跨节点) |
| **mDNS Discovery** | Bonjour/mDNS zero-config node discovery, manual IP join fallback |
| **FMP Protocol** | Three-layer binary protocol, AES-GCM encryption, TCP long connection, circuit breaker, hop_count, FMP inbound server |
| **Distributed MLX Bridge** | Pipeline/data parallelism, model sharding, Caveman compression, KV cache sharing |
| **MCP Cluster Gateway** | Unified MCP endpoint, tool routing, Claude Desktop/Code integration |
| **Security** | Node approval, Master/Worker permission isolation, Worker sandbox, OS-level sandbox-exec, data scrubbing, FMPCrypto (AES-256-GCM + ECDH), Metal AES-GCM acceleration |
| **Observability** | Metrics, logs, alerts, log store & export, intelligent fault diagnosis, optimization suggestions, 7-day retention |
| **Autoscaler** ⚠️未接线 | Conservative/Balanced/Aggressive scale policies, auto scale-up/down/rebalance, hot-reload config. **当前未接入 ClusterMaster 生命周期, `/api/v1/autoscaler/*` 返回 404; 代码留作未来启用** |
| **Storage Volumes** | Volume abstraction, checkpoint persistence, capacity monitoring, LRU eviction. **ShardReplicator / DistributedKVStore / quorum 读写未接线生产路径, 仅库级可用** |

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Claude Code / API / fusion-desk UI         │
│                           ↓                                  │
│              fusion-multi-node Cluster Master                 │
│  (Discovery, Scheduler, KV Pool, [Election·HA 可选],          │
│   [Autoscaler⚠未接线], Cloud Fallback, Degradation,          │
│   Security, Observability)                                   │
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
fusion-multi-node cluster pending/approve/reject # Node approval
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

The single source of truth for the cluster — node registration, health checks, task scheduling, KV cache, master election, cloud fallback, task auto-degradation, 真实张量 PIPELINE 层切分链。

#### Pipeline Parallelism — 真实张量层切分 (接 fusion-mlx `/distributed/*`, #621)

PIPELINE 模式按 `model_shards` 把模型切成多段, 每节点跑一段层前向。首段带 `input_ids`
(embed + layers), 后续段带上一段出口 `hidden_states` (b64.npy, 仅 layers)。激活张量
经调度器顺序链传到末节点, 末节点出口 = 最终 hidden_states。

```python
from fusion_multi_node.master import ClusterMaster, ClusterTask, ParallelMode

task = ClusterTask(
    task_id="task-pipeline",
    name="layer-split",
    mode=ParallelMode.PIPELINE,
    model_name="Llama-3.2-1B-Instruct-4bit",
    model_shards=[
        {"shard_index": 0, "layer_range": [0, 8]},
        {"shard_index": 1, "layer_range": [8, 16]},
    ],
    task_type="pipeline_step",
    params={
        "model_id": "~/.fusion-mlx/models/mlx-community-Llama-3.2-1B-Instruct-4bit",
        "input_ids": [10, 20, 30, 40],
    },
)
await master.assign_task(task)
# → 末节点返回 hidden_states (shape [1,4,2048] float16, b64.npy)
# lm_head/解码超上游 /distributed/* 首版范围 — 调度器只负责层前向链, 不做 token 生成
```

> 真模型 E2E 已验证 (Llama-3.2-1B-Instruct-4bit, 16 层切 [0,8]/[8,16],
> 见 `tests/test_pipeline_e2e.py`)。需 fusion-mlx 运行 + `mlx.fusion_mlx_api_key` 配置。

```python
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode

master = ClusterMaster(host="127.0.0.1", port=11452)

node = NodeInfo(
    node_id="node_1",
    hostname="mac-studio-1",
    ip_address="10.0.0.1",
    port=11458,
    total_memory_gb=64.0,
    available_memory_gb=48.0,
)
await master.register_node(node)  # 再注册 = PATCH (保留运行态), 返回 bool (ban 期内 False)

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

**Key capabilities**: Load-aware routing (BALANCED/VRAM_FIRST/LOCALITY_FIRST/LOW_LATENCY, thread-safe strategy switching), local-force gate (≤0.5B models), VRAM-first scheduling (≥13B), score-based node selection with capability filtering, task lifecycle (PENDING→RUNNING→COMPLETED/FAILED/TIMEOUT/MIGRATED), recursive cancel, model auto-degradation chain, migration, KV cache pool with FMP sync, AST diff-only transmission, task sharding (inference/AST/vectorize, shard timeout), heartbeat timeout, task-level circuit breaker (S1 dispatch-fault auto-ban).

#### 节点注册幂等 + 故障黑名单 (F-A12 / F-A13, #20)

- **F-A12 幂等注册**: 同一 `node_id` 再注册 = PATCH 语义 — 保留 Master 权威运行态字段
  (`active_tasks`/`max_tasks`/`network_rtt_ms`/`status`), 只更新硬件声明字段
  (内存/CPU/GPU/hostname/port)。节点重启不丢运行态, 不会冲掉派发中的任务计数。
- **F-A13 故障黑名单**: `report_fault` 在 `_FAULT_WINDOW_S` (60s) 窗口内累积达
  `_FAULT_THRESHOLD` (3) → 自动 ban `_BAN_DURATION_S` (300s)。ban 期内 `register_node`
  返回 `False` (HTTP 403 拒绝)。`unregister_node(reason="banned")` 主动拉黑。
  到期惰性自动解封; `is_node_banned()` / `unban_node()` 手动查询/解封。

```python
# 故障熔断: 连报 3 次 → ban 5 分钟, ban 期内拒绝再注册
await master.report_fault("node_1", "oom", "out of memory")
assert not master.is_node_banned("node_1")
await master.report_fault("node_1", "oom", "again")  # 第 3 次触发 ban
assert master.is_node_banned("node_1")
assert await master.register_node(node) is False       # ban 期拒绝
master.unban_node("node_1")                            # 手动解封
```

#### 任务级熔断器 (S1, #70) — 派发失败自动 ban

- **派发失败报故障**: `_dispatch_to_node` 失败 (SSRF 拒绝 / agent HTTP 非 200 / agent 返回非 ok)
  → 自动调 `report_fault(node_id, "dispatch_failed")`, 计入 F-A13 故障窗口。
- **调度跳过 ban 节点**: `select_nodes` 候选过滤跳过 ban 期内节点 — 原仅 `register_node`
  拦截, 调度路径漏拦, 故障节点会被反复派发; S1 补齐调度侧拦截。
- 连续派发失败达 `_FAULT_THRESHOLD` (3) 自动 ban, ban 期内不再被选中; 到期/解封后恢复可选。

```python
# 派发失败 3 次 → 节点自动 ban, select_nodes 不再选它
for i in range(master._FAULT_THRESHOLD):
    await master._dispatch_task(task_failing_on_node_1)
assert master.is_node_banned("node_1")
assert await master.select_nodes(ParallelMode.DATA, count=1) == []  # 全 ban 返回空
```

#### 生产监控指标端点 (S2, #71) — Prometheus exposition

- **`GET /api/v1/metrics`**: 纯文本 Prometheus 0.0.4 exposition, 无外部依赖, 可被 Prometheus / Grafana agent 直接抓取。
- 集群级聚合指标:
  - 节点: `fusion_cluster_nodes_total` / `fusion_cluster_nodes_online`
  - 任务: `fusion_cluster_tasks_total` / `_running` / `_pending` / `_completed` / `_failed`
  - 重试: `fusion_cluster_task_retries_total` (counter)
  - KV: `fusion_cluster_kv_cache_entries`
  - 内存: `fusion_cluster_memory_total_gb` / `_available_gb`
  - 派发延迟: `fusion_cluster_dispatch_latency_seconds` (summary, p50/p90/p99 + sum/count)
- 复用 `get_stats` + 派发延迟 (`completed_at - started_at`) + `_retry_count`。Bearer 鉴权不豁免 — 内部抓取携带集群 token。

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:11452/api/v1/metrics
```

### Master Election (`fusion_multi_node.master.election`) — P4 已接 start(), 默认关闭

> **当前状态: P4 已接线 `ClusterMaster.start(ha_config=...)`。** `ha.enabled=True` 时调
> `setup_election` 启动选举循环; 默认 `enabled=False` 单 Master 向后兼容。Raft-simplified
> 优先级投票, `on_elected`/`on_deposed` 回调。**注意:** StandbyMaster/LEARNING 状态同步 +
> 持久化 term/vote 仍为原型, 不构成完整 HA 承诺; 多 Master 部署需自行确保状态同步与持久化。
>
> **H3 任务持久化 (v0.8.2, 已接):** 即使单 Master 无完整 HA, RUNNING/PENDING 任务会原子落盘
> (`~/.fusion/multi-node/tasks.json`), Master 进程崩溃后重启 `start()` 自动 `_restore_tasks`
> 恢复 (RUNNING→PENDING 重派), 不丢任务。
>
> **H2 崩溃自愈 (v0.8.2, 已接):** launchd 进程守护 — `./start.sh install-launchd` 渲染
> `deploy/com.dahai80.fusion-multi-node.plist` (KeepAlive 崩溃 10s 节流自动重启) → launchctl load。
> 崩溃 → launchd 重启 → H3 恢复任务 = 自愈闭环, 不丢任务。详见 `docs/HA-CRASH-RECOVERY.md`。

```python
from fusion_multi_node.master import ClusterMaster

master = ClusterMaster(host="127.0.0.1", port=11452)
await master.start(ha_config={
    "enabled": True,
    "node_id": "master-1",
    "priority": 5,
    "peers": ["master-2", "master-3"],
})
```

### Cloud API Fallback (`fusion_multi_node.master.cloud_fallback`) — ⚠️ 合规边界外, v0.8.2 调度路径已切断

> **违反"100%本地/离线"定位。** 本项目定位为本地优先离线集群, 不提供云 API 出站。**v0.8.2 起 `ClusterMaster` 调度路径已全部切断** — `setup_cloud_fallback` / `fallback_to_cloud` / `_cloud_client` / `_retry_loop` 云端分支均已删除, Master 不再可达云 API。`cloud_fallback.py` 模块文件 + 单元测试保留供独立验证, 计划迁移至 fusion-gateway (issue #106)。以下模块级 API 仍可独立使用, 但不应接入本地集群调度:

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
mdns.register(port=11452, properties={"role": "master"})
master = await mdns.find_master_async(timeout=5.0)

# Manual IP join (mDNS fallback)
client = ManualJoinClient()
resp = await client.join(master_host="10.0.0.1", master_port=11452, node_id="node-1")

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

### 7. Autoscaler (`fusion_multi_node.autoscaler`) ⚠️未接线

> **状态**: 代码完整但**未接入 ClusterMaster 生命周期**。`ClusterMaster._autoscaler` 从未赋值,
> `/api/v1/autoscaler/config` GET/PUT 返回 404「Autoscaler 未启用」。本节为库级 API 参考,
> 非现网可用功能。启用需在 `ClusterMaster.start()` 中实例化并启动评估循环。

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

> **状态**: `StorageVolume`/`CheckpointManager`/`DistributedKVStore` 库级可用。
> `ShardReplicator` 的 FMP 跨节点传输与 quorum 读写在生产路径**未接线**
> (`set_fmp_interface` 无人调用)。quorum 读写已加 E9 守卫: 无 `storage_volume` 时
> 一律拒绝 (`error=no_storage_volume`), 不再退回内存自洽, 避免谎报多数持久化成功。
> 本节为库级 API 参考。

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
    "name": "fusion-cluster",
    "master_host": "127.0.0.1",
    "master_port": 11452,
    "discovery_port": 11450,
    "agent_port": 11445,
    "mcp_port": 11446,
    "heartbeat_timeout": 15.0,
    "heartbeat_interval": 3.0,
    "report_interval": 15.0
  },
  "parallel": {
    "default_mode": "pipeline",
    "pipeline_timeout": 300.0,
    "data_parallel_timeout": 120.0,
    "caveman_compress": true,
    "communication": "auto"
  },
  "mlx": {
    "fusion_mlx_port": 11432,
    "fusion_kb_port": 11434,
    "fusion_desk_port": 9000,
    "model_hub_port": 11435
  },
  "mcp": {
    "enabled": true,
    "token_budget": 10000000,
    "tool_timeout": 60.0
  },
  "observability": {
    "retention_hours": 24.0,
    "alert_enabled": true,
    "log_level": "info"
  }
}
```

**端口迁移**: v0.6.5 旧端口（master 9753 / discovery 9754 / agent 9755 / mcp 9756 / fusion_mlx 8000）会在加载配置时自动迁移到当前默认值，并把误设的 `master_host=0.0.0.0` 回退为 `127.0.0.1`，迁移后写回 `config.json`。`ClusterConfig` 加载使用深拷贝，`set()` 不会污染类级 `DEFAULT_CONFIG`。


---

## 🧪 Testing

```bash
pip install -e ".[test]"

# Run all tests (805 tests)
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
| Master port | 11452 | Cluster Master service port |
| Discovery port | 11450 | mDNS discovery port |
| Agent port | 11445 | Node Agent port |
| MCP port | 11446 | MCP Gateway port |
| Heartbeat timeout | 15.0s | Stale node threshold |
| Task timeout | 300.0s | Default task timeout |
| KV cache TTL | 3600.0s | Default KV cache expiry |
| Token budget | 10,000,000 | MCP gateway token limit |
| Degradation chain | 70b→32b→13b→8b→3b→1b | Model auto-degradation |

---

## 📋 Changelog

### v0.7.0 ✅ (Current) — 对抗性审查修复 (AR 2026-08-24)

**P0 安全地基重构**
- [x] F1-F2 path traversal 防护: cluster_sync 路径遍历拦截 (NUL/绝对/drive/normpath + is_safe_path_segment)
- [x] F3-F4 SSRF 守卫: is_safe_peer_host 拒环回/链路本地/元数据/多播, build_safe_url 强制 scheme
- [x] F5 TLS key 持久化: 私钥 NoEncryption + 文件权限 0600
- [x] F6 TLS pinning: 无 pin fail-closed (raise), pin 指纹 CERT_REQUIRED+VERIFY_PEER+DER 回调
- [x] F7 FMP protobuf 二进制 payload base64, 禁 utf-8 replace 损坏
- [x] F8 fmp_server shard_id/file_path 路径校验
- [x] F9 mDNS sticky-master + node_id 绑定 cluster_hash, 防 Worker 伪造 master
- [x] F10 validate_node_id 拆 is_safe_path_segment + is_safe_peer_host, 所有 sink 加固

**P1 现网路径正确性 + 生命周期**
- [x] #8 assign_task TOCTOU 消除: re-check-inside-lock
- [x] #9 heartbeat/fault 路由走加锁方法, 未知节点 404 (fail-visible)
- [x] #10 真任务取消: CANCELLED 状态, Master→Agent /api/tasks/cancel 中止运行推理
- [x] #11 SIGTERM + 优雅关停 drain: asyncio.Event + 信号处理 + 在途 task 协程 gather
- [x] #12 config.save() 原子写: temp + os.fsync + os.replace
- [x] #13 task_id uuid4 替 int(time.time())

**P1 HA 接线或砍 + 合规边界**
- [x] #14 砍 HA 虚假宣称: StandbyMaster/MasterElection/setup_election 标未接线死代码, 现网单 Master 无 HA
- [x] #15 合规边界: cloud_fallback **v0.8.2 调度路径已切断** (ClusterMaster 不再可达云 API); mcp_gateway/ast_diff/cluster_sync 功能归属债待迁移 fusion-gateway (#106) / fusion-cowork (#61); cluster_sync LAN-only is_safe_peer_host 加固

**P2 未接线原型门禁 + security 接线 + 无界增长**
- [x] #17 DataScrubber 补 openai_key/github_pat/slack_token/jwt_token + 数字边界修 CJK 邻接; DataIsolation realpath+commonpath 防符号链接绕过; PermissionManager block-by-default (已验证 fail-closed)
- [x] #18 _metric_times list→deque(maxlen=10000) 对齐 metrics, 修无界增长+索引错位
- [x] #24 WorkerSandbox 接 NodeAgent 执行路径: `execute_task` 入口 `_sandbox_gate` 校验任务携带路径/网络 (`check_path_access`/`check_network_access`), 拒则不派发 (硬伤5: security/ 原死代码零过滤 → 进程内 gate 实防御); `_execute_model_sync` 走 `is_safe_peer_host`+`build_safe_url`+`is_safe_path_segment` (与 master_server 一致, 修弱 `.replace()`); 不接 `apply_limits`/`setrlimit` (进程级资源限制误杀单长跑 agent), `SandboxExecutor` 仅子进程插件适用
- [x] #23 M9/M10 集成测试门禁 — 硬伤4 四处契约 bug 修复+回归门禁 (audit 允许: 接线 OR pragma/移除; 选修复, 真 correctness, 可单测):
  - caveman 字典压缩静默损坏: 变长 2/4 字节码无定界 → 解压只读 2 字节永不匹配。改定长 2 字节码 (`>H`, `dictionary_size` 截断 65536) + 长度前缀记录 (控制字节 0x01=字典命中/0x02=原始透传)
  - autoscaler 冷却门绕过: `update_config` 清零 `_last_action_time` → 热重载即绕冷却, 连续扩缩容风暴。改为保留上次动作时间, 冷却跨热更新连续生效
  - kv_store/fmp_server 签名不匹配: `_on_kv_get` 调 `get_entry(key, partition)`, 原签名 1 参 → inbound KV_GET 必 TypeError。`get_entry` 增可选 `partition`, 给定校验分区匹配; `ttl` None→`or 0.0` 防 `is_expired` TypeError
  - shard_replication quorum 虚假宣称: `_sync_via_fmp` fire-and-forget (`ensure_future` 不 await) 却返 `success=True`/`checksum_verified=True` → quorum 写保证虚构。诚实化: 仅同步 `await` 的 send 称 `success`, fire-and-forget 标 `success=False`+"未确认"日志, `checksum_verified` 恒 False (无应用层 ACK)
- [x] M9/M10/shard_replication 未接线原型标非生产 (audit 允许: 接线 OR pragma/移除); WorkerSandbox 已接 (#24), M9/M10 契约 bug 已修 (#23)

回归: 826 tests passed, 0 ruff errors.

### v0.7.1 ✅ — 二轮架构审计修复 (2026-08-24, 22 项)

> 审计源: `audit/fusion-multi-node-audit-report-0824.md` (363 行, H1-H5 / R1-R8 / E1-E9)。
> 流程: 涉上游问题先提 issue → 落地 code (PR #18, 分支 `release/v0.7.0-ar-audit-fixes`)。

**P0 (H1/H4/E2/E7) — 伪实现/死代码/诚实性**
- [x] H1 核实 fusion-mlx 无 `/distributed/*` 端点 → 上游 issue #621; distributed_bridge Pipeline 标未实现 + 诚实报错 (in-repo)
- [x] H4 四死子系统 (HA/autoscaler/cluster_sync/shard_replication) 标未接线 + 移除对外暴露路由
- [x] E2 kv_transfer `source_node` 用真实节点地址 (非 `localhost`)
- [x] E7 kv_warm 目标节点从在线节点表取 (非空集默认值)

**P1 (H2/H5/R1/R2/R8/E3/R6) — 并发/性能/正确性**
- [x] H2 拆 ClusterMaster 单锁按资源域 (nodes / tasks / kv)
- [x] H5 LoadRouter/KVSharing threading.Lock → asyncio (去跨线程阻塞)
- [x] R1 硬件信息启动缓存, 心跳只取动态字段
- [x] R2 task_id uuid4 替 `int(time.time())`
- [x] R8+E3 distributed_bridge `raise_for_status` + 响应 schema 校验 + 错误日志
- [x] R6 `get_online_nodes` 纯快照 (不触发副作用)

**P2 (H3/R3/R4/R5/R7/E1/E4/E5/E6/E8/E9) — 原型门禁/安全/健壮性**
- [x] H3 HA 死代码 (StandbyMaster/MasterElection) 文档降级标注
- [x] R3 `sync_kv_cache` 仅登记元数据, 返回 False (张量迁移待上游 #621)
- [x] R4 `cancel_task` 改 `asyncio.gather` + 复用单 AsyncClient (去顺序通知)
- [x] R5 agent `_running_tasks` set + 五维负载上报
- [x] R7 模型大小正则边界匹配 (防 `1b` 误匹配 `10b/100b`)
- [x] E1 `ClusterSyncManager` 移到 `__init__` + `start()`/`stop()` 生命周期 (4 路由折叠)
- [x] E4 config 字段级校验表 + `schema_version` + `set_many` 批量单落盘 + 加载自修复脏值
- [x] E5 plugin/action/model_name `is_safe_path_segment` 净化 + `_sandbox_gate` 覆盖全任务类型
- [x] E6 `model_config` 失败 raise 不静默吞
- [x] E8 mDNS `_discovered` 跨线程加 `threading.Lock` (修 dict changed size race)
- [x] E9 quorum 读写无 `storage_volume` 一律拒绝 (不再内存自洽谎报多数持久化)

回归: 849 tests passed, 0 ruff errors.

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

### v0.5.0 ✅
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
- [x] 805 tests, 0 ruff errors

### v0.8.2 ✅ — 生产就绪硬阻断 + 软债 (2026-08-25)
- [x] H3 Master 任务持久化 + 崩溃启动恢复 (原子落盘, RUNNING→PENDING 重派)
- [x] H2 launchd 进程守护 — 崩溃自愈闭环 (KeepAlive 重启 + H3 恢复)
- [x] H4 cloud_fallback 调度路径切断 (100% 本地合规); 功能归属债待迁移 fusion-gateway/fusion-cowork
- [x] H1 PIPELINE 无 token 输出 — 上游 fusion-mlx #630 (decode/lm_head 端点, 本仓不可修)
- [x] S1 任务级熔断器 — 派发失败报故障 + select_nodes 跳过 ban 节点
- [x] S2 生产监控指标端点 /api/v1/metrics (Prometheus exposition)
- [x] 882 tests, 0 ruff errors

### Future
- [ ] S3 负载/压测基线测试 (派发吞吐 / 尾延迟 / 无丢失)
- [ ] S4 真实模型集成测试覆盖 (DATA 并行 E2E + KV 共享 E2E)
- [ ] Distributed MLX operator bridge (mlx.distributed API)
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

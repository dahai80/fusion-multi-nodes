<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>Cluster scheduling core for distributed Apple Silicon MLX inference</strong></p>
  <p><em>Pool multiple Macs into a unified AI cluster — pipeline parallelism, data parallelism, 100% local-first.</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.12.2-blue" alt="Version">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License">
  <img src="https://img.shields.io/badge/tests-1268%20passed-brightgreen" alt="Tests">
</p>

---

> **📦 v0.12.1 (2026-08-28) — 审计 0826 P2+P3 整改 (15 项)**
>
> 审计 `fusion-multi-node-audit-result-product-0826.md` 判定 12 P2 + 3 P3 项全部代码修复落地
> (含设计取舍项 env-gated 破开, 非仅文档)。安全/资源 (3): mTLS 证书 SAN + `check_hostname=True` /
> MLXKVTransport SSRF 守卫 / docker-compose 资源上限。KV 容量 (2): export size 同步 / ban 期满主动探测。
> 事件/选举 (3): 选举锁外 I/O / 事件丢弃告警 / F2 动态子路径全 op。容器/隔离设计取舍破开 (4):
> sandbox rlimit / PARTIAL 崩溃补全 / PIPELINE 段级检查点 / observability deque 持久化 (均 env-gated)。
> 部署/配置 (3): autoscaler 措辞 503 / AgentServer KV 落盘 critical 告警 / MIGRATED 自动语义校准。
> 资源泄漏 (1): AgentServer.stop 调 kv_manager.close。基线 1262 → 1317 测试全绿。详见
> [CHANGELOG](docs/CHANGELOG.md)。至此审计 0826 全 47 项 (5 P0 + 27 P1 + 12 P2 + 3 P3) 落地完成。

---

> **📦 v0.12.0 (2026-08-27) — 审计 0826 P1 整改 (27 项)**
>
> 审计 `fusion-multi-node-audit-result-product-0826.md` 判定 27 P1 项全部代码修复落地。
> 容错调度 (8): H3 RUNNING→PENDING 重派补 `exclude_nodes` / 节点 OFFLINE 自动迁移在途 /
> `_pending_queue` 上限 503 / 重试指数退避 / agent_server 429 不累熔断 / 增量持久化 /
> httpx 连接池显式配置 / `sync_kv_cache` 异常分类。KV 张量 (2): `import_tensor` 区分降级
> vs 真失败 / 跨节点异常分类 + 连续失败告警。安全 (7): HTTP 派发可选 PII 脱敏 / cloud_fallback
> import-time 禁用守卫 / RBAC fail-closed + 全路由登记 / 审计写失败告警 / `compare_digest` /
> manual_join mTLS scheme。API 契约 (1): 9 raw dict → pydantic (422 not 400)。Agent (3):
> `/api/hardware` to_thread / 选举空窗 503 / 本地 `max_tasks` 过载 gate。性能/运维 (6): 真推理
> 吞吐基准 / Prometheus 节点级指标 / HA doc 校准 / kv+user fsync / 日志文件 stderr 提示。
> 基线 1213 → 1262 测试全绿。详见 [CHANGELOG](docs/CHANGELOG.md)。P2/P3 整改续 (v0.12.1)。

---

> **📦 v0.11.0 (2026-08-27) — GAP-7 KV 张量跨节点传输 (close #33)**
>
> `ClusterMaster.sync_kv_cache` 经可插拔张量后端 (合成默认 / MLX 真张量 env-gated
> `FUSION_KV_TENSOR_BACKEND=mlx`) 编排源 agent `/api/kv/export` → 目标 `/api/kv/import`,
> 返 `True`。`KVShard` 加 `tensor` 字段 (base64 压缩随 JSON 传输, `store_local` 预算门)。
> 合成后端满足 #33 验收 (张量 round-trip 跨 2 agent); 真张量待上游 fusion-mlx issue #650
> 落地激活 (env-gated bonus, 404→降级合成 + warn)。新增 `kv_tensor_transport.py` +
> 三组测试 (`test_kv_tensor_serialize.py` 11, `test_kv_export_import_routes.py` 6,
> `test_kv_tensor_e2e.py` 4+1 skip) + 改写 `test_new_features.py` sync 用例。
> 详见 [CHANGELOG](docs/CHANGELOG.md)。
>
> 已完成 (Phase A-F + GAP-7): issues/PR → RC → GAP-1 always-on → GAP-6 限流 → GAP-5 死代码 → F1-F5 多租户 → GAP-7 KV 张量传输。

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
| **Cluster Master** | Node discovery, resource scheduler, task lifecycle, KV cache pool, fault tolerance, task auto-degradation, load-aware routing, task sharding, AST diff, FMP KV sync, 真实张量 PIPELINE 层切分链 (接 fusion-mlx `/distributed/*`, ✅上游端点已交付 issue #621/#630 closed; 多节点客户端存根 `load_shard`/`pipeline_step` 已接, ⚠️真模型端到端验证待长期落地), master→agent 派发循环, **H3 任务持久化+崩溃恢复** (RUNNING/PENDING 原子落盘, 崩溃重启自动重派)。HA 选举接 `start(ha_config=)` (默认关闭单 Master; StandbyMaster 类为死代码原型)。cloud_fallback 调度路径 v0.8.2 已切断 (100% 本地) |
| **Node Agent** | Per-machine daemon, hardware reporting, task execution, mDNS auto-discovery, pipeline_step (上游 `/distributed/load_shard`+`pipeline_step` 已交付 issue #621 closed, b64.npy 激活跨节点, ⚠️真模型端到端待长期落地) |
| **mDNS Discovery** | Bonjour/mDNS zero-config node discovery, manual IP join fallback |
| **FMP Protocol** | Three-layer binary protocol, AES-GCM encryption, TCP long connection, circuit breaker, hop_count, FMP inbound server. ⚠️启动但从不作为派发传输 (仅 HTTP 派发) |
| **Distributed MLX Bridge** | Pipeline/data parallelism, model sharding, Caveman compression, KV cache sharing. ⚠️Master 级 KV 张量同步为 no-op (跨节点 KV 仅 HTTP 元数据, 非张量; P3-28 张量级传输待长期落地) |
| **MCP Cluster Gateway** ⚠️未接线 | Unified MCP endpoint, tool routing, Claude Desktop/Code integration. **当前零路由/零实例化/零 CLI, 死代码, 计划迁移 fusion-gateway #106** |
| **Security** | Node approval, Master/Worker permission isolation, Worker sandbox, OS-level sandbox-exec, data scrubbing, FMPCrypto (AES-256-GCM + ECDH), Metal AES-GCM acceleration |
| **Observability** ✅已接线 | Metrics, logs, alerts, log store & export, intelligent fault diagnosis, optimization suggestions. **P0-8 接 `ClusterMaster.start/stop` 生命周期 + `_health_check_loop` 周期采集指标/告警 (去重); `/api/v1/observability/{logs/export,suggestions,alerts}` 已返 200; `/api/v1/metrics` (Prometheus) 同样已接。全内存 deque, 重启即失 (P1 同类债)** |
| **Autoscaler** ⚠️未接线 | Conservative/Balanced/Aggressive scale policies, auto scale-up/down/rebalance, hot-reload config. **当前未接入 ClusterMaster 生命周期, `/api/v1/autoscaler/*` 返回 503 not-wired (非 404); 代码留作未来启用** |
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

#### Health endpoints (C11 — readiness vs liveness)

- `GET /api/health` — **liveness**: 本地依赖 (磁盘剩余 >512MB / 内存 >256MB / task-store 可写), 不检上游/节点 quorum。恒 HTTP 200, body `status: "ok"|"degraded"`。供 `start.sh` / docker livenessProbe — 进程活着即可, 不 block 启动。
- `GET /api/health/deep` — **readiness**: liveness + 节点 quorum (≥1 ONLINE 节点)。body `status: "ok"|"degraded"`, 含 `online_nodes` 计数。供 LB / 编排器 drain 半坏 master (本机健康但无可用节点 → 不 ready)。**不用于 inter-service depends_on** (会与 agent 启动死锁)。
- 两端点均豁免 Bearer 鉴权 (k8s probe 不带 token)。

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

> **当前状态: P4 + P0-1 已接线 `ClusterMaster.start(ha_config=...)`。** `ha.enabled=True` 时调
> `setup_election` 启动选举循环; 默认 `enabled=False` 单 Master 向后兼容。Raft-simplified
> 优先级投票, `on_elected`/`on_deposed` 回调。leader 心跳广播 + term/voted_for 持久化
> (`~/.fusion/multi-node/election_state.json`) 已接 (P0-1, 修 term churn 重选)。
> **注意:** `StandbyMaster` 类 (独立于 MasterElection) 仍为死代码原型, 非生产可用。
>
> **H3 任务持久化 (v0.8.2, 已接):** 即使单 Master 无完整 HA, RUNNING/PENDING 任务会原子落盘
> (`~/.fusion/multi-node/tasks.json`), Master 进程崩溃后重启 `start()` 自动 `_restore_tasks`
> 恢复 (RUNNING→PENDING 重派), 不丢任务。
>
> **H2 崩溃自愈 (v0.8.2, 已接):** launchd 进程守护 — `./start.sh install-launchd` 渲染
> `deploy/com.dahai80.fusion-multi-node.plist` (KeepAlive 崩溃 10s 节流自动重启) → launchctl load。
> 崩溃 → launchd 重启 → H3 恢复任务 = 自愈闭环, 不丢任务。详见 `docs/HA-CRASH-RECOVERY.md`。
>
> **部署方案**: 单机 nohup / 单机 launchd 守护 / docker-compose 多机小集群 / 多 Master HA (技术预览)。
> 本项目定位 local-first Apple Silicon 小集群, **非 K8s 编排目标** — 详见 `docs/DEPLOYMENT.md`。
> **运维手册**: 故障处置 (节点/Master 下线、脑裂、磁盘满、fusion-mlx 不可达、任务积压) + 版本升级/备份恢复/Token 轮换 — 详见 `docs/OPERATIONS.md`。

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

### 2. Node Agent (`fusion_multi_node.agent`)

Runs on every Mac — hardware metrics, heartbeat, task execution via fusion-mlx API.

**Health endpoints (C11)**: `GET /api/health` (liveness — 磁盘/内存 + fusion-mlx 端口探测, 无 HTTP 出站) / `GET /api/health/deep` (readiness — liveness + 真 HTTP 探 fusion-mlx `/v1/models`, 判定 agent 是否真能推理)。两端点豁免 Bearer 鉴权。

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

#### mTLS 节点互信 (#80)

集群内节点互连可选双向 TLS (mTLS), 私有 CA + 每节点叶证书。env 开关 `FUSION_MTLS_ENABLED=1` 启用, 关闭则全 http no-op (不破坏现有测试/CLI)。

```python
from fusion_multi_node.security import mtls

# 生成集群 CA (3650 天) + 每节点叶证书 (CN=node_id, O=role, 365 天)
ca_cert, ca_key = mtls.provision_cluster("/path/to/ca")
node_cert, node_key = mtls.provision_node("worker-1", "worker", ca_cert, ca_key, "/path/to/worker-1")

# 服务端: uvicorn.Config(**server_ssl_kwargs()) — 要求对端客户端证书 (CERT_REQUIRED)
# 客户端: httpx.AsyncClient(**client_kwargs()) — verify=ctx 同时验服务端证书 + 呈递客户端证书
# URL scheme: mtls.scheme() → "https" / "http"
```

细粒度权限 (mTLS 开启时强制): AgentServer 从 `X-Node-Id`/`X-Node-Role` header 取调用方身份 → `PermissionManager` 校验路径权限。
- MASTER: 全部 API (含 execute + cancel)
- WORKER: execute / heartbeat / KV lookup-transfer / hardware; **无** cancel
- 强制模式缺 `X-Node-Id` → 403; 角色无权 → 403
- 兼容模式 (mTLS 关) 无 header → 放行 (现有 http 测试/CLI 不带头)

#### 多租户配额 + 优先级队列 (#81)

P1-H 多租户调度: 全局默认每租户最大并发运行任务数, 超额入优先级队列 (非拒绝); 高优先级任务排队时优先获得空闲节点 (非抢占, 不杀运行中任务)。

```python
from fusion_multi_node.master import ClusterMaster

master = ClusterMaster()
master.configure_scheduling(tenant_max_concurrent=4)  # 0 = 不限配额 (节点容量仍限)

# 超配额任务自动入队, assign_task 返回 True (非拒绝)
await master.assign_task(task)
# 队列按 priority 降序, 节点上线 / 任务完成 / 取消占槽任务 → 排空队首
```

- 配额全局默认: 配置键 `scheduling.tenant_max_concurrent` (默认 4, 0=不限), CLI 启动自动加载
- 超额入队: 租户运行任务达配额 → 新任务 `TaskStatus.PENDING` 入队, `assign_task` 返 True
- 无节点入队: `select_nodes` 无可用节点 → 入队 (不再返 503), 节点上线排空
- 优先级: `ClusterTask.priority` (TaskPriority: LOW=0/NORMAL=1/HIGH=2/CRITICAL=3), 队列降序排
- 排空触发: `complete_task` / `register_node` / `cancel_task` (取消占槽任务释放并发槽)
- HTTP: `POST /api/tasks/submit` 入队返回 `202 {"queued": true}` (派发成功仍返 200)
- 取消: `cancel_task` 递归移除队列中主/子任务; 队列任务注册于 `master.tasks` 可查/可取消

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
> `/api/v1/autoscaler/config` GET/PUT 返回 503「Autoscaler 未接线 (not-wired)」。本节为库级 API 参考,
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

# Run all tests (888 tests)
pytest tests/ -v

# With coverage
pytest tests/ --cov=fusion_multi_node --cov-report=html

# Run specific module
pytest tests/test_cluster_master.py -v
pytest tests/test_protocol.py -v
pytest tests/test_new_features.py -v
```

### 真实模型 E2E (需 fusion-mlx 运行)

```bash
~/claude-home/fusion-mlx/start.sh start        # 启动推理引擎 (端口 11434)

# DATA 并行 2 节点真推理 (skip-gate: fusion-mlx alive + 模型在 /v1/models 列表)
pytest tests/test_data_parallelism_e2e.py -v

# 跨节点 KV 缓存共享 (合成数据, 无需模型, 无 skip-gate)
pytest tests/test_kv_sharing_e2e.py -v

# Pipeline 并行层切分真推理
pytest tests/test_pipeline_e2e.py -v

~/claude-home/fusion-mlx/start.sh stop         # 用完关闭
```

> 默认模型 `mlx-community-Llama-3.2-1B-Instruct-4bit`, api_key 走配置 `mlx.fusion_mlx_api_key`。
> fusion-mlx 停时 E2E 自动跳过 (skip-gate), 不阻塞 CI 全绿。

### 跨机真网络 E2E (#76)

真 bind 端口 + 真 HTTP 跨进程 (非 ASGITransport) — 进程内起真 uvicorn 真端口, 跨 TCP socket 通信。

```bash
# 真端口跨进程: 注册 / 派发 / 掉线重连 (免真模型, FakeBackend)
pytest tests/test_real_network_e2e.py -v

# 容器跨机: docker-compose 1 Master + 2 Agent (skip-gate docker 可用)
pytest tests/test_real_network_e2e.py::TestContainerE2E -v
```

- 真注册: agent 经真 HTTP `/api/nodes/register` 到 master (真 socket)
- 真派发: master → agent `/api/execute` 跨 HTTP (FakeBackend 完成非真推理)
- 掉线重连: 停 agent → master 心跳超时标 OFFLINE → 重启同节点 → 重连恢复 ONLINE + 可派
- 容器 E2E: `docker compose up --scale agent=2` 跨容器注册 + 派发; docker 不可用时 skip

### 跨机 KV 共享规模化压测 (#79)

N 真端口 agent 跨 HTTP 验 KV 缓存大规模迁移 — warm_cache 规模 + transfer 迁移 + 延迟 + 0 丢失 (合成 KVCacheEntry, 免真模型)。

```bash
# 4 压测用例: warm 规模 / warm 延迟 / warm→transfer 迁移 / 显存累计
pytest tests/test_kv_stress.py -v
```

- warm 规模: M prompt × N node 全成功 (0 丢失)
- warm 延迟: 单次 warm p99 < 1.0s
- transfer 迁移: warm 到 node-0 → transfer 拉取到 node-1, 跨节点 0 丢失 (推模型: 源节点回传序列化 entry → 目标 store_local)
- 显存累计: local_entries / total_size_bytes 代理显存占用

> KV transfer 推模型修复 (v0.8.4): 原 `/api/kv/transfer` 路由回调 `transfer_from_remote` 致递归 + source_node 含冒号过 sanitize 失败 — 改推模型 (源节点查本地回传 entry, 目标反序列化 + store_local), 补 `_serialize_entry` + `lookup_local_by_id`。

### 容器节点自动审批 (v0.8.4)

`docker-compose` master 默认配 `FUSION_AUTO_APPROVE_PATTERNS` (可信网段子串匹配) — 容器/LAN 节点免手动 `cluster approve` 自动加入。

```bash
# compose 默认: 192.168. / 10. / 172.16.0.0/12 网段自动审批
docker compose up -d --scale agent=2

# 裸机自定义可信网段 (逗号分隔; CIDR 优先精确匹配, 非 CIDR 回退子串/通配)
FUSION_AUTO_APPROVE_PATTERNS="10.0.1." ./start.sh start
```

> 生产仅对可信网段开放自动审批; 未配 env 则走手动审批门 (`fusion-multi-node cluster approve <node_id>`)。

### 端口冲突明确报错 (v0.8.7)

issue #25 后续: NodeAgent 默认端口已于 v0.8.0 迁出 11445 → 11458 (与 fusion-comfyui 解冲突, `_STALE_PORT_MAP` 自动迁移旧配置)。本次补 bind 失败明确报错 — `AgentServer.start` / `MasterServer.start` 捕获 `OSError`, 对已知冲突端口 (comfyui 11445 / fusion-mlx 11432/11434 / master 11452 / mDNS 11450 / MCP 11446) 附提示 "(与 {服务} 默认端口冲突)", 非通用 bind 错误。测试: `test_start_port_conflict_raises_with_hint` (agent + master, mock uvicorn serve 抛 EADDRINUSE)。全量 946 passed 1 skipped。

### Phase 4 故障注入 E2E (v0.8.6)

调度器对真实故障的端到端自愈验证 (真 ASGI 路由, 非单元 mock; 推理用合成 FakeBackend, 不触 fusion-mlx):

1. **agent 宕机 → 超时 → 重试 → 重派存活节点** — agent-a 移出路由 (模拟宕机, 派发 404), 任务超时 `check_timeouts` → `_enqueue_retry` (TIMEOUT→PENDING) → 排空重试队列 `assign_task` (select_nodes 跳过 ban 的 agent-a) → 落 agent-b → COMPLETED。锁全链路: 超时入队 + 重派存活 + 任务完成。

2. **反复派发失败 → ban → 新任务路由存活节点** — agent-a 宕机, 连续派发 `_FAULT_THRESHOLD` 次均 404 → `report_fault` 窗口内达阈值自动 ban → 新任务 `select_nodes` 跳过 ban 节点 → 路由 agent-b → COMPLETED。集成级验证 (现有 `test_task_circuit_breaker` 为单元级)。

3. **HA leader 故障 → standby 升 leader → 恢复派发 + 同步任务可读** — m1 (leader) 持有任务经 `_persist_tasks` → HTTP 推送到 m2 (standby) `receive_synced_tasks` 落盘; m1 降级 + m2 升 leader (`_on_demoted_from_leader`/`_on_elected_leader` 翻 `_is_leader`) → m2 `assign_task` 不再因 standby 守卫返回 False → 同步任务接管后不丢失。

测试: `tests/test_fault_injection.py` (3 场景, PortRoutingTransport + 真 AgentServer `/api/execute` + FakeBackend)。全量 943 passed 1 skipped。

### KV 跨节点 lookup 契约修复 + 审批 CIDR 精确匹配 (v0.8.5)

两处严格审视暴露的缺陷修复:

1. **`lookup_remote` 永远返回 None** — `/api/kv/lookup` 路由返扁平 dict (无 `found`/`entry` 键), `lookup_remote` 解码 `data.get("found")` 恒 falsy → 跨节点 KV 复用查找静默失效。单元 mock 捏造 `{"found":True,"entry":{...}}` 形状掩盖此 bug (假信心测试)。修复: route 对齐契约返 `{"found":True,"entry":_serialize_entry}`, 补真链路 E2E 锁契约 (`test_kv_lookup_remote_cross_node_contract` — store node-a, node-b 经 HTTP 查回, 非 mock)。

2. **自动审批 `"172."` 子串过匹配公网** — compose 默认 `172.` 子串匹配公网 `172.0–15`/`172.32–255` (私网仅 `172.16.0.0/12`)。修复: CIDR 优先精确匹配 (`ipaddress.ip_network` 包含判定), 非 CIDR 回退子串/通配兼容旧配置; compose 默认改 `172.16.0.0/12`。补回归测试 (`test_auto_approve_cidr_precision` — `172.16.1.5` 放行 / `172.1.2.3` 拒绝)。

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
- [x] H1 核实 fusion-mlx 无 `/distributed/*` 端点 → 上游 issue #621; distributed_bridge Pipeline 标未实现 + 诚实报错 (in-repo)。注: 上游 #621/#630 后续已交付, 真 E2E `tests/test_pipeline_e2e.py` 验证通过
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
- [x] R3 `sync_kv_cache` 经张量后端编排骨跨节点传输, 返 True (P3-28 / GAP-7 / #33 已交付 v0.11.0; 合成默认 + MLX 真张量 env-gated 待上游 #650)
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

### v0.10.3 ✅ — GAP-8 Phase F1: 多租户令牌基座 (2026-08-27)
- [x] **per-user 令牌存储** (`security/user_store.py`) — `UserStore` 文件持久化 `users.json` (scrypt 哈希, 0600, 原子写), 令牌格式 `fmu_<uid>_<secret>`, 多活签发/吊销/轮换
- [x] **UserRole** (`security/permission.py`) — 与 NodeRole 正交的 ADMIN/USER/VIEWER + `check_user_path_access` 路径鉴权
- [x] **双令牌中间件** (`utils/auth.py`) — `BearerAuthMiddleware` 按 `fmu_` 前缀分流到 UserStore, cluster_token 热路径 O(1) 不变; 无 user_store 回退纯 cluster_token (单租户零配置向后兼容)
- [x] **首启引导** — `FUSION_BOOTSTRAP_ADMIN` env 自动创建 ADMIN + 签发首令牌
- [x] 28 个新测试 (test_user_store 22 + TestUserTokenAuth 6); 1112 tests, 0 ruff errors

### v0.10.2 ✅ — GAP-5 死代码清理/标注 (2026-08-26)
- [x] **autoscaler 路由显式 not-wired** (GAP-5) — `GET/PUT /api/v1/autoscaler/config` 由歧义 `{"enabled":False}` 改 503 + detail 明示未接线; 模块保留待迁移
- [x] **StandbyMaster 死代码删除** (GAP-5) — 零实例化/零 import/零测试/零引用, 独立于已接线的 MasterElection; HA 路径唯一化为 MasterElection
- [x] 2 个 autoscaler not-wired 测试; 1085 tests, 0 ruff errors

### v0.10.1 ✅ — GAP-6 限流适配 (2026-08-26)
- [x] **客户端限流适配** (GAP-6) — `agent/rate_pacer.py` 拦截 fusion-mlx 429: 读 `Retry-After`, 指数退避重试 (3 次, 10s 预算, 确定性无 jitter), 耗尽抛 `RateLimitExhausted`
- [x] `FusionMLXBackend.chat`/`embed` 经 `dispatch_with_pacing` 包裹 (不再直接 `raise_for_status` 误判 429 为逻辑错误)
- [x] master 限流归类修正 — `rate_limited` → 瞬时失败 (`transient_fail`, 可重试), 不进 `logic_fail`, **不调 `report_fault`, 不 ban 健康节点**
- [x] 上游 fusion-mlx #635 CLOSED (PR #637, `--rate-limit 0` 真正关闭限流, 默认关); 显式上限 429 由退避吸收
- [x] 16 个限流测试 (14 unit + 2 集成); 1083 tests, 0 ruff errors

### v0.10.0 ✅ — GAP-1 always-on SLA (2026-08-26)
- [x] **HA 全状态同步** (GAP-1) — leader 周期推 nodes/kv_cache/banned_nodes 到 standby; standby 持完整拓扑, failover 即调度 (always-on 空窗 ≤ 选举超时 ~10s)。HA 仍 opt-in, 2+ Master 显式配置获 always-on, 单 Master 部署不变
- [x] `/api/ha/sync-state` 端点 + `receive_synced_state` 幂等合并 (锁序 nodes→kv 不嵌套); `_state_sync_loop` (5s) 接 `start()`/`stop()` 生命周期
- [x] 6 个 HA 状态同步测试 (拓扑同步 / 幂等 / failover 立即调度 / 端点 round-trip / 单 Master 无目标 / 非法 status 回退)
- [x] 1067 tests, 0 ruff errors

### v0.10.0-rc.1 🔶 — Release Candidate (2026-08-26)
- [x] #31 重试节点规避 — `exclude_nodes` 硬黑名单 (select_nodes 过滤 + assign_task 透传 + 补选遵守, 打破重试回坏节点死循环)
- [x] GAP-4 CI 修复 — `pytest-randomly` 声明 + 3 个 Linux x86_64 不兼容测试 skip-gate
- [x] 复审计 §8 发布条件 2/4/5 披露补齐 — GAP-1 HA SPOF / GAP-6 吞吐上限 / GAP-5 死代码 + GAP-7 KV no-op
- [x] 单租户 LAN 可带条件商用; 多租户/远程 SaaS + always-on SLA 阻塞声明
- [x] 1061 tests, 0 ruff errors, CI 全绿

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
- [x] H1 PIPELINE token 输出 — 上游 fusion-mlx #630 decode 端点已交付 (closed); 真 E2E `tests/test_pipeline_e2e.py` 验证通过
- [x] S1 任务级熔断器 — 派发失败报故障 + select_nodes 跳过 ban 节点
- [x] S2 生产监控指标端点 /api/v1/metrics (Prometheus exposition)
- [x] S3 负载/压测基线测试 (调度层吞吐 / 尾延迟 / 无丢失)
- [x] S4 真实模型集成测试覆盖 (DATA 并行 E2E 真推理 + KV 共享 E2E 真 ASGI 路由链; 附修 3 生产 bug: FusionMLXBackend `/v1/*` 漏鉴权 / KVSharingManager 跨节点 HTTP 漏鉴权 / KVWarmRequest 契约错配)
- [x] 888 tests, 0 ruff errors

### v0.8.3 ✅ — 容器规模化压测 + 调度 TOCTOU 修复 (2026-08-25)
- [x] P0-A HA 双 Master 选举接 `start(ha_config=)` 默认开 (election HTTP vote 层复用, 无外部依赖)
- [x] P0-B 容器化 — `Dockerfile` + `docker-compose.yml` (1 Master + N Agent, `--scale agent=N` 无上限扩容); agent 经容器 bridge IP 回连, 不占主机端口; 推理引擎裸机 `host.docker.internal:11434` 回连
- [x] BUG#3 Agent 容器内本机 IP 探测 — 跨平台 socket UDP connect (零依赖, 取 master 回连源 IP), 替代 macOS-only `ipconfig`
- [x] BUG#4 NodeApprovalManager 审批路径丢硬件元数据 — register 透传 metadata, approve 从 metadata 重建 NodeInfo (mem/max_tasks/cpu 不再回退默认 0/4)
- [x] **调度 TOCTOU 竞态修复** — `select_nodes` 锁外执行 → 并发抢占首选节点满载 → 锁内补选空闲节点 (`_select_free_nodes_locked`), 不再直接 503。c8 并发 40 任务 0× 503 验证
- [x] `FUSION_AGENT_MAX_TASKS` env — 单 agent 并发上限可调 (压测时 16)
- [x] 容器压测客户端 `scripts/stress_live.py` — 经 master:11452 并发提交, 测吞吐/尾延迟/成功率; `--rps` 客户端速率门对齐上游限流桶
- [x] 集群运维工具 `scripts/cluster_ops.py` — approve-all / status / unban-all
- [x] P1-E 观测栈模板 — Grafana dashboard / Prometheus / Alertmanager (deploy/observability/)
- [x] 阶段3 调度压测通过 — 4 节点 50 任务 success 1.0, c8 contention 40 任务 success 1.0, 0× 503
- [x] 上游阻塞 fusion-mlx #635 — `--rate-limit 0` 不禁用模块级 60rpm 限流器, 多 agent 共享 api_key 撞 1 桶, 已提 issue (本仓不可修)
- [x] 911 tests, 0 ruff errors

### v0.8.8 ✅ — 企业级审计 P0 整改 (2026-08-26, AR #24)

> 审计源: `audit/fusion-multi-node-audit-result-0826.md` (29 项, P0×8)。本批落地 P0-1~P0-8 (P0 全清)。
- [x] **P0-1 HA leader 心跳 + term/voted_for 持久化** — 修多 Master term churn 持续重选; `election_state.json` 原子落盘, 重启恢复投票状态
- [x] **P0-2 派发失败重试** — `_dispatch_to_node` HTTP 非-200/status!=ok raise → `report_fault("dispatch_failed")` + 重试; 重试超限 FAILED (非云端回退)
- [x] **P0-3 agent 内部错误进熔断器** — 200+ok 但 result.error (OOM/坏模型) → `report_fault("agent_internal_error")` + 节点 FAULT + 任务 FAILED (非重试)
- [x] **P0-4 默认安全姿态** — E5 路径穿越 gate 无沙箱时也强制 (plugin/action/model_name 段校验); README 披露默认安全边界 + 最小加固步骤 + Preview 定位
- [x] **P0-5 SSRF 校验统一** — H1 register 拒云元数据/链路本地 IP; H2 cancel 通知走 build_safe_url; H3 KV 跨节点出站守卫 (3 处); 新增 `is_registerable_host`/`is_safe_outbound_host` 两语义分离
- [x] **P0-6 深度健康检查** — `/api/health` liveness (磁盘/内存/task-store, HTTP 200 body status) + `/api/health/deep` readiness (master +节点 quorum / agent +fusion-mlx `/v1/models`); compose healthcheck 验 body status; 两端点豁免 Bearer
- [x] **P0-7 声明对齐** — README 修 MCP/Observability/FMP/KV-张量/PIPELINE 死代码标注; `__init__.py` 分离 MasterElection(已接) vs StandbyMaster(死); cluster_sync docstring 修过时; CLAUDE.md 单锁 → 三锁
- [x] **P0-8 Observability 接线** — `ClusterObservability` 接 `ClusterMaster.start/stop` 生命周期 + `_health_check_loop` 周期采集节点指标/告警规则 (按 node_id+title 去重, 防 deque 灌满); cli 注入带配置 retention 的实例; `/api/v1/observability/{logs/export,suggestions,alerts}` 不再 503
- [x] 994 tests, 0 ruff errors

### v0.8.8 ✅ — 企业级审计 P1 整改 (2026-08-26, AR #24)

> 审计源: `audit/fusion-multi-node-audit-result-0826.md` (29 项, P1×9)。本批逐项落地 P1-9~P1-18。
- [x] **P1-9 KV 缓存持久化** (C12) — `KVSharingManager` 加磁盘 `save()`/`load()` (原子 tmp+replace, 跳过期条目); `AgentServer.start` 恢复本地 KV 缓存, `stop` 落盘 → agent 重启可恢复/预热 (审计 §6.3, 原纯内存 OrderedDict 重启即失)
- [x] **P1-10 async 阻塞消除** (C13/§4.1/§4.5) — async handler/路径内同步阻塞 (psutil 100ms、system_profiler 至 10s、sysctl、airport、ifconfig) 全部 `asyncio.to_thread` 移出事件循环: master_server `get_node_load`、node_agent `report_hardware`、cluster_master `_start_mdns`、network_topology `detect()` 全链 (5 处 subprocess + `_get_interface_type` 转 async); 新增 3 跨线程断言测试 (调用线程 ≠ 事件循环线程)
- [x] **P1-11 fsync 移出锁** (C14/§4.2) — `_persist_tasks_locked` 拆为锁内快照 + 锁外 `_write_task_store` (含 `os.fsync` 阻塞 I/O); 7 处状态写点 (assign/complete_dispatch/cancel/receive_synced_tasks/_persist_tasks) 改锁内快照→释放锁→落盘; 新增断言测试 (落盘时 `_tasks_lock.locked()` 为 False)
- [x] **P1-12 find_kv_cache 锁序修正** (C15/§2.4/§4.4) — `find_kv_cache` 原持 `_kv_lock` 内 `await _is_node_online` (跨域取 `_nodes_lock`) = kv→nodes 嵌套持锁, 违反 nodes→kv 约定, 死锁风险; 改为先 `_nodes_lock` 下快照在线节点集合→释放→再 `_kv_lock` 下匹配, 两锁域不嵌套持有; 新增断言测试 (`_kv_lock` 持有区不得获取 `_nodes_lock`)
- [x] **P1-13 单任务 HTTP 超时** (C16/§5.4) — `_dispatch_to_node` HTTP 超时原固定客户端默认 300s, >300s 任务被提前掐断 → FAILED 无重试; 改为单请求 `timeout=task.timeout_seconds+30` 缓冲 (下限 30s), 让任务级超时 (`_check_task_timeouts`→TIMEOUT+重试) 先于 HTTP 死代理兜底; 新增 2 测试 (600s→630s, 1s→下限 31s)
- [x] **P1-14 派发去重 token** (C17/§5.3) — `/api/execute` payload 原硬编码 `task_id=""` → agent 无法识别重复派发 (master 重派同 task_id 到同节点双重推理); master `_dispatch_to_node` 传真实 task_id (pipeline 各段 `{task_id}-step{N}`), `ExecuteRequest` 加 task_id 字段透传, `NodeAgent.execute_task` 拒同 task_id 已在运行 (返回 dedup_blocked → master 归类逻辑错误不重试); 无 task_id 直接调用分配匿名 id 防 `_running_task_handles` 撞键; 新增 2 测试 (拒重复 / 匿名序号递增)
- [x] **P1-15 H3 持久化失败可见** (C18/§5.6) — `_write_task_store` 落盘失败原仅 `logger.error` 静默吞 (任务落盘是崩溃恢复根基, 失败则 Master 崩溃后 RUNNING 任务全失); 改为接 P0-8 Observability 发 `critical` 告警 (含磁盘/权限指引) + `task_persist_failed` 指标; 新增测试 (失败→critical 告警 + 指标 1.0)
- [x] **P1-16 日志轮转** (§6.4) — `setup_logger` 设环境变量 `FUSION_MULTINODE_LOG_FILE` 时追加 `RotatingFileHandler` (10MB×5 有界); `start.sh` nohup stdout → `/dev/null` (应用日志走文件 handler 落盘轮转, 避免与 nohup stdout.log 重复无界增长), stderr 仍落盘捕获崩溃栈; launchd plist `StandardOutPath`→`/dev/null` + 传 `FUSION_MULTINODE_LOG_FILE` env; `docker-compose.yml` 两服务加 `logging: json-file max-size 10m max-file 3`; 新增 4 测试 (env 触发 / 无 env 单 handler / 写入+上限 / 坏路径回退控制台)
- [x] **P1-17 协议版本兼容校验** (§6.7) — `NodeRegisterRequest` 加 `protocol_version` 字段 (多节点协议版本, 非 mlx_version); `NodeAgent` 注册上报 `__version__`; `master_server` `_check_protocol_compat` 比对 agent 版本 ≥ `MIN_COMPAT_PROTOCOL_VERSION` (0.8.0), 低于则拒 400 + 降级指引 (升级至 ≥ min); 空串/非标准格式放行 + warn (灰度期向后兼容, 不误拒); 新增 4 测试 (拒不兼容 / 放行兼容 / 放行旧客户端空串 / 放行非标准格式)
- [x] **P1-18 失败推送通道** (§5.5) — `ClusterMaster` 加任务状态事件总线 (`_event_subscribers` asyncio.Queue 列表, `subscribe_task_events`/`unsubscribe_task_events`/`_emit_task_event` 非阻塞广播, 满队列丢最旧); `_finalize_task`(completed/failed)/`_enqueue_retry`(retry/failed)/`assign_task`(running)/`cancel_task`(cancelled) 状态转换点全接 emit (锁内纯内存, 不阻塞调度); 新增 `GET /api/tasks/events` SSE 端点 (text/event-stream, ready 首帧 + 15s keepalive, BearerAuthMiddleware 鉴权, 路由注册先于 `/api/tasks/{task_id}` 避免 path-param 捕获); 新增 8 测试 (FAILED/COMPLETED/retry 耗尽/cancel emit / 满队列丢最旧 / unsubscribe 停推 / SSE 路由契约 / 401 鉴权)
- [x] 1029 tests, 0 ruff errors

### v0.8.8 ✅ — 企业级审计 P2 整改 (2026-08-26, AR #24)

> 审计源: `audit/fusion-multi-node-audit-result-0826.md` (29 项, P2×8)。本批逐项落地 P2-19~P2-26。

- [x] **P2-22 Master 限流** (§3.8) — `MasterServer` 加 `RateLimitMiddleware` (复用 agent_server `InMemoryRateLimiter`, 120 req/60s/IP, 阈值高于 agent 因集群内部 heartbeat 10s×N + 派发流量); 健康检查/文档豁免; 防 DoS + 审批队列 (`max_pending=100`) 耗尽。新增 2 测试 (429 突发 / health 豁免)
- [x] **P2-26 重试计数持久化** (§5.7) — `_retry_count` 为动态属性, `asdict` 不序列化 → Master 崩溃重启归零 → 允许额外重试超 `_max_retry_attempts`。`_task_to_dict` 显式序列化 `_retry_count`, `_task_from_dict` 恢复; 持久化闭环测试 (落盘含字段 + 新 Master 恢复保留预算不归零)
- [x] **P2-25 过时文档清理** (§1.8/§2.4) — 三处过时声明已校正: `cluster_sync.py:5` docstring (自述"未接入" → 实已接 master_server 生命周期)、CLAUDE.md 单锁描述 (→ "拆三锁 nodes→tasks→kv")、`__init__.py` HA 描述 (MasterElection 已接线 / StandbyMaster 死代码边界澄清); 核实 autoscaler "未接线死代码 (恒 404)" 声明仍属实
- [x] **P2-23 compose 默认凭据去除** (§6.10) — `docker-compose.yml` 去除 `FUSION_CLUSTER_TOKEN:-dev-cluster-token-change-me` 与 `FUSION_MLX_API_KEY:-dahai168` 弱默认, 改用 `${VAR:?提示}` 未设则 compose 启动失败并提示; 新增 `.env.example` 模板 (含强随机值生成指引); `.gitignore` 加 `.env` 防真实凭据入库
- [x] **P2-24 PII 脱敏作用域文档化** (§3.7) — 核实 `data_scrubber`/`FMPCrypto`/`SecureTransferPipeline` 仅 FMP 路径实例化 (`fmp_server.py:230` DATA_SYNC), 默认 HTTP 派发路径明文无脱敏无加密; README 安全边界表 + Capabilities 已标 "FMP path only"; CLAUDE.md security 模块加作用域注 (审计允许 "或明确仅 FMP 路径保护"); 同步修正 Master 限流行 (P2-22 后已非"无限流")
- [x] **P2-19 部署方案文档** (§6.5) — 新增 `docs/DEPLOYMENT.md` 明确 local-first Apple Silicon 小集群定位: 四模式 (单机 nohup / 单机 launchd / docker-compose 多机 / 多 Master HA 技术预览) + 扩容资源 + "非目标-为何无 K8s" (平台绑定 MLX/Metal / 离线约束 / 规模错配 / 运维成本, 企业编排属 fusion-gateway 职责); README 链接; 顺带校正 `docs/HA-CRASH-RECOVERY.md` 过时声明 (MasterElection 已接线非原型)
- [x] **P2-20 配置热加载** (§6.8) — 新增 `POST /api/v1/config/reload` 端点 (Bearer 鉴权): 重读 `config.json` + 重应用运行时可调字段 (`scheduling.tenant_max_concurrent` → `configure_scheduling`); 须重启字段 (端口/ha_config/mdns) 响应中 `restart_required` 列出提示; `MasterServer(config=)` + `ClusterMaster.start(config=)` 注入 `ClusterConfig`, CLI 传 `_config`; 未注入返 503; 新增 3 测试 (热加载重应用配额 / 改盘后再 reload 生效 / 未注入 503 / 无鉴权 401)
- [x] **P2-21 运维 runbook** (§6.9) — 新增 `docs/OPERATIONS.md` 覆盖 10 类处置流程 (诊断入口 / 节点下线 / Master 下线 / 脑裂 / 磁盘满 / fusion-mlx 不可达 / 任务积压 / 版本升级 / 备份恢复 / Token 轮换), 每节含 症状/诊断/处置/恢复验证; 命令含 health/metrics/alerts 端点 + `~/.fusion/multi-node/` 持久化路径 + 端口 (11452/11458) + H3 恢复/熔断/优先级队列交叉引用; README 链接

### P3 — 长期 (审计 §5.9 / 功能完整度)

- [x] **P3-27 PIPELINE 端到端** — 上游 fusion-mlx `/distributed/*` 已交付 (issue #621/#630 closed: load_shard/pipeline_step/decode/sync_weights); 多节点客户端存根 `node_agent.load_shard`/`pipeline_step` + `_execute_pipeline_step` 已接; 真 E2E `tests/test_pipeline_e2e.py` (Llama-3.2-1B 16 层切 [0,8]/[8,16] b64.npy 张量 round-trip) 验证通过
- [x] **P3-28 张量级 KV 跨节点传输** (GAP-7, #33) — v0.11.0 交付: `sync_kv_cache` 经可插拔张量后端 (合成默认 / MLX 真张量 env-gated `FUSION_KV_TENSOR_BACKEND=mlx`) 编排源 `/api/kv/export` → 目标 `/api/kv/import`, 返 True; `KVShard.tensor` base64 压缩随 JSON 跨节点; 合成后端满足 #33 验收 (张量 round-trip 跨 2 agent); 真张量待上游 fusion-mlx issue #650 落地激活 (404→降级合成 + warn)
- [x] 1203 tests, 0 ruff errors
- [x] **P3-29 部分成功语义** (§5.9) — DATA 并行部分节点成功部分失败不再整任务 FAILED: 新增 `TaskStatus.PARTIAL` 终态 (不重试, 保留 `result.outputs` 供客户端取部分结果); `_dispatch_data` 聚合三态 (全成功 COMPLETED / 部分成功 PARTIAL / 全失败 FAILED); `_finalize_task(partial=)` 分支 + 事件总线 emit `partial`; stats `partial_tasks` 计数 + Prometheus gauge `fusion_cluster_tasks_partial`; CLI 🟡 图标; `/api/tasks` 进度事件 `partial`; 崩溃恢复 PARTIAL 终态保持 (不重派); 集成测试 `test_data_parallel_partial_success` (agent-a 成功 + agent-b 失败 → PARTIAL 保留 output)
- [x] 1036 tests, 0 ruff errors

### Future
- [ ] Distributed MLX operator bridge (mlx.distributed API)
- [ ] Distributed MLX operator bridge (mlx.distributed API)
- [ ] Plugin ecosystem cluster registration
- [ ] Cluster monitoring dashboard (fusion-studio)
- [ ] Thunderbolt RDMA acceleration
- [ ] Cross-node KV cache with Caveman compression

---

## 🔒 Security

### ⚠️ Default Deployment Security Boundary

Current version is positioned as a **technical preview (Preview)**, suitable for single-machine
development and trusted-LAN experimentation. It is **not** a production-grade commercial cluster
release. The default deployment posture has these known limits — hardened alternatives are listed:

| Area | Default posture | Hardening step |
|------|-----------------|----------------|
| **Node identity** | Single shared Bearer token (`~/.fusion/multi-node/.cluster_token`) is the only node identity; one leak = whole cluster compromised | Provision per-node certs + enable mTLS (below) |
| **mTLS** | **Off by default** — intra-cluster HTTP is plaintext, zero node-identity verification at transport | `FUSION_MTLS_ENABLED=1` + `provision_cluster`/`provision_node` (see `security/mtls.py`) |
| **Worker sandbox** | **`None` by default** — no OS-level resource isolation for inference/plugin tasks. Untrusted-input path traversal **is** still enforced at the task gate (E5, always-on); model_sync network & model_path whitelisting are only active when a sandbox is configured | Construct `WorkerSandbox(SandboxConfig(...))` and pass to `NodeAgent(sandbox=...)` |
| **Master rate limit** | 120 req/60s/IP global throttle (v0.8.8 P2-22) — guards register/join/vote/submit against burst DoS + approval-queue exhaustion; health/metrics/SSE exempt. For hostile-LAN exposure add a reverse proxy with finer policy | Deploy behind a rate-limiting reverse proxy for finer per-route policy on untrusted-LAN exposure |
| **PII scrubbing / AES-GCM** | Wired only on the FMP protocol path (not the default HTTP dispatch path) | Use `--transport fmp` to get encrypted, scrubbed transport |
| **Availability** | Single Master — Master crash = whole-cluster stall. Multi-Master HA election exists (P4) but is **technical preview**, not production-validated | Run on supervised host (launchd KeepAlive) for crash restart; do not rely on HA for production SLA |

**Minimum hardening for any multi-machine deployment** (trusted LAN):
1. Enable mTLS: `provision_cluster` once, `provision_node` per node, set `FUSION_MTLS_ENABLED=1` + cert paths on every node.
2. Restrict `FUSION_AUTO_APPROVE_PATTERNS` to your exact subnet CIDR (not broad patterns).
3. Run Master under `./start.sh install-launchd` (KeepAlive crash restart).
4. Do not expose Master/Agent ports to public networks.

### Capabilities

- **100% local offline** — Zero external network dependencies
- **Node approval** — New nodes require approval or pattern-based auto-approval
- **Master/Worker isolation** — Role-based permission, API path access control
- **mTLS node auth** — Private CA + per-node leaf cert, env-gated mutual TLS — **off by default, opt-in** (#80)
- **Multi-tenant quota + priority queue** — Per-tenant concurrent cap, over-quota enqueue, priority-ordered dispatch (#81)
- **Real-network E2E** — True port bind + real HTTP cross-process; node drop/reconnect; docker-compose cross-container (#76)
- **KV cache stress** — N-node cross-HTTP KV warm/transfer at scale, 0-loss migration, p99 latency baseline (#79)
- **Cross-node KV lookup** — `lookup_remote` contract-aligned (route→found/entry→decode); real-chain E2E lock (v0.8.5)
- **Auto node approval** — Trusted-subnet auto-join via `FUSION_AUTO_APPROVE_PATTERNS` env; CIDR-precise (`172.16.0.0/12`), substring/wildcard fallback (v0.8.4→v0.8.5)
- **Worker sandbox** — CPU/memory/disk limits, path & network whitelisting — **opt-in (`NodeAgent(sandbox=...)`), not default**; E5 untrusted-input traversal guard is always-on regardless of sandbox
- **Data scrubbing** — Auto-detect and redact PII (phone, email, API keys, ID cards) — **FMP path only**
- **AES-GCM encryption** — FMP protocol encrypted communication — **FMP path only**
- **Circuit breaker** — Automatic fault isolation for failing nodes (dispatch failure + agent-internal error both visible)
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

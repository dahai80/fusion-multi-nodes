<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>分布式 Apple Silicon MLX 推理的集群调度核心</strong></p>
  <p><em>将多台 Mac 组成统一 AI 集群 — 流水线并行、数据并行、100% 本地优先。</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.16.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License">
  <img src="https://img.shields.io/badge/tests-1433%20passed-brightgreen" alt="Tests">
</p>

> 本文件是 fusion-multi-node 的中文 README，镜像英文 `README.md`，版本 v0.16.0。

---

> **🚀 v0.16.0（2026-09-03）— 集群 drain + 幂等 + fencing + supervisor 协调 + 可选 identity（#69-#74）**
>
> 六个 issue 修复：
> - **#69 集群 drain + 健康门控** — `GET /api/nodes/{id}/drain-status` 返回 `{draining, in_flight, ready,
>   long_task_active}`；`ready` 是 supervisor 停服前等待的信号。CLI `cluster drain --wait [--timeout N]` 轮询至
>   ready。MVP refuse-long（配置 `drain.long_task_threshold_seconds`）。
> - **#70 提交时 exclude_nodes** — `TaskSubmitRequest.exclude_nodes` 排除指定节点不派发（回归测试 + OpenAPI 描述）。
> - **#71 X-Idempotency-Key** — 相同 header 的重复提交返回已存在 task_id，不新建任务（TTL
>   `scheduling.idempotency_ttl_seconds`，默认 86400）。两个提交路由均支持。
> - **#72 fencing token + 权威成员视图** — 单调递增 `MasterElection.fencing_token` 随派发 header 传播；NodeAgent 拒
>   绝更低的 token（`fencing_rejected` = 过期 master）。token 0（单 master / active-active）永不拒绝。
>   `/api/nodes` 带 `cluster_view` + `partitioned`，客户端据此对分区少数派 master 禁写。
> - **#73 supervisor 协调** — `SupervisorBridge` shell-out 调 `fusion-sv`（缺失则离线安全）；`supervisor_rpc` 任务
>   类型；`GET/POST /api/supervisor/{op}` agent 路由；master 转发至 `/api/nodes/{id}/supervisor/{op}`；CLI
>   `cluster supervisor` + `cluster rollout-node`。
> - **#74 可选 fusion-identity** — `IdentityProvider`（经 `FUSION_IDENTITY_URL` 显式 opt-in）：校验 JWT、取 per-tenant
>   配额、上报用量。**离线默认不变**（未设 env → `None`，用本地配置 + `fmu_` UserStore）。`fmu_` store 不退役；JWT
>   与 `fmu_` 共存。仅运维显式 opt-in 时 fail-closed。
>
> 1433 测试，ruff 通过。见 [CHANGELOG](docs/CHANGELOG.md)。

---

> **🚀 v0.15.0（2026-09-02）— Active-Active 双主 + 真实 GPU 负载 + pipeline 门控（#63 #64 #65）**
>
> 三个 issue 修复：
> - **#63 Active-Active 双主** — `ha.mode = "active-active"` 让两 master 同时活跃接受提交（无 standby 503）。
>   双向 peer-sync + owner-wins 收敛，无需 Redis。任务归属（`owner_master`）— 仅归属 master 派发，对端持镜像。
>   跨 master 任务 ID 唯一（`master-1-<uuid>`）。节点角色亲和 + drain（`POST /api/nodes/{id}/drain`，CLI
>   `cluster drain|undrain`）。
> - **#64 真实 GPU/Metal 负载** — `fetch_mlx_memory()` 抓取 fusion-mlx `GET /v1/health` 真实 Metal 显存（此前是
>   `pass` 空操作 → `gpu_memory_*_gb` 恒 0，`metal_util` VRAM_FIRST 权重失效）。agent 心跳现带 `metal_util` + gpu
>   字段；`GET /api/v1/nodes/{id}/metrics` 不再 `AttributeError`。
> - **#65 pipeline 404 门控** — `parallel.pipeline_enabled`（默认 `False`）早拒 `mode=pipeline` 并返回明确的
>   上游缺失 400，而非下游 404；`pipeline_shard_roles` 按角色硬过滤；`404 → upstream_missing → FAILED`（不可重试，
>   不触发熔断器）。
>
> 1356 测试，ruff 通过。见 [CHANGELOG](docs/CHANGELOG.md)。Redis/配额/LB 部署依赖跟踪于
> [fusion-gateway #159](https://github.com/dahai80/fusion-gateway/issues/159)。

---

> **🐛 v0.14.2（2026-09-02）— 容器化 agent 深度健康就绪修复（issue #60）**
>
> agent `/api/health/deep` 就绪探测（Docker healthcheck 使用）在容器化 agent 上永不上报 ok，导致容器永远
> `(unhealthy)`，尽管 agent 已在 master 注册在线。三处修复：(1) 探测现遵从 `FUSION_MLX_URL`（此前回退
> `localhost:11432` 网关端口）；(2) `/v1/models` 探测现带 `Authorization: Bearer` api_key 头（启用鉴权时此前
> 恒 `401`）；(3) 远程/宿主 MLX 不再被本地 socket 检查误判为下线。容器化 agent 现上报 `status: ok` 并转
> `(healthy)`。1347 测试，ruff 通过。见 [CHANGELOG](docs/CHANGELOG.md)。

> **📦 v0.14.2-rc.1（Release Candidate）— 2026-08-28**
>
> RC — v0.14.1 最终基线打包为候选版本。内容 = HEAD（企业级 7 阻断项 v0.14.0 + TarSlip 安全补丁 v0.14.1），
> 无新增代码变更。**非正式发布（GA）。** 1343 测试，ruff 通过，随机顺序双向全绿。见 [CHANGELOG](docs/CHANGELOG.md)。

> **🔒 v0.14.1（2026-08-28）— 安全补丁：备份恢复路径逃逸加固**
>
> `backup restore` TarSlip 变种修复 — 符号链接/硬链接 `linkname` 越界校验 +
> `extractall(filter="data")`（PEP 706）兜底。不假定备份可信（Rule 12）。1343 测试，ruff 通过。

> **📦 v0.14.0（2026-08-28）— 企业级生产就绪阻断项修复（7 项）**
>
> 全部 7 项企业级生产阻断项落地：(1) HA `cluster start` 接线缺口修复（该路径此前从未启动 HA）；
> (2) 可观测性持久化默认开启（`observability.persist=True` + `_cleanup_loop` 300s 周期落盘，重启不再丢失）；
> (3) 告警出站 webhook 配置段（env 优先，非零配置即不发告警）；(4) mTLS 配置段 + 惰性 `is_enabled()` +
> `configure_from_config` 配置→env 桥接（fail-closed 不变，默认仍关闭以兼容测试）；(5) 合成 KV 跨节点传输
> 宣告**生产就绪**（#33 关闭，真张量 env-gated 附加）；(6) CLI `backup create/restore` 一次性备份/恢复
>（完整 9 文件 + tls/ + kv/，原子 tar.gz 0600，路径逃逸校验）；(7) 规则纪元/confirm 持久化（重启 / HA failover
> 不再归零，纳入 `_build_state_sync_payload` 同步）。策略 = 配置段 + 部署层 env 透传 + 文档指引
>（除 `observability.persist` 外不翻默认值）。基线 1309 → 1343 测试全绿（随机顺序双向），ruff 通过。见
> [CHANGELOG](docs/CHANGELOG.md)。生产 mTLS/HA 须显式开启：见 `docs/DEPLOYMENT.md`。

---

> **📦 v0.12.1（2026-08-28）— 审计 0826 P2+P3 整改（15 项）**
>
> 审计 `fusion-multi-node-audit-result-product-0826.md` 裁定全部 12 项 P2 + 3 项 P3 已在代码层修复
>（含经 env-gate 拆开的设计权衡项，非纯文档）。安全/资源（3）：mTLS 证书 SAN + `check_hostname=True` /
> MLXKVTransport SSRF 守卫 / docker-compose 资源上限。KV 容量（2）：导出大小同步 / ban 到期主动探活。
> 事件/选举（3）：选举 I/O 移出锁 / 事件丢弃告警 / F2 动态子路径全操作。容器/隔离设计权衡拆开（4）：
> sandbox rlimit / PARTIAL 崩溃完成 / PIPELINE 分段级 checkpoint / 可观测性 deque 持久化（均 env-gated）。
> 部署/配置（3）：autoscaler 文案 503 / AgentServer KV 持久化 critical 告警 / MIGRATED 自动语义校准。
> 资源泄漏（1）：AgentServer.stop 调 kv_manager.close。基线 1262 → 1317 测试全绿。见
> [CHANGELOG](docs/CHANGELOG.md)。至此完成审计 0826 全部 47 项（5 P0 + 27 P1 + 12 P2 + 3 P3）。

---

> **📦 v0.12.0（2026-08-27）— 审计 0826 P1 整改（27 项）**
>
> 审计 `fusion-multi-node-audit-result-product-0826.md` 裁定全部 27 项 P1 已在代码层修复。
> 容错调度（8）：H3 RUNNING→PENDING 重派含 `exclude_nodes` / 节点 OFFLINE 自动迁移在途任务 /
> `_pending_queue` 上限 503 / 重试指数退避 / agent_server 429 不计入熔断器 / 增量持久化 /
> httpx 连接池显式配置 / `sync_kv_cache` 异常分类。KV 张量（2）：`import_tensor` 区分降级
> 与真失败 / 跨节点异常分类 + 连续失败告警。安全（7）：HTTP 派发可选 PII 脱敏 / cloud_fallback
> 导入期禁用守卫 / RBAC fail-closed + 全路由注册表 / 审计写失败告警 / `compare_digest` /
> manual_join mTLS scheme。API 契约（1）：9 个原始 dict → pydantic（422 非 400）。Agent（3）：
> `/api/hardware` to_thread / 选举空窗 503 / 本地 `max_tasks` 过载门控。性能/运维（6）：真推理
> 吞吐基准 / Prometheus 节点级指标 / HA 文档校准 / kv+user fsync / 日志文件 stderr 提示。
> 基线 1213 → 1262 测试全绿。见 [CHANGELOG](docs/CHANGELOG.md)。P2/P3 整改继续（v0.12.1）。

---

> **📦 v0.11.0（2026-08-27）— GAP-7 KV 张量跨节点传输（关闭 #33）**
>
> `ClusterMaster.sync_kv_cache` 经可插拔张量后端编排（合成默认 / MLX 真张量 env-gated
> `FUSION_KV_TENSOR_BACKEND=mlx`）源 agent `/api/kv/export` → 目标 `/api/kv/import`，
> 返回 `True`。`KVShard` 新增 `tensor` 字段（JSON 上 base64 压缩，`store_local` 预算门控）。
> 合成后端满足 #33 验收（2 agent 间张量 round-trip）；真张量待上游 fusion-mlx issue #650
> 激活（env-gated 附加，404→降级合成 + warn）。新增 `kv_tensor_transport.py` +
> 三组测试（`test_kv_tensor_serialize.py` 11，`test_kv_export_import_routes.py` 6，
> `test_kv_tensor_e2e.py` 4+1 skip）+ 重写 `test_new_features.py` 同步用例。
> 见 [CHANGELOG](docs/CHANGELOG.md)。
>
> 已完成（Phase A-F + GAP-7）：issues/PR → RC → GAP-1 always-on → GAP-6 限流适配 → GAP-5 死代码 → F1-F5 多租户 → GAP-7 KV 张量传输。

---

## 📋 概述

**Fusion-Multi-Node** 是 [Fusion-MLX](https://github.com/dahai80) 生态的集群调度核心。它将多台 Apple Silicon Mac（M4/M5 Studio/Max）组成分布式推理集群。

### 两种分布式模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **流水线并行（Pipeline Parallelism）** | 将大模型（70B+）切分到多台 Mac，每台处理一部分层 | 运行超大的本地模型 |
| **数据并行（Data Parallelism）** | 在多台 Mac 上加载同一模型，分发批量请求以提升吞吐 | 高吞吐批量推理 |

### 核心模块

| 模块 | 职责 |
|--------|---------------|
| **Cluster Master** | 节点发现、资源调度、任务生命周期、KV 缓存池、容错、任务自动降级、负载感知路由（#64 真实 GPU/Metal `metal_util`）、任务分片、FMP KV 同步、真张量 PIPELINE 分层链（接 fusion-mlx `/distributed/*`，✅上游端点已交付 issue #621/#630 关闭；**#65 pipeline 门控** `parallel.pipeline_enabled` 默认关，`pipeline_shard_roles` 角色过滤，404→upstream_missing→FAILED 不可重试），master→agent 派发循环、**H3 任务持久化 + 崩溃恢复**（RUNNING/PENDING 原子落盘，崩溃重启自动重派）。HA：**#63 Active-Active** `ha.mode="active-active"`（双主活跃，双向 peer-sync，owner-wins，无 Redis）+ standby 选举接到 `start(ha_config=)`（默认关单 Master）。**Drain**（`POST /api/nodes/{id}/drain`，CLI `cluster drain|undrain`）。cloud_fallback 调度路径 v0.8.2 已切断（100% 本地） |
| **Node Agent** | 每机守护进程、硬件上报、任务执行、mDNS 自动发现、pipeline_step（上游 `/distributed/load_shard`+`pipeline_step` 已交付 issue #621 关闭，b64.npy 跨节点激活，⚠️真模型端到端长期待定），**#64 真实 GPU/Metal 负载抓取**（`fetch_mlx_memory` → 心跳带 `metal_util` + gpu 字段） |
| **mDNS Discovery** | Bonjour/mDNS 零配置节点发现，手动 IP 加入兜底 |
| **FMP Protocol** | 三层二进制协议，AES-GCM 加密，TCP 长连接，熔断器，hop_count，FMP 入站服务端。⚠️启动但从未作为派发传输（仅 HTTP 派发） |
| **Distributed MLX Bridge** | 流水线/数据并行、模型分片、Caveman 压缩、KV 缓存共享。✅跨节点 KV 传输生产就绪（GAP-7/#33，v0.11.0）：`SyntheticKVTransport` 默认后端路由合成 KVCacheEntry 跨节点；真张量 `MLXKVTransport` env-gated 实验附加（`FUSION_KV_TENSOR_BACKEND=mlx`，待上游 #650） |
| **Security** | 节点审批、Master/Worker 权限隔离、Worker 沙箱、OS 级 sandbox-exec、数据脱敏、FMPCrypto（AES-256-GCM + ECDH）、Metal AES-GCM 加速 |
| **Observability** ✅已接 | 指标、日志、告警、日志存储与导出、智能故障诊断、优化建议。**P0-8 接入 `ClusterMaster.start/stop` 生命周期 + `_health_check_loop` 周期采集指标/告警（去重）；`/api/v1/observability/{logs/export,suggestions,alerts}` 现返 200；`/api/v1/metrics`（Prometheus）亦接。v0.14.0 持久化默认开启（`observability.persist=True`，`observability.jsonl` 落盘，`_cleanup_loop` 300s 周期落盘）** |
| **Storage Volumes** | 卷抽象、checkpoint 持久化、容量监控、LRU 淘汰。**ShardReplicator / DistributedKVStore / quorum 读写未接入生产路径，仅库级** |

### 架构

```
┌──────────────────────────────────────────────────────────────┐
│                    Claude Code / API / fusion-desk UI         │
│                           ↓                                  │
│              fusion-multi-node Cluster Master                 │
│  (Discovery, Scheduler, KV Pool, [Election·HA optional],      │
│   Degradation, Security, Observability)                      │
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

### 生态定位

```
┌─────────────────────────────────────────────────────────────┐
│                    应用层                                      │
│   fusion-desk  │  fusion-code  │  fusion-ui  │  Claude App   │
└──────────────────────────┬──────────────────────────────────┘
                           │ MCP / HTTP
┌──────────────────────────▼──────────────────────────────────┐
│                    控制层                                      │
│         fusion-multi-node (Cluster Master + Node Agent)        │
└──────────────────────────┬──────────────────────────────────┘
                           │ distributed API
┌──────────────────────────▼──────────────────────────────────┐
│                    推理层                                      │
│         fusion-mlx (MLX distributed, quantization, Metal)     │
│         Fusion-KB (vector search, RAG)                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 快速开始

### 安装

```bash
git clone https://github.com/dahai80/fusion-multi-node.git
cd fusion-multi-node

pip install -e .            # 核心安装
pip install -e ".[all]"     # 全部可选依赖
pip install -e ".[test]"    # 测试依赖
```

### 启动集群

```bash
# 启动 Cluster Master
fusion-multi-node cluster start --mode master

# 在每台 Mac 上启动 Node Agent
fusion-multi-node cluster start --mode agent

# 查看状态
fusion-multi-node cluster status
fusion-multi-node node list
```

### CLI 速查

```bash
fusion-multi-node cluster start/stop/status    # 集群管理
fusion-multi-node cluster pending/approve/reject # 节点审批
fusion-multi-node node list/info/discover      # 节点管理
fusion-multi-node task submit/list/cancel      # 任务管理
fusion-multi-node config list/get/set          # 配置
fusion-multi-node network detect               # 网络拓扑
fusion-multi-node caveman test                 # Caveman 压缩
fusion-multi-node kv stats/warm                # KV 缓存管理
```

---

## 🏗️ 模块架构

### 1. Cluster Master（`fusion_multi_node.master`）

集群单一事实来源 — 节点注册、健康检查、任务调度、KV 缓存、master 选举、cloud fallback、任务自动降级、真张量 PIPELINE 分层链。

#### 健康端点（C11 — readiness vs liveness）

- `GET /api/health` — **liveness**：本地依赖（磁盘剩余 >512MB / 内存 >256MB / task-store 可写），不查上游/节点 quorum。恒 HTTP 200，body `status: "ok"|"degraded"`。供 `start.sh` / docker livenessProbe — 进程存活即可，不阻塞启动。
- `GET /api/health/deep` — **readiness**：liveness + 节点 quorum（≥1 ONLINE 节点）。body `status: "ok"|"degraded"`，含 `online_nodes` 数。供 LB / 编排器摘流半残 master（主机健康但无可用节点 → 未就绪）。**不可用于服务间 depends_on**（会与 agent 启动死锁）。
- 两端点均免 Bearer 鉴权（k8s 探针不带 token）。

#### 流水线并行 — 真张量分层（接 fusion-mlx `/distributed/*`，#621）

PIPELINE 模式按 `model_shards` 将模型切分为段；每节点跑一段 layer-forward。首段携带 `input_ids`
（embed + layers）；后续段携带上一段输出 `hidden_states`（b64.npy，仅 layers）。激活张量
由调度器按序链接到末节点；末节点输出 = 最终 hidden_states。

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
# → 末节点返回 hidden_states（shape [1,4,2048] float16，b64.npy）
# lm_head / decode 超出上游 /distributed/* 首版范围 — 调度器仅做 layer-forward 链，不生成 token
```

> 真模型 E2E 已验证（Llama-3.2-1B-Instruct-4bit，16 层切分 [0,8]/[8,16]，
> 见 `tests/test_pipeline_e2e.py`）。需 fusion-mlx 运行 + 配置 `mlx.fusion_mlx_api_key`。

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
await master.register_node(node)  # 重复注册 = PATCH（保留运行时状态），返 bool（ban 期间为 False）

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

**核心能力**：负载感知路由（BALANCED/VRAM_FIRST/LOCALITY_FIRST/LOW_LATENCY，线程安全策略切换）、本地强制门控（≤0.5B 模型）、VRAM-first 调度（≥13B）、带能力过滤的评分选节点、任务生命周期（PENDING→RUNNING→COMPLETED/FAILED/TIMEOUT/MIGRATED）、递归取消、模型自动降级链、迁移、含 FMP 同步的 KV 缓存池、AST diff-only 传输、任务分片（inference/AST/vectorize，分片超时）、心跳超时、任务级熔断器（S1 dispatch-fault 自动 ban）。

#### 幂等节点注册 + 故障黑名单（F-A12 / F-A13，#20）

- **F-A12 幂等注册**：重复注册同一 `node_id` = PATCH 语义 — 保留 Master 权威运行时状态字段
  （`active_tasks`/`max_tasks`/`network_rtt_ms`/`status`），仅更新硬件声明字段
  （内存/CPU/GPU/hostname/port）。节点重启不丢运行时状态，不抹除在途派发计数。
- **F-A13 故障黑名单**：`report_fault` 在 `_FAULT_WINDOW_S`（60s）窗口内累计达
  `_FAULT_THRESHOLD`（3）→ 自动 ban `_BAN_DURATION_S`（300s）。ban 期间 `register_node`
  返 `False`（HTTP 403 拒绝）。`unregister_node(reason="banned")` 主动拉黑。
  到期惰性自动解 ban；`is_node_banned()` / `unban_node()` 供手动查询/解 ban。

```python
# 故障熔断器：连续 3 次上报 → ban 5 分钟，ban 期间拒绝重复注册
await master.report_fault("node_1", "oom", "out of memory")
assert not master.is_node_banned("node_1")
await master.report_fault("node_1", "oom", "again")  # 第 3 次上报触发 ban
assert master.is_node_banned("node_1")
assert await master.register_node(node) is False       # ban 期间被拒
master.unban_node("node_1")                            # 手动解 ban
```

#### 任务级熔断器（S1，#70）— 派发失败自动 ban

- **派发失败故障上报**：`_dispatch_to_node` 失败（SSRF 拒绝 / agent HTTP 非 200 / agent 返非 ok）
  → 自动调 `report_fault(node_id, "dispatch_failed")`，计入 F-A13 故障窗口。
- **调度跳过 banned 节点**：`select_nodes` 候选过滤跳过 ban 期内节点 — 原本仅 `register_node`
  拦截，调度路径漏拦，致故障节点被反复派发；S1 补齐调度侧缺口。
- 连续派发失败达 `_FAULT_THRESHOLD`（3）自动 ban；ban 期间不被选中；到期/解 ban 后恢复可选。

```python
# 3 次派发失败 → 节点自动 ban，select_nodes 不再选它
for i in range(master._FAULT_THRESHOLD):
    await master._dispatch_task(task_failing_on_node_1)
assert master.is_node_banned("node_1")
assert await master.select_nodes(ParallelMode.DATA, count=1) == []  # 全 ban → 空
```

#### 生产指标端点（S2，#71）— Prometheus exposition

- **`GET /api/v1/metrics`**：纯文本 Prometheus 0.0.4 exposition，无外部依赖，可被 Prometheus / Grafana agent 直接抓取。
- 集群级聚合指标：
  - 节点：`fusion_cluster_nodes_total` / `fusion_cluster_nodes_online`
  - 任务：`fusion_cluster_tasks_total` / `_running` / `_pending` / `_completed` / `_failed`
  - 重试：`fusion_cluster_task_retries_total`（counter）
  - KV：`fusion_cluster_kv_cache_entries`
  - 内存：`fusion_cluster_memory_total_gb` / `_available_gb`
  - 派发延迟：`fusion_cluster_dispatch_latency_seconds`（summary，p50/p90/p99 + sum/count）
- 复用 `get_stats` + 派发延迟（`completed_at - started_at`）+ `_retry_count`。Bearer 鉴权不豁免 — 内部抓取须带集群 token。

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:11452/api/v1/metrics
```

### Master Election（`fusion_multi_node.master.election`）— P4 接入 start()，默认关

> **当前状态：P4 + P0-1 已接入 `ClusterMaster.start(ha_config=...)`。** 当 `ha.enabled=True` 调
> `setup_election` 启动选举循环；默认 `enabled=False` 单 Master 向后兼容。Raft 简化版
> 优先级投票，`on_elected`/`on_deposed` 回调。Leader 心跳广播 + term/voted_for 持久化
>（`~/.fusion/multi-node/election_state.json`）已接（P0-1，修复 term 抖动重选）。
> **注意：** `StandbyMaster` 类（独立于 MasterElection）仍为死代码原型，非生产就绪。
>
> **H3 任务持久化（v0.8.2，已接）：** 即便单 Master 无完整 HA，RUNNING/PENDING 任务也原子落盘
>（`~/.fusion/multi-node/tasks.json`）；Master 崩溃后 `start()` 重启，`_restore_tasks` 自动恢复
>（RUNNING→PENDING 重派），无任务丢失。
>
> **H2 崩溃自愈（v0.8.2，已接）：** launchd 进程守护 — `./start.sh install-launchd` 渲染
> `deploy/com.dahai80.fusion-multi-node.plist`（KeepAlive 崩溃 10s 节流自动重启）→ launchctl load。
> 崩溃 → launchd 重启 → H3 任务恢复 = 自愈闭环，无任务丢失。见 `docs/HA-CRASH-RECOVERY.md`。
>
> **部署选项**：单机 nohup / 单机 launchd 守护 / docker-compose 多机小集群 / 多 Master HA（技术预览）。
> 本项目面向本地优先 Apple Silicon 小集群，**非 K8s 编排** — 见 `docs/DEPLOYMENT.md`。
> **运维 runbook**：故障处理（节点/Master 离线、脑裂、磁盘满、fusion-mlx 不可达、任务积压）+ 版本升级/备份恢复/token 轮换 — 见 `docs/OPERATIONS.md`。

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

### 2. Node Agent（`fusion_multi_node.agent`）

运行于每台 Mac — 硬件指标、心跳、经 fusion-mlx API 执行任务。

**健康端点（C11）**：`GET /api/health`（liveness — 磁盘/内存 + fusion-mlx 端口探活，无出站 HTTP）/ `GET /api/health/deep`（readiness — liveness + 真实 HTTP 探活 fusion-mlx `/v1/models`，判定 agent 是否真能推理）。两端点均免 Bearer 鉴权。

```python
from fusion_multi_node.agent import NodeAgent, AgentConfig

config = AgentConfig(node_id="my_mac", master_host="10.0.0.1")
agent = NodeAgent(config)
await agent.start()

info = agent.collect_hardware_info()
result = await agent.execute_task({"task_id": "t1", "type": "inference", "model": "qwen3.5-9b"})
```

### 3. mDNS Discovery（`fusion_multi_node.discovery`）

零配置 Bonjour/mDNS 节点发现，手动 IP 加入兜底。

```python
from fusion_multi_node.discovery import MDNSDiscovery
from fusion_multi_node.discovery.manual_join import ManualJoinClient, ManualJoinManager

# mDNS 自动发现
mdns = MDNSDiscovery(node_id="fusion-master")
mdns.register(port=11452, properties={"role": "master"})
master = await mdns.find_master_async(timeout=5.0)

# 手动 IP 加入（mDNS 兜底）
client = ManualJoinClient()
resp = await client.join(master_host="10.0.0.1", master_port=11452, node_id="node-1")

mgr = ManualJoinManager(cluster_secret="my-secret", auto_approve=True)
result = mgr.handle_join_request({"node_id": "node-1", "cluster_secret": "my-secret"})
```

### 4. FMP Protocol（`fusion_multi_node.protocol`）

三层二进制协议，AES-GCM 加密，熔断器，hop_count 广播上限。

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

**三层**：LinkLayer（路由，hop_count）、BusinessLayer（payload，rounds）、ControlLayer（心跳，ACK，流控）。**统一接口**：FMPInterface 封装连接管理、消息构造、加密、心跳。**Protobuf v2**：结构化 .proto 含 Envelope/Control/Payload 消息，自动回退 JSON/msgpack。

### 5. Security（`fusion_multi_node.security`）

节点审批、Master/Worker 权限隔离、Worker 沙箱、数据脱敏。

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

# 权限隔离
pm = PermissionManager()
pm.assign_role("master-1", NodeRole.MASTER)
pm.assign_role("worker-1", NodeRole.WORKER)
pm.has_permission("worker-1", Permission.TASK_EXECUTE)  # True
pm.has_permission("worker-1", Permission.TASK_SUBMIT)  # False
pm.check_path_access("worker-1", "/api/execute", "POST")  # True

# 节点审批
mgr = NodeApprovalManager(auto_approve_patterns=["192.168."])
req = mgr.request_join(node_id="n1", hostname="mac-1", ip_address="192.168.1.10", port=11445)
mgr.approve("n1", approved_by="admin")

# Worker 沙箱
sandbox = WorkerSandbox(
    config=SandboxConfig(
        allowed_paths=["/tmp", "/data"],
        allowed_network_hosts=["api.openai.com"],
    )
)
sandbox.check_path_access("/tmp/out", write=True)  # True
sandbox.check_network_access("api.openai.com")  # True
sandbox.filter_environment({"HOME": "/u", "SECRET": "x"})  # SECRET 被移除

# 数据脱敏（电话、邮箱、API key、身份证等）
scrubber = DataScrubber()
text, hits = scrubber.scrub_text("Call 13912345678, key=sk-abc123...")

# OS 级沙箱执行（macOS sandbox-exec / Linux unshare）
executor = SandboxExecutor()
result = await executor.execute_in_sandbox("task-1", ["python", "script.py"])

# Metal AES-GCM 加速（Apple Silicon 硬件）
metal = MetalCryptoBackend()
encrypted = metal.encrypt(key, plaintext)
decrypted = metal.decrypt(key, encrypted)

# 安全传输流水线（AST diff + PII 脱敏）
from fusion_multi_node.security.secure_transfer import SecureTransferPipeline

pipeline = SecureTransferPipeline()
transfer = pipeline.prepare_transfer(old_ast, new_ast)  # diff + 脱敏
restored = pipeline.apply_transfer(base_ast, transfer)  # 重建
```

#### mTLS 节点互信（#80）

集群内节点连接可选双向 TLS（mTLS），私有 CA + 每节点叶证书。env 开关 `FUSION_MTLS_ENABLED=1` 开启；关 = 全 HTTP 无操作（不破坏现有测试/CLI）。

```python
from fusion_multi_node.security import mtls

# 生成集群 CA（3650 天）+ 每节点叶证书（CN=node_id，O=role，365 天）
ca_cert, ca_key = mtls.provision_cluster("/path/to/ca")
node_cert, node_key = mtls.provision_node("worker-1", "worker", ca_cert, ca_key, "/path/to/worker-1")

# 服务端：uvicorn.Config(**server_ssl_kwargs()) — 要求对端带客户端证书（CERT_REQUIRED）
# 客户端：httpx.AsyncClient(**client_kwargs()) — verify=ctx 校验服务端证书 + 呈交客户端证书
# URL scheme：mtls.scheme() → "https" / "http"
```

细粒度权限（mTLS 开启时强制）：AgentServer 从 `X-Node-Id`/`X-Node-Role` 头读调用方身份 → `PermissionManager` 校验路径权限。
- MASTER：全部 API（含 execute + cancel）
- WORKER：execute / heartbeat / KV lookup-transfer / hardware；**不可** cancel
- 强制模式缺 `X-Node-Id` → 403；角色缺权限 → 403
- 兼容模式（mTLS 关）缺 header → 放行（现有 http 测试/CLI 不带 header）

#### 多租户配额 + 优先级队列（#81）

P1-H 多租户调度：全局默认每租户最大并发运行任务数；超配额任务进入优先级队列（非拒绝）；高优先级排队任务优先获空闲节点（非抢占式，不杀运行中任务）。

```python
from fusion_multi_node.master import ClusterMaster

master = ClusterMaster()
master.configure_scheduling(tenant_max_concurrent=4)  # 0 = 无限配额（节点容量仍受限）

# 超配额任务自动入队，assign_task 返 True（非拒绝）
await master.assign_task(task)
# 队列按优先级降序；节点上线 / 任务完成 / 取消占位任务 → 排空队首
```

- 配额全局默认：配置键 `scheduling.tenant_max_concurrent`（默认 4，0=无限），CLI 启动自动加载
- 超配额入队：租户运行任务达配额 → 新任务 `TaskStatus.PENDING` 入队，`assign_task` 返 True
- 无节点入队：`select_nodes` 找不到可用节点 → 入队（不再返 503），节点上线时排空
- 优先级：`ClusterTask.priority`（TaskPriority：LOW=0/NORMAL=1/HIGH=2/CRITICAL=3），队列降序排序
- 排空触发：`complete_task` / `register_node` / `cancel_task`（取消占位任务释放并发槽）
- HTTP：`POST /api/tasks/submit` 入队返 `202 {"queued": true}`（成功派发仍返 200）
- 取消：`cancel_task` 递归移除主/子任务出队；排队任务注册于 `master.tasks` 可查询/可取消

### 6. Observability（`fusion_multi_node.observability`）

指标、日志、告警、日志存储与导出、智能故障诊断。

```python
from fusion_multi_node.observability import ClusterObservability, LogEntry
from fusion_multi_node.observability.log_store import (
    LogStore,
    StoredLog,
    FaultDiagnoser,
)

# 指标与告警
obs = ClusterObservability(retention_hours=168.0)
obs.record_metric("node_1", "memory_used_gb", 16.0, tags={"gpu": "m4_ultra"})
obs.add_log(LogEntry(time.time(), "node_1", "INFO", "scheduler", "Task completed"))
logs = obs.export_logs(fmt="json")  # M8-02 日志导出
suggestions = obs.generate_optimization_suggestions()  # M8-03 智能建议

# 日志存储与导出
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

# 故障诊断（模式匹配 + 根因分析）
diagnoser = FaultDiagnoser()
results = diagnoser.diagnose(logs)
freq = diagnoser.analyze_frequency(logs, group_by="source")
```

### 7. Storage Volumes（`fusion_multi_node.storage`）

> **状态**：`StorageVolume`/`CheckpointManager`/`DistributedKVStore` 库级可用。
> `ShardReplicator` FMP 跨节点传输与 quorum 读写**未接入**生产路径
>（`set_fmp_interface` 无调用方）。quorum 读写有 E9 守卫：无 `storage_volume` 时
> 恒拒绝（`error=no_storage_volume`），不再回退内存自一致，避免误报多数持久化成功。
> 本节为库级 API 参考。

卷抽象、分片复制、checkpoint 持久化。

```python
from fusion_multi_node.storage import StorageVolume, VolumeSpec, VolumeType
from fusion_multi_node.storage import ShardReplicator, ReplicationConfig
from fusion_multi_node.storage import CheckpointManager, CheckpointEntry
from fusion_multi_node.storage import DistributedKVStore, KVEntry

# 卷管理
sv = StorageVolume(base_dir="/data/volumes")
sv.create_volume(VolumeSpec(name="models", volume_type=VolumeType.LOCAL))
sv.write_file("models", "config.json", b'{"model": "llama-70b"}')
data = sv.read_file("models", "config.json")

# 分片复制
replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
replicas = replicator.assign_replicas("shard-1", "/models/llama.bin", 1024, nodes)
healthy = replicator.get_healthy_replica("shard-1")

# checkpoint 持久化
cp = CheckpointManager(checkpoint_dir="/data/checkpoints")
cp.save(CheckpointEntry(checkpoint_id="cp-1", task_id="t1", node_id="n1", step=5, state_data={...}))
latest = cp.load_latest("t1")

# 分布式 KV Store，含 TTL、分区、快照/恢复
kv = DistributedKVStore(data_dir="/data/kv")
kv.put("config:model", {"name": "llama-70b"}, partition="config", ttl_seconds=3600)
val = kv.get("config:model")
kv.snapshot()  # M9-03：落盘
kv.restore("snapshot.json", merge=True)

# 分片复制 quorum 读写
qr = replicator.quorum_write("shard-1", data, storage_volume=sv)
qread = replicator.quorum_read("shard-1", storage_volume=sv)
```

---

## 🔧 配置

默认配置位于 `~/.fusion/multi-node/config.json`：

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

**端口迁移**：v0.6.5 旧端口（master 9753 / discovery 9754 / agent 9755 / mcp 9756 / fusion_mlx 8000）在配置加载时自动迁移到当前默认值，误设的 `master_host=0.0.0.0` 回退为 `127.0.0.1`；迁移结果写回 `config.json`。`ClusterConfig` 加载用深拷贝，故 `set()` 不污染类级 `DEFAULT_CONFIG`。

---

## 🧪 测试

```bash
pip install -e ".[test]"

# 运行全部测试（1343 测试）
pytest tests/ -v

# 含覆盖率
pytest tests/ --cov=fusion_multi_node --cov-report=html

# 运行特定模块
pytest tests/test_cluster_master.py -v
pytest tests/test_protocol.py -v
pytest tests/test_new_features.py -v
```

### 真模型 E2E（需 fusion-mlx 运行）

```bash
~/claude-home/fusion-mlx/start.sh start        # 启动推理引擎（端口 11434）

# DATA 并行 2 节点真推理（skip-gate：fusion-mlx 存活 + 模型在 /v1/models 列表）
pytest tests/test_data_parallelism_e2e.py -v

# 跨节点 KV 缓存共享（合成数据，无需模型，无 skip-gate）
pytest tests/test_kv_sharing_e2e.py -v

# 流水线并行分层真推理
pytest tests/test_pipeline_e2e.py -v

~/claude-home/fusion-mlx/start.sh stop         # 完成后关闭
```

> 默认模型 `mlx-community-Llama-3.2-1B-Instruct-4bit`，api_key 经配置 `mlx.fusion_mlx_api_key`。
> fusion-mlx 停止时 E2E 自动跳过（skip-gate），不阻塞 CI 全绿。

### 跨机真网络 E2E（#76）

真端口绑定 + 真跨进程 HTTP（非 ASGITransport）— 进程内起真 uvicorn 真端口服务端，经真 TCP socket 通信。

```bash
# 真端口跨进程：注册 / 派发 / 离线重连（无真模型，FakeBackend）
pytest tests/test_real_network_e2e.py -v

# 容器跨机：docker-compose 1 Master + 2 Agent（skip-gate docker 可用）
pytest tests/test_real_network_e2e.py::TestContainerE2E -v
```

- 真注册：agent 经真 HTTP `/api/nodes/register` 到 master（真 socket）
- 真派发：master → agent `/api/execute` 经 HTTP（FakeBackend 完成非真推理）
- 离线重连：停 agent → master 心跳超时标 OFFLINE → 重启同节点 → 重连恢复 ONLINE + 可派发
- 容器 E2E：`docker compose up --scale agent=2` 跨容器注册 + 派发；docker 不可用时跳过

### 跨机 KV 共享规模化压测（#79）

N 个真端口 agent 经 HTTP 验证大规模 KV 缓存迁移 — warm_cache 规模 + transfer 迁移 + 延迟 + 0 丢失（合成 KVCacheEntry，无真模型）。

```bash
# 4 个压测用例：warm 规模 / warm 延迟 / warm→transfer 迁移 / VRAM 累积
pytest tests/test_kv_stress.py -v
```

- warm 规模：M prompt × N 节点全成功（0 丢失）
- warm 延迟：单 warm p99 < 1.0s
- transfer 迁移：warm 到 node-0 → transfer 拉到 node-1，跨节点 0 丢失（推模型：源节点返回序列化条目 → 目标反序列化 + store_local）
- VRAM 累积：local_entries / total_size_bytes 作为 VRAM 用量代理

> KV transfer 推模型修复（v0.8.4）：原 `/api/kv/transfer` 路由回调 `transfer_from_remote` 致递归 + source_node 含冒号 sanitize 失败 — 改推模型（源节点查本地返回条目，目标反序列化 + store_local），加 `_serialize_entry` + `lookup_local_by_id`。

### 容器节点自动审批（v0.8.4）

`docker-compose` master 默认配 `FUSION_AUTO_APPROVE_PATTERNS`（可信子网子串匹配）— 容器/LAN 节点自动加入，免手动 `cluster approve`。

```bash
# compose 默认：192.168. / 10. / 172.16.0.0/12 子网自动审批
docker compose up -d --scale agent=2

# 裸机自定义可信子网（逗号分隔；CIDR 精确匹配优先，非 CIDR 回退子串/通配）
FUSION_AUTO_APPROVE_PATTERNS="10.0.1." ./start.sh start
```

> 生产应仅对可信子网开放自动审批；无此 env 回退手动审批门控（`fusion-multi-node cluster approve <node_id>`）。

### 端口冲突明确报错（v0.8.7）

issue #25 后续：NodeAgent 默认端口于 v0.8.0 从 11445 迁出 → 11458（解除与 fusion-comfyui 冲突，`_STALE_PORT_MAP` 自动迁移旧配置）。此处新增绑定失败的明确报错 — `AgentServer.start` / `MasterServer.start` 捕获 `OSError`，对已知冲突端口（comfyui 11445 / fusion-mlx 11432/11434 / master 11452 / mDNS 11450 / MCP 11446）追加提示"(conflicts with {service} default port)"，而非泛用绑定错误。测试：`test_start_port_conflict_raises_with_hint`（agent + master，mock uvicorn serve 抛 EADDRINUSE）。全量套件 946 通过 1 跳过。

### Phase 4 故障注入 E2E（v0.8.6）

调度器对真实故障的端到端自愈验证（真 ASGI 路由，非单测 mock；推理用合成 FakeBackend，不碰 fusion-mlx）：

1. **agent 崩溃 → 超时 → 重试 → 重派到存活节点** — agent-a 从路由移除（模拟崩溃，派发 404），任务超时 `check_timeouts` → `_enqueue_retry`（TIMEOUT→PENDING）→ 排空重试队列 `assign_task`（select_nodes 跳过 banned agent-a）→ 落到 agent-b → COMPLETED。全链加锁：超时入队 + 重派到存活 + 任务完成。

2. **重复派发失败 → ban → 新任务路由到存活节点** — agent-a 崩溃，连续派发 `_FAULT_THRESHOLD` 次全 404 → `report_fault` 窗口内达阈值自动 ban → 新任务 `select_nodes` 跳过 banned 节点 → 路由到 agent-b → COMPLETED。集成级验证（现有 `test_task_circuit_breaker` 为单测级）。

3. **HA leader 故障 → standby 升 leader → 派发恢复 + 同步任务可读** — m1（leader）经 `_persist_tasks` 持任务 → HTTP 推 m2（standby）`receive_synced_tasks` 落盘；m1 降级 + m2 升 leader（`_on_demoted_from_leader`/`_on_elected_leader` 翻 `_is_leader`）→ m2 `assign_task` 不再因 standby 守卫返 False → 同步任务无损接管。

测试：`tests/test_fault_injection.py`（3 场景，PortRoutingTransport + 真 AgentServer `/api/execute` + FakeBackend）。全量套件 943 通过 1 跳过。

### KV 跨节点 lookup 契约修复 + 审批 CIDR 精确匹配（v0.8.5）

严格审查暴露的两个缺陷修复：

1. **`lookup_remote` 恒返 None** — `/api/kv/lookup` 路由返扁平 dict（无 `found`/`entry` 键），故 `lookup_remote` 解 `data.get("found")` 恒 falsy → 跨节点 KV 复用查找静默失败。伪造 `{"found":True,"entry":{...}}` 形态的单测 mock 掩盖了此 bug（假信心测试）。修复：路由对齐契约返 `{"found":True,"entry":_serialize_entry}`，加真链 E2E 契约锁（`test_kv_lookup_remote_cross_node_contract` — 在 node-a 存储，node-b 经 HTTP 回查，非 mock）。

2. **自动审批 `"172."` 子串过度匹配公网** — compose 默认 `172.` 子串匹配公网 `172.0–15`/`172.32–255`（私有仅 `172.16.0.0/12`）。修复：CIDR 精确匹配优先（`ipaddress.ip_network` 包含测试），非 CIDR 回退子串/通配兼容旧配置；compose 默认改为 `172.16.0.0/12`。加回归测试（`test_auto_approve_cidr_precision` — `172.16.1.5` 放行 / `172.1.2.3` 拒绝）。

---

## 📊 关键常量

| 常量 | 默认值 | 用途 |
|----------|---------|---------|
| Master 端口 | 11452 | Cluster Master 服务端口 |
| Discovery 端口 | 11450 | mDNS 发现端口 |
| Agent 端口 | 11445 | Node Agent 端口 |
| MCP 端口 | 11446 | MCP 网关端口 |
| 心跳超时 | 15.0s | 陈旧节点阈值 |
| 任务超时 | 300.0s | 默认任务超时 |
| KV 缓存 TTL | 3600.0s | 默认 KV 缓存过期 |
| Token 预算 | 10,000,000 | MCP 网关 token 上限 |
| 降级链 | 70b→32b→13b→8b→3b→1b | 模型自动降级 |

---

## 📋 更新日志

### v0.7.0 ✅（当前）— 对抗性审查修复（AR 2026-08-24）

**P0 安全地基重构**
- [x] F1-F2 路径穿越守卫：cluster_sync 路径穿越拦截（NUL/绝对路径/驱动器/normpath + is_safe_path_segment）
- [x] F3-F4 SSRF 守卫：is_safe_peer_host 拒绝 loopback/link-local/metadata/multicast，build_safe_url 强制 scheme
- [x] F5 TLS 密钥持久化：私钥 NoEncryption + 文件 mode 0600
- [x] F6 TLS pinning：无 pin → fail-closed（raise），pin 指纹 CERT_REQUIRED+VERIFY_PEER+DER 回调
- [x] F7 FMP protobuf 二进制 payload base64，禁止 utf-8 replace 损坏
- [x] F8 fmp_server shard_id/file_path 路径校验
- [x] F9 mDNS sticky-master + node_id 绑 cluster_hash，防 Worker 伪造 master
- [x] F10 validate_node_id 拆为 is_safe_path_segment + is_safe_peer_host，所有 sink 加固

**P1 生产路径正确性 + 生命周期**
- [x] #8 assign_task TOCTOU 消除：锁内重检
- [x] #9 心跳/故障路由走加锁方法，未知节点 404（fail-visible）
- [x] #10 真任务取消：CANCELLED 状态，Master→Agent /api/tasks/cancel 中止运行中推理
- [x] #11 SIGTERM + 优雅关停排空：asyncio.Event + 信号处理 + 在途任务协程 gather
- [x] #12 config.save() 原子写：temp + os.fsync + os.replace
- [x] #13 task_id uuid4 替代 int(time.time())

**P1 HA 接线或砍 + 合规边界**
- [x] #14 砍虚假 HA 宣称：StandbyMaster/MasterElection/setup_election 标为未接线死代码，生产单 Master 无 HA
- [x] #15 合规边界：cloud_fallback **v0.8.2 调度路径切断**（ClusterMaster 不再触达 cloud API）；mcp_gateway/ast_diff/cluster_sync 功能债待迁移 fusion-gateway（#106）/ fusion-cowork（#61）；cluster_sync LAN-only is_safe_peer_host 加固

**P2 未接线原型门控 + 安全接线 + 无界增长**
- [x] #17 DataScrubber 增 openai_key/github_pat/slack_token/jwt_token + CJK 邻接数值边界修复；DataIsolation realpath+commonpath 防符号链接绕过；PermissionManager 默认阻断（已验 fail-closed）
- [x] #18 _metric_times list→deque(maxlen=10000) 对齐 metrics，修无界增长 + 索引错位
- [x] #24 WorkerSandbox 接入 NodeAgent 执行路径：`execute_task` 入口 `_sandbox_gate` 校验任务携带路径/网络（`check_path_access`/`check_network_access`），失败拒派发（缺陷 5：security/ 为零过滤死代码 → 进程内门控是真实防御）；`_execute_model_sync` 用 `is_safe_peer_host`+`build_safe_url`+`is_safe_path_segment`（与 master_server 一致，修弱 `.replace()`）；不调 `apply_limits`/`setrlimit`（进程级资源限制会误杀单长跑 agent），`SandboxExecutor` 仅子进程插件用
- [x] #23 M9/M10 集成测试门控 — 缺陷 4，四个契约 bug 修复 + 回归门控（审计允许：接线 OR pragma/移除；选修复，真正确性，可单测）：
  - caveman 字典压缩静默损坏：变长 2/4 字节码无分隔符 → 解压只读 2 字节，永不匹配。改为定长 2 字节码（`>H`，`dictionary_size` 截断 65536）+ 长度前缀记录（控制字节 0x01=字典命中/0x02=原始透传）
  - autoscaler 冷却门控绕过：`update_config` 清零 `_last_action_time` → 热重载绕过冷却，连续扩缩风暴。改为保留最近动作时间，冷却跨热重载连续
  - kv_store/fmp_server 签名不匹配：`_on_kv_get` 调 `get_entry(key, partition)`，原签名 1 参 → 入站 KV_GET 恒 TypeError。`get_entry` 增可选 `partition`，给定时校验分区匹配；`ttl` None→`or 0.0` 防 `is_expired` TypeError
  - shard_replication quorum 虚假宣称：`_sync_via_fmp` fire-and-forget（`ensure_future` 未 await）却返 `success=True`/`checksum_verified=True` → quorum 写保证伪造。诚实化：仅 await 的发送为 `success`，fire-and-forget 标 `success=False`+"unconfirmed" 日志，`checksum_verified` 恒 False（无应用层 ACK）
- [x] M9/M10/shard_replication 未接线原型标非生产（审计允许：接线 OR pragma/移除）；WorkerSandbox 已接（#24），M9/M10 契约 bug 已修（#23）

回归：826 测试通过，0 ruff 错误。

### v0.7.1 ✅ — 二轮架构审计修复（2026-08-24，22 项）

> 审计来源：`audit/fusion-multi-node-audit-report-0824.md`（363 行，H1-H5 / R1-R8 / E1-E9）。
> 流程：先提上游 issue → 代码落地（PR #18，分支 `release/v0.7.0-ar-audit-fixes`）。

**P0（H1/H4/E2/E7）— 假实现/死代码/诚实性**
- [x] H1 验证 fusion-mlx 无 `/distributed/*` 端点 → 提上游 issue #621；distributed_bridge Pipeline 标未实现 + 诚实报错（仓内）。注：上游 #621/#630 后续已交付，真 E2E `tests/test_pipeline_e2e.py` 验证通过
- [x] H4 四个死子系统（HA/autoscaler/cluster_sync/shard_replication）标未接线 + 移除外暴露路由
- [x] E2 kv_transfer `source_node` 用真实节点地址（非 `localhost`）
- [x] E7 kv_warm 目标节点取自在线节点表（非空集默认）

**P1（H2/H5/R1/R2/R8/E3/R6）— 并发/性能/正确性**
- [x] H2 按资源域拆分 ClusterMaster 单锁（nodes / tasks / kv）
- [x] H5 LoadRouter/KVSharing threading.Lock → asyncio（消除跨线程阻塞）
- [x] R1 硬件信息启动期缓存，心跳只取动态字段
- [x] R2 task_id uuid4 替代 `int(time.time())`
- [x] R8+E3 distributed_bridge `raise_for_status` + 响应 schema 校验 + 错误日志
- [x] R6 `get_online_nodes` 纯快照（无副作用）

**P2（H3/R3/R4/R5/R7/E1/E4/E5/E6/E8/E9）— 原型门控/安全/健壮性**
- [x] H3 HA 死代码（StandbyMaster/MasterElection）文档降级
- [x] R3 `sync_kv_cache` 经张量后端编排跨节点传输，返 True（P3-28 / GAP-7 / #33 于 v0.11.0 交付；合成默认 + MLX 真张量 env-gated 待上游 #650）
- [x] R4 `cancel_task` 改 `asyncio.gather` + 复用单 AsyncClient（消除串行通知）
- [x] R5 agent `_running_tasks` set + 五维负载上报
- [x] R7 模型大小正则边界匹配（防 `1b` 误匹配 `10b/100b`）
- [x] E1 `ClusterSyncManager` 移入 `__init__` + `start()`/`stop()` 生命周期（4 路由合并）
- [x] E4 配置字段级校验表 + `schema_version` + `set_many` 批量单持久化 + 载入自修复脏值
- [x] E5 plugin/action/model_name `is_safe_path_segment` 净化 + `_sandbox_gate` 覆盖全任务类型
- [x] E6 `model_config` 失败抛异常，不静默吞
- [x] E8 mDNS `_discovered` 跨线程 `threading.Lock`（修 dict-changed-size 竞态）
- [x] E9 无 `storage_volume` 的 quorum 读写恒拒绝（不再自一致假报多数持久化）

回归：849 测试通过，0 ruff 错误。

---

## 🛣️ 路线图

### v0.10.3 ✅ — GAP-8 Phase F1：多租户令牌地基（2026-08-27）
- [x] **per-user 令牌存储**（`security/user_store.py`）— `UserStore` 文件持久化 `users.json`（scrypt 哈希，0600，原子写），令牌格式 `fmu_<uid>_<secret>`，多活签发/吊销/轮换
- [x] **UserRole**（`security/permission.py`）— ADMIN/USER/VIEWER，与 NodeRole 正交 + `check_user_path_access` 路径鉴权
- [x] **双令牌中间件**（`utils/auth.py`）— `BearerAuthMiddleware` 按 `fmu_` 前缀路由到 UserStore，cluster_token 热路径 O(1) 不变；无 user_store 时回退纯 cluster_token（单租户零配置向后兼容）
- [x] **首启引导** — `FUSION_BOOTSTRAP_ADMIN` env 自动创建 ADMIN + 签发首令牌
- [x] 28 新测试（test_user_store 22 + TestUserTokenAuth 6）；1112 测试，0 ruff 错误

### v0.10.2 ✅ — GAP-5 死代码清理/标注（2026-08-26）
- [x] **autoscaler 路由显式标注未接线**（GAP-5）— `GET/PUT /api/v1/autoscaler/config` 从歧义的 `{"enabled":False}` 改为 503 + detail 说明未接线；模块保留待迁移
- [x] **StandbyMaster 死代码删除**（GAP-5）— 零实例化/零 import/零测试/零引用，独立于已接线的 MasterElection；HA 路径统一到 MasterElection
- [x] 2 autoscaler 未接线测试；1085 测试，0 ruff 错误

### v0.10.1 ✅ — GAP-6 限流适配（2026-08-26）
- [x] **客户端限流适配**（GAP-6）— `agent/rate_pacer.py` 拦截 fusion-mlx 429：读 `Retry-After`，指数退避重试（3 次，10s 预算，确定性无 jitter），耗尽抛 `RateLimitExhausted`
- [x] `FusionMLXBackend.chat`/`embed` 经 `dispatch_with_pacing` 包裹（不再直接 `raise_for_status` 误判 429 为逻辑错误）
- [x] Master 限流归类修复 — `rate_limited` → 瞬时失败（`transient_fail`，可重试），非 `logic_fail`，**不调 `report_fault`，不 ban 健康节点**
- [x] 上游 fusion-mlx #635 CLOSED（PR #637，`--rate-limit 0` 真正禁用限流，默认关）；显式上限 429 被退避吸收
- [x] 16 限流测试（14 单元 + 2 集成）；1083 测试，0 ruff 错误

### v0.10.0 ✅ — GAP-1 always-on SLA（2026-08-26）
- [x] **HA 全状态同步**（GAP-1）— leader 周期推送 nodes/kv_cache/banned_nodes 到 standby；standby 持有完整拓扑，failover 立即派发（always-on 间隙 ≤ 选举超时 ~10s）。HA 仍为 opt-in，2+ Master 显式配置才得 always-on，单 Master 部署不变
- [x] `/api/ha/sync-state` 端点 + `receive_synced_state` 幂等合并（锁序 nodes→kv 不嵌套）；`_state_sync_loop`（5s）接 `start()`/`stop()` 生命周期
- [x] 6 HA 状态同步测试（拓扑同步 / 幂等 / failover 立即派发 / 端点 round-trip / 单 Master 无目标 / 非法状态回退）
- [x] 1067 测试，0 ruff 错误

### v0.10.0-rc.1 🔶 — Release Candidate（2026-08-26）
- [x] #31 重试节点规避 — `exclude_nodes` 硬黑名单（select_nodes 过滤 + assign_task 透传 + 备选选择尊重，打破重试回坏节点循环）
- [x] GAP-4 CI 修复 — `pytest-randomly` 声明 + 3 Linux x86_64 不兼容测试 skip-gate
- [x] 再审计 §8 发布条件 2/4/5 披露完成 — GAP-1 HA SPOF / GAP-6 吞吐上限 / GAP-5 死代码 + GAP-7 KV no-op
- [x] 单租户 LAN 条件性商用就绪；多租户/远程 SaaS + always-on SLA 阻塞项声明
- [x] 1061 测试，0 ruff 错误，CI green

### v0.1.0 ✅
- [x] Cluster Master — 节点发现、调度器、任务生命周期、容错
- [x] Node Agent — 硬件上报、心跳、任务执行、mDNS 自动发现
- [x] mDNS Discovery — Bonjour 零配置服务注册与浏览
- [x] FMP Protocol — 三层二进制协议，AES-GCM 加密，熔断器
- [x] Distributed MLX — 模型分片、流水线/数据并行、Caveman 压缩、KV cache 共享
- [x] MCP Gateway — Claude 集成统一 MCP 端点
- [x] Observability — 指标、日志、告警、集群报告
- [x] CLI — 15+ 命令覆盖 cluster/node/task/config/network/caveman/kv 管理

### v0.3.0 ✅
- [x] 全审计整改（P0-P3），585 测试，0 ruff 错误

### v0.5.0 ✅
- [x] M1-02 device_model + UMA size 进 mDNS 发现 & NodeInfo
- [x] M1-03 心跳间隔 5s→3s
- [x] M1-02/03 mDNS heartbeat_interval/timeout 进广播属性，真实 device_model + uma_size_gb
- [x] M1-05 手动 IP 加入回退（mDNS 失败场景）
- [x] M2-04 hop_count 广播风暴防护
- [x] M2-01 结构化 .proto（Envelope/Control/Payload 消息）
- [x] M2-03 FMP 心跳发送（连接上 start_heartbeat/stop_heartbeat）
- [x] M2-05 FMPInterface 统一 API（connect, send_heartbeat, send_task_assign, broadcast）
- [x] M3-01 Master/Worker 权限隔离
- [x] M3-02 节点审批机制（集成进 /api/nodes/register）
- [x] M3-02 NodeInfo.role 字段（master/worker/standby）
- [x] M3-05 TaskSpec 分离（任务定义 vs 运行时状态）
- [x] M3-02 NodeStatus.FAULT 枚举值
- [x] M3-03 Master 选举（Raft 简化版带优先级）
- [x] M4-01 LoadMetrics + LoadRouter 结构化负载感知路由
- [x] M4-02 本地强派门控（≤0.5B 模型强制本地）
- [x] M4-03 VRAM 优先调度（≥13B 模型，线程安全策略切换）
- [x] M4-04 任务自动降级（70b→32b→13b→8b→3b→1b）
- [x] M4-05 云 API 回退（OpenAI/Anthropic，日成本上限）
- [x] M5-01/02/05 任务分片（inference/AST/vectorize，by_file/by_document/by_batch，结果合并）
- [x] M5-03 超时任务自动重试队列（_enqueue_retry，最多 1 次）
- [x] M5-03 TaskShard timeout 字段 + is_timed_out 属性
- [x] M5-04 任务全生命周期取消（递归子任务）
- [x] M6-01 Master 数据隔离强制
- [x] M6-01 Worker 临时目录清理（任务执行自动 mkdir/rmtree）
- [x] M6-02 Worker 沙箱（资源限制、路径/网络过滤、用量监控、子进程 env）
- [x] M6-03 节点审批集成进 register 端点
- [x] M6-04 AST 差量传输
- [x] M6-04 数据脱敏（电话、邮箱、API key、身份证等）
- [x] M6-04 FMPCrypto（AES-256-GCM + ECDH 协商会话密钥）
- [x] M7-06 监控 API v1（/api/v1/nodes/{id}/metrics, /api/v1/tasks/{id}/progress）
- [x] M7-06 /api/v1/cluster/stats + /api/v1/tasks/{id}/timeline 端点
- [x] M8-01 LogLevel 标准枚举（INFO/WARN/ERROR/FATAL）+ Master 全节点日志聚合（collect_node_logs）
- [x] M8 日志存储 & 导出（JSON/CSV/text）
- [x] M8 智能故障诊断（模式匹配 + 根因）
- [x] M9-02/03 存储数据传输 + 容量监控 + LRU 淘汰
- [x] M9-01 分布式 KV Store（TTL，分区，快照/恢复，持久化）
- [x] M9-02 分片副本 quorum 读写
- [x] M9-03 KV Store 快照/恢复
- [x] M9-04 FMP 协议 KV cache 同步
- [x] M9 存储卷（local/shared/distributed）
- [x] M9 分片副本带健康追踪
- [x] M9 Checkpoint 持久化
- [x] M9 模型分片分发
- [x] M10-02/03 Autoscaler 内建缩放动作（standby 激活 + migrate-then-deactivate）
- [x] M10 Autoscaler（conservative/balanced/aggressive 策略）
- [x] M10 缩容时任务迁移
- [x] protobuf>=5.0.0 依赖
- [x] P0：FMPServer 入站 TCP server（跨节点 shard/KV 传输）
- [x] P0：Protobuf 结构化编码（envelope/control/payload 字段）
- [x] P0：Autoscaler 热重载（update_config/update_policy）
- [x] P0：跨节点 FMP 传输（ShardReplicator + DistributedKVStore remote ops）
- [x] P1：日志保留 7 天（168h 默认）+ 日志导出 API
- [x] P1：智能优化建议（告警驱动 + 错误模式分析）
- [x] P1：SandboxExecutor（macOS sandbox-exec / Linux unshare / python-resource 回退）
- [x] P2：Metal AES-GCM 加速（Apple Silicon CommonCrypto 桥 + 自动回退）
- [x] P2：CLI --transport fmp 接线（FMPServer + FMPConnectionManager）
- [x] 805 测试，0 ruff 错误

### v0.8.2 ✅ — 生产就绪硬阻塞 + 软债（2026-08-25）
- [x] H3 Master 任务持久化 + 崩溃启动恢复（原子持久化，RUNNING→PENDING 重派）
- [x] H2 launchd 进程守护 — 崩溃自愈循环（KeepAlive 重启 + H3 恢复）
- [x] H4 cloud_fallback 调度路径切断（100% 本地合规）；功能债待迁移 fusion-gateway/fusion-cowork
- [x] H1 PIPELINE token 输出 — 上游 fusion-mlx #630 decode 端点交付（closed）；真 E2E `tests/test_pipeline_e2e.py` 验证通过
- [x] S1 任务级熔断器 — 派发失败报故障 + select_nodes 跳过 banned 节点
- [x] S2 生产指标端点 /api/v1/metrics（Prometheus exposition）
- [x] S3 负载/压测基线测试（调度层吞吐 / 尾延迟 / 零丢失）
- [x] S4 真模型集成测试覆盖（DATA 并行 E2E 真推理 + KV sharing E2E 真 ASGI 路由链；另 3 生产 bug 修复：FusionMLXBackend `/v1/*` 缺鉴权 / KVSharingManager 跨节点 HTTP 缺鉴权 / KVWarmRequest 契约不匹配）
- [x] 888 测试，0 ruff 错误

### v0.8.3 ✅ — 容器规模压测 + 调度 TOCTOU 修复（2026-08-25）
- [x] P0-A HA 双 Master 选举接 `start(ha_config=)` 默认开（选举 HTTP vote 层复用，无外部依赖）
- [x] P0-B 容器化 — `Dockerfile` + `docker-compose.yml`（1 Master + N Agent，`--scale agent=N` 无限扩容）；agent 经容器 bridge IP 回连，不占主机端口；推理引擎裸机 `host.docker.internal:11434` 回连
- [x] BUG#3 Agent 容器内本地 IP 探测 — 跨平台 socket UDP connect（零依赖，取 master 回连源 IP），替代 macOS-only `ipconfig`
- [x] BUG#4 NodeApprovalManager 审批路径丢弃硬件元数据 — register 透传元数据，approve 从元数据重建 NodeInfo（mem/max_tasks/cpu 不再回落默认 0/4）
- [x] **调度 TOCTOU 竞态修复** — `select_nodes` 原在锁外运行 → 偏好节点被并发抢占满载时 → 锁内备选选空闲节点（`_select_free_nodes_locked`），不再直接 503。c8 并发 40 任务 0× 503 验证
- [x] `FUSION_AGENT_MAX_TASKS` env — 每 agent 并发上限可调（压测时 16）
- [x] 容器压测客户端 `scripts/stress_live.py` — 经 master:11452 并发提交，测吞吐/尾延迟/成功率；`--rps` 客户端速率门控对齐上游 rate-limit 桶
- [x] 集群运维工具 `scripts/cluster_ops.py` — approve-all / status / unban-all
- [x] P1-E 可观测性栈模板 — Grafana dashboard / Prometheus / Alertmanager（deploy/observability/）
- [x] Phase-3 调度压测通过 — 4 节点 50 任务 success 1.0，c8 争抢 40 任务 success 1.0，0× 503
- [x] 上游阻塞 fusion-mlx #635 — `--rate-limit 0` 不禁用模块级 60rpm limiter，多 agent 共享一 api_key 撞一个桶，已提 issue（本仓不可修）
- [x] 911 测试，0 ruff 错误

### v0.8.8 ✅ — 企业审计 P0 整改（2026-08-26，AR #24）

> 审计来源：`audit/fusion-multi-node-audit-result-0826.md`（29 项，P0×8）。本批落地 P0-1~P0-8（P0 全清）。
- [x] **P0-1 HA leader 心跳 + term/voted_for 持久化** — 修多 Master term 抖动致持续重选；`election_state.json` 原子持久化，重启恢复投票状态
- [x] **P0-2 派发失败重试** — `_dispatch_to_node` HTTP 非 200/status!=ok 抛 → `report_fault("dispatch_failed")` + 重试；重试耗尽 → FAILED（非云回退）
- [x] **P0-3 agent 内部错误进熔断器** — 200+ok 但 result.error（OOM/坏模型）→ `report_fault("agent_internal_error")` + 节点 FAULT + 任务 FAILED（不重试）
- [x] **P0-4 默认安全姿态** — E5 路径穿越门控即便无沙箱也强制（plugin/action/model_name 段校验）；README 披露默认安全边界 + 最小加固步骤 + Preview 定位
- [x] **P0-5 SSRF 校验统一** — H1 register 拒云元数据/link-local IP；H2 cancel 通知走 build_safe_url；H3 KV 跨节点出站守卫（3 处）；增 `is_registerable_host`/`is_safe_outbound_host` 双语义分离
- [x] **P0-6 深度健康检查** — `/api/health` liveness（disk/mem/task-store，HTTP 200 body status）+ `/api/health/deep` readiness（master +node quorum / agent +fusion-mlx `/v1/models`）；compose healthcheck 校验 body status；两端点 Bearer 豁免
- [x] **P0-7 声明对齐** — README 修 MCP/Observability/FMP/KV-tensor/PIPELINE 死代码标注；`__init__.py` 分 MasterElection(已接线) vs StandbyMaster(死)；cluster_sync docstring 修过时；CLAUDE.md 单锁 → 三锁
- [x] **P0-8 可观测性接线** — `ClusterObservability` 接 `ClusterMaster.start/stop` 生命周期 + `_health_check_loop` 周期采集节点指标/告警规则（按 node_id+title 去重，防 deque 泛滥）；cli 注入带配置保留期的实例；`/api/v1/observability/{logs/export,suggestions,alerts}` 不再 503
- [x] 994 测试，0 ruff 错误

### v0.8.8 ✅ — 企业审计 P1 整改（2026-08-26，AR #24）

> 审计来源：`audit/fusion-multi-node-audit-result-0826.md`（29 项，P1×9）。本批逐项落地 P1-9~P1-18。
- [x] **P1-9 KV cache 持久化**（C12）— `KVSharingManager` 增磁盘 `save()`/`load()`（原子 tmp+replace，跳过期条目）；`AgentServer.start` 恢复本地 KV cache，`stop` 落盘 → agent 重启可恢复/预热（审计 §6.3，原纯内存 OrderedDict 重启即失）
- [x] **P1-10 async 阻塞消除**（C13/§4.1/§4.5）— async handler/path 内 sync 阻塞（psutil 100ms，system_profiler 至 10s，sysctl，airport，ifconfig）全经 `asyncio.to_thread` 移出事件循环：master_server `get_node_load`、node_agent `report_hardware`、cluster_master `_start_mdns`、network_topology `detect()` 全链（5 subprocess 点 + `_get_interface_type` 转 async）；增 3 跨线程断言测试（调用线程 ≠ 事件循环线程）
- [x] **P1-11 fsync 移出锁**（C14/§4.2）— `_persist_tasks_locked` 拆锁内快照 + 锁外 `_write_task_store`（含 `os.fsync` 阻塞 I/O）；7 状态写点（assign/complete_dispatch/cancel/receive_synced_tasks/_persist_tasks）改锁内快照→释放锁→持久化；增断言测试（持久化期间 `_tasks_lock.locked()` 为 False）
- [x] **P1-12 find_kv_cache 锁序修复**（C15/§2.4/§4.4）— `find_kv_cache` 原持 `_kv_lock` 内 `await _is_node_online`（跨域 `_nodes_lock`）= kv→nodes 嵌套持锁，违 nodes→kv 约定，死锁风险；改 `_nodes_lock` 下快照在线节点集→释放→`_kv_lock` 下匹配，两锁域不嵌套；增断言测试（`_kv_lock` 持有区不得取 `_nodes_lock`）
- [x] **P1-13 单任务 HTTP 超时**（C16/§5.4）— `_dispatch_to_node` HTTP 超时原固定客户端默认 300s，>300s 任务被提前掐断 → FAILED 不重试；改单请求 `timeout=task.timeout_seconds+30` 缓冲（下限 30s），让任务级超时（`_check_task_timeouts`→TIMEOUT+retry）先于 HTTP 死 agent 兜底触发；增 2 测试（600s→630s，1s→下限 31s）
- [x] **P1-14 派发去重 token**（C17/§5.3）— `/api/execute` payload 原硬编码 `task_id=""` → agent 无法检测重复派发（master 同 task_id 重派同节点 = 双重推理）；master `_dispatch_to_node` 带真实 task_id（pipeline 各段 `{task_id}-step{N}`），`ExecuteRequest` 增 task_id 字段透传，`NodeAgent.execute_task` 拒同 task_id 已运行（返 dedup_blocked → master 归类逻辑错误不重试）；无 task_id 直接调用分配匿名 id 防 `_running_task_handles` 撞键；增 2 测试（拒重复 / 匿名序列递增）
- [x] **P1-15 H3 持久化失败可见**（C18/§5.6）— `_write_task_store` 持久化失败原仅 `logger.error` 静默吞（任务持久化是崩溃恢复基石；失败 = Master 崩溃丢全部 RUNNING 任务）；改接 P0-8 可观测性发 `critical` 告警（带磁盘/权限指引）+ `task_persist_failed` 指标；增测试（失败→critical 告警 + 指标 1.0）
- [x] **P1-16 日志轮转**（§6.4）— `setup_logger` 设 env `FUSION_MULTINODE_LOG_FILE` 时追加 `RotatingFileHandler`（10MB×5 有界）；`start.sh` nohup stdout → `/dev/null`（应用日志经文件 handler 有界轮转，避免与 nohup stdout.log 重复无界增长），stderr 仍落盘捕获崩溃栈；launchd plist `StandardOutPath`→`/dev/null` + 传 `FUSION_MULTINODE_LOG_FILE` env；`docker-compose.yml` 两服务加 `logging: json-file max-size 10m max-file 3`；增 4 测试（env 触发 / 无 env 单 handler / 写+cap / 坏路径回退控制台）
- [x] **P1-17 协议版本兼容校验**（§6.7）— `NodeRegisterRequest` 增 `protocol_version` 字段（多节点协议版本，非 mlx_version）；`NodeAgent` 注册报 `__version__`；`master_server` `_check_protocol_compat` 比 agent 版本 ≥ `MIN_COMPAT_PROTOCOL_VERSION`（0.8.0），低于拒 400 + 降级指引（升级到 ≥ min）；空串/非标准格式放行 + warn（灰度期向后兼容，不误拒）；增 4 测试（拒不兼容 / 过兼容 / 过老客户端空串 / 过非标准格式）
- [x] **P1-18 失败推送通道**（§5.5）— `ClusterMaster` 增任务状态事件总线（`_event_subscribers` asyncio.Queue 列表，`subscribe_task_events`/`unsubscribe_task_events`/`_emit_task_event` 非阻塞广播，满队列丢最旧）；`_finalize_task`(completed/failed)/`_enqueue_retry`(retry/failed)/`assign_task`(running)/`cancel_task`(cancelled) 状态转换点全 emit（锁内纯内存，不阻塞调度）；增 `GET /api/tasks/events` SSE 端点（text/event-stream，ready 首帧 + 15s keepalive，BearerAuthMiddleware 鉴权，路由注册在 `/api/tasks/{task_id}` 之前防 path-param 捕获）；增 8 测试（FAILED/COMPLETED/重试耗尽/取消 emit / 满队列丢最旧 / unsubscribe 停推 / SSE 路由契约 / 401 鉴权）
- [x] 1029 测试，0 ruff 错误

### v0.8.8 ✅ — 企业审计 P2 整改（2026-08-26，AR #24）

> 审计来源：`audit/fusion-multi-node-audit-result-0826.md`（29 项，P2×8）。本批逐项落地 P2-19~P2-26。

- [x] **P2-22 Master 限流**（§3.8）— `MasterServer` 加 `RateLimitMiddleware`（复用 agent_server `InMemoryRateLimiter`，120 req/60s/IP，阈值高于 agent 因集群内心跳 10s×N + 派发流量）；健康检查/docs 豁免；防 DoS + 审批队列（`max_pending=100`）耗尽。增 2 测试（429 突发 / 健康豁免）
- [x] **P2-26 重试计数持久化**（§5.7）— `_retry_count` 是动态属性，`asdict` 不序列化 → Master 崩溃重启归零 → 允许超 `_max_retry_attempts` 的额外重试。`_task_to_dict` 显式序列化 `_retry_count`，`_task_from_dict` 恢复；持久化循环测试（持久化含字段 + 新 Master 恢复保留预算不归零）
- [x] **P2-25 过时文档清理**（§1.8/§2.4）— 三处过时声明更正：`cluster_sync.py:5` docstring（自述"未接线" → 实际接 master_server 生命周期），CLAUDE.md 单锁描述（→ "拆三锁 nodes→tasks→kv"），`__init__.py` HA 描述（MasterElection 已接线 / StandbyMaster 死代码边界厘清）；验 autoscaler"未接线死代码（恒 404）"声明仍成立
- [x] **P2-23 compose 默认凭据移除**（§6.10）— `docker-compose.yml` 删 `FUSION_CLUSTER_TOKEN:-dev-cluster-token-change-me` 和 `FUSION_MLX_API_KEY:-dahai168` 弱默认，改 `${VAR:?hint}` 未设时 compose 启动失败带提示；增 `.env.example` 模板（含强随机值生成指引）；`.gitignore` 加 `.env` 防真实凭据入库
- [x] **P2-24 PII 脱敏范围文档化**（§3.7）— 验 `data_scrubber`/`FMPCrypto`/`SecureTransferPipeline` 仅 FMP 路径实例化（`fmp_server.py:230` DATA_SYNC），默认 HTTP 派发路径明文无脱敏无加密；README 安全边界表 + Capabilities 已标"FMP 路径专用"；CLAUDE.md security 模块加范围注记（审计允许"或显式 FMP 路径专用保护"）；并更正 Master 限流行（P2-22 后不再"无限制"）
- [x] **P2-19 部署方案文档**（§6.5）— 增 `docs/DEPLOYMENT.md` 厘清本地优先 Apple Silicon 小集群定位：四模式（单机 nohup / 单机 launchd / docker-compose 多机 / 多 Master HA 技术预览）+ 扩容资源 + "非目标 — 为何不上 K8s"（平台绑定 MLX/Metal / 离线约束 / 规模错配 / 运维成本，企业编排是 fusion-gateway 的活）；README 链接；并更正 `docs/HA-CRASH-RECOVERY.md` 过时声明（MasterElection 已接线，非原型）
- [x] **P2-20 配置热重载**（§6.8）— 增 `POST /api/v1/config/reload` 端点（Bearer 鉴权）：重读 `config.json` + 重应用运行时可调字段（`scheduling.tenant_max_concurrent` → `configure_scheduling`）；需重启字段（port/ha_config/mdns）列入响应 `restart_required` 提示；`MasterServer(config=)` + `ClusterMaster.start(config=)` 注入 `ClusterConfig`，CLI 传 `_config`；未注入返 503；增 3 测试（热重载重应用配额 / 改盘再重载生效 / 未注入 503 / 无鉴权 401）
- [x] **P2-21 运维 runbook**（§6.9）— 增 `docs/OPERATIONS.md` 覆盖 10 处理流（诊断入口 / 节点离线 / Master 离线 / 脑裂 / 磁盘满 / fusion-mlx 不可达 / 任务积压 / 版本升级 / 备份恢复 / 令牌轮换），每节有症状/诊断/处理/恢复验证；命令含 health/metrics/alerts 端点 + `~/.fusion/multi-node/` 持久化路径 + 端口（11452/11458）+ H3 恢复/熔断器/优先队列交叉引用；README 链接

### P3 — 长期（审计 §5.9 / 功能完整性）

- [x] **P3-27 PIPELINE 端到端** — 上游 fusion-mlx `/distributed/*` 交付（issue #621/#630 closed：load_shard/pipeline_step/decode/sync_weights）；多节点客户端 stub `node_agent.load_shard`/`pipeline_step` + `_execute_pipeline_step` 接线；真 E2E `tests/test_pipeline_e2e.py`（Llama-3.2-1B 16 层 split [0,8]/[8,16] b64.npy tensor round-trip）验证通过
- [x] **P3-28 tensor 级 KV 跨节点传输**（GAP-7, #33）— v0.11.0 交付：`sync_kv_cache` 经可插拔 tensor 后端（合成默认 / MLX 真张量 env-gated `FUSION_KV_TENSOR_BACKEND=mlx`）编排源 `/api/kv/export` → 目标 `/api/kv/import`，返 True；`KVShard.tensor` base64 压缩随 JSON 跨节点；合成后端满足 #33 验收（2 agent 间 tensor round-trip）；真张量待上游 fusion-mlx issue #650 激活（404→降级合成 + warn）
- [x] 1203 测试，0 ruff 错误
- [x] **P3-29 部分成功语义**（§5.9）— DATA 并行部分节点成功部分失败不再整任务标 FAILED：增 `TaskStatus.PARTIAL` 终态（不重试，保留 `result.outputs` 供客户端取部分结果）；`_dispatch_data` 聚合三态（全成功 COMPLETED / 部分成功 PARTIAL / 全失败 FAILED）；`_finalize_task(partial=)` 分支 + 事件总线 emit `partial`；stats `partial_tasks` 计数 + Prometheus gauge `fusion_cluster_tasks_partial`；CLI 🟡 图标；`/api/tasks` progress 事件 `partial`；崩溃恢复 PARTIAL 终态保留（不重派）；集成测试 `test_data_parallel_partial_success`（agent-a 成功 + agent-b 失败 → PARTIAL 保留输出）
- [x] 1036 测试，0 ruff 错误

### Future
- [ ] 分布式 MLX operator 桥（mlx.distributed API）
- [ ] 分布式 MLX operator 桥（mlx.distributed API）
- [ ] 插件生态集群注册
- [ ] 集群监控 dashboard（fusion-studio）
- [ ] Thunderbolt RDMA 加速
- [ ] 跨节点 KV cache 带 Caveman 压缩

---

## 🔒 安全

### ⚠️ 默认部署安全边界

当前版本定位为**技术预览（Preview）**，适合单机开发与可信 LAN 实验。**非**生产级商用集群发布。默认部署姿态有以下已知限制 — 加固替代方案如下：

| 领域 | 默认姿态 | 加固步骤 |
|------|----------|----------|
| **节点身份** | 单一共享 Bearer token（`~/.fusion/multi-node/.cluster_token`）是唯一节点身份；一次泄露 = 整集群被攻陷 | 签发每节点证书 + 开启 mTLS（见下） |
| **mTLS** | **默认关闭** — 集群内 HTTP 明文，传输层零节点身份校验 | `FUSION_MTLS_ENABLED=1` + `provision_cluster`/`provision_node`（见 `security/mtls.py`） |
| **Worker 沙箱** | **默认 `None`** — 推理/插件任务无 OS 级资源隔离。不可信输入的路径穿越**仍**在任务门控强制（E5，always-on）；model_sync 网络与 model_path 白名单仅在配置沙箱时激活 | 构造 `WorkerSandbox(SandboxConfig(...))` 传给 `NodeAgent(sandbox=...)` |
| **Master 限流** | 120 req/60s/IP 全局节流（v0.8.8 P2-22）— 守 register/join/vote/submit 防突发 DoS + 审批队列耗尽；health/metrics/SSE 豁免。对抗性 LAN 暴露需加反向代理做更细策略 | 对不可信 LAN 暴露，部署带限流的反向代理做更细逐路由策略 |
| **PII 脱敏 / AES-GCM** | 仅 FMP 协议路径接线（非默认 HTTP 派发路径） | 用 `--transport fmp` 获加密 + 脱敏传输 |
| **可用性** | 单 Master — Master 崩溃 = 整集群停摆。多 Master HA 选举存在（P4）但为**技术预览**，未经生产验证 | 跑在受监管主机（launchd KeepAlive）崩溃重启；勿依赖 HA 满足生产 SLA |

**任何多机部署的最小加固**（可信 LAN）：
1. 开启 mTLS：`provision_cluster` 一次，`provision_node` 每节点，每节点设 `FUSION_MTLS_ENABLED=1` + 证书路径。
2. 限制 `FUSION_AUTO_APPROVE_PATTERNS` 到你的精确子网 CIDR（勿用宽泛模式）。
3. Master 跑在 `./start.sh install-launchd` 下（KeepAlive 崩溃重启）。
4. 勿将 Master/Agent 端口暴露到公网。

- **100% 本地离线** — 零外部网络依赖
- **节点审批** — 新节点需审批或基于模式自动审批
- **Master/Worker 隔离** — 基于角色的权限，API 路径访问控制
- **mTLS 节点认证** — 私有 CA + 每节点叶证书，env 开启双向 TLS — **默认关闭，opt-in**（#80）
- **多租户配额 + 优先队列** — 每租户并发上限，超配入队，优先级有序派发（#81）
- **真网络 E2E** — 真端口绑定 + 真 HTTP 跨进程；节点掉线/重连；docker-compose 跨容器（#76）
- **KV cache 压测** — N 节点跨 HTTP KV warm/transfer 大规模，0 丢失迁移，p99 延迟基线（#79）
- **跨节点 KV 查询** — `lookup_remote` 契约对齐（route→found/entry→decode）；真链 E2E 锁（v0.8.5）
- **自动节点审批** — 可信子网自动加入经 `FUSION_AUTO_APPROVE_PATTERNS` env；CIDR 精确（`172.16.0.0/12`），子串/通配回退（v0.8.4→v0.8.5）
- **Worker 沙箱** — CPU/内存/磁盘限制，路径 & 网络白名单 — **opt-in（`NodeAgent(sandbox=...)`），非默认**；E5 不可信输入穿越守卫 regardless of sandbox 恒开
- **数据脱敏** — 自动检测并脱敏 PII（电话、邮箱、API key、身份证）— **仅 FMP 路径**
- **AES-GCM 加密** — FMP 协议加密通信 — **仅 FMP 路径**
- **熔断器** — 故障节点自动隔离（派发失败 + agent 内部错误均可见）
- **跨节点守卫传输** — issue #52：3 个 TRANSPORT 原语供 fusion-guard 消费 — 审计链 HMAC（防篡改 `seq`/`prev_hash`/`mac`，`GET /api/v1/audit/chain` 段拉取）+ 集群级 rule-epoch 广播（`GET /api/v1/rules/epoch`）+ 跨节点 confirm 中继（`POST /api/confirm`）。HKDF-SHA256 从 cluster_token 派生 3 个域分离 MAC 密钥（无新秘密）。100% 本地/LAN，无云。多节点仅定义 TRANSPORT+IDENTITY+KEY SCHEME；guard 实现消费方。
- **无遥测** — 无分析，无 phone home

---

## 📄 许可证

Apache License 2.0。详见 [LICENSE](LICENSE)。

---

## 🤝 贡献

欢迎贡献！请确保：

1. 测试通过：`pytest tests/ -v`
2. Lint 通过：`ruff check fusion_multi_node/`
3. 4 空格缩进，不写 docstring（自文档化命名）
4. 所有类使用 `logging.getLogger(__name__)`

---

<p align="center">
  <strong>Fusion-Multi-Node — 汇聚 Mac，统一推理，本地扩展。</strong>
</p>
<p align="center">
  <sub>由 Fusion-MLX 团队用 ❤️ 构建</sub>
</p>

# Changelog — fusion-multi-node

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.2] - 2026-08-25

### Added

- **H3 Master 任务持久化 + 崩溃启动恢复** (#66)
  - `_persist_tasks_locked`: 原子落盘非终态任务 (tmp+os.replace+fsync), 终态不存
  - 写点即时落盘: assign_task (RUNNING) / _finalize_task (终态) / cancel_task (终态), 均持 `_tasks_lock`
  - `_persist_loop`: 15s 周期快照兜底; `start()` 调 `_restore_tasks()` 恢复, `stop()` 最终落盘
  - `_restore_tasks`: RUNNING/MIGRATED → PENDING 重派 (崩溃前派发中任务须重新调度)
  - `tests/test_task_persistence.py` (10 用例): 恢复语义 / 终态不存 / 原子写 / 损坏文件 / 全链路 start→stop→restore
- **H2 launchd 进程守护 — 崩溃自愈闭环** (#69)
  - `deploy/com.dahai80.fusion-multi-node.plist`: launchd 模板 (KeepAlive 崩溃重启 + ThrottleInterval 10s 节流 + RunAtLoad), 占位符渲染 (venv/host/port/logdir)
  - `start.sh install-launchd` / `uninstall-launchd`: 渲染 plist → `~/Library/LaunchAgents` → launchctl load/unload; 检测 nohup 进程先停转交 launchd (避双实例)
  - 崩溃 → launchd 重启 → H3 `_restore_tasks` 恢复 = 不丢任务 (进程层 + 数据层双保障)
  - `docs/HA-CRASH-RECOVERY.md`: 崩溃自愈链路图 + 两层保障 + 局限说明
- **S1 任务级熔断器** (#70) — 派发失败自动 ban, 不再持续往故障节点派发
  - `_dispatch_to_node` 失败 (SSRF 拒绝 / HTTP 非 200 / agent 返回非 ok) → `report_fault(node_id, "dispatch_failed")` 累计故障
  - `select_nodes` 候选过滤跳过 ban 期内节点 (gap: 原 ban 仅在 `register_node` 拦截, 调度路径漏拦)
  - 窗口内达 `_FAULT_THRESHOLD` (3) 自动 ban, ban 期内不被选中, 到期/手动解封后恢复可选
  - `tests/test_task_circuit_breaker.py` (6 用例): 派发失败报故障 / 重复失败 ban / 成功不报故障 / ban 节点跳过 / 全 ban 返回空 / 解封重选
- **S2 生产监控指标端点** (#71) — Prometheus exposition `/api/v1/metrics`
  - `ClusterMaster.get_prometheus_metrics`: 纯文本 0.0.4 exposition, 无外部依赖
  - 集群级聚合: 节点总数/在线, 任务总数/运行/待派发/完成/失败, 重试总次数, KV 缓存条目, 内存总量/可用, 派发延迟分位 (p50/p90/p99 + sum/count)
  - 复用 `get_stats` + 派发延迟 (completed_at - started_at) + `_retry_count`; Bearer 鉴权不豁免 (内部抓取携带 token)
  - `tests/test_master_server.py::TestPrometheusMetrics` (4 用例): 鉴权 / text-plain / exposition shape / 空集群
- **S3 负载/压测基线测试** (#72) — 调度层压测吞吐 / 尾延迟 / 无丢失
  - `tests/test_load_stress.py` (3 用例): 四节点集群 + FastBackend (零延迟, 免真模型), 真派发走 PortRoutingTransport ASGI 路由
  - 并发 40 任务无丢失 (lost=0/failed=0/backend_calls=40), 派发吞吐 > 20 task/s
  - 派发延迟尾部分布 (p95 < 1.0s, p99 < 2.0s); DATA 并行两节点 20 任务吞吐 > 10 task/s
  - 压测放开 agent 限流 (默认 30 req/min → 100000) + 节点 max_tasks=200 (测调度吞吐非容量上限)

### Changed

- **H4 cloud_fallback 调度路径切断** (#67) — 唯一违"100%本地/离线"定位的模块
  - 删 `ClusterMaster.setup_cloud_fallback` / `fallback_to_cloud` / `_cloud_client` 字段 / import
  - `_enqueue_retry`: 重试超限直接 FAILED, 不再转云端回退
  - `_retry_loop`: 删 `_cloud_fallback_pending` 分支, 纯重试
  - `cloud_fallback.py` 模块文件 + 单元测试保留供独立验证, 不再接调度器; 待迁移 fusion-gateway (#106)
- **功能归属债区分** (#67) — ast_diff / cluster_sync / mcp_gateway 均纯本地计算 (非云合规债)
  - ast_diff 被 `secure_transfer` (PII 脱敏传输) 复用 → 待迁移 fusion-cowork (#61)
  - cluster_sync 被 `master_server` (LAN 模型清单) 复用 → 待迁移 fusion-cowork (#61)
  - mcp_gateway 未接线死代码 → 待迁移 fusion-gateway (#106)
- `__init__.py` `__version__` 0.7.1 → 0.8.2 (历史漏更新修正); 注释区分云合规债 vs 功能归属债
- pyproject.toml version 0.8.1 → 0.8.2

### Fixed

- CLAUDE.md: cluster_sync "not wired into lifecycle" 实为已接 master_server start()/stop() — 更正; cloud_fallback 标注 v0.8.2 切断现状

## [0.8.1] - 2026-08-25

### Added

- **节点注册幂等 + 故障黑名单** (#20, F-A12 / F-A13)
  - F-A12: `register_node` 再注册 = PATCH 语义 — 保留 Master 权威运行态字段 (`active_tasks`/`max_tasks`/`network_rtt_ms`/`status`), 只更新硬件声明字段。节点重启不冲掉派发中任务计数。返回值由 `None` 改 `bool` (ban 期内 `False`)
  - F-A13: `report_fault` 在 `_FAULT_WINDOW_S` (60s) 窗口内累积达 `_FAULT_THRESHOLD` (3) → 自动 ban `_BAN_DURATION_S` (300s); ban 期内 `register_node` 拒绝 (master_server HTTP 403)
  - `unregister_node(reason="banned")` 主动拉黑; `is_node_banned()` / `unban_node()` 手动查询/解封; 到期惰性自动解封
  - `tests/test_node_registration.py` (9 用例): PATCH 保留运行态 / OFFLINE 恢复 / 故障阈值 ban / ban 拒注册 / 手动解封 / reason 拉黑 / 窗口衰减

### Fixed

- agent 端口迁移 (#19, PR #22): Node Agent 默认端口 11445 → 11458 (与 fusion-comfyui 撞), 全量 81 处替换 + `ClusterConfig._STALE_PORT_MAP` 自动迁移 (9755→11458, 11445→11458)
- `tests/test_pipeline_e2e.py` E2E skip 门补本地 `mlx` 包可导入性检查 — 无 mlx 包的 venv 干净 skip 而非 import 崩溃

## [0.8.0] - 2026-08-25

### Added

- **真实张量 PIPELINE 层切分链** (P3, 接 fusion-mlx #621 `/distributed/*`)
  - `FusionMLXBackend`: `load_shard` / `pipeline_step` / `drop_shard` HTTP 调用上游分布式端点, b64.npy 激活格式, Bearer api_key 鉴权 (显式优先于 env, Rule 5)
  - `NodeAgent._execute_pipeline_step`: pipeline_step 任务类型, 读 model_id/layer_range/hidden_states/input_ids, 调上游 load_shard + pipeline_step, 返回 {shard_id, hidden_states, shape, dtype, node_id}
  - `ClusterMaster._dispatch_pipeline`: 真实层切分 — 按 `task.model_shards` 切段, 首段带 input_ids (embed+layers), 后续段链传 hidden_states, 末节点出口 = 最终张量。`_dispatch_to_node` 透传 pipeline_step_params
  - `AgentConfig.fusion_mlx_api_key` + `DEFAULT_CONFIG.mlx.fusion_mlx_api_key` + `to_node_agent_config` 透传
  - `agent_server`: ALLOWED_TASK_TYPES 加 pipeline_step; PIPELINE_EXTRA_KEYS 透传 model_id/shard_index/layer_range/hidden_states/input_ids/position_ids
  - **真模型 E2E** (`tests/test_pipeline_e2e.py`): Llama-3.2-1B-Instruct-4bit (16 层) 切 [0,8]/[8,16], 两 NodeAgent 共享真 fusion-mlx, PortRoutingTransport 派发, 末节点返回 hidden_states shape [1,4,2048] float16, b64.npy round-trip 校验。需 fusion-mlx 运行 + 小模型, 不满足 skip

- **master→agent 派发循环接线** (P1): `assign_task` 真发 HTTP 到 assigned_nodes (PIPELINE 顺序链 / DATA 并发), `_dispatch_tasks` 跟踪 + `_finalize_task` 回填
- **HA 选举接线** (P4): `ClusterMaster.start(ha_config=...)` enabled=True 时调 `setup_election` 启动选举循环 (默认关闭单 Master)
- **start.sh agent 角色** (P2): 支持 `--role agent` 启动 NodeAgent
- **真实多节点集成测试** (P5, `tests/test_dispatch_integration.py`): PortRoutingTransport 路由 + 真 ASGI agent, 无真实 TCP

### Changed

- `cluster_master.py`: 过时注释 "setup_election 不被 start() 调用" 更新为 P4 接线现状
- pyproject.toml: version 0.7.1 → 0.8.0
- README.md: badge/tests 更新 (852), 模块表 + 架构图 + election 段 + 真实张量 PIPELINE 示例

### Fixed

- `test_node_agent.py::test_hardware_report_loop` hang: R1 重构把 `_hardware_report_loop` 改调 `_collect_dynamic_load`, 测试仍 mock `collect_hardware_info` → call_count 永不增长 → 无限 hang。改 mock `_collect_dynamic_load`

## [0.4.0] - 2026-07-26

### Added

- **LoadMetrics + LoadRouter** (`master/load_metrics.py`)
  - Five-dimensional load metrics: uma_used_ratio, cpu_percent, metal_util, task_queue_len, net_rtt_ms
  - Four routing strategies: BALANCED, VRAM_FIRST, LOCALITY_FIRST, LOW_LATENCY
  - LocalForcedGate: ≤0.5B models forced local execution
  - VRAM-first scheduling: ≥13B models routed to highest-VRAM node

- **Task Sharding** (`master/task_sharding.py`)
  - ShardingType: INFERENCE, AST, VECTORIZE
  - ShardingStrategy: BY_FILE, BY_DOCUMENT, BY_BATCH
  - ShardResult merge with ordering and dedup

- **AST Diff** (`master/ast_diff.py`)
  - compute_ast_diff: added/removed/modified node detection
  - apply_ast_diff: incremental AST reconstruction

- **FMP KV Cache Sync** (`protocol/fmp_message.py`, `distributed_mlx/kv_cache_sharing.py`)
  - KVCacheSyncMessage with FMP protocol
  - sync_to_cluster() in KVSharingManager

- **Storage Enhancements** (`storage/storage_volume.py`, `storage/shard_replication.py`)
  - Capacity monitoring with configurable thresholds
  - LRU auto-eviction for cache volumes
  - ShardReplicator with SHA-256 checksum verification
  - distribute_shard() for model shard distribution to Worker volumes

- **NodeInfo Extensions**
  - device_model + uma_size_gb in NodeInfo, mDNS properties, and registration API
  - role field added to NodeInfo (master/worker/standby)
  - NodeStatus.FAULT state added

- **Node Approval Integration** (`server/master_server.py`)
  - /api/nodes/register now routes through NodeApprovalManager
  - Unapproved nodes blocked from joining cluster

- **Monitoring API** (`server/master_server.py`)
  - GET /api/v1/nodes/{node_id}/metrics — node load metrics
  - GET /api/v1/tasks/{task_id}/progress — task execution progress

- **Log Level Standardization** (`observability/log_store.py`)
  - LogLevel enum: INFO, WARN, ERROR, FATAL
  - Master log aggregation via collect_node_logs()

- **Autoscaler Built-in Actions** (`autoscaler/autoscaler.py`)
  - scale_up: activate standby nodes
  - scale_down: migrate tasks via ClusterMaster.migrate_task() then deactivate

- **Sandbox Resource Limits** (`security/sandbox.py`)
  - CPU/memory/disk limit enforcement via resource module
  - macOS sandbox-exec integration notes

- **Timeout Auto-retry** (`master/cluster_master.py`)
  - Timed-out tasks auto-enter retry queue (1 retry per architecture spec)

### Changed

- Heartbeat interval: 5.0s → 3.0s (DEFAULT_CONFIG)
- Retry attempts: 3 → 1 (architecture spec alignment)
- pyproject.toml: added protobuf>=5.0.0 dependency
- pyproject.toml: version bumped 0.3.0 → 0.4.0
- protocol/__init__.py: exports FMPProtoMessage, FMPEnvelope, FMPControl, FMPPayload

### Fixed

- AST diff _find_node/_remove_nodes root-path traversal bug
- ClusterConfig.save() PermissionError on system dirs (chmod try/except)
- test_get_gpu_cores → test_get_gpu_info method rename

## [0.3.0] - 2026-07-25

### Added

- Full audit remediation (P0-P3, 53 findings)
- asyncio.Lock for all shared mutable state
- Unbounded dict cleanup (tasks/nodes/requests/shards/pipelines/rounds/hot_prompts)
- TLS certificate fingerprint pinning
- Task retry queue for assign_task failures
- O(1) KV lookup index
- httpx connection reuse
- InferenceBackend protocol decoupling
- StandbyMaster HA stub
- msgpack serialization option
- Load-aware task assignment
- MDNSDiscovery async browse rewrite

### Fixed

- 53 audit findings across P0-P3 severity levels
- All ruff lint errors resolved

## [0.2.0] - 2026-07-25

### Added

- BearerAuth middleware for all API endpoints
- SSRF validation on user-supplied URLs
- AES-GCM AAD (Additional Authenticated Data) binding
- InMemoryRateLimiter (time-driven, sliding window)
- ECDH key exchange + TLS protocol extension
- mDNS shared-secret verification
- encrypt_message immutability guarantee
- ASGI auth + rate-limit middleware

### Fixed

- 16 security findings (4 CRITICAL) from initial security audit

## [0.1.0] - 2026-07-25

### Added

- **Cluster Master** — node registration, score-based selection, task lifecycle, KV cache pool, heartbeat monitoring
- **Node Agent** — hardware info collection, heartbeat, task execution, mDNS auto-discovery
- **mDNS Discovery** — Bonjour zero-config service registration and browsing
- **FMP Protocol** — three-layer binary protocol, AES-GCM encryption, circuit breaker, hop_count
- **Distributed MLX Bridge** — model sharding, pipeline/data parallelism, Caveman compression, KV cache sharing
- **MCP Cluster Gateway** — tool registration, node selection, request forwarding
- **Cluster Observability** — metrics, logs, alerts, cluster reports
- **Configuration** — JSON config with dot-notation, recursive merge
- **CLI** — 15+ commands across 7 groups
- 585 tests, 96.1% code coverage

[0.8.2]: https://github.com/dahai80/fusion-multi-node/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/dahai80/fusion-multi-node/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.4.0...v0.8.0
[0.4.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dahai80/fusion-multi-node/releases/tag/v0.1.0

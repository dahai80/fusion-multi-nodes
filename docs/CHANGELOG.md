# Changelog — fusion-multi-node

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.8.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.4.0...v0.8.0
[0.4.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dahai80/fusion-multi-node/releases/tag/v0.1.0

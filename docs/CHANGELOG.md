# Changelog — fusion-multi-node

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.12.0] - 2026-08-27 — 审计 0826 P1 整改 (27 项)

> **企业级生产加固**: 审计 `fusion-multi-node-audit-result-product-0826.md` 判定 27 P1 项
> (容错调度 / KV 张量 / 安全 / API 契约 / Agent / 性能运维) 全部代码修复落地。P0 热修后
> 基线 1213 → 1262 测试全绿 (新增 ~49 例)。ruff 净。无 API 破坏 (minor bump)。

### 容错/调度 — cluster_master (8)

- **P1-1 H3 RUNNING→PENDING 重派未实现 → 孤儿任务**: `_restore_tasks` 仅重派 PENDING,
  RUNNING/MIGRATED 崩溃恢复后 `started_at=0` 永久卡死。修复: RUNNING/MIGRATED 转 PENDING +
  清 started_at + 清 assigned_nodes + 原节点并入 exclude_nodes (避回坏节点) + retry_count 不增
  (崩溃非任务失败)。对齐 CLAUDE.md "RUNNING→PENDING 重派" 自述。
- **P1-14 超时重试不补 `task.exclude_nodes` → 回同坏节点**: `_enqueue_retry` 重置 PENDING 但
  不动 exclude_nodes。修复: `assigned_nodes` 并入 `exclude_nodes` (去重); `migrate_task` 源
  节点并入 exclude; P1-1 转换时原 assigned_nodes 亦并入。`assign_task` 用 exclude_nodes 过滤。
- **P1-15 节点 OFFLINE 不自动迁移在途任务 → 等满 300s timeout**: `_refresh_node_statuses` 标
  OFFLINE 但不动在途任务。修复: OFFLINE 时该节点所有 RUNNING 任务转 PENDING + 节点并入
  exclude_nodes + `_enqueue_retry` (走 P1-14 同路径, 避锁嵌套)。限频防抖动雪崩。等价自动迁移
  (P3-3 语义由此路径 + 手动 `migrate_task` 共同满足)。
- **P1-16 `sync_kv_cache` 不分类异常 → 一律 False 无重试**: `status_code != 200` + `except`
  全 False。修复: 区分 transient (429/5xx/超时 — warning + 可重试标记) vs logic (404/逻辑错 —
  False); P0-3 流式基础上叠加分类。
- **P1-19 `_pending_queue` 无长度上限 → 过载堆积**: 节点不足入队无上限。修复: `MAX_PENDING_QUEUE`
  (config `scheduling.max_pending_queue`, 默认 1000); 满则拒入队, `assign_task` 返 False →
  master_server submit 回 503 `集群队列已满`。
- **P1-21 `_retry_loop` 无退避无限重试**: assign 失败立即 `_enqueue_retry` 无退避。修复: per-task
  指数退避 — `next_retry_at = now + backoff` (基 30s, 上限 600s, 确定性无 jitter); `_retry_loop`
  跳过未到时任务; `_max_retry_loop_attempts` (默认 10) 超限转 FAILED。
- **P1-23 agent_server 限流 429 累熔断 fault → 高 QPS 健康节点误 ban**: GAP-6 修 fusion-mlx 内部
  429 漏 agent_server 自身 429。修复: `_dispatch_to_node` 通用 `!= 200` raise 前检测 429 → 归
  transient (不 report_fault, 读 `Retry-After` 走 P0-2 同分类)。
- **P1-11 `_persist_tasks_locked` 全量 asdict O(N) 每次派发**: 1000 任务 O(N²)。修复: 增量持久化 —
  `_dirty_task_ids` 脏标记, 只 asdict 脏任务 + 写增量 patch (周期全量+增量混合)。
- **P1-13 httpx 连接池无显式配置**: 默认 max_connections=100。修复: 读 config `network.http_limits`
  构 `httpx.Limits` 传 `_get_dispatch_http`; 默认按集群规模 (节点数×4)。

### KV 张量 — kv_cache_sharing / kv_tensor_transport (2)

- **P1-20 `MLXKVTransport.import_tensor` 失败返 True 掩盖**: `except: return True`。修复: 404
  (上游未落地) 仍降级返 True (合成兜底 + warning); 其他 `except` 返 False (真装载失败, 调用方
  知); `SyntheticKVTransport.import_tensor` 保持 True。
- **P1-22 `KVSharingManager` 跨节点调用静默吞异常**: `lookup_remote` (debug) /
  `transfer_from_remote` (error+False) / `warm_cache` (warning+failed++)。修复: 分类 — 429/5xx/
  超时/连接拒 warning + `record_metric` (transient 不计 ban); 连续失败达阈值 (3) 提
  `create_alert` (warning, node_id); `lookup_remote` 日志 debug→info (网络分区运维须可见)。

### 安全 — security / discovery (7)

- **P1-3 HTTP 派发路径 PII 明文** (校准降级): DataScrubber 仅 FMP 路径。修复: 可选 HTTP 路径脱敏 —
  `ClusterConfig.security.http_pii_scrub` (默认 False, LAN 可信保留明文)。开启时 `_dispatch_to_node`
  payload + chat 代理 + warm_cache 经 `DataScrubber.scrub` prompt/messages。文档强制: 跨不可信
  网络段须 mTLS + 此项。
- **P1-4 `cloud_fallback` 模块保留含云 API 硬编码** (校准降级): 加 import-time 禁用守卫 — 模块顶部
  `if FUSION_CLOUD_FALLBACK_ENABLED != "1": raise ImportError(...)`; `__init__.py` 导入处 try/except
  降级; 测试 stub 设 env。保留模块文件 (迁移债有形化, 待 #106)。
- **P1-5 RBAC `check_user_path_access` fail-open**: 未登记路径 `perm is None`→`return True`。修复:
  fail-closed — `perm is None`→`return False`; 显式白名单放行集群内部/健康/文档路由
  (`_USER_EXEMPT_PATHS` frozenset)。集群令牌走 `role is None` 提前返不经此函数, 不受影响。
- **P1-6 `_enforce_user_rbac` 覆盖不全** (8 路由无 user-RBAC): config/reload / autoscaler PUT /
  observability logs export / ha/sync-state / nodes / kv / metrics。修复: 全登记
  `_USER_PATH_PERMISSION_MAP` (ADMIN: config/autoscaler/ha; VIEWER: metrics; USER: kv read);
  集群内部路由用 sentinel `CLUSTER_INTERNAL` (仅 cluster_token, 用户令牌全拒)。
- **P1-7 AuditLogger 写失败静默降级**: `except: warning` 不 raise。修复: `record_metric("audit",
  "write_failed",1.0)` + `create_alert` (warning, "审计日志写入失败"); 不 raise (鉴权主路径不被
  拖垮, 运维可见)。`read()` 同理。
- **P1-8 `manual_join.py` cluster_secret 非常量时间比较**: `!=`。修复:
  `secrets.compare_digest`; 空 secret → warning + 强制 compare 失败 (拒所有 join)。
- **P1-9 `manual_join.py` 硬编码 `http://`, mTLS 开启 join 失效**: URL 改
  `f"{mtls_scheme()}://..."`; `_get_client` 传 `**mtls_client_kwargs()`。

### API 契约 — master_server (1)

- **P1-10 9 路由 raw dict 无 pydantic 校验**: sync/incremental / join / approve / reject /
  autoscaler PUT / ha/vote / ha/sync-tasks / ha/sync-state / ha/heartbeat。修复: 各建 pydantic
  `BaseModel` (`IncrementalSyncRequest` / `ManualJoinRequest` / `NodeApproveRequest` /
  `NodeRejectRequest` / `AutoscalerConfigUpdateRequest` / `VoteRequest` (复用) /
  `HASyncTasksRequest` / `HASyncStateRequest` / `HAHeartbeatRequest`); handler 签名 dict→Model;
  FastAPI 返 422 (非 400) for missing/invalid。

### Agent — node_agent / agent_server / election (3)

- **P1-2 `/api/hardware` 同步阻塞事件循环**: `async def hardware_info()` 同步调
  `collect_hardware_info` (system_profiler 5s + ipconfig)。修复:
  `await asyncio.to_thread(self.agent.collect_hardware_info)` — 对齐 `report_hardware` 范式。
- **P1-17 HA 选举空窗无 503**: 选举期 `_is_leader` 未定时 `/api/tasks/submit` 仍派发。修复:
  `MasterElection.leader_known` 属性; 守卫 `_election 配置且非 _is_leader 且非 leader_known` → 503
  `选举过渡中`; 同步周期 5s→2s (config `ha.state_sync_interval`)。
- **P1-18 agent `_running_task_handles` 无本地容量上限 → TOCTOU**: master 依赖心跳 TOCTOU。修复:
  `execute_task` 入口 `if len(_running_task_handles) >= config.max_tasks: return {"overload":True,
  "error":"节点任务已满"}`; master `_dispatch_*` 加 `overload` 分类 (transient, 不 report_fault, 选
  其他节点)。匿名 task `anon-{seq}` 防 `_running_task_handles` 撞键。

### 性能/运维 — tests / observability / docs / config / utils (6)

- **P1-12 无真推理吞吐基准**: `test_load_stress.py` FastBackend fake 零延迟。修复: 新
  `test_real_inference_benchmark.py` — skip-gate `_mlx_alive() and _model_available()`, 真 fusion-mlx
  + `mlx-community-Llama-3.2-1B-Instruct-4bit`, 测单/多节点 DATA 并行吞吐, 断言多节点 ≥0.9× 单节点
  (真模型抖动留余量)。
- **P1-24 Prometheus 缺熔断/限流/节点级指标**: 仅集群聚合。修复: `get_prometheus_metrics` 补
  `fusion_cluster_banned_nodes` gauge / `fusion_cluster_rate_limited_total` counter / 节点级
  `fusion_node_memory_total_gb{node_id}` / `fusion_node_memory_available_gb{node_id}` /
  `fusion_node_active_tasks{node_id}` / `fusion_node_banned{node_id}` (节点快照单独持 `_nodes_lock`,
  不与 `_tasks_lock` 嵌套)。
- **P1-25 `HA-CRASH-RECOVERY.md:133` 过期 (KV no-op)**: 改 "KV 张量跨节点传输已交付 (GAP-7 v0.11.0),
  合成默认 + MLX env-gated 待上游 #650; v0.11.1 起流式传输"。
- **P1-26 `kv_cache.json`/`users.json` 缺 fsync**: 对齐 `config.py` save 范式 — `f.flush()`+
  `os.fsync(f.fileno())` 再 `os.replace`。两文件同改。
- **P1-27 命令行直起无日志文件**: `setup_logger` 无 env → 仅 stdout 不落盘。修复: 未配
  `FUSION_MULTINODE_LOG_FILE` 写 stderr 提示 (不加 handler → `len(handlers)==1` 断言不破)。README
  运行章节强调 env。
- **P1-12 基准配套**: `AgentConfig(max_tasks=64)` + register payload `max_tasks=64` +
  `master._task_store_path` 防 H3 persist dir-missing + agent overload 误拒。

### Tests — 新增 ~49 例

- `test_cluster_master.py`: H3 重派 / exclude / OFFLINE 迁移 / 队列上限 / 退避 / 429 / 增量持久化 /
  选举空窗 503。
- `test_master_server.py`: pydantic 9 路由 422 + RBAC fail-closed + 全路由登记 + node-level metrics。
- `test_kv_*.py`: import_tensor 降级/失败区分 + 跨节点异常分类告警。
- `test_enterprise_security.py` / `test_mtls.py` / `test_user_rbac.py`: cloud_fallback 守卫 /
  manual_join compare_digest+mTLS / RBAC fail-closed。
- `test_real_inference_benchmark.py` (新): skip-gated 真推理基准。
- `test_utils.py`: P1-27 stderr 提示。

### Maintenance

- ruff format 应用到全批触及文件 (cluster_master / kv / security / server / agent / tests)。

## [0.11.1] - 2026-08-27 — 审计 0826 P0 热修 (5 阻断项)

> **生产阻断消除**: 审计 `fusion-multi-node-audit-result-product-0826.md` 判定 5 P0 阻断项
> (❌ 不具备企业级生产商用发布条件) 全部代码修复落地。循环容错 / 派发误 ban / KV 流式 /
> 异步落盘 / 告警出站五项闭环, 重审可发布。基线 1203 → 1213 测试全绿。

### Fixed — P0 阻断 (5)

- **P0-1 4 背景循环无逐次异常隔离**: `_persist_loop` / `_retry_loop` / `_health_check_loop`
  (cluster_master) + `_election_loop` (election) 仅外层 `try/except CancelledError`, 循环体内
  `await` 抛非取消异常 → 杀整个循环, Master 表面健康 (HTTP 200) 但持久化/重试/超时/选举
  静默停滞, 零告警。修复: 每个循环体加内层 `try: <body> except CancelledError: raise
  except Exception: logger.warning; continue` (复用既有 `_state_sync_loop` 范式)。
  test: 4 循环首调抛 RuntimeError, 断言循环未死 (计数器递增)。
- **P0-2 `dedup_blocked`/`sandbox_blocked` 误归 logic_fail + report_fault**: GAP-6 修了
  `rate_limited` 漏 `dedup_blocked`/`sandbox_blocked` → 走 `"error" in r` → `logic_fail` +
  `report_fault`。H3 重派触发去重 → 累 fault → 60s 窗口 3 次 → 健康节点 ban 300s。
  修复: `_dispatch_data`/`_dispatch_pipeline` 在 `rate_limited` 分支后、`"error" in r` 前,
  加独立分类 (不 report_fault, 不重试 — 去重属 master 自身重派错误, sandbox 阻塞归配置)。
  test: mock agent 返 `{"dedup_blocked":True}`, 断言 `report_fault` 未调 + 节点未 ban。
- **P0-3 KV 张量 base64+JSON 单 POST → 1.5GB 峰值/JSON 阻塞**: 全 bundle 经
  `exp_resp.json().get("bundle")` 物化内存再 `client.post(json=)`。500MB 张量 → base64 膨胀
  1.33× → JSON 解析峰值 1.5GB。修复: 流式二进制协议 — 头部 JSON 元数据 (shards 无 tensor)
  + 定长 magic + 各 shard 原始 tensor bytes 拼接。agent `/api/kv/export-stream`
  (`StreamingResponse` octet-stream) + `/api/kv/import-stream` (raw body); master
  `sync_kv_cache` `aread()` 源响应 → `content=src_bytes` 目标请求体, 旧 JSON bundle 路径
  向后兼容 (export-stream 404 降级)。test: 10MB 合成张量流式 round-trip 字节完整
  (tracemalloc 峰值记录供审计)。
- **P0-4 `_write_task_store` 同步 fsync 阻塞事件循环**: fsync 已移出 `_tasks_lock` (P1-11)
  但仍阻塞 asyncio 单线程 (SSD 1-5ms/fsync, 100 task/s 占 10-50%)。修复: 5 call sites
  (`_persist_tasks`/assign/finalize/cancel/retry) 改 `await asyncio.to_thread(
  self._write_task_store, snapshot)` — 锁内快照拷贝纯内存, to_thread 内写盘不持锁不阻塞。
  test: monkeypatch 慢盘 80ms, 并行 40ms `asyncio.sleep` 计时器 <0.07s 完成 (证明 fsync
  移出事件循环)。
- **P0-5 告警无出站通道, `on_alert` 零注册**: 告警机制存在 (`create_alert` 同步调 handler)
  但 master 从不注册 → 节点掉线/内存告警只进 deque, 运维须轮询。修复: `ClusterMaster.start`
  读 env `FUSION_ALERT_WEBHOOK_URL`, 非空则注册 fire-and-forget handler — 收 `Alert` →
  `asyncio.create_task(_post_alert_webhook)` (`to_thread` 包 httpx POST, 失败 warning 不
  拖垮告警链)。空 env → `logger.info` 不强制。test: env 设 webhook, monkeypatch httpx POST
  断言 Alert 序列化 POST 被调 + `create_alert` <50ms 不阻塞。

### Tests — 新增 ~10 例

- `test_cluster_master.py`: 4 背景循环容错 + P0-2 dedup 不 ban。
- `test_kv_tensor_e2e.py`: `TestKVTensorStreamingMemory` 10MB 流式 round-trip。
- `test_task_persistence.py`: `test_fsync_does_not_block_event_loop`。
- `test_observability.py`: `TestP05AlertWebhook` (2 例)。
- `tests/test_scheduling.py::test_quota_zero_unlimited`: 修复既有基线失败 (SSRF 守卫
  monkeypatch 双模块 + `_HoldClient` 锁定 RUNNING 计数稳定)。

### Maintenance

- ruff format 应用到本批触及文件 (cluster_master / kv_cache_sharing / agent_server / 4 测试)。

## [0.11.0] - 2026-08-27 — GAP-7 KV 张量跨节点传输 (close #33)

> **`sync_kv_cache` 张量级跨节点传输交付**: 经可插拔张量后端编排骨 `/api/kv/export` → 目标
> `/api/kv/import`, 返 `True`。合成后端 (默认, 确定性 `hashlib` 生张量, 无依赖) 满足 #33 验收
> (张量 round-trip 跨 2 agent); MLX 真张量后端 env-gated (`FUSION_KV_TENSOR_BACKEND=mlx`)
> 待上游 fusion-mlx issue #650 落地激活 — 404→降级合成 + warn (fail-visible, Rule 12)。
> P3-28 / GAP-7 / issue #33 三项归一关闭。

### Added

- **KVShard 张量字段** (`distributed_mlx/kv_cache_sharing.py`) — S1, GAP-7
  - `KVShard.tensor: bytes | None = None` 新字段 (metadata 不变, 张量是新 payload)。
  - `_serialize_entry`/`_deserialize_entry` 扩展: tensor base64 随 JSON 传输 (压缩标记 `tensor_compress`: "caveman"/"none"); 无 tensor 旧 bundle 向后兼容 (tensor=None, 省略 key)。
  - `KVSharingManager` ctor 加 `transport: KVTransportBackend | None` 注入 (默认合成); `export_bundle(cache_id, model_name)`/`import_bundle(bundle)` 新方法 (经 transport 产/存张量, store_local 预算门)。
- **可插拔张量后端** (`distributed_mlx/kv_tensor_transport.py`) — S1, GAP-7 (新文件)
  - `KVTransportBackend` Protocol (`export_tensor`/`import_tensor`/`name`/`close`)。
  - `SyntheticKVTransport` (默认, name="synthetic"): 确定性 sha256-based 合成张量 (默认 512 字节, 同 seed 同字节, 不同 node_id 差异), 无 numpy 依赖, 纯本地。
  - `MLXKVTransport` (env-gated `FUSION_KV_TENSOR_BACKEND=mlx`, name="mlx"): 调 fusion-mlx `/distributed/kv_cache/export|import` (待 #650); 404→降级合成 + warn。读 `FUSION_MLX_URL`/`FUSION_MLX_API_KEY`。
  - `get_kv_transport()` 工厂读 env 选后端 (默认 "synthetic")。
- **Agent export/import 路由** (`server/agent_server.py`) — S2, GAP-7
  - `POST /api/kv/export` body `{cache_id, model_name}` → `{status, bundle}` (源本地缓存含张量)。
  - `POST /api/kv/import` body `{bundle}` → `{status, stored}` (目标 store_local 预算门 + LRU)。
  - `KVExportRequest`/`KVImportRequest` 请求模型。
- **Master `sync_kv_cache` 真传输** (`master/cluster_master.py`) — S3, GAP-7
  - 重写: 注册 KVCacheSyncMessage 元数据 → `_kv_lock` 快照 entry → 解析源 (`_snapshot_nodes`) + 目标 (显式或 `select_nodes(DATA, exclude_nodes=[src])`) → 双向 SSRF 守卫 (`is_safe_peer_host`) → 源 `/api/kv/export` (build_safe_url + Bearer + X-Node-Id/Role "master", timeout=max(30, size_mb*2+30)) → 目标 `/api/kv/import` → 成功注册 replica `KVCacheEntry(cache_id="{id}@{tgt}")` + LRU trim。返 True/False (任一跳失败/缺失 → False, 不谎报部分成功)。
  - 加 `target_node_id=""` 可选参数 (空→自动选非源在线节点)。
- **`/api/kv/sync` 路由** (`server/master_server.py`) — S3, GAP-7
  - `POST /api/kv/sync` body `{cache_id, model_name, source_node_id, size_mb, target_node_id?}` → `{status, synced}` (Bearer 鉴权, standby 守卫 503, 审计 action `kv_sync`)。
  - `KVSyncRequest` 请求模型。

### Changed

- `tests/test_new_features.py::TestKVCacheSyncMessage::test_sync_kv_cache_in_master` 改写 — 不再断言 "如实返回 False", 改为断言传输执行 (返 True + 目标查回张量); `test_sync_kv_cache_missing_entry` 保持 False。

### Tests

- **`tests/test_kv_tensor_serialize.py`** (新, 11 用例) — S1: KVShard.tensor round-trip / 无 tensor 向后兼容 / SyntheticKVTransport 确定性 / 不同 node_id 差异 / env 后端选择 / export/import_bundle 接张量。
- **`tests/test_kv_export_import_routes.py`** (新, 6 用例) — S2: ASGI 路由 round-trip (PortRoutingTransport) / 预算拒 oversize / 缺缓存 404 / auth 401。
- **`tests/test_kv_tensor_e2e.py`** (新, 4+1 skip) — S3: master 编排 2 agent 真 ASGI, 张量字节跨节点完整 / 自动选目标 / 缺 entry False; env-gated 真张量测试 skip (待 #650)。
- `tests/test_master_server.py` 加 `test_kv_sync_route_missing_entry`。

### Docs / Version

- 版本 0.10.7 → **0.11.0** (`pyproject.toml`, `fusion_multi_node/__init__.py`)。
- `__init__.py` 模块 docstring: 删 "张量级 KV 跨节点传输仍 no-op", 反映交付 + 上游 #650 gating。
- README badge: version 0.10.7→0.11.0, tests 1181→1203; 头部 F5→GAP-7 发布块; R3/P3-28 标记交付。
- 全量 `pytest tests/ -q`: **1203 passed**, 1 skipped, ruff clean。

### 验收 (#33)

1. `sync_kv_cache` 转真 KV 张量跨节点 + 返 True ✓
2. 集成测试验张量 round-trip 跨 2 agent (`test_kv_tensor_e2e.py`) ✓
3. README "Master 级 KV 张量同步为 no-op" → 交付 ✓

### 风险 / 约束

- **JSON 张量体积**: base64 压缩 shard 张量随 JSON — 大张量膨胀。缓解: Caveman 压缩默认开; `size_mb` 预算门拒 oversized; 路由 timeout 按 size_mb 缩放 (复用 P1-13 模式)。v0.11.0 无流式 (metadata+bundle 单 POST), 流式延后。
- **上游依赖**: `MLXKVTransport` 在 #650 落地前为死代码 — 合成后端为始终可用默认, #33 验收不依赖上游。真张量为 env-gated bonus。
- **100% 本地/离线**: 合成后端纯本地计算; `MLXKVTransport` 仅调本地 fusion-mlx (同节点/集群), 不引入云路径。

## [0.10.7] - 2026-08-27 — GAP-8 Phase F5: 令牌轮换 + 多租户运维 Runbook

> **用户多活令牌轮换 + 集群共享令牌零停机滚动**: 用户令牌 rotate 签新留旧 (多活, 客户端灰度
> 切换无停机), revoke 另调。集群共享令牌经 `FUSION_CLUSTER_TOKEN_PREVIOUS` 环境变量开重叠窗 —
> 入站接受 current + previous (常量时间 `secrets.compare_digest`, 不泄露哪个匹配), 出站始终发
> current (`_get_dispatch_token` 读 `FUSION_CLUSTER_TOKEN`)。按 master→agent 顺序逐节点轮换,
> 无 401 离线窗口。补 `docs/OPERATIONS.md` 多租户用户令牌运维章节 (bootstrap admin / CRUD /
> 轮换吊销 / 审计)。GAP-8 Phase F (多租户/远程接入) 至此完成 (KV no-op 待上游, issue #33)。

### Added

- **集群令牌 previous-active 重叠窗** (`utils/auth.py`) — GAP-8 Phase F5
  - `BearerAuthMiddleware.__init__` 读 `FUSION_CLUSTER_TOKEN_PREVIOUS` env 注入旧令牌; 空串/未设/与 current 一致 → 不开重叠窗 (单令牌, 行为不变)。
  - 集群令牌校验路径: current 不中 → 若 previous 存在且匹配则通过 (常量时间比较, info 日志 `集群令牌重叠窗: previous-active 令牌通过`), 否则 401 + 审计 `auth_fail`。
  - 出站 `_get_dispatch_token` (cluster_master.py) 不变 — 读 `FUSION_CLUSTER_TOKEN` (current), 滚动重启期对端已先接受旧值。
- **多租户用户令牌运维 Runbook** (`docs/OPERATIONS.md`) — GAP-8 Phase F5
  - 重写 "Token 轮换" 章节: F5 零停机滚动流程 (设 previous → 逐节点轮换 current → 关窗) + 全停全启备选; 出站语义说明 (master→agent 顺序)。
  - 新增 "多租户用户令牌" 章节: 首启引导 ADMIN (`FUSION_BOOTSTRAP_ADMIN`) / 用户 CRUD API (create/issue/rotate/revoke/list) / 多活轮换语义 / 零配置向后兼容 / 审计查询。
  - 诊断入口数据目录表补 `users.json` (多租户 scrypt 哈希) + `audit.log` (安全审计 JSONL)。
- **令牌轮换测试** (`tests/test_token_rotation.py`, 7 用例) — GAP-8 Phase F5
  - 用户: rotate 签新留旧 (old+new 均 200); revoke 旧令牌后 old 401 new 200; rotate 路由返回新令牌 + 旧令牌仍有效。
  - 集群: previous+current 均接受 (200); 未设 env → previous 401; previous==current 不开窗 (另一令牌 401)。
  - 出站: `_get_dispatch_token` 返 current (非 previous); previous 令牌对入站仍 200 (出/入两端语义分离)。

### Changed

- 版本 0.10.6 → **0.10.7** (`pyproject.toml`, `fusion_multi_node/__init__.py`)。
- README badge: version 0.10.6→0.10.7, tests 1174→1181; 头部 F4→F5 发布块; 剩余任务删 F5 (已完成), 仅留 KV no-op (#33)。

### Fixed

- 无 (本轮无缺陷修复)。

### Tests

- 全量 `pytest tests/ -q`: **1181 passed**, ruff clean。
- 新增 `test_token_rotation.py` 7 用例 (+7, 1174→1181)。
- 注: `test_pipeline_e2e.py::test_pipeline_two_shard_real_tensor` 真 fusion-mlx 张量推理, 全量套件负载下偶现 RUNNING (真模型前向时序竞争); 单独运行通过, 非代码缺陷。

## [0.10.6] - 2026-08-27 — GAP-8 Phase F4: 集群控制 API 契约 /api/v1

> **/api/v1 typed 契约 + HTTP 文档 + 漂移检测**: `/api/v1/*` 路由补齐 `response_model=` Pydantic 契约,
> 覆盖 9 集群控制操作 (list_nodes/register/remove/submit/migrate/degrade/progress/cluster_stats/
> observability_suggestions)。fusion-agent-studio 可据此桥接真实 multi-node 集群 (替代内存 dev 集群,
> 解决 #32)。OpenAPI `/openapi.json` 现对 9 操作 + autoscaler/observability 返回 typed schema。

### Added

- **13 V1* Pydantic 响应模型** (`server/master_server.py`) — GAP-8 Phase F4, issue #32
  - `V1NodeResponse` (16 字段含 role), `V1NodeListResponse`, `V1NodeRegisterResponse`, `V1StatusResponse`
  - `V1TaskResponse` (16 字段), `V1TaskSubmitResponse` (+queued), `V1TaskProgressResponse`
  - `V1ClusterStatsResponse`, `V1ObservabilitySuggestionsResponse`, `V1AutoscalerConfigResponse`
  - 对齐 `_node_to_resp`/`_task_to_resp` 实际输出; 旧 v0.1 时代 `NodeResponse`/`TaskResponse` (字段过时且未接 response_model) 已删
- **typed /api/v1 路由** — 9 操作经 `response_model=` 类型化:
  - `GET /api/v1/nodes`, `GET /api/v1/nodes/{id}`, `POST /api/v1/nodes/register`, `DELETE /api/v1/nodes/{id}`
  - `POST /api/v1/tasks/submit` (200 派发 / 202 queued), `POST /api/v1/tasks/{id}/migrate`, `POST /api/v1/tasks/{id}/degrade`
  - `GET /api/v1/tasks/{id}/progress`, `GET /api/v1/cluster/stats`, `GET /api/v1/observability/suggestions`
  - autoscaler GET/PUT 显式 503 not-wired (契约文档化, 非歧义 enabled:False)
- **HTTP 文档** — `docs/API.md` 重写为 HTTP 路由契约表 (9-op contract table + 其余路由分组); Python 类文档迁出 `docs/PYTHON_API.md`
- **漂移检测测试** — `tests/test_api_docs_contract.py`: 每 `/api/v1` 路由须在 API.md 出现; 9-op 契约表完整; PYTHON_API.md 存在

### Fixed

- **重复路由遮蔽** (first-registered-wins): 旧 untyped `/api/v1/cluster/stats`、`/observability/suggestions`、
  `/autoscaler/config` (GET/PUT)、`/tasks/{id}/progress` 与新 typed 副本同路径 → 后注册被遮蔽 (死代码)。
  修复: bless 旧路由加 `response_model=` (字段对齐 V1* 模型), 删 5 个遮蔽副本 + 未用 `V1AutoscalerConfigUpdateRequest`。
  验证: OpenAPI `/openapi.json` 对 4 路由返回正确 `$ref` (V1ClusterStatsResponse 等)。

### Tests

- `tests/test_v1_contract.py` (17): 9 操作 schema 校验 + 注册 400 / 提交 202 queued / 降级链模型 / progress 404
- `tests/test_api_docs_contract.py` (3): docs 漂移检测
- 回归 1174 passed (+20), ruff clean

## [0.10.5] - 2026-08-27 — GAP-8 Phase F3: 统一推理代理 /v1/chat/completions

> **统一推理入口 + 租户在途配额**: master 新增 `/v1/chat/completions` 轻量 pass-through 代理,
> 经 `select_nodes(DATA, count=1)` 路由选中节点 agent `/api/v1/chat/completions` →
> `FusionMLXBackend.chat` (原生 OpenAI 格式, 支持流式 SSE 透传)。用户令牌经 `chat:complete` RBAC +
> 租户在途并发配额 (复用 `_tenant_max_concurrent`, 超限 429 + 审计 `chat_quota_exceeded`); 集群令牌
> 内部放行 (无租户 gate)。解决 #27 两套路由分叉 — 客户端经 master 统一推理入口, 非任务流水线 (同步直返,
> 不进 self.tasks/持久化/优先级队列)。

### Added

- **master `/v1/chat/completions` 代理** (`server/master_server.py`) — GAP-8 Phase F3, issue #27
  - `ChatCompletionsProxyRequest` (model/messages/temperature/max_tokens/stream/extra)
  - 流程: `_enforce_user_rbac` (chat:complete; VIEWER→403) → `acquire_chat_slot` (租户配额, 超限 429 +
    审计 `chat_quota_exceeded`) → `select_nodes(DATA, count=1)` → `build_safe_url` + `is_safe_peer_host`
    (出站 SSRF 守卫) → 复用 `_get_dispatch_http` 连接池转发 → 原生 OpenAI 格式直返 / 流式 `StreamingResponse`
  - 槽释放统一 try/finally + `stream_released` 标记 (流式在 `_relay` finally 释放, 非流式/异常外层释放, 防双重)
  - 审计 `actor=user_id` (用户令牌) / `master` (集群令牌), `action=chat`, `node_id=选中节点`
- **agent `/api/v1/chat/completions` 透传** (`server/agent_server.py`) — GAP-8 Phase F3
  - `ChatCompletionsRequest` + `POST /api/v1/chat/completions` → `_check_permission` (TASK_EXECUTE) →
    `FusionMLXBackend.chat` (429 退避 + api_key Bearer); 非流式直返, 流式 `StreamingResponse` 透传 fusion-mlx SSE
  - `is_safe_path_segment(model)` 守卫 (防路径穿越, 非法 → 400); 非 FusionMLXBackend → 503
- **租户在途配额** (`master/cluster_master.py`) — GAP-8 Phase F3
  - `_chat_lock` + `_inflight_chat: dict[str,int]` 轻量计数器 (独立于三域锁, 不污染 self.tasks)
  - `acquire_chat_slot` / `release_chat_slot` 复用 `_tenant_max_concurrent` (0=不限); 配额满返 False → 429
- **node-RBAC 映射** (`security/permission.py`): `/api/v1/chat/completions` → TASK_EXECUTE (集群内部 master 派发)

### Tests

- `tests/test_chat_proxy.py` (8): USER 非流式 200 路由 / 集群令牌放行 / VIEWER 403 / 无节点 503 /
  租户配额满 429 + 审计 / 审计 actor=chat / 流式 SSE / 槽释放归零
- `tests/test_agent_chat_passthrough.py` (5): 非流式 / 流式 SSE / 非法 model 400 / 无 token 401 / 空 model 400

**回归**: 1154 tests passed, 0 ruff errors.

## [0.10.4] - 2026-08-27 — GAP-8 Phase F2: per-user RBAC + user CRUD + tamper-proof audit

> **多租户 RBAC 强制 + 用户管理**: 用户令牌经 `check_user_path_access` 按 UserRole 鉴权 (USER
> 可提交/取消, VIEWER 只读, migrate/degrade 仅 ADMIN); `task.user` 取已认证 user_id, 忽略客户端
> 自报 (防伪造审计 actor); 新增 ADMIN-only 用户 CRUD + 令牌签发/吊销/轮换 API。集群令牌路径不变
> (内部可信, 用户层鉴权不拦)。修复动态 task 子路径 `/api/tasks/{id}/<op>` 的 RBAC 绕过缺陷。

### Added

- **per-user RBAC 强制** (`server/master_server.py`) — GAP-8 Phase F2
  - `_resolve_actor` / `_user_token_role` / `_enforce_user_rbac`: 用户令牌按 `check_user_path_access` 鉴权;
    集群令牌无 `user_role` → 跳过用户层, 落 node-RBAC (内部可信)
  - `submit_task` / `cancel_task` / `degrade_task` / `migrate_task`: 用户令牌 → per-user RBAC;
    `task.user=已认证 user_id` (忽略客户端 `req.user`, 防伪造审计 actor)
  - 审计 `actor=已认证 user_id`; VIEWER/USER 越权 → 403 + 审计 `permission_deny`
- **用户管理 CRUD** (`server/master_server.py`) — 仅 ADMIN (`user:manage` 权限)
  - `POST /api/v1/users` (建用户), `GET /api/v1/users[/{id}]` (列表/详情, 不返哈希/salt)
  - `DELETE /api/v1/users/{id}` (删, 拒自删), `PUT /api/v1/users/{id}/role` (改角色)
  - `POST /api/v1/users/{id}/tokens` (签发, 明文仅此一次返回), `DELETE /api/v1/users/{id}/tokens/{tid}` (吊销)
  - `POST /api/v1/users/{id}/tokens/rotate` (轮换, 旧令牌保留多活)
  - 集群令牌调用户管理 → 403 (须 ADMIN 用户令牌); 无 user_store → 503
  - 令牌明文不进审计日志 (防日志泄露); `is_safe_path_segment` 守卫 user_id
- **请求模型**: `UserCreateRequest` / `UserTokenIssueRequest` / `UserRoleUpdateRequest`

### Fixed

- **RBAC 动态 task 子路径绕过** (`security/permission.py`): `check_user_path_access` 前缀匹配够不到
  `/api/tasks/{task_id}/<op>` (op 在尾部非前缀); 补 task 父路径 + 尾部 op 联合判定
  (cancel/migrate/degrade), 否则 VIEWER 可绕过 cancel 鉴权

### Tests

- `tests/test_user_rbac.py` (12): USER 提交 OK / VIEWER 只读 403 / migrate·degrade 仅 ADMIN /
  `task.user`=已认证非伪造 / 审计 actor=已认证 / 集群令牌路径不变
- `tests/test_user_crud.py` (17): 建查删改/签发吊销轮换 / 非 ADMIN 403 / 集群令牌 403 /
  令牌明文不入审计 / 持久化跨重启 / 自删拒绝 / 503 无 store
- 全量 1141 passed (F1 基线 1112 + F2 29)

## [0.10.3] - 2026-08-27 — GAP-8 Phase F1: per-user token store + dual-token middleware

> **多租户令牌基座**: 引入 per-user API 令牌 (与集群共享令牌正交), BearerAuthMiddleware 按 `fmu_`
> 前缀分流; 用户令牌存储文件持久化 (scrypt 哈希, 0600); UserRole (ADMIN/USER/VIEWER) + 用户层
> 路径鉴权。单租户零配置向后兼容 (无 users.json → 纯 cluster_token, 字节级旧行为)。

### Added

- **用户令牌存储** (`security/user_store.py`) — GAP-8 Phase F1
  - `UserStore`: 文件持久化 `~/.fusion/multi-node/users.json` (FUSION_USERS_FILE 覆盖), 原子 tmp+replace, 0600
  - 密钥只存 scrypt 哈希 (`hashlib.scrypt`, stdlib, 无新依赖); 明文令牌仅签发时返回一次
  - 令牌格式 `fmu_<userid>_<secret>`; 多活令牌 (rotate 签新不废旧, revoke 吊销)
  - `create/delete/list/issue/revoke/revoke_all/rotate/validate/set_role/bootstrap_admin`
  - `load_user_store()` — 无 env 无文件 → None (中间件回退纯 cluster_token); 有 → UserStore
- **UserRole** (`security/permission.py`) — 与 NodeRole 正交的用户层角色 (ADMIN/USER/VIEWER)
  - `_USER_ROLE_PERMISSIONS` + `_USER_PATH_PERMISSION_MAP` + `check_user_path_access(role, path, method)`
  - ADMIN: 用户管理 + 任务全操作; USER: 任务提交/取消/查询 + 推理; VIEWER: 只读
- **双令牌中间件** (`utils/auth.py` `BearerAuthMiddleware`)
  - `user_store` 可选参数: 注入后 `fmu_` 前缀 → UserStore.validate → 注入 scope user_id/user_role
  - cluster_token 路径 O(1) 不变; 无 user_store 时 `fmu_` 显式拒 (集群内部流量不携带用户令牌)
  - 鉴权失败审计 detail 区分 (用户令牌校验失败 / 不可用于节点路由 / token 不匹配)
- **首启引导** — `FUSION_BOOTSTRAP_ADMIN` env: 无用户库时自动创建 ADMIN 并签发首个令牌 (仅记日志, 令牌不回显)
- **`security/__init__.py`** re-export UserRole/UserStore/UserRecord/UserToken/check_user_path_access/load_user_store

### Backward compatibility

- 无 `FUSION_USERS_FILE` 且无 `~/.fusion/multi-node/users.json` → `load_user_store()` 返回 None →
  中间件纯 cluster_token, 与旧版字节级一致。单租户零配置部署无任何行为变化。
- 集群内部 HTTP (master→agent 派发, agent 心跳, KV 跨节点, CLI) 仍用 cluster_token, 不受影响。

### Tests

- `tests/test_user_store.py` (22 用例): 创建/删除/角色/签发/校验/吊销/轮换/多活/持久化/原子写/损坏降级/空库/bootstrap
- `tests/test_enterprise_security.py::TestUserTokenAuth` (6 用例): fmu 通过/错误 401+审计/cluster 不变/agent 拒 fmu_/无 store 回退/bootstrap env
- `tests/conftest.py`: 清 FUSION_USERS_FILE/FUSION_BOOTSTRAP_ADMIN (隔离 HOME 下无 users.json → None)
- 1112 tests (was 1085), 0 ruff errors

## [0.10.2] - 2026-08-26 — GAP-5 dead-code remediation

> **死代码清理/标注**: autoscaler 未接线路由由歧义 `{"enabled":False}` 改为显式 503 not-wired;
> StandbyMaster 死代码删除 (零实例化, 独立于已接线的 MasterElection)。模块保留待迁移。

### Changed

- **autoscaler 路由显式 not-wired** (`server/master_server.py`) — GAP-5 审计 §7
  - 旧 `GET /api/v1/autoscaler/config` 返回 `{"enabled": False}` — 歧义 ("禁用" vs "未实现")
  - 改为 503 + detail 明示未接线 (`Autoscaler 未接线 (not-wired): 模块存在但未实例化`)
  - `PUT /api/v1/autoscaler/config` 同步由 404 改 503 not-wired
  - 模块 (`autoscaler/`) 保留待迁移 (非生产路径, 非云合规债)

### Removed

- **StandbyMaster 死代码** (`master/cluster_master.py`) — GAP-5 审计 §7
  - 零生产实例化, 零 import (除 `master/__init__.py` re-export), 零测试, 零 CLI/server 引用
  - 独立于已接线的 `MasterElection` (P4 HA + GAP-1 全状态同步的实际路径)
  - 删除 class + `master/__init__.py` re-export; `__init__.py` 模块 docstring 更新
  - 现 HA 路径唯一: `MasterElection` (单 Master `_election is None` 无 HA; 多 Master `ha_config` 显式启动)

### Fixed

- **autoscaler 路由歧义** (GAP-5 审计 §7) — `enabled:False` 改显式 503 not-wired, 避免误读为已接但关闭

### Tests

- `tests/test_master_server.py::TestMasterServerAutoscalerNotWired` (2 用例): GET/PUT 未接线 → 503 + detail 含 not-wired
- 1085 tests (was 1083), 0 ruff errors

## [0.10.1] - 2026-08-26 — GAP-6 throughput cap + client-side pacing

> **限流适配补齐**: 上游 fusion-mlx #635 已修 (PR #637, `--rate-limit 0` 真正关闭限流, 默认关);
> 本版补客户端 429 退避重试 + master 限流归类修正 — 健康节点被限流不再误 ban。

### Added

- **GAP-6 客户端限流适配** (`agent/rate_pacer.py`) — 补 fusion-mlx 429 限流处理
  - `dispatch_with_pacing(send_request, pacer)`: 包 HTTP 发送, 429 时读 `Retry-After` 头, 指数退避 sleep, 在 `budget_seconds` 预算内重试; 非 429 (含 5xx/401) 原样返回不重试
  - `PacerConfig` dataclass (确定性无 jitter, Rule 5): `max_retries=3`, `initial_backoff=0.5`, `max_backoff=5.0`, `budget_seconds=10.0`, `next_backoff(attempt)=min(initial*2^attempt, max)`
  - `parse_retry_after(resp)`: 秒数 / HTTP-date / 缺失回落 1.0s / 非法回落 1.0s / 负数 clamp 0.0
  - `RateLimitExhausted` 异常: 预算耗尽仍 429 → 上抛, 带 `last_status`/`retry_after`/`attempts`
  - `FusionMLXBackend.__init__` 加 `pacer: PacerConfig | None` 参数; `chat()`/`embed()` 经 `dispatch_with_pacing` 包裹 (不再直接 `raise_for_status`)
  - `_execute_inference`/`_execute_embedding` catch `RateLimitExhausted` → 返回 `{"error":..., "rate_limited": True, "node_id":...}` (标记限流瞬时失败)
  - **缺陷链 (修前)**: 429 → `raise_for_status` 抛 `HTTPStatusError` → agent 包 `{"error":...}` → master `_dispatch_data` `"error" in r` → `logic_fail=True` + `report_fault("agent_internal_error")` → 3 故障/60s → **健康限流节点 ban 300s** (误判: 限流是瞬时, 非逻辑错误)
  - `tests/test_rate_pacer.py` (14 用例): 退避确定性 / Retry-After 解析 / 429 重试到成功 / 429 耗尽抛 / budget 截断 / 5xx 不重试 / backend chat 429 退避到成功 / 耗尽抛 RateLimitExhausted

### Changed

- **master 限流归类修正** (`master/cluster_master.py` `_dispatch_data` / `_dispatch_pipeline`) — GAP-6
  - `_dispatch_data` 新增分支 (置于 `"error" in r` 之前, 因 rate_limited dict 亦含 "error" key): `r.get("rate_limited")` → `transient_fail=True`, 不进 `logic_fail`, **不调 `report_fault`**, 不累加熔断器故障计数, 不 ban
  - `_dispatch_pipeline` 同理: rate_limited → `_finalize_task(success=False, retryable=True)`; Exception 分支亦改 `retryable=True` (原不可重试)
  - **效果**: 限流节点故障计数保持空, `is_node_banned` 恒 False, 健康节点限流不再拉黑
  - `tests/test_dispatch_integration.py::TestRateLimitedDispatch` (2 用例): 单节点限流 → FAILED 不 ban + fault_counts 空; 一健康一限流 → PARTIAL + 限流节点不 ban

### Fixed

- **健康限流节点误 ban** (GAP-6 审计 §7) — 429 限流归类为 `transient_fail` (可重试) 而非 `logic_fail`, 跳过 `report_fault`, 不进熔断窗口

### External

- **上游 fusion-mlx #635 CLOSED** (2026-08-25, PR #637 `fix(auth): --api-key on --model-dir path + --rate-limit 0 disables limiter (#636, #635)`): `--rate-limit 0` 真正关闭 60rpm 限流器, 默认即关; 显式设上限值时仍返 429 → 由本版客户端退避吸收

## [0.10.0] - 2026-08-26 — GAP-1 always-on SLA

> **企业级 HA 补齐**: 多 Master 全状态同步落地, standby 持有完整集群拓扑, leader 宕机后立即接管调度。
> HA 仍 opt-in (单 Master 部署不变), 2+ Master 显式配置即获 always-on (空窗 ≤ 选举超时 ~10s)。

### Added

- **GAP-1 HA 全状态同步** (`master/cluster_master.py` / `server/master_server.py`) — 补齐 always-on SLA
  - 原 HA (v0.8.3) 仅同步 tasks; Master 宕机后 standby 缺 nodes/kv/banned → 须等节点重注册才能调度, 不满足 always-on
  - 扩展同步范围: leader 周期推 **nodes + kv_cache + banned_nodes** 到 standby, standby `receive_synced_state` 幂等合并
  - `_node_to_dict`/`_node_from_dict` (NodeInfo 序列化, status 枚举 ↔ 字符串) + `_kv_to_dict`/`_kv_from_dict`
  - `_build_state_sync_targets` (自带 nodes→kv 两锁分别快照, 不嵌套) + `_push_sync_state_to_standbys` (锁外异步 best-effort)
  - `_state_sync_loop` (5s 周期) 接 `start(ha_config=)` 启动 / `stop()` 取消, 仅 leader 推送
  - 新端点 `POST /api/ha/sync-state` — standby 接收全状态, 返回 `{"status":"ok","counts":{"nodes":N,"kv":K,"banned":B}}`
  - **锁序**: nodes→kv (声明顺序), `receive_synced_state` 两域分别持锁不嵌套 (与 `find_kv_cache` P1-12 一致)
  - **ban 合并**: 取较晚解封时间 (leader/standby 任意一方 ban 更权威); 过期 ban 不合并
  - **HA 仍 opt-in**: 单 Master (`_election is None`) 不启动同步循环, `_build_state_sync_targets` 返回空, 行为不变
  - **failover 语义**: standby promote 为 leader 后已持同步来的 nodes/kv/banned, `assign_task` 立即可派发, 无空窗
  - `tests/test_ha_election.py::TestHAStateSync` (6 用例): 拓扑同步到达 / 幂等合并 / failover 立即调度 / 端点 round-trip / 单 Master 无目标 / 非法 status 回退 OFFLINE
  - `docs/HA-CRASH-RECOVERY.md` 补 "多 Master HA + 全状态同步" 章节 (同步内容表 / 启用配置 / 故障转移链路)

### Changed

- `pyproject.toml` / `__init__.py`: 0.10.0rc1 → 0.10.0 (GAP-1 always-on = minor)
- `__init__.py` 模块文档: MasterElection 描述补 "GAP-1 全状态同步 + always-on"

## [0.10.0-rc.1] - 2026-08-26 — Release Candidate

> ⚠️ **RC 版本**: 企业生产商用前置披露补齐 + #31 重试节点规避。复审计 §8 发布条件 2/4/5 落地 (条件 1 CI 已于 v0.9.0 落地, 条件 3 mTLS 强制已落地 v0.9.0)。**非 GA** — GAP-1/6/5 企业残留 gap 仍未补齐 (见下 Phase C/D/E 计划)。

### Added

- **#31 重试节点规避: `exclude_nodes` 硬黑名单** (`server/master_server.py` / `master/cluster_master.py` / `master/task_spec.py`)
  - `TaskSubmitRequest` / `ClusterTask` / `TaskSpec` 新增 `exclude_nodes: list[str]` 字段, 经 `to_dict`/`from_dict`/`_task_from_dict` round-trip (H3 持久化跨重启保留)
  - `select_nodes`: LoadRouter 打分**前**过滤 `exclude_nodes` — 硬黑名单, 绝不回退到列表内节点。过滤后无候选 → 返回 `[]` + warning
  - `assign_task`: 透传 `task.exclude_nodes` 到 `select_nodes`
  - `_select_free_nodes_locked` (TOCTOU 补选): 补选同样遵守黑名单, 并发抢占补选不回退失败节点
  - `preferred_node_id` 保持软提示 (LoadRouter `preferred_bonus`), 与硬黑名单正交
  - **行为**: 重试时调用方把失败节点加入 `exclude_nodes` 重提, 调度选不同健康节点; 全部规避 → 入优先级队列 (P1-H) 而非派发黑名单节点。打破"重试回同一坏节点"死循环
  - `tests/test_cluster_master.py` (5 用例): 黑名单过滤 / 全规避空候选 / preferred 软提示 / 端到端 assign_task 透传

### Fixed

- **GAP-4 CI 工作流修复** (复审计 §8 条件 1 补齐, v0.9.0 引入的 latent 缺陷)
  - `pyproject.toml` `[test]`: 声明 `pytest-randomly>=3.15.0` — CI 跑 `pytest -p randomly` 但未声明依赖, 仅本地 venv 有, 全新 CI 安装 `ImportError: No module named 'randomly'`
  - 3 个 Linux x86_64 runner 不兼容测试 skip-gate (Apple Silicon 目标项目, CI=ubuntu-latest):
    - `tests/test_sandbox_executor.py::test_execute_in_sandbox_timeout`: unshare 需 CAP_SYS_ADMIN, CI 无权限 → 运行时 probe + skip
    - `tests/test_core.py::test_collect_hardware`: 断言 `arch == arm64`, 非 darwin → skip
    - `tests/test_real_network_e2e.py::test_container_cross_register_and_dispatch`: docker-compose 需 `FUSION_MLX_API_KEY` env, CI 无 → 扩展 skip-gate 要求该 env (CI 不跑真推理 E2E)

### 补披露 — 复审计 §8 发布条件 2/4/5 (商用前置声明)

> v0.9.0 判定 ⚠️ CONDITIONAL-READY。本 RC 补齐 3 项披露条件, 使单租户 LAN 场景可**带条件**商用。多租户/远程 SaaS + always-on SLA 仍阻塞 (见下)。

- **条件 2 — GAP-1 HA SPOF 披露**: 默认单 Master 部署, Master 宕机集群不可用。`MasterElection` 选举为 opt-in (`start(ha_config={...})`), standby 仅同步任务 (不同步 nodes/kv/banned-set)。**不满足 always-on SLA**。生产 always-on 须: HA 默认开 + standby 全状态同步 (Phase C 计划)。当前适用: 可容忍短时不可用的单租户 LAN。
- **条件 4 — GAP-6 吞吐上限声明**: 上游 fusion-mlx issue #635 — `--rate-limit 0` 不真正禁用 60rpm 限流器, 多节点高 QPS 压测被限。**单节点推理吞吐受上游 60rpm 限制**, 多节点线性扩展但单节点不突破。客户端高 QPS 须适配 429 (Phase D 计划: 客户端节流 + 文档声明)。
- **条件 5 — GAP-5 死代码 + GAP-7 KV no-op 披露**:
  - **GAP-5 死代码**: `autoscaler/` 路由静默返回 `enabled: False` (语义模糊); `mcp_gateway/` 未接线死代码 (待迁移 fusion-gateway #106); `cloud_fallback.py` 调度路径 v0.8.2 已切断 (模块+单测保留供独立验证, 待迁移); `StandbyMaster` 未接线死代码 (现网单 Master 无 HA)。Phase E 计划: 标注未实现 / 迁移 / 清理。
  - **GAP-7 张量 KV no-op**: `ClusterMaster.sync_kv_cache` 返回 False (仅元数据同步, 非张量)。上游 fusion-mlx 无 KV 张量导出端点 (issue #650 已提, 阻塞 #33)。跨节点 KV 张量复用不可用; 当前 KV 仅本地预热 (`/api/kv/warm` 本地 `store_local`)。

### 多租户 / 远程 SaaS 阻塞声明 (GAP-8 残留)

- v0.9.0 修复审计日志 + 权限强制默认开, **但单一共享 Bearer token** (`~/.fusion/multi-node/.cluster_token`) 无 per-user RBAC。多租户/远程 SaaS 场景**不可用** — 须替换为多用户鉴权 + token 轮转 (Phase F 计划)。当前适用: 信任的单一运维团队的局域网。

### Changed

- `pyproject.toml` / `__init__.py`: 0.9.0 → 0.10.0rc1 (RC, 商用前置披露 = minor pre-release)
- `pyproject.toml` `[test]`: 加 `pytest-randomly>=3.15.0`

## [0.9.0] - 2026-08-26

### Added

- **企业级商业生产发布阻塞项修复** (复审计 2026-08-26 GAP-2/GAP-4/GAP-8) — 安全态势升级
  - **GAP-2 mTLS fail-closed** (`security/mtls.py`): 旧实现 mTLS 开启但证书路径不全时静默回退明文 (fail-open), 默认部署零节点身份校验。改 fail-closed — `server_ssl_context()` / `client_ssl_context()` / `server_ssl_kwargs()` / `client_kwargs()` 开启但证书不全 → raise `RuntimeError` 拒绝回退明文。新增 `certs_available()` helper。mTLS 关闭行为不变 (明文合法)。
  - **GAP-8 审计日志** (`security/audit_log.py` 新模块): `AuditLogger` 追加写 JSONL 到 `~/.fusion/multi-node/audit.log`, 字段 ts/actor/action/path/method/node_id/result/detail。模块级单例 `get_audit_logger()`, 线程安全 (threading.Lock), 写失败降级 warning 不拖垮主路径。路径经 `FUSION_AUDIT_LOG` env 可覆盖。接入安全动作点: `BearerAuthMiddleware` 鉴权失败 (auth_fail) / agent 权限拒绝 (permission_deny) / master 节点注册 (register ok/denied) / 审批通过 (approve) / 审批拒绝 (reject) / 任务提交 (task_submit) / 任务取消 (task_cancel)。`BearerAuthMiddleware` 加 `audit_logger` 参数。
  - **GAP-8 权限强制校验默认开** (`server/agent_server.py`): 旧 `_permission_enforce` 仅随 mTLS (默认关)。改读 `FUSION_PERMISSION_ENFORCE` env (默认 "1"=开), 缺 X-Node-Id → 403 (生产零信任)。mTLS 开亦强制。测试隔离: `tests/conftest.py` autouse 设 `FUSION_PERMISSION_ENFORCE=0` 回退兼容模式 (现有 http 测试 AUTH_HEADERS 无 X-Node-Id 须放行)。
  - **GAP-4 CI 工作流** (`.github/workflows/ci.yml` 新增): ruff check + pytest (random order + 固定 seed 双跑, 捕获测试隔离回归)。Gates 所有 release。`FUSION_PERMISSION_ENFORCE=0` 隔离。
  - `tests/test_enterprise_security.py` (21 用例): mTLS fail-closed (8) / AuditLogger (6) / 鉴权失败审计 (2) / 权限强制默认 (3) / master 路由审计 (2)。

### Changed

- `security/__init__.py`: 导出 `AuditLogger`
- `pyproject.toml` / `__init__.py`: 0.8.9 → 0.9.0 (安全态势升级 = minor)
- `tests/conftest.py`: autouse fixture 加 `FUSION_PERMISSION_ENFORCE=0` + `FUSION_AUDIT_LOG` 隔离 + `reset_audit_logger()` 每测试重建单例

## [0.8.9] - 2026-08-26

### Fixed

- **测试隔离缺陷修复** (复审计 2026-08-26 发现, Rule 9/12 违规) — 测试污染真实 `~/.fusion/multi-node`
  - `tests/conftest.py` (新增): autouse `_isolated_home` fixture 重定向 HOME 到 per-test tmp_path, 隔离 tasks.json/config.json/kv_cache.json/election_state.json 所有 `~/.fusion` 写入; symlink 真实 `~/.docker` 保 docker compose 插件发现 (容器 E2E 不受影响)
  - `fusion_multi_node/cli.py`: 模块级 `_config = ClusterConfig()` (导入即解析 `Path.home()`, 缓存真实路径, HOME 重定向无法覆盖) → 懒加载 `_get_config()`, 首次访问才实例化; 14 处读取点全部更新
  - 根因: H3 任务持久化写非终态任务到真实 tasks.json + CLI 导入即实例化; 污染源 `TestPriorityQueue::test_cancel_running_drains_queue` 留 RUNNING 任务; 随机顺序下 order-dependent 失败, 确定性顺序 1036 绿掩盖 bug
  - 验证: `pytest tests/ -q` 随机顺序 1036 passed, ruff clean, 真实 `~/.fusion/multi-node` 全程未碰

### Added

- **复审计报告** `docs/audit/RE_AUDIT_2026-08-26.md` — v0.8.8 对照原审计 13 CRITICAL + 29 项证据复核; 判定 ⚠️ CONDITIONAL-READY (原 ❌); 8 项企业级残留 gap + 5 发布条件; P0 8/8 P1 10/10 P2 8/8 P3 2/3 (P3-28 张量 KV no-op issue #33 已知限制)

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
- **S4 真实模型集成测试覆盖** (#73) — DATA 并行 E2E 真推理 + KV 共享 E2E 真 ASGI 路由链
  - `tests/test_data_parallelism_e2e.py` (1 用例): skip-gate `_mlx_alive() and _model_available()` (查 `/v1/models` 列表含模型 id), fusion-mlx 停则跳过; 2 节点 DATA 并行真推理 (`mlx-community-Llama-3.2-1B-Instruct-4bit`), 断 COMPLETED / node_count==2 / 两节点各返非空 content+usage
  - `tests/test_kv_sharing_e2e.py` (4 用例): 合成 KVCacheEntry 验跨节点 HTTP 路由链 (非模型张量, 无 skip-gate) — 同节点 store→lookup round-trip / 未命中 404 / warm 跨节点推送 / stats 路由; `PortRoutingTransport` 按 URL 端口路由到 ASGI (无真 TCP), manager `_get_http_client` monkeypatch 重写 `:11458` 端口
  - 覆盖原 17/24 单元 mock 文件未触达的 agent_server KV 路由端到端

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
- **FusionMLXBackend `/v1/*` 漏带鉴权头** (#73, S4 E2E 暴露) — `chat`/`embed` POST `/v1/chat/completions`、`/v1/embeddings` 未带 `Authorization`。fusion-mlx 启用 api_key 时 `/v1/*` 同样受保护 (与 `/distributed/*` 同源), 漏带一律 401。补 `headers=self._dist_headers()`。生产缺陷: 任何启用 auth 的 fusion-mlx 推理直接 401
- **KVSharingManager 跨节点 HTTP 漏带鉴权头** (#73, S4 E2E 暴露) — `lookup_remote`/`transfer_from_remote`/`warm_cache` POST 对端 agent `/api/kv/*` 未带 `Authorization`。对端 `BearerAuthMiddleware` 默认鉴权, 缺 token 全部 401。`KVSharingManager` 加 `cluster_token` 参数 + `_auth_headers()`, `AgentServer.__init__` 透传 `shared_token`。生产缺陷: 跨节点 KV 共享在鉴权 agent 上全部 401
- **KVWarmRequest 契约错配 + kv_warm 路由递归** (#73, S4 E2E 暴露) — schema 要求 `prompts: list[str]` (复数必填) 但 `warm_cache` 发 `{model_name, prompt, prompt_hash}` (单数) → 422; 且 `/api/kv/warm` 路由回调 `self.kv_manager.warm_cache` (二次跨节点远推 → 递归)。改 schema 为 `{model_name, prompt, prompt_hash, total_tokens, total_size_bytes}`, 路由只本地 `store_local` (跨节点分发归 `warm_cache`)

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

[0.10.2]: https://github.com/dahai80/fusion-multi-node/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/dahai80/fusion-multi-node/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.10.0-rc.1...v0.10.0
[0.9.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.8.9...v0.9.0
[0.8.2]: https://github.com/dahai80/fusion-multi-node/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/dahai80/fusion-multi-node/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.4.0...v0.8.0
[0.4.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dahai80/fusion-multi-node/releases/tag/v0.1.0

# Changelog — fusion-multi-node

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

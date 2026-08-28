# 运维 Runbook (Operations)

> P2-21 (审计 §6.9): 处置流程覆盖常见故障与运维操作。每节含 症状/诊断/处置/恢复验证。
> 部署模式见 `docs/DEPLOYMENT.md`, 崩溃恢复见 `docs/HA-CRASH-RECOVERY.md`。

## 诊断入口

| 检查项 | 命令 | 正常 |
|--------|------|------|
| Master 健康 | `curl -sH "Authorization: Bearer $TOKEN" http://127.0.0.1:11452/api/health` | `status: ok` |
| Master 深度健康 | `... /api/health/deep` | `status: ok` + node quorum |
| Agent 健康 (本机) | `curl -sH "Authorization: Bearer $TOKEN" http://127.0.0.1:11458/api/health/deep` | `status: ok` + fusion-mlx 可达 |
| 节点清单 | `... /api/nodes` | 在线节点 active |
| 任务清单 | `... /api/tasks` | 无大量 PENDING 堆积 |
| 进程 | `./start.sh status` / `launchctl list \| grep fusion-multi-node` | PID 在跑 |
| 指标 | `... /api/v1/metrics` (Prometheus 文本) | — |
| 告警 | `... /api/v1/observability/alerts` | 空 |

数据目录 `~/.fusion/multi-node/`: `tasks.json` (H3 任务持久化) / `election_state.json` (选举 term/voted_for) / `kv_cache.json` (KV 缓存) / `config.json` / `.cluster_token` (mode 0600) / `users.json` (多租户用户令牌, GAP-8 F1, scrypt 哈希) / `audit.log` (安全审计 JSONL, GAP-8)。
日志: `FUSION_MULTINODE_LOG_FILE` 指向文件 + RotatingFileHandler 10MB×5; 容器 `docker compose logs`。

## 节点下线 (node down)

**症状**: Master `/api/nodes` 某节点 `status: offline`; 派往该节点的任务超时重试。
**诊断**: `GET /api/nodes/{node_id}` 看 `last_heartbeat`; 离线 3600s 后 Master 自动清理。
**处置**:
1. 登录该节点: `./start.sh status` (nohup) 或 `launchctl list | grep fusion` (launchd)。进程死 → `restart` / `install-launchd`。
2. 进程在但 heartbeat 断 → 查 agent 日志 (网络/防火墙; 11458 端口可达性)。
3. 节点硬件故障不可恢复 → Master 自动隔离 (心跳超时), 已派任务超时重派其他节点 (`_enqueue_retry`, 最多 1 次)。
4. 调度侧: `select_nodes` 已跳过 `is_node_banned()` 节点 (S1 任务级熔断, dispatch 失败累计 ban)。
**恢复验证**: `GET /api/nodes` 该节点回 `online` + `active_tasks` 正常。

## Master 下线 (master down)

**症状**: `11452` 不可达; 所有 agent 失去调度; CLI `cluster status` 超时。
**诊断**: `./start.sh status` 退出码 1; `logs/stderr.log` 含崩溃栈。
**处置**:
1. nohup 模式: `./start.sh restart`。
2. launchd 模式: KeepAlive 自动重启 (10s 节流); 若未重启查 `launchctl list` 退出码 + plist。
3. docker-compose: `restart:unless-stopped` 自动重启; `docker compose logs master`。
4. 启动后 H3 自动恢复: `_restore_tasks` 读 `tasks.json`, RUNNING→PENDING 重派, 不丢任务。
**恢复验证**: `GET /api/health` `ok` + `GET /api/tasks` 在途任务已重派。
**单 Master = SPOF**: launchd/docker 自愈仅本机崩溃, 整机宕机无 failover (除非启用多 Master HA, 见 DEPLOYMENT.md, 技术预览)。

## 脑裂 (split brain)

**症状**: 多 Master HA 模式下, 网络分区导致两个 leader。
**诊断**: `election_state.json` term 抖动增长; `/api/ha/vote` 高频; standby `/api/tasks/submit` 返 503 与 leader 冲突。
**处置**:
1. **单 Master 模式无脑裂风险** (默认, `_election is None`)。
2. 多 Master HA: 恢复网络分区 → 选举自动收敛 (Raft term 高者胜)。分区期间 standby 拒派发 (`assign_task` 返 False, submit 返 503) → 任务不双派。
3. 持续不稳 → 退回单 Master: 停所有 standby, 主 Master `start(ha_config=None)` (默认单 Master)。
**恢复验证**: `GET /api/ha/vote` 静默; 一个 leader `_is_leader=True`, 其余 standby。

## 磁盘满 (disk full)

**症状**: `GET /api/health` `degraded` (磁盘 <512MB); H3 落盘失败告警 `H3 任务持久化失败` (critical) + `task_persist_failed` 指标。
**诊断**: `df -h ~/.fusion/multi-node/`; `/api/v1/observability/alerts` 看持久化告警。
**处置**:
1. 清理: 容器 `docker compose logs` 占盘 → 已配 json-file 10MB×3 (P1-16); 日志文件 `FUSION_MULTINODE_LOG_FILE` RotatingFileHandler 10MB×5; 删旧归档。
2. `tasks.json` 过大 → 终态任务不落盘 (仅非终态), `_max_completed_tasks=1000` 内存上限; 必要时备份后清空 (见备份章节)。
3. `kv_cache.json` → `_max_kv_cache=500` 内存上限 + TTL 过期; `save()` 跳过期条目。
4. 清出空间后, 下次 `_persist_loop` (15s) 自动恢复落盘; 告警自清。
**恢复验证**: `GET /api/health` `ok`; 持久化告警消失。

## fusion-mlx 不可达

**症状**: Agent `/api/health/deep` `degraded` (fusion-mlx `/v1/models` 不可达); DATA 并行推理任务 FAILED `httpx.ConnectError`。
**诊断**: Agent 日志 `FusionMLXBackend` 连接错误; `FUSION_MLX_URL` 端口 (默认 11434, 本项目 config 默认 11432 — **实测固化一方**, 见 CLAUDE.md 端口表)。
**处置**:
1. `~/claude-home/fusion-mlx/start.sh status` — 进程/端口/已载模型。
2. 停 → `start.sh start` (真实加载模型); 等待 `/v1/models` 返回模型列表。
3. api_key 不匹配 → `FUSION_MLX_API_KEY` 须与 fusion-mlx 启用 key 一致 (401 = 漏带/错 key)。
4. Master 侧: 推理失败任务 `report_fault` → 节点进熔断 ban 期, 不再派发; fusion-mlx 恢复后 ban 期过自动复派。
**恢复验证**: Agent `/api/health/deep` `ok` + `/v1/models` 含目标模型 id。

## 任务积压 (task backlog)

**症状**: `GET /api/tasks` 大量 PENDING; 派发延迟; agent `active_tasks` 打满。
**诊断**: `/api/v1/cluster/stats` 看 active/pending 计数; 各节点 `active_tasks` vs `max_tasks`。
**处置**:
1. 节点算力不足 → 扩容 agent (`docker compose up --scale agent=N` 或裸机新 Mac 加入)。
2. 单 agent 并发瓶颈 → 调高 `FUSION_AGENT_MAX_TASKS` (压测时)。
3. 租户配额限流 → `scheduling.tenant_max_concurrent` (热加载: `POST /api/v1/config/reload` 无需重启, P2-20)。
4. 优先级倒置 → 提交任务带高 `priority` (优先级队列, P1-H)。
5. 派往死节点卡住 → 任务超时 (`task.timeout_seconds`) 自动重试/FAILED; 确认节点熔断已生效。
**恢复验证**: PENDING 计数下降; 派发延迟回正常基线 (`tests/test_load_stress.py` 压测基线参考)。

## 版本升级 (upgrade)

**症状/场景**: fusion-multi-node 版本升级 (协议兼容: agent 版本 ≥ `MIN_COMPAT_PROTOCOL_VERSION` 0.8.0, 否则注册被拒 400)。
**处置**:
1. 滚动升级 (推荐, 零停机): 先升 agent (旧 master 兼容新 agent 注册), 再升 master。agent 重注册带 `protocol_version` (=`__version__`)。
2. 全量升级: 停 master → `git pull` + `pip install -e .` → `./start.sh restart` (H3 恢复在途任务)。agent 同步升。
3. 降级: 旧 agent 连新 master — 低于 `MIN_COMPAT_PROTOCOL_VERSION` 注册返 400 + 降级指引; 空串/非标准放行 + warn (灰度兼容)。
4. 配置迁移: `ClusterConfig.load()` 自动迁移旧端口 (`_migrate_stale_ports`) + 校验脏键回退默认, 无需手动改 `config.json`。
**恢复验证**: `GET /api/health` `ok`; 所有 agent `/api/nodes` `online`; `protocol_version` 一致。

## 备份与恢复 (backup/restore)

### CLI 备份 (v0.14.0, 推荐)

`fusion-multi-node backup` 命令组一键打包/恢复 `~/.fusion/multi-node/` 全量数据 (原子 tar.gz, 0600 权限):

```bash
# 备份 — 默认输出 ~/.fusion/multi-node/backups/mn-<时间戳>.tar.gz
fusion-multi-node backup create
# 自定义输出目录
fusion-multi-node backup create --out /data/backups/

# 恢复 — 停服后解包覆盖 (默认交互确认, --yes 跳确认)
./start.sh stop
fusion-multi-node backup restore --in ~/.fusion/multi-node/backups/mn-XXXXXX.tar.gz --yes
./start.sh start
```

**备份范围** (9 文件 + tls/ + kv/ 子目录):
- `config.json` — 集群配置 (端口/HA/scheduling/mTLS 段)。
- `tasks.json` — H3 任务持久化 (非终态 RUNNING/MIGRATED/PENDING)。
- `election_state.json` — 选举 term/voted_for (多 Master HA)。
- `rule_epoch.json` — 规则纪元/confirm 持久化 (v0.14.0, issue #52 guard 基线)。
- `kv_cache.json` — KV 缓存 (P1-9, 过期条目不恢复)。
- `users.json` — 多租户用户令牌 (GAP-8 F1, scrypt 哈希)。
- `audit.log` — 安全审计 JSONL (GAP-8)。
- `observability.jsonl` — 可观测指标/告警持久化 (v0.14.0)。
- `.cluster_token` — 集群共享密钥 (mode 0600) — **备份文件含明文 token, 0600 权限, 须妥善保管**。
- `tls/` — mTLS CA/叶证书 (v0.14.0 生产必配)。
- `kv/` — KV 张量分片 (GAP-7)。

**恢复语义**:
- `tasks.json`: RUNNING/MIGRATED → PENDING 启动重派; 终态不落盘。
- `election_state.json`: 单 Master 模式无影响; 多 Master HA 恢复投票状态防 term churn。
- `rule_epoch.json`: 恢复 guard 纪元基线, 不从 0 重查 (v0.14.0, 修重启归零/HA failover 从 0)。
- `.cluster_token`: **所有节点必须一致** — 恢复后须同步全集群 (见 Token 轮换), 否则节点间 401。
- restore 含**路径逃逸校验** (拒 tar 内 `..`/绝对路径), 损坏文件中止不部分写。

### 手动备份 (CLI 不可用时兜底)

```bash
BACKUP=~/.fusion/multi-node-backup-$(date +%Y%m%d)
mkdir -p "$BACKUP"
# 全量数据文件 (v0.14.0 范围)
cp ~/.fusion/multi-node/{tasks.json,election_state.json,rule_epoch.json,kv_cache.json,users.json,config.json} "$BACKUP/" 2>/dev/null || true
cp ~/.fusion/multi-node/{audit.log,observability.jsonl} "$BACKUP/" 2>/dev/null || true
cp -r ~/.fusion/multi-node/{tls,kv} "$BACKUP/" 2>/dev/null || true
# .cluster_token 单独安全备份 (mode 0600)
cp ~/.fusion/multi-node/.cluster_token "$BACKUP/" && chmod 600 "$BACKUP/.cluster_token"
```
**手动恢复**:
```bash
./start.sh stop
cp "$BACKUP"/{tasks.json,election_state.json,rule_epoch.json,kv_cache.json,users.json,config.json,audit.log,observability.jsonl,.cluster_token} ~/.fusion/multi-node/ 2>/dev/null || true
cp -r "$BACKUP"/{tls,kv} ~/.fusion/multi-node/ 2>/dev/null || true
chmod 600 ~/.fusion/multi-node/.cluster_token
./start.sh start   # H3 _restore_tasks 恢复在途任务; rule_epoch 恢复 guard 纪元
```

### mTLS 证书轮换 (v0.14.0 生产必配)

mTLS 开启后, 叶证书到期前轮换 (CA 3650 天, 叶 365 天):

1. 生成新叶证书: `mtls.provision_node(node_id, role, ca_cert, ca_key, ip=...)` (见 `docs/DEPLOYMENT.md`)。
2. 停节点: `./start.sh stop`。
3. 替换 `~/.fusion/multi-node/tls/node.crt` + `node.key`。
4. 启动: `./start.sh start` — `mtls.configure_from_config()` 读 config 段 (env 优先), fail-closed 校验证书路径齐全。
5. CA 轮换 (罕见): `mtls.provision_cluster()` 生成新 CA → 全节点重签叶证书 → 同步 ca.crt。须停机窗口 (全节点重签)。

## Token 轮换 (token rotation)

### 集群共享令牌 — 零停机滚动 (F5 `FUSION_CLUSTER_TOKEN_PREVIOUS`)

**场景**: 怀疑 cluster token 泄露 / 定期轮换。共享 Bearer token = 节点间唯一身份, 一处泄露全集群沦陷。

**F5 零停机流程** (重叠窗, 无 401 离线窗口, 推荐):
1. 生成新 token: `python -c "import secrets; print(secrets.token_urlsafe(32))"`。
2. **第一步 — 全节点先设 previous** = 当前旧值: `FUSION_CLUSTER_TOKEN_PREVIOUS=<旧值>` (env)。此时各节点入站仍只认 current=旧值, previous 窗尚未生效 (current 未变)。
3. **第二步 — 逐节点轮换 current**: 设 `FUSION_CLUSTER_TOKEN=<新值>` 并重启该节点 (`./start.sh restart` / launchd / `docker compose up -d`)。
   - 重启后该节点入站接受 **新值 (current) + 旧值 (previous)** — 重叠窗开启 (`BearerAuthMiddleware` 常量时间比较两值)。
   - 未重启节点仍持旧值 current, 对已轮换节点发出的 **新值出站请求** 暂时 401 (出站始终发 current, 见下)。
   - 故按 **先 master 再 agent** 顺序逐节点轮换, 保证派发链上游先就位。
4. **第三步 — 全节点轮换完毕**: 所有节点 current=新值。此时旧值仅经 previous 窗被接受。
5. **第四步 — 关闭重叠窗**: 全节点删 `FUSION_CLUSTER_TOKEN_PREVIOUS` (或置空) 并重启 → 旧值彻底失效。
6. 删旧 token 文件: `rm ~/.fusion/multi-node/.cluster_token` (env 接管后文件不再用)。

**出站语义**: `_get_dispatch_token` 读 `FUSION_CLUSTER_TOKEN` (current) — 出站请求始终发 current, **不发 previous**。故滚动期 "未轮换节点发出旧值 → 已轮换节点经 previous 窗接受", 反之 "已轮换节点发出新值 → 未轮换节点 401 直到它也轮换"。这要求按 master→agent 顺序轮换, 不可乱序。

**全停全启 (更简单, 须停机)**: 全节点停 → 设 `FUSION_CLUSTER_TOKEN=<新值>` (不设 previous) → 全启。无重叠窗, 无 401, 但有停机窗口。

**恢复验证**: `GET /api/health` 带新 token 返 200; 重叠窗内带旧 token 亦 200; 关窗后旧 token 返 401; 所有 `/api/nodes` `online`。

**加固**: 启用 mTLS (`FUSION_MTLS_ENABLED=1` + per-node cert) 减少单 token 依赖 — 见 README 安全边界表。

### 多租户用户令牌 (GAP-8 F1-F5)

**场景**: 多租户/远程接入 — per-user API 令牌 (`fmu_<userid>_<secret>`) 鉴权用户面路由 (master `/v1/chat/completions`、`/api/tasks/*`、`/api/v1/users/*`)。与集群共享令牌正交: 节点间内部流量不走用户令牌。

**首启引导 ADMIN**:
1. 首次启动设 `FUSION_BOOTSTRAP_ADMIN=admin` (env) → MasterServer 无用户库时自动创建 ADMIN `admin` 并签发首个令牌。
2. 令牌明文 **仅日志显示一次** (`首启引导 ADMIN 用户已创建 ... 首个令牌已签发`), 妥善保存。后续用户管理走 API。

**用户管理 (ADMIN 令牌)**:
```bash
ADMIN_TOK="<首启令牌>"
# 创建用户 (role: admin|user|viewer)
curl -sX POST -H "Authorization: Bearer $ADMIN_TOK" \
  -d '{"user_id":"alice","role":"user"}' http://127.0.0.1:11452/api/v1/users
# 签发令牌 (明文仅返回一次)
curl -sX POST -H "Authorization: Bearer $ADMIN_TOK" \
  -d '{"label":"dev"}' http://127.0.0.1:11452/api/v1/users/alice/tokens
# 轮换令牌 (签新留旧, 多活 — 旧令牌仍有效, 须另调吊销)
curl -sX POST -H "Authorization: Bearer $ADMIN_TOK" \
  -d '{"label":"rotated"}' http://127.0.0.1:11452/api/v1/users/alice/tokens/rotate
# 吊销旧令牌 (tid 从 list_users 取)
curl -sX DELETE -H "Authorization: Bearer $ADMIN_TOK" \
  http://127.0.0.1:11452/api/v1/users/alice/tokens/<tid>
# 列用户
curl -sH "Authorization: Bearer $ADMIN_TOK" http://127.0.0.1:11452/api/v1/users
```

**用户令牌轮换 (多活)**: `POST /api/v1/users/{id}/tokens/rotate` 签发新令牌, **旧令牌保留有效** (多活, 客户端灰度切换无停机)。客户端切到新令牌后, ADMIN 调吊销旧令牌 (`DELETE .../tokens/{tid}`)。密钥只存 scrypt 哈希, 明文仅签发/轮换时返回一次。

**零配置向后兼容**: 无 `users.json` 且无 `FUSION_BOOTSTRAP_ADMIN` → `UserStore is None` → 中间件纯集群令牌路径, 单租户行为不变 (现有部署无需改动)。

**审计**: 所有用户管理动作 (create/delete/issue/rotate/revoke) + 鉴权失败 + 权限拒绝记 `audit.log` JSONL, `actor`=已认证 user_id (非客户端自报, 防伪造)。查审计:
```bash
tail -f ~/.fusion/multi-node/audit.log   # 或 FUSION_AUDIT_LOG 覆盖路径
```

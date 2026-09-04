# 部署指南 (Deployment)

> P2-19 (审计 §6.5): 本项目定位澄清 — local-first Apple Silicon MLX 集群, **非 K8s/云原生编排目标**。
> 部署方案围绕单机 + 可信 LAN 小集群设计, 不提供 Kubernetes/Helm chart。

## 定位与边界

fusion-multi-node 是 Fusion 生态的 **底层算力基座**: 将多台 Apple Silicon Mac 池化为分布式 MLX 推理集群。
设计前提:

- **100% 本地 / 离线** — 无云 API 调用, 无外部网络依赖。
- **Apple Silicon 单平台** — 依赖 MLX/Metal, 非跨架构容器编排目标。
- **可信 LAN 小集群** — 单机到几台 Mac, 非大规模数据中心。

适用场景: 个人多 Mac 工作站、实验室/团队共享算力、可信 LAN 协同推理。
**不适用**: 公网暴露、敌对网络、多租户隔离强需求、跨架构混部 — 这些需上层 (fusion-gateway 等) 处理。

## 部署模式对比

| 模式 | 进程管理 | 崩溃自愈 | 跨机 | 适用 |
|------|---------|---------|------|------|
| A. 单机 nohup | `./start.sh start` | 无 (手动拉起) | 否 | 开发/试用 |
| B. 单机 launchd | `./start.sh install-launchd` | KeepAlive 自动重启 | 否 | 单机生产 (Mac) |
| C. docker-compose | `docker compose up` | `restart:unless-stopped` | 是 (1 Master + N Agent) | 可信 LAN 小集群 |
| D. 多 Master HA (standby) | `start(ha_config={mode:standby})` | 选举故障转移 | 是 | **技术预览**, 非生产 SLA |
| E. Active-Active 双主 | `start(ha_config={mode:active-active})` | 双 master 均活跃 | 是 | v0.15.0, 无 Redis, 小集群 |

默认 = 单 Master。模式 B/C 是推荐生产路径。模式 D/E 可选叠加, D 经选举故障转移, E 双活无选举 (owner-wins 收敛)。

## 模式 A — 单机 nohup

```bash
cd /Users/dahai/fusion && source .venv/bin/activate
cd fusion-multi-node
./start.sh start    # Master nohup 后台, PID 文件 .fusion-multi-node.master.pid
./start.sh status
./start.sh stop
```

日志: `logs/stdout.log` + `logs/stderr.log` (nohup)。崩溃需手动 `restart`。适合本地开发快速试用。

## 模式 B — 单机 launchd 守护 (推荐, Mac 生产)

```bash
./start.sh install-launchd
# 渲染 deploy/*.plist → ~/Library/LaunchAgents/com.dahai80.fusion-multi-node.plist
# launchctl load: RunAtLoad 启动 + KeepAlive 崩溃自愈
launchctl list | grep fusion-multi-node
./start.sh uninstall-launchd   # 卸载
```

崩溃自愈闭环: launchd KeepAlive 重启 + H3 任务持久化恢复 (`_restore_tasks` 重派 RUNNING→PENDING)。
日志: `FUSION_MULTINODE_LOG_FILE` 指定文件 + RotatingFileHandler 10MB×5 (P1-16)。
详见 `docs/HA-CRASH-RECOVERY.md`。**与 nohup 二者择一**, install-launchd 检测 nohup 在跑会先转交。

## 模式 C — docker-compose 多机小集群

```bash
cp .env.example .env             # 填强随机 FUSION_CLUSTER_TOKEN + fusion-mlx api_key (P2-23)
docker compose up -d --scale agent=2
```

- 1 Master (暴露宿主 11452) + N Agent (容器 bridge IP 回连, 不占主机端口)。
- 推理引擎 fusion-mlx 跑裸机, 容器经 `host.docker.internal:11434` 回连。
- `--scale agent=N` 无上限扩容; agent 无 host 端口映射。
- `restart:unless-stopped` 容器级崩溃重启; json-file 日志轮转 10MB×3 (P1-16)。
- 凭据: compose 不带弱默认, 未设 `FUSION_CLUSTER_TOKEN`/`FUSION_MLX_API_KEY` 启动失败并提示 (P2-23)。
- 可信网段自动审批: `FUSION_AUTO_APPROVE_PATTERNS` (生产收紧到精确 CIDR)。

跨机真网络: 多台 Mac 各跑 agent, Master 统一调度。详见 E2E 测试 `tests/test_data_parallelism_e2e.py`。

## 多 Master HA (技术预览)

`start(ha_config={"enabled":True,"node_id":"m1","priority":1,"peers":[...]})` 启动选举:
- `MasterElection` Raft-simplified: leader 选举 + term/voted_for 持久化 (P0-1) + leader 心跳广播 + 任务快照推 standby。
- Standby (`_election 配置且非 _is_leader`): `assign_task` 拒派发, `/api/tasks/submit` 返 503。
- `StandbyMaster` 类为未接线死代码 (与已接线的 `MasterElection` 分离)。
- **v0.14.0**: `cluster start` 路径已修接线漏 (原 `node start` 带 ha_config, `cluster start` 漏), 两启动路径一致。规则纪元/confirm 持久化 (不再重启归零 / HA failover 从 0 起)。

**生产多 Master 配置示例** (`~/.fusion/multi-node/config.json`):
```json
{
  "ha": {
    "enabled": true,
    "node_id": "master-1",
    "priority": 10,
    "peers": [
      {"node_id": "master-2", "ip_address": "10.0.0.2", "port": 11452, "priority": 5},
      {"node_id": "master-3", "ip_address": "10.0.0.3", "port": 11452, "priority": 1}
    ],
    "state_sync_interval": 2.0
  }
}
```
各 Master 节点设不同 `node_id`/`priority`, `peers` 列全集群 Master (含地址, 裸字符串仅 node_id 不可达)。leader 崩溃 → 选举转移 → standby 接管调度。配合 launchd/docker 双 Master 部署, 每节点各跑一份。

**警告**: 技术预览, 非生产 SLA 验证。生产关键负载仍建议单 Master + launchd + 定期备份。

## Active-Active 双主 (v0.15.0, #63)

`ha.mode = "active-active"` — 两 master 同时活跃, 均接受任务提交 (无 standby 503), 无需 Redis。与上方 standby 选举模式**互斥** (`mode` 二选一):

- **不启动选举**: active-active 下 `_election` 留 `None`, `_is_leader` 留 `True` → standby 守卫放行, 双 master 均派发。
- **双向 peer-sync**: `_peer_sync_loop` (默认 `state_sync_interval=2.0s`) 推 nodes+kv+banned+epoch + 非终态任务到所有 `peers`, 两 master 各跑 = 双向收敛。
- **owner-wins**: `ClusterTask.owner_master` 标归属 master, 仅归属 master 派发, 对端持镜像 (`assign_task` 对非自有任务返 False)。最终一致, 无强线性一致 (小集群部署足够)。
- **任务 ID 唯一**: `master-1-<uuid>` 前缀 → 跨 master 不撞, agent P1-14 去重安全。
- **节点角色亲和 + drain**: `NodeInfo.role` (`worker`/`general`/`heavy`) + `ClusterTask.tier`; heavy tier 软亲和 heavy 节点 (+0.15)。`POST /api/nodes/{id}/drain` (CLI `cluster drain|undrain <id>`) 排除节点承接新任务 (in-flight 继续), 对端 master 节点不受影响。

**Active-Active 配置示例** (master-1, `~/.fusion/multi-node/config.json`):
```json
{
  "ha": {
    "mode": "active-active",
    "node_id": "master-1",
    "peers": [
      {"node_id": "master-2", "ip_address": "10.0.0.2", "port": 11452}
    ],
    "state_sync_interval": 2.0
  },
  "node": {
    "role": "general"
  },
  "parallel": {
    "pipeline_enabled": false,
    "pipeline_shard_roles": ["heavy"]
  }
}
```
master-2 镜像配置 (`node_id`/`peers` 互换)。两 master 各跑一份 (launchd/docker), 端口可同 11452 (不同主机) 或 11452/11453 (同机需隔离 HOME/端口)。

**流量分发**: multi-node 不做 active-active 流量分发 (100% 本地, 不引 Redis/LB)。Redis 配额 + tier 优先队列 + Traefik active-active LB 为 fusion-gateway 部署依赖 (fusion-gateway #159), 在 multi-node 之上做流量切分。

**drain 与维护**: `cluster drain <node_id>` → 节点不再接新任务, 等存量任务结束 → 停 agent → 维护 → `cluster undrain <node_id>` 恢复。drain 仅影响该 master 视图的新任务选择, 对端 master 节点照常服务。

## 集群 drain 健康门控 + supervisor 协调 (v0.16.0)

### drain --wait 健康门控契约 (issue #69)

`cluster drain <node_id> --wait [--timeout N]` 触发 drain 后轮询 `GET /api/nodes/{node_id}/drain-status`, 每 2s 一次, 至 `ready==true` 或超时。响应:

```json
{"draining": true, "in_flight": 3, "ready": false, "long_task_active": false}
```

- `ready` = `draining AND in_flight==0` — **supervisor 停服前等待的信号**。fusion-sv / 运维脚本应轮询此端点, ready 后再停止 agent 服务。
- `long_task_active` = 该节点存在 `timeout_seconds > drain.long_task_threshold_seconds` (默认 300s) 的 RUNNING 任务 — MVP refuse-long 信号: 长任务活跃时 ready 保持 false, --wait 会超时。**检查点迁移不在本 PR** (跨节点 KV 传输 #33 是未来路径); 当前长任务需手动 cancel 或等待自然结束。
- 超时退出码 1 (打印 `in_flight` + `long_task_active`), ready 退出码 0。

### supervisor 协调 (issue #73)

`SupervisorBridge` 经 agent 本地 shell-out 调 `fusion-sv <op> [svc]` (`subprocess.run`)。跨节点经 master HTTP 转发到对端 agent。

- **agent 路由** (本机): `GET /api/supervisor/status`, `POST /api/supervisor/{op}` (op ∈ status/drain/rollout/shutdown/backup, 可选 `svc` query)。
- **master 转发** (跨节点): `POST /api/nodes/{node_id}/supervisor/{op}`, `GET /api/nodes/{node_id}/supervisor/status` — SSRF 守卫 (`is_safe_peer_host` + `build_safe_url`)。
- **CLI**: `cluster supervisor <op> <node_id> [--svc S]`; `cluster rollout-node <node_id>` 驱动 drain → rollout 序列 (MVP 顺序, 非跨节点并行编排)。
- **离线安全**: `fusion-sv` 未安装 → `FileNotFoundError` → `{"available": false}`, 不崩溃, 推理路径不受影响。env `FUSION_SV_BIN` 覆盖二进制路径。
- **心跳**: agent 心跳带 `supervisor_available` (缓存 ping, 30s 节流), master 聚合进 `NodeInfo`, `/api/nodes` 含此字段。failover 触发由运维/gateway 读此字段 (Traefik circuit-breaker 为 gateway #159)。

### fencing token + 权威成员视图 (issue #72)

仅适用于 **standby/HA 选举模式** (有 quorum)。active-active (#63) 无选举, fencing token = 0, 永不拒绝。

- master 选举胜出 → `MasterElection.fencing_token` 单调递增, 随派发 header `X-Fencing-Token` + `X-Leader-Id` 传播。
- agent `execute_task` 跟踪 `self._last_fencing_token`; 收到更低 token → 拒绝 `{"error": "stale master (fencing token expired)", "fencing_rejected": true}` (分区愈合后过期 master 不再写入)。
- master 将 `fencing_rejected` 归为不可重试逻辑失败 (过期 master 不应重试)。
- `/api/nodes` 响应带 `cluster_view` (本 master 为 leader 或近期从 leader 同步) + `partitioned` (本 master 为无法达 quorum 的非 leader 少数派)。**客户端 (fusion-studio / gateway) 读 `partitioned` 对分区 master 全局禁写**。

## epoch/leader_id 暴露 + per-leader token (v0.17.0, issue #76 #77)

### epoch/leader_id 客户端契约 (issue #76)

集群 API 响应增量暴露领导纪元与当前 leader 标识, 客户端据此**确定性拒绝过期 leader 响应**, 替代客户端侧脑裂启发式 (跨轮询数 master 数)。

暴露字段 (`/api/nodes`、`/api/nodes/{id}`、`/api/cluster/stats`、`/api/v1/nodes`、`/api/v1/cluster/stats`):

| 字段 | 类型 | 含义 |
|------|------|------|
| `epoch` | int | 领导纪元 (Raft `current_term`, 单调递增)。HA standby → 选举 term; 单 master / active-active → `0` |
| `leader_id` | str | 当前 leader 标识。HA → 当选 leader node id; active-active → 本 master `_ha_node_id`; 单 master → `""` |
| `is_leader` | bool | 本 master 是否为 leader |
| `leader_token` | str | per-leader token (见下, 仅 `/api/cluster/stats` + v1 stats) |

客户端判定逻辑:
- `epoch == 0` 且 `leader_id == ""` → 单权威 (单 master), 无脑裂概念, 不拒。
- `epoch` 递增 → 新 leader 当选; 客户端缓存所见最大 epoch, 收到更小 epoch 的响应 → 视为过期 leader, 拒绝/重试。
- 仅增量字段 — 现有客户端忽略未知字段, 无需改动即可升级。

### per-leader token 过期写入拒绝 (issue #77, opt-in)

**仅 HA standby 模式 + env `FUSION_LEADER_TOKEN_ENFORCE=1` 生效**。单 master / active-active 永不拒绝, 离线默认不变。

- `leader_token()` = `HMAC-SHA256(集群 token, "{epoch}:{leader_id}")[:32]`, 复用现有共享集群 token (`FUSION_CLUSTER_TOKEN` / `.cluster_token`), 无新秘密、无云、离线安全。同一 epoch+leader_id 在所有 master 派生同一 token; failover (新 epoch) 派生不同 token。
- **`GET /api/leader/credentials`** 返回 `{epoch, leader_id, leader_token, is_leader, enforce}` (Bearer 鉴权不豁免)。客户端 failover 后取此端点刷新本地 token, 再发变更请求带 `X-Leader-Token: <token>`。
- submit (`/api/tasks/submit`、`/api/v1/tasks/submit`) + cancel (`/api/tasks/{task_id}/cancel`) 路由读 `X-Leader-Token`; enforce 开 + HA + header 存在且 ≠ 当前 `leader_token()` → **`409 LeaderChanged`** (warning + 审计 `leader_token_reject`)。
- **缺 header 仍放行** (灰度兼容 — 纵深防御: 不发 header 的客户端行为不变; 发了过期 token 才拒)。

启用 (仅 HA standby 部署):

```bash
export FUSION_LEADER_TOKEN_ENFORCE=1
```

客户端刷新流程:
1. 提交收到 `409 LeaderChanged` → 判定 failover 已发生。
2. `GET /api/leader/credentials` 取新 `leader_token`。
3. 后续变更请求带 `X-Leader-Token: <新 token>` 重试。

## 可选 fusion-identity 集成 (v0.16.0, issue #74)

**OPTIONAL — 默认离线不变**。fusion-identity 是 Fusion 生态的租户/鉴权服务 (签发 JWT + per-tenant 配额 + 用量)。multi-node 是它的可选客户端:

- **未设 `FUSION_IDENTITY_URL`** (默认): `get_identity_provider()` 返 `None`, 全部行为退回本地 `config.json` + `fmu_` UserStore。100% 本地/离线规则不破。
- **运维显式 opt-in** (设 `FUSION_IDENTITY_URL`): JWT 令牌经 `POST /api/v1/auth/verify` 校验, per-tenant 并发配额从 `/api/v1/admin/tenants/{tid}/quota` 拉取, 任务完成上报用量至 `/api/v1/tenants/{tid}/usage` (best-effort, 不阻塞调度)。identity 为权威 — **fail-closed**: opt-in 后 identity 不可达 → `verify_jwt` raise → 401 (不静默放行)。

**配置** (env, 不进 config.json):
```bash
export FUSION_IDENTITY_URL="http://10.0.0.5:11470"
export FUSION_IDENTITY_SERVICE_TOKEN="<service-token>"
```

**令牌共存**: JWT 令牌 (三段点分, 非 `fmu_` 前缀) 走 identity 校验路径; `fmu_` 令牌仍走 UserStore (不退役); 集群令牌走 cluster_token。三者并存。`BearerAuthMiddleware` 注入 `scope["user_id"]`/`["user_role"]`/`["tenant_quota"]`。

## mTLS 节点互信 (生产必配)

v0.14.0: mTLS **默认关** (测试兼容); 企业生产**必须显式开启** — 否则集群内 HTTP 无节点身份校验, 任何同网段主机可注册节点。

**1. 生成集群 CA (一次性, 各节点共享 ca.crt)**:
```bash
python -c "from fusion_multi_node.security.mtls import provision_cluster; print(provision_cluster())"
# → (/path/ca.crt, /path/ca.key)
```

**2. 各节点签发叶证书** (CN=node_id, O=role):
```bash
python -c "from fusion_multi_node.security.mtls import provision_node; print(provision_node('master-1', 'master', '/path/ca.crt', '/path/ca.key', ip='10.0.0.1'))"
# → (/path/node.crt, /path/node.key)  叶证书带 SAN (DNSName + IPAddress) 防 MITM
```

**3. config 段开启** (`~/.fusion/multi-node/config.json`):
```json
{
  "security": {
    "mtls": {
      "enabled": true,
      "ca_cert": "/tls/ca.crt",
      "node_cert": "/tls/node.crt",
      "node_key": "/tls/node.key",
      "node_id": "master-1",
      "node_role": "master"
    }
  }
}
```
启动时 `mtls.configure_from_config()` 把 config 段写回 env (env 优先兼容旧 env-only 部署)。**fail-closed**: `enabled=true` 但证书路径不全 → 启动 raise, 不回退明文 (GAP-2)。亦可经 env 直配 (`FUSION_MTLS_ENABLED=1` + `FUSION_MTLS_CA_CERT/NODE_CERT/NODE_KEY`), 见 `deploy/.env.example` + plist/docker env 透传。

证书轮换见 `docs/OPERATIONS.md`。

## 告警出站通道 (生产必配)

v0.14.0: 告警**默认仅留内存 deque** (需轮询 `/api/v1/observability/alerts`); 企业生产**须配 webhook** 接收节点掉线/内存告警推送。

config 段 (`~/.fusion/multi-node/config.json`):
```json
{
  "observability": {
    "alerts": {
      "webhook_url": "http://内网告警端点/alert",
      "webhook_timeout": 10.0
    }
  }
}
```
非空则 `_register_alert_webhook` 注册 fire-and-forget POST (httpx, to_thread 不阻塞告警链, 失败 logger.warning 不拖垮)。env 优先: `FUSION_ALERT_WEBHOOK_URL` 覆盖 config。**100% 本地** — webhook 指内网端点, 无云依赖。

## KV 跨节点传输 (生产可用)

合成 KV 跨节点传输**生产可用** (v0.11.0 起, issue #33 已闭合): `SyntheticKVTransport` 默认后端, 跨节点 HTTP 路由合成 KVCacheEntry, 不依赖上游 fusion-mlx 张量接口。`sync_kv_cache` 返回 True, 跨节点 warm/export/import 路由全通。

真实张量 transport (`MLXKVTransport`) 为 **env-gated 实验性 bonus** (`FUSION_KV_TENSOR_BACKEND=mlx`), 代码已写, 纯 env flip 当上游 fusion-mlx #650 落地, 404→degrade 优雅。生产用合成 KV 即可。

## 可观测持久化

v0.14.0: `observability.persist` **默认开** — 指标/告警/日志跨重启保留 (`~/.fusion/multi-node/observability.jsonl`, 限最近 N 条)。`_cleanup_loop` 周期 save (300s) 防崩溃丢 stop 后数据, stop 时最终落盘。关持久化: config `observability.persist=false`。

## 扩容与资源

- **水平扩容 (Agent)**: docker-compose `--scale agent=N` 或裸机各 Mac 跑 `fusion-multi-node node start --role agent`。
- **资源限制**: 容器未设 `mem_limit`/`cpus` (审计 §6.11 债), 裸机靠 OS 调度; `FUSION_AGENT_MAX_TASKS` 限单 agent 并发任务。
- **推理引擎**: fusion-mlx 单机裸机 (非容器), MLX 统一显存管理。

## 非目标 — 为何无 Kubernetes

本项目 **不提供** Kubernetes/Helm/Nomad manifests, 原因:

1. **平台绑定**: MLX/Metal 仅 Apple Silicon, 容器编排跨架构混部无收益。
2. **离线约束**: 100% 本地, K8s 控制面 + 镜像仓库引入外部依赖, 违定位。
3. **规模错配**: 设计为单机到几台 Mac 的小集群, K8s 面向大规模数据中心。
4. **运维成本**: launchd/docker-compose 已覆盖单机+可信 LAN 自愈, K8s 运维开销超出目标场景。

企业级多机编排 (跨集群/多租户网关/公网暴露) 属 fusion-gateway (Go) 职责, 非 fusion-multi-node 范围。
如需容器编排以外的部署形态, 在上层网关处理, 本项目保持算力基座纯本地。

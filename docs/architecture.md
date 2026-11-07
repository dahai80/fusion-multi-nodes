---
doc_id: fusion-multi-node/architecture
product: fusion-multi-node
class: core
entry: service
doc_type: architecture
title: {zh: "fusion-multi-node 架构", en: "fusion-multi-node Architecture"}
audience: engineer
status: draft
version: 0.20.5
updated: 2026-11-07
owner: "@dahai"
source_of_truth:
  - fusion_multi_node
  - README.md
  - docs/API.md
  - docs/DEPLOYMENT.md
---

# fusion-multi-node 架构

单 Master + N Agent 的星型拓扑，全部进程内通信走 HTTP（可选 FMP/protobuf 与 MCP），推理执行下沉到各节点的 fusion-mlx（`:11434`）。

## 模块图

```
fusion-studio (multiNodePort 11452)      fusion-web-shell
        |  start.sh 生命周期                     | /shell/fusion-multi-nodes/
        v                                       v
+--------------------------------------------------+
|  ClusterMaster  127.0.0.1:11452                  |
|  master/ : 节点表·调度·队列·幂等·降级链·drain      |
|  observability/ : 指标·告警·日志·Prometheus       |
|  server/ : /api/v1 路由 + RBAC                    |
|  security/ : mTLS·审计链·纪元·confirm 中继        |
|  storage/ : HA 持久化·任务快照                     |
+---------+----------------------------------------+
          | 注册/心跳/派发 (Bearer 集群令牌)
          v
+------------------+     +------------------+
| NodeAgent :11458 |     | NodeAgent :11458 |
| agent/ : 执行·上报| ... | discovery/ : mDNS |
+--------+---------+     +--------+---------+
         |  chat 代理/派发
         v
   fusion-mlx :11434（每节点本地）
```

## 模块职责（`fusion_multi_node/`）

| 模块 | 职责 | 关键约束 |
|------|------|----------|
| `master/` | 节点表、任务派发、优先级队列、租户并发、幂等、迁移/降级、drain、选举（`election.py`） | standby 非 leader 503；active-active owner-wins 最终一致 |
| `agent/` | 注册回连、心跳上报、任务执行、`_restore_tasks` 恢复 | 首注册需审批，`protocol_version` ≥ 0.8.0 |
| `discovery/` | mDNS 局域网发现（zeroconf） | 仅 LAN；`--no-mdns` 须手动 join |
| `protocol/` | FMP protobuf 传输（`:11446`）与 MCP 网关 | `--transport fmp` 才启用 FMP |
| `security/` | mTLS 互信、HKDF 密钥派生、HMAC 审计链、规则纪元 | mTLS fail-closed；不新增秘密，从集群令牌派生 |
| `observability/` | 指标/告警/日志、Prometheus 导出、持久化 jsonl | deque 持久化默认开，300s 周期 save |
| `storage/` | 任务/HA 状态落盘 | launchd 崩溃重启后恢复 RUNNING→PENDING |
| `distributed_mlx/` | Caveman 压缩与 KV 共享（`KVSharingManager`） | 依赖节点内 fusion-mlx |
| `config/` | `ClusterConfig`：默认合并、脏值修复、旧端口迁移 | 深拷贝默认，防 `set()` 污染类级配置 |
| `server/` | FastAPI 路由、RBAC（集群令牌/用户令牌） | typed `/api/v1/*`，遗留 `/api/*` 兼容 |
| `mcp_gateway/` | MCP 工具预算与超时 | `token_budget` 10M、`tool_timeout` 60s |

## 关键数据流

1. **注册**：Agent mDNS 发现或手动指定 master → `POST /api/v1/nodes/register`（带协议版本与硬件信息）→ 审批通过进入节点表，心跳 3s。
2. **提交**：消费者带令牌 `POST /api/v1/tasks/submit` → 幂等检查 → 租户并发/队列闸门 → 节点打分（内存、负载、角色亲和；heavy 任务软亲和 heavy 节点 +0.15）→ 派发或入队（202）。
3. **推理**：Agent 调本节点 fusion-mlx；chat 代理路径 `POST /v1/chat/completions` 由 Master 选单节点透传。
4. **故障**：心跳超时 15s 判离线；launchd/docker 负责进程级自愈；任务持久化恢复重派；模型降级链兜底（最多 2 次）。
5. **HA 同步**：standby 选举（term/voted_for 持久化 + leader 心跳 + 快照推送）或 active-active 双向 peer-sync（2.0s，nodes+kv+banned+epoch+非终态任务）。

## 部署形态

| 形态 | 进程管理 | 自愈 | 跨机 |
|------|----------|------|------|
| A nohup | `./start.sh start` | 手动 | 否 |
| B launchd | `./start.sh install-launchd` | KeepAlive + 任务恢复 | 否 |
| C docker-compose | `docker compose up` | `restart: unless-stopped` | 1 Master + N Agent |
| D/E HA | config `ha.*` | 选举/双活 | 多 Master |

## 上游与消费者

- **上游**：fusion-mlx（推理，`:11434`，端口归 fusion-gateway 段）；fusion-core（配置/日志/HTTP 底座）。
- **消费者契约**：fusion-agent-studio（9 操作 + 会话存储 + sandbox 委派头）、fusion-guard（audit chain / rules epoch / confirm）、fusion-studio（start.sh 生命周期）、fusion-supervisor（manifest 编排）。
- **不再需要消费者实现的**：节点选择、队列、幂等、审批、证书轮换外勤、指标导出、任务恢复。约束： autoscaler 契约固定 503（未接线）、pipeline 依赖上游 `/distributed/*`、HA 无生产 SLA。

## 设计取舍

- 星型而非对等：调度状态单点收敛，Master 是会话/纪元事实源；代价是 Master 需 launchd/HA 守护。
- HTTP 优先，FMP/protobuf 为可选传输：减少协议面；代价是高频小消息走 HTTP 开销。
- fail-closed 安全默认：mTLS 证书不全即拒启，不静默降级明文。

---
doc_id: fusion-multi-node/api-reference
product: fusion-multi-node
class: core
entry: service
doc_type: api-reference
title: {zh: "fusion-multi-node HTTP API 参考", en: "fusion-multi-node HTTP API Reference"}
audience: engineer
status: draft
version: 0.20.5
updated: 2026-11-07
owner: @dahai
source_of_truth:
  - docs/API.md
  - fusion_multi_node/server
  - fusion_multi_node/master
  - fusion_multi_node/cli.py
---

# fusion-multi-node HTTP API 参考

Master HTTP API 默认 `127.0.0.1:11452`。除豁免路径（`/api/health`、`/docs`、`/openapi.json`、`/redoc`、`/`）外均需 `Authorization: Bearer` 令牌头。OpenAPI 交互文档：`/docs`。

两类令牌：

- 集群令牌：节点间共享密钥，自动落 `~/.fusion/multi-node/.cluster_token`。
- 用户令牌：`fmu_<userid>_<secret>` 形态，仅用于 Master 用户面路由，解析出 ADMIN / USER / VIEWER 角色。

完整字段级契约见 `docs/API.md`；本文是常用面摘要。

## 集群控制契约（9 操作）

| 操作 | 方法 + 路径 | 说明 |
|------|-------------|------|
| list_nodes | `GET /api/v1/nodes` | 节点列表（16 字段/节点） |
| join_node | `POST /api/v1/nodes/register` | Agent 注册，需 `protocol_version` ≥ 0.8.0；400 非法 id/协议不兼容，403 已封禁 |
| remove_node | `DELETE /api/v1/nodes/{node_id}` | 移除节点，404 不存在 |
| submit_task | `POST /api/v1/tasks/submit` | 200 已派发 / 202 排队 / 503 HA standby 或队列满 |
| migrate_task | `POST /api/v1/tasks/{task_id}/migrate` | 迁移运行中任务 |
| degrade_task | `POST /api/v1/tasks/{task_id}/degrade` | 模型降级链 70b→1b，最多 2 次 |
| task_progress | `GET /api/v1/tasks/{task_id}/progress` | 进度 0.0–1.0、分片计数 |
| cluster_stats | `GET /api/v1/cluster/stats` | 集群/任务/负载汇总 |
| observability suggestions | `GET /api/v1/observability/suggestions` | 运维建议 |

submit 可选头：`X-Fusion-Target-Node` 硬钉节点（不重路由；404 未注册 / 409 排水中 / 400 冲突），`X-Fusion-Run-Id` 注册 run→task 索引（`GET /api/v1/runs/{run_id}` 查询）。

## 推理代理

`POST /v1/chat/completions` — OpenAI 兼容透传到选中节点的 fusion-mlx（DATA count=1）。`stream: true` 走 SSE。租户并发超 `scheduling.tenant_max_concurrent` 返回 429。

## 可观测性

| 方法 + 路径 | 说明 |
|-------------|------|
| `GET /api/v1/observability/alerts?severity=` | 当前告警（空 webhook 配置时仅内存 deque，需轮询） |
| `GET /api/v1/observability/logs/export?fmt=json\|csv` | 日志导出 |
| `GET /api/v1/metrics` | Prometheus 文本格式 0.0.4 |
| `GET /api/v1/nodes/{node_id}/metrics` | 单节点负载 |
| `GET /api/v1/tasks/{task_id}/timeline` | 任务事件时间线 |
| `GET /api/tasks/events` | SSE 任务状态流（15s keepalive） |

## 用户管理（仅 ADMIN）

`POST/GET /api/v1/users`、`GET/DELETE /api/v1/users/{user_id}`、`PUT /api/v1/users/{user_id}/role`、`POST /api/v1/users/{user_id}/tokens`（明文仅返回一次）、`DELETE .../tokens/{tid}`、`POST .../tokens/rotate`。

## 配置热加载与其他

- `POST /api/v1/config/reload` — 重读 `config.json` + 重应用租户并发；端口/HA/mDNS 改动需重启（响应带 `restart_required` 提示）。
- Guard transport（fusion-guard 消费）：`GET /api/v1/audit/chain`、`GET/POST /api/v1/rules/epoch(/advance)`、`POST /api/rules/epoch`、`POST /api/confirm`、`GET /api/v1/confirms`。
- 共享会话存储（agent-studio 跨节点 resume）：`GET/PUT/DELETE /api/v1/session-store/{session_id}`、`POST .../fork`、`GET /api/v1/runs/{run_id}`。
- Autoscaler：`GET/PUT /api/v1/autoscaler/config` — **固定返回 503**（模块未接线，GAP-5，已文档化的契约而非静默假配置）。
- 遗留 `/api/*` 路由（无 response_model）保留兼容：`/api/health`、`/api/nodes`、`/api/tasks/submit` 等；新集成一律用 `/api/v1/*`。

## CLI 与 HTTP 的对应

`fusion-multi-node cluster status` ≈ `GET /api/v1/cluster/stats`；`cluster approve` 调 `POST /api/nodes/approve`；`task submit` 走本地 Master 实例。完整 CLI 清单：`fusion-multi-node --help`。

## 消费者减负与约束

- 不必自写节点选择/队列/幂等（`X-Idempotency-Key` TTL 默认 86400s）；约束：队列满 503，幂等键过期即新建任务。
- 不必自建跨节点会话存储；约束：Master 单一事实源，session id 做路径段校验。
- 不必自管集群内审计链 MAC；约束：HMAC 密钥从集群令牌 HKDF 派生，不改则不增密。

---
doc_id: fusion-multi-node/config-reference
product: fusion-multi-node
class: core
entry: service
doc_type: config-reference
title: {zh: "fusion-multi-node 配置参考", en: "fusion-multi-node Configuration Reference"}
audience: engineer
status: draft
version: 0.20.5
updated: 2026-11-07
owner: @dahai
source_of_truth:
  - fusion_multi_node/config/config.py
  - .env.example
  - start.sh
  - docker-compose.yml
---

# fusion-multi-node 配置参考

配置文件：`~/.fusion/multi-node/config.json`（`fusion-multi-node config get/set/list` 读写；`POST /api/v1/config/reload` 热加载，端口/HA/mDNS 例外需重启）。校验失败或脏值回退内置默认，不写死进程。全部默认值定义在 `fusion_multi_node/config/config.py` 的 `DEFAULT_CONFIG`。

## cluster — 拓扑与心跳

| 键 | 默认 | 说明 |
|----|------|------|
| `cluster.name` | fusion-cluster | 集群名 |
| `cluster.master_host` | 127.0.0.1 | Master 绑定地址；误设 `0.0.0.0` 加载时回退 |
| `cluster.master_port` | 11452 | Master HTTP 端口（fusion-studio multiNodePort 对齐） |
| `cluster.discovery_port` | 11450 | 发现端口 |
| `cluster.agent_port` | 11458 | Agent HTTP 端口 |
| `cluster.mcp_port` | 11446 | MCP/FMP 端口 |
| `cluster.heartbeat_timeout` | 15.0 | 心跳超时（秒），超时判离线 |
| `cluster.heartbeat_interval` | 3.0 | 心跳间隔（秒） |

## node — 节点角色

`node.role`: `worker`（默认）/ `general`（通用推理）/ `heavy`（大模型/pipeline shard 亲和）。调度按角色亲和打分。

## ha — 高可用（默认关）

`ha.enabled=False`。开启选举需 `node_id`、`priority`、`peers`（含 ip_address+port 全地址）。`ha.mode`: `standby`（默认，非 leader 派发返回 503）或 `active-active`（双主均派发，owner-wins 收敛，2.0s peer 同步）。两种模式互斥。standby/active-active 均为技术预览，无生产 SLA。

## scheduling — 多租户调度

| 键 | 默认 | 说明 |
|----|------|------|
| `scheduling.tenant_max_concurrent` | 4 | 每租户并发上限，超额入优先级队列（0 不限） |
| `scheduling.max_pending_queue` | 1000 | 队列长度上限，满则 submit 503（0 不限） |
| `scheduling.idempotency_ttl_seconds` | 86400 | `X-Idempotency-Key` 幂等键存活秒数 |

## parallel — 并行模式

`default_mode=pipeline`、`pipeline_timeout=300.0`、`data_parallel_timeout=120.0`、`caveman_compress=true`、`communication=auto`。**`pipeline_enabled=False`（默认关）**：上游 fusion-mlx `/distributed/*` 未实现，开启前置条件未满足时 submit mode=pipeline 直接 400。`pipeline_shard_roles=["heavy"]` 指定 shard 派发角色。

## mlx — 上游引擎

`fusion_mlx_port=11432`、`fusion_kb_port=11434`、`fusion_desk_port=9000`、`model_hub_port=11435`、`fusion_mlx_api_key=""`（规范名 `FUSION_CLIENT_KEY`，env 优先）。

## security — PII 脱敏与 mTLS

`security.http_pii_scrub=False`（默认关，LAN 可信保留明文；跨不可信网段须开 + mTLS）。`security.mtls.*`：`enabled/ca_cert/node_cert/node_key/node_id/node_role` — 开启但证书路径不全时 fail-closed 启动报错，不回退明文。env（`FUSION_MTLS_*`）优先于 config 段。

## observability / session_store / mcp

- `observability.retention_hours=24.0`、`alert_enabled=true`、`log_level=info`、`persist=true`（落盘 `~/.fusion/multi-node/observability.jsonl`）、`observability.alerts.webhook_url=""`（空=仅内存 deque，须轮询告警接口）。
- `session_store.dir=""`：空 = env `FUSION_MULTINODE_SESSION_STORE_DIR` > 默认 `~/.fusion/multi-node/session-store`。
- `mcp.enabled=true`、`mcp.token_budget=10000000`、`mcp.tool_timeout=60.0`。

## 环境变量（start.sh / docker-compose）

| 变量 | 默认 | 说明 |
|------|------|------|
| `FUSION_MULTINODE_ROLE` | master | `start.sh` 角色 master/agent |
| `FUSION_MULTINODE_HOST` / `FUSION_MULTINODE_PORT` | 127.0.0.1 / 11452 | Master 绑定 |
| `FUSION_AGENT_HOST` / `FUSION_AGENT_PORT` | 127.0.0.1 / 11458 | Agent 绑定 |
| `FUSION_MASTER_HOST` / `FUSION_MASTER_PORT` | 127.0.0.1 / 11452 | Agent 回连目标 |
| `FUSION_MULTINODE_WEB_PORT` | 11472 | web 控制台端口（`start-web`） |
| `FUSION_MULTINODE_LOG_FILE` | logs/app_{role}.log | RotatingFileHandler 10MB×5 |
| `FUSION_REGISTRY` | ~/fusion/architecture/port-registry.yaml | start.sh 注册自检依据 |
| `FUSION_SKIP_REGISTRY_CHECK` | 未设 | 置 1 旁路注册自检 |
| `FUSION_CLUSTER_TOKEN` | 无 | 集群共享密钥，compose 必填（弱默认启动失败） |
| `FUSION_CLIENT_KEY` | 无 | fusion-mlx api_key（#92 规范名，弃用旧名 FUSION_MLX_API_KEY） |
| `FUSION_MLX_URL` | http://host.docker.internal:11434 | 容器内回连推理引擎 |
| `FUSION_AGENT_MAX_TASKS` | 4 | 单 agent 并发任务上限 |
| `FUSION_AUTO_APPROVE_PATTERNS` | 无 | 可信网段自动审批（生产收紧到精确 CIDR） |
| `FUSION_ALERT_WEBHOOK_URL` | 空 | 告警出站内网 webhook |
| `FUSION_BOOTSTRAP_ADMIN` | 空 | 首启引导 ADMIN 用户（空=单租户零配置） |
| `FUSION_USERS_FILE` | ~/.fusion/multi-node/users.json | 用户库路径 |

## 约束提醒

- 端口属于 `11400-11499` fusion 分配段，改动须同步 `architecture/port-registry.yaml` 并跑 `python scripts/check-ports.py`。
- v0.6.5 旧端口（9753-9756、8000）加载时自动迁移到现默认并写回。
- `docker-compose --scale agent=N` 共享 env 会同 node_id，mTLS 多 agent 须独立 compose（见 `docs/DEPLOYMENT.md`）。
- 不要把真实令牌写入任何文档、issue 或提交；模板值见 `.env.example`。

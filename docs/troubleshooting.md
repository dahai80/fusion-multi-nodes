---
doc_id: fusion-multi-node/troubleshooting
product: fusion-multi-node
class: core
entry: service
doc_type: troubleshooting
title: {zh: "fusion-multi-node 故障排查", en: "fusion-multi-node Troubleshooting"}
audience: engineer
status: draft
version: 0.20.5
updated: 2026-11-07
owner: "@dahai"
source_of_truth:
  - start.sh
  - fusion_multi_node/utils/utils.py
  - docs/HA-CRASH-RECOVERY.md
  - docs/OPERATIONS.md
  - fusion_multi_node/config/config.py
---

# fusion-multi-node 故障排查

日志位置：应用日志 `logs/app_master.log` / `logs/app_agent.log`（RotatingFileHandler 10MB×5）；启动错误 `logs/stderr_master.log` / `logs/stderr_agent.log`；web 控制台 `logs/stdout_web.log`。launchd 模式另有 `logs/stdout_master.launchd.log`。

## 症状速查

| 症状 | 可能原因 | 处置 |
|------|----------|------|
| `start.sh start` 报 "not registered in port-registry — refusing to start" | 端口注册自检失败（#92 前置检查） | 确认 `~/fusion/architecture/port-registry.yaml` 有 `fusion-multi-node: port 11452`（agent 为 `fusion-multi-node-agent: 11458`）；standalone 部署可 `FUSION_SKIP_REGISTRY_CHECK=1` |
| `start.sh status` 显示 not running 但进程存在 | PID 文件丢失或健康检查 404 | `kill` 旧进程后 `./start.sh restart`；健康定义是进程存活且 `GET /api/health` 返回 200 |
| 启动超时（60s health wait） | 端口被占 / 配置脏值 / 依赖缺失 | 查 `logs/stderr_master.log` 尾部（start.sh 失败时自动打印最近 20 行）；`lsof -i :11452` 查占用 |
| Agent 起来但 `node list` 看不到 | master 地址错 / mDNS 关闭 / 未审批 | 核对 `FUSION_MASTER_HOST/PORT`；`--no-mdns` 环境须手动注册；`fusion-multi-node cluster pending` 后 `approve` |
| 注册返回 400 | `node_id` 非法或 `protocol_version` 低于 0.8.0 | 升级两侧到同版本（当前 0.20.5） |
| 注册返回 403 | 节点曾被封禁 | Master 侧解除封禁或换 node_id |
| submit 返回 503 | HA standby 非 leader，或优先级队列满（`max_pending_queue` 默认 1000） | 打到 leader；或调大队列/消化积压 |
| submit mode=pipeline 返回 400 | `pipeline_enabled=False`（默认），上游 fusion-mlx `/distributed/*` 未实现 | 用 `--mode data`；确需 pipeline 先落上游（fusion-mlx issue #621） |
| chat 代理返回 429 | 租户并发超 `scheduling.tenant_max_concurrent`（默认 4） | 提高配额或降低并发；配额热加载生效 |
| API 返回 401 | 令牌缺失/错误 | 集群令牌在 `~/.fusion/multi-node/.cluster_token`；用户令牌格式 `fmu_` 前缀，明文仅签发时可见，丢了就 rotate |
| mTLS 服务拒启 | `security.mtls.enabled=true` 但证书路径不全（fail-closed，设计如此） | 补全 `ca_cert/node_cert/node_key` 或显式关掉 mTLS 段 |
| 告警只有内存里的 | `observability.alerts.webhook_url` 为空 | 配置内网 webhook（env `FUSION_ALERT_WEBHOOK_URL`）或轮询 `GET /api/v1/observability/alerts` |
| web 控制台打不开 | web/dist 未构建或 11472 未起 | `cd web && bun install && bun run build`，然后 `./start.sh start-web` |
| docker compose 启动即失败 | 未设 `FUSION_CLUSTER_TOKEN` / `FUSION_CLIENT_KEY`（P2-23 弱默认禁用） | `cp .env.example .env` 填强随机值；`python -c "import secrets; print(secrets.token_urlsafe(32))"` 生成 |
| 容器 agent 连不上 fusion-mlx | 引擎跑在宿主机 | 确认 `FUSION_MLX_URL=http://host.docker.internal:11434` |
| 崩溃后任务丢失 | 未开进程守护 | `./start.sh install-launchd`（KeepAlive + `_restore_tasks` RUNNING→PENDING 重派）；详见 `docs/HA-CRASH-RECOVERY.md` |
| 配置改了没生效 | 端口/HA/mDNS 不支持热加载 | `POST /api/v1/config/reload` 响应带 `restart_required` 时须重启进程 |

## 诊断命令

```bash
./start.sh status                              # 进程 + /api/health 双重健康
fusion-multi-node cluster status               # 节点/任务/KV/内存统计
fusion-multi-node node list --online           # 在线节点
fusion-multi-node config list                  # 当前生效配置
curl -s http://127.0.0.1:11452/api/v1/cluster/stats -H "Authorization: Bearer $(cat ~/.fusion/multi-node/.cluster_token)"
curl -s http://127.0.0.1:11452/api/v1/metrics  # Prometheus 文本指标
launchctl list | grep fusion-multi-node        # launchd 守护状态
```

## 恢复路径

1. 进程级：`./start.sh restart`（stop 最多等 10s 后 kill -9）。
2. 任务级：崩溃重启后 `_restore_tasks` 自动把 RUNNING 任务重置为 PENDING 重派；无需手工干预。
3. 配置级：脏值加载时自动回退默认（`_validate_and_repair`）并写回，先看 `config list` 实际值再手改。
4. 备份级：`fusion-multi-node backup create --out <file>` 周期备份，`backup restore --in <file>` 恢复。

## 上报边界

若确认问题出在 fusion-mlx（推理侧 5xx、模型加载失败），到 fusion-mlx 仓提 issue，不在本仓修；本仓只管调度与集群层。

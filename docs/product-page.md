---
doc_id: fusion-multi-node/product-page
product: fusion-multi-node
class: core
entry: service
doc_type: product-page
title: {zh: "fusion-multi-node 产品页 — 分布式 Apple Silicon MLX 推理集群", en: "fusion-multi-node Product Page — Distributed Apple Silicon MLX Inference Cluster"}
audience: engineer
status: draft
version: 0.20.5
updated: 2026-11-07
owner: "@dahai"
source_of_truth:
  - pyproject.toml
  - README.md
  - docs/DEPLOYMENT.md
  - fusion_multi_node/config/config.py
---

# fusion-multi-node 产品页

将多台 Apple Silicon Mac 池化为一个分布式 MLX 推理集群。单 Master 统一调度，多 Node Agent 回连执行，任务按数据并行分片派发，全部流量留在本地/可信 LAN。

## 定位

| 维度 | 说明 |
|------|------|
| 生态角色 | Fusion 生态底层算力基座（`architecture/PROJECT_RELATIONSHIPS.md`：Domain 层之上的共享调度层） |
| 运行形态 | 常驻服务：Master `127.0.0.1:11452`，Agent `127.0.0.1:11458` |
| 部署边界 | 单机 + 可信 LAN 小集群；100% 本地离线，无云依赖 |
| 非目标 | 不是 K8s/云原生编排；不做公网暴露、跨架构混部（见 `docs/DEPLOYMENT.md` 定位澄清） |

## 核心能力与消费者减负

| 能力 | 消费者不再需要自己写 | 约束 / 限制 |
|------|----------------------|-------------|
| 节点发现与注册 | 不必自建 mDNS 浏览、心跳与注册表 | mDNS 仅 LAN 可用；`--no-mdns` 时须手动注册 |
| 节点审批 | 不必手写准入控制 | 需 `protocol_version` ≥ 0.8.0；封禁节点 403 |
| 任务派发与队列 | 不必实现节点选择、优先级队列、幂等键 | 优先级队列默认上限 1000（满则 503）；租户并发默认 4 |
| 任务迁移 / 降级 | 不必写跨节点迁移与模型降级链逻辑 | 降级链 70b→1b，最多 2 次；迁移不搬 checkpoint |
| 统一 chat 代理 | 不必自己路由到某节点 fusion-mlx | 透传单节点（DATA count=1），OpenAI 兼容 |
| KV 缓存共享 | 不必自建跨节点前缀缓存 | 仅同集群内有效 |
| 可观测性 | 不必搭建指标/告警/日志管道 | Prometheus 文本格式 `/api/v1/metrics`；webhook 自备内网端点 |
| mTLS 节点互信 | 不必自管集群内证书 | fail-closed：证书不全则拒绝启动，不回退明文 |
| HA 双主 / 选举 | 不必自研故障转移 | standby 模式为技术预览，无生产 SLA |

## 集成边界

- **上游**：fusion-mlx（`:11434`，推理执行体）；fusion-core（配置/日志/HTTP 客户端）。
- **下游消费者**：fusion-agent-studio（9 操作集群契约 + 共享会话存储）、fusion-guard（TRANSPORT/纪元/confirm）、fusion-studio（`multiNodePort` 11452 启停）、fusion-web-shell（`/shell/fusion-multi-nodes/` 经 11472 web 控制台）。
- **安全模型**：集群令牌（节点间）+ 用户令牌 `fmu_<userid>_<secret>`（RBAC ADMIN/USER/VIEWER）。密钥只来自环境/配置文件，见 `docs/config-reference.md`。

## 入口

- 上手：`docs/get-started.md`
- HTTP 契约：`docs/api-reference.md`（详细版 `docs/API.md`）
- 配置：`docs/config-reference.md`
- 架构：`docs/architecture.md`
- 故障排查：`docs/troubleshooting.md`

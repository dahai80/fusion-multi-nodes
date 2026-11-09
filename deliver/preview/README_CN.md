---
doc_id: fusion-multi-node/preview-stack-cn
product: fusion-multi-node
class: core
entry: service
doc_type: guide
title: {zh: "自动化客户预览栈", en: "Automated Customer Preview Stack"}
audience: engineer
status: draft
version: 0.20.8
updated: 2026-11-09
owner: @dahai
source_of_truth:
  - deliver/preview/preview-stack.sh
  - deliver/preview/docker-compose.preview.yml
  - deliver/preview/synthetic_seed.py
---

# 自动化客户预览栈 (issue #108)

fusion-cowork#422 (match node) 的配套能力。Owner 要求：流水线每个节点全自动，不允许人工兜底演示。

一条命令为"试用已有项目"拉起隔离演示栈：

```
./deliver/preview/preview-stack.sh up [project-dir]
```

流水线（全自动，无人工步骤）：

1. compose 预览栈 — 基础 `docker-compose.yml` + 预览 overlay，独立项目名（`fusion-preview`），宿主端口 21452 / 21472，可与交付栈共存；**无需 fusion-mlx**（agent 健康检查降级为存活探针，演示数据不依赖推理）。
2. 合成演示数据注入 — 确定性 fixture 写入集群会话存储（`preview-demo-*`，标题带 `[SYNTHETIC DEMO]`）。绝不使用真实客户数据（business/AGENTS.md 规则 6 红线）。
3. 健康门 — master `/api/health` 必须 `ok` 才发放 URL；失败自动拆除。
4. 预览 URL + 通知 — 输出 `http://127.0.0.1:21472/`；设置 `FUSION_PREVIEW_NOTIFY_CMD` 时以 URL 作为 `$1` 执行（fusion-cowork cow#404 邮件通道在此接入）。
5. TTL 自动拆除 — `FUSION_PREVIEW_TTL_MINUTES`（默认 120；0 关闭）；守护进程到点执行 `compose down -v` 并清理状态。立即拆除：`./deliver/preview/preview-stack.sh teardown`。

配置（环境变量）：

| 变量 | 默认 | 含义 |
|------|------|------|
| `FUSION_PREVIEW_TTL_MINUTES` | 120 | 自动拆除延时 |
| `FUSION_PREVIEW_NOTIFY_CMD` | — | 接收预览 URL 的命令 |
| `FUSION_PREVIEW_MASTER_PORT` | 21452 | master API 宿主端口 |
| `FUSION_PREVIEW_WEB_PORT` | 21472 | web 控制台宿主端口 |
| `FUSION_PREVIEW_PROJECT` | fusion-preview | compose 项目名 |

terminal-mac 路径不在本特性范围内（依 #108）。

前置：项目目录内先跑过一次 `deliver/ubuntu/bring-up.sh`（生成 `.env` 与 fusion-core checkout、预构建镜像）。

---
doc_id: fusion-multi-node/demo-fleet-cn
product: fusion-multi-node
class: core
entry: service
doc_type: guide
title: {zh: "演示机编排（客户面 Mac A + 隐身算力 Mac B）", en: "Demo Fleet Orchestration (Customer-Facing Mac A, Hidden Compute Mac B)"}
audience: engineer
status: draft
version: 0.20.9
updated: 2026-11-09
owner: @dahai
source_of_truth:
  - deliver/demo/orchestrate-demo.sh
  - deliver/macos/bootstrap-guest.sh
---

# 演示机编排（#107）

客户只从 **Mac A 的 IP 进入**。Mac B 为演示提供算力，但绝不暴露给客户——不给 URL、不给端口、不提及。fusion-multi-node 就是让 B "隐身但有用"的那一层。

## 拓扑

```
客户 ──▶ Mac A（客户面）                       Mac B（纯算力，隐身）
         master :11452  ◀── 加入 ───────       NodeAgent :11458
         web 控制台 :11472                     fusion-mlx :11434
         fusion-gateway :11432（可选）
         ai-* 演示应用 UI/API
```

推理链路：A 上应用 → master F3 代理 `POST /v1/chat/completions`（`:11452`，OpenAI 兼容）→ 路由到 B 的 agent → B 本地 fusion-mlx。客户只和 A 通信。

## 部署

**Mac B（算力，一条命令）**——复用现有 guest 引导脚本，agent 角色：

```bash
FUSION_MASTER_HOST=<mac-a-ip> bash deliver/macos/bootstrap-guest.sh --with-mlx
```

注意：agent 的推理端点须指向 B 本地 mlx（`FUSION_MLX_URL=http://127.0.0.1:11434`；本项目默认配置端口不同——见根 README 端口表）。

**Mac A（客户入口）**——跑 master + web 控制台（`start.sh`）、ai-* 演示应用，可选 fusion-gateway（云端 LLM 上游在 gateway 配置，不在本仓库）。应用的 LLM 调用指向 `http://<mac-a-ip>:11452/v1/chat/completions`。

## 编排（在 Mac A 执行）

```bash
bash deliver/demo/orchestrate-demo.sh status   # master/gateway 健康 + 已加入算力节点
bash deliver/demo/orchestrate-demo.sh verify   # E2E：chat 补全路由到 worker 节点
```

`verify` 用 `FUSION_VERIFY_MODEL` 选模型（默认 `mlx-community-Llama-3.2-1B-Instruct-4bit`），无算力节点可达时响亮失败——演示绝不能起在静默坏掉的集群上。

给客户发布的：只有 Mac A 的应用 URL（可选 A 的 web 控制台）。B 留在局域网内。

## 沙箱说明

算力侧 test 执行（往 B 派发批次命令，如演示健康探测）需要沙箱，agent 侧 opt-in：

```bash
FUSION_NODE_TEST_EXEC=1 FUSION_NODE_SANDBOX=1 \
FUSION_NODE_SANDBOX_PATHS="$HOME/fusion" \
    # + 启动 agent —— 自动组装 AgentSandbox（gate + 子进程执行）
```

默认保持关闭——不开时 test 类派发返 `sandbox_unavailable`（失败可见）。

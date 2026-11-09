---
doc_id: fusion-multi-node/delivery-cn
product: fusion-multi-node
class: core
entry: service
doc_type: guide
title: {zh: "fusion-multi-node 交付包", en: "fusion-multi-node 交付包"}
audience: engineer
status: draft
version: 0.20.6
updated: 2026-11-09
owner: @dahai
source_of_truth:
  - deliver/ubuntu/bring-up.sh
  - deliver/macos/bootstrap-guest.sh
  - docker-compose.yml
  - Dockerfile
---

# 客户体验交付包 (issue #103)

面向客户评估部署的两条交付路径。

## 路径 1 — macOS (terminal-mac 风格虚拟机)

在全新 macOS 客户机 VM (Apple Virtualization) **内部**运行
`deliver/macos/bootstrap-guest.sh`。脚本会部署：

- 共享 venv + `fusion-multi-node`（master `:11452`、agent `:11458`）
- 可选 `--with-mlx`：同时部署 fusion-mlx 推理引擎（`:11434`，模型经
  `hf-mirror.com` 拉取）
- `:11472` 上的 web 控制台（客户机需装 bun，否则跳过）

```bash
bash deliver/macos/bootstrap-guest.sh --with-mlx
```

第二台 Mac 作为 worker agent 加入：

```bash
FUSION_MASTER_HOST=<master-ip> bash deliver/macos/bootstrap-guest.sh <dir>
```

**上游缺口 (terminal-mac RC)**：客户机 reset / clone / 文件导入上游
尚未提供。在此之前，把客户机当作一次性环境——重跑 bootstrap 脚本即可
从零重建；脚本是幂等的（已有 clone 只 pull 不重建）。

**栈范围**：本包交付集群层（multi-node + 可选推理引擎）。
Gateway / cowork / web-shell 联合栈由各自交付负责人部署；其交付脚本
就绪后将客户机指向相应入口即可。

## 路径 2 — Ubuntu (docker compose)

干净 Ubuntu VM（需 Docker Engine + compose v2）上一条命令：

```bash
./deliver/ubuntu/bring-up.sh
```

脚本行为：

1. 从 `.env.example` 生成 `.env`；占位/弱 `FUSION_CLUSTER_TOKEN`
   自动替换为强随机令牌。
2. `docker compose up -d --build --wait` — master + agent + web 控制台
   （同一镜像三种角色）。web dist 在镜像内构建（bun 多阶段构建），
   客户机无需 bun/node。
3. 校验容器健康并输出访问地址。

| 端点 | 地址 |
|------|------|
| 集群 API | `http://127.0.0.1:11452/api/health` |
| Web 控制台 | `http://127.0.0.1:11472/` |

扩容 agent：`docker compose up -d --scale agent=3`。
拆除：`docker compose down`。

**Ubuntu 上的推理**：fusion-mlx 仅支持 Apple Silicon，保持 Mac 裸机
部署。容器经 `host.docker.internal` 回连（见 `docker-compose.yml` 的
`FUSION_MLX_URL`）。干净 Ubuntu VM 可跑调度层 E2E（任务提交、路由、
重试）；推理结果需 Mac 节点加入集群。离线友好：镜像预构建
（先 `docker compose build`，再 `docker save` / `docker load` 到无网
客户机），模型走 hf-mirror。

## 验收对照

| issue #103 验收项 | 覆盖方式 |
|-------------------|----------|
| macOS：全新客户机启动、栈健康、E2E 流水线 | `bootstrap-guest.sh` + `start.sh status` + cluster CLI |
| Ubuntu：干净 VM `docker compose up`、E2E 通过 | `bring-up.sh`（封装 `docker compose up --wait` + 健康校验） |
| 双语文档 | 本文件 + 同目录 `README.md` |

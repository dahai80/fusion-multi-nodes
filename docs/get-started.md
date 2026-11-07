---
doc_id: fusion-multi-node/get-started
product: fusion-multi-node
class: core
entry: service
doc_type: get-started
title: {zh: "fusion-multi-node 快速上手", en: "fusion-multi-node Get Started"}
audience: engineer
status: draft
version: 0.20.5
updated: 2026-11-07
owner: @dahai
source_of_truth:
  - start.sh
  - pyproject.toml
  - README.md
  - .env.example
---

# fusion-multi-node 快速上手

前提：macOS + Apple Silicon，Python ≥ 3.11，fusion 仓根 `.venv/` 已激活。

## 1. 安装

```bash
cd ~/fusion/fusion-multi-nodes
source ../.venv/bin/activate
pip install -e .            # core
pip install -e ".[all]"     # 含 web/grpcio-tools 可选依赖
```

## 2. 启动 Master（本机）

```bash
./start.sh start            # 默认 ROLE=master, 127.0.0.1:11452
./start.sh status           # running master (PID ...) at 127.0.0.1:11452
curl -s http://127.0.0.1:11452/api/health
```

`start.sh` 启动前会自检 `~/fusion/architecture/port-registry.yaml`（未注册端口拒绝启动；standalone 部署可用 `FUSION_SKIP_REGISTRY_CHECK=1` 旁路）。

## 3. 启动 Agent（其他 Mac）

```bash
ROLE=agent FUSION_MASTER_HOST=<master-ip> ./start.sh start   # Agent 127.0.0.1:11458 回连 master:11452
```

单机试用可同机起 agent：`ROLE=agent ./start.sh start`。Agent 首次注册需审批：

```bash
fusion-multi-node cluster pending          # 查看待审批节点
fusion-multi-node cluster approve <node_id> --by admin
fusion-multi-node node list
```

## 4. 提交任务

```bash
fusion-multi-node task submit --name demo --model mlx-community/Qwen3-8B-4bit \
  --mode data --prompt "hello cluster"
fusion-multi-node task list
fusion-multi-node cluster status
```

HTTP 等价（带 Bearer 集群令牌）：

```bash
curl -s -X POST http://127.0.0.1:11452/api/v1/tasks/submit \
  -H "Authorization: Bearer $FUSION_CLUSTER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"demo","mode":"data","model_name":"mlx-community/Qwen3-8B-4bit","prompt":"hi"}'
```

## 5. 验证与停止

```bash
fusion-multi-node cluster status   # 节点/任务/KV/内存统计
./start.sh stop
```

## 常见下一步

- 生产驻留（launchd 守护）：`./start.sh install-launchd`，见 `docs/DEPLOYMENT.md` 模式 B。
- 多机 docker-compose：`cp .env.example .env` 填 `FUSION_CLUSTER_TOKEN` 与 `FUSION_CLIENT_KEY` 后 `docker compose up -d --scale agent=2`。
- 完整 HTTP 契约：`docs/api-reference.md`。
- 报错先查 `docs/troubleshooting.md`；服务日志在 `logs/`（应用日志 `app_master.log`/`app_agent.log`，RotatingFileHandler 10MB×5）。

## 边界提醒

- pipeline 并行默认关闭（上游 fusion-mlx `/distributed/*` 未实现前，submit mode=pipeline 返回 400）。
- HA 多 Master 为技术预览，生产建议单 Master + launchd。
- 令牌：集群令牌在 `~/.fusion/multi-node/.cluster_token` 自动生成；不要把真实令牌写进文档或 issue。

# Fusion-Multi-Node Docker 部署

集群调度层 (Master + Agent) 容器化部署。**推理引擎 fusion-mlx 保持裸机部署**, 容器经 `host.docker.internal` 回连。

## 构建

```bash
docker build -t fusion-multi-node:latest .
```

## 启动 (1 Master + 2 Agent)

```bash
# 设置共享集群密钥 (所有容器必须一致)
export FUSION_CLUSTER_TOKEN="your-secret-token-here"

# 启动
docker compose up -d

# 查看状态
docker compose ps
docker compose logs -f master
```

- Master 暴露 `11452` (集群 API)。
- Agent 暴露 `11458` / `11459` (2 副本映射到不同宿主端口)。
- 容器间经 bridge 网络按服务名 `master:11452` 通信。
- Agent 经 `host.docker.internal:11434` 回连裸机 fusion-mlx。

## 扩容 Agent

```bash
# 扩到 3 个 agent (注意: 第 3+ 副本的宿主端口映射需调整, 见下)
docker compose up -d --scale agent=3
```

> **端口冲突警告**: `docker-compose.yml` 为前 2 个 agent 固定了宿主端口映射 (`11458`/`11459`)。`--scale agent=N` 当 N>2 时, 第 3+ 个容器会因端口冲突启动失败。生产扩容建议:
> - 用反向代理 (nginx/traefik) 统一入口, 容器内只暴露 `11458` 不映射宿主端口; 或
> - 用独立 compose 覆盖文件 (`docker-compose.scale.yml`) 为每个副本指定独立端口。

## 环境变量参考

| 变量 | 作用 | 默认值 |
|------|------|--------|
| `FUSION_MULTINODE_ROLE` | 节点角色 (`master`/`agent`) | `master` |
| `FUSION_MULTINODE_HOST` | Master 监听地址 | `0.0.0.0` (容器) |
| `FUSION_MULTINODE_PORT` | Master 监听端口 | `11452` |
| `FUSION_AGENT_HOST` | Agent 监听地址 | `0.0.0.0` (容器) |
| `FUSION_AGENT_PORT` | Agent 监听端口 | `11458` |
| `FUSION_MASTER_HOST` | Agent 回连 Master 地址 | `master` (服务名) |
| `FUSION_MASTER_PORT` | Agent 回连 Master 端口 | `11452` |
| `FUSION_CLUSTER_TOKEN` | **集群共享密钥** (必填, 所有容器一致) | `dev-cluster-token-change-me` |
| `FUSION_MLX_URL` | 推理引擎地址 (Agent 回连 fusion-mlx) | `http://host.docker.internal:11434` |
| `FUSION_MLX_API_KEY` | fusion-mlx API key (鉴权 `/v1/*` 与 `/distributed/*`) | `dahai168` |

## 指向裸机 fusion-mlx

1. 确认裸机 fusion-mlx 已启动: `~/claude-home/fusion-mlx/start.sh status` (监听 `11434`)。
2. Docker Desktop (Mac/Windows): `host.docker.internal` 自动可用, 无需额外配置。
3. Linux: `docker-compose.yml` 已加 `extra_hosts: host.docker.internal:host-gateway`, 自动映射到宿主网关。
4. 若 fusion-mlx 在远程机器: 设置 `FUSION_MLX_URL=http://<远程IP>:11434` 覆盖默认。

## 故障排查

### Agent 日志报 401 / 认证失败
- 检查所有容器的 `FUSION_CLUSTER_TOKEN` 是否完全一致。
- Master 与 Agent 各自的 `BearerAuthMiddleware` 校验同一 token。

### Agent 连不上 fusion-mlx
- 容器内测试: `docker compose exec agent python -c "import httpx; print(httpx.get('http://host.docker.internal:11434/v1/models').status_code)"`
- Mac/Windows: 确认 Docker Desktop 运行中。
- Linux: 确认 `extra_hosts` 生效 (`docker compose exec agent getent hosts host.docker.internal`)。
- fusion-mlx 启用了 api_key: 确认 `FUSION_MLX_API_KEY` 与裸机 `settings.auth.api_key` 一致。

### Agent 注册被 Master 拒绝 (403 未审批)
- Master 默认启用 `NodeApprovalManager` 节点审批门。
- 查看待审批: `docker compose exec master fusion-multi-node cluster pending`
- 审批节点: `docker compose exec master fusion-multi-node cluster approve <node_id>`

### mDNS 发现不可用
- 容器内禁用 mDNS (`--no-mdns`), 走显式 `FUSION_MASTER_HOST` 回连。这是预期行为 — Bonjour 不跨容器网络。

### `host.docker.internal` 解析失败 (Linux)
- `docker-compose.yml` 的 `extra_hosts: host.docker.internal:host-gateway` 已覆盖。
- 老版 Docker (<20.10) 不支持 `host-gateway`: 升级 Docker 或手动改 `extra_hosts` 为宿主 IP。

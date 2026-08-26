# 部署指南 (Deployment)

> P2-19 (审计 §6.5): 本项目定位澄清 — local-first Apple Silicon MLX 集群, **非 K8s/云原生编排目标**。
> 部署方案围绕单机 + 可信 LAN 小集群设计, 不提供 Kubernetes/Helm chart。

## 定位与边界

fusion-multi-node 是 Fusion 生态的 **底层算力基座**: 将多台 Apple Silicon Mac 池化为分布式 MLX 推理集群。
设计前提:

- **100% 本地 / 离线** — 无云 API 调用, 无外部网络依赖。
- **Apple Silicon 单平台** — 依赖 MLX/Metal, 非跨架构容器编排目标。
- **可信 LAN 小集群** — 单机到几台 Mac, 非大规模数据中心。

适用场景: 个人多 Mac 工作站、实验室/团队共享算力、可信 LAN 协同推理。
**不适用**: 公网暴露、敌对网络、多租户隔离强需求、跨架构混部 — 这些需上层 (fusion-gateway 等) 处理。

## 部署模式对比

| 模式 | 进程管理 | 崩溃自愈 | 跨机 | 适用 |
|------|---------|---------|------|------|
| A. 单机 nohup | `./start.sh start` | 无 (手动拉起) | 否 | 开发/试用 |
| B. 单机 launchd | `./start.sh install-launchd` | KeepAlive 自动重启 | 否 | 单机生产 (Mac) |
| C. docker-compose | `docker compose up` | `restart:unless-stopped` | 是 (1 Master + N Agent) | 可信 LAN 小集群 |
| D. 多 Master HA | `start(ha_config=...)` | 选举故障转移 | 是 | **技术预览**, 非生产 SLA |

默认 = 单 Master。模式 B/C 是推荐生产路径。模式 D 可选叠加, 但未经生产验证。

## 模式 A — 单机 nohup

```bash
cd /Users/dahai/fusion && source .venv/bin/activate
cd fusion-multi-node
./start.sh start    # Master nohup 后台, PID 文件 .fusion-multi-node.master.pid
./start.sh status
./start.sh stop
```

日志: `logs/stdout.log` + `logs/stderr.log` (nohup)。崩溃需手动 `restart`。适合本地开发快速试用。

## 模式 B — 单机 launchd 守护 (推荐, Mac 生产)

```bash
./start.sh install-launchd
# 渲染 deploy/*.plist → ~/Library/LaunchAgents/com.dahai80.fusion-multi-node.plist
# launchctl load: RunAtLoad 启动 + KeepAlive 崩溃自愈
launchctl list | grep fusion-multi-node
./start.sh uninstall-launchd   # 卸载
```

崩溃自愈闭环: launchd KeepAlive 重启 + H3 任务持久化恢复 (`_restore_tasks` 重派 RUNNING→PENDING)。
日志: `FUSION_MULTINODE_LOG_FILE` 指定文件 + RotatingFileHandler 10MB×5 (P1-16)。
详见 `docs/HA-CRASH-RECOVERY.md`。**与 nohup 二者择一**, install-launchd 检测 nohup 在跑会先转交。

## 模式 C — docker-compose 多机小集群

```bash
cp .env.example .env             # 填强随机 FUSION_CLUSTER_TOKEN + fusion-mlx api_key (P2-23)
docker compose up -d --scale agent=2
```

- 1 Master (暴露宿主 11452) + N Agent (容器 bridge IP 回连, 不占主机端口)。
- 推理引擎 fusion-mlx 跑裸机, 容器经 `host.docker.internal:11434` 回连。
- `--scale agent=N` 无上限扩容; agent 无 host 端口映射。
- `restart:unless-stopped` 容器级崩溃重启; json-file 日志轮转 10MB×3 (P1-16)。
- 凭据: compose 不带弱默认, 未设 `FUSION_CLUSTER_TOKEN`/`FUSION_MLX_API_KEY` 启动失败并提示 (P2-23)。
- 可信网段自动审批: `FUSION_AUTO_APPROVE_PATTERNS` (生产收紧到精确 CIDR)。

跨机真网络: 多台 Mac 各跑 agent, Master 统一调度。详见 E2E 测试 `tests/test_data_parallelism_e2e.py`。

## 多 Master HA (技术预览)

`start(ha_config={"enabled":True,"node_id":"m1","priority":1,"peers":[...]})` 启动选举:
- `MasterElection` Raft-simplified: leader 选举 + term/voted_for 持久化 (P0-1) + leader 心跳广播 + 任务快照推 standby。
- Standby (`_election 配置且非 _is_leader`): `assign_task` 拒派发, `/api/tasks/submit` 返 503。
- `StandbyMaster` 类为未接线死代码 (与已接线的 `MasterElection` 分离)。

**警告**: 技术预览, 非生产 SLA 验证。生产关键负载仍建议单 Master + launchd + 定期备份。

## 扩容与资源

- **水平扩容 (Agent)**: docker-compose `--scale agent=N` 或裸机各 Mac 跑 `fusion-multi-node node start --role agent`。
- **资源限制**: 容器未设 `mem_limit`/`cpus` (审计 §6.11 债), 裸机靠 OS 调度; `FUSION_AGENT_MAX_TASKS` 限单 agent 并发任务。
- **推理引擎**: fusion-mlx 单机裸机 (非容器), MLX 统一显存管理。

## 非目标 — 为何无 Kubernetes

本项目 **不提供** Kubernetes/Helm/Nomad manifests, 原因:

1. **平台绑定**: MLX/Metal 仅 Apple Silicon, 容器编排跨架构混部无收益。
2. **离线约束**: 100% 本地, K8s 控制面 + 镜像仓库引入外部依赖, 违定位。
3. **规模错配**: 设计为单机到几台 Mac 的小集群, K8s 面向大规模数据中心。
4. **运维成本**: launchd/docker-compose 已覆盖单机+可信 LAN 自愈, K8s 运维开销超出目标场景。

企业级多机编排 (跨集群/多租户网关/公网暴露) 属 fusion-gateway (Go) 职责, 非 fusion-multi-node 范围。
如需容器编排以外的部署形态, 在上层网关处理, 本项目保持算力基座纯本地。

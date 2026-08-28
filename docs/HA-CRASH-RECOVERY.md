# HA 与崩溃恢复 (H2 + GAP-1)

> fusion-multi-node **默认单 Master** 模式 (`_election is None`, `_is_leader=True`)。多 Master
> HA 选举 (`MasterElection`/`setup_election`) **已接线** (P4 + P0-1 + GAP-1): leader 心跳广播 +
> term/voted_for 持久化 + 任务快照推 standby + **全状态同步** (nodes/kv_cache/banned_nodes),
> 经 `start(ha_config={"enabled":True,...})` 显式启用; `StandbyMaster`
> 仍为未接线死代码 (独立类, 与已接线的 `MasterElection` 分离)。默认部署不启用 HA。
> 本文档描述 **两条 HA 路线**:
> - **单 Master + launchd 进程守护 + H3 任务持久化** = 本机崩溃自愈, 不丢任务 (默认)。
> - **多 Master HA + 全状态同步** (GAP-1, v0.10.0) = 跨机故障转移, standby 持完整拓扑,
>   leader 宕机 standby 立即接管调度, 满足 always-on SLA (显式启用, 2+ Master 部署)。

## 崩溃自愈链路

```
Master 进程崩溃
  → launchd 检测退出 (KeepAlive.Crashed=true, SuccessfulExit=false)
  → 10s 节流后自动重启 (ThrottleInterval)
  → start() 调 _restore_tasks()
  → 读 ~/.fusion/multi-node/tasks.json
  → 重建非终态任务 (RUNNING/MIGRATED → PENDING 重派)
  → _health_check_loop / _retry_loop / _persist_loop 重启
  → 任务不丢, 自动重派
```

## 两层保障

| 层 | 机制 | 作用 |
|----|------|------|
| 进程层 (H2) | launchd `KeepAlive` 崩溃自动重启 | Master 不需人工拉起 |
| 数据层 (H3) | 任务原子落盘 + 启动恢复 | 崩溃前的 RUNNING/PENDING 任务不丢, 自动重派 |

任一层单独不够: 仅进程守护 (无 H3) → 崩溃丢全部在途任务; 仅持久化 (无守护) → 崩溃后需人工手动重启。
两者组合 = 崩溃自愈闭环。

## 安装 launchd 守护

```bash
./start.sh install-launchd
# 渲染 deploy/com.dahai80.fusion-multi-node.plist (占位符 @@VENV_BIN@@ 等替换为实际路径)
# → ~/Library/LaunchAgents/com.dahai80.fusion-multi-node.plist
# → launchctl load (RunAtLoad 立即启动 + KeepAlive 崩溃自愈)
```

环境变量覆盖 (与 start.sh 一致):
- `FUSION_MULTINODE_HOST` / `FUSION_MULTINODE_PORT` (master 监听, 默认 127.0.0.1/11452)

验证:
```bash
launchctl list | grep fusion-multi-node     # 状态列 (PID 在跑 / 非0退出码待重启)
tail -f logs/stdout_master.launchd.log       # launchd 托管日志 (区别于 nohup 的 stdout_master.log)
```

## 卸载

```bash
./start.sh uninstall-launchd
# launchctl unload + 删 plist
```

## 与 nohup 模式的关系

- `./start.sh start` = nohup 后台进程 (PID 文件 `.fusion-multi-node.master.pid`), 不自动重启。
- `./start.sh install-launchd` = launchd 托管, 崩溃自愈。**二者择一**。
- `install-launchd` 检测到 nohup 进程在跑会先 `stop` 转交 launchd, 避免双实例。

## 持久化文件

- 路径: `~/.fusion/multi-node/tasks.json`
- 写点: assign_task (RUNNING 即时落盘) / _finalize_task (终态落盘, 清掉该任务) / cancel_task (终态落盘) / _persist_loop (15s 周期兜底)
- 终态任务 (COMPLETED/FAILED/CANCELLED/TIMEOUT) 不落盘, 节省空间
- 恢复语义: RUNNING/MIGRATED → PENDING (派发中任务崩溃后须重新调度); PENDING 保持 PENDING

## 多 Master HA + 全状态同步 (GAP-1, v0.10.0)

### 为什么需要全状态同步

原 HA (v0.8.3) 仅同步 **任务** 到 standby。Master 宕机后 standby 缺 nodes/kv_cache/banned_nodes,
节点须重新注册才能调度 → always-on SLA 不满足。GAP-1 扩展同步范围到完整集群拓扑:
standby 持有 nodes + kv_cache + banned_nodes + fault_counts, leader 宕机后 standby promote
为 leader 即可立即调度, 无须等节点重注册。

### 同步内容

| 状态 | 来源域 | 同步方式 |
|------|--------|----------|
| tasks (非终态) | `_tasks_lock` | `_persist_tasks` 触发推送 `/api/ha/sync-tasks` (即时) |
| nodes | `_nodes_lock` | `_state_sync_loop` (5s 周期) 推送 `/api/ha/sync-state` |
| kv_cache | `_kv_lock` | 同上 |
| banned_nodes | `_nodes_lock` | 同上 (ban 解封时间, 取较晚) |

- leader 周期 (5s) 推全状态到所有 standby, best-effort, 不阻塞派发。
- standby `receive_synced_state` 幂等合并; 锁序 nodes→kv, 两域分别持锁不嵌套。
- HA 仍 **opt-in**: 单 Master 部署 (`_election is None`) 不启动同步循环, 行为不变。

### 启用 always-on (2+ Master 部署)

```python
await master.start(
    ha_config={
        "enabled": True,
        "node_id": "master-1",
        "priority": 10,                 # 高优先级 = 初始 leader
        "peers": [
            {"node_id": "master-2", "ip": "10.0.0.2", "port": 11452, "priority": 1},
        ],
    }
)
```

- leader 宕机 → standby 超时 (election_timeout 5-10s) → 发起选举 → 获多数票 → promote。
- promote 后 standby 已持完整拓扑 (nodes/kv/banned 来自周期同步), 立即派发, 无空窗。
- `StandbyMaster` 类仍为死代码 (与已接线的 `MasterElection` 分离), 不参与本路径。

### 故障转移链路

```
Leader Master 宕机 (整机/进程)
  → standby 选举超时 (5-10s)
  → 发起拉票 → 获多数票 → promote 为 Leader (_is_leader=True)
  → standby 已持 nodes/kv/banned (周期同步)
  → assign_task 立即可派发 (无须等节点重注册)
  → always-on: 空窗 ≤ 选举超时 (~10s)
```

## v0.14.0 — HA 接线修复 + 规则纪元/confirm 持久化

两处企业级阻塞修复 (详见 `docs/DEPLOYMENT.md` + `docs/CHANGELOG.md`):

1. **`cluster start` 接线漏修复**: v0.14.0 前 `fusion-multi-node cluster start` (cli.py `_async_cluster_start`)
   调 `_master.start()` **不带 ha_config** → 该路径永不启 HA (仅 `node start` 路径带)。v0.14.0 两启动路径
   对齐, 均读 `ClusterConfig.get_ha_config()` + 注入 `config=`。生产经 `config.json` `ha.enabled=true` +
   peers 显式启用 (config 示例见 DEPLOYMENT.md「多 Master HA」段)。HA 仍默认关 (单 Master 兼容)。
2. **规则纪元/confirm 不再内存态**: v0.14.0 前 `_rule_epoch`/`_confirms` 纯内存 → 重启归零 / HA failover
   从 0 起 (guard 重新基线/重查, v0.13.0 CHANGELOG 已知限制)。v0.14.0 加 `rule_epoch.json` 持久化
   (原子落盘, start 恢复, 坏盘容错→默认 0) + leader `_build_state_sync_payload` 纳入 epoch+confirm,
   standby `receive_synced_state` 取 max epoch (防回退) + 合并 confirm。HA failover 后 standby 接 leader
   推进的纪元, 不再从 0。

## 局限

- 单 Master = SPOF, launchd 守护仅保证 **本机** 崩溃自愈, 不防整机宕机/网络分区。
- 多 Master HA (跨机故障转移) 已接线 + 全状态同步 (GAP-1): `start(ha_config=)` 显式配 peers
  启动选举, leader 心跳 + 任务快照 + **全状态** (含规则纪元/confirm) 推 standby。**always-on 空窗 ≤ 选举超时 (~10s)**。
  仍为 opt-in, 默认单 Master 部署不启用。生产 always-on 须 2+ Master 显式配置 (见 DEPLOYMENT.md)。
- 本机崩溃自愈 (launchd + H3) 已覆盖主要故障模式; 跨机故障转移为可选增强 (GAP-1 补齐)。
- KV 跨节点张量复用已交付 (GAP-7, v0.11.0): `sync_kv_cache` 传输真张量, 默认 `SyntheticKVTransport`
  合成兜底, `MLXKVTransport` env-gated 待上游 #650 内存直传落地 (未落地 404→降级合成, 不阻断);
  v0.11.1 起流式传输 (二进制流协议, 不 base64+JSON 物化整 bundle, 大张量可行)。全状态同步亦传 KV 完整条目 (含张量)。

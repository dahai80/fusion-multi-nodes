# HA 与崩溃恢复 (H2)

> fusion-multi-node **默认单 Master** 模式 (`_election is None`, `_is_leader=True`)。多 Master
> HA 选举 (`MasterElection`/`setup_election`) **已接线** (P4 + P0-1): leader 心跳广播 + term/voted_for
> 持久化 + 任务快照推 standby, 经 `start(ha_config={"enabled":True,...})` 显式启用; `StandbyMaster`
> 仍为未接线死代码 (独立类, 与已接线的 `MasterElection` 分离)。默认部署不启用 HA。
> 本文档描述 **务实 HA 路线**: 单 Master + launchd 进程守护 + H3 任务持久化 = 崩溃自愈, 不丢任务。
> 多 Master HA 为技术预览, 非生产 SLA 承诺。

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

## 局限

- 单 Master = SPOF, launchd 守护仅保证 **本机** 崩溃自愈, 不防整机宕机/网络分区。
- 多 Master HA (跨机故障转移) 已接线但为 **技术预览** (非生产 SLA 验证): `start(ha_config=)` 显式配 peers 启动选举, leader 心跳 + 任务快照推 standby。生产关键负载仍建议单 Master + launchd + 定期备份。
- 本机崩溃自愈 (launchd + H3) 已覆盖主要故障模式; 跨机故障转移为可选增强。

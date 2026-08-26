"""Fusion-Multi-Node — distributed Apple Silicon MLX cluster orchestration.

Provides mDNS node discovery, FMP binary protocol routing,
load-aware scheduling, KV cache sharing, Caveman compression,
cluster observability, task auto-degradation, security sandbox, autoscaler,
and storage volumes.

注意 (AR审计 2026-08-24, P0 整改 2026-08-26 更新):
- MasterElection (P4 HA 已接 + GAP-1 全状态同步): leader 选举 + term/voted_for 持久化 +
  leader 心跳广播 + 任务快照推 standby + **全状态同步** (nodes/kv_cache/banned_nodes)。
  `start(ha_config=...)` 启动多 Master HA; 2+ Master 显式配置获 always-on (standby 持完整拓扑,
  failover 即调度, 空窗 ≤ 选举超时)。**已接线, 非原型。**
- StandbyMaster: **未接线死代码** (独立类, 与已接线的 MasterElection 分离)。现网单 Master
  模式 (`_election is None`) 无 HA; 多 Master 须显式配 ha_config 启动。
- cloud fallback: 违"100%本地/离线"定位 — v0.8.2 起 ClusterMaster 调度路径已切断
  (setup_cloud_fallback/fallback_to_cloud/_retry_loop 云端分支全部移除)。
  cloud_fallback.py 模块文件 + 测试保留供独立验证, 不再接调度器。计划迁移至 fusion-gateway。
- mcp_gateway: **未接线死代码** (零路由/零实例化/零 CLI), 计划迁移 fusion-gateway #106。
- ast_diff / cluster_sync: 功能归属债 (非云合规债, 均纯本地计算), 计划迁移至
  fusion-gateway / fusion-cowork。ast_diff 被 secure_transfer (PII 脱敏传输) 复用,
  cluster_sync 被 master_server (LAN 模型清单同步) 复用 — 保留至迁移落地。
- autoscaler: **未接线死代码** (零实例化, /api/v1/autoscaler/* 恒 404)。
- PIPELINE 并行: 接 fusion-mlx `/distributed/*` (上游 issue #621/#630 已交付 closed),
  多节点客户端存根已接 + 真 E2E 验证通过; 张量级 KV 跨节点传输 (sync_kv_cache) 仍 no-op (P3-28 长期, issue #33)。
"""

__version__ = "0.10.0"
__app_name__ = "Fusion-Multi-Node"

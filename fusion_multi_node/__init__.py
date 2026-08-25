"""Fusion-Multi-Node — distributed Apple Silicon MLX cluster orchestration.

Provides mDNS node discovery, FMP binary protocol routing,
load-aware scheduling, KV cache sharing, Caveman compression,
cluster observability, task auto-degradation, security sandbox, autoscaler,
and storage volumes.

注意 (AR审计 2026-08-24):
- master election / StandbyMaster: 未接线原型, 现网单 Master 无 HA, 非生产可用。
- cloud fallback: 违"100%本地/离线"定位 — v0.8.2 起 ClusterMaster 调度路径已切断
  (setup_cloud_fallback/fallback_to_cloud/_retry_loop 云端分支全部移除)。
  cloud_fallback.py 模块文件 + 测试保留供独立验证, 不再接调度器。计划迁移至 fusion-gateway。
- mcp_gateway / ast_diff / cluster_sync: 功能归属债 (非云合规债, 均纯本地计算),
  计划迁移至 fusion-gateway / fusion-cowork。ast_diff 被 secure_transfer (PII 脱敏传输)
  复用, cluster_sync 被 master_server (LAN 模型清单同步) 复用 — 保留至迁移落地。
"""

__version__ = "0.8.3"
__app_name__ = "Fusion-Multi-Node"

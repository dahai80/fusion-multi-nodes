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
- StandbyMaster: **v0.10.2 已删** (零实例化死代码, 独立于已接线的 MasterElection)。现网单 Master
  模式 (`_election is None`) 无 HA; 多 Master 须显式配 ha_config 启动。
- cloud_fallback / mcp_gateway / ast_diff / secure_transfer 迁移债已清理 (v0.12.2):
  接收端 fusion-gateway #106 (Go 网关吸收 cloud adapter + MCP gateway) +
  fusion-cowork #61 (ast_diff + cluster_sync 已 CLOSED 落地, 自包含不依赖多节点)。
  死模块连同 re-export 与测试删除; cloud 调度路径残留彻底无。
  **cluster_sync 保留** (live — agent 跨节点模型同步经 master manifest 路由, 删则丢功能)。
- autoscaler: **未接线死代码** (零实例化, /api/v1/autoscaler/* 恒 503 not-wired, 非 404)。
- PIPELINE 并行: 接 fusion-mlx `/distributed/*` (上游 issue #621/#630 已交付 closed),
  多节点客户端存根已接 + 真 E2E 验证通过。
- GAP-7 KV 张量跨节点传输 (v0.11.0, close #33): `sync_kv_cache` 经可插拔张量后端
  (合成默认 / MLX 真张量 env-gated FUSION_KV_TENSOR_BACKEND=mlx 待上游 #650) 编排
  源 agent /api/kv/export → 目标 /api/kv/import, 返 True。合成后端满足 #33 验收
  (张量 round-trip), 真张量为 env-gated bonus。
"""

__version__ = "0.12.2"
__app_name__ = "Fusion-Multi-Node"

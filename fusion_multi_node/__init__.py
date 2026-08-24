"""Fusion-Multi-Node — distributed Apple Silicon MLX cluster orchestration.

Provides mDNS node discovery, FMP binary protocol routing,
load-aware scheduling, KV cache sharing, Caveman compression,
cluster observability, task auto-degradation, security sandbox, autoscaler,
and storage volumes.

注意 (AR审计 2026-08-24):
- master election / StandbyMaster: 未接线原型, 现网单 Master 无 HA, 非生产可用。
- cloud fallback / mcp_gateway / ast_diff: 违"100%本地/离线"定位, 默认禁用,
  计划迁移至 fusion-gateway / fusion-cowork (上游 issue 跟踪)。
"""

__version__ = "0.7.0"
__app_name__ = "Fusion-Multi-Node"

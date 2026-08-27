"""Cluster Master module exports."""

from .ast_diff import apply_ast_diff, compute_ast_diff

# P1-4 (审计 §3.2): cloud_fallback 已加 import-time 禁用守卫 (调度路径切断, 待迁 #106)。
# 默认 ImportError → 降级不导出 (类/常量置 None), 不破包级导入。
# 显式 FUSION_CLOUD_FALLBACK_ENABLED=1 (独立验证) 时正常导出。
try:
    from .cloud_fallback import (
        AVAILABLE_MODELS,
        CloudConfig,
        CloudFallbackClient,
        CloudModel,
        CloudProvider,
        CloudUsage,
    )
except ImportError:
    AVAILABLE_MODELS = None  # type: ignore[assignment]
    CloudConfig = None  # type: ignore[assignment,misc]
    CloudFallbackClient = None  # type: ignore[assignment,misc]
    CloudModel = None  # type: ignore[assignment,misc]
    CloudProvider = None  # type: ignore[assignment,misc]
    CloudUsage = None  # type: ignore[assignment,misc]
from .cluster_master import (
    ClusterMaster,
    ClusterTask,
    KVCacheEntry,
    NodeInfo,
    NodeStatus,
    ParallelMode,
    TaskStatus,
)
from .cluster_sync import (
    ClusterSyncManager,
    FileEntry,
    ModelManifest,
    NodeHealth,
    NodeLoadReport,
    PartitionDetector,
    PartitionState,
    build_manifest,
    compute_file_sha256,
    compute_sync_diff,
)
from .election import (
    ElectionCandidate,
    ElectionState,
    MasterElection,
    VoteRequest,
    VoteResponse,
)
from .load_metrics import (
    STRATEGY_WEIGHTS,
    LoadMetrics,
    LoadRouter,
    RoutingResult,
    RoutingStrategy,
    RoutingWeights,
)
from .task_sharding import (
    MergedResult,
    ShardingStrategy,
    ShardingType,
    ShardMerger,
    ShardResult,
    TaskShard,
    TaskSharder,
)
from .task_spec import TaskPriority, TaskSpec

__all__ = [
    "AVAILABLE_MODELS",
    "STRATEGY_WEIGHTS",
    "ClusterSyncManager",
    "CloudConfig",
    "CloudFallbackClient",
    "CloudModel",
    "CloudProvider",
    "CloudUsage",
    "ClusterMaster",
    "ClusterTask",
    "ElectionCandidate",
    "FileEntry",
    "ElectionState",
    "KVCacheEntry",
    "LoadMetrics",
    "LoadRouter",
    "MasterElection",
    "MergedResult",
    "ModelManifest",
    "NodeHealth",
    "NodeInfo",
    "NodeLoadReport",
    "NodeStatus",
    "ParallelMode",
    "PartitionDetector",
    "PartitionState",
    "RoutingResult",
    "RoutingStrategy",
    "RoutingWeights",
    "ShardMerger",
    "ShardResult",
    "ShardingStrategy",
    "ShardingType",
    "TaskPriority",
    "TaskShard",
    "TaskSharder",
    "TaskSpec",
    "TaskStatus",
    "VoteRequest",
    "VoteResponse",
    "apply_ast_diff",
    "build_manifest",
    "compute_ast_diff",
    "compute_file_sha256",
    "compute_sync_diff",
]

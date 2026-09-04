"""Cluster Master module exports."""

# cloud_fallback / ast_diff 迁移债已清理 (接收端 fusion-gateway #106 + fusion-cowork #61 已 CLOSED 落地),
# 死模块连同 re-export 删除; cluster_sync 保留 (live, agent 跨节点模型同步)。
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
from .test_batch import TestBatch, TestJob

__all__ = [
    "STRATEGY_WEIGHTS",
    "ClusterSyncManager",
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
    "TestBatch",
    "TestJob",
    "VoteRequest",
    "VoteResponse",
    "build_manifest",
    "compute_file_sha256",
    "compute_sync_diff",
]

"""Task Sharding — 任务分片类型、自动分片算法、结果合并。

M5-01: ShardingType 枚举 (inference/ast/vectorize)
M5-02: 自动分片算法 (by file/document/batch)
M5-05: 分片结果合并/聚合
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ShardingType(Enum):
    INFERENCE = "inference"
    AST = "ast"
    VECTORIZE = "vectorize"


class ShardingStrategy(Enum):
    BY_FILE = "by_file"
    BY_DOCUMENT = "by_document"
    BY_BATCH = "by_batch"


@dataclass
class TaskShard:
    """任务分片。"""

    shard_id: str
    parent_task_id: str
    sharding_type: ShardingType
    shard_index: int
    total_shards: int
    payload: Dict[str, Any] = field(default_factory=dict)
    assigned_node_id: str = ""
    status: str = "pending"
    result: Dict[str, Any] = field(default_factory=dict)
    created_at: float = 0.0
    completed_at: float = 0.0
    error: str = ""

    def __post_init__(self):
        if self.created_at == 0.0:
            self.created_at = time.time()


@dataclass
class ShardResult:
    """分片执行结果。"""

    shard_id: str
    success: bool
    data: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    duration_ms: float = 0.0


@dataclass
class MergedResult:
    """合并后的最终结果。"""

    task_id: str
    sharding_type: ShardingType
    total_shards: int
    success_count: int
    fail_count: int
    data: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    merged_at: float = 0.0

    def __post_init__(self):
        if self.merged_at == 0.0:
            self.merged_at = time.time()

    @property
    def is_complete(self) -> bool:
        return (self.success_count + self.fail_count) == self.total_shards

    @property
    def success_rate(self) -> float:
        if self.total_shards == 0:
            return 0.0
        return self.success_count / self.total_shards


class TaskSharder:
    """任务分片管理器。

    支持三种分片类型和三种分片策略，自动根据任务特征选择分片方式。
    """

    DEFAULT_STRATEGIES: Dict[ShardingType, ShardingStrategy] = {
        ShardingType.INFERENCE: ShardingStrategy.BY_BATCH,
        ShardingType.AST: ShardingStrategy.BY_FILE,
        ShardingType.VECTORIZE: ShardingStrategy.BY_DOCUMENT,
    }

    DEFAULT_SHARD_SIZES: Dict[ShardingType, int] = {
        ShardingType.INFERENCE: 8,
        ShardingType.AST: 1,
        ShardingType.VECTORIZE: 10,
    }

    def __init__(
        self,
        default_shard_size: int = 8,
        max_shards: int = 64,
    ):
        self.default_shard_size = default_shard_size
        self.max_shards = max_shards
        self._shards: Dict[str, List[TaskShard]] = {}
        logger.info(f"TaskSharder 初始化: shard_size={default_shard_size}, max={max_shards}")

    def create_shards(
        self,
        task_id: str,
        sharding_type: ShardingType,
        items: List[Dict[str, Any]],
        strategy: Optional[ShardingStrategy] = None,
        shard_size: Optional[int] = None,
    ) -> List[TaskShard]:
        """根据策略创建分片。"""
        if not items:
            logger.warning(f"分片输入为空: {task_id}")
            return []

        strat = strategy or self.DEFAULT_STRATEGIES.get(sharding_type, ShardingStrategy.BY_BATCH)
        size = shard_size or self.DEFAULT_SHARD_SIZES.get(sharding_type, self.default_shard_size)

        groups = self._group_items(items, strat, size)
        total = len(groups)

        if total > self.max_shards:
            logger.warning(f"分片数 {total} 超过上限 {self.max_shards}，合并尾部分片")
            tail_items = [item for g in groups[self.max_shards - 1:] for item in g]
            groups = groups[:self.max_shards - 1] + [tail_items]
            total = len(groups)

        shards = []
        for idx, group in enumerate(groups):
            shard_id = f"{task_id}_shard_{idx}"
            shard = TaskShard(
                shard_id=shard_id,
                parent_task_id=task_id,
                sharding_type=sharding_type,
                shard_index=idx,
                total_shards=total,
                payload={"items": group, "strategy": strat.value},
            )
            shards.append(shard)

        self._shards[task_id] = shards
        logger.info(f"任务分片: {task_id} → {total} 分片 (type={sharding_type.value}, strategy={strat.value})")
        return shards

    def _group_items(
        self,
        items: List[Dict[str, Any]],
        strategy: ShardingStrategy,
        shard_size: int,
    ) -> List[List[Dict[str, Any]]]:
        """按策略分组。"""
        if strategy == ShardingStrategy.BY_FILE:
            return self._group_by_key(items, "file_path", shard_size)
        elif strategy == ShardingStrategy.BY_DOCUMENT:
            return self._group_by_key(items, "document_id", shard_size)
        else:
            return self._group_by_batch(items, shard_size)

    def _group_by_key(
        self,
        items: List[Dict[str, Any]],
        key: str,
        shard_size: int,
    ) -> List[List[Dict[str, Any]]]:
        """按键值分组。"""
        key_groups: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            k = item.get(key, "__default__")
            key_groups.setdefault(k, []).append(item)

        result = []
        batch: List[Dict[str, Any]] = []
        for group_items in key_groups.values():
            for item in group_items:
                batch.append(item)
                if len(batch) >= shard_size:
                    result.append(batch)
                    batch = []
        if batch:
            result.append(batch)
        return result

    def _group_by_batch(
        self,
        items: List[Dict[str, Any]],
        shard_size: int,
    ) -> List[List[Dict[str, Any]]]:
        """按固定大小分批。"""
        result = []
        for i in range(0, len(items), shard_size):
            result.append(items[i:i + shard_size])
        return result

    def get_shards(self, task_id: str) -> List[TaskShard]:
        return self._shards.get(task_id, [])

    def get_shard(self, shard_id: str) -> Optional[TaskShard]:
        for shards in self._shards.values():
            for s in shards:
                if s.shard_id == shard_id:
                    return s
        return None

    def update_shard_status(self, shard_id: str, status: str, result: Optional[Dict[str, Any]] = None) -> bool:
        """更新分片状态。"""
        shard = self.get_shard(shard_id)
        if not shard:
            return False
        shard.status = status
        if result:
            shard.result = result
        if status in ("completed", "failed"):
            shard.completed_at = time.time()
        logger.debug(f"分片状态更新: {shard_id} → {status}")
        return True


class ShardMerger:
    """分片结果合并器。"""

    MERGE_STRATEGIES: Dict[ShardingType, str] = {
        ShardingType.INFERENCE: "concat_results",
        ShardingType.AST: "merge_trees",
        ShardingType.VECTORIZE: "merge_embeddings",
    }

    def merge(self, task_id: str, shards: List[TaskShard]) -> MergedResult:
        """合并分片结果。"""
        if not shards:
            return MergedResult(
                task_id=task_id,
                sharding_type=ShardingType.INFERENCE,
                total_shards=0,
                success_count=0,
                fail_count=0,
            )

        sharding_type = shards[0].sharding_type
        success_count = 0
        fail_count = 0
        errors: List[str] = []
        all_data: List[Dict[str, Any]] = []

        for shard in shards:
            if shard.status == "completed":
                success_count += 1
                all_data.append(shard.result)
            else:
                fail_count += 1
                if shard.error:
                    errors.append(f"shard_{shard.shard_index}: {shard.error}")

        merge_strategy = self.MERGE_STRATEGIES.get(sharding_type, "concat_results")
        merged_data = self._apply_merge(all_data, merge_strategy)

        result = MergedResult(
            task_id=task_id,
            sharding_type=sharding_type,
            total_shards=len(shards),
            success_count=success_count,
            fail_count=fail_count,
            data=merged_data,
            errors=errors,
        )
        logger.info(
            f"分片合并: {task_id} ({sharding_type.value}) "
            f"成功={success_count}/{len(shards)}, 策略={merge_strategy}"
        )
        return result

    def _apply_merge(self, all_data: List[Dict[str, Any]], strategy: str) -> Dict[str, Any]:
        """应用合并策略。"""
        if strategy == "merge_trees":
            return self._merge_ast_trees(all_data)
        elif strategy == "merge_embeddings":
            return self._merge_embeddings(all_data)
        else:
            return self._concat_results(all_data)

    def _concat_results(self, all_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """推理结果拼接。"""
        results = []
        for d in all_data:
            if "results" in d:
                results.extend(d["results"])
            elif "items" in d:
                results.extend(d["items"])
            else:
                results.append(d)
        return {"results": results, "count": len(results)}

    def _merge_ast_trees(self, all_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """AST 树合并。"""
        trees = []
        for d in all_data:
            if "tree" in d:
                trees.append(d["tree"])
            elif "trees" in d:
                trees.extend(d["trees"])
            elif "ast" in d:
                trees.append(d["ast"])
        return {"trees": trees, "file_count": len(trees)}

    def _merge_embeddings(self, all_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """向量化结果合并。"""
        embeddings = []
        total_tokens = 0
        for d in all_data:
            if "embeddings" in d:
                embeddings.extend(d["embeddings"])
            total_tokens += d.get("token_count", 0)
        return {"embeddings": embeddings, "count": len(embeddings), "total_tokens": total_tokens}

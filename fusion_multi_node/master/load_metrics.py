"""LoadMetrics + LoadRouter — 结构化负载采集与感知路由。

P0 核心调度模块：
- LoadMetrics: 五维负载指标 (uma_used_ratio, cpu_percent, metal_util, task_queue_len, net_rtt_ms)
- LoadRouter: 结构化负载感知路由，替代 NodeInfo.score 简单评分
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class LoadMetrics:
    """节点负载五维指标。"""

    uma_used_ratio: float = 0.0
    cpu_percent: float = 0.0
    metal_util: float = 0.0
    task_queue_len: int = 0
    net_rtt_ms: float = 0.0
    timestamp: float = 0.0
    node_id: str = ""

    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()

    @property
    def is_stale(self) -> bool:
        return (time.time() - self.timestamp) > 30.0

    @property
    def uma_available_ratio(self) -> float:
        return max(0.0, 1.0 - self.uma_used_ratio)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uma_used_ratio": self.uma_used_ratio,
            "cpu_percent": self.cpu_percent,
            "metal_util": self.metal_util,
            "task_queue_len": self.task_queue_len,
            "net_rtt_ms": self.net_rtt_ms,
            "timestamp": self.timestamp,
            "node_id": self.node_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> LoadMetrics:
        return cls(
            uma_used_ratio=data.get("uma_used_ratio", 0.0),
            cpu_percent=data.get("cpu_percent", 0.0),
            metal_util=data.get("metal_util", 0.0),
            task_queue_len=data.get("task_queue_len", 0),
            net_rtt_ms=data.get("net_rtt_ms", 0.0),
            timestamp=data.get("timestamp", 0.0),
            node_id=data.get("node_id", ""),
        )


class RoutingStrategy(Enum):
    BALANCED = "balanced"
    VRAM_FIRST = "vram_first"
    LOCALITY_FIRST = "locality_first"
    LOW_LATENCY = "low_latency"


@dataclass
class RoutingWeights:
    """路由权重配置。"""

    uma_weight: float = 0.3
    cpu_weight: float = 0.2
    metal_weight: float = 0.15
    queue_weight: float = 0.2
    net_weight: float = 0.15

    def validate(self) -> bool:
        total = self.uma_weight + self.cpu_weight + self.metal_weight + self.queue_weight + self.net_weight
        return abs(total - 1.0) < 0.01


STRATEGY_WEIGHTS: Dict[RoutingStrategy, RoutingWeights] = {
    RoutingStrategy.BALANCED: RoutingWeights(),
    RoutingStrategy.VRAM_FIRST: RoutingWeights(
        uma_weight=0.5, cpu_weight=0.15, metal_weight=0.2, queue_weight=0.1, net_weight=0.05,
    ),
    RoutingStrategy.LOCALITY_FIRST: RoutingWeights(
        uma_weight=0.15, cpu_weight=0.15, metal_weight=0.1, queue_weight=0.1, net_weight=0.5,
    ),
    RoutingStrategy.LOW_LATENCY: RoutingWeights(
        uma_weight=0.1, cpu_weight=0.1, metal_weight=0.1, queue_weight=0.2, net_weight=0.5,
    ),
}


@dataclass
class RoutingResult:
    """路由决策结果。"""

    node_id: str
    score: float
    strategy: RoutingStrategy
    metrics: LoadMetrics
    breakdown: Dict[str, float] = field(default_factory=dict)


class LoadRouter:
    """结构化负载感知路由器。

    基于 LoadMetrics 五维指标计算节点评分，替代 NodeInfo.score 简单公式。
    支持多种路由策略（均衡/VRAM优先/本地优先/低延迟）。
    """

    def __init__(
        self,
        strategy: RoutingStrategy = RoutingStrategy.BALANCED,
        stale_threshold: float = 30.0,
        queue_capacity: int = 8,
    ):
        self.strategy = strategy
        self.stale_threshold = stale_threshold
        self.queue_capacity = queue_capacity
        self._metrics: Dict[str, LoadMetrics] = {}
        self._weights = STRATEGY_WEIGHTS.get(strategy, RoutingWeights())
        logger.info(f"LoadRouter 初始化: 策略={strategy.value}, 权重校验={self._weights.validate()}")

    def set_strategy(self, strategy: RoutingStrategy) -> None:
        self.strategy = strategy
        self._weights = STRATEGY_WEIGHTS.get(strategy, RoutingWeights())
        logger.info(f"LoadRouter 策略切换: {strategy.value}")

    def update_metrics(self, node_id: str, metrics: LoadMetrics) -> None:
        metrics.node_id = node_id
        metrics.timestamp = time.time()
        self._metrics[node_id] = metrics
        logger.debug(f"负载指标更新: {node_id} uma={metrics.uma_used_ratio:.2f} cpu={metrics.cpu_percent:.1f}%")

    def remove_node(self, node_id: str) -> None:
        self._metrics.pop(node_id, None)

    def get_metrics(self, node_id: str) -> Optional[LoadMetrics]:
        return self._metrics.get(node_id)

    def compute_score(self, metrics: LoadMetrics, weights: Optional[RoutingWeights] = None) -> float:
        """计算节点综合评分 (0~1, 越高越优先)。"""
        w = weights or self._weights

        uma_score = max(0.0, 1.0 - metrics.uma_used_ratio)
        cpu_score = max(0.0, 1.0 - metrics.cpu_percent / 100.0)
        metal_score = max(0.0, 1.0 - metrics.metal_util)
        queue_score = max(0.0, 1.0 - metrics.task_queue_len / max(self.queue_capacity, 1))
        net_score = max(0.0, 1.0 - min(metrics.net_rtt_ms / 100.0, 1.0))

        score = (
            uma_score * w.uma_weight
            + cpu_score * w.cpu_weight
            + metal_score * w.metal_weight
            + queue_score * w.queue_weight
            + net_score * w.net_weight
        )
        return max(0.0, min(1.0, score))

    def score_breakdown(self, metrics: LoadMetrics, weights: Optional[RoutingWeights] = None) -> Dict[str, float]:
        """返回各维度评分明细。"""
        w = weights or self._weights
        uma_score = max(0.0, 1.0 - metrics.uma_used_ratio)
        cpu_score = max(0.0, 1.0 - metrics.cpu_percent / 100.0)
        metal_score = max(0.0, 1.0 - metrics.metal_util)
        queue_score = max(0.0, 1.0 - metrics.task_queue_len / max(self.queue_capacity, 1))
        net_score = max(0.0, 1.0 - min(metrics.net_rtt_ms / 100.0, 1.0))
        return {
            "uma": uma_score * w.uma_weight,
            "cpu": cpu_score * w.cpu_weight,
            "metal": metal_score * w.metal_weight,
            "queue": queue_score * w.queue_weight,
            "net": net_score * w.net_weight,
            "total": self.compute_score(metrics, w),
        }

    def select_best(
        self,
        candidate_ids: List[str],
        preferred_node_id: str = "",
        required_uma_ratio: float = 0.0,
    ) -> Optional[RoutingResult]:
        """从候选节点中选择最优节点。"""
        now = time.time()
        candidates: List[RoutingResult] = []

        for nid in candidate_ids:
            m = self._metrics.get(nid)
            if not m:
                logger.debug(f"节点 {nid} 无负载指标，跳过")
                continue
            if now - m.timestamp > self.stale_threshold:
                logger.debug(f"节点 {nid} 指标过期，跳过")
                continue
            if required_uma_ratio > 0 and m.uma_available_ratio < required_uma_ratio:
                logger.debug(f"节点 {nid} UMA不足 ({m.uma_available_ratio:.2f} < {required_uma_ratio:.2f})")
                continue

            score = self.compute_score(m)
            preferred_bonus = 0.1 if nid == preferred_node_id else 0.0
            final_score = min(1.0, score + preferred_bonus)
            breakdown = self.score_breakdown(m)
            if nid == preferred_node_id:
                breakdown["preferred_bonus"] = preferred_bonus

            candidates.append(RoutingResult(
                node_id=nid,
                score=final_score,
                strategy=self.strategy,
                metrics=m,
                breakdown=breakdown,
            ))

        if not candidates:
            return None

        candidates.sort(key=lambda r: r.score, reverse=True)
        best = candidates[0]
        logger.debug(
            f"路由选择: {best.node_id} (score={best.score:.3f}, "
            f"uma_avail={best.metrics.uma_available_ratio:.2f}, "
            f"net_rtt={best.metrics.net_rtt_ms:.1f}ms)"
        )
        return best

    def select_n(
        self,
        candidate_ids: List[str],
        count: int = 1,
        preferred_node_id: str = "",
        required_uma_ratio: float = 0.0,
    ) -> List[RoutingResult]:
        """选择 top-N 个最优节点。"""
        now = time.time()
        candidates: List[RoutingResult] = []

        for nid in candidate_ids:
            m = self._metrics.get(nid)
            if not m:
                continue
            if now - m.timestamp > self.stale_threshold:
                continue
            if required_uma_ratio > 0 and m.uma_available_ratio < required_uma_ratio:
                continue

            score = self.compute_score(m)
            preferred_bonus = 0.1 if nid == preferred_node_id else 0.0
            final_score = min(1.0, score + preferred_bonus)
            breakdown = self.score_breakdown(m)
            if nid == preferred_node_id:
                breakdown["preferred_bonus"] = preferred_bonus

            candidates.append(RoutingResult(
                node_id=nid,
                score=final_score,
                strategy=self.strategy,
                metrics=m,
                breakdown=breakdown,
            ))

        candidates.sort(key=lambda r: r.score, reverse=True)
        return candidates[:count]

    def get_cluster_load_summary(self) -> Dict[str, Any]:
        """获取集群负载摘要。"""
        if not self._metrics:
            return {"node_count": 0, "avg_score": 0.0}

        scores = [self.compute_score(m) for m in self._metrics.values()]
        avg_uma = sum(m.uma_used_ratio for m in self._metrics.values()) / len(self._metrics)
        avg_cpu = sum(m.cpu_percent for m in self._metrics.values()) / len(self._metrics)
        avg_queue = sum(m.task_queue_len for m in self._metrics.values()) / len(self._metrics)
        avg_rtt = sum(m.net_rtt_ms for m in self._metrics.values()) / len(self._metrics)

        return {
            "node_count": len(self._metrics),
            "strategy": self.strategy.value,
            "avg_score": sum(scores) / len(scores),
            "min_score": min(scores),
            "max_score": max(scores),
            "avg_uma_used": avg_uma,
            "avg_cpu_percent": avg_cpu,
            "avg_queue_len": avg_queue,
            "avg_net_rtt_ms": avg_rtt,
        }

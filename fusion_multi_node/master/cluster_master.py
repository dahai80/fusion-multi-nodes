"""Cluster Master — 全局唯一主调度节点。

核心职责：
- 集群节点自动发现（LAN P2P + Thunderbolt）
- 全局资源打分调度器
- 流水线/数据并行任务分配
- 全局 KV 缓存池管理
- 任务生命周期管控（超时熔断、故障迁移）
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# ── 数据模型 ──

class NodeStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    BUSY = "busy"
    ERROR = "error"


class ParallelMode(Enum):
    PIPELINE = "pipeline"
    DATA = "data"


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    MIGRATED = "migrated"
    TIMEOUT = "timeout"


@dataclass
class NodeInfo:
    """集群节点信息。"""
    node_id: str
    hostname: str
    ip_address: str
    port: int
    arch: str = "arm64"
    total_memory_gb: float = 0.0
    available_memory_gb: float = 0.0
    cpu_cores: int = 0
    mlx_version: str = ""
    gpu_cores: int = 0
    status: NodeStatus = NodeStatus.OFFLINE
    last_heartbeat: float = 0.0
    tags: List[str] = field(default_factory=list)
    active_tasks: int = 0
    max_tasks: int = 4
    network_rtt_ms: float = 0.0

    @property
    def score(self) -> float:
        """资源评分（越高越优先分配任务）。"""
        mem_score = self.available_memory_gb / max(self.total_memory_gb, 1)
        task_score = 1.0 - (self.active_tasks / max(self.max_tasks, 1))
        net_penalty = min(self.network_rtt_ms / 100.0, 1.0)
        return (mem_score * 0.4 + task_score * 0.4) * (1.0 - net_penalty * 0.2)


@dataclass
class ClusterTask:
    """集群任务定义。"""
    task_id: str
    name: str
    mode: ParallelMode
    model_name: str = ""
    model_shards: List[Dict[str, Any]] = field(default_factory=list)
    assigned_nodes: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0
    timeout_seconds: float = 300.0
    error: str = ""
    user: str = ""


@dataclass
class KVCacheEntry:
    """全局 KV 缓存条目。"""
    cache_id: str
    model_name: str
    node_id: str
    created_at: float
    size_mb: float
    ttl_seconds: float = 3600.0
    access_count: int = 0


# ── Cluster Master ──

class ClusterMaster:
    """集群主调度节点 — 全局唯一。

    管理集群生命周期：发现节点 → 健康检查 → 任务调度 → 故障迁移。
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9753,
        discovery_port: int = 9754,
        heartbeat_timeout: float = 15.0,
    ):
        self.host = host
        self.port = port
        self.discovery_port = discovery_port
        self.heartbeat_timeout = heartbeat_timeout

        # 集群状态
        self.nodes: Dict[str, NodeInfo] = {}
        self.tasks: Dict[str, ClusterTask] = {}
        self.kv_cache: Dict[str, KVCacheEntry] = {}

        # 内部状态
        self._running = False
        self._server: Optional[asyncio.AbstractServer] = None

    # ── 节点管理 ──

    def register_node(self, info: NodeInfo) -> None:
        """注册或更新节点。"""
        info.status = NodeStatus.ONLINE
        info.last_heartbeat = time.time()
        self.nodes[info.node_id] = info
        logger.info(f"节点注册: {info.hostname} ({info.ip_address}:{info.port})")

    def unregister_node(self, node_id: str) -> None:
        """注销节点。"""
        self.nodes.pop(node_id, None)
        logger.info(f"节点离线: {node_id}")

    def get_online_nodes(self) -> List[NodeInfo]:
        """获取所有在线节点。"""
        now = time.time()
        online = []
        for node in self.nodes.values():
            if node.status == NodeStatus.ONLINE:
                if now - node.last_heartbeat < self.heartbeat_timeout:
                    online.append(node)
                else:
                    node.status = NodeStatus.OFFLINE
                    logger.warning(f"节点心跳超时: {node.hostname}")
        return online

    def check_heartbeat(self, node_id: str) -> bool:
        """检查节点心跳是否超时。"""
        node = self.nodes.get(node_id)
        if not node:
            return False
        now = time.time()
        if now - node.last_heartbeat > self.heartbeat_timeout:
            node.status = NodeStatus.OFFLINE
            return False
        return True

    # ── 资源调度 ──

    def select_nodes(
        self,
        mode: ParallelMode,
        required_memory_gb: float = 0.0,
        count: int = 1,
    ) -> List[NodeInfo]:
        """根据策略选择最优节点。

        Args:
            mode: 并行模式（pipeline/data）
            required_memory_gb: 所需内存（GB）
            count: 需要节点数

        Returns:
            排序后的节点列表
        """
        candidates = self.get_online_nodes()

        if required_memory_gb > 0:
            candidates = [n for n in candidates if n.available_memory_gb >= required_memory_gb]

        if mode == ParallelMode.PIPELINE:
            # 流水线并行：按剩余内存降序，适合模型分片
            candidates.sort(key=lambda n: n.score, reverse=True)
        else:
            # 数据并行：按负载升序，适合批量任务
            candidates.sort(key=lambda n: (n.active_tasks, -n.score))

        return candidates[:count]

    def assign_task(self, task: ClusterTask) -> bool:
        """分配任务到节点。"""
        required_mem = self._estimate_memory(task)
        nodes = self.select_nodes(task.mode, required_memory_gb=required_mem, count=len(task.model_shards) or 1)

        if len(nodes) < (len(task.model_shards) or 1):
            logger.error(f"可用节点不足: 需要 {len(task.model_shards) or 1}, 可用 {len(nodes)}")
            return False

        task.assigned_nodes = [n.node_id for n in nodes]
        task.status = TaskStatus.RUNNING
        task.started_at = time.time()
        self.tasks[task.task_id] = task

        for node in nodes:
            node.active_tasks += 1

        logger.info(f"任务分配: {task.name} → {[n.hostname for n in nodes]}")
        return True

    def complete_task(self, task_id: str, error: str = "") -> None:
        """完成任务。"""
        task = self.tasks.get(task_id)
        if not task:
            return
        task.status = TaskStatus.COMPLETED if not error else TaskStatus.FAILED
        task.completed_at = time.time()
        task.error = error

        for nid in task.assigned_nodes:
            node = self.nodes.get(nid)
            if node:
                node.active_tasks = max(0, node.active_tasks - 1)

    def migrate_task(self, task_id: str) -> bool:
        """故障迁移任务到其他节点。"""
        task = self.tasks.get(task_id)
        if not task or task.status != TaskStatus.RUNNING:
            return False

        logger.info(f"迁移任务: {task.name} ({task_id})")
        task.status = TaskStatus.MIGRATED
        # 释放原节点
        for nid in task.assigned_nodes:
            node = self.nodes.get(nid)
            if node:
                node.active_tasks = max(0, node.active_tasks - 1)

        # 重新分配
        task.assigned_nodes = []
        task.status = TaskStatus.PENDING
        return self.assign_task(task)

    def check_timeouts(self) -> List[str]:
        """检查并处理超时任务。"""
        now = time.time()
        timed_out = []
        for tid, task in self.tasks.items():
            if task.status == TaskStatus.RUNNING and task.started_at > 0:
                if now - task.started_at > task.timeout_seconds:
                    task.status = TaskStatus.TIMEOUT
                    task.error = f"任务超时 ({task.timeout_seconds}s)"
                    timed_out.append(tid)
                    logger.warning(f"任务超时: {task.name} ({tid})")
        return timed_out

    def _estimate_memory(self, task: ClusterTask) -> float:
        """估算任务所需内存。"""
        base = 2.0  # 基础内存
        if task.model_name:
            base += 4.0  # 模型基础占用
            if "70b" in task.model_name.lower():
                base += 32.0
            elif "13b" in task.model_name.lower():
                base += 8.0
            elif "8b" in task.model_name.lower():
                base += 4.0
            elif "3b" in task.model_name.lower():
                base += 2.0
        return base

    # ── KV 缓存管理 ──

    def register_kv_cache(self, entry: KVCacheEntry) -> None:
        """注册 KV 缓存。"""
        self.kv_cache[entry.cache_id] = entry
        logger.info(f"KV 缓存注册: {entry.model_name} @ {entry.node_id} ({entry.size_mb:.1f}MB)")

    def find_kv_cache(self, model_name: str) -> Optional[KVCacheEntry]:
        """查找可复用的 KV 缓存。"""
        now = time.time()
        for cid, entry in list(self.kv_cache.items()):
            if entry.model_name == model_name and now - entry.created_at < entry.ttl_seconds:
                node = self.nodes.get(entry.node_id)
                if node and node.status == NodeStatus.ONLINE:
                    entry.access_count += 1
                    return entry
            # 清理过期
            if now - entry.created_at > entry.ttl_seconds:
                self.kv_cache.pop(cid, None)
        return None

    # ── 生命周期 ──

    async def start(self) -> None:
        """启动集群主节点服务。"""
        self._running = True
        logger.info(f"Cluster Master 启动: {self.host}:{self.port}")
        logger.info(f"节点发现端口: {self.discovery_port}")

        # 启动后台健康检查
        asyncio.create_task(self._health_check_loop())

    async def stop(self) -> None:
        """停止集群主节点。"""
        self._running = False
        logger.info("Cluster Master 已停止")

    async def _health_check_loop(self) -> None:
        """后台健康检查循环。"""
        while self._running:
            await asyncio.sleep(10)
            self.check_timeouts()
            # 统计
            online = len(self.get_online_nodes())
            active = sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING)
            logger.debug(f"集群状态: {online} 在线, {active} 活跃任务")

    # ── 统计信息 ──

    def get_stats(self) -> Dict[str, Any]:
        """获取集群统计信息。"""
        online_nodes = self.get_online_nodes()
        return {
            "total_nodes": len(self.nodes),
            "online_nodes": len(online_nodes),
            "total_tasks": len(self.tasks),
            "active_tasks": sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING),
            "completed_tasks": sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED),
            "failed_tasks": sum(1 for t in self.tasks.values() if t.status in (TaskStatus.FAILED, TaskStatus.TIMEOUT)),
            "kv_cache_entries": len(self.kv_cache),
            "total_memory_gb": sum(n.total_memory_gb for n in online_nodes),
            "available_memory_gb": sum(n.available_memory_gb for n in online_nodes),
        }
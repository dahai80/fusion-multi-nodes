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
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

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
        host: str = "127.0.0.1",
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
        self._lock = asyncio.Lock()
        self._health_task: Optional[asyncio.Task] = None
        self._retry_task: Optional[asyncio.Task] = None
        self._pending_retry: List[ClusterTask] = []
        self._max_retry_attempts = 3
        self._max_completed_tasks = 1000
        self._max_kv_cache = 500

    # ── 节点管理 ──

    async def register_node(self, info: NodeInfo) -> None:
        """注册或更新节点。"""
        async with self._lock:
            info.status = NodeStatus.ONLINE
            info.last_heartbeat = time.time()
            self.nodes[info.node_id] = info
            logger.info(f"节点注册: {info.hostname} ({info.ip_address}:{info.port})")

    async def unregister_node(self, node_id: str) -> None:
        """注销节点。"""
        async with self._lock:
            self.nodes.pop(node_id, None)
            logger.info(f"节点离线: {node_id}")

    async def get_online_nodes(self) -> List[NodeInfo]:
        """获取所有在线节点。"""
        now = time.time()
        online = []
        async with self._lock:
            for node in self.nodes.values():
                if node.status == NodeStatus.ONLINE:
                    if now - node.last_heartbeat < self.heartbeat_timeout:
                        online.append(node)
                    else:
                        node.status = NodeStatus.OFFLINE
                        logger.warning(f"节点心跳超时: {node.hostname}")
        return online

    async def check_heartbeat(self, node_id: str) -> bool:
        """检查节点心跳是否超时。"""
        async with self._lock:
            node = self.nodes.get(node_id)
            if not node:
                return False
            now = time.time()
            if now - node.last_heartbeat > self.heartbeat_timeout:
                node.status = NodeStatus.OFFLINE
                return False
            return True

    # ── 资源调度 ──

    async def select_nodes(
        self,
        mode: ParallelMode,
        required_memory_gb: float = 0.0,
        count: int = 1,
    ) -> List[NodeInfo]:
        """根据策略选择最优节点。"""
        candidates = await self.get_online_nodes()

        if required_memory_gb > 0:
            candidates = [n for n in candidates if n.available_memory_gb >= required_memory_gb]

        if mode == ParallelMode.PIPELINE:
            candidates.sort(key=lambda n: n.score, reverse=True)
        else:
            candidates.sort(key=lambda n: (n.active_tasks, -n.score))

        return candidates[:count]

    async def complete_task(self, task_id: str, error: str = "") -> None:
        """完成任务。"""
        async with self._lock:
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

    async def assign_task(self, task: ClusterTask) -> bool:
        """分配任务到节点 — 幂等: 已 RUNNING 的任务直接返回 True。"""
        async with self._lock:
            existing = self.tasks.get(task.task_id)
            if existing and existing.status == TaskStatus.RUNNING:
                logger.debug(f"任务已分配，跳过: {task.task_id}")
                return True

        required_mem = self._estimate_memory(task)
        nodes = await self.select_nodes(task.mode, required_memory_gb=required_mem, count=len(task.model_shards) or 1)

        if len(nodes) < (len(task.model_shards) or 1):
            logger.error(f"可用节点不足: 需要 {len(task.model_shards) or 1}, 可用 {len(nodes)}")
            return False

        async with self._lock:
            task.assigned_nodes = [n.node_id for n in nodes]
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()
            self.tasks[task.task_id] = task

            for node in nodes:
                node.active_tasks += 1

        logger.info(f"任务分配: {task.name} → {[n.hostname for n in nodes]}")
        return True

    def _enqueue_retry(self, task: ClusterTask) -> None:
        """将任务加入重试队列。"""
        retry_count = getattr(task, "_retry_count", 0)
        if retry_count >= self._max_retry_attempts:
            task.status = TaskStatus.FAILED
            task.error = f"重试次数超限 ({self._max_retry_attempts})"
            logger.error(f"任务重试放弃: {task.name} ({task.task_id})")
            return
        task._retry_count = retry_count + 1
        task.status = TaskStatus.PENDING
        task.assigned_nodes = []
        self._pending_retry.append(task)
        logger.info(f"任务入重试队列: {task.name} ({task.task_id}), 第 {task._retry_count} 次重试")

    async def migrate_task(self, task_id: str) -> bool:
        """故障迁移任务到其他节点。"""
        async with self._lock:
            task = self.tasks.get(task_id)
            if not task or task.status != TaskStatus.RUNNING:
                return False

            logger.info(f"迁移任务: {task.name} ({task_id})")
            task.status = TaskStatus.MIGRATED
            for nid in task.assigned_nodes:
                node = self.nodes.get(nid)
                if node:
                    node.active_tasks = max(0, node.active_tasks - 1)

            task.assigned_nodes = []
            task.status = TaskStatus.PENDING

        ok = await self.assign_task(task)
        if not ok:
            self._enqueue_retry(task)
        return ok

    async def check_timeouts(self) -> List[str]:
        """检查并处理超时任务。"""
        now = time.time()
        timed_out = []
        async with self._lock:
            for tid, task in list(self.tasks.items()):
                if task.status == TaskStatus.RUNNING and task.started_at > 0:
                    if now - task.started_at > task.timeout_seconds:
                        task.status = TaskStatus.TIMEOUT
                        task.error = f"任务超时 ({task.timeout_seconds}s)"
                        timed_out.append(tid)
                        logger.warning(f"任务超时: {task.name} ({tid})")
        return timed_out

    MODEL_MEMORY_PROFILE = {
        "70b": 34.0,
        "32b": 18.0,
        "13b": 12.0,
        "8b": 8.0,
        "3b": 6.0,
        "1b": 4.0,
    }
    _BASE_MEMORY_GB = 2.0
    _DEFAULT_MODEL_MEMORY_GB = 4.0

    def _estimate_memory(self, task: ClusterTask) -> float:
        """估算任务所需内存。"""
        base = self._BASE_MEMORY_GB
        if task.model_name:
            base += self._DEFAULT_MODEL_MEMORY_GB
            name_lower = task.model_name.lower()
            for size_key, mem in self.MODEL_MEMORY_PROFILE.items():
                if size_key in name_lower:
                    base += mem - self._DEFAULT_MODEL_MEMORY_GB
                    break
        return base

    # ── KV 缓存管理 ──

    async def register_kv_cache(self, entry: KVCacheEntry) -> None:
        """注册 KV 缓存。"""
        async with self._lock:
            self.kv_cache[entry.cache_id] = entry
            if len(self.kv_cache) > self._max_kv_cache:
                oldest = min(self.kv_cache.items(), key=lambda x: x[1].created_at)
                del self.kv_cache[oldest[0]]
            logger.info(f"KV 缓存注册: {entry.model_name} @ {entry.node_id} ({entry.size_mb:.1f}MB)")

    async def find_kv_cache(self, model_name: str) -> Optional[KVCacheEntry]:
        """查找可复用的 KV 缓存。"""
        now = time.time()
        async with self._lock:
            for cid, entry in list(self.kv_cache.items()):
                if entry.model_name == model_name and now - entry.created_at < entry.ttl_seconds:
                    node = self.nodes.get(entry.node_id)
                    if node and node.status == NodeStatus.ONLINE:
                        entry.access_count += 1
                        return entry
                if now - entry.created_at > entry.ttl_seconds:
                    self.kv_cache.pop(cid, None)
        return None

    # ── 生命周期 ──

    async def start(self, with_server: bool = True, with_mdns: bool = True) -> None:
        """启动集群主节点服务。"""
        self._running = True
        logger.info(f"Cluster Master 启动: {self.host}:{self.port}")
        logger.info(f"节点发现端口: {self.discovery_port}")

        self._health_task = asyncio.create_task(self._health_check_loop())
        self._retry_task = asyncio.create_task(self._retry_loop())

        if with_mdns:
            self._start_mdns()

        if with_server:
            from fusion_multi_node.server import MasterServer
            server = MasterServer(master=self)
            await server.start(host=self.host, port=self.port)

    async def stop(self) -> None:
        """停止集群主节点。"""
        self._running = False
        for task in (self._health_task, self._retry_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._stop_mdns()
        logger.info("Cluster Master 已停止")

    def _start_mdns(self) -> None:
        """启动 mDNS 服务注册。"""
        try:
            from fusion_multi_node.discovery import MDNSDiscovery
            self._mdns = MDNSDiscovery(node_id="fusion-master")
            ok = self._mdns.register(
                port=self.port,
                properties={
                    "role": "master",
                    "discovery_port": str(self.discovery_port),
                    "host": self.host,
                },
            )
            if ok:
                logger.info("mDNS 服务注册成功")
            else:
                logger.warning("mDNS 服务注册失败，节点发现不可用")
        except Exception as e:
            logger.warning(f"mDNS 启动异常: {e}")
            self._mdns = None

    def _stop_mdns(self) -> None:
        """停止 mDNS 服务注册。"""
        if hasattr(self, "_mdns") and self._mdns:
            self._mdns.unregister()
            self._mdns = None

    async def _retry_loop(self) -> None:
        """重试队列处理循环。"""
        try:
            while self._running:
                await asyncio.sleep(30)
                if not self._pending_retry:
                    continue
                retry_tasks = self._pending_retry[:]
                self._pending_retry.clear()
                for task in retry_tasks:
                    ok = await self.assign_task(task)
                    if not ok:
                        self._enqueue_retry(task)
        except asyncio.CancelledError:
            pass

    async def _health_check_loop(self) -> None:
        """后台健康检查循环。"""
        try:
            while self._running:
                await asyncio.sleep(10)
                await self.check_timeouts()
                await self._cleanup_completed_tasks()
                await self._cleanup_offline_nodes()
                online = len(await self.get_online_nodes())
                active = sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING)
                logger.debug(f"集群状态: {online} 在线, {active} 活跃任务")
        except asyncio.CancelledError:
            pass

    async def _cleanup_completed_tasks(self) -> None:
        """清理已完成的旧任务，防止 tasks 无限增长。"""
        async with self._lock:
            terminal = [
                tid for tid, t in self.tasks.items()
                if t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.MIGRATED)
            ]
            if len(terminal) > self._max_completed_tasks:
                remove = terminal[:len(terminal) - self._max_completed_tasks]
                for tid in remove:
                    del self.tasks[tid]
                logger.debug(f"清理旧任务: {len(remove)} 个")

    async def _cleanup_offline_nodes(self) -> None:
        """清理长时间离线节点，防止 nodes 无限增长。"""
        now = time.time()
        async with self._lock:
            stale = [
                nid for nid, n in self.nodes.items()
                if n.status == NodeStatus.OFFLINE and now - n.last_heartbeat > 3600
            ]
            for nid in stale:
                del self.nodes[nid]
            if stale:
                logger.debug(f"清理离线节点: {len(stale)} 个")

    # ── 统计信息 ──

    async def get_stats(self) -> Dict[str, Any]:
        """获取集群统计信息。"""
        online_nodes = await self.get_online_nodes()
        async with self._lock:
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


# ── HA Standby ──

class StandbyMaster:
    """HA 备用主节点 — 监听主节点心跳，故障时接管。

    状态机: STANDBY → LEARNING → TAKING_OVER → ACTIVE
    - STANDBY: 等待主节点心跳
    - LEARNING: 同步主节点状态（节点、任务、KV 缓存）
    - TAKING_OVER: 主节点失联，开始接管
    - ACTIVE: 已成为主节点
    """

    class HAState(Enum):
        STANDBY = "standby"
        LEARNING = "learning"
        TAKING_OVER = "taking_over"
        ACTIVE = "active"

    def __init__(
        self,
        master_host: str,
        master_port: int = 9753,
        heartbeat_timeout: float = 30.0,
        take_over_delay: float = 10.0,
    ):
        self.master_host = master_host
        self.master_port = master_port
        self.heartbeat_timeout = heartbeat_timeout
        self.take_over_delay = take_over_delay
        self.state = self.HAState.STANDBY
        self._last_master_heartbeat: float = 0.0
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        self._promoted_master: Optional[ClusterMaster] = None
        self._lock = asyncio.Lock()
        logger.info(f"StandbyMaster 初始化: 监听 {master_host}:{master_port}")

    async def start(self) -> None:
        """启动备用主节点监控。"""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("StandbyMaster 启动，进入 STANDBY 状态")

    async def stop(self) -> None:
        """停止备用主节点。"""
        self._running = False
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("StandbyMaster 已停止")

    def on_master_heartbeat(self) -> None:
        """收到主节点心跳，更新时间戳。"""
        self._last_master_heartbeat = time.time()
        if self.state == self.HAState.STANDBY:
            self.state = self.HAState.LEARNING
            logger.info("StandbyMaster 状态: STANDBY → LEARNING")

    async def _monitor_loop(self) -> None:
        """监控主节点心跳，超时后接管。"""
        try:
            while self._running:
                await asyncio.sleep(5)
                now = time.time()
                if self._last_master_heartbeat == 0.0:
                    continue
                elapsed = now - self._last_master_heartbeat
                if self.state == self.HAState.LEARNING and elapsed > self.heartbeat_timeout:
                    logger.warning(
                        f"主节点心跳超时 ({elapsed:.1f}s > {self.heartbeat_timeout}s)，准备接管"
                    )
                    self.state = self.HAState.TAKING_OVER
                    await asyncio.sleep(self.take_over_delay)
                    if self._last_master_heartbeat < now - self.heartbeat_timeout:
                        await self._take_over()
        except asyncio.CancelledError:
            pass

    async def _take_over(self) -> None:
        """接管成为主节点。"""
        async with self._lock:
            if self.state == self.HAState.ACTIVE:
                return
            self._promoted_master = ClusterMaster(
                host="0.0.0.0",
                port=self.master_port,
            )
            await self._promoted_master.start()
            self.state = self.HAState.ACTIVE
            logger.warning("StandbyMaster 已接管成为主节点 (ACTIVE)")

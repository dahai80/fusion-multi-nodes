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

from fusion_multi_node.master.load_metrics import LoadMetrics, LoadRouter, RoutingStrategy

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
    device_model: str = ""
    uma_size_gb: float = 0.0
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
    # M3-05 TaskSpec 结构化能力匹配
    required_capability: str = ""
    preferred_node_id: str = ""
    priority: int = 0
    # M4-04 任务自动降级
    degraded_from_model: str = ""
    degradation_count: int = 0
    max_degradations: int = 2
    # M5-04 任务全生命周期取消
    sub_tasks: List[str] = field(default_factory=list)
    cancel_reason: str = ""


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

        # M4-01 负载感知路由
        self.load_router = LoadRouter(strategy=RoutingStrategy.BALANCED)

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
            # M4-01 同步负载指标到 LoadRouter
            self._sync_node_metrics(info)
            logger.info(f"节点注册: {info.hostname} ({info.ip_address}:{info.port})")

    async def unregister_node(self, node_id: str) -> None:
        """注销节点。"""
        async with self._lock:
            self.nodes.pop(node_id, None)
            self.load_router.remove_node(node_id)
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

    def _sync_node_metrics(self, info: NodeInfo) -> None:
        """从 NodeInfo 同步基础指标到 LoadRouter。"""
        uma_ratio = 1.0 - (info.available_memory_gb / max(info.total_memory_gb, 1))
        metrics = LoadMetrics(
            uma_used_ratio=uma_ratio,
            cpu_percent=0.0,
            metal_util=0.0,
            task_queue_len=info.active_tasks,
            net_rtt_ms=info.network_rtt_ms,
            node_id=info.node_id,
        )
        self.load_router.update_metrics(info.node_id, metrics)

    async def update_node_load(self, node_id: str, metrics: LoadMetrics) -> None:
        """更新节点负载指标（由 Worker 上报调用）。"""
        async with self._lock:
            node = self.nodes.get(node_id)
            if not node:
                logger.warning(f"更新负载: 节点不存在 {node_id}")
                return
            self.load_router.update_metrics(node_id, metrics)
            # 回写关键字段到 NodeInfo
            node.available_memory_gb = node.total_memory_gb * (1.0 - metrics.uma_used_ratio)
            node.active_tasks = metrics.task_queue_len
            node.network_rtt_ms = metrics.net_rtt_ms
            logger.debug(f"节点负载更新: {node_id} uma={metrics.uma_used_ratio:.2f}")

    # ── 资源调度 ──

    async def select_nodes(
        self,
        mode: ParallelMode,
        required_memory_gb: float = 0.0,
        count: int = 1,
        required_capability: str = "",
        preferred_node_id: str = "",
    ) -> List[NodeInfo]:
        """根据策略选择最优节点。M4-01 负载感知 + M4-02 本地优先。"""
        candidates = await self.get_online_nodes()

        # M3-05 capability 过滤
        if required_capability:
            candidates = [n for n in candidates if required_capability in n.tags]

        if required_memory_gb > 0:
            candidates = [n for n in candidates if n.available_memory_gb >= required_memory_gb]

        candidate_ids = [n.node_id for n in candidates]
        required_uma = required_memory_gb / max(sum(n.total_memory_gb for n in candidates) or 1, 1)

        # M4-01 优先使用 LoadRouter 结构化评分
        results = self.load_router.select_n(
            candidate_ids=candidate_ids,
            count=count,
            preferred_node_id=preferred_node_id,
            required_uma_ratio=required_uma,
        )

        if results:
            id_to_node = {n.node_id: n for n in candidates}
            selected = [id_to_node[r.node_id] for r in results if r.node_id in id_to_node]
            if selected:
                return selected

        # Fallback: 无 LoadMetrics 时退回旧逻辑
        if preferred_node_id:
            preferred = [n for n in candidates if n.node_id == preferred_node_id]
            others = [n for n in candidates if n.node_id != preferred_node_id]
            candidates = preferred + others

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
        """分配任务到节点 — 幂等: 已 RUNNING 的任务直接返回 True。

        M4-02: 轻量级任务/≤0.5B 模型强制本地执行。
        M4-03: 大模型(≥13B) 使用 VRAM 优先策略。
        """
        async with self._lock:
            existing = self.tasks.get(task.task_id)
            if existing and existing.status == TaskStatus.RUNNING:
                logger.debug(f"任务已分配，跳过: {task.task_id}")
                return True

        required_mem = self._estimate_memory(task)

        # M4-02 本地强制门控: 轻量级任务强制本地
        if self._is_local_force(task, required_mem):
            preferred = task.preferred_node_id
            if preferred and preferred in self.nodes:
                node = self.nodes[preferred]
                if node.status == NodeStatus.ONLINE and node.available_memory_gb >= required_mem:
                    async with self._lock:
                        task.assigned_nodes = [preferred]
                        task.status = TaskStatus.RUNNING
                        task.started_at = time.time()
                        self.tasks[task.task_id] = task
                        node.active_tasks += 1
                    logger.info(f"M4-02 本地强制: {task.name} → {preferred} (轻量级任务)")
                    return True
            logger.info(f"M4-02 本地强制: {task.name} 首选节点不可用，退回普通调度")

        # M4-03 VRAM 优先: 大模型切换路由策略
        original_strategy = self.load_router.strategy
        if self._is_vram_first(task):
            self.load_router.set_strategy(RoutingStrategy.VRAM_FIRST)
            logger.debug(f"M4-03 VRAM优先策略: {task.name} (model={task.model_name})")

        try:
            nodes = await self.select_nodes(
                task.mode,
                required_memory_gb=required_mem,
                count=len(task.model_shards) or 1,
                required_capability=task.required_capability,
                preferred_node_id=task.preferred_node_id,
            )
        finally:
            if self._is_vram_first(task) and self.load_router.strategy != original_strategy:
                self.load_router.set_strategy(original_strategy)

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

    # M5-04 任务全生命周期取消
    async def cancel_task(self, task_id: str, reason: str = "", cancel_sub_tasks: bool = True) -> bool:
        """取消任务及其子任务。"""
        async with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                logger.warning(f"取消任务未找到: {task_id}")
                return False
            if task.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
                logger.warning(f"任务无法取消 (状态: {task.status.value}): {task_id}")
                return False

            task.cancel_reason = reason or "用户取消"
            for nid in task.assigned_nodes:
                node = self.nodes.get(nid)
                if node:
                    node.active_tasks = max(0, node.active_tasks - 1)
            task.assigned_nodes = []

            # 递归取消子任务
            cancelled_sub = []
            if cancel_sub_tasks and task.sub_tasks:
                for sub_id in task.sub_tasks:
                    sub = self.tasks.get(sub_id)
                    if sub and sub.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                        for nid in sub.assigned_nodes:
                            node = self.nodes.get(nid)
                            if node:
                                node.active_tasks = max(0, node.active_tasks - 1)
                        sub.assigned_nodes = []
                        sub.status = TaskStatus.FAILED
                        sub.cancel_reason = f"父任务取消: {task_id}"
                        sub.error = sub.cancel_reason
                        cancelled_sub.append(sub_id)

            task.status = TaskStatus.FAILED
            task.error = task.cancel_reason

        logger.info(f"任务取消: {task_id} (原因: {task.cancel_reason}), 子任务取消: {cancelled_sub}")
        return True

    # M4-04 任务自动降级
    MODEL_DEGRADATION_CHAIN = {
        "70b": "32b",
        "32b": "13b",
        "13b": "8b",
        "8b": "3b",
        "3b": "1b",
    }

    async def degrade_task(self, task_id: str) -> bool:
        """将任务降级到更小的模型并重新分配。"""
        async with self._lock:
            task = self.tasks.get(task_id)
            if not task or task.status not in (TaskStatus.RUNNING, TaskStatus.PENDING, TaskStatus.FAILED):
                return False

            if task.degradation_count >= task.max_degradations:
                logger.error(f"任务降级次数超限: {task_id} ({task.degradation_count})")
                return False

            current_size = self._extract_model_size(task.model_name)
            next_size = self.MODEL_DEGRADATION_CHAIN.get(current_size)
            if not next_size:
                logger.warning(f"任务无法降级，无更小模型: {task.model_name}")
                return False

            # 释放原节点
            for nid in task.assigned_nodes:
                node = self.nodes.get(nid)
                if node:
                    node.active_tasks = max(0, node.active_tasks - 1)
            task.assigned_nodes = []

            # 降级模型
            old_model = task.model_name
            task.model_name = task.model_name.replace(current_size, next_size)
            task.degraded_from_model = old_model
            task.degradation_count += 1
            task.status = TaskStatus.PENDING
            task.error = ""

        logger.info(f"任务降级: {task_id} {old_model} → {task.model_name} (第{task.degradation_count}次)")

        ok = await self.assign_task(task)
        if not ok:
            self._enqueue_retry(task)
        return ok

    def _extract_model_size(self, model_name: str) -> str:
        name_lower = model_name.lower()
        for size_key in self.MODEL_DEGRADATION_CHAIN:
            if size_key in name_lower:
                return size_key
        return ""

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
        "0.5b": 2.0,
    }
    _BASE_MEMORY_GB = 2.0
    _DEFAULT_MODEL_MEMORY_GB = 4.0

    # M4-02 轻量级模型阈值 (≤0.5B 强制本地)
    _LOCAL_FORCE_MODEL_SIZES = {"0.5b", "1b"}
    # M4-02 轻量级任务内存阈值 (GB)
    _LOCAL_FORCE_MEMORY_GB = 4.0

    # M4-03 大模型阈值 (≥13B 使用 VRAM 优先策略)
    _VRAM_FIRST_MODEL_SIZES = {"70b", "32b", "13b"}

    def _is_local_force(self, task: ClusterTask, required_mem: float) -> bool:
        """M4-02 判断是否强制本地执行。"""
        if not task.model_name:
            return required_mem <= self._LOCAL_FORCE_MEMORY_GB
        name_lower = task.model_name.lower()
        for size in self._LOCAL_FORCE_MODEL_SIZES:
            if size in name_lower:
                return True
        return required_mem <= self._LOCAL_FORCE_MEMORY_GB

    def _is_vram_first(self, task: ClusterTask) -> bool:
        """M4-03 判断是否使用 VRAM 优先策略。"""
        if not task.model_name:
            return False
        name_lower = task.model_name.lower()
        for size in self._VRAM_FIRST_MODEL_SIZES:
            if size in name_lower:
                return True
        return False

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

    async def sync_kv_cache(self, cache_id: str, model_name: str, source_node_id: str, size_mb: float) -> bool:
        """通过 FMP 协议同步 KV 缓存元数据到集群。"""
        from fusion_multi_node.protocol import KVCacheSyncMessage

        sync_msg = KVCacheSyncMessage(
            cache_id=cache_id,
            model_name=model_name,
            source_node_id=source_node_id,
            size_mb=size_mb,
            protocol="fmp",
        )
        logger.info(f"M9-04 FMP KV 缓存同步: cache_id={cache_id} model={model_name} "
                    f"source={source_node_id} size={size_mb:.1f}MB protocol={sync_msg.protocol}")

        async with self._lock:
            entry = self.kv_cache.get(cache_id)
            if not entry:
                logger.warning(f"M9-04 KV 缓存同步: cache_id={cache_id} 未注册，跳过同步")
                return False

        return True

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
                    "device_model": "",
                    "uma_size_gb": "0.0",
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
            stats = {
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
            stats["load_summary"] = self.load_router.get_cluster_load_summary()
            return stats


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
                await asyncio.sleep(3)
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

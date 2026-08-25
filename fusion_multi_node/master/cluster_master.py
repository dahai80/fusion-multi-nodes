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
import os
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import httpx

from fusion_multi_node.master.election import (
    ElectionCandidate,
    MasterElection,
    VoteRequest,
    VoteResponse,
)
from fusion_multi_node.master.load_metrics import (
    LoadMetrics,
    LoadRouter,
    RoutingStrategy,
)
from fusion_multi_node.master.task_spec import TaskSpec
from fusion_multi_node.security.mtls import client_kwargs as mtls_client_kwargs
from fusion_multi_node.security.mtls import scheme as mtls_scheme
from fusion_multi_node.utils.auth import (
    build_safe_url,
    is_safe_peer_host,
    load_or_create_token,
)

logger = logging.getLogger(__name__)


# ── 数据模型 ──


class NodeStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    BUSY = "busy"
    FAULT = "fault"
    ERROR = "error"


class ParallelMode(Enum):
    PIPELINE = "pipeline"
    DATA = "data"


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
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
    role: str = "worker"
    status: NodeStatus = NodeStatus.OFFLINE
    last_heartbeat: float = 0.0
    tags: list[str] = field(default_factory=list)
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
    """集群任务 — spec 定义 + 运行时状态。"""

    task_id: str
    name: str
    mode: ParallelMode
    model_name: str = ""
    model_id: str | None = None
    model_shards: list[dict[str, Any]] = field(default_factory=list)
    assigned_nodes: list[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0
    timeout_seconds: float = 300.0
    error: str = ""
    user: str = ""
    required_capability: str = ""
    preferred_node_id: str = ""
    priority: int = 0
    # M4-04 任务自动降级
    degraded_from_model: str = ""
    degradation_count: int = 0
    max_degradations: int = 2
    # M5-04 任务全生命周期取消
    sub_tasks: list[str] = field(default_factory=list)
    cancel_reason: str = ""
    # M3-05 TaskSpec 引用
    spec: TaskSpec | None = None
    # P1 派发载荷 — Master→Agent /api/execute 下发的任务体
    # task_type 对齐 agent execute_task (inference|embedding|plugin|model_sync)
    # params 对齐 agent _execute_inference 读取 (prompt/messages/max_tokens/temperature)
    task_type: str = "inference"
    params: dict[str, Any] = field(default_factory=dict)
    # 派发结果 (单 agent 推理结果 / DATA 模式聚合结果)
    result: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_spec(cls, task_id: str, spec: TaskSpec) -> ClusterTask:
        mode = ParallelMode.PIPELINE if spec.mode == "pipeline" else ParallelMode.DATA
        return cls(
            task_id=task_id,
            name=spec.name,
            mode=mode,
            model_name=spec.model_name,
            model_shards=spec.model_shards,
            timeout_seconds=spec.timeout_seconds,
            user=spec.user,
            required_capability=spec.required_capability,
            preferred_node_id=spec.preferred_node_id,
            priority=spec.priority.value,
            spec=spec,
        )


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
        port: int = 11452,
        discovery_port: int = 11450,
        heartbeat_timeout: float = 15.0,
    ):
        self.host = host
        self.port = port
        self.discovery_port = discovery_port
        self.heartbeat_timeout = heartbeat_timeout

        # 集群状态
        self.nodes: dict[str, NodeInfo] = {}
        self.tasks: dict[str, ClusterTask] = {}
        self.kv_cache: dict[str, KVCacheEntry] = {}

        # M4-01 负载感知路由
        self.load_router = LoadRouter(strategy=RoutingStrategy.BALANCED)

        # 内部状态
        self._running = False
        self._server: asyncio.AbstractServer | None = None
        # H2: 按资源域分锁 (nodes/tasks/kv), 替代单全局锁。
        # 锁序约定: nodes -> tasks -> kv。跨域写操作须按此序同时获取所需域锁, 防死锁。
        # 注: asyncio 单线程协作, 锁仅在临界区含 await 时才实际阻塞;
        #     此处分锁主要为 master_server 等外部读取者提供快照接口
        #     (snapshot_nodes/snapshot_tasks/snapshot_kv), 避免边迭代边改 dict。
        self._nodes_lock = asyncio.Lock()
        self._tasks_lock = asyncio.Lock()
        self._kv_lock = asyncio.Lock()
        self._health_task: asyncio.Task | None = None
        self._retry_task: asyncio.Task | None = None
        self._pending_retry: list[ClusterTask] = []
        self._max_retry_attempts = 1
        self._max_completed_tasks = 1000
        self._max_kv_cache = 500
        # M3-03 选举
        self._election: MasterElection | None = None
        self._is_leader = True
        # P1 派发: Master→Agent /api/execute 投递。惰性复用 httpx + 集群 token
        # (与 master_server cancel 通知同 token, agent BearerAuthMiddleware 校验)。
        self._dispatch_token: str | None = None
        self._dispatch_http: httpx.AsyncClient | None = None
        self._dispatch_tasks: dict[str, asyncio.Task] = {}
        # F-A13 故障隔离: node_id → 解封时刻 (time.time()+_BAN_DURATION_S)。
        # report_fault 窗口内达阈值自动 ban; register_node 拒绝 ban 内节点。
        self._banned_nodes: dict[str, float] = {}
        self._fault_counts: dict[str, list[float]] = defaultdict(list)
        # H3 任务持久化: 非终态任务落盘 + 启动恢复, Master 崩溃不丢 RUNNING/PENDING。
        # 原子写 (tmp + os.replace), 与 config.py 同范式。终态任务 (COMPLETED/FAILED/
        # CANCELLED/TIMEOUT) 不持久化 — 一次性结果, 恢复时按 RUNNING 重新派发。
        self._task_store_path = Path.home() / ".fusion" / "multi-node" / "tasks.json"
        self._persist_task: asyncio.Task | None = None

    # ── 节点管理 ──

    async def register_node(self, info: NodeInfo) -> bool:
        """注册或更新节点 (F-A12 幂等: 再注册 = PATCH, 保留 Master 权威运行态字段)。

        ban 内节点拒绝注册。返回 True=放行, False=被 ban 拒绝。
        """
        async with self._nodes_lock:
            if self._is_node_banned_locked(info.node_id):
                logger.warning(
                    f"拒绝注册 banned 节点: {info.node_id} "
                    f"(解封剩余 {self._banned_nodes[info.node_id] - time.time():.0f}s)"
                )
                return False
            now = time.time()
            existing = self.nodes.get(info.node_id)
            if existing is not None:
                # 再注册 = PATCH: Master 权威运行态字段不动, 只更新硬件声明字段。
                info.status = existing.status if existing.status != NodeStatus.OFFLINE else NodeStatus.ONLINE
                info.last_heartbeat = now
                info.active_tasks = existing.active_tasks
                info.max_tasks = existing.max_tasks
                info.network_rtt_ms = existing.network_rtt_ms
                logger.info(f"节点再注册 (PATCH): {info.hostname} ({info.ip_address}:{info.port})")
            else:
                info.status = NodeStatus.ONLINE
                info.last_heartbeat = now
                logger.info(f"节点注册: {info.hostname} ({info.ip_address}:{info.port})")
            self.nodes[info.node_id] = info
            # M4-01 同步负载指标到 LoadRouter
            self._sync_node_metrics(info)
            return True

    async def unregister_node(self, node_id: str, reason: str = "") -> None:
        """注销节点 (F-A13: reason="banned" 写入黑名单 _BAN_DURATION_S)。"""
        async with self._nodes_lock:
            self.nodes.pop(node_id, None)
            self.load_router.remove_node(node_id)
            if reason == "banned":
                self._banned_nodes[node_id] = time.time() + self._BAN_DURATION_S
                logger.warning(f"节点拉黑: {node_id} ({self._BAN_DURATION_S:.0f}s)")
            else:
                logger.info(f"节点离线: {node_id} ({reason})" if reason else f"节点离线: {node_id}")

    def _is_node_banned_locked(self, node_id: str) -> bool:
        """已持 _nodes_lock 下查 ban (过期条目惰性清理)。"""
        unban_at = self._banned_nodes.get(node_id)
        if unban_at is None:
            return False
        if time.time() >= unban_at:
            self._banned_nodes.pop(node_id, None)
            self._fault_counts.pop(node_id, None)
            logger.info(f"节点 ban 到期自动解封: {node_id}")
            return False
        return True

    def is_node_banned(self, node_id: str) -> bool:
        """外部查询是否在 ban 期 (无锁读快照, 过期惰性清理交由 register 路径)。"""
        unban_at = self._banned_nodes.get(node_id)
        return unban_at is not None and time.time() < unban_at

    def unban_node(self, node_id: str) -> bool:
        """手动解封节点, 返回是否确实解封了一条 ban。"""
        removed = self._banned_nodes.pop(node_id, None) is not None
        self._fault_counts.pop(node_id, None)
        if removed:
            logger.info(f"节点手动解封: {node_id}")
        return removed

    # ── H3 任务持久化 + 启动恢复 ──

    _PERSIST_SKIP_FIELDS = {"spec"}  # spec 由 model_* 字段重建, 不直接序列化

    def _task_to_dict(self, task: ClusterTask) -> dict[str, Any]:
        d = asdict(task)
        for f in self._PERSIST_SKIP_FIELDS:
            d.pop(f, None)
        d["mode"] = task.mode.value
        d["status"] = task.status.value
        return d

    def _task_from_dict(self, d: dict[str, Any]) -> ClusterTask:
        # RUNNING 恢复为 PENDING 重派 (派发中的任务崩溃后须重新调度)
        st = d.get("status", "pending")
        if st in ("running", "migrated"):
            st = "pending"
        return ClusterTask(
            task_id=d["task_id"],
            name=d.get("name", ""),
            mode=ParallelMode(d.get("mode", "pipeline")),
            model_name=d.get("model_name", ""),
            model_id=d.get("model_id"),
            model_shards=d.get("model_shards", []),
            assigned_nodes=d.get("assigned_nodes", []),
            status=TaskStatus(st),
            created_at=d.get("created_at", 0.0),
            started_at=d.get("started_at", 0.0),
            timeout_seconds=d.get("timeout_seconds", 300.0),
            error=d.get("error", ""),
            user=d.get("user", ""),
            required_capability=d.get("required_capability", ""),
            preferred_node_id=d.get("preferred_node_id", ""),
            priority=d.get("priority", 0),
            degraded_from_model=d.get("degraded_from_model", ""),
            degradation_count=d.get("degradation_count", 0),
            max_degradations=d.get("max_degradations", 2),
            sub_tasks=d.get("sub_tasks", []),
            cancel_reason=d.get("cancel_reason", ""),
            task_type=d.get("task_type", "inference"),
            params=d.get("params", {}),
            result=d.get("result", {}),
        )

    def _persist_tasks_locked(self) -> None:
        """已持 _tasks_lock 下落盘非终态任务。终态 (COMPLETED/FAILED/CANCELLED/TIMEOUT) 不存。"""
        _TERMINAL = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT}
        try:
            pending = [self._task_to_dict(t) for t in self.tasks.values() if t.status not in _TERMINAL]
            self._task_store_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._task_store_path.with_suffix(self._task_store_path.suffix + ".tmp")
            with open(tmp, "w") as f:
                json.dump({"tasks": pending, "saved_at": time.time()}, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._task_store_path)
            logger.debug(f"H3 任务持久化: {len(pending)} 非终态任务落盘")
        except Exception as e:
            logger.error(f"H3 任务持久化失败: {e}")

    async def _persist_tasks(self) -> None:
        """加锁落盘 (状态写点调用)。HA leader 额外推送任务快照到 standby。"""
        targets: list[tuple[str, str, int, dict[str, Any]]] = []
        async with self._tasks_lock:
            self._persist_tasks_locked()
            # HA: leader 构建推送目标 (锁内构建 payload, 锁外异步发送)
            targets = self._sync_tasks_to_standbys_locked()
        if targets:
            await self._push_sync_to_standbys(targets)

    async def _restore_tasks(self) -> int:
        """启动恢复: 读盘 → 重建非终态任务 (RUNNING/MIGRATED → PENDING 重派)。返回恢复数。"""
        if not self._task_store_path.exists():
            return 0
        try:
            data = json.loads(self._task_store_path.read_text())
        except Exception as e:
            logger.error(f"H3 任务恢复读盘失败: {e}")
            return 0
        tasks = data.get("tasks", [])
        restored = 0
        async with self._tasks_lock:
            for d in tasks:
                try:
                    task = self._task_from_dict(d)
                    self.tasks[task.task_id] = task
                    restored += 1
                except Exception as e:
                    logger.warning(f"H3 任务恢复跳过 {d.get('task_id', '?')}: {e}")
        if restored:
            logger.warning(f"H3 启动恢复 {restored} 任务 (崩溃前未完成, 已置 PENDING 待重派)")
        return restored

    async def update_heartbeat(
        self,
        node_id: str,
        total_memory_gb: float | None = None,
        available_memory_gb: float | None = None,
        active_tasks: int | None = None,
    ) -> bool:
        """加锁更新节点心跳 — 禁止路由层裸改 node 字段 (与 _health_check_loop 竞态)。"""
        async with self._nodes_lock:
            node = self.nodes.get(node_id)
            if not node:
                return False
            node.last_heartbeat = time.time()
            if total_memory_gb is not None:
                node.total_memory_gb = total_memory_gb
            if available_memory_gb is not None:
                node.available_memory_gb = available_memory_gb
            if active_tasks is not None:
                node.active_tasks = active_tasks
            if node.status == NodeStatus.OFFLINE:
                node.status = NodeStatus.ONLINE
                logger.info(f"节点恢复上线: {node_id}")
            return True

    async def report_fault(self, node_id: str, fault_type: str = "", message: str = "") -> bool:
        """加锁标记节点故障 + 累计故障计数 (F-A13: 窗口内达阈值自动 ban)。"""
        async with self._nodes_lock:
            node = self.nodes.get(node_id)
            if not node:
                return False
            node.status = NodeStatus.FAULT
            now = time.time()
            counts = self._fault_counts[node_id]
            counts.append(now)
            # 惰性清理窗口外旧故障
            self._fault_counts[node_id] = [t for t in counts if now - t <= self._FAULT_WINDOW_S]
            windowed = self._fault_counts[node_id]
            logger.warning(
                f"节点故障: {node_id} [{fault_type}] {message} "
                f"(窗口内 {len(windowed)}/{self._FAULT_THRESHOLD})"
            )
            if len(windowed) >= self._FAULT_THRESHOLD:
                self._banned_nodes[node_id] = now + self._BAN_DURATION_S
                logger.warning(
                    f"节点故障达阈值自动 ban: {node_id} "
                    f"({self._BAN_DURATION_S:.0f}s) [{fault_type}]"
                )
            return True

    async def get_online_nodes(self) -> list[NodeInfo]:
        """纯快照: 返回当前 ONLINE/BUSY 且心跳未超时的节点, 不改任何状态。

        状态跃迁 (ONLINE→BUSY/OFFLINE, BUSY→ONLINE/OFFLINE) 由 _refresh_node_statuses
        在 _health_check_loop 中统一执行 — 读操作不应有副作用 (R6)。
        """
        now = time.time()
        online = []
        async with self._nodes_lock:
            for node in self.nodes.values():
                if node.status in (NodeStatus.ONLINE, NodeStatus.BUSY):
                    if now - node.last_heartbeat < self.heartbeat_timeout:
                        online.append(node)
        return online

    async def _refresh_node_statuses(self) -> None:
        """统一状态跃迁 — 仅在 _health_check_loop 调用, 不在读取路径。"""
        now = time.time()
        async with self._nodes_lock:
            for node in self.nodes.values():
                if node.status == NodeStatus.ONLINE:
                    if now - node.last_heartbeat >= self.heartbeat_timeout:
                        node.status = NodeStatus.OFFLINE
                        logger.warning(f"节点心跳超时: {node.hostname}")
                    elif node.active_tasks >= node.max_tasks:
                        node.status = NodeStatus.BUSY
                        logger.info(f"节点任务满载: {node.hostname} ({node.active_tasks}/{node.max_tasks})")
                elif node.status == NodeStatus.BUSY:
                    if now - node.last_heartbeat >= self.heartbeat_timeout:
                        node.status = NodeStatus.OFFLINE
                        logger.warning(f"节点心跳超时(BUSY): {node.hostname}")
                    elif node.active_tasks < node.max_tasks:
                        node.status = NodeStatus.ONLINE
                        logger.info(f"节点恢复空闲: {node.hostname}")

    async def check_heartbeat(self, node_id: str) -> bool:
        """检查节点心跳是否超时。"""
        async with self._nodes_lock:
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
        async with self._nodes_lock:
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
    ) -> list[NodeInfo]:
        """根据策略选择最优节点。M4-01 负载感知 + M4-02 本地优先。"""
        candidates = await self.get_online_nodes()

        # S1 熔断: 跳过 ban 期内节点 (派发失败累积达阈值自动 ban, 不再被选中)
        candidates = [n for n in candidates if not self.is_node_banned(n.node_id)]

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

    def _select_free_nodes_locked(
        self,
        mode: ParallelMode,
        required_memory_gb: float,
        required_capability: str,
        exclude_ids: set[str],
        count: int,
    ) -> list[NodeInfo]:
        """锁内补选空闲节点 — select_nodes 锁外执行后, 并发抢占致首选满载时调用。
        复用 select_nodes 的过滤/排序逻辑, 但直接读 self.nodes (已持 _nodes_lock)。
        仅返回 active_tasks < max_tasks 的在线未 ban 节点。
        """
        if count <= 0:
            return []
        candidates = [
            n
            for n in self.nodes.values()
            if n.status == NodeStatus.ONLINE
            and n.node_id not in exclude_ids
            and n.active_tasks < n.max_tasks
            and not self._is_node_banned_locked(n.node_id)
        ]
        if required_capability:
            candidates = [n for n in candidates if required_capability in n.tags]
        if required_memory_gb > 0:
            candidates = [n for n in candidates if n.available_memory_gb >= required_memory_gb]
        if mode == ParallelMode.PIPELINE:
            candidates.sort(key=lambda n: n.score, reverse=True)
        else:
            candidates.sort(key=lambda n: (n.active_tasks, -n.score))
        return candidates[:count]

    async def complete_task(self, task_id: str, error: str = "") -> None:
        """完成任务。"""
        async with self._nodes_lock:
            async with self._tasks_lock:
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
        HA: standby (选举配置且非 leader) 拒绝派发, 仅 leader 调度。
        """
        # HA standby 守卫: 选举已配置且本节点非 leader → 拒绝派发
        if self._election is not None and not self._is_leader:
            logger.warning(f"standby 模式拒绝派发任务: {task.task_id} (非 leader)")
            return False
        async with self._tasks_lock:
            existing = self.tasks.get(task.task_id)
            if existing and existing.status == TaskStatus.RUNNING:
                logger.debug(f"任务已分配，跳过: {task.task_id}")
                return True

        required_mem = self._estimate_memory(task)

        # M4-02 本地强制门控: 轻量级任务强制本地
        if self._is_local_force(task, required_mem):
            preferred = task.preferred_node_id
            if preferred and preferred in self.nodes:
                # nodes 读取也走 nodes 锁, 与 register_node 写隔离 (R6/H2)
                async with self._nodes_lock:
                    node = self.nodes.get(preferred)
                    available_ok = bool(
                        node
                        and node.status == NodeStatus.ONLINE
                        and node.available_memory_gb >= required_mem
                    )
                if available_ok:
                    async with self._nodes_lock:
                        async with self._tasks_lock:
                            # TOCTOU 再确认: 并发 assign_task 可能已分配本任务或抢空容量
                            existing = self.tasks.get(task.task_id)
                            if existing and existing.status == TaskStatus.RUNNING:
                                logger.debug(f"M4-02 本地强制: 任务已被并发分配: {task.task_id}")
                                return True
                            node = self.nodes.get(preferred)
                            if not node or node.status != NodeStatus.ONLINE or node.active_tasks >= node.max_tasks:
                                logger.info("M4-02 本地强制: 首选节点并发抢占后不可用，退回普通调度")
                            else:
                                task.assigned_nodes = [preferred]
                                task.status = TaskStatus.RUNNING
                                task.started_at = time.time()
                                self.tasks[task.task_id] = task
                                node.active_tasks += 1
                                logger.info(f"M4-02 本地强制: {task.name} → {preferred} (轻量级任务)")
                                self._trigger_dispatch(task)
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

        async with self._nodes_lock:
            async with self._tasks_lock:
                # TOCTOU 再确认: 并发 assign_task 可能已分配本任务 (双计 active_tasks)
                existing = self.tasks.get(task.task_id)
                if existing and existing.status == TaskStatus.RUNNING:
                    logger.debug(f"任务已被并发分配，跳过重复计 active_tasks: {task.task_id}")
                    return True
                # 重新确认所选节点仍在线且未满载 (select_nodes 在锁外执行)
                confirmed = []
                for n in nodes:
                    node = self.nodes.get(n.node_id)
                    if node and node.status == NodeStatus.ONLINE and node.active_tasks < node.max_tasks:
                        confirmed.append(node)
                need = len(task.model_shards) or 1
                # TOCTOU 补选: 首选节点被并发抢占满载 → 锁内补选其它空闲节点, 不直接 503。
                if len(confirmed) < need:
                    shortfall = need - len(confirmed)
                    excluded = {n.node_id for n in confirmed}
                    extra = self._select_free_nodes_locked(
                        mode=task.mode,
                        required_memory_gb=required_mem,
                        required_capability=task.required_capability,
                        exclude_ids=excluded,
                        count=shortfall,
                    )
                    if extra:
                        logger.info(
                            f"并发抢占补选: 原选 {len(confirmed)}/{need}, "
                            f"补选 {len(extra)} 节点 {[x.hostname for x in extra]}"
                        )
                        confirmed.extend(extra)
                if len(confirmed) < need:
                    logger.warning(f"并发抢占后可用节点不足: 需要 {need}, 确认 {len(confirmed)}")
                    return False
                task.assigned_nodes = [n.node_id for n in confirmed]
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()
                self.tasks[task.task_id] = task
                self._persist_tasks_locked()  # H3 即时落盘

                for node in confirmed:
                    node.active_tasks += 1

        logger.info(f"任务分配: {task.name} → {[n.hostname for n in nodes]}")
        self._trigger_dispatch(task)
        return True

    def _enqueue_retry(self, task: ClusterTask) -> None:
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

    # ── P1 Master→Agent 任务派发 ──

    def _trigger_dispatch(self, task: ClusterTask) -> None:
        """触发后台派发 — assign_task 标 RUNNING 后调用, fire-and-forget。

        旧实现 assign_task 仅标 RUNNING 填 assigned_nodes, 不发任何 HTTP →
        agent 从未执行提交任务 (消费链断裂)。此处补真实派发。
        幂等: 同 task_id 已在派发则跳过。
        """
        if not task.assigned_nodes:
            logger.warning(f"派发跳过: 任务 {task.task_id} 无 assigned_nodes")
            return
        if task.task_id in self._dispatch_tasks and not self._dispatch_tasks[task.task_id].done():
            logger.debug(f"派发跳过: 任务 {task.task_id} 已在派发中")
            return
        handle = asyncio.create_task(self._dispatch_task(task), name=f"dispatch_{task.task_id}")
        self._dispatch_tasks[task.task_id] = handle
        handle.add_done_callback(lambda h: self._dispatch_tasks.pop(task.task_id, None))

    def _get_dispatch_token(self) -> str:
        """惰性加载集群 token — 与 agent BearerAuthMiddleware 同源。"""
        if self._dispatch_token is None:
            self._dispatch_token = load_or_create_token()
        return self._dispatch_token

    async def _get_dispatch_http(self) -> httpx.AsyncClient:
        if self._dispatch_http is None or self._dispatch_http.is_closed:
            self._dispatch_http = httpx.AsyncClient(timeout=300.0, **mtls_client_kwargs())
        return self._dispatch_http

    async def _dispatch_task(self, task: ClusterTask) -> None:
        """派发任务到 assigned_nodes — DATA 并发各节点 / PIPELINE 顺序链式。

        PIPELINE 接上游 fusion-mlx /distributed/* (#621) 真实层切分: 各节点跑模型
        一段层前向, hidden_states (b64.npy) 经调度器顺序链传; 末节点输出 = 最终
        hidden_states (lm_head/解码超上游首版范围, 见 _dispatch_pipeline)。
        """
        node_ids = list(task.assigned_nodes)
        nodes_snap = await self._snapshot_nodes(node_ids)
        token = self._get_dispatch_token()
        try:
            if task.mode == ParallelMode.PIPELINE:
                await self._dispatch_pipeline(task, node_ids, nodes_snap, token)
            else:
                await self._dispatch_data(task, node_ids, nodes_snap, token)
        except Exception as e:
            logger.error(f"派发异常: {task.name} ({task.task_id}): {e}")
            await self._finalize_task(task, success=False, error=f"派发异常: {e}")

    async def _dispatch_data(
        self, task: ClusterTask, node_ids: list[str], nodes_snap: dict[str, NodeInfo], token: str
    ) -> None:
        """DATA 并行 — 各 assigned_node 并发 POST /api/execute, 任一失败记 error 但不阻塞其余。"""
        client = await self._get_dispatch_http()
        coros = [self._dispatch_to_node(client, task, nid, nodes_snap, token) for nid in node_ids]
        results = await asyncio.gather(*coros, return_exceptions=True)
        outputs = []
        errors = []
        for nid, r in zip(node_ids, results):
            if isinstance(r, Exception):
                errors.append(f"{nid}: {type(r).__name__}: {r}")
            elif isinstance(r, dict) and "error" in r:
                errors.append(f"{nid}: {r['error']}")
            elif isinstance(r, dict):
                outputs.append(r)
            else:
                errors.append(f"{nid}: 空响应")
        success = bool(outputs) and not errors
        await self._finalize_task(
            task,
            success=success,
            error="; ".join(errors) if errors else "",
            result={"outputs": outputs, "errors": errors, "node_count": len(node_ids)},
        )

    async def _dispatch_pipeline(
        self, task: ClusterTask, node_ids: list[str], nodes_snap: dict[str, NodeInfo], token: str
    ) -> None:
        """PIPELINE 真实层切分链式 — 接上游 fusion-mlx /distributed/* (#621)。

        task.model_shards 各段带 layer_range + shard_index; 节点顺序对应 assigned_nodes。
        首段 (shard_index=0) 带 input_ids (embed+layers), 后续段带上一段出口 hidden_states
        (b64.npy, 仅 layers)。末段出口 = 最终 hidden_states (lm_head/解码超上游首版范围,
        docs/distributed-pipeline.md line 151 — 调度器只负责层前向链, 不做 token 生成)。
        每节点派发 task_type=pipeline_step, 经 _dispatch_to_node 透传 pipeline 字段。
        """
        client = await self._get_dispatch_http()
        model_id = task.params.get("model_id", task.model_name)
        input_ids = task.params.get("input_ids")
        hidden_states = None  # 首段 None → 上游 embed+layers
        steps = []
        for idx, nid in enumerate(node_ids):
            shard = task.model_shards[idx] if idx < len(task.model_shards) else {}
            layer_range = shard.get("layer_range", [])
            shard_index = int(shard.get("shard_index", idx))
            step_params = {
                "model_id": model_id,
                "shard_index": shard_index,
                "layer_range": layer_range,
                "hidden_states": hidden_states,
                "input_ids": input_ids if idx == 0 else None,
            }
            r = await self._dispatch_to_node(
                client, task, nid, nodes_snap, token, pipeline_step_params=step_params
            )
            if isinstance(r, Exception):
                await self._finalize_task(task, success=False, error=f"流水线步骤 {nid} 失败: {r}")
                return
            if isinstance(r, dict) and "error" in r:
                await self._finalize_task(task, success=False, error=f"流水线步骤 {nid}: {r['error']}")
                return
            if not isinstance(r, dict) or "hidden_states" not in r:
                await self._finalize_task(
                    task, success=False, error=f"流水线步骤 {nid} 未返回 hidden_states"
                )
                return
            hidden_states = r["hidden_states"]
            steps.append({
                "node_id": nid,
                "shard_id": r.get("shard_id", ""),
                "shape": r.get("shape"),
                "dtype": r.get("dtype"),
            })
            logger.info(f"P3 流水线段 {idx} ({nid}) 完成: shape={r.get('shape')} dtype={r.get('dtype')}")
        await self._finalize_task(
            task,
            success=True,
            error="",
            result={
                "hidden_states": hidden_states,
                "shape": steps[-1].get("shape") if steps else None,
                "dtype": steps[-1].get("dtype") if steps else None,
                "steps": steps,
                "node_count": len(node_ids),
            },
        )

    async def _dispatch_to_node(
        self,
        client: httpx.AsyncClient,
        task: ClusterTask,
        node_id: str,
        nodes_snap: dict[str, NodeInfo],
        token: str,
        pipeline_step_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """POST /api/execute 到单 agent。返回 agent 结果 dict 或 raise。

        pipeline_step_params 非空 → task_type=pipeline_step, extra 透传 pipeline 字段
        (model_id/layer_range/hidden_states/input_ids/...)。否则走 task.task_type 常规派发。
        """
        node = nodes_snap.get(node_id)
        if not node:
            raise RuntimeError(f"节点 {node_id} 不存在或已离线")
        # S1 熔断: 派发失败 (SSRF 拒绝 / HTTP 非 200 / agent 返回非 ok) 累计报告故障,
        # 窗口内达 _FAULT_THRESHOLD (3) 自动 ban, select_nodes 跳过 ban 节点不再派发。
        try:
            # SSRF 守卫: agent ip 走 is_safe_peer_host (与 master_server/agent 一致)
            if not is_safe_peer_host(node.ip_address):
                raise RuntimeError(f"节点 {node_id} ip {node.ip_address} 非安全对端, 拒绝派发")
            params = dict(task.params)
            if pipeline_step_params is not None:
                # P3 真实张量 PIPELINE — 走 /distributed/* 层前向链
                task_type = "pipeline_step"
                extra = {k: v for k, v in pipeline_step_params.items() if v is not None}
                payload = {
                    "task_type": task_type,
                    "model_name": task.model_name,
                    "extra": extra,
                }
            else:
                # 常规推理/Embedding 派发
                payload = {
                    "task_type": task.task_type,
                    "model_name": task.model_name,
                    "prompt": params.get("prompt", ""),
                    "messages": params.get("messages", []),
                    "max_tokens": params.get("max_tokens", 2048),
                    "temperature": params.get("temperature", 0.7),
                    "extra": {k: v for k, v in params.items() if k in ("top_p", "top_k", "repeat_penalty", "seed")},
                }
            url = build_safe_url(mtls_scheme(), node.ip_address, node.port, "/api/execute")
            headers = {"Authorization": f"Bearer {token}", "X-Node-Id": "master", "X-Node-Role": "master"}
            resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code != 200:
                raise RuntimeError(f"agent {node_id} HTTP {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            if data.get("status") != "ok":
                raise RuntimeError(f"agent {node_id} 返回非 ok: {str(data)[:200]}")
            agent_result = data.get("result", {})
            # 注入 node_id 供上层聚合
            if isinstance(agent_result, dict):
                agent_result.setdefault("node_id", node_id)
            return agent_result
        except Exception as e:
            await self.report_fault(node_id, fault_type="dispatch_failed", message=str(e)[:200])
            raise

    async def _finalize_task(
        self, task: ClusterTask, success: bool, error: str, result: dict[str, Any] | None = None
    ) -> None:
        """派发完成回填任务状态 + 释放节点 active_tasks 计数。"""
        async with self._nodes_lock:
            async with self._tasks_lock:
                t = self.tasks.get(task.task_id)
                if not t or t.status != TaskStatus.RUNNING:
                    # 已被 cancel/timeout 改态, 不覆盖
                    cur_state = t.status.value if t else "gone"
                    logger.debug(f"派发回填跳过: 任务 {task.task_id} 状态已非 RUNNING ({cur_state})")
                    return
                t.status = TaskStatus.COMPLETED if success else TaskStatus.FAILED
                t.completed_at = time.time()
                t.error = error
                if result is not None:
                    t.result = result
                for nid in t.assigned_nodes:
                    node = self.nodes.get(nid)
                    if node:
                        node.active_tasks = max(0, node.active_tasks - 1)
                self._persist_tasks_locked()  # H3 终态落盘 (清掉该任务)
                logger.info(f"派发回填: {t.name} ({t.task_id}) → {t.status.value}")

    async def _snapshot_nodes(self, node_ids: list[str]) -> dict[str, NodeInfo]:
        """快照节点 (深拷贝引用, 派发期间不被 heartbeat 改字段影响)。"""
        async with self._nodes_lock:
            return {nid: self.nodes[nid] for nid in node_ids if nid in self.nodes}

    async def migrate_task(self, task_id: str) -> bool:
        """故障迁移任务到其他节点。"""
        async with self._nodes_lock:
            async with self._tasks_lock:
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
        async with self._nodes_lock:
            async with self._tasks_lock:
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

                # 递归取消子任务（支持多层）
                cancelled_sub = []
                if cancel_sub_tasks and task.sub_tasks:
                    cancel_stack = list(task.sub_tasks)
                    while cancel_stack:
                        sub_id = cancel_stack.pop()
                        sub = self.tasks.get(sub_id)
                        if sub and sub.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                            for nid in sub.assigned_nodes:
                                node = self.nodes.get(nid)
                                if node:
                                    node.active_tasks = max(0, node.active_tasks - 1)
                            sub.assigned_nodes = []
                            sub.status = TaskStatus.CANCELLED
                            sub.cancel_reason = f"父任务取消: {task_id}"
                            sub.error = sub.cancel_reason
                            cancelled_sub.append(sub_id)
                            if sub.sub_tasks:
                                cancel_stack.extend(sub.sub_tasks)

                task.status = TaskStatus.CANCELLED
                task.error = task.cancel_reason
                self._persist_tasks_locked()  # H3 终态落盘

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
        async with self._nodes_lock:
            async with self._tasks_lock:
                task = self.tasks.get(task_id)
                if not task or task.status not in (
                    TaskStatus.RUNNING,
                    TaskStatus.PENDING,
                    TaskStatus.FAILED,
                ):
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

                # 降级模型 (R7: 用正则边界替换, 避免 130b→320b 这类子串误伤)
                old_model = task.model_name
                pattern = rf"(?<!\d){re.escape(current_size)}(?!\d)"
                task.model_name = re.sub(pattern, next_size, task.model_name, flags=re.IGNORECASE)
                task.degraded_from_model = old_model
                task.degradation_count += 1
                task.status = TaskStatus.PENDING
                task.error = ""

        logger.info(f"任务降级: {task_id} {old_model} → {task.model_name} (第{task.degradation_count}次)")

        ok = await self.assign_task(task)
        if not ok:
            self._enqueue_retry(task)
        return ok

    def _match_model_size(self, model_name: str, size_keys: set[str] | dict) -> str | None:
        """R7: 用正则边界匹配模型尺寸, 替代子串 in。

        `(?<!\\d)<N>b(?!\\d)` 确保 13b 不命中 130b, 3b 不命中 13b/30b/33b。
        按数值降序匹配 (70b 先于 7b), 避免短键误吞长键。
        返回命中的 size_key, 未命中返回 None。
        """
        name_lower = (model_name or "").lower()
        if not name_lower:
            return None
        # 按数值降序: "70b"->70, "0.5b"->0.5
        def _num(key: str) -> float:
            return float(key[:-1]) if key.endswith("b") else 0.0

        for size_key in sorted(size_keys, key=_num, reverse=True):
            pattern = rf"(?<!\d){re.escape(size_key)}(?!\d)"
            if re.search(pattern, name_lower):
                return size_key
        return None

    def _extract_model_size(self, model_name: str) -> str:
        size = self._match_model_size(model_name, self.MODEL_DEGRADATION_CHAIN)
        return size or ""

    async def check_timeouts(self) -> list[str]:
        """检查并处理超时任务，自动入重试队列。"""
        now = time.time()
        timed_out = []
        async with self._tasks_lock:
            for tid, task in list(self.tasks.items()):
                if task.status == TaskStatus.RUNNING and task.started_at > 0:
                    if now - task.started_at > task.timeout_seconds:
                        task.status = TaskStatus.TIMEOUT
                        task.error = f"任务超时 ({task.timeout_seconds}s)"
                        timed_out.append(tid)
                        logger.warning(f"任务超时: {task.name} ({tid}), 入重试队列")
                        self._enqueue_retry(task)
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
    _LOCAL_FORCE_MODEL_SIZES = {"0.5b"}
    # M4-02 轻量级任务内存阈值 (GB)
    _LOCAL_FORCE_MEMORY_GB = 4.0

    # M4-03 大模型阈值 (≥13B 使用 VRAM 优先策略)
    _VRAM_FIRST_MODEL_SIZES = {"70b", "32b", "13b"}

    # F-A13 故障隔离 — report_fault 在窗口内达阈值进黑名单, ban 期内拒绝注册。
    _FAULT_WINDOW_S = 60.0
    _FAULT_THRESHOLD = 3
    _BAN_DURATION_S = 300.0

    def _is_local_force(self, task: ClusterTask, required_mem: float) -> bool:
        """M4-02 判断是否强制本地执行 (R7: 正则边界匹配)。"""
        if not task.model_name:
            return required_mem <= self._LOCAL_FORCE_MEMORY_GB
        if self._match_model_size(task.model_name, self._LOCAL_FORCE_MODEL_SIZES):
            return True
        return required_mem <= self._LOCAL_FORCE_MEMORY_GB

    def _is_vram_first(self, task: ClusterTask) -> bool:
        """M4-03 判断是否使用 VRAM 优先策略 (R7: 正则边界匹配)。"""
        if not task.model_name:
            return False
        return self._match_model_size(task.model_name, self._VRAM_FIRST_MODEL_SIZES) is not None

    def _estimate_memory(self, task: ClusterTask) -> float:
        """估算任务所需内存 (R7: 正则边界匹配)。"""
        base = self._BASE_MEMORY_GB
        if task.model_name:
            base += self._DEFAULT_MODEL_MEMORY_GB
            size_key = self._match_model_size(task.model_name, self.MODEL_MEMORY_PROFILE)
            if size_key:
                mem = self.MODEL_MEMORY_PROFILE[size_key]
                base += mem - self._DEFAULT_MODEL_MEMORY_GB
        return base

    # ── KV 缓存管理 ──

    async def _is_node_online(self, node_id: str) -> bool:
        """节点是否在线快照 (nodes 域只读, 供 kv 等跨域读取调用, 避免跨域嵌套持锁)。"""
        async with self._nodes_lock:
            node = self.nodes.get(node_id)
            return bool(node and node.status == NodeStatus.ONLINE)

    async def register_kv_cache(self, entry: KVCacheEntry) -> None:
        """注册 KV 缓存。"""
        async with self._kv_lock:
            self.kv_cache[entry.cache_id] = entry
            if len(self.kv_cache) > self._max_kv_cache:
                oldest = min(self.kv_cache.items(), key=lambda x: x[1].created_at)
                del self.kv_cache[oldest[0]]
            logger.info(f"KV 缓存注册: {entry.model_name} @ {entry.node_id} ({entry.size_mb:.1f}MB)")

    async def find_kv_cache(self, model_name: str) -> KVCacheEntry | None:
        """查找可复用的 KV 缓存。"""
        now = time.time()
        async with self._kv_lock:
            for cid, entry in list(self.kv_cache.items()):
                if entry.model_name == model_name and now - entry.created_at < entry.ttl_seconds:
                    if await self._is_node_online(entry.node_id):
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
        logger.info(
            f"M9-04 FMP KV 缓存同步(元数据): cache_id={cache_id} model={model_name} "
            f"source={source_node_id} size={size_mb:.1f}MB protocol={sync_msg.protocol}"
        )

        async with self._kv_lock:
            entry = self.kv_cache.get(cache_id)
            if not entry:
                logger.warning(f"M9-04 KV 缓存同步: cache_id={cache_id} 未注册，跳过同步")
                return False

        # R3: 仅登记了 KV 缓存元数据消息, 未实现张量级跨节点传输执行层。
        # 跨节点张量迁移依赖 fusion-mlx /distributed/* 路由 (上游 issue #621 未实现),
        # 当前无传输通道 → 真实同步未发生。返回 False 以如实反映, 避免向调用方谎报成功。
        logger.warning(
            f"M9-04 KV 缓存跨节点传输未实现: cache_id={cache_id} model={model_name} "
            f"source={source_node_id}。元数据已登记, 张量迁移需上游 /distributed/* 路由 (issue #621)"
        )
        return False

    # ── M3-03 选举配置 ──
    # P4: setup_election 已接 start(ha_config=...) — enabled=True 时启动选举循环。
    # HA = leader + standby, 投票走 HTTP POST /api/ha/vote, 任务状态走 /api/ha/sync-tasks。
    # 默认 enabled=False (单 Master 向后兼容)。

    def setup_election(
        self,
        node_id: str,
        priority: int = 0,
        known_nodes: list[dict[str, Any]] | None = None,
    ) -> None:
        logger.info(f"M3-03 Master 选举配置: node_id={node_id} priority={priority}")
        self._election = MasterElection(
            node_id=node_id,
            priority=priority,
            send_vote_request=self._send_vote_request_cb,
        )
        if known_nodes:
            for node in known_nodes:
                self._election.add_known_node(
                    ElectionCandidate(
                        node_id=node.get("node_id", ""),
                        priority=node.get("priority", 0),
                        hostname=node.get("hostname", ""),
                        ip_address=node.get("ip_address", ""),
                        port=node.get("port", 0),
                        term=0,
                        voted_for="",
                        last_heartbeat=time.time(),
                    )
                )
        self._election.on_elected = self._on_elected_leader
        self._election.on_demoted = self._on_demoted_from_leader
        logger.info(f"M3-03 Master 选举已配置: node_id={node_id} priority={priority}")

    async def _send_vote_request_cb(self, vote_req: VoteRequest, peer_node_id: str) -> VoteResponse:
        """HTTP 拉票回调 — POST /api/ha/vote 到对端 master。best-effort, 失败抛异常由调用方吞。"""
        if not self._election:
            return VoteResponse(term=0, vote_granted=False, voter_id=peer_node_id)
        cand = self._election.get_candidate(peer_node_id)
        if not cand or not cand.ip_address or not cand.port:
            logger.debug(f"拉票跳过无地址对端: {peer_node_id}")
            return VoteResponse(term=0, vote_granted=False, voter_id=peer_node_id)
        if not is_safe_peer_host(cand.ip_address):
            logger.warning(f"拉票拒绝不安全对端主机: {peer_node_id} ({cand.ip_address!r})")
            return VoteResponse(term=0, vote_granted=False, voter_id=peer_node_id)
        url = build_safe_url(mtls_scheme(), cand.ip_address, cand.port, "/api/ha/vote")
        token = self._get_dispatch_token()
        payload = {
            "term": vote_req.term,
            "candidate_id": vote_req.candidate_id,
            "candidate_priority": vote_req.candidate_priority,
            "last_log_index": vote_req.last_log_index,
            "last_log_term": vote_req.last_log_term,
        }
        try:
            client = await self._get_dispatch_http()
            resp = await client.post(url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=5.0)
            if resp.status_code != 200:
                logger.debug(f"拉票 HTTP {resp.status_code} from {peer_node_id}")
                return VoteResponse(term=0, vote_granted=False, voter_id=peer_node_id)
            data = resp.json()
            return VoteResponse(
                term=data.get("term", 0),
                vote_granted=bool(data.get("vote_granted", False)),
                voter_id=data.get("voter_id", peer_node_id),
            )
        except Exception as e:
            logger.debug(f"拉票异常 {peer_node_id}: {e}")
            return VoteResponse(term=0, vote_granted=False, voter_id=peer_node_id)

    async def handle_vote_request(self, req: VoteRequest) -> VoteResponse:
        """接收对端 master 拉票 — 透传到选举管理器。无选举配置时拒绝 (单 Master 模式)。"""
        if not self._election:
            return VoteResponse(term=0, vote_granted=False, voter_id="")
        return await self._election.handle_vote_request(req)

    async def receive_synced_tasks(self, tasks: list[dict[str, Any]]) -> int:
        """Standby 接收 leader 推送的任务状态, 合并到 self.tasks 并持久化。幂等。"""
        merged = 0
        async with self._tasks_lock:
            for d in tasks:
                try:
                    task = self._task_from_dict(d)
                    existing = self.tasks.get(task.task_id)
                    # 已有终态任务不覆盖 (leader 旧快照可能落后)
                    if existing and existing.status in (
                        TaskStatus.COMPLETED,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED,
                        TaskStatus.TIMEOUT,
                    ):
                        continue
                    self.tasks[task.task_id] = task
                    merged += 1
                except Exception as e:
                    logger.warning(f"HA 同步任务跳过 {d.get('task_id', '?')}: {e}")
            if merged:
                self._persist_tasks_locked()
        if merged:
            logger.info(f"HA 同步接收 {merged} 任务 (standby 合并落盘)")
        return merged

    def _sync_tasks_to_standbys_locked(self) -> list[tuple[str, str, int, dict[str, Any]]]:
        """已持 _tasks_lock 下构建待推送 payload + 对端列表。返回 [(node_id, ip, port, payload), ...]。

        仅 leader 调用。终态任务不推送 (与 _persist_tasks_locked 一致)。
        """
        if not self._election or not self._is_leader:
            return []
        _TERMINAL = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT}
        pending = [self._task_to_dict(t) for t in self.tasks.values() if t.status not in _TERMINAL]
        payload = {"tasks": pending, "saved_at": time.time()}
        targets: list[tuple[str, str, int, dict[str, Any]]] = []
        for peer_id in self._election._known_nodes:
            if peer_id == self._election.node_id:
                continue
            cand = self._election.get_candidate(peer_id)
            if not cand or not cand.ip_address or not cand.port:
                continue
            if not is_safe_peer_host(cand.ip_address):
                continue
            targets.append((peer_id, cand.ip_address, cand.port, payload))
        return targets

    async def _push_sync_to_standbys(self, targets: list[tuple[str, str, int, dict[str, Any]]]) -> None:
        """锁外异步推送任务快照到各 standby (best-effort, 不阻塞派发)。"""
        if not targets:
            return
        token = self._get_dispatch_token()
        try:
            client = await self._get_dispatch_http()
        except Exception as e:
            logger.warning(f"HA 同步获取 HTTP 客户端失败: {e}")
            return
        for peer_id, ip, port, payload in targets:
            try:
                url = build_safe_url(mtls_scheme(), ip, port, "/api/ha/sync-tasks")
                resp = await client.post(
                    url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=5.0
                )
                if resp.status_code != 200:
                    logger.debug(f"HA 同步推送 {peer_id} HTTP {resp.status_code}")
                else:
                    logger.debug(f"HA 同步推送 {peer_id} ok ({len(payload['tasks'])} 任务)")
            except Exception as e:
                logger.debug(f"HA 同步推送 {peer_id} 异常: {e}")

    def _on_elected_leader(self) -> None:
        self._is_leader = True
        logger.info("本节点被选举为 Leader")

    def _on_demoted_from_leader(self) -> None:
        self._is_leader = False
        logger.warning("本节点从 Leader 降级")

    # ── 生命周期 ──

    async def start(
        self,
        with_server: bool = True,
        with_mdns: bool = True,
        ha_config: dict[str, Any] | None = None,
    ) -> None:
        """启动集群主节点服务。

        ha_config (P4 HA): {"enabled": bool, "node_id": str, "priority": int,
                            "peers": [str | {"node_id","ip","port","priority"}]}
        enabled=True 时接 setup_election 启动选举循环 + HTTP 拉票 + 任务同步;
        否则单 Master 无 HA (默认)。peers 裸字符串向后兼容 (仅 node_id, 不可达)。
        """
        self._running = True
        logger.info(f"Cluster Master 启动: {self.host}:{self.port}")
        logger.info(f"节点发现端口: {self.discovery_port}")

        # H3 启动恢复: 重建崩溃前未完成任务 (RUNNING→PENDING 重派)
        await self._restore_tasks()

        self._health_task = asyncio.create_task(self._health_check_loop())
        self._retry_task = asyncio.create_task(self._retry_loop())
        self._persist_task = asyncio.create_task(self._persist_loop())

        # P4 HA 选举接线 — config 门控, 单 Master 默认不启用
        if ha_config and ha_config.get("enabled") and ha_config.get("node_id") and ha_config.get("peers"):
            peer_specs: list[dict[str, Any]] = []
            for peer in ha_config["peers"]:
                if peer == ha_config["node_id"]:
                    continue
                if isinstance(peer, dict):
                    peer_specs.append(
                        {
                            "node_id": peer.get("node_id", ""),
                            "priority": peer.get("priority", 0),
                            "hostname": peer.get("hostname", ""),
                            "ip_address": peer.get("ip_address", ""),
                            "port": peer.get("port", 0),
                        }
                    )
                else:
                    # 裸字符串 peer: 仅 node_id, 无地址 (向后兼容, 不可达)
                    peer_specs.append({"node_id": str(peer), "priority": 0})
            self.setup_election(
                node_id=ha_config["node_id"],
                priority=ha_config.get("priority", 0),
                known_nodes=peer_specs,
            )
            if self._election:
                await self._election.start()
                logger.info("P4 HA 选举已启动 (HTTP 拉票 + 任务同步已接线)")
        else:
            logger.info("P4 HA 未启用 — 单 Master 模式 (默认)")

        if with_mdns:
            self._start_mdns()

        if with_server:
            from fusion_multi_node.server import MasterServer

            server = MasterServer(master=self)
            await server.start(host=self.host, port=self.port)

    async def stop(self) -> None:
        """停止集群主节点。"""
        self._running = False
        for task in (self._health_task, self._retry_task, self._persist_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        # H3 停机前最终落盘 (保留未完成任务供下次启动恢复)
        await self._persist_tasks()
        # P1 派发: 取消在途派发任务 + 关闭 http 客户端
        for dtask in list(self._dispatch_tasks.values()):
            if not dtask.done():
                dtask.cancel()
                try:
                    await dtask
                except asyncio.CancelledError:
                    pass
        self._dispatch_tasks.clear()
        if self._dispatch_http and not self._dispatch_http.is_closed:
            await self._dispatch_http.aclose()
            self._dispatch_http = None
        # P4 HA: 停止选举循环
        if self._election:
            await self._election.stop()
        self._stop_mdns()
        logger.info("Cluster Master 已停止")

    def _start_mdns(self) -> None:
        try:
            import subprocess

            from fusion_multi_node.discovery import MDNSDiscovery

            device_model = ""
            uma_size_gb = "0.0"
            try:
                out = subprocess.check_output(["sysctl", "-n", "hw.model"], text=True).strip()
                device_model = out
            except Exception:
                pass
            try:
                out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
                uma_size_gb = str(int(out) / (1024**3))
            except Exception:
                pass
            self._mdns = MDNSDiscovery(node_id="fusion-master")
            ok = self._mdns.register(
                port=self.port,
                properties={
                    "role": "master",
                    "discovery_port": str(self.discovery_port),
                    "host": self.host,
                    "device_model": device_model,
                    "uma_size_gb": uma_size_gb,
                    "heartbeat_interval": "3",
                    "heartbeat_timeout": "15",
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

    async def _persist_loop(self) -> None:
        """H3 周期快照兜底 (15s) — 关键状态写点已即时落盘, 此处防漏写。"""
        try:
            while self._running:
                await asyncio.sleep(15)
                await self._persist_tasks()
        except asyncio.CancelledError:
            pass

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
                await self._refresh_node_statuses()
                await self._cleanup_completed_tasks()
                await self._cleanup_offline_nodes()
                online = len(await self.get_online_nodes())
                active = sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING)
                logger.debug(f"集群状态: {online} 在线, {active} 活跃任务")
        except asyncio.CancelledError:
            pass

    async def _cleanup_completed_tasks(self) -> None:
        """清理已完成的旧任务，防止 tasks 无限增长。"""
        async with self._tasks_lock:
            terminal = [
                tid
                for tid, t in self.tasks.items()
                if t.status
                in (
                    TaskStatus.COMPLETED,
                    TaskStatus.FAILED,
                    TaskStatus.TIMEOUT,
                    TaskStatus.MIGRATED,
                )
            ]
            if len(terminal) > self._max_completed_tasks:
                remove = terminal[: len(terminal) - self._max_completed_tasks]
                for tid in remove:
                    del self.tasks[tid]
                logger.debug(f"清理旧任务: {len(remove)} 个")

    async def _cleanup_offline_nodes(self) -> None:
        """清理长时间离线节点，防止 nodes 无限增长。"""
        now = time.time()
        async with self._nodes_lock:
            stale = [
                nid for nid, n in self.nodes.items() if n.status == NodeStatus.OFFLINE and now - n.last_heartbeat > 3600
            ]
            for nid in stale:
                del self.nodes[nid]
            if stale:
                logger.debug(f"清理离线节点: {len(stale)} 个")

    # ── 统计信息 ──

    async def snapshot_nodes(self) -> list[NodeInfo]:
        """节点表快照 (nodes 域只读) — 供外部 (master_server) 迭代用, 避免裸读 self.nodes。"""
        async with self._nodes_lock:
            return list(self.nodes.values())

    async def snapshot_tasks(self) -> list[ClusterTask]:
        """任务表快照 (tasks 域只读) — 供外部迭代用, 避免裸读 self.tasks。"""
        async with self._tasks_lock:
            return list(self.tasks.values())

    async def get_node(self, node_id: str) -> NodeInfo | None:
        """取单节点快照引用 (nodes 域只读)。"""
        async with self._nodes_lock:
            return self.nodes.get(node_id)

    async def get_task(self, task_id: str) -> ClusterTask | None:
        """取单任务快照引用 (tasks 域只读)。"""
        async with self._tasks_lock:
            return self.tasks.get(task_id)

    async def get_stats(self) -> dict[str, Any]:
        """获取集群统计信息。"""
        online_nodes = await self.get_online_nodes()
        async with self._nodes_lock:
            total_nodes = len(self.nodes)
        async with self._tasks_lock:
            total_tasks = len(self.tasks)
            active_tasks = sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING)
            completed_tasks = sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED)
            failed_tasks = sum(
                1 for t in self.tasks.values() if t.status in (TaskStatus.FAILED, TaskStatus.TIMEOUT)
            )
        async with self._kv_lock:
            kv_cache_entries = len(self.kv_cache)
        stats = {
            "total_nodes": total_nodes,
            "online_nodes": len(online_nodes),
            "total_tasks": total_tasks,
            "active_tasks": active_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "kv_cache_entries": kv_cache_entries,
            "total_memory_gb": sum(n.total_memory_gb for n in online_nodes),
            "available_memory_gb": sum(n.available_memory_gb for n in online_nodes),
        }
        stats["load_summary"] = self.load_router.get_cluster_load_summary()
        return stats

    async def get_prometheus_metrics(self) -> str:
        """S2 Prometheus exposition format — 集群级聚合指标供 /api/v1/metrics 抓取。

        复用 get_stats (节点/任务/KV/内存) + 派发延迟 (completed_at - started_at)
        + 重试计数 (_retry_count)。纯文本 0.0.4 exposition, 无外部依赖。
        """
        stats = await self.get_stats()
        latencies: list[float] = []
        retries = 0
        pending = 0
        running = 0
        async with self._tasks_lock:
            for t in self.tasks.values():
                retries += getattr(t, "_retry_count", 0)
                if t.status == TaskStatus.PENDING:
                    pending += 1
                elif t.status == TaskStatus.RUNNING:
                    running += 1
                if t.status == TaskStatus.COMPLETED and t.started_at > 0 and t.completed_at > t.started_at:
                    latencies.append(t.completed_at - t.started_at)
        lat_sorted = sorted(latencies)
        n = len(lat_sorted)

        def _pctl(q: float) -> float:
            if n == 0:
                return 0.0
            idx = min(n - 1, max(0, int(q * n)))
            return lat_sorted[idx]

        lines = [
            "# HELP fusion_cluster_nodes_total 集群注册节点总数",
            "# TYPE fusion_cluster_nodes_total gauge",
            f"fusion_cluster_nodes_total {stats['total_nodes']}",
            "# HELP fusion_cluster_nodes_online 在线节点数",
            "# TYPE fusion_cluster_nodes_online gauge",
            f"fusion_cluster_nodes_online {stats['online_nodes']}",
            "# HELP fusion_cluster_tasks_total 任务总数",
            "# TYPE fusion_cluster_tasks_total gauge",
            f"fusion_cluster_tasks_total {stats['total_tasks']}",
            "# HELP fusion_cluster_tasks_running 运行中任务数",
            "# TYPE fusion_cluster_tasks_running gauge",
            f"fusion_cluster_tasks_running {running}",
            "# HELP fusion_cluster_tasks_pending 待派发任务数",
            "# TYPE fusion_cluster_tasks_pending gauge",
            f"fusion_cluster_tasks_pending {pending}",
            "# HELP fusion_cluster_tasks_completed 已完成任务数",
            "# TYPE fusion_cluster_tasks_completed gauge",
            f"fusion_cluster_tasks_completed {stats['completed_tasks']}",
            "# HELP fusion_cluster_tasks_failed 失败/超时任务数",
            "# TYPE fusion_cluster_tasks_failed gauge",
            f"fusion_cluster_tasks_failed {stats['failed_tasks']}",
            "# HELP fusion_cluster_task_retries_total 任务重试总次数",
            "# TYPE fusion_cluster_task_retries_total counter",
            f"fusion_cluster_task_retries_total {retries}",
            "# HELP fusion_cluster_kv_cache_entries KV 缓存条目数",
            "# TYPE fusion_cluster_kv_cache_entries gauge",
            f"fusion_cluster_kv_cache_entries {stats['kv_cache_entries']}",
            "# HELP fusion_cluster_memory_total_gb 集群总内存 GB",
            "# TYPE fusion_cluster_memory_total_gb gauge",
            f"fusion_cluster_memory_total_gb {stats['total_memory_gb']:.2f}",
            "# HELP fusion_cluster_memory_available_gb 集群可用内存 GB",
            "# TYPE fusion_cluster_memory_available_gb gauge",
            f"fusion_cluster_memory_available_gb {stats['available_memory_gb']:.2f}",
            "# HELP fusion_cluster_dispatch_latency_seconds 派发延迟秒 (已完成任务)",
            "# TYPE fusion_cluster_dispatch_latency_seconds summary",
            f'fusion_cluster_dispatch_latency_seconds{{quantile="0.5"}} {_pctl(0.5):.4f}',
            f'fusion_cluster_dispatch_latency_seconds{{quantile="0.9"}} {_pctl(0.9):.4f}',
            f'fusion_cluster_dispatch_latency_seconds{{quantile="0.99"}} {_pctl(0.99):.4f}',
            f"fusion_cluster_dispatch_latency_seconds_sum {sum(latencies):.4f}",
            f"fusion_cluster_dispatch_latency_seconds_count {n}",
            "",
        ]
        return "\n".join(lines)


# ── HA Standby (未接线原型, 非生产 HA) ──
# 注意: StandbyMaster 零生产实例化, LEARNING 状态同步 + term/vote 持久化未实现。
# 现网为单 Master 无 HA。AR审计 P1: 接线或砍。


class StandbyMaster:
    """HA 备用主节点 — 监听主节点心跳，故障时接管。

    ⚠️ 未接线原型: 零生产实例化, 不构成高可用承诺。

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
        master_port: int = 11452,
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
        self._monitor_task: asyncio.Task | None = None
        self._promoted_master: ClusterMaster | None = None
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
                    logger.warning(f"主节点心跳超时 ({elapsed:.1f}s > {self.heartbeat_timeout}s)，准备接管")
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
                host="127.0.0.1",
                port=self.master_port,
            )
            await self._promoted_master.start()
            self.state = self.HAState.ACTIVE
            logger.warning("StandbyMaster 已接管成为主节点 (ACTIVE)")

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
from typing import TYPE_CHECKING, Any

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
from fusion_multi_node.observability import ClusterObservability
from fusion_multi_node.security.mtls import client_kwargs as mtls_client_kwargs
from fusion_multi_node.security.mtls import scheme as mtls_scheme
from fusion_multi_node.utils.auth import (
    build_safe_url,
    is_registerable_host,
    is_safe_peer_host,
    load_or_create_token,
)

if TYPE_CHECKING:
    # P2-20 (审计 §6.8): start(config=) 注入 ClusterConfig, 仅类型提示用 (避免循环导入)。
    from fusion_multi_node.config import ClusterConfig

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
    # P3-29 (审计 §5.9): DATA 并行部分节点成功部分失败 — 返部分结果, 终态不重试。
    PARTIAL = "partial"


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
    # #31 重试节点规避: 硬黑名单, 调度绝不派发到列表内节点 (重试时带入失败节点打破死循环)
    exclude_nodes: list[str] = field(default_factory=list)
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
            exclude_nodes=list(spec.exclude_nodes),
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
        # P2-20 (审计 §6.8): ClusterConfig 实例 (start() 注入), 供 MasterServer 热加载。
        self._cluster_config: ClusterConfig | None = None

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
        # P1-H 优先级队列 + 租户配额: assign_task 节点不足或租户超配额时入队 (非 503),
        # 任务完成/节点上线时 _drain_pending_locked 按优先级降序派发。
        # tenant_max_concurrent=0 不限; 默认 4 (DEFAULT_CONFIG scheduling)。
        self._pending_queue: list[ClusterTask] = []
        self._tenant_max_concurrent = 4
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
        # C2: 选举状态持久化路径 (term/voted_for), 与 tasks.json 同目录。
        self._election_state_path = Path.home() / ".fusion" / "multi-node" / "election_state.json"
        self._persist_task: asyncio.Task | None = None
        # GAP-1 (Phase C): 全状态同步循环 — leader 周期推 nodes/kv/banned 到 standby,
        # standby 持有完整集群拓扑, failover 后可立即调度 (always-on)。
        self._state_sync_task: asyncio.Task | None = None
        # P0-8: 全集群可观测 — 接 health_check_loop 周期采集节点指标 + 告警规则,
        # /api/v1/observability/* 路由经 master._observability 读 (原恒 None → 503)。
        # 全内存 deque (maxlen 10000), 重启即失 (P1 KV 持久化同类债)。
        self._observability: ClusterObservability | None = None
        # P1-18 (审计 §5.5): 任务状态推送通道 — SSE 订阅者队列列表。
        # 任务终态/失败/重试/取消时 _emit_task_event 向所有订阅者非阻塞 put_nowait,
        # /api/tasks/events SSE 端点流式推客户端, 不再纯轮询知 FAILED。
        self._event_subscribers: list[asyncio.Queue] = []
        # F3 (#27): /v1/chat/completions 轻量代理 — 同步推理不进任务流水线 (不污染
        # self.tasks/持久化/优先级队列)。用 per-user 在途计数器 gate 租户并发:
        # _tenant_max_concurrent (0=不限) 复用调度配额, 429 超限。轻量锁独立于三域锁。
        self._chat_lock = asyncio.Lock()
        self._inflight_chat: dict[str, int] = {}

    # ── 节点管理 ──

    async def register_node(self, info: NodeInfo) -> bool:
        """注册或更新节点 (F-A12 幂等: 再注册 = PATCH, 保留 Master 权威运行态字段)。

        ban 内节点拒绝注册。返回 True=放行, False=被 ban 拒绝。
        """
        # H1 (AR #24): 注册期校验 ip — 挡云元数据/链路本地等恶意主机入库。
        # 允许 loopback + 私网 (单机/可信 LAN), 比出站 is_safe_peer_host 宽松。
        if not is_registerable_host(info.ip_address):
            logger.warning(f"拒绝注册: 节点 {info.node_id} ip 非法 {info.ip_address!r}")
            return False
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
        # P1-H: 新节点上线 → 排空待派发队列 (优先级高的先得空闲节点)。
        await self._drain_pending_locked()
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
        # P2-26 (审计 §5.7): _retry_count 是动态属性 (非 dataclass 字段), asdict 不序列化 →
        # master 崩溃恢复后归 0, 允许超 _max_retry_attempts 的额外重试。显式持久化恢复。
        d["_retry_count"] = getattr(task, "_retry_count", 0)
        return d

    def _task_from_dict(self, d: dict[str, Any]) -> ClusterTask:
        # RUNNING 恢复为 PENDING 重派 (派发中的任务崩溃后须重新调度)
        # P3-29: PARTIAL 是终态, 恢复时保持不变 (不重派, 保留部分结果)
        st = d.get("status", "pending")
        if st in ("running", "migrated"):
            st = "pending"
        t = ClusterTask(
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
            exclude_nodes=d.get("exclude_nodes", []),
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
        # P2-26 (审计 §5.7): 恢复 _retry_count, 避崩溃后重试预算被重置 (允许超限重试)。
        t._retry_count = int(d.get("_retry_count", 0) or 0)
        return t

    def _persist_tasks_locked(self) -> list[dict[str, Any]]:
        """已持 _tasks_lock 下快照非终态任务 (不落盘)。终态 (COMPLETED/FAILED/CANCELLED/TIMEOUT) 不存。

        P1-11 (审计 §4.2): 拆出 I/O — 锁内仅建快照, 落盘 (含 os.fsync 阻塞) 移到 _write_task_store,
        在 _tasks_lock 释放后执行, 不再持锁 fsync。
        """
        _TERMINAL = {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMEOUT,
            TaskStatus.PARTIAL,
        }
        return [self._task_to_dict(t) for t in self.tasks.values() if t.status not in _TERMINAL]

    def _write_task_store(self, pending: list[dict[str, Any]]) -> None:
        """不持 _tasks_lock 落盘任务快照 (P1-11: fsync 移出锁外)。原子 tmp+replace。"""
        if pending is None:
            return
        try:
            self._task_store_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._task_store_path.with_suffix(self._task_store_path.suffix + ".tmp")
            with open(tmp, "w") as f:
                json.dump({"tasks": pending, "saved_at": time.time()}, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._task_store_path)
            logger.debug(f"H3 任务持久化: {len(pending)} 非终态任务落盘")
        except Exception as e:
            # P1-15 (审计 §5.6): 持久化失败不静默吞 — 任务落盘是崩溃恢复根基, 失败则
            # Master 崩溃后 RUNNING 任务无法恢复。发 critical 告警 + 失败指标, 接 P0-8 Observability。
            logger.error(f"H3 任务持久化失败: {e}")
            obs = self._observability
            if obs is not None:
                try:
                    obs.create_alert(
                        severity="critical",
                        title="H3 任务持久化失败",
                        message=(
                            f"任务落盘失败: {e}。Master 崩溃后 RUNNING 任务将无法恢复, "
                            f"请检查磁盘/权限: {self._task_store_path}"
                        ),
                    )
                    obs.record_metric("cluster", "task_persist_failed", 1.0)
                except Exception as alert_err:
                    logger.error(f"H3 持久化失败告警发出异常: {alert_err}")

    async def _persist_tasks(self) -> None:
        """加锁快照 → 释放锁 → 落盘 (P1-11: fsync 不持锁)。HA leader 额外推送任务快照到 standby。"""
        snapshot: list[dict[str, Any]] = []
        targets: list[tuple[str, str, int, dict[str, Any]]] = []
        async with self._tasks_lock:
            snapshot = self._persist_tasks_locked()
            # HA: leader 构建推送目标 (锁内构建 payload, 锁外异步发送)
            targets = self._sync_tasks_to_standbys_locked()
        # P1-11: 落盘 (fsync) 已移出 _tasks_lock 持有区。
        self._write_task_store(snapshot)
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
                f"节点故障: {node_id} [{fault_type}] {message} (窗口内 {len(windowed)}/{self._FAULT_THRESHOLD})"
            )
            if len(windowed) >= self._FAULT_THRESHOLD:
                self._banned_nodes[node_id] = now + self._BAN_DURATION_S
                logger.warning(f"节点故障达阈值自动 ban: {node_id} ({self._BAN_DURATION_S:.0f}s) [{fault_type}]")
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
        exclude_nodes: list[str] | None = None,
    ) -> list[NodeInfo]:
        """根据策略选择最优节点。M4-01 负载感知 + M4-02 本地优先。"""
        candidates = await self.get_online_nodes()

        # S1 熔断: 跳过 ban 期内节点 (派发失败累积达阈值自动 ban, 不再被选中)
        candidates = [n for n in candidates if not self.is_node_banned(n.node_id)]

        # #31 重试节点规避: 硬黑名单过滤 (重试带入失败节点, 打破"重试回同一坏节点"死循环)
        if exclude_nodes:
            excluded = set(exclude_nodes)
            candidates = [n for n in candidates if n.node_id not in excluded]
            if not candidates:
                logger.warning(f"select_nodes: exclude_nodes={list(excluded)} 过滤后无候选节点")
                return []

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

    # ── P1-H 优先级队列 + 租户配额 ──

    def _running_count_for_user(self, user: str) -> int:
        """当前该租户 RUNNING 任务数 (锁内调用, 直接读 self.tasks)。"""
        if not user:
            user = ""
        return sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING and (t.user or "") == user)

    def configure_scheduling(self, tenant_max_concurrent: int) -> None:
        """P1-H: 设置租户并发配额 (0=不限)。供 CLI 从 ClusterConfig 注入。"""
        self._tenant_max_concurrent = max(0, int(tenant_max_concurrent))
        logger.info(f"P1-H 租户并发配额: {self._tenant_max_concurrent} (0=不限)")

    # ── F3 (#27): /v1/chat/completions 轻量代理租户配额 ──

    async def acquire_chat_slot(self, user: str) -> bool:
        """尝试占用一租户在途推理槽。超配额返 False (调用方 429)。

        与 _running_count_for_user 不同: chat 代理不建任务 (同步直返), 不进 self.tasks,
        故用独立计数器。配额复用 _tenant_max_concurrent (0=不限)。锁内纯计数无 await, 安全。
        """
        if self._tenant_max_concurrent == 0:
            return True
        uid = user or ""
        async with self._chat_lock:
            current = self._inflight_chat.get(uid, 0)
            if current >= self._tenant_max_concurrent:
                logger.info(f"chat 代理租户配额满: user={uid!r} {current}/{self._tenant_max_concurrent}")
                return False
            self._inflight_chat[uid] = current + 1
            return True

    async def release_chat_slot(self, user: str) -> None:
        """释放一租户在途推理槽 (finally 调用, 防泄漏)。配额 0=不限时空操作。"""
        if self._tenant_max_concurrent == 0:
            return
        uid = user or ""
        async with self._chat_lock:
            current = self._inflight_chat.get(uid, 0)
            if current <= 1:
                self._inflight_chat.pop(uid, None)
            else:
                self._inflight_chat[uid] = current - 1

    def _enqueue_pending(self, task: ClusterTask) -> None:
        """入优先级队列 — 按 priority 降序插入保持队列有序 (稳定排序)。

        同时登记到 self.tasks — 队列任务须可被 cancel_task/list_tasks/H3 持久化找到
        (PENDING 属非终态, _persist_tasks_locked 会落盘)。
        """
        task.status = TaskStatus.PENDING
        task.assigned_nodes = []
        self.tasks[task.task_id] = task
        idx = len(self._pending_queue)
        for i, t in enumerate(self._pending_queue):
            if task.priority > t.priority:
                idx = i
                break
        self._pending_queue.insert(idx, task)
        logger.info(
            f"P1-H 任务入队: {task.name} ({task.task_id}) user={task.user} "
            f"priority={task.priority} 队列长度={len(self._pending_queue)}"
        )

    async def _drain_pending_locked(self) -> None:
        """派发队列中可调度的任务 — 节点空闲/配额释放后调用。

        须持 _tasks_lock (调用方负责)。逐个取出队首任务重试 assign_task;
        assign_task 内部会再取锁 — 此处先释放再重入避免双重加锁。
        """
        if not self._pending_queue:
            return
        # 快照队列, 释放锁后逐个派发 (assign_task 自取锁)。
        queued = self._pending_queue[:]
        self._pending_queue.clear()
        logger.info(f"P1-H 排空队列: {len(queued)} 个待派发任务")
        for task in queued:
            ok = await self.assign_task(task)
            if not ok:
                # 仍派不出去 (节点不足/配额满) → 重新入队, 保持优先级序。
                self._enqueue_pending(task)

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
        # P1-H: 任务完成释放配额/节点 → 排空待派发队列 (高优先级先得)。
        await self._drain_pending_locked()

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
        # P1-11: 锁内仅快照, 落盘 (fsync) 移出锁外 (审计 §4.2)。
        _snapshot: list[dict[str, Any]] | None = None
        async with self._tasks_lock:
            existing = self.tasks.get(task.task_id)
            if existing and existing.status == TaskStatus.RUNNING:
                logger.debug(f"任务已分配，跳过: {task.task_id}")
                return True
            # P1-H 租户配额: 该租户 RUNNING 任务达上限 → 入优先级队列 (非 503)。
            # 0 = 不限。队列内任务重派时 _drain_pending_locked 已持有判断, 此处仅首入口拦截。
            if self._tenant_max_concurrent > 0:
                running = self._running_count_for_user(task.user)
                if running >= self._tenant_max_concurrent:
                    if task.task_id in {t.task_id for t in self._pending_queue}:
                        logger.debug(f"P1-H 任务已在队列, 跳过重复入队: {task.task_id}")
                        return True
                    self._enqueue_pending(task)
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
                        node and node.status == NodeStatus.ONLINE and node.available_memory_gb >= required_mem
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
                                self._emit_task_event(task, "running")
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
                exclude_nodes=task.exclude_nodes,
            )
        finally:
            if self._is_vram_first(task) and self.load_router.strategy != original_strategy:
                self.load_router.set_strategy(original_strategy)

        if len(nodes) < (len(task.model_shards) or 1):
            # P1-H: 节点不足不再直接 503 → 入优先级队列, 节点空闲时排空派发。
            logger.warning(f"可用节点不足: 需要 {len(task.model_shards) or 1}, 可用 {len(nodes)} → 入队等待")
            async with self._tasks_lock:
                if task.task_id in {t.task_id for t in self._pending_queue}:
                    return True
                self._enqueue_pending(task)
            return True

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
                    # #31: 补选同样遵守重试节点黑名单, 不回退到失败节点
                    excluded.update(task.exclude_nodes)
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
                    # P1-H: 并发抢占后仍不足 → 入优先级队列 (锁内, _enqueue_pending 无锁需求)。
                    logger.warning(f"并发抢占后可用节点不足: 需要 {need}, 确认 {len(confirmed)} → 入队等待")
                    self._enqueue_pending(task)
                    return True
                task.assigned_nodes = [n.node_id for n in confirmed]
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()
                self.tasks[task.task_id] = task
                # P1-11: 锁内仅快照, 落盘 (fsync) 移出锁外 (审计 §4.2)。
                _snapshot = self._persist_tasks_locked()

                for node in confirmed:
                    node.active_tasks += 1
                self._emit_task_event(task, "running")

        # P1-11: 落盘在 _nodes_lock/_tasks_lock 释放后。
        self._write_task_store(_snapshot)
        logger.info(f"任务分配: {task.name} → {[n.hostname for n in nodes]}")
        self._trigger_dispatch(task)
        return True

    def _enqueue_retry(self, task: ClusterTask) -> None:
        retry_count = getattr(task, "_retry_count", 0)
        if retry_count >= self._max_retry_attempts:
            task.status = TaskStatus.FAILED
            task.error = f"重试次数超限 ({self._max_retry_attempts})"
            logger.error(f"任务重试放弃: {task.name} ({task.task_id})")
            self._emit_task_event(task, "failed", error=task.error)
            return
        task._retry_count = retry_count + 1
        task.status = TaskStatus.PENDING
        task.assigned_nodes = []
        self._pending_retry.append(task)
        logger.info(f"任务入重试队列: {task.name} ({task.task_id}), 第 {task._retry_count} 次重试")
        self._emit_task_event(task, "retry")

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
            # C8: 派发级异常 (节点缺失/SSRF/HTTP 框架错误) = 瞬时传输失败, 可重试。
            await self._finalize_task(task, success=False, error=f"派发异常: {e}", retryable=True)

    async def _dispatch_data(
        self, task: ClusterTask, node_ids: list[str], nodes_snap: dict[str, NodeInfo], token: str
    ) -> None:
        """DATA 并行 — 各 assigned_node 并发 POST /api/execute, 任一失败记 error 但不阻塞其余。

        C8/C9: 区分两类失败 —
          - 传输级 (Exception: TCP reset/5xx/超时/SSRF) → report_fault + 瞬时可重试;
          - agent 内部逻辑错误 (200+ok+result 含 error) → report_fault + 不可重试 (重试同节点同任务大概率复现)。
        任一节点成功即有 output; 全失败则按失败类型决定 retryable。
        """
        client = await self._get_dispatch_http()
        coros = [self._dispatch_to_node(client, task, nid, nodes_snap, token) for nid in node_ids]
        results = await asyncio.gather(*coros, return_exceptions=True)
        outputs = []
        errors = []
        transient_fail = False
        logic_fail = False
        for nid, r in zip(node_ids, results):
            if isinstance(r, Exception):
                # 传输级失败 — _dispatch_to_node 已 report_fault (raise 前调), 此处只聚合
                errors.append(f"{nid}: {type(r).__name__}: {r}")
                transient_fail = True
            elif isinstance(r, dict) and r.get("rate_limited"):
                # GAP-6: fusion-mlx 429 限流 (客户端退避预算耗尽) = 瞬时失败, 可重试;
                # 不进 logic_fail (不 ban 健康节点), 不累加熔断器故障计数。
                errors.append(f"{nid}: 限流 (rate_limited): {r.get('error', '')}")
                transient_fail = True
                logger.info(f"节点 {nid} 限流瞬时失败 (可重试, 不计熔断): {r.get('error', '')[:120]}")
            elif isinstance(r, dict) and "error" in r:
                # C9: agent 内部错误 (OOM/坏模型) 返 200+ok+error — 对熔断器可见
                errors.append(f"{nid}: {r['error']}")
                logic_fail = True
                await self.report_fault(nid, fault_type="agent_internal_error", message=str(r["error"])[:200])
            elif isinstance(r, dict):
                outputs.append(r)
            else:
                errors.append(f"{nid}: 空响应")
                transient_fail = True
        # P3-29 (审计 §5.9): 部分成功语义 —
        #   全成功 → COMPLETED; 全失败 → FAILED (瞬时可重试走 _enqueue_retry);
        #   有成功有失败 → PARTIAL (终态, 不重试, 保留 outputs 供客户端取部分结果)。
        all_success = bool(outputs) and not errors
        partial_success = bool(outputs) and bool(errors)
        if all_success:
            await self._finalize_task(
                task,
                success=True,
                error="",
                result={"outputs": outputs, "errors": errors, "node_count": len(node_ids)},
            )
        elif partial_success:
            logger.warning(
                f"DATA 并行部分成功: {task.name} ({task.task_id}) "
                f"成功 {len(outputs)}/{len(node_ids)} 节点, 失败 {len(errors)} 节点"
            )
            await self._finalize_task(
                task,
                success=False,
                partial=True,
                error="; ".join(errors),
                result={"outputs": outputs, "errors": errors, "node_count": len(node_ids)},
            )
        else:
            # 全失败: 仅有传输级失败 → 可重试; 有 agent 逻辑错误 → 不重试 (C9 已 report_fault, 熔断器会 ban)
            retryable = transient_fail and not logic_fail
            await self._finalize_task(
                task,
                success=False,
                error="; ".join(errors),
                result={"outputs": outputs, "errors": errors, "node_count": len(node_ids)},
                retryable=retryable,
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
            r = await self._dispatch_to_node(client, task, nid, nodes_snap, token, pipeline_step_params=step_params)
            if isinstance(r, Exception):
                await self._finalize_task(task, success=False, error=f"流水线步骤 {nid} 失败: {r}", retryable=True)
                return
            if isinstance(r, dict) and r.get("rate_limited"):
                # GAP-6: 流水线段限流 = 瞬时可重试, 不 ban 节点。
                await self._finalize_task(
                    task, success=False, error=f"流水线步骤 {nid} 限流: {r.get('error', '')}", retryable=True
                )
                return
            if isinstance(r, dict) and "error" in r:
                await self._finalize_task(task, success=False, error=f"流水线步骤 {nid}: {r['error']}")
                return
            if not isinstance(r, dict) or "hidden_states" not in r:
                await self._finalize_task(task, success=False, error=f"流水线步骤 {nid} 未返回 hidden_states")
                return
            hidden_states = r["hidden_states"]
            steps.append(
                {
                    "node_id": nid,
                    "shard_id": r.get("shard_id", ""),
                    "shape": r.get("shape"),
                    "dtype": r.get("dtype"),
                }
            )
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
                # P1-14 (审计 §5.3): 传真实 dispatch id 供 agent 去重 — pipeline 每段一 id
                # (同 task 跨节点不同段不冲突, 同段重派同 id 触发 agent 拒重复)。
                shard_index = int(extra.get("shard_index", 0))
                dispatch_id = f"{task.task_id}-step{shard_index}"
                payload = {
                    "task_id": dispatch_id,
                    "task_type": task_type,
                    "model_name": task.model_name,
                    "extra": extra,
                }
            else:
                # 常规推理/Embedding 派发
                # P1-14 (审计 §5.3): 传真实 task_id 供 agent 拒同 task_id 重复派发。
                payload = {
                    "task_id": task.task_id,
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
            # P1-13 (审计 §5.4): 单请求 HTTP 超时随 task.timeout_seconds, 不再用客户端默认固定 300s。
            # 设为 task 超时 + 缓冲, 让任务级超时 (_check_task_timeouts → TIMEOUT+重试) 先于 HTTP 死代理兜底触发;
            # >300s 任务不再被 HTTP 提前掐断误判 FAILED 无重试。下限 30s 防极小超时。
            http_timeout = max(30.0, float(task.timeout_seconds) + 30.0)
            resp = await client.post(url, json=payload, headers=headers, timeout=http_timeout)
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
        self,
        task: ClusterTask,
        success: bool,
        error: str,
        result: dict[str, Any] | None = None,
        retryable: bool = False,
        partial: bool = False,
    ) -> None:
        """派发完成回填任务状态 + 释放节点 active_tasks 计数。

        C8: success=False 且 retryable=True (瞬时传输失败: TCP reset/5xx/超时/SSRF) →
        不直接 FAILED, 走 _enqueue_retry (重试预算内重派, 超限才 FAILED)。
        agent 内部逻辑错误 (OOM/坏模型) retryable=False → 直接 FAILED 不重试 (重试同任务同节点大概率复现)。
        P3-29 (审计 §5.9): partial=True (DATA 并行部分节点成功) → PARTIAL 终态, 不重试,
        保留 result.outputs 供客户端取部分结果, 不浪费已成功节点的工作。
        """
        # P1-11: 锁内仅快照, 落盘 (fsync) 移出锁外 (审计 §4.2)。
        _snapshot: list[dict[str, Any]] | None = None
        async with self._nodes_lock:
            async with self._tasks_lock:
                t = self.tasks.get(task.task_id)
                if not t or t.status != TaskStatus.RUNNING:
                    # 已被 cancel/timeout 改态, 不覆盖
                    cur_state = t.status.value if t else "gone"
                    logger.debug(f"派发回填跳过: 任务 {task.task_id} 状态已非 RUNNING ({cur_state})")
                    return
                for nid in t.assigned_nodes:
                    node = self.nodes.get(nid)
                    if node:
                        node.active_tasks = max(0, node.active_tasks - 1)
                if success:
                    t.status = TaskStatus.COMPLETED
                    t.completed_at = time.time()
                    t.error = ""
                    if result is not None:
                        t.result = result
                    _snapshot = self._persist_tasks_locked()  # H3 终态快照 (落盘移出锁外)
                    logger.info(f"派发回填: {t.name} ({t.task_id}) → {t.status.value}")
                    self._emit_task_event(t, "completed")
                # P3-29: 部分成功 → PARTIAL 终态 (不重试, 保留部分结果)
                elif partial:
                    t.status = TaskStatus.PARTIAL
                    t.completed_at = time.time()
                    t.error = error
                    if result is not None:
                        t.result = result
                    _snapshot = self._persist_tasks_locked()
                    logger.info(f"派发回填: {t.name} ({t.task_id}) → {t.status.value}")
                    self._emit_task_event(t, "partial", error=error)
                # 失败: 瞬时可重试 → 入重试队列 (预算内); 否则 FAILED
                elif retryable:
                    t.error = error
                    self._enqueue_retry(t)
                    _snapshot = self._persist_tasks_locked()
                else:
                    t.status = TaskStatus.FAILED
                    t.completed_at = time.time()
                    t.error = error
                    if result is not None:
                        t.result = result
                    _snapshot = self._persist_tasks_locked()  # H3 终态快照 (落盘移出锁外)
                    logger.info(f"派发回填: {t.name} ({t.task_id}) → {t.status.value}")
                    self._emit_task_event(t, "failed", error=error)
        if _snapshot is not None:
            self._write_task_store(_snapshot)

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
        cancelled_sub = []
        # P1-11: 锁内仅快照, 落盘 (fsync) 移出锁外 (审计 §4.2)。
        _snapshot: list[dict[str, Any]] | None = None
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
                # P1-H: 从优先级队列移除 (若在队)。
                self._pending_queue = [t for t in self._pending_queue if t.task_id != task_id]

                # 递归取消子任务（支持多层）
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
                            self._pending_queue = [t for t in self._pending_queue if t.task_id != sub_id]
                            if sub.sub_tasks:
                                cancel_stack.extend(sub.sub_tasks)

                task.status = TaskStatus.CANCELLED
                task.error = task.cancel_reason
                _snapshot = self._persist_tasks_locked()  # H3 终态快照 (落盘移出锁外)
                self._emit_task_event(task, "cancelled")
                for sid in cancelled_sub:
                    sub = self.tasks.get(sid)
                    if sub:
                        self._emit_task_event(sub, "cancelled", error=sub.cancel_reason)

        # P1-11: 落盘在 _nodes_lock/_tasks_lock 释放后。
        if _snapshot is not None:
            self._write_task_store(_snapshot)
        logger.info(f"任务取消: {task_id} (原因: {task.cancel_reason}), 子任务取消: {cancelled_sub}")
        # P1-H: 取消释放节点/配额 → 排空队列。
        await self._drain_pending_locked()
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
        """查找可复用的 KV 缓存。

        P1-12 (审计 §2.4/§4.4): 不再持 _kv_lock 跨域 await _is_node_online
        (原 kv→nodes 锁序违反 nodes→kv 约定, 嵌套跨域持锁有死锁风险)。
        先在 _nodes_lock 下快照在线节点集合 (nodes 域), 释放后在 _kv_lock 下匹配 —
        锁序 nodes→kv, 两个锁域不嵌套持有。
        """
        now = time.time()
        async with self._nodes_lock:
            online_nodes = {nid for nid, n in self.nodes.items() if n.status == NodeStatus.ONLINE}
        async with self._kv_lock:
            for cid, entry in list(self.kv_cache.items()):
                if entry.model_name == model_name and now - entry.created_at < entry.ttl_seconds:
                    if entry.node_id in online_nodes:
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
        # 上游 fusion-mlx /distributed/* 已交付 (issue #621/#630 closed: load_shard/
        # pipeline_step/decode/sync_weights), 但无 KV 张量迁移端点 → 本仓需自建
        # 跨节点张量传输通道 (P3-28 长期)。当前真实同步未发生, 返回 False 如实反映。
        logger.warning(
            f"M9-04 KV 缓存跨节点传输未实现: cache_id={cache_id} model={model_name} "
            f"source={source_node_id}。元数据已登记, 张量迁移待 P3-28 长期落地"
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
            send_heartbeat=self._send_heartbeat_cb,
            state_path=self._election_state_path,
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

    async def _send_heartbeat_cb(self, peer_node_id: str) -> None:
        """C1: Leader 心跳广播回调 — POST /api/ha/heartbeat 到对端 follower。

        best-effort: 无对端地址 / 不安全主机 / HTTP 失败一律吞, 不影响 leader 权威。
        """
        if not self._election:
            return
        cand = self._election.get_candidate(peer_node_id)
        if not cand or not cand.ip_address or not cand.port:
            return
        if not is_safe_peer_host(cand.ip_address):
            logger.warning(f"心跳跳过不安全对端: {peer_node_id} ({cand.ip_address!r})")
            return
        url = build_safe_url(mtls_scheme(), cand.ip_address, cand.port, "/api/ha/heartbeat")
        token = self._get_dispatch_token()
        payload = {
            "leader_id": self._election.node_id,
            "term": self._election.current_term,
        }
        try:
            client = await self._get_dispatch_http()
            resp = await client.post(url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=3.0)
            if resp.status_code != 200:
                logger.debug(f"心跳 HTTP {resp.status_code} from {peer_node_id}")
        except Exception as e:
            logger.debug(f"心跳异常 {peer_node_id}: {e}")

    async def handle_heartbeat(self, leader_id: str, term: int) -> None:
        """接收对端 leader 心跳 — 透传到选举管理器 (FOLLOWER 更新 term/心跳)。无选举配置时忽略。"""
        if not self._election:
            return
        await self._election.receive_heartbeat(leader_id, term)

    async def handle_vote_request(self, req: VoteRequest) -> VoteResponse:
        """接收对端 master 拉票 — 透传到选举管理器。无选举配置时拒绝 (单 Master 模式)。"""
        if not self._election:
            return VoteResponse(term=0, vote_granted=False, voter_id="")
        return await self._election.handle_vote_request(req)

    async def receive_synced_tasks(self, tasks: list[dict[str, Any]]) -> int:
        """Standby 接收 leader 推送的任务状态, 合并到 self.tasks 并持久化。幂等。"""
        merged = 0
        # P1-11: 锁内仅快照, 落盘 (fsync) 移出锁外 (审计 §4.2)。
        _snapshot: list[dict[str, Any]] | None = None
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
                _snapshot = self._persist_tasks_locked()
        # P1-11: 落盘在 _tasks_lock 释放后。
        if _snapshot is not None:
            self._write_task_store(_snapshot)
        if merged:
            logger.info(f"HA 同步接收 {merged} 任务 (standby 合并落盘)")
        return merged

    def _sync_tasks_to_standbys_locked(self) -> list[tuple[str, str, int, dict[str, Any]]]:
        """已持 _tasks_lock 下构建待推送 payload + 对端列表。返回 [(node_id, ip, port, payload), ...]。

        仅 leader 调用。终态任务不推送 (与 _persist_tasks_locked 一致)。
        """
        if not self._election or not self._is_leader:
            return []
        _TERMINAL = {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMEOUT,
            TaskStatus.PARTIAL,
        }
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
                resp = await client.post(url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=5.0)
                if resp.status_code != 200:
                    logger.debug(f"HA 同步推送 {peer_id} HTTP {resp.status_code}")
                else:
                    logger.debug(f"HA 同步推送 {peer_id} ok ({len(payload['tasks'])} 任务)")
            except Exception as e:
                logger.debug(f"HA 同步推送 {peer_id} 异常: {e}")

    # ── GAP-1 (Phase C): HA 全状态同步 — standby 持有完整集群拓扑, failover 即可调度 ──
    # 原 HA 仅同步 tasks; Master 宕机后 standby 缺 nodes/kv/banned → 重新注册才能调度,
    # 不满足 always-on。扩展: leader 周期推 nodes/kv_cache/banned_set/fault_counts,
    # standby receive_synced_state 幂等合并。HA 仍 opt-in (单 Master 部署不受影响)。

    def _node_to_dict(self, n: NodeInfo) -> dict[str, Any]:
        """NodeInfo 序列化 (status 枚举 → value 字符串, 与 _node_to_resp 一致)。"""
        return {
            "node_id": n.node_id,
            "hostname": n.hostname,
            "ip_address": n.ip_address,
            "port": n.port,
            "arch": n.arch,
            "total_memory_gb": n.total_memory_gb,
            "available_memory_gb": n.available_memory_gb,
            "cpu_cores": n.cpu_cores,
            "mlx_version": n.mlx_version,
            "gpu_cores": n.gpu_cores,
            "device_model": n.device_model,
            "uma_size_gb": n.uma_size_gb,
            "role": n.role,
            "status": n.status.value,
            "last_heartbeat": n.last_heartbeat,
            "tags": list(n.tags),
            "active_tasks": n.active_tasks,
            "max_tasks": n.max_tasks,
            "network_rtt_ms": n.network_rtt_ms,
        }

    def _node_from_dict(self, d: dict[str, Any]) -> NodeInfo:
        """NodeInfo 反序列化 (status 字符串 → NodeStatus 枚举)。"""
        try:
            status = NodeStatus(d.get("status", "offline"))
        except ValueError:
            status = NodeStatus.OFFLINE
        return NodeInfo(
            node_id=str(d.get("node_id", "")),
            hostname=str(d.get("hostname", "")),
            ip_address=str(d.get("ip_address", "")),
            port=int(d.get("port", 0)),
            arch=str(d.get("arch", "arm64")),
            total_memory_gb=float(d.get("total_memory_gb", 0.0)),
            available_memory_gb=float(d.get("available_memory_gb", 0.0)),
            cpu_cores=int(d.get("cpu_cores", 0)),
            mlx_version=str(d.get("mlx_version", "")),
            gpu_cores=int(d.get("gpu_cores", 0)),
            device_model=str(d.get("device_model", "")),
            uma_size_gb=float(d.get("uma_size_gb", 0.0)),
            role=str(d.get("role", "worker")),
            status=status,
            last_heartbeat=float(d.get("last_heartbeat", 0.0)),
            tags=list(d.get("tags", [])),
            active_tasks=int(d.get("active_tasks", 0)),
            max_tasks=int(d.get("max_tasks", 4)),
            network_rtt_ms=float(d.get("network_rtt_ms", 0.0)),
        )

    def _kv_to_dict(self, e: KVCacheEntry) -> dict[str, Any]:
        return {
            "cache_id": e.cache_id,
            "model_name": e.model_name,
            "node_id": e.node_id,
            "created_at": e.created_at,
            "size_mb": e.size_mb,
            "ttl_seconds": e.ttl_seconds,
            "access_count": e.access_count,
        }

    def _kv_from_dict(self, d: dict[str, Any]) -> KVCacheEntry:
        return KVCacheEntry(
            cache_id=str(d.get("cache_id", "")),
            model_name=str(d.get("model_name", "")),
            node_id=str(d.get("node_id", "")),
            created_at=float(d.get("created_at", 0.0)),
            size_mb=float(d.get("size_mb", 0.0)),
            ttl_seconds=float(d.get("ttl_seconds", 3600.0)),
            access_count=int(d.get("access_count", 0)),
        )

    async def receive_synced_state(self, state: dict[str, Any]) -> dict[str, int]:
        """Standby 接收 leader 推送的集群状态 (nodes/kv/banned), 幂等合并。

        锁序 nodes→kv (声明顺序), 两域分别持锁不嵌套。
        返回 {"nodes": N, "kv": K, "banned": B} 合并计数。
        """
        counts = {"nodes": 0, "kv": 0, "banned": 0}
        nodes_data = state.get("nodes", [])
        kv_data = state.get("kv_cache", [])
        banned_data = state.get("banned_nodes", {})

        # nodes 域: 合并节点表 + banned + fault_counts (同受 _nodes_lock)。
        async with self._nodes_lock:
            for d in nodes_data:
                try:
                    info = self._node_from_dict(d)
                    if not info.node_id:
                        continue
                    # standby 不覆盖本机 master 节点自身记录的活跃运行态
                    # (leader 推来的 active_tasks 可能略滞后, 但 failover 后以 leader 快照为准)
                    self.nodes[info.node_id] = info
                    self._sync_node_metrics(info)
                    counts["nodes"] += 1
                except Exception as e:
                    logger.warning(f"HA 状态同步跳过节点 {d.get('node_id', '?')}: {e}")
            if isinstance(banned_data, dict):
                now = time.time()
                # 合并 ban: 取较晚解封时间 (leader 与 standby 任意一方 ban 更权威)
                for nid, unban_at_raw in banned_data.items():
                    try:
                        unban_at = float(unban_at_raw)
                    except (TypeError, ValueError):
                        continue
                    if unban_at <= now:
                        continue
                    cur = self._banned_nodes.get(nid)
                    if cur is None or unban_at > cur:
                        self._banned_nodes[nid] = unban_at
                        counts["banned"] += 1

        # kv 域: 合并 KV 缓存 (按 cache_id 覆盖, 不超 _max_kv_cache 上限则保留)。
        async with self._kv_lock:
            for d in kv_data:
                try:
                    entry = self._kv_from_dict(d)
                    if not entry.cache_id:
                        continue
                    self.kv_cache[entry.cache_id] = entry
                    counts["kv"] += 1
                except Exception as e:
                    logger.warning(f"HA 状态同步跳过 KV {d.get('cache_id', '?')}: {e}")
            # 惰性裁剪超限 (与 register_kv_cache 一致)
            while len(self.kv_cache) > self._max_kv_cache:
                oldest = min(self.kv_cache.items(), key=lambda x: x[1].created_at)
                del self.kv_cache[oldest[0]]

        if any(counts.values()):
            logger.info(f"HA 状态同步接收: nodes={counts['nodes']} kv={counts['kv']} banned={counts['banned']}")
        return counts

    async def _build_state_sync_targets(self) -> list[tuple[str, str, int, dict[str, Any]]]:
        """构建全状态推送 payload + 对端列表 (自带 nodes→kv 两锁分别快照, 不嵌套)。

        仅 leader 调用。返回 [(node_id, ip, port, payload), ...]。
        """
        if not self._election or not self._is_leader:
            return []
        now = time.time()
        # nodes 域快照 (含 banned — 同受 _nodes_lock)
        nodes_list: list[dict[str, Any]] = []
        banned_snapshot: dict[str, float] = {}
        async with self._nodes_lock:
            nodes_list = [self._node_to_dict(n) for n in self.nodes.values()]
            banned_snapshot = {nid: unban_at for nid, unban_at in self._banned_nodes.items() if unban_at > now}
        # kv 域快照
        kv_list: list[dict[str, Any]] = []
        async with self._kv_lock:
            kv_list = [self._kv_to_dict(e) for e in self.kv_cache.values()]
        payload = {
            "nodes": nodes_list,
            "kv_cache": kv_list,
            "banned_nodes": banned_snapshot,
            "saved_at": now,
        }
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

    async def _push_sync_state_to_standbys(self, targets: list[tuple[str, str, int, dict[str, Any]]]) -> None:
        """锁外异步推送全状态到各 standby (best-effort)。"""
        if not targets:
            return
        token = self._get_dispatch_token()
        try:
            client = await self._get_dispatch_http()
        except Exception as e:
            logger.warning(f"HA 状态同步获取 HTTP 客户端失败: {e}")
            return
        for peer_id, ip, port, payload in targets:
            try:
                url = build_safe_url(mtls_scheme(), ip, port, "/api/ha/sync-state")
                resp = await client.post(url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=5.0)
                if resp.status_code != 200:
                    logger.debug(f"HA 状态同步推送 {peer_id} HTTP {resp.status_code}")
                else:
                    logger.debug(
                        f"HA 状态同步推送 {peer_id} ok (nodes={len(payload['nodes'])} kv={len(payload['kv_cache'])})"
                    )
            except Exception as e:
                logger.debug(f"HA 状态同步推送 {peer_id} 异常: {e}")

    async def _sync_state_to_standbys(self) -> None:
        """leader 周期推全状态到 standby (由 _state_sync_loop 调)。"""
        targets = await self._build_state_sync_targets()
        if targets:
            await self._push_sync_state_to_standbys(targets)

    async def _state_sync_loop(self) -> None:
        """GAP-1: 周期全状态同步 (5s) — leader 推 nodes/kv/banned 到 standby, best-effort。"""
        try:
            while self._running:
                await asyncio.sleep(5)
                if self._election and self._is_leader:
                    try:
                        await self._sync_state_to_standbys()
                    except Exception as e:
                        logger.warning(f"HA 状态同步循环异常: {e}")
        except asyncio.CancelledError:
            pass

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
        config: ClusterConfig | None = None,
    ) -> None:
        """启动集群主节点服务。

        ha_config (P4 HA): {"enabled": bool, "node_id": str, "priority": int,
                            "peers": [str | {"node_id","ip","port","priority"}]}
        enabled=True 时接 setup_election 启动选举循环 + HTTP 拉票 + 任务同步;
        否则单 Master 无 HA (默认)。peers 裸字符串向后兼容 (仅 node_id, 不可达)。
        config (P2-20 热加载 §6.8): ClusterConfig 实例, 传给 MasterServer 供
        /api/v1/config/reload 热重载 (重读 config.json + 重应用运行时可调字段)。
        """
        self._cluster_config = config
        self._running = True
        logger.info(f"Cluster Master 启动: {self.host}:{self.port}")
        logger.info(f"节点发现端口: {self.discovery_port}")

        # H3 启动恢复: 重建崩溃前未完成任务 (RUNNING→PENDING 重派)
        restored = await self._restore_tasks()
        # P1-H: 恢复的 PENDING 任务入优先级队列排空派发 (无节点则留在队列等注册)。
        if restored:
            async with self._tasks_lock:
                for task in list(self.tasks.values()):
                    if task.status == TaskStatus.PENDING and task.task_id not in {
                        t.task_id for t in self._pending_queue
                    }:
                        self._enqueue_pending(task)
            await self._drain_pending_locked()

        self._health_task = asyncio.create_task(self._health_check_loop())
        self._retry_task = asyncio.create_task(self._retry_loop())
        self._persist_task = asyncio.create_task(self._persist_loop())

        # P0-8: 接线 Observability — 周期采集指标 + 告警, 路由不再 503。
        if self._observability is None:
            self._observability = ClusterObservability()
        await self._observability.start()
        logger.info("P0-8 Observability 已接线 (周期采集 + 告警规则)")

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
                # GAP-1 (Phase C): 启动全状态同步循环 — standby 持有完整拓扑, failover 即调度。
                self._state_sync_task = asyncio.create_task(self._state_sync_loop())
                logger.info("P4 HA 选举已启动 (HTTP 拉票 + 任务同步 + 全状态同步已接线)")
        else:
            logger.info("P4 HA 未启用 — 单 Master 模式 (默认)")

        if with_mdns:
            await self._start_mdns()

        if with_server:
            from fusion_multi_node.server import MasterServer

            server = MasterServer(master=self, config=self._cluster_config)
            await server.start(host=self.host, port=self.port)

    async def stop(self) -> None:
        """停止集群主节点。"""
        self._running = False
        for task in (
            self._health_task,
            self._retry_task,
            self._persist_task,
            self._state_sync_task,
        ):
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
        # P0-8: 停止 Observability 清理循环
        if self._observability:
            await self._observability.stop()
        self._stop_mdns()
        logger.info("Cluster Master 已停止")

    async def _start_mdns(self) -> None:
        try:
            from fusion_multi_node.discovery import MDNSDiscovery

            # P1-10: sysctl 同步子进程 (100ms-1s) — to_thread 移出 event loop (审计 §4.5)。
            device_model, uma_size_gb = await asyncio.to_thread(self._collect_mdns_props)
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

    @staticmethod
    def _collect_mdns_props() -> tuple[str, str]:
        """同步采 sysctl 设备型号/内存 (供 _start_mdns to_thread 调)。"""
        import subprocess

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
        return device_model, uma_size_gb

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

    async def _collect_observability_locked(self) -> None:
        """P0-8: 周期采集节点指标 + 告警规则 (health_loop 内调)。

        nodes 域快照 (锁内取 dict 视图) → 锁外 record_metric/check_alert_rules,
        避免 record_metric 入队 deque 持锁跨 await。check_alert_rules 内部去重。
        """
        async with self._nodes_lock:
            node_view: dict[str, dict[str, Any]] = {
                nid: {
                    "status": n.status.value,
                    "hostname": n.hostname,
                    "available_memory_gb": n.available_memory_gb,
                    "total_memory_gb": n.total_memory_gb,
                }
                for nid, n in self.nodes.items()
            }
        obs = self._observability
        if obs is None:
            return
        for nid, view in node_view.items():
            if view["total_memory_gb"] > 0:
                obs.record_metric(nid, "mem_used_gb", view["total_memory_gb"] - view["available_memory_gb"])
        running = sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING)
        obs.record_metric("cluster", "active_tasks", float(running))
        try:
            await obs.check_alert_rules(node_view)
        except Exception as e:
            logger.warning(f"P0-8 告警规则检查失败: {e}")

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
                # P0-8: 周期采集节点指标 + 告警规则 (接 _observability, 路由不再 503)。
                if self._observability:
                    await self._collect_observability_locked()
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
                    TaskStatus.PARTIAL,
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

    # ── P1-18 (审计 §5.5): 任务状态 SSE 推送通道 ──

    def subscribe_task_events(self) -> asyncio.Queue:
        """订阅任务状态事件 — 返回 Queue, SSE 端点 await get() 流式推客户端。
        满队列时 _emit_task_event 丢最旧事件 (put_nowait 抛 QueueFull → pop), 不阻塞调度路径。
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        self._event_subscribers.append(q)
        logger.info(f"P1-18 新增 SSE 订阅者 (共 {len(self._event_subscribers)})")
        return q

    def unsubscribe_task_events(self, q: asyncio.Queue) -> None:
        """SSE 端点断开时注销, 避免泄漏 + 向死队列推事件。"""
        if q in self._event_subscribers:
            self._event_subscribers.remove(q)
            logger.info(f"P1-18 SSE 订阅者注销 (剩 {len(self._event_subscribers)})")

    def _emit_task_event(self, task: ClusterTask, event: str, error: str = "") -> None:
        """向所有 SSE 订阅者非阻塞推任务状态事件。锁内或锁外皆可调 (纯内存, 无 await)。
        满队列丢最旧条目腾位 (put_nowait QueueFull → get_nowait 丢一条再 put), 不阻塞调度。
        """
        if not self._event_subscribers:
            return
        payload = {
            "task_id": task.task_id,
            "name": task.name,
            "status": task.status.value,
            "event": event,
            "error": error,
            "model_name": task.model_name,
            "retry_count": getattr(task, "_retry_count", 0),
        }
        for q in list(self._event_subscribers):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(payload)
                except asyncio.QueueEmpty:
                    pass
                except Exception:
                    logger.debug("P1-18 丢 SSE 事件 (订阅者队列异常)")

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
            # P3-29: PARTIAL 单独计数, 不并入 failed (有部分结果, 非全失败)
            partial_tasks = sum(1 for t in self.tasks.values() if t.status == TaskStatus.PARTIAL)
            failed_tasks = sum(1 for t in self.tasks.values() if t.status in (TaskStatus.FAILED, TaskStatus.TIMEOUT))
        async with self._kv_lock:
            kv_cache_entries = len(self.kv_cache)
        stats = {
            "total_nodes": total_nodes,
            "online_nodes": len(online_nodes),
            "total_tasks": total_tasks,
            "active_tasks": active_tasks,
            "completed_tasks": completed_tasks,
            "partial_tasks": partial_tasks,
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
            "# HELP fusion_cluster_tasks_partial 部分成功任务数 (DATA 并行部分节点成功)",
            "# TYPE fusion_cluster_tasks_partial gauge",
            f"fusion_cluster_tasks_partial {stats['partial_tasks']}",
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

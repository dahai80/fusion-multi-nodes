"""M10 自动伸缩器 — 根据集群负载自动调整节点资源。

- 自动扩容: 任务队列积压时唤醒/添加节点
- 自动缩容: 空闲节点超时后进入休眠
- 负载再平衡: 节点负载不均时重新分配任务
- 策略可配: 支持 min/max nodes、冷却时间、阈值等
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class ScalePolicy(Enum):
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"


class ScaleAction(Enum):
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    REBALANCE = "rebalance"
    NOOP = "noop"


@dataclass
class AutoscalerConfig:
    min_nodes: int = 1
    max_nodes: int = 10
    scale_up_threshold: float = 0.8
    scale_down_threshold: float = 0.3
    cooldown_seconds: float = 120.0
    idle_timeout_seconds: float = 300.0
    policy: ScalePolicy = ScalePolicy.BALANCED
    check_interval: float = 30.0
    rebalance_threshold: float = 0.4


POLICY_DEFAULTS = {
    ScalePolicy.CONSERVATIVE: AutoscalerConfig(
        scale_up_threshold=0.9,
        scale_down_threshold=0.2,
        cooldown_seconds=300.0,
        idle_timeout_seconds=600.0,
    ),
    ScalePolicy.BALANCED: AutoscalerConfig(
        scale_up_threshold=0.8,
        scale_down_threshold=0.3,
        cooldown_seconds=120.0,
        idle_timeout_seconds=300.0,
    ),
    ScalePolicy.AGGRESSIVE: AutoscalerConfig(
        scale_up_threshold=0.6,
        scale_down_threshold=0.4,
        cooldown_seconds=60.0,
        idle_timeout_seconds=120.0,
    ),
}


class Autoscaler:
    """自动伸缩器。

    定期检查集群负载，根据策略执行扩缩容。
    优先使用回调；无回调时使用内建动作直接与 ClusterMaster 协同。
    """

    def __init__(
        self,
        config: Optional[AutoscalerConfig] = None,
        policy: Optional[ScalePolicy] = None,
        on_scale_up: Optional[Callable[[int], Any]] = None,
        on_scale_down: Optional[Callable[[str], Any]] = None,
        on_rebalance: Optional[Callable[[], Any]] = None,
        get_cluster_state: Optional[Callable[[], Dict[str, Any]]] = None,
        migrate_task: Optional[Callable[[str], Any]] = None,
        cluster_master: Any = None,
    ):
        if policy and not config:
            defaults = POLICY_DEFAULTS.get(policy, AutoscalerConfig())
            self.config = AutoscalerConfig(
                min_nodes=defaults.min_nodes,
                max_nodes=defaults.max_nodes,
                scale_up_threshold=defaults.scale_up_threshold,
                scale_down_threshold=defaults.scale_down_threshold,
                cooldown_seconds=defaults.cooldown_seconds,
                idle_timeout_seconds=defaults.idle_timeout_seconds,
                policy=policy,
            )
        else:
            self.config = config or AutoscalerConfig()

        self._on_scale_up = on_scale_up
        self._on_scale_down = on_scale_down
        self._on_rebalance = on_rebalance
        self._get_cluster_state = get_cluster_state
        self._migrate_task = migrate_task
        self._master = cluster_master

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_action_time: float = 0.0
        self._action_history: List[Dict[str, Any]] = []
        self._max_history = 200

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        logger.info(f"自动伸缩器启动 (policy={self.config.policy.value})")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("自动伸缩器已停止")

    async def _check_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.config.check_interval)
                await self.evaluate()
        except asyncio.CancelledError:
            pass

    async def evaluate(self) -> ScaleAction:
        if not self._get_cluster_state:
            return ScaleAction.NOOP

        state = self._get_cluster_state()
        nodes = state.get("nodes", [])
        tasks = state.get("tasks", [])

        online_nodes = [n for n in nodes if n.get("status") == "online"]
        _active_tasks = [t for t in tasks if t.get("status") == "running"]
        pending_tasks = [t for t in tasks if t.get("status") == "pending"]

        num_online = len(online_nodes)

        # 计算集群负载
        total_capacity = sum(n.get("max_tasks", 4) for n in online_nodes)
        total_load = sum(n.get("active_tasks", 0) for n in online_nodes)
        load_ratio = total_load / max(total_capacity, 1)

        # 冷却检查
        now = time.time()
        in_cooldown = (now - self._last_action_time) < self.config.cooldown_seconds

        action = ScaleAction.NOOP

        # 扩容: 负载超过阈值 + 有待执行任务 + 不在冷却期 + 未达最大节点数
        if (load_ratio > self.config.scale_up_threshold
                and pending_tasks
                and not in_cooldown
                and num_online < self.config.max_nodes):
            target_count = min(
                num_online + max(1, len(pending_tasks) // 4),
                self.config.max_nodes,
            )
            action = ScaleAction.SCALE_UP
            logger.info(f"自动扩容: {num_online} → {target_count} (负载={load_ratio:.0%})")
            if self._on_scale_up:
                if asyncio.iscoroutinefunction(self._on_scale_up):
                    await self._on_scale_up(target_count)
                else:
                    self._on_scale_up(target_count)
            else:
                await self._builtin_scale_up(target_count)

        # 缩容: 负载低于阈值 + 有空闲节点 + 不在冷却期 + 未达最小节点数
        elif load_ratio < self.config.scale_down_threshold and not in_cooldown and num_online > self.config.min_nodes:
            idle_nodes = [
                n for n in online_nodes
                if n.get("active_tasks", 0) == 0
                and (now - n.get("last_heartbeat", now)) > self.config.idle_timeout_seconds
            ]
            scale_down_candidates = idle_nodes if idle_nodes else [
                n for n in online_nodes
                if n.get("active_tasks", 0) > 0
                and n.get("node_id", "") not in [t.get("preferred_node_id", "") for t in _active_tasks]
            ]
            if idle_nodes:
                victim = idle_nodes[0]
            elif scale_down_candidates and num_online > self.config.min_nodes:
                victim = min(scale_down_candidates, key=lambda n: n.get("active_tasks", 0))
            else:
                victim = None

            if victim:
                victim_id = victim.get("node_id", "")
                if victim.get("active_tasks", 0) > 0:
                    await self.migrate_tasks_from_node(victim_id)
                action = ScaleAction.SCALE_DOWN
                logger.info(f"自动缩容: 移除 {victim_id}")
                if self._on_scale_down:
                    if asyncio.iscoroutinefunction(self._on_scale_down):
                        await self._on_scale_down(victim_id)
                    else:
                        self._on_scale_down(victim_id)
                else:
                    await self._builtin_scale_down(victim_id)

        # 再平衡: 节点间负载差异过大
        elif online_nodes:
            loads = [n.get("active_tasks", 0) / max(n.get("max_tasks", 4), 1) for n in online_nodes]
            if loads:
                load_spread = max(loads) - min(loads)
                if load_spread > self.config.rebalance_threshold:
                    action = ScaleAction.REBALANCE
                    migrated = await self._rebalance_tasks(online_nodes, tasks)
                    logger.info(f"M10-05 负载再平衡: 差异={load_spread:.0%}, 迁移任务={migrated}")
                    if self._on_rebalance:
                        if asyncio.iscoroutinefunction(self._on_rebalance):
                            await self._on_rebalance()
                        else:
                            self._on_rebalance()

        if action != ScaleAction.NOOP:
            self._last_action_time = time.time()
            self._record_action(action, load_ratio, num_online)

        return action

    def _record_action(self, action: ScaleAction, load_ratio: float, node_count: int) -> None:
        self._action_history.append({
            "action": action.value,
            "load_ratio": round(load_ratio, 3),
            "node_count": node_count,
            "timestamp": time.time(),
        })
        if len(self._action_history) > self._max_history:
            self._action_history = self._action_history[-self._max_history:]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "policy": self.config.policy.value,
            "config": {
                "min_nodes": self.config.min_nodes,
                "max_nodes": self.config.max_nodes,
                "scale_up_threshold": self.config.scale_up_threshold,
                "scale_down_threshold": self.config.scale_down_threshold,
            },
            "action_history": self._action_history[-10:],
            "total_actions": len(self._action_history),
        }

    async def migrate_tasks_from_node(self, node_id: str) -> int:
        """M10-03: 缩容前迁移节点上的活跃任务到其他节点。"""
        if not self._get_cluster_state:
            logger.warning("M10-03 任务迁移: 无法获取集群状态")
            return 0

        state = self._get_cluster_state()
        tasks = state.get("tasks", [])
        running_on_node = [
            t for t in tasks
            if t.get("status") == "running" and node_id in t.get("assigned_nodes", [])
        ]

        if not running_on_node:
            logger.info(f"M10-03 任务迁移: 节点 {node_id} 无活跃任务")
            return 0

        migrated = 0
        for task in running_on_node:
            task_id = task.get("task_id", "")
            logger.info(f"M10-03 任务迁移: {task_id} 从节点 {node_id} 迁出")
            if self._migrate_task and asyncio.iscoroutinefunction(self._migrate_task):
                ok = await self._migrate_task(task_id)
            elif self._migrate_task:
                ok = self._migrate_task(task_id)
            else:
                logger.warning(f"M10-03 任务迁移: 无 migrate_task 回调，跳过 {task_id}")
                ok = False
            if ok:
                migrated += 1
                logger.info(f"M10-03 任务迁移成功: {task_id}")
            else:
                logger.warning(f"M10-03 任务迁移失败: {task_id}")

        logger.info(f"M10-03 节点 {node_id} 任务迁移完成: {migrated}/{len(running_on_node)}")
        return migrated

    async def _rebalance_tasks(self, online_nodes: List[Dict[str, Any]], tasks: List[Dict[str, Any]]) -> int:
        """M10-05: 实际任务再平衡 — 从过载节点迁移任务到低载节点。"""
        if not online_nodes or not self._migrate_task:
            return 0

        node_load = {}
        for n in online_nodes:
            nid = n.get("node_id", "")
            active = n.get("active_tasks", 0)
            capacity = max(n.get("max_tasks", 4), 1)
            ratio = active / capacity
            score = n.get("score", 0.0)
            node_load[nid] = {
                "active_tasks": active,
                "max_tasks": capacity,
                "load_ratio": ratio,
                "score": score,
            }

        if not node_load:
            return 0

        avg_load = sum(v["load_ratio"] for v in node_load.values()) / len(node_load)

        overloaded = sorted(
            [nid for nid, v in node_load.items() if v["load_ratio"] > avg_load + self.config.rebalance_threshold / 2],
            key=lambda nid: -node_load[nid]["load_ratio"],
        )
        underloaded = sorted(
            [nid for nid, v in node_load.items() if v["load_ratio"] < avg_load - self.config.rebalance_threshold / 2],
            key=lambda nid: node_load[nid]["load_ratio"],
        )

        if not overloaded or not underloaded:
            logger.debug("M10-05 再平衡: 无明显过载/低载节点")
            return 0

        running_tasks = [t for t in tasks if t.get("status") == "running"]
        task_by_node = {}
        for t in running_tasks:
            for nid in t.get("assigned_nodes", []):
                task_by_node.setdefault(nid, []).append(t)

        migrated = 0
        for over_nid in overloaded:
            tasks_on_node = task_by_node.get(over_nid, [])
            excess = int(node_load[over_nid]["active_tasks"] - avg_load * node_load[over_nid]["max_tasks"])
            if excess <= 0:
                continue

            for task in tasks_on_node[:excess]:
                task_id = task.get("task_id", "")
                logger.info(f"M10-05 再平衡迁移: {task_id} 从 {over_nid}")
                if asyncio.iscoroutinefunction(self._migrate_task):
                    ok = await self._migrate_task(task_id)
                else:
                    ok = self._migrate_task(task_id)
                if ok:
                    migrated += 1
                    logger.info(f"M10-05 再平衡迁移成功: {task_id}")
                else:
                    logger.warning(f"M10-05 再平衡迁移失败: {task_id}")
                if migrated >= excess:
                    break

        logger.info(f"M10-05 再平衡完成: 迁移 {migrated} 个任务")
        return migrated

    def force_scale_up(self, count: int = 1) -> None:
        logger.info(f"强制扩容: +{count}")
        if self._on_scale_up:
            current = 0
            self._on_scale_up(current + count)

    def force_scale_down(self, node_id: str) -> None:
        logger.info(f"强制缩容: {node_id}")
        if self._on_scale_down:
            self._on_scale_down(node_id)

    def update_config(self, config: AutoscalerConfig) -> None:
        """M10-04 热更新 Autoscaler 配置。"""
        old = self.config
        self.config = config
        self._last_action_time = 0.0
        logger.info(
            f"M10-04 配置热更新: "
            f"min={old.min_nodes}→{config.min_nodes} "
            f"max={old.max_nodes}→{config.max_nodes} "
            f"up_thresh={old.scale_up_threshold}→{config.scale_up_threshold} "
            f"down_thresh={old.scale_down_threshold}→{config.scale_down_threshold} "
            f"cooldown={old.cooldown_seconds}→{config.cooldown_seconds}"
        )

    def update_policy(self, policy: ScalePolicy) -> None:
        """M10-04 通过策略名热更新配置。"""
        defaults = POLICY_DEFAULTS.get(policy, AutoscalerConfig())
        new_config = AutoscalerConfig(
            min_nodes=defaults.min_nodes,
            max_nodes=defaults.max_nodes,
            scale_up_threshold=defaults.scale_up_threshold,
            scale_down_threshold=defaults.scale_down_threshold,
            cooldown_seconds=defaults.cooldown_seconds,
            idle_timeout_seconds=defaults.idle_timeout_seconds,
            policy=policy,
            check_interval=self.config.check_interval,
            rebalance_threshold=self.config.rebalance_threshold,
        )
        self.update_config(new_config)

    async def _builtin_scale_up(self, target_count: int) -> None:
        """M10-02 内建扩容: 激活 standby 节点使其上线。"""
        if not self._master:
            logger.warning("M10-02 内建扩容: 无 cluster_master 引用，跳过")
            return
        standby_nodes = []
        try:
            for nid, info in self._master.nodes.items():
                if getattr(info, "role", "worker") == "standby":
                    standby_nodes.append(nid)
        except Exception as e:
            logger.error(f"M10-02 内建扩容: 读取节点列表失败: {e}")
            return
        if not standby_nodes:
            logger.info("M10-02 内建扩容: 无 standby 节点可激活")
            return
        online_count = sum(
            1 for n in (self._master.nodes.values() if self._master else [])
            if getattr(n, "status", None) and n.status.value == "online"
        )
        needed = min(target_count - online_count, len(standby_nodes))
        activated = 0
        for nid in standby_nodes[:needed]:
            try:
                info = self._master.nodes.get(nid)
                if info:
                    from fusion_multi_node.master.cluster_master import NodeStatus
                    info.status = NodeStatus.ONLINE
                    info.role = "worker"
                    activated += 1
                    logger.info(f"M10-02 激活 standby 节点: {nid}")
            except Exception as e:
                logger.error(f"M10-02 激活 standby 节点失败 {nid}: {e}")
        logger.info(f"M10-02 内建扩容完成: 激活 {activated}/{needed} standby 节点")

    async def _builtin_scale_down(self, node_id: str) -> None:
        """M10-03 内建缩容: 先迁移任务，再设为 standby。"""
        if not self._master:
            logger.warning("M10-03 内建缩容: 无 cluster_master 引用，跳过")
            return
        migrated = await self.migrate_tasks_from_node(node_id)
        logger.info(f"M10-03 内建缩容: 节点 {node_id} 迁移 {migrated} 个任务")
        try:
            info = self._master.nodes.get(node_id)
            if info:
                from fusion_multi_node.master.cluster_master import NodeStatus
                info.status = NodeStatus.OFFLINE
                info.role = "standby"
                logger.info(f"M10-03 节点 {node_id} 已设为 standby")
        except Exception as e:
            logger.error(f"M10-03 内建缩容失败 {node_id}: {e}")

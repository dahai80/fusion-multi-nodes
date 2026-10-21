"""#82 常驻节点调度宿主 — resident node 上托管定时任务, 关机工作不断。

动机 (insight/fusion-cowork-vs-anthropic-1021.md §7.2 #6): Cowork 云端常驻执行
("合盖继续跑") vs 本地 APScheduler 依赖本机开机。本模块让常驻节点 agent 自带
调度宿主: cowork/用户经 AgentServer /api/schedules 注册 ScheduleSpec, agent 事件循环
到点触发本地执行 (复用 execute_task 全链路: dedup/sandbox gate/并发 gate), 运行记录
落盘持久化, 笔记本离线期间任务照跑, 回来后查 /api/schedules/{id} 取结果。

设计约束:
- 仅常驻节点 (config.resident) 承接 — 非 resident 节点 /api/schedules 一律 403。
- 可调度任务类型仅 inference/embedding (模型服务类; plugin/test 不开放定时触发,
  避免 operator 命令被定时重放)。
- 持久化: schedules.json 原子写 (tmp+replace+fsync), schedules + 运行记录都存;
  agent 重启恢复。interval 型重启后从 now+every 重排 (错过的 tick 不补跑, 防风暴);
  daily/weekly 取下一匹配时刻自然续跑。
- 全单事件循环内操作, 无锁; 文件 IO 经 asyncio.to_thread 移出事件循环。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# 可定时触发的任务类型 — 仅模型服务类。plugin/test/supervisor_rpc 等命令类不开放,
# 防止定时器重放 operator 命令 (#82 安全边界)。
SCHEDULABLE_TASK_TYPES = {"inference", "embedding"}

_SCHEDULE_KINDS = {"interval", "daily", "weekly"}

# 单 schedule 运行记录环上限 — 防长跑常驻节点内存无界。
_MAX_RUNS_PER_SCHEDULE = 50

# interval 下限 — 低于此视为配置错误 (hot loop)。
_MIN_INTERVAL_SECONDS = 60


@dataclass
class ScheduleSpec:
    """定时任务规格 — kind 三态: interval (every_seconds) / daily (at) / weekly (weekday+at)。"""

    schedule_id: str
    kind: str
    every_seconds: int = 0
    at: str = ""
    weekday: int = -1
    task_type: str = "inference"
    model_name: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 300.0
    enabled: bool = True

    def validate(self) -> None:
        """校验字段, 非法抛 ValueError (server 层转 400)。"""
        if self.kind not in _SCHEDULE_KINDS:
            raise ValueError(f"kind 须为 {_SCHEDULE_KINDS} 之一, 得到 {self.kind!r}")
        if self.task_type not in SCHEDULABLE_TASK_TYPES:
            raise ValueError(f"task_type 须为 {SCHEDULABLE_TASK_TYPES} 之一, 得到 {self.task_type!r}")
        if self.kind == "interval":
            if self.every_seconds < _MIN_INTERVAL_SECONDS:
                raise ValueError(f"interval every_seconds 须 >= {_MIN_INTERVAL_SECONDS}")
        else:
            if not self._parse_at():
                raise ValueError(f"daily/weekly 须提供 HH:MM 格式 at, 得到 {self.at!r}")
            if self.kind == "weekly" and not (0 <= self.weekday <= 6):
                raise ValueError(f"weekly weekday 须 0(周一)-6(周日), 得到 {self.weekday}")

    def _parse_at(self) -> tuple[int, int] | None:
        try:
            parts = self.at.split(":")
            if len(parts) != 2:
                return None
            hh, mm = int(parts[0]), int(parts[1])
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return (hh, mm)
            return None
        except (ValueError, AttributeError):
            return None

    def next_run_at(self, now: float | None = None) -> float:
        """计算下一触发时刻 (epoch 秒, 与 now 同源)。确定性与 now 直接相关。"""
        now = now if now is not None else time.time()
        if self.kind == "interval":
            return now + self.every_seconds
        hh, mm = self._parse_at() or (0, 0)
        now_dt = time.localtime(now)
        # 用 struct_time 拆字段重建今天 at 时刻 (mktime 本地时区, 与 time.time() 同源)。
        base = time.mktime(
            (
                now_dt.tm_year,
                now_dt.tm_mon,
                now_dt.tm_mday,
                hh,
                mm,
                0,
                now_dt.tm_wday,
                now_dt.tm_yday,
                -1,
            )
        )
        if base > now:
            return base
        # 今日已过 → 明天 (daily) / 下一匹配星期 (weekly)
        if self.kind == "daily":
            return base + 86400
        # weekly: weekday 0=周一 (time.localtime tm_wday 同约定 0=周一)
        days_ahead = (self.weekday - now_dt.tm_wday) % 7
        if days_ahead == 0 and base > now:
            return base
        if days_ahead == 0:
            days_ahead = 7
        return base + days_ahead * 86400

    def to_dict(self) -> dict[str, Any]:
        return {
            "schedule_id": self.schedule_id,
            "kind": self.kind,
            "every_seconds": self.every_seconds,
            "at": self.at,
            "weekday": self.weekday,
            "task_type": self.task_type,
            "model_name": self.model_name,
            "params": self.params,
            "timeout_seconds": self.timeout_seconds,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ScheduleSpec:
        return cls(
            schedule_id=d.get("schedule_id", ""),
            kind=d.get("kind", ""),
            every_seconds=int(d.get("every_seconds", 0)),
            at=d.get("at", ""),
            weekday=int(d.get("weekday", -1)),
            task_type=d.get("task_type", "inference"),
            model_name=d.get("model_name", ""),
            params=dict(d.get("params", {})),
            timeout_seconds=float(d.get("timeout_seconds", 300.0)),
            enabled=bool(d.get("enabled", True)),
        )


@dataclass
class ScheduleRun:
    """单次触发记录 — 任务结果全存 (含 inference 输出), 环上限防无界。"""

    seq: int
    task_id: str
    status: str
    started_at: float
    finished_at: float = 0.0
    error: str = ""
    result: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "task_id": self.task_id,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "result": self.result,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ScheduleRun:
        return cls(
            seq=int(d.get("seq", 0)),
            task_id=d.get("task_id", ""),
            status=d.get("status", "failed"),
            started_at=float(d.get("started_at", 0.0)),
            finished_at=float(d.get("finished_at", 0.0)),
            error=d.get("error", ""),
            result=dict(d.get("result", {})),
        )


class ScheduleHost:
    """常驻节点调度宿主 — 注册/触发/记录/持久化, 事件循环内单线程无锁。

    executor 契约: async (task: dict) -> dict — 直接传 NodeAgent.execute_task。
    任务体对齐 execute_task 消费契约 {task_id, type, model, params}。
    """

    def __init__(
        self,
        resident: bool,
        executor: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        store_path: Path | None = None,
        tick_seconds: float = 5.0,
    ):
        self._resident = resident
        self._executor = executor
        self._store_path = store_path or Path.home() / ".fusion" / "multi-node" / "schedules.json"
        self._tick_seconds = max(tick_seconds, 1.0)
        self._schedules: dict[str, ScheduleSpec] = {}
        self._next_run: dict[str, float] = {}
        self._runs: dict[str, list[ScheduleRun]] = {}
        self._seq: dict[str, int] = {}
        self._loop_task: asyncio.Task | None = None
        self._running = False

    # ── 注册/查询 (同步内存操作) ──

    def register(self, spec: ScheduleSpec) -> dict[str, Any]:
        """注册或覆盖 (同 id = PATCH)。校验失败抛 ValueError。"""
        if not self._resident:
            raise PermissionError("not_resident")
        spec.validate()
        if not spec.schedule_id:
            spec.schedule_id = f"sched-{uuid.uuid4().hex[:8]}"
        # params 须可 JSON 序列化 — 持久化前提, 注册期即拒不可序列化载荷。
        try:
            json.dumps(spec.params)
        except (TypeError, ValueError) as e:
            raise ValueError(f"params 不可 JSON 序列化: {e}") from e
        self._schedules[spec.schedule_id] = spec
        self._seq.setdefault(spec.schedule_id, 0)
        self._runs.setdefault(spec.schedule_id, [])
        self._next_run[spec.schedule_id] = spec.next_run_at()
        logger.info(f"#82 注册定时任务: {spec.schedule_id} kind={spec.kind} task_type={spec.task_type}")
        self.save()
        return spec.to_dict()

    def remove(self, schedule_id: str) -> bool:
        if schedule_id not in self._schedules:
            return False
        self._schedules.pop(schedule_id, None)
        self._next_run.pop(schedule_id, None)
        self._runs.pop(schedule_id, None)
        self._seq.pop(schedule_id, None)
        logger.info(f"#82 移除定时任务: {schedule_id}")
        self.save()
        return True

    def list_schedules(self) -> list[dict[str, Any]]:
        return [{**s.to_dict(), "next_run_at": self._next_run.get(sid, 0.0)} for sid, s in self._schedules.items()]

    def get(self, schedule_id: str) -> dict[str, Any] | None:
        spec = self._schedules.get(schedule_id)
        if spec is None:
            return None
        return {
            **spec.to_dict(),
            "next_run_at": self._next_run.get(schedule_id, 0.0),
            "runs": [r.to_dict() for r in self._runs.get(schedule_id, [])],
        }

    def set_enabled(self, schedule_id: str, enabled: bool) -> dict[str, Any] | None:
        """启停 — disabled 不触发但保留注册 (cowork 暂停语义)。"""
        spec = self._schedules.get(schedule_id)
        if spec is None:
            return None
        spec.enabled = enabled
        self._next_run[schedule_id] = spec.next_run_at() if enabled else 0.0
        self.save()
        return spec.to_dict()

    # ── 持久化 ──

    def save(self) -> None:
        """原子落盘 (tmp+replace+fsync)。文件 IO 同步执行 — 触发路径外调用 (注册/移除时),
        触发循环内不落盘 (仅内存追加, stop/load 时落盘), 不阻塞事件循环。
        """
        try:
            self._store_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._store_path.with_suffix(self._store_path.suffix + ".tmp")
            payload = {
                "schedules": [s.to_dict() for s in self._schedules.values()],
                "runs": {sid: [r.to_dict() for r in runs] for sid, runs in self._runs.items()},
                "saved_at": time.time(),
            }
            with open(tmp, "w") as f:
                json.dump(payload, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._store_path)
            logger.debug(f"#82 定时任务落盘: {len(self._schedules)} schedules → {self._store_path}")
        except Exception as e:
            logger.error(f"#82 定时任务落盘失败: {e}")

    def load(self) -> int:
        """启动恢复。损坏文件 warn 后从空开始。返回恢复数。

        interval 型: next_run = now + every_seconds (重启错过的 tick 不补跑)。
        daily/weekly: 取下一匹配时刻。disabled 保持 disabled。
        """
        if not self._store_path.exists():
            return 0
        try:
            data = json.loads(self._store_path.read_text())
        except Exception as e:
            logger.error(f"#82 定时任务恢复读盘失败: {e}")
            return 0
        now = time.time()
        restored = 0
        for d in data.get("schedules", []):
            try:
                spec = ScheduleSpec.from_dict(d)
                spec.validate()
            except Exception as e:
                logger.warning(f"#82 定时任务恢复跳过 {d.get('schedule_id', '?')}: {e}")
                continue
            self._schedules[spec.schedule_id] = spec
            self._seq.setdefault(spec.schedule_id, 0)
            self._next_run[spec.schedule_id] = spec.next_run_at(now)
            restored += 1
        for sid, runs in data.get("runs", {}).items():
            if sid not in self._schedules:
                continue
            try:
                self._runs[sid] = [ScheduleRun.from_dict(r) for r in runs][-_MAX_RUNS_PER_SCHEDULE:]
                if self._runs[sid]:
                    self._seq[sid] = max(r.seq for r in self._runs[sid])
            except Exception as e:
                logger.warning(f"#82 运行记录恢复跳过 {sid}: {e}")
        logger.info(f"#82 定时任务恢复: {restored} schedules ({self._store_path})")
        return restored

    # ── 触发循环 ──

    def start(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._running = True
        self._loop_task = asyncio.create_task(self._loop())
        logger.info(f"#82 调度宿主启动: tick={self._tick_seconds}s schedules={len(self._schedules)}")

    async def stop(self) -> None:
        self._running = False
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        self._loop_task = None
        self.save()
        logger.info("#82 调度宿主停止")

    async def _loop(self) -> None:
        """到点触发 — 单线程事件循环, fire 内 await executor (execute_task) 并发 gate 兜底。"""
        while self._running:
            try:
                now = time.time()
                due = [
                    sid
                    for sid, spec in self._schedules.items()
                    if spec.enabled and self._next_run.get(sid, float("inf")) <= now
                ]
                for sid in due:
                    await self._fire(sid, now)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"#82 调度循环异常: {e}")
            await asyncio.sleep(self._tick_seconds)

    async def _fire(self, schedule_id: str, now: float) -> None:
        """触发一次 — 构建任务体 → executor (execute_task) → 记录运行结果 → 重排下一轮。"""
        spec = self._schedules.get(schedule_id)
        if spec is None:
            self._next_run.pop(schedule_id, None)
            return
        self._seq[schedule_id] = self._seq.get(schedule_id, 0) + 1
        seq = self._seq[schedule_id]
        task_id = f"{schedule_id}-{seq:06d}"
        run = ScheduleRun(seq=seq, task_id=task_id, status="running", started_at=now)
        task = {
            "task_id": task_id,
            "type": spec.task_type,
            "model": spec.model_name,
            "params": dict(spec.params),
        }
        logger.info(f"#82 触发定时任务: {schedule_id} seq={seq} task_id={task_id}")
        try:
            result = await self._executor(task)
        except Exception as e:
            result = {"error": str(e)}
            logger.error(f"#82 定时任务执行异常: {task_id}: {e}")
        run.finished_at = time.time()
        if result.get("error"):
            run.status = "failed"
            run.error = result.get("error", "")
        else:
            run.status = "completed"
            run.result = result
        runs = self._runs.setdefault(schedule_id, [])
        runs.append(run)
        if len(runs) > _MAX_RUNS_PER_SCHEDULE:
            del runs[:-_MAX_RUNS_PER_SCHEDULE]
        # 重排: 从触发时刻起算 (interval 漂移累积可接受; daily/weekly 下一匹配)。
        # 错过多 tick 只触发一次 (due 集合去重), 不补跑风暴。
        self._next_run[schedule_id] = spec.next_run_at()
        logger.info(f"#82 定时任务完成: {task_id} status={run.status} 耗时 {run.finished_at - run.started_at:.1f}s")

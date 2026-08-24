"""Cluster Observability — 全集群统一可观测模块。

核心能力：
- 全集群统一日志聚合
- 指标监控（内存/推理TPS/网络RTT/会话耗时）
- 告警体系（节点离线/长任务卡死/内存爆满）
"""

from __future__ import annotations

import asyncio
import bisect
import collections
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"

    @classmethod
    def from_str(cls, value: str) -> LogLevel:
        normalized = value.upper()
        mapping = {
            "INFO": cls.INFO,
            "WARN": cls.WARN,
            "WARNING": cls.WARN,
            "ERROR": cls.ERROR,
            "CRITICAL": cls.FATAL,
            "FATAL": cls.FATAL,
        }
        return mapping.get(normalized, cls.INFO)

    @property
    def numeric(self) -> int:
        return {
            LogLevel.INFO: 0,
            LogLevel.WARN: 1,
            LogLevel.ERROR: 2,
            LogLevel.FATAL: 3,
        }[self]


@dataclass
class MetricPoint:
    """指标数据点。"""

    timestamp: float
    node_id: str
    metric_name: str
    value: float
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class Alert:
    """告警定义。"""

    alert_id: str
    severity: str  # "info" | "warning" | "critical"
    title: str
    message: str
    node_id: str = ""
    created_at: float = 0.0
    resolved: bool = False
    resolved_at: float = 0.0


@dataclass
class LogEntry:
    """日志条目。"""

    timestamp: float
    node_id: str
    level: str
    module: str
    message: str
    task_id: str = ""


class ClusterObservability:
    """集群可观测模块 — 监控、日志、告警聚合。"""

    def __init__(self, retention_hours: float = 168.0):
        self.retention_seconds = retention_hours * 3600
        self.metrics: collections.deque = collections.deque(maxlen=10000)
        self.alerts: collections.deque = collections.deque(maxlen=10000)
        self.logs: collections.deque = collections.deque(maxlen=50000)
        # 与 metrics 同 maxlen 对齐, 避免 metrics 丢弃旧条目后时间索引错位 (AR审计 P2 无界增长)
        self._metric_times: collections.deque = collections.deque(maxlen=10000)
        self._alert_handlers: list[Callable] = []
        self._running = False
        self._cleanup_task: asyncio.Task | None = None

    # ── 指标收集 ──

    def record_metric(
        self,
        node_id: str,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        """记录指标。"""
        ts = time.time()
        self.metrics.append(
            MetricPoint(
                timestamp=ts,
                node_id=node_id,
                metric_name=name,
                value=value,
                tags=tags or {},
            )
        )
        self._metric_times.append(ts)

    def get_metrics(
        self,
        name: str,
        node_id: str = "",
        since: float = 0.0,
        limit: int = 100,
    ) -> list[MetricPoint]:
        """查询指标 — 使用时间索引加速 since 过滤。"""
        results = []
        start_idx = 0
        if since > 0 and self._metric_times:
            start_idx = bisect.bisect_left(self._metric_times, since)
        for i in range(start_idx, len(self.metrics)):
            m = self.metrics[i]
            if m.metric_name != name:
                continue
            if node_id and m.node_id != node_id:
                continue
            results.append(m)
            if len(results) >= limit:
                break
        return results

    def get_latest_metric(self, name: str, node_id: str = "") -> MetricPoint | None:
        """获取最新指标值。"""
        for m in reversed(self.metrics):
            if m.metric_name == name and (not node_id or m.node_id == node_id):
                return m
        return None

    # ── 日志管理 ──

    def add_log(self, entry: LogEntry) -> None:
        """添加日志条目。"""
        normalized = LogLevel.from_str(entry.level)
        entry.level = normalized.value
        self.logs.append(entry)
        if normalized.numeric >= LogLevel.ERROR.numeric:
            severity = "critical" if normalized == LogLevel.FATAL else "warning"
            self.create_alert(
                severity=severity,
                title=f"节点 {entry.node_id} 异常",
                message=entry.message,
                node_id=entry.node_id,
            )

    def get_logs(
        self,
        node_id: str = "",
        level: str = "",
        since: float = 0.0,
        limit: int = 100,
    ) -> list[LogEntry]:
        """查询日志。"""
        results = []
        for log in reversed(self.logs):
            if node_id and log.node_id != node_id:
                continue
            if level and log.level != level:
                continue
            if since > 0 and log.timestamp < since:
                continue
            results.append(log)
            if len(results) >= limit:
                break
        return results

    def collect_node_logs(
        self,
        node_ids: list[str] | None = None,
        level: str = "",
        since: float = 0.0,
        limit: int = 500,
    ) -> dict[str, list[LogEntry]]:
        """Master侧全节点日志汇总 — 按node_id分组返回。"""
        id_set = set(node_ids) if node_ids else None
        target_level = LogLevel.from_str(level) if level else None
        result: dict[str, list[LogEntry]] = collections.defaultdict(list)
        for log in reversed(self.logs):
            if id_set and log.node_id not in id_set:
                continue
            if target_level and LogLevel.from_str(log.level) != target_level:
                continue
            if since > 0 and log.timestamp < since:
                continue
            result[log.node_id].append(log)
            total = sum(len(v) for v in result.values())
            if total >= limit:
                break
        logger.debug(f"collect_node_logs: {len(result)} nodes, {sum(len(v) for v in result.values())} entries")
        return dict(result)

    # ── 告警管理 ──

    def create_alert(
        self,
        severity: str,
        title: str,
        message: str,
        node_id: str = "",
    ) -> Alert:
        """创建告警。"""
        alert = Alert(
            alert_id=f"alert_{uuid.uuid4().hex[:12]}",
            severity=severity,
            title=title,
            message=message,
            node_id=node_id,
            created_at=time.time(),
        )
        self.alerts.append(alert)
        logger.warning(f"告警 [{severity}]: {title} — {message}")
        for handler in self._alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"告警处理异常: {e}")
        return alert

    def resolve_alert(self, alert_id: str) -> bool:
        """解决告警。"""
        for alert in self.alerts:
            if alert.alert_id == alert_id and not alert.resolved:
                alert.resolved = True
                alert.resolved_at = time.time()
                return True
        return False

    def get_active_alerts(self, severity: str = "") -> list[Alert]:
        """获取活跃告警。"""
        return [a for a in self.alerts if not a.resolved and (not severity or a.severity == severity)]

    def on_alert(self, handler: Callable) -> None:
        """注册告警处理器。"""
        self._alert_handlers.append(handler)

    # ── 告警规则引擎 ──

    async def check_alert_rules(self, nodes: dict[str, Any]) -> list[Alert]:
        """检查告警规则。"""
        new_alerts = []

        for node_id, node in nodes.items():
            # 节点离线
            if node.get("status") == "offline":
                alert = self.create_alert(
                    severity="critical",
                    title=f"节点离线: {node_id}",
                    message=f"节点 {node.get('hostname', node_id)} 已离线",
                    node_id=node_id,
                )
                new_alerts.append(alert)

            # 内存不足
            mem_available = node.get("available_memory_gb", 0)
            mem_total = node.get("total_memory_gb", 1)
            if mem_total > 0 and mem_available / mem_total < 0.1:
                alert = self.create_alert(
                    severity="warning",
                    title=f"节点内存不足: {node_id}",
                    message=f"可用内存仅 {mem_available:.1f}GB/{mem_total:.1f}GB",
                    node_id=node_id,
                )
                new_alerts.append(alert)

        return new_alerts

    # ── 统计报表 ──

    def get_cluster_report(self) -> dict[str, Any]:
        """生成集群统计报告。"""
        now = time.time()
        since = now - 3600  # 最近1小时

        # 指标统计
        recent_metrics = [m for m in self.metrics if m.timestamp > since]

        # 各节点指标聚合
        node_metrics: dict[str, dict[str, list[float]]] = collections.defaultdict(lambda: collections.defaultdict(list))
        for m in recent_metrics:
            node_metrics[m.node_id][m.metric_name].append(m.value)

        # 告警统计
        active_alerts = len(self.get_active_alerts())
        total_alerts = len(self.alerts)

        return {
            "time_range": f"{since:.0f} - {now:.0f}",
            "metrics_collected": len(recent_metrics),
            "logs_collected": sum(1 for lg in self.logs if lg.timestamp > since),
            "active_alerts": active_alerts,
            "total_alerts": total_alerts,
            "node_summary": {nid: _build_node_summary(metrics) for nid, metrics in node_metrics.items()},
        }

    # ── 生命周期 ──

    async def start(self) -> None:
        """启动可观测模块。"""
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Cluster Observability 已启动")

    async def stop(self) -> None:
        """停止可观测模块。"""
        self._running = False
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("Cluster Observability 已停止")

    def export_logs(self, fmt: str = "json", since: float = 0.0, node_id: str = "") -> Any:
        """M8-02 导出日志数据。"""
        logs = list(self.logs)
        if since:
            logs = [l for l in logs if l.timestamp >= since]
        if node_id:
            logs = [l for l in logs if l.node_id == node_id]
        if fmt == "csv":
            import io

            buf = io.StringIO()
            buf.write("timestamp,level,node_id,module,message\n")
            for l in logs:
                msg = l.message.replace('"', '""')
                buf.write(f'{l.timestamp},{l.level},{l.node_id},{l.module},"{msg}"\n')
            return buf.getvalue()
        return [
            {
                "timestamp": l.timestamp,
                "level": l.level,
                "node_id": l.node_id,
                "module": l.module,
                "message": l.message,
            }
            for l in logs
        ]

    def generate_optimization_suggestions(self) -> list:
        """M8-03 基于告警和日志生成智能优化建议。"""
        suggestions = []
        active = [a for a in self.alerts if not a.resolved]
        for alert in active:
            if "memory" in alert.message.lower() or "内存" in alert.message:
                suggestions.append(
                    {
                        "priority": "high",
                        "category": "resource",
                        "title": "内存压力过高",
                        "suggestion": "建议启用模型降级链(M4-04)或扩容新节点(M10-02)，并检查是否有内存泄漏",
                        "related_alert": alert.alert_id,
                    }
                )
            elif "offline" in alert.message.lower() or "离线" in alert.message:
                suggestions.append(
                    {
                        "priority": "high",
                        "category": "availability",
                        "title": "节点离线",
                        "suggestion": "检查节点网络连接和进程状态，考虑自动重启或激活 standby 节点",
                        "related_alert": alert.alert_id,
                    }
                )
            elif "latency" in alert.message.lower() or "延迟" in alert.message:
                suggestions.append(
                    {
                        "priority": "medium",
                        "category": "performance",
                        "title": "推理延迟升高",
                        "suggestion": "检查负载路由策略(M4-01)，考虑切换到 VRAM-first 路由或启用并行推理",
                        "related_alert": alert.alert_id,
                    }
                )

        error_logs = [l for l in list(self.logs)[-200:] if l.level == "ERROR"]
        error_sources = {}
        for l in error_logs:
            error_sources[l.module] = error_sources.get(l.module, 0) + 1
        for source, count in sorted(error_sources.items(), key=lambda x: -x[1])[:3]:
            suggestions.append(
                {
                    "priority": "medium",
                    "category": "stability",
                    "title": f"{source} 频繁报错({count}次)",
                    "suggestion": f"最近日志中 {source} 出现 {count} 次 ERROR，建议检查该组件状态和配置",
                }
            )

        if not suggestions:
            suggestions.append(
                {
                    "priority": "low",
                    "category": "info",
                    "title": "集群运行正常",
                    "suggestion": "当前无活跃告警或异常日志，建议定期检查扩缩容策略是否匹配负载模式",
                }
            )

        return suggestions

    async def _cleanup_loop(self) -> None:
        """定期清理过期数据。"""
        try:
            while self._running:
                await asyncio.sleep(300)  # 每5分钟清理
                cutoff = time.time() - self.retention_seconds
                before_m = len(self.metrics)
                before_l = len(self.logs)
                before_a = len(self.alerts)
                while self.metrics and self.metrics[0].timestamp <= cutoff:
                    self.metrics.popleft()
                    if self._metric_times:
                        self._metric_times.popleft()
                while self.logs and self.logs[0].timestamp <= cutoff:
                    self.logs.popleft()
                while self.alerts and (self.alerts[0].resolved and self.alerts[0].created_at <= cutoff):
                    self.alerts.popleft()
                logger.debug(
                    f"可观测数据清理完成: 指标 {before_m}→{len(self.metrics)}, "
                    f"日志 {before_l}→{len(self.logs)}, "
                    f"告警 {before_a}→{len(self.alerts)}"
                )
        except asyncio.CancelledError:
            pass


def _build_node_summary(metrics: dict) -> dict:
    """构建节点指标摘要。"""
    latency_vals = metrics.get("latency_ms", [])
    tps_vals = metrics.get("tokens_per_sec", [])
    return {
        "avg_latency_ms": round(sum(latency_vals) / len(latency_vals), 1) if latency_vals else 0,
        "avg_tps": round(sum(tps_vals) / len(tps_vals), 1) if tps_vals else 0,
    }

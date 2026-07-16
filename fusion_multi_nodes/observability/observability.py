"""Cluster Observability — 全集群统一可观测模块。

核心能力：
- 全集群统一日志聚合
- 指标监控（内存/推理TPS/网络RTT/会话耗时）
- 告警体系（节点离线/长任务卡死/内存爆满）
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """指标数据点。"""
    timestamp: float
    node_id: str
    metric_name: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)


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

    def __init__(self, retention_hours: float = 24.0):
        self.retention_seconds = retention_hours * 3600
        self.metrics: List[MetricPoint] = []
        self.alerts: List[Alert] = []
        self.logs: List[LogEntry] = []
        self._alert_handlers: List[Callable] = []
        self._running = False
        self._max_metrics = 10000
        self._max_logs = 50000

    # ── 指标收集 ──

    def record_metric(
        self,
        node_id: str,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """记录指标。"""
        self.metrics.append(MetricPoint(
            timestamp=time.time(),
            node_id=node_id,
            metric_name=name,
            value=value,
            tags=tags or {},
        ))
        if len(self.metrics) > self._max_metrics:
            self.metrics = self.metrics[-self._max_metrics:]

    def get_metrics(
        self,
        name: str,
        node_id: str = "",
        since: float = 0.0,
        limit: int = 100,
    ) -> List[MetricPoint]:
        """查询指标。"""
        results = []
        for m in self.metrics:
            if m.metric_name == name:
                if node_id and m.node_id != node_id:
                    continue
                if since > 0 and m.timestamp < since:
                    continue
                results.append(m)
                if len(results) >= limit:
                    break
        return results

    def get_latest_metric(self, name: str, node_id: str = "") -> Optional[MetricPoint]:
        """获取最新指标值。"""
        for m in reversed(self.metrics):
            if m.metric_name == name:
                if not node_id or m.node_id == node_id:
                    return m
        return None

    # ── 日志管理 ──

    def add_log(self, entry: LogEntry) -> None:
        """添加日志条目。"""
        self.logs.append(entry)
        if len(self.logs) > self._max_logs:
            self.logs = self.logs[-self._max_logs:]
        # 日志级别告警
        if entry.level in ("ERROR", "CRITICAL"):
            self.create_alert(
                severity="warning" if entry.level == "ERROR" else "critical",
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
    ) -> List[LogEntry]:
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
            alert_id=f"alert_{len(self.alerts)}",
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

    def get_active_alerts(self, severity: str = "") -> List[Alert]:
        """获取活跃告警。"""
        return [
            a for a in self.alerts
            if not a.resolved and (not severity or a.severity == severity)
        ]

    def on_alert(self, handler: Callable) -> None:
        """注册告警处理器。"""
        self._alert_handlers.append(handler)

    # ── 告警规则引擎 ──

    async def check_alert_rules(self, nodes: Dict[str, Any]) -> List[Alert]:
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

    def get_cluster_report(self) -> Dict[str, Any]:
        """生成集群统计报告。"""
        now = time.time()
        since = now - 3600  # 最近1小时

        # 指标统计
        recent_metrics = [m for m in self.metrics if m.timestamp > since]

        # 各节点指标聚合
        node_metrics = defaultdict(lambda: defaultdict(list))
        for m in recent_metrics:
            node_metrics[m.node_id][m.metric_name].append(m.value)

        # 告警统计
        active_alerts = len(self.get_active_alerts())
        total_alerts = len(self.alerts)

        return {
            "time_range": f"{since:.0f} - {now:.0f}",
            "metrics_collected": len(recent_metrics),
            "logs_collected": sum(1 for l in self.logs if l.timestamp > since),
            "active_alerts": active_alerts,
            "total_alerts": total_alerts,
            "node_summary": {
                nid: _build_node_summary(metrics)
                for nid, metrics in node_metrics.items()
            },
        }

    # ── 生命周期 ──

    async def start(self) -> None:
        """启动可观测模块。"""
        self._running = True
        asyncio.create_task(self._cleanup_loop())
        logger.info("Cluster Observability 已启动")

    async def stop(self) -> None:
        """停止可观测模块。"""
        self._running = False
        logger.info("Cluster Observability 已停止")

    async def _cleanup_loop(self) -> None:
        """定期清理过期数据。"""
        while self._running:
            await asyncio.sleep(300)  # 每5分钟清理
            cutoff = time.time() - self.retention_seconds
            self.metrics = [m for m in self.metrics if m.timestamp > cutoff]
            self.logs = [l for l in self.logs if l.timestamp > cutoff]
            logger.debug(f"可观测数据清理完成: {len(self.metrics)} 指标, {len(self.logs)} 日志")


def _build_node_summary(metrics: dict) -> dict:
    """构建节点指标摘要。"""
    latency_vals = metrics.get("latency_ms", [])
    tps_vals = metrics.get("tokens_per_sec", [])
    return {
        "avg_latency_ms": round(sum(latency_vals) / len(latency_vals), 1) if latency_vals else 0,
        "avg_tps": round(sum(tps_vals) / len(tps_vals), 1) if tps_vals else 0,
    }
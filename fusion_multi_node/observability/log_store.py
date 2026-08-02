"""M8 日志存储与智能诊断 — 集中日志存储、导出、异常模式识别。

- 日志集中存储（内存+磁盘）
- 多格式导出（JSON/CSV/文本）
- 智能故障诊断（异常模式检测、根因分析）
- 日志清理与轮转
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class StoredLog:
    """存储的日志条目。"""

    timestamp: float
    level: str
    source: str
    message: str
    node_id: str = ""
    task_id: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def log_level(self):
        from fusion_multi_node.observability.observability import LogLevel

        return LogLevel.from_str(self.level)


@dataclass
class DiagnosisResult:
    """诊断结果。"""

    pattern: str
    severity: str
    description: str
    affected_nodes: list[str] = field(default_factory=list)
    affected_tasks: list[str] = field(default_factory=list)
    suggestion: str = ""
    confidence: float = 0.0


# 已知异常模式
FAULT_PATTERNS = [
    {
        "name": "node_heartbeat_timeout",
        "pattern": r"节点心跳超时|heartbeat.*timeout|node.*offline",
        "severity": "warning",
        "description": "节点心跳超时，可能已离线",
        "suggestion": "检查节点网络连接，确认服务是否运行",
    },
    {
        "name": "task_execution_failure",
        "pattern": r"任务执行失败|task.*failed|execution.*error|OOM|out of memory",
        "severity": "error",
        "description": "任务执行失败，可能是资源不足",
        "suggestion": "检查节点可用内存，考虑模型降级或增加节点",
    },
    {
        "name": "kv_cache_miss",
        "pattern": r"KV.*缓存.*未找到|cache.*miss|no.*kv.*cache",
        "severity": "info",
        "description": "KV缓存未命中，可能影响推理延迟",
        "suggestion": "预热常用模型的KV缓存",
    },
    {
        "name": "network_partition",
        "pattern": r"连接.*失败|connection.*refused|network.*error|timeout.*connect",
        "severity": "critical",
        "description": "网络分区或连接故障",
        "suggestion": "检查网络连通性，确认防火墙设置",
    },
    {
        "name": "disk_full",
        "pattern": r"磁盘.*满|disk.*full|no.*space|ENOSPC",
        "severity": "critical",
        "description": "磁盘空间不足",
        "suggestion": "清理旧日志和检查点，扩展存储空间",
    },
    {
        "name": "model_load_failure",
        "pattern": r"模型.*加载.*失败|model.*load.*error|shard.*corrupt",
        "severity": "error",
        "description": "模型加载失败，分片可能损坏",
        "suggestion": "重新下载模型分片，验证checksum",
    },
]


class LogStore:
    """日志存储管理器。"""

    def __init__(
        self,
        store_dir: str = "",
        max_memory_entries: int = 10000,
        persist_to_disk: bool = True,
        retention_seconds: float = 604800.0,
    ):
        self._store_dir = store_dir or str(Path.home() / ".fusion" / "logs")
        self._max_memory = max_memory_entries
        self._persist = persist_to_disk
        self._retention = retention_seconds
        self._entries: list[StoredLog] = []
        self._by_node: dict[str, list[StoredLog]] = defaultdict(list)
        self._by_task: dict[str, list[StoredLog]] = defaultdict(list)
        self._by_level: dict[str, list[StoredLog]] = defaultdict(list)

    def store(self, log: StoredLog) -> None:
        self._entries.append(log)
        if log.node_id:
            self._by_node[log.node_id].append(log)
        if log.task_id:
            self._by_task[log.task_id].append(log)
        self._by_level[log.level].append(log)

        if self._persist:
            self._persist_log(log)

        if len(self._entries) > self._max_memory:
            self._cleanup_memory()

    def store_batch(self, logs: list[StoredLog]) -> None:
        for log in logs:
            self.store(log)

    def query(
        self,
        node_id: str = "",
        task_id: str = "",
        level: str | None = None,
        source: str = "",
        start_time: float = 0.0,
        end_time: float = 0.0,
        keyword: str = "",
        limit: int = 100,
    ) -> list[StoredLog]:
        from fusion_multi_node.observability.observability import LogLevel as _LogLevel

        results = self._entries

        if node_id:
            results = [e for e in results if e.node_id == node_id]
        if task_id:
            results = [e for e in results if e.task_id == task_id]
        if level:
            level_str = level.value if isinstance(level, _LogLevel) else level
            results = [e for e in results if e.level == level_str]
        if source:
            results = [e for e in results if e.source == source]
        if start_time:
            results = [e for e in results if e.timestamp >= start_time]
        if end_time:
            results = [e for e in results if e.timestamp <= end_time]
        if keyword:
            kw_lower = keyword.lower()
            results = [e for e in results if kw_lower in e.message.lower()]

        return results[-limit:]

    def export_json(self, logs: list[StoredLog] | None = None) -> str:
        entries = logs or self._entries
        data = [
            {
                "timestamp": e.timestamp,
                "level": e.level,
                "source": e.source,
                "message": e.message,
                "node_id": e.node_id,
                "task_id": e.task_id,
                "extra": e.extra,
            }
            for e in entries
        ]
        return json.dumps(data, ensure_ascii=False, indent=2)

    def export_csv(self, logs: list[StoredLog] | None = None) -> str:
        entries = logs or self._entries
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["timestamp", "level", "source", "node_id", "task_id", "message"])
        for e in entries:
            writer.writerow([e.timestamp, e.level, e.source, e.node_id, e.task_id, e.message])
        return output.getvalue()

    def export_text(self, logs: list[StoredLog] | None = None) -> str:
        entries = logs or self._entries
        lines = []
        for e in entries:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e.timestamp))
            line = f"[{ts}] [{e.level.upper():7s}] [{e.source}] {e.message}"
            if e.node_id:
                line += f" (node={e.node_id})"
            if e.task_id:
                line += f" (task={e.task_id})"
            lines.append(line)
        return "\n".join(lines)

    def _persist_log(self, log: StoredLog) -> None:
        try:
            log_dir = Path(self._store_dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            date_str = time.strftime("%Y%m%d", time.localtime(log.timestamp))
            log_file = log_dir / f"fusion-{date_str}.jsonl"
            data = {
                "ts": log.timestamp,
                "lv": log.level,
                "src": log.source,
                "msg": log.message,
                "nid": log.node_id,
                "tid": log.task_id,
            }
            with open(log_file, "a") as f:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.debug(f"日志持久化失败: {e}")

    def _cleanup_memory(self) -> None:
        cutoff = time.time() - self._retention
        self._entries = [e for e in self._entries if e.timestamp > cutoff]
        for key in list(self._by_node.keys()):
            self._by_node[key] = [e for e in self._by_node[key] if e.timestamp > cutoff]
        for key in list(self._by_task.keys()):
            self._by_task[key] = [e for e in self._by_task[key] if e.timestamp > cutoff]
        for key in list(self._by_level.keys()):
            self._by_level[key] = [e for e in self._by_level[key] if e.timestamp > cutoff]

    def get_stats(self) -> dict[str, Any]:
        level_counts = Counter(e.level for e in self._entries)
        return {
            "total_entries": len(self._entries),
            "by_level": dict(level_counts),
            "by_node": {k: len(v) for k, v in self._by_node.items()},
            "oldest": self._entries[0].timestamp if self._entries else 0,
            "newest": self._entries[-1].timestamp if self._entries else 0,
        }


class FaultDiagnoser:
    """智能故障诊断器。

    基于日志模式匹配和统计分析，识别异常模式并提供诊断建议。
    """

    def __init__(self, custom_patterns: list[dict[str, str]] | None = None):
        self._patterns = list(FAULT_PATTERNS)
        if custom_patterns:
            self._patterns.extend(custom_patterns)
        self._compiled = [(p, re.compile(p["pattern"], re.IGNORECASE)) for p in self._patterns]
        self._diagnosis_history: list[DiagnosisResult] = []

    def diagnose(self, logs: list[StoredLog], time_window: float = 300.0) -> list[DiagnosisResult]:
        now = time.time()
        recent = [e for e in logs if now - e.timestamp <= time_window]

        if not recent:
            return []

        results: list[DiagnosisResult] = []
        for pattern_def, regex in self._compiled:
            matches = [e for e in recent if regex.search(e.message)]
            if not matches:
                continue

            affected_nodes = list({e.node_id for e in matches if e.node_id})
            affected_tasks = list({e.task_id for e in matches if e.task_id})

            confidence = min(len(matches) / 5.0, 1.0)

            result = DiagnosisResult(
                pattern=pattern_def["name"],
                severity=pattern_def["severity"],
                description=pattern_def["description"],
                affected_nodes=affected_nodes,
                affected_tasks=affected_tasks,
                suggestion=pattern_def["suggestion"],
                confidence=confidence,
            )
            results.append(result)

        results.sort(key=lambda r: {"critical": 0, "error": 1, "warning": 2, "info": 3}.get(r.severity, 4))

        self._diagnosis_history.extend(results)
        if len(self._diagnosis_history) > 200:
            self._diagnosis_history = self._diagnosis_history[-200:]

        return results

    def analyze_frequency(self, logs: list[StoredLog], group_by: str = "source") -> dict[str, int]:
        if group_by == "source":
            counter = Counter(e.source for e in logs)
        elif group_by == "node_id":
            counter = Counter(e.node_id for e in logs if e.node_id)
        elif group_by == "level":
            counter = Counter(e.level for e in logs)
        else:
            counter = Counter(e.source for e in logs)
        return dict(counter.most_common(20))

    def get_history(self, limit: int = 20) -> list[DiagnosisResult]:
        return self._diagnosis_history[-limit:]

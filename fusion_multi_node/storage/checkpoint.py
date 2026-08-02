"""M9 任务检查点 — 推理任务的状态持久化与恢复。"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CheckpointEntry:
    """检查点条目。"""

    checkpoint_id: str
    task_id: str
    node_id: str
    step: int
    state_data: dict[str, Any] = field(default_factory=dict)
    created_at: float = 0.0
    size_bytes: int = 0
    metadata: dict[str, str] = field(default_factory=dict)


class CheckpointManager:
    """检查点管理器。

    - 保存/加载任务执行状态
    - 自动清理过期检查点
    - 支持按 task_id 查询最新检查点
    """

    def __init__(
        self,
        checkpoint_dir: str = "",
        max_checkpoints: int = 100,
        ttl_seconds: float = 86400.0,
    ):
        self._dir = checkpoint_dir or str(Path.home() / ".fusion" / "checkpoints")
        self._max_checkpoints = max_checkpoints
        self._ttl = ttl_seconds
        self._entries: dict[str, CheckpointEntry] = {}

    def save(self, entry: CheckpointEntry) -> bool:
        entry.created_at = entry.created_at or time.time()
        try:
            cp_path = Path(self._dir) / entry.task_id
            cp_path.mkdir(parents=True, exist_ok=True)
            file_path = cp_path / f"{entry.checkpoint_id}.json"
            data = {
                "checkpoint_id": entry.checkpoint_id,
                "task_id": entry.task_id,
                "node_id": entry.node_id,
                "step": entry.step,
                "state_data": entry.state_data,
                "created_at": entry.created_at,
                "metadata": entry.metadata,
            }
            content = json.dumps(data, ensure_ascii=False)
            file_path.write_text(content)
            entry.size_bytes = len(content.encode("utf-8"))
            self._entries[entry.checkpoint_id] = entry
            logger.info(f"检查点保存: {entry.checkpoint_id} (task={entry.task_id}, step={entry.step})")
            self._cleanup()
            return True
        except Exception as e:
            logger.error(f"检查点保存失败: {entry.checkpoint_id}: {e}")
            return False

    def load(self, checkpoint_id: str) -> CheckpointEntry | None:
        entry = self._entries.get(checkpoint_id)
        if entry:
            return entry
        # 尝试从磁盘加载
        for task_dir in Path(self._dir).iterdir():
            if not task_dir.is_dir():
                continue
            cp_file = task_dir / f"{checkpoint_id}.json"
            if cp_file.exists():
                return self._load_from_file(cp_file)
        return None

    def load_latest(self, task_id: str) -> CheckpointEntry | None:
        task_dir = Path(self._dir) / task_id
        if not task_dir.exists():
            # 从内存查
            task_entries = [e for e in self._entries.values() if e.task_id == task_id]
            if task_entries:
                return max(task_entries, key=lambda e: e.step)
            return None

        checkpoints = []
        for cp_file in task_dir.glob("*.json"):
            entry = self._load_from_file(cp_file)
            if entry:
                checkpoints.append(entry)

        if not checkpoints:
            return None
        return max(checkpoints, key=lambda e: e.step)

    def delete(self, checkpoint_id: str) -> bool:
        entry = self._entries.pop(checkpoint_id, None)
        if entry:
            cp_file = Path(self._dir) / entry.task_id / f"{checkpoint_id}.json"
            try:
                if cp_file.exists():
                    cp_file.unlink()
                return True
            except Exception as e:
                logger.error(f"检查点删除失败: {checkpoint_id}: {e}")
        return False

    def list_by_task(self, task_id: str) -> list[CheckpointEntry]:
        results = [e for e in self._entries.values() if e.task_id == task_id]
        results.sort(key=lambda e: e.step)
        return results

    def _load_from_file(self, path: Path) -> CheckpointEntry | None:
        try:
            data = json.loads(path.read_text())
            entry = CheckpointEntry(
                checkpoint_id=data["checkpoint_id"],
                task_id=data["task_id"],
                node_id=data["node_id"],
                step=data["step"],
                state_data=data.get("state_data", {}),
                created_at=data.get("created_at", 0.0),
                metadata=data.get("metadata", {}),
            )
            self._entries[entry.checkpoint_id] = entry
            return entry
        except Exception as e:
            logger.error(f"检查点加载失败: {path}: {e}")
            return None

    def _cleanup(self) -> None:
        if len(self._entries) <= self._max_checkpoints:
            return
        sorted_entries = sorted(self._entries.values(), key=lambda e: e.created_at)
        remove_count = len(self._entries) - self._max_checkpoints
        for entry in sorted_entries[:remove_count]:
            self.delete(entry.checkpoint_id)
        logger.info(f"清理过期检查点: {remove_count} 个")

    def get_stats(self) -> dict[str, Any]:
        total_size = sum(e.size_bytes for e in self._entries.values())
        return {
            "total_checkpoints": len(self._entries),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "unique_tasks": len({e.task_id for e in self._entries.values()}),
        }

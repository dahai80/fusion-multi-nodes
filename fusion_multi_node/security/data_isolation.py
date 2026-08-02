"""M6-01 Master 数据隔离 — 阻止 Master 专有数据同步到 Worker 节点。

受保护路径/模式:
- *.db, *.sqlite (SQLite 数据库)
- soul.md, memory.md (Agent 持久记忆)
- .fusion/master/ (Master 专有目录)
"""

from __future__ import annotations

import fnmatch
import logging
import os
from dataclasses import dataclass, field

from .permission import NodeRole

logger = logging.getLogger(__name__)


@dataclass
class DataIsolationPolicy:
    master_only_patterns: list[str] = field(
        default_factory=lambda: [
            "*.db",
            "*.sqlite",
            "soul.md",
            "memory.md",
            ".fusion/master/*",
        ]
    )

    master_only_paths: list[str] = field(
        default_factory=lambda: [
            ".fusion/master",
        ]
    )

    def is_master_only(self, path: str) -> bool:
        norm = os.path.normpath(path)
        basename = os.path.basename(norm)

        for pattern in self.master_only_patterns:
            if fnmatch.fnmatch(basename, pattern):
                logger.debug(f"数据隔离: {path} 匹配 Master 专有模式 {pattern}")
                return True
            if fnmatch.fnmatch(norm, pattern):
                logger.debug(f"数据隔离: {path} 匹配 Master 专有模式 {pattern}")
                return True

        for master_path in self.master_only_paths:
            norm_master = os.path.normpath(master_path)
            if norm == norm_master or norm.startswith(norm_master + os.sep):
                logger.debug(f"数据隔离: {path} 位于 Master 专有路径 {master_path}")
                return True

        return False

    def is_transfer_allowed(self, source_path: str, target_role: str) -> bool:
        role_value = target_role.value if isinstance(target_role, NodeRole) else str(target_role).lower()

        if role_value != "worker":
            return True

        if self.is_master_only(source_path):
            logger.warning(f"数据隔离拦截: Master 专有数据 {source_path} 禁止传输到 Worker 节点")
            return False

        return True

    def filter_transferable_paths(
        self,
        paths: list[str],
        target_role: str,
    ) -> list[str]:
        allowed = []
        blocked = []
        for p in paths:
            if self.is_transfer_allowed(p, target_role):
                allowed.append(p)
            else:
                blocked.append(p)
        if blocked:
            logger.info(f"数据隔离: 过滤 {len(blocked)} 条 Master 专有路径, 放行 {len(allowed)} 条")
        return allowed

    def add_pattern(self, pattern: str) -> None:
        if pattern not in self.master_only_patterns:
            self.master_only_patterns.append(pattern)
            logger.info(f"数据隔离: 添加 Master 专有模式 {pattern}")

    def add_path(self, path: str) -> None:
        norm = os.path.normpath(path)
        if norm not in self.master_only_paths:
            self.master_only_paths.append(norm)
            logger.info(f"数据隔离: 添加 Master 专有路径 {norm}")

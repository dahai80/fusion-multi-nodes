"""M6-02 Worker 沙箱 — 限制 Worker 节点的资源与文件访问。

- CPU/内存/磁盘配额限制
- 文件系统路径白名单
- 网络出站白名单
- 执行超时
"""

from __future__ import annotations

import logging
import os
import resource
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional

from .data_isolation import DataIsolationPolicy

logger = logging.getLogger(__name__)


@dataclass
class SandboxConfig:
    """沙箱配置。"""
    max_cpu_seconds: int = 300
    max_memory_mb: int = 8192
    max_disk_mb: int = 10240
    max_processes: int = 8
    execution_timeout: int = 600
    allowed_paths: List[str] = field(default_factory=lambda: [
        "/tmp",
        "/var/tmp",
    ])
    allowed_network_hosts: List[str] = field(default_factory=list)
    allowed_env_prefixes: List[str] = field(default_factory=lambda: [
        "HOME",
        "PATH",
        "LANG",
        "LC_",
        "TMPDIR",
        "FUSION_",
        "MODEL_",
    ])
    read_only_paths: List[str] = field(default_factory=list)


class WorkerSandbox:
    """Worker 沙箱管理器 — 在任务执行前设置资源限制。"""

    def __init__(self, config: Optional[SandboxConfig] = None):
        self.config = config or SandboxConfig()
        self._active_sandboxes: Dict[str, Dict] = {}
        self._allowed_paths_set: FrozenSet[str] = frozenset(
            os.path.normpath(p) for p in self.config.allowed_paths
        )
        self._isolation_policy = DataIsolationPolicy()

    def apply_limits(self, task_id: str) -> bool:
        """为当前进程设置资源限制。仅对 Unix 有效。"""
        try:
            resource.setrlimit(
                resource.RLIMIT_CPU,
                (self.config.max_cpu_seconds, self.config.max_cpu_seconds),
            )
            logger.info(f"沙箱[{task_id}]: CPU 限制 {self.config.max_cpu_seconds}s")

            max_bytes = self.config.max_memory_mb * 1024 * 1024
            try:
                resource.setrlimit(resource.RLIMIT_AS, (max_bytes, max_bytes))
                logger.info(f"沙箱[{task_id}]: 内存限制 {self.config.max_memory_mb}MB")
            except (ValueError, AttributeError):
                logger.debug(f"沙箱[{task_id}]: RLIMIT_AS 不可用")

            try:
                resource.setrlimit(
                    resource.RLIMIT_NPROC,
                    (self.config.max_processes, self.config.max_processes),
                )
                logger.info(f"沙箱[{task_id}]: 进程数限制 {self.config.max_processes}")
            except (ValueError, AttributeError):
                logger.debug(f"沙箱[{task_id}]: RLIMIT_NPROC 不可用")

            max_file_bytes = self.config.max_disk_mb * 1024 * 1024
            try:
                resource.setrlimit(resource.RLIMIT_FSIZE, (max_file_bytes, max_file_bytes))
                logger.info(f"沙箱[{task_id}]: 磁盘限制 {self.config.max_disk_mb}MB")
            except (ValueError, AttributeError):
                logger.debug(f"沙箱[{task_id}]: RLIMIT_FSIZE 不可用")

            self._active_sandboxes[task_id] = {
                "cpu_limit": self.config.max_cpu_seconds,
                "memory_mb": self.config.max_memory_mb,
                "disk_mb": self.config.max_disk_mb,
                "max_processes": self.config.max_processes,
            }
            return True

        except Exception as e:
            logger.error(f"沙箱[{task_id}]: 设置资源限制失败: {e}")
            return False

    def clear_limits(self, task_id: str) -> None:
        self._active_sandboxes.pop(task_id, None)
        logger.info(f"沙箱[{task_id}]: 已清理")

    def check_path_access(self, path: str, write: bool = False) -> bool:
        norm_path = os.path.normpath(path)

        if self._isolation_policy.is_master_only(path):
            logger.warning(f"沙箱: 拒绝访问 Master 专有路径 {path}")
            return False

        for ro_path in self.config.read_only_paths:
            norm_ro = os.path.normpath(ro_path)
            if norm_path.startswith(norm_ro):
                if write:
                    logger.warning(f"沙箱: 拒绝写入只读路径 {path}")
                    return False
                return True

        for allowed in self._allowed_paths_set:
            if norm_path.startswith(allowed):
                return True

        logger.warning(f"沙箱: 拒绝访问路径 {path}")
        return False

    def check_network_access(self, host: str, port: int = 443) -> bool:
        if not self.config.allowed_network_hosts:
            return True

        for allowed_host in self.config.allowed_network_hosts:
            if host == allowed_host or host.endswith("." + allowed_host):
                return True

        logger.warning(f"沙箱: 拒绝网络访问 {host}:{port}")
        return False

    def is_transfer_allowed(self, source_path: str, target_role: str) -> bool:
        return self._isolation_policy.is_transfer_allowed(source_path, target_role)

    def filter_environment(self, env: Dict[str, str]) -> Dict[str, str]:
        filtered: Dict[str, str] = {}
        for key, value in env.items():
            for prefix in self.config.allowed_env_prefixes:
                if key.startswith(prefix):
                    filtered[key] = value
                    break
        logger.debug(f"沙箱: 环境变量过滤 {len(env)} → {len(filtered)}")
        return filtered

    def get_active_sandbox(self, task_id: str) -> Optional[Dict]:
        return self._active_sandboxes.get(task_id)

    @property
    def active_count(self) -> int:
        return len(self._active_sandboxes)

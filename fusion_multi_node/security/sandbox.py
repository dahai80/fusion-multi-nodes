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
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional

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

    def check_resource_usage(self, task_id: str) -> Dict[str, Any]:
        """检查当前进程资源使用情况，与沙箱限制对比。"""
        usage: Dict[str, Any] = {"task_id": task_id, "within_limits": True, "warnings": []}
        try:
            usage_ru = resource.getrusage(resource.RUSAGE_SELF)
            cpu_used = usage_ru.ru_utime + usage_ru.ru_stime
            usage["cpu_seconds"] = round(cpu_used, 1)
            if cpu_used > self.config.max_cpu_seconds * 0.9:
                usage["within_limits"] = False
                usage["warnings"].append(f"CPU 接近上限: {cpu_used:.0f}s/{self.config.max_cpu_seconds}s")

            max_rss_mb = usage_ru.ru_maxrss / 1024.0
            usage["memory_mb"] = round(max_rss_mb, 1)
            if max_rss_mb > self.config.max_memory_mb * 0.9:
                usage["within_limits"] = False
                usage["warnings"].append(f"内存接近上限: {max_rss_mb:.0f}MB/{self.config.max_memory_mb}MB")

            file_size_limit = resource.getrlimit(resource.RLIMIT_FSIZE)[0]
            usage["disk_limit_bytes"] = file_size_limit
        except Exception as e:
            logger.debug(f"沙箱[{task_id}]: 资源使用检查失败: {e}")
            usage["error"] = str(e)
        return usage

    def apply_to_subprocess(self, task_id: str) -> Dict[str, str]:
        """生成子进程可继承的环境变量，用于在子进程中执行资源限制。"""
        env = {
            "FUSION_SANDBOX_TASK_ID": task_id,
            "FUSION_SANDBOX_CPU": str(self.config.max_cpu_seconds),
            "FUSION_SANDBOX_MEMORY_MB": str(self.config.max_memory_mb),
            "FUSION_SANDBOX_DISK_MB": str(self.config.max_disk_mb),
            "FUSION_SANDBOX_NPROC": str(self.config.max_processes),
        }
        if sys.platform == "darwin":
            env["_FUSION_SANDBOX_NOTE"] = "macOS: sandbox-exec recommended for strict enforcement"
        return env

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


class SandboxExecutor:
    """M6-02 OS 级沙箱执行器 — macOS sandbox-exec / Linux unshare。"""

    def __init__(self, config: Optional[SandboxConfig] = None):
        self.config = config or SandboxConfig()
        self._backend = self._detect_backend()
        self._profile_cache: Dict[str, str] = {}
        logger.info(f"SandboxExecutor: 后端={self._backend}")

    def _detect_backend(self) -> str:
        if sys.platform == "darwin":
            sandbox_exec = "/usr/bin/sandbox-exec"
            if os.path.isfile(sandbox_exec):
                return "sandbox-exec"
            logger.warning("macOS 但 sandbox-exec 不可用，降级到 python-resource")
        if sys.platform == "linux":
            try:
                import ctypes
                libc = ctypes.CDLL("libc.so.6", use_errno=True)
                if hasattr(libc, "unshare"):
                    return "unshare"
            except Exception:
                pass
        return "python-resource"

    def _build_sbpl_profile(self) -> str:
        """生成 macOS sandbox-exec SBPL profile。"""
        allow_paths = "\n".join(
            f'    (allow file-read* file-write* (subpath "{p}"))'
            for p in self.config.allowed_paths
        )
        ro_paths = "\n".join(
            f'    (allow file-read* (subpath "{p}"))'
            for p in self.config.read_only_paths
        )
        net_rules = ""
        if self.config.allowed_network_hosts:
            for host in self.config.allowed_network_hosts:
                net_rules += f'\n    (allow network-outbound (host "{host}"))'
            net_rules += "\n    (deny network-outbound)"
        else:
            net_rules = "\n    (allow network-outbound)"

        profile = f"""(version 1)
(deny default)
(allow process-exec (subpath "/usr") (subpath "/bin") (subpath "/sbin"))
(allow file-read* (subpath "/System") (subpath "/Library") (subpath "/usr"))
{allow_paths}
{ro_paths}
{net_rules}
(allow process-fork)
(allow signal)
(allow ipc-posix-sem)
(allow ipc-posix-shm)
(allow mach-lookup)
"""
        return profile

    def _get_profile_path(self, task_id: str) -> str:
        if task_id not in self._profile_cache:
            profile = self._build_sbpl_profile()
            profile_dir = os.path.join("/tmp", "fusion_sandbox_profiles")
            os.makedirs(profile_dir, exist_ok=True)
            profile_path = os.path.join(profile_dir, f"sbpl_{task_id}.sb")
            with open(profile_path, "w") as f:
                f.write(profile)
            self._profile_cache[task_id] = profile_path
            logger.debug(f"SandboxExecutor: 生成 SBPL profile {profile_path}")
        return self._profile_cache[task_id]

    async def execute_in_sandbox(
        self, task_id: str, command: list, timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """在 OS 沙箱中执行命令，返回执行结果。"""
        import asyncio as _asyncio

        exec_timeout = timeout or self.config.execution_timeout
        result: Dict[str, Any] = {"task_id": task_id, "exit_code": -1, "stdout": "", "stderr": ""}

        if self._backend == "sandbox-exec":
            profile_path = self._get_profile_path(task_id)
            full_cmd = ["/usr/bin/sandbox-exec", "-f", profile_path, "--"] + command
            logger.info(f"SandboxExecutor[{task_id}]: sandbox-exec 执行: {command}")
        elif self._backend == "unshare":
            full_cmd = ["unshare", "--pid", "--fork", "--mount-proc", "--"] + command
            logger.info(f"SandboxExecutor[{task_id}]: unshare 执行: {command}")
        else:
            full_cmd = command
            logger.info(f"SandboxExecutor[{task_id}]: 无 OS 沙箱，直接执行")

        try:
            proc = await _asyncio.create_subprocess_exec(
                *full_cmd,
                stdout=_asyncio.subprocess.PIPE,
                stderr=_asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await _asyncio.wait_for(
                    proc.communicate(), timeout=exec_timeout
                )
                result["exit_code"] = proc.returncode or 0
                result["stdout"] = stdout.decode(errors="replace")
                result["stderr"] = stderr.decode(errors="replace")
            except _asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                result["exit_code"] = -9
                result["stderr"] = f"执行超时({exec_timeout}s)，已终止"
                logger.warning(f"SandboxExecutor[{task_id}]: 执行超时")
        except FileNotFoundError as e:
            result["exit_code"] = -1
            result["stderr"] = f"命令未找到: {e}"
            logger.error(f"SandboxExecutor[{task_id}]: 命令未找到: {e}")
        except Exception as e:
            result["exit_code"] = -1
            result["stderr"] = str(e)
            logger.error(f"SandboxExecutor[{task_id}]: 执行失败: {e}")

        return result

    def cleanup_profile(self, task_id: str) -> None:
        profile_path = self._profile_cache.pop(task_id, None)
        if profile_path and os.path.isfile(profile_path):
            os.unlink(profile_path)
            logger.debug(f"SandboxExecutor: 清理 profile {profile_path}")

    @property
    def backend(self) -> str:
        return self._backend

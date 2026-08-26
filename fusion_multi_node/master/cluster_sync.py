"""集群同步与韧性 — 共享模型缓存、增量同步、网络分区降级、节点负载报告。

⚠️ AR审计 P1 合规: 仅限局域网对端同步 (is_safe_peer_host 拒环回/链路本地/元数据,
放行私网), 不出站云。路径安全 (is_safe_path_segment + normpath 遍历拦截)。
ClusterSyncManager 已接 master_server start()/stop() 生命周期 (E1 一次性构造于 __init__)。

- ModelManifest: 模型文件清单 + SHA256 哈希
- IncrementalSync: 仅同步差异文件
- PartitionDetector: 心跳超时检测，降级单机运行，恢复后自动同步
- NodeLoadReport: 节点硬件负载报告
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import httpx

from fusion_multi_node.utils.auth import build_safe_url, is_safe_path_segment, is_safe_peer_host

logger = logging.getLogger(__name__)


def _safe_rel_path(rel: str) -> str:
    """校验来自远端 manifest 的相对路径，防路径穿越。

    规范化后必须仍在 model_dir 内，且每个段合法。
    """
    if not rel or "\x00" in rel:
        raise ValueError(f"非法同步路径: {rel!r}")
    # 拒绝对绝对路径与盘符
    if rel.startswith("/") or ":" in rel.split("/")[0]:
        raise ValueError(f"非法同步路径: {rel!r}")
    norm = os.path.normpath(rel)
    if norm.startswith("..") or "/.." in norm or norm == "..":
        raise ValueError(f"路径穿越被拒: {rel!r}")
    for seg in norm.split("/"):
        if not is_safe_path_segment(seg):
            raise ValueError(f"路径段非法: {seg!r} (in {rel!r})")
    return norm


class NodeHealth(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DISCONNECTED = "disconnected"
    SYNCING = "syncing"


class PartitionState(StrEnum):
    CONNECTED = "connected"
    PARTIAL = "partial"
    PARTITIONED = "partitioned"


@dataclass
class FileEntry:
    path: str
    size: int = 0
    sha256: str = ""
    modified_at: float = 0.0


@dataclass
class ModelManifest:
    model_name: str
    model_id: str = ""
    files: list[FileEntry] = field(default_factory=list)
    total_size: int = 0
    created_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "model_id": self.model_id,
            "total_size": self.total_size,
            "created_at": self.created_at,
            "files": [
                {"path": f.path, "size": f.size, "sha256": f.sha256, "modified_at": f.modified_at} for f in self.files
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ModelManifest:
        files = [FileEntry(**f) for f in data.get("files", [])]
        return cls(
            model_name=data.get("model_name", ""),
            model_id=data.get("model_id", ""),
            files=files,
            total_size=data.get("total_size", 0),
            created_at=data.get("created_at", 0.0),
        )


@dataclass
class NodeLoadReport:
    node_id: str
    gpu_memory_used_gb: float = 0.0
    gpu_memory_total_gb: float = 0.0
    ram_used_gb: float = 0.0
    ram_total_gb: float = 0.0
    disk_used_gb: float = 0.0
    disk_total_gb: float = 0.0
    cpu_percent: float = 0.0
    active_tasks: int = 0
    max_tasks: int = 0
    reported_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "gpu_memory_used_gb": self.gpu_memory_used_gb,
            "gpu_memory_total_gb": self.gpu_memory_total_gb,
            "ram_used_gb": self.ram_used_gb,
            "ram_total_gb": self.ram_total_gb,
            "disk_used_gb": self.disk_used_gb,
            "disk_total_gb": self.disk_total_gb,
            "cpu_percent": self.cpu_percent,
            "active_tasks": self.active_tasks,
            "max_tasks": self.max_tasks,
            "reported_at": self.reported_at,
        }


def compute_file_sha256(file_path: str) -> str:
    h = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logger.error(f"计算文件哈希失败: {file_path}, {e}")
        return ""


def build_manifest(model_name: str, model_dir: str, model_id: str = "") -> ModelManifest:
    """扫描模型目录，生成 ModelManifest。"""
    files = []
    total_size = 0
    if not os.path.isdir(model_dir):
        logger.warning(f"模型目录不存在: {model_dir}")
        return ModelManifest(model_name=model_name, model_id=model_id)
    for root, _dirs, filenames in os.walk(model_dir):
        for fname in filenames:
            fpath = os.path.join(root, fname)
            rel_path = os.path.relpath(fpath, model_dir)
            try:
                stat = os.stat(fpath)
                sha256 = compute_file_sha256(fpath)
                files.append(
                    FileEntry(
                        path=rel_path,
                        size=stat.st_size,
                        sha256=sha256,
                        modified_at=stat.st_mtime,
                    )
                )
                total_size += stat.st_size
            except Exception as e:
                logger.error(f"扫描文件失败: {fpath}, {e}")
    return ModelManifest(
        model_name=model_name,
        model_id=model_id,
        files=files,
        total_size=total_size,
        created_at=time.time(),
    )


def compute_sync_diff(local: ModelManifest, remote: ModelManifest) -> list[FileEntry]:
    """对比本地与远端 manifest，返回需要同步的文件列表。"""
    local_map = {f.path: f for f in local.files}
    remote_map = {f.path: f for f in remote.files}
    diff_files = []
    for path, remote_entry in remote_map.items():
        local_entry = local_map.get(path)
        if local_entry is None or local_entry.sha256 != remote_entry.sha256:
            diff_files.append(remote_entry)
    for path in local_map:
        if path not in remote_map:
            diff_files.append(FileEntry(path=path, sha256="__deleted__"))
    logger.info(f"增量同步差异: {len(diff_files)}/{len(remote_map)} files need sync")
    return diff_files


class PartitionDetector:
    """网络分区检测与降级管理。"""

    def __init__(
        self,
        node_id: str,
        heartbeat_timeout: float = 30.0,
        check_interval: float = 10.0,
    ):
        self.node_id = node_id
        self.heartbeat_timeout = heartbeat_timeout
        self.check_interval = check_interval
        self._last_heartbeat: dict[str, float] = {}
        self._state = PartitionState.CONNECTED
        self._degraded = False
        self._running = False
        self._task: asyncio.Task | None = None
        self._on_partition: Any = None
        self._on_reconnect: Any = None

    @property
    def state(self) -> PartitionState:
        return self._state

    @property
    def is_degraded(self) -> bool:
        return self._degraded

    def update_heartbeat(self, node_id: str) -> None:
        self._last_heartbeat[node_id] = time.time()
        if self._degraded:
            logger.info(f"分区恢复: 收到 {node_id} 心跳")
            self._degraded = False
            self._state = PartitionState.CONNECTED
            if self._on_reconnect:
                self._on_reconnect()

    def register_callbacks(self, on_partition: Any = None, on_reconnect: Any = None) -> None:
        self._on_partition = on_partition
        self._on_reconnect = on_reconnect

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        logger.info(f"分区检测启动: timeout={self.heartbeat_timeout}s")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _check_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.check_interval)
            self._check_partition()

    def _check_partition(self) -> None:
        now = time.time()
        disconnected = []
        for nid, last_time in list(self._last_heartbeat.items()):
            if now - last_time > self.heartbeat_timeout:
                disconnected.append(nid)
        if disconnected and not self._degraded:
            self._degraded = True
            self._state = PartitionState.PARTITIONED
            logger.warning(f"网络分区检测: {disconnected} 已断连，降级为单机运行")
            if self._on_partition:
                self._on_partition(disconnected)
        elif not disconnected and self._degraded:
            self._degraded = False
            self._state = PartitionState.CONNECTED
            logger.info("网络分区恢复: 所有节点已重连")
            if self._on_reconnect:
                self._on_reconnect()

    def get_status(self) -> dict[str, Any]:
        now = time.time()
        nodes_status = {}
        for nid, last_time in self._last_heartbeat.items():
            elapsed = now - last_time
            nodes_status[nid] = {
                "last_heartbeat_ago": round(elapsed, 1),
                "status": "disconnected" if elapsed > self.heartbeat_timeout else "connected",
            }
        return {
            "node_id": self.node_id,
            "partition_state": self._state.value,
            "is_degraded": self._degraded,
            "nodes": nodes_status,
        }


class ClusterSyncManager:
    """集群模型同步管理器。"""

    def __init__(
        self,
        model_cache_dir: str = "",
        shared_storage_path: str = "",
        node_id: str = "",
    ):
        self.model_cache_dir = model_cache_dir or os.path.expanduser("~/.fusion-mlx/models")
        self.shared_storage_path = shared_storage_path
        self.node_id = node_id
        self._partition_detector = PartitionDetector(node_id)
        self._sync_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._sync_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._running = True
        self._sync_task = asyncio.create_task(self._sync_loop())
        await self._partition_detector.start()
        logger.info(f"集群同步管理器启动: cache_dir={self.model_cache_dir}")

    async def stop(self) -> None:
        self._running = False
        await self._partition_detector.stop()
        if self._sync_task:
            self._sync_task.cancel()
            self._sync_task = None

    def get_manifest(self, model_name: str) -> ModelManifest:
        """获取本地模型的 manifest。"""
        model_dir = os.path.join(self.model_cache_dir, model_name)
        return build_manifest(model_name, model_dir)

    async def incremental_sync(
        self,
        model_name: str,
        remote_manifest: ModelManifest,
        source_host: str,
        source_port: int = 11452,
    ) -> dict[str, Any]:
        """增量同步: 对比 manifest，仅下载差异文件。"""
        local_manifest = self.get_manifest(model_name)
        diff_files = compute_sync_diff(local_manifest, remote_manifest)
        if not diff_files:
            logger.info(f"增量同步: {model_name} 无差异，跳过")
            return {"model_name": model_name, "synced": 0, "status": "up_to_date"}
        synced = 0
        for fentry in diff_files:
            if fentry.sha256 == "__deleted__":
                continue
            try:
                if not is_safe_peer_host(source_host):
                    raise ValueError(f"不安全对端主机: {source_host!r}")
                safe_rel = _safe_rel_path(fentry.path)
                if not is_safe_path_segment(model_name):
                    raise ValueError(f"非法 model_name: {model_name!r}")
                client = httpx.AsyncClient(timeout=300.0)
                url = build_safe_url(
                    "http", source_host, source_port, f"/api/models/{model_name}/files"
                )
                resp = await client.get(url, params={"path": safe_rel})
                model_dir = os.path.join(self.model_cache_dir, model_name)
                dest = os.path.normpath(os.path.join(model_dir, safe_rel))
                # 二次确认 dest 仍在 model_dir 内
                if not dest.startswith(os.path.normpath(model_dir) + os.sep) and dest != os.path.normpath(model_dir):
                    raise ValueError(f"逃逸目标目录: {dest!r}")
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest, "wb") as f:
                    f.write(resp.content)
                synced += 1
                await client.aclose()
            except Exception as e:
                logger.error(f"同步文件失败: {fentry.path}, {e}")
        logger.info(f"增量同步完成: {model_name}, {synced}/{len(diff_files)} files")
        return {"model_name": model_name, "synced": synced, "total_diff": len(diff_files)}

    def trigger_sync(self, model_name: str, source_host: str, source_port: int = 11452) -> None:
        """触发异步同步任务。"""
        self._sync_queue.put_nowait((model_name, source_host, source_port))
        logger.info(f"同步任务入队: {model_name} from {source_host}")

    async def _sync_loop(self) -> None:
        while self._running:
            try:
                model_name, source_host, source_port = await asyncio.wait_for(self._sync_queue.get(), timeout=5.0)
            except TimeoutError:
                continue
            try:
                if not is_safe_peer_host(source_host):
                    raise ValueError(f"不安全对端主机: {source_host!r}")
                if not is_safe_path_segment(model_name):
                    raise ValueError(f"非法 model_name: {model_name!r}")
                client = httpx.AsyncClient(timeout=30.0)
                url = build_safe_url(
                    "http", source_host, source_port, f"/api/models/{model_name}/manifest"
                )
                resp = await client.get(url)
                remote_manifest = ModelManifest.from_dict(resp.json())
                await client.aclose()
                await self.incremental_sync(model_name, remote_manifest, source_host, source_port)
            except Exception as e:
                logger.error(f"同步循环处理失败: {model_name}, {e}")

    def collect_load_report(self) -> NodeLoadReport:
        """采集本节点硬件负载。"""
        try:
            import psutil

            ram = psutil.virtual_memory()
            disk = psutil.disk_usage("/")
            cpu = psutil.cpu_percent(interval=0.1)
        except ImportError:
            logger.warning("psutil 未安装，负载报告不可用")
            return NodeLoadReport(node_id=self.node_id, reported_at=time.time())
        gpu_total = 0.0
        gpu_used = 0.0
        try:
            import subprocess

            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            for line in result.stdout.splitlines():
                if "VRAM" in line or "Total Number of Cores" in line:
                    pass
        except Exception:
            pass
        return NodeLoadReport(
            node_id=self.node_id,
            gpu_memory_used_gb=gpu_used,
            gpu_memory_total_gb=gpu_total,
            ram_used_gb=round(ram.used / (1024**3), 2),
            ram_total_gb=round(ram.total / (1024**3), 2),
            disk_used_gb=round(disk.used / (1024**3), 2),
            disk_total_gb=round(disk.total / (1024**3), 2),
            cpu_percent=cpu,
            reported_at=time.time(),
        )

    def get_cluster_status(self) -> dict[str, Any]:
        return self._partition_detector.get_status()

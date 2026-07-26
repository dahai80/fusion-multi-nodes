"""M9 存储卷 — 统一存储抽象层。

- StorageVolume: 逻辑存储卷（本地/分布式）
- VolumeSpec: 卷规格定义
- 支持多种后端: 本地文件系统、共享目录、分布式
- M9-03: 容量监控 + LRU 自动驱逐
- M9-05: 模型分片分发到 Worker 存储卷
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class VolumeType(Enum):
    LOCAL = "local"
    SHARED = "shared"
    DISTRIBUTED = "distributed"


@dataclass
class VolumeSpec:
    """卷规格。"""
    name: str
    volume_type: VolumeType = VolumeType.LOCAL
    path: str = ""
    size_limit_mb: int = 0
    replication_factor: int = 1
    encrypted: bool = False
    compress: bool = False
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class VolumeInfo:
    """卷运行时信息。"""
    name: str
    spec: VolumeSpec
    used_bytes: int = 0
    file_count: int = 0
    created_at: float = 0.0
    last_accessed: float = 0.0
    status: str = "active"


@dataclass
class FileEntry:
    """文件条目，用于 LRU 追踪。"""
    path: str
    volume_name: str
    size_bytes: int
    last_accessed: float
    created_at: float = 0.0


@dataclass
class CapacityReport:
    """容量报告。"""
    volume_name: str
    total_mb: float
    used_mb: float
    available_mb: float
    usage_ratio: float
    file_count: int
    needs_eviction: bool


class StorageVolume:
    """存储卷管理器。

    提供统一的存储操作接口:
    - create_volume / delete_volume
    - write_file / read_file / delete_file
    - list_files / get_info
    - M9-03: 容量监控 + LRU 自动驱逐
    - M9-05: 模型分片分发
    """

    EVICTION_HIGH_WATERMARK = 0.9
    EVICTION_LOW_WATERMARK = 0.7

    def __init__(self, base_dir: str = ""):
        self._base_dir = base_dir or str(Path.home() / ".fusion" / "volumes")
        self._volumes: Dict[str, VolumeInfo] = {}
        self._file_entries: Dict[str, Dict[str, FileEntry]] = {}
        self._shard_distributions: Dict[str, List[Dict[str, Any]]] = {}

    def create_volume(self, spec: VolumeSpec) -> bool:
        vol_path = self._resolve_path(spec.name, spec)
        try:
            vol_path = Path(vol_path)
            vol_path.mkdir(parents=True, exist_ok=True)
            self._volumes[spec.name] = VolumeInfo(
                name=spec.name,
                spec=spec,
                created_at=time.time(),
                last_accessed=time.time(),
            )
            self._file_entries[spec.name] = {}
            logger.info(f"创建存储卷: {spec.name} ({spec.volume_type.value}) @ {vol_path}")
            return True
        except Exception as e:
            logger.error(f"创建存储卷失败: {spec.name}: {e}")
            return False

    def delete_volume(self, name: str) -> bool:
        info = self._volumes.get(name)
        if not info:
            return False
        vol_path = self._resolve_path(name, info.spec)
        try:
            if Path(vol_path).exists():
                shutil.rmtree(vol_path)
            del self._volumes[name]
            self._file_entries.pop(name, None)
            logger.info(f"删除存储卷: {name}")
            return True
        except Exception as e:
            logger.error(f"删除存储卷失败: {name}: {e}")
            return False

    def write_file(self, volume_name: str, file_path: str, data: bytes) -> bool:
        info = self._volumes.get(volume_name)
        if not info:
            logger.error(f"存储卷不存在: {volume_name}")
            return False
        # M9-03 容量检查 + LRU 驱逐
        if info.spec.size_limit_mb > 0:
            limit_bytes = info.spec.size_limit_mb * 1024 * 1024
            if info.used_bytes + len(data) > limit_bytes:
                self._evict_lru(volume_name, needed_bytes=len(data))
                if info.used_bytes + len(data) > limit_bytes:
                    logger.error(f"存储卷已满: {volume_name} ({info.used_bytes}/{limit_bytes})")
                    return False
        full_path = Path(self._resolve_path(volume_name, info.spec)) / file_path
        try:
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_bytes(data)
            now = time.time()
            info.last_accessed = now
            info.file_count += 1
            info.used_bytes += len(data)
            entries = self._file_entries.get(volume_name, {})
            entries[file_path] = FileEntry(
                path=file_path,
                volume_name=volume_name,
                size_bytes=len(data),
                last_accessed=now,
                created_at=now,
            )
            return True
        except Exception as e:
            logger.error(f"写入文件失败: {volume_name}/{file_path}: {e}")
            return False

    def read_file(self, volume_name: str, file_path: str) -> Optional[bytes]:
        info = self._volumes.get(volume_name)
        if not info:
            return None
        full_path = Path(self._resolve_path(volume_name, info.spec)) / file_path
        try:
            if full_path.exists():
                now = time.time()
                info.last_accessed = now
                entries = self._file_entries.get(volume_name, {})
                if file_path in entries:
                    entries[file_path].last_accessed = now
                return full_path.read_bytes()
            return None
        except Exception as e:
            logger.error(f"读取文件失败: {volume_name}/{file_path}: {e}")
            return None

    def delete_file(self, volume_name: str, file_path: str) -> bool:
        info = self._volumes.get(volume_name)
        if not info:
            return False
        full_path = Path(self._resolve_path(volume_name, info.spec)) / file_path
        try:
            if full_path.exists():
                size = full_path.stat().st_size
                full_path.unlink()
                info.used_bytes = max(0, info.used_bytes - size)
                info.file_count = max(0, info.file_count - 1)
            entries = self._file_entries.get(volume_name, {})
            entries.pop(file_path, None)
            return True
        except Exception as e:
            logger.error(f"删除文件失败: {volume_name}/{file_path}: {e}")
            return False

    def list_files(self, volume_name: str, prefix: str = "") -> List[str]:
        info = self._volumes.get(volume_name)
        if not info:
            return []
        vol_path = Path(self._resolve_path(volume_name, info.spec))
        if not vol_path.exists():
            return []
        results = []
        search_dir = vol_path / prefix if prefix else vol_path
        if search_dir.exists():
            for f in search_dir.rglob("*"):
                if f.is_file():
                    results.append(str(f.relative_to(vol_path)))
        return results

    def get_volume_info(self, name: str) -> Optional[VolumeInfo]:
        return self._volumes.get(name)

    def list_volumes(self) -> List[VolumeInfo]:
        return list(self._volumes.values())

    # ── M9-03 容量监控 ──

    def get_capacity_report(self, volume_name: str) -> Optional[CapacityReport]:
        """获取卷容量报告。"""
        info = self._volumes.get(volume_name)
        if not info:
            return None
        total_mb = info.spec.size_limit_mb if info.spec.size_limit_mb > 0 else 0
        used_mb = info.used_bytes / (1024 * 1024)
        if total_mb > 0:
            available_mb = max(0, total_mb - used_mb)
            usage_ratio = used_mb / total_mb
            needs_eviction = usage_ratio >= self.EVICTION_HIGH_WATERMARK
        else:
            available_mb = 0
            usage_ratio = 0.0
            needs_eviction = False
        return CapacityReport(
            volume_name=volume_name,
            total_mb=total_mb,
            used_mb=used_mb,
            available_mb=available_mb,
            usage_ratio=usage_ratio,
            file_count=info.file_count,
            needs_eviction=needs_eviction,
        )

    def check_all_capacities(self) -> List[CapacityReport]:
        """检查所有卷容量。"""
        reports = []
        for name in self._volumes:
            report = self.get_capacity_report(name)
            if report:
                reports.append(report)
        return reports

    def _evict_lru(self, volume_name: str, needed_bytes: int = 0) -> bool:
        """M9-03 LRU 自动驱逐。"""
        info = self._volumes.get(volume_name)
        if not info:
            return False
        entries = self._file_entries.get(volume_name, {})
        if not entries:
            return False

        limit_bytes = info.spec.size_limit_mb * 1024 * 1024
        target_bytes = int(limit_bytes * self.EVICTION_LOW_WATERMARK)
        evicted = 0

        sorted_entries = sorted(entries.items(), key=lambda x: x[1].last_accessed)
        for file_path, entry in sorted_entries:
            if info.used_bytes <= target_bytes:
                break
            if self.delete_file(volume_name, file_path):
                evicted += 1
                logger.info(f"LRU 驱逐: {volume_name}/{file_path} ({entry.size_bytes} bytes)")

        if evicted > 0:
            logger.info(f"LRU 驱逐完成: {volume_name} 释放 {evicted} 文件")
        return evicted > 0

    # ── M9-05 模型分片分发 ──

    def distribute_shard(
        self,
        shard_id: str,
        shard_data: bytes,
        target_volume: str,
        shard_path: str,
        node_id: str,
    ) -> bool:
        """M9-05 分发模型分片到 Worker 存储卷。"""
        ok = self.write_file(target_volume, shard_path, shard_data)
        if ok:
            dist = self._shard_distributions.setdefault(shard_id, [])
            dist.append({
                "shard_id": shard_id,
                "volume_name": target_volume,
                "shard_path": shard_path,
                "node_id": node_id,
                "size_bytes": len(shard_data),
                "distributed_at": time.time(),
            })
            logger.info(f"分片分发: {shard_id} → {node_id}:{target_volume}/{shard_path}")
        return ok

    def get_shard_distribution(self, shard_id: str) -> List[Dict[str, Any]]:
        """获取分片分发记录。"""
        return self._shard_distributions.get(shard_id, [])

    def verify_shard(self, volume_name: str, shard_path: str, expected_size: int) -> bool:
        """验证分片完整性。"""
        data = self.read_file(volume_name, shard_path)
        if data is None:
            return False
        return len(data) == expected_size

    def _resolve_path(self, name: str, spec: VolumeSpec) -> str:
        if spec.path:
            return spec.path
        return os.path.join(self._base_dir, name)

"""M9 分片副本管理 — 模型分片的多副本复制与同步。

M9-02: ShardReplicator 实际数据传输/同步（通过 StorageVolume）
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ReplicationConfig:
    """副本配置。"""
    replication_factor: int = 2
    sync_timeout: float = 60.0
    retry_attempts: int = 3
    retry_delay: float = 5.0
    verify_checksum: bool = True


@dataclass
class ShardReplica:
    """分片副本。"""
    shard_id: str
    node_id: str
    volume_name: str
    file_path: str
    size_bytes: int = 0
    checksum: str = ""
    created_at: float = 0.0
    last_synced: float = 0.0
    status: str = "active"


@dataclass
class SyncResult:
    """同步结果。"""
    shard_id: str
    target_node_id: str
    success: bool
    bytes_transferred: int = 0
    checksum_verified: bool = False
    duration_ms: float = 0.0
    error: str = ""


class ShardReplicator:
    """分片副本管理器。

    管理模型分片的多副本分布:
    - 分配副本到不同节点
    - 检测副本健康状态
    - M9-02: 实际数据传输与同步
    - 触发副本同步/修复
    """

    def __init__(self, config: Optional[ReplicationConfig] = None):
        self.config = config or ReplicationConfig()
        self._replicas: Dict[str, List[ShardReplica]] = {}
        self._shard_data: Dict[str, bytes] = {}

    def register_shard_data(self, shard_id: str, data: bytes) -> None:
        """M9-02 注册分片数据（用于后续同步传输）。"""
        self._shard_data[shard_id] = data
        logger.debug(f"分片数据注册: {shard_id} ({len(data)} bytes)")

    def get_shard_data(self, shard_id: str) -> Optional[bytes]:
        """获取缓存的分片数据。"""
        return self._shard_data.get(shard_id)

    def assign_replicas(
        self,
        shard_id: str,
        file_path: str,
        size_bytes: int,
        available_nodes: List[Dict[str, Any]],
        volume_name: str = "models",
    ) -> List[ShardReplica]:
        count = min(self.config.replication_factor, len(available_nodes))
        replicas = []
        for i in range(count):
            node = available_nodes[i]
            replica = ShardReplica(
                shard_id=shard_id,
                node_id=node.get("node_id", f"node-{i}"),
                volume_name=volume_name,
                file_path=file_path,
                size_bytes=size_bytes,
                created_at=time.time(),
                last_synced=time.time(),
            )
            replicas.append(replica)

        self._replicas[shard_id] = replicas
        logger.info(f"分片副本分配: {shard_id} × {count}")
        return replicas

    def sync_to_node(
        self,
        shard_id: str,
        target_node_id: str,
        storage_volume: Any = None,
    ) -> SyncResult:
        """M9-02 将分片数据同步到目标节点。"""
        start = time.time()
        data = self._shard_data.get(shard_id)
        if data is None:
            return SyncResult(
                shard_id=shard_id,
                target_node_id=target_node_id,
                success=False,
                error="分片数据未注册",
            )

        replica = None
        for r in self._replicas.get(shard_id, []):
            if r.node_id == target_node_id:
                replica = r
                break

        if not replica:
            return SyncResult(
                shard_id=shard_id,
                target_node_id=target_node_id,
                success=False,
                error="目标节点未分配副本",
            )

        try:
            if storage_volume is not None:
                ok = storage_volume.write_file(
                    replica.volume_name, replica.file_path, data,
                )
                if not ok:
                    return SyncResult(
                        shard_id=shard_id,
                        target_node_id=target_node_id,
                        success=False,
                        error="写入存储卷失败",
                    )

            checksum = hashlib.sha256(data).hexdigest()
            verified = True
            if self.config.verify_checksum and storage_volume is not None:
                read_data = storage_volume.read_file(replica.volume_name, replica.file_path)
                if read_data != data:
                    verified = False
                    logger.error(f"分片校验失败: {shard_id}@{target_node_id}")

            replica.last_synced = time.time()
            replica.checksum = checksum
            replica.size_bytes = len(data)

            duration = (time.time() - start) * 1000
            logger.info(f"分片同步完成: {shard_id} → {target_node_id} ({len(data)} bytes, {duration:.1f}ms)")

            return SyncResult(
                shard_id=shard_id,
                target_node_id=target_node_id,
                success=True,
                bytes_transferred=len(data),
                checksum_verified=verified,
                duration_ms=duration,
            )
        except Exception as e:
            duration = (time.time() - start) * 1000
            logger.error(f"分片同步异常: {shard_id} → {target_node_id}: {e}")
            return SyncResult(
                shard_id=shard_id,
                target_node_id=target_node_id,
                success=False,
                duration_ms=duration,
                error=str(e),
            )

    def sync_all_replicas(self, shard_id: str, storage_volume: Any = None) -> List[SyncResult]:
        """M9-02 同步分片到所有副本节点。"""
        results = []
        for replica in self._replicas.get(shard_id, []):
            if replica.status == "active":
                result = self.sync_to_node(shard_id, replica.node_id, storage_volume)
                results.append(result)
        logger.info(f"分片全量同步: {shard_id} 成功={sum(1 for r in results if r.success)}/{len(results)}")
        return results

    def repair_replica(self, shard_id: str, target_node_id: str, storage_volume: Any = None) -> SyncResult:
        """修复失败副本。"""
        self.mark_replica_recovering(shard_id, target_node_id)
        result = self.sync_to_node(shard_id, target_node_id, storage_volume)
        if result.success:
            self.mark_replica_active(shard_id, target_node_id)
        return result

    def get_replicas(self, shard_id: str) -> List[ShardReplica]:
        return self._replicas.get(shard_id, [])

    def get_healthy_replica(self, shard_id: str) -> Optional[ShardReplica]:
        for replica in self._replicas.get(shard_id, []):
            if replica.status == "active":
                return replica
        return None

    def mark_replica_failed(self, shard_id: str, node_id: str) -> None:
        for replica in self._replicas.get(shard_id, []):
            if replica.node_id == node_id:
                replica.status = "failed"
                logger.warning(f"分片副本失败: {shard_id}@{node_id}")

    def mark_replica_recovering(self, shard_id: str, node_id: str) -> None:
        for replica in self._replicas.get(shard_id, []):
            if replica.node_id == node_id:
                replica.status = "recovering"
                logger.info(f"分片副本恢复中: {shard_id}@{node_id}")

    def mark_replica_active(self, shard_id: str, node_id: str) -> None:
        for replica in self._replicas.get(shard_id, []):
            if replica.node_id == node_id:
                replica.status = "active"
                logger.info(f"分片副本恢复完成: {shard_id}@{node_id}")

    def remove_node_replicas(self, node_id: str) -> List[str]:
        affected = []
        for shard_id, replicas in self._replicas.items():
            for replica in replicas:
                if replica.node_id == node_id:
                    replica.status = "failed"
                    affected.append(shard_id)
        return affected

    def get_under_replicated(self) -> List[str]:
        result = []
        for shard_id, replicas in self._replicas.items():
            active = sum(1 for r in replicas if r.status == "active")
            if active < self.config.replication_factor:
                result.append(shard_id)
        return result

    def get_stats(self) -> Dict[str, Any]:
        total = sum(len(r) for r in self._replicas.values())
        active = sum(sum(1 for r in rlist if r.status == "active") for rlist in self._replicas.values())
        return {
            "total_shards": len(self._replicas),
            "total_replicas": total,
            "active_replicas": active,
            "under_replicated": len(self.get_under_replicated()),
            "cached_shard_data": len(self._shard_data),
        }

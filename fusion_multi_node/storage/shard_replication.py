"""M9 分片副本管理 — 模型分片的多副本复制与同步。

M9-02: ShardReplicator 实际数据传输/同步（通过 StorageVolume 或 FMP 跨节点传输）
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any

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

    def __init__(self, config: ReplicationConfig | None = None, fmp_interface: Any = None):
        self.config = config or ReplicationConfig()
        self._replicas: dict[str, list[ShardReplica]] = {}
        self._shard_data: dict[str, bytes] = {}
        self._fmp_interface = fmp_interface
        self._local_node_id: str | None = None

    def register_shard_data(self, shard_id: str, data: bytes) -> None:
        """M9-02 注册分片数据（用于后续同步传输）。"""
        self._shard_data[shard_id] = data
        logger.debug(f"分片数据注册: {shard_id} ({len(data)} bytes)")

    def get_shard_data(self, shard_id: str) -> bytes | None:
        """获取缓存的分片数据。"""
        return self._shard_data.get(shard_id)

    def assign_replicas(
        self,
        shard_id: str,
        file_path: str,
        size_bytes: int,
        available_nodes: list[dict[str, Any]],
        volume_name: str = "models",
    ) -> list[ShardReplica]:
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
        """M9-02 将分片数据同步到目标节点。

        当目标节点是远程节点且 fmp_interface 已设置时，通过 FMP 传输；
        否则写入本地 StorageVolume。
        """
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

        is_remote = (
            self._fmp_interface is not None
            and self._local_node_id is not None
            and target_node_id != self._local_node_id
        )

        if is_remote:
            return self._sync_via_fmp(shard_id, target_node_id, data, replica, start)

        return self._sync_local(shard_id, target_node_id, data, replica, storage_volume, start)

    def _sync_via_fmp(
        self,
        shard_id: str,
        target_node_id: str,
        data: bytes,
        replica: ShardReplica,
        start: float,
    ) -> SyncResult:
        """通过 FMP 协议将分片数据发送到远程节点。"""
        try:
            from fusion_multi_node.protocol.fmp_message import FMPMessage, PayloadType

            payload = {
                "shard_id": shard_id,
                "volume_name": replica.volume_name,
                "file_path": replica.file_path,
                "data_b64": base64.b64encode(data).decode("ascii"),
                "checksum": hashlib.sha256(data).hexdigest(),
                "size_bytes": len(data),
            }
            msg = FMPMessage.create(
                source_id=self._local_node_id,
                target_id=target_node_id,
                payload_type=PayloadType.SHARD_SYNC,
                payload=payload,
            )

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            # AR审计硬伤4: 原实现 fire-and-forget (ensure_future 不 await) 却返回
            # success=True/checksum_verified=True → quorum 写保证虚构 (远端可能从未收到,
            # 也无 ACK 校验)。诚实化: 仅同步 await 的 send 可声称 success, checksum_verified
            # 始终 False (无应用层 ACK 确认远端校验)。
            delivered = False
            if loop and loop.is_running():
                # 事件循环内: 不阻塞, 标记为未确认
                asyncio.ensure_future(self._fmp_interface._conn_mgr.send_to(target_node_id, msg))
                logger.warning(f"分片 FMP 同步 fire-and-forget: {shard_id} → {target_node_id} 未确认")
            else:
                # 无事件循环: 同步 await 发送, 确认投递到 socket (仍非应用层 ACK)
                asyncio.run(self._fmp_interface._conn_mgr.send_to(target_node_id, msg))
                delivered = True

            checksum = hashlib.sha256(data).hexdigest()
            replica.last_synced = time.time()
            replica.checksum = checksum
            replica.size_bytes = len(data)

            duration = (time.time() - start) * 1000
            logger.info(f"分片 FMP 同步: {shard_id} → {target_node_id} ({len(data)} bytes, {duration:.1f}ms)")

            return SyncResult(
                shard_id=shard_id,
                target_node_id=target_node_id,
                success=delivered,
                bytes_transferred=len(data) if delivered else 0,
                checksum_verified=False,
                duration_ms=duration,
            )
        except Exception as e:
            duration = (time.time() - start) * 1000
            logger.error(f"分片 FMP 同步异常: {shard_id} → {target_node_id}: {e}")
            return SyncResult(
                shard_id=shard_id,
                target_node_id=target_node_id,
                success=False,
                duration_ms=duration,
                error=str(e),
            )

    def _sync_local(
        self,
        shard_id: str,
        target_node_id: str,
        data: bytes,
        replica: ShardReplica,
        storage_volume: Any,
        start: float,
    ) -> SyncResult:
        """本地 StorageVolume 写入。"""
        try:
            if storage_volume is not None:
                ok = storage_volume.write_file(
                    replica.volume_name,
                    replica.file_path,
                    data,
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
            logger.info(f"分片本地同步: {shard_id} → {target_node_id} ({len(data)} bytes, {duration:.1f}ms)")

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
            logger.error(f"分片本地同步异常: {shard_id} → {target_node_id}: {e}")
            return SyncResult(
                shard_id=shard_id,
                target_node_id=target_node_id,
                success=False,
                duration_ms=duration,
                error=str(e),
            )

    def set_fmp_interface(self, fmp_interface: Any, local_node_id: str) -> None:
        """设置 FMPInterface 和本地节点 ID，启用跨节点传输。"""
        self._fmp_interface = fmp_interface
        self._local_node_id = local_node_id
        logger.info(f"ShardReplicator FMP 传输已启用: local_node={local_node_id}")

    def sync_all_replicas(self, shard_id: str, storage_volume: Any = None) -> list[SyncResult]:
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

    def get_replicas(self, shard_id: str) -> list[ShardReplica]:
        return self._replicas.get(shard_id, [])

    def get_healthy_replica(self, shard_id: str) -> ShardReplica | None:
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

    def remove_node_replicas(self, node_id: str) -> list[str]:
        affected = []
        for shard_id, replicas in self._replicas.items():
            for replica in replicas:
                if replica.node_id == node_id:
                    replica.status = "failed"
                    affected.append(shard_id)
        return affected

    def get_under_replicated(self) -> list[str]:
        result = []
        for shard_id, replicas in self._replicas.items():
            active = sum(1 for r in replicas if r.status == "active")
            if active < self.config.replication_factor:
                result.append(shard_id)
        return result

    # ── M9-02 Quorum 读/写 ──

    def quorum_write(self, shard_id: str, data: bytes, storage_volume: Any = None) -> dict[str, Any]:
        """M9-02 Quorum 写入：写入多数副本成功即视为写入成功。

        写入 ⌈N/2⌉ 个副本即返回成功，其余异步补齐。
        """
        self.register_shard_data(shard_id, data)
        replicas = self._replicas.get(shard_id, [])
        if not replicas:
            logger.warning(f"Quorum 写入跳过：分片 {shard_id} 无副本")
            return {"shard_id": shard_id, "success": False, "error": "no_replicas"}

        quorum = (len(replicas) + 1) // 2
        results = []
        for replica in replicas:
            if replica.status in ("active", "recovering"):
                r = self.sync_to_node(shard_id, replica.node_id, storage_volume)
                results.append(r)
            if sum(1 for r in results if r.success) >= quorum:
                break

        ok_count = sum(1 for r in results if r.success)
        success = ok_count >= quorum
        logger.info(f"M9-02 Quorum 写入: {shard_id} 成功={ok_count}/{len(replicas)} quorum={quorum} result={success}")
        return {
            "shard_id": shard_id,
            "success": success,
            "ok_count": ok_count,
            "total": len(replicas),
            "quorum": quorum,
        }

    def quorum_read(self, shard_id: str, storage_volume: Any = None) -> dict[str, Any]:
        """M9-02 Quorum 读取：从多数副本读取并校验一致性。

        读取 ⌈N/2⌉ 个副本，校验 checksum 一致后返回数据。
        """
        replicas = self._replicas.get(shard_id, [])
        if not replicas:
            return {"shard_id": shard_id, "success": False, "error": "no_replicas"}

        quorum = (len(replicas) + 1) // 2
        reads: dict[str, bytes] = {}
        checksums: dict[str, str] = {}
        for replica in replicas:
            if replica.status != "active":
                continue
            if storage_volume is None:
                cached = self._shard_data.get(shard_id)
                if cached is not None:
                    reads[replica.node_id] = cached
                    checksums[replica.node_id] = hashlib.sha256(cached).hexdigest()
            else:
                try:
                    data = storage_volume.read_file(replica.volume_name, replica.file_path)
                    if data is not None:
                        reads[replica.node_id] = data
                        checksums[replica.node_id] = hashlib.sha256(data).hexdigest()
                except Exception as e:
                    logger.debug(f"Quorum 读取跳过节点: {replica.node_id}: {e}")
            if len(reads) >= quorum:
                break

        if len(reads) < quorum:
            logger.warning(f"M9-02 Quorum 读取不足: {shard_id} 读到={len(reads)} quorum={quorum}")
            return {"shard_id": shard_id, "success": False, "error": "quorum_not_met"}

        first_cksum = next(iter(checksums.values()))
        consistent = all(c == first_cksum for c in checksums.values())
        if not consistent:
            logger.error(f"M9-02 Quorum 读取不一致: {shard_id}")
            return {"shard_id": shard_id, "success": False, "error": "inconsistent"}

        data = next(iter(reads.values()))
        logger.info(f"M9-02 Quorum 读取: {shard_id} 成功 读取={len(reads)} quorum={quorum}")
        return {
            "shard_id": shard_id,
            "success": True,
            "data": data,
            "read_count": len(reads),
        }

    def get_stats(self) -> dict[str, Any]:
        total = sum(len(r) for r in self._replicas.values())
        active = sum(sum(1 for r in rlist if r.status == "active") for rlist in self._replicas.values())
        return {
            "total_shards": len(self._replicas),
            "total_replicas": total,
            "active_replicas": active,
            "under_replicated": len(self.get_under_replicated()),
            "cached_shard_data": len(self._shard_data),
        }

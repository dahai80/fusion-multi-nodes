"""M9-01 分布式 KV Store — 内存 + 磁盘持久化的键值存储。

支持：
- PUT/GET/DELETE 基本操作
- 分区 (partition) 隔离
- TTL 自动过期
- 快照/恢复 (M9-03)
- 与 ShardReplicator 集成实现多副本
- M9-04 FMP 跨节点读写
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class KVEntry:
    """KV 条目。"""
    key: str
    value: Any
    partition: str = "default"
    created_at: float = 0.0
    updated_at: float = 0.0
    ttl_seconds: float = 0.0
    version: int = 1

    @property
    def is_expired(self) -> bool:
        if self.ttl_seconds <= 0:
            return False
        return time.time() - self.updated_at > self.ttl_seconds


@dataclass
class KVSnapEntry:
    """快照条目。"""
    key: str
    value: Any
    partition: str
    ttl_seconds: float
    version: int


class DistributedKVStore:
    """分布式 KV Store — 内存 + 磁盘持久化。"""

    def __init__(
        self,
        data_dir: str = "",
        auto_save_interval: float = 60.0,
        max_entries: int = 100000,
    ):
        self._data_dir = Path(data_dir) if data_dir else Path.home() / ".fusion" / "multi-node" / "kv"
        self._auto_save_interval = auto_save_interval
        self._max_entries = max_entries
        self._store: Dict[str, KVEntry] = {}
        self._partitions: Dict[str, Set[str]] = {}
        self._dirty = False
        self._fmp_interface: Any = None
        self._local_node_id: Optional[str] = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        logger.info(f"DistributedKVStore 初始化: dir={self._data_dir}")

    def put(
        self,
        key: str,
        value: Any,
        partition: str = "default",
        ttl_seconds: float = 0.0,
    ) -> KVEntry:
        now = time.time()
        existing = self._store.get(key)
        version = existing.version + 1 if existing else 1
        entry = KVEntry(
            key=key,
            value=value,
            partition=partition,
            created_at=existing.created_at if existing else now,
            updated_at=now,
            ttl_seconds=ttl_seconds,
            version=version,
        )
        self._store[key] = entry
        if partition not in self._partitions:
            self._partitions[partition] = set()
        self._partitions[partition].add(key)
        self._dirty = True
        logger.debug(f"KV PUT: {key} partition={partition} v{version}")
        return entry

    def get(self, key: str, default: Any = None) -> Any:
        entry = self._store.get(key)
        if not entry:
            return default
        if entry.is_expired:
            self.delete(key)
            return default
        return entry.value

    def get_entry(self, key: str) -> Optional[KVEntry]:
        entry = self._store.get(key)
        if entry and entry.is_expired:
            self.delete(key)
            return None
        return entry

    def delete(self, key: str) -> bool:
        entry = self._store.pop(key, None)
        if entry:
            part = self._partitions.get(entry.partition)
            if part:
                part.discard(key)
                if not part:
                    del self._partitions[entry.partition]
            self._dirty = True
            logger.debug(f"KV DELETE: {key}")
            return True
        return False

    def list_keys(self, partition: Optional[str] = None, prefix: str = "") -> List[str]:
        self._cleanup_expired()
        if partition:
            keys = self._partitions.get(partition, set())
        else:
            keys = set(self._store.keys())
        if prefix:
            keys = {k for k in keys if k.startswith(prefix)}
        return sorted(keys)

    def list_partition(self, partition: str) -> Dict[str, Any]:
        self._cleanup_expired()
        keys = self._partitions.get(partition, set())
        return {k: self._store[k].value for k in keys if k in self._store and not self._store[k].is_expired}

    def size(self) -> int:
        return len(self._store)

    def partition_count(self) -> int:
        return len(self._partitions)

    def _cleanup_expired(self) -> int:
        expired = [k for k, v in self._store.items() if v.is_expired]
        for k in expired:
            self.delete(k)
        if expired:
            logger.info(f"KV 清理过期条目: {len(expired)} 个")
        return len(expired)

    # ── M9-03 快照/恢复 ──

    def snapshot(self, path: Optional[str] = None) -> str:
        snap_path = Path(path) if path else self._data_dir / f"kv_snapshot_{int(time.time())}.json"
        snap_path.parent.mkdir(parents=True, exist_ok=True)
        self._cleanup_expired()
        entries = []
        for entry in self._store.values():
            entries.append({
                "key": entry.key,
                "value": entry.value,
                "partition": entry.partition,
                "ttl_seconds": entry.ttl_seconds,
                "version": entry.version,
            })
        data = {
            "snapshot_at": time.time(),
            "entry_count": len(entries),
            "entries": entries,
        }
        snap_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"M9-03 KV 快照保存: {snap_path} ({len(entries)} 条)")
        return str(snap_path)

    def restore(self, path: str, merge: bool = False) -> int:
        snap_path = Path(path)
        if not snap_path.exists():
            logger.error(f"快照文件不存在: {path}")
            return 0
        data = json.loads(snap_path.read_text(encoding="utf-8"))
        entries = data.get("entries", [])
        restored = 0
        for item in entries:
            key = item["key"]
            if not merge and key in self._store:
                continue
            self.put(
                key=key,
                value=item["value"],
                partition=item.get("partition", "default"),
                ttl_seconds=item.get("ttl_seconds", 0.0),
            )
            if key in self._store:
                self._store[key].version = item.get("version", 1)
            restored += 1
        logger.info(f"M9-03 KV 快照恢复: {path} ({restored}/{len(entries)} 条, merge={merge})")
        return restored

    # ── 持久化 ──

    def save(self, path: Optional[str] = None) -> bool:
        save_path = Path(path) if path else self._data_dir / "kv_store.json"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.snapshot(str(save_path))
            self._dirty = False
            return True
        except Exception as e:
            logger.error(f"KV 持久化失败: {e}")
            return False

    def load(self, path: Optional[str] = None) -> int:
        load_path = Path(path) if path else self._data_dir / "kv_store.json"
        if not load_path.exists():
            return 0
        try:
            return self.restore(str(load_path), merge=False)
        except Exception as e:
            logger.error(f"KV 加载失败: {e}")
            return 0

    # ── M9-04 FMP 跨节点读写 ──

    def set_fmp_interface(self, fmp_interface: Any, local_node_id: str) -> None:
        """设置 FMPInterface，启用跨节点 KV 操作。"""
        self._fmp_interface = fmp_interface
        self._local_node_id = local_node_id
        logger.info(f"DistributedKVStore FMP 已启用: local_node={local_node_id}")

    async def get_remote(self, key: str, node_id: str, partition: str = "default", timeout: float = 5.0) -> Any:
        """M9-04 通过 FMP 从远程节点读取 KV 值。"""
        if not self._fmp_interface:
            logger.error("FMP 未设置，无法远程读取")
            return None
        from fusion_multi_node.protocol.fmp_message import FMPMessage, PayloadType

        request_id = f"kv_get_{uuid.uuid4().hex[:8]}"
        msg = FMPMessage.create(
            source_id=self._local_node_id,
            target_id=node_id,
            payload_type=PayloadType.KV_GET,
            payload={"key": key, "partition": partition, "request_id": request_id},
        )

        try:
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._pending_requests[request_id] = future

            await self._fmp_interface._conn_mgr.send_to(node_id, msg)

            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            logger.warning(f"M9-04 KV 远程读取超时: {key}@{node_id}")
            return None
        except Exception as e:
            logger.error(f"M9-04 KV 远程读取异常: {key}@{node_id}: {e}")
            return None
        finally:
            self._pending_requests.pop(request_id, None)

    async def put_remote(
        self,
        key: str,
        value: Any,
        node_id: str,
        partition: str = "default",
        ttl_seconds: float = 0.0,
        timeout: float = 5.0,
    ) -> bool:
        """M9-04 通过 FMP 向远程节点写入 KV 值。"""
        if not self._fmp_interface:
            logger.error("FMP 未设置，无法远程写入")
            return False
        from fusion_multi_node.protocol.fmp_message import FMPMessage, PayloadType

        request_id = f"kv_put_{uuid.uuid4().hex[:8]}"
        msg = FMPMessage.create(
            source_id=self._local_node_id,
            target_id=node_id,
            payload_type=PayloadType.KV_PUT,
            payload={
                "key": key,
                "value": value,
                "partition": partition,
                "ttl": ttl_seconds,
                "request_id": request_id,
            },
        )

        try:
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._pending_requests[request_id] = future

            await self._fmp_interface._conn_mgr.send_to(node_id, msg)

            result = await asyncio.wait_for(future, timeout=timeout)
            return bool(result)
        except asyncio.TimeoutError:
            logger.warning(f"M9-04 KV 远程写入超时: {key}@{node_id}")
            return False
        except Exception as e:
            logger.error(f"M9-04 KV 远程写入异常: {key}@{node_id}: {e}")
            return False
        finally:
            self._pending_requests.pop(request_id, None)

    def handle_kv_response(self, msg: Any) -> None:
        """处理 KV_GET_RESP / KV_PUT_ACK 响应 — 匹配 pending request 并 resolve future。"""
        try:
            payload = msg.business.payload_as_json()
            request_id = payload.get("request_id", "")
            future = self._pending_requests.pop(request_id, None)
            if future and not future.done():
                if msg.business.payload_type.value == "kv_get_resp":
                    future.set_result(payload.get("value") if payload.get("found") else None)
                elif msg.business.payload_type.value == "kv_put_ack":
                    future.set_result(payload.get("success", False))
        except Exception as e:
            logger.error(f"KV 响应处理异常: {e}")

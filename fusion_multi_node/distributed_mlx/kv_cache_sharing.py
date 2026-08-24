"""Enhanced KV Cache Sharing — 跨节点 KV 缓存共享与复用。

解决分布式推理中的显存瓶颈：
1. 跨节点 KV 缓存读写：节点间共享 KV 缓存，避免重复计算
2. 缓存预热：预加载高频 prompt 的 KV 缓存到多节点
3. 缓存淘汰策略：LRU + 分片大小加权
4. 缓存压缩：使用 Caveman 压缩减少传输量
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

import httpx

from fusion_multi_node.protocol import KVCacheSyncMessage
from fusion_multi_node.utils.auth import sanitize_node_url_part

logger = logging.getLogger(__name__)

KV_SYNC_PROTOCOL = "fmp"

ALLOWED_SHARD_KEYS = {
    "shard_id",
    "model_name",
    "layer_index",
    "node_id",
    "token_count",
    "size_bytes",
    "created_at",
    "access_count",
    "last_access",
    "is_compressed",
}


@dataclass
class KVShard:
    """KV 缓存分片。"""

    shard_id: str
    model_name: str
    layer_index: int
    node_id: str
    token_count: int
    size_bytes: int
    created_at: float
    access_count: int = 0
    last_access: float = 0.0
    is_compressed: bool = False


@dataclass
class KVCacheEntry:
    """KV 缓存条目。"""

    cache_id: str
    model_name: str
    prompt_hash: str
    prompt_prefix: str
    shards: list[KVShard] = field(default_factory=list)
    total_tokens: int = 0
    total_size_bytes: int = 0
    created_at: float = 0.0
    ttl_seconds: float = 3600.0
    access_count: int = 0

    @property
    def is_expired(self) -> bool:
        return time.time() - self.created_at > self.ttl_seconds


class KVSharingManager:
    """KV 缓存共享管理器 — 跨节点 KV 缓存复用。

    支持：
    - 本地缓存管理（LRU 淘汰）
    - 远程缓存查询（通过 Node Agent）
    - 缓存预热（预加载高频 prompt）
    - 缓存压缩（Caveman）
    """

    def __init__(
        self,
        max_local_cache_mb: float = 4096.0,
        max_remote_lookup_ms: float = 50.0,
        enable_compression: bool = True,
    ):
        self.max_local_cache_mb = max_local_cache_mb
        self.max_remote_lookup_ms = max_remote_lookup_ms
        self.enable_compression = enable_compression

        # 本地缓存
        self._local_cache: OrderedDict[str, KVCacheEntry] = OrderedDict()
        self._local_size_bytes: int = 0
        # H5: 无锁设计。KVSharingManager 仅被 agent_server (asyncio 单事件循环) 调用,
        # 无 to_thread/executor 跨线程入口; 本地缓存读改写均纯同步无 await,
        # 单线程协作调度不会在 sync 段中段切换 → OrderedDict 读改写天然原子。
        self._lookup_index: dict[tuple[str, str], str] = {}

        # 远程节点 KV 缓存索引
        self._remote_cache_index: dict[str, list[KVCacheEntry]] = {}

        # 预热缓存
        self._warm_cache: dict[str, KVCacheEntry] = {}

        # 压缩器
        self._compressor = None
        if enable_compression:
            from .caveman_compress import CavemanCompressor

            self._compressor = CavemanCompressor()

        # 复用 HTTP 客户端
        self._http_client: httpx.AsyncClient | None = None

    async def _get_http_client(self, timeout: float = 30.0) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=timeout)
        return self._http_client

    async def close(self) -> None:
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    # ── 本地缓存管理 ──

    def store_local(self, entry: KVCacheEntry) -> bool:
        """存储本地 KV 缓存。"""
        max_bytes = self.max_local_cache_mb * 1024 * 1024
        if entry.total_size_bytes > max_bytes:
            logger.warning(
                f"KV 缓存条目过大: {entry.total_size_bytes / 1024:.1f}KB > 容量 {max_bytes / 1024:.1f}KB，拒绝存储"
            )
            return False
        if self._local_size_bytes + entry.total_size_bytes > max_bytes:
            self._evict(entry.total_size_bytes)
        self._local_cache[entry.cache_id] = entry
        self._local_size_bytes += entry.total_size_bytes
        self._lookup_index[(entry.model_name, entry.prompt_hash)] = entry.cache_id
        logger.debug(
            f"KV 缓存存储: {entry.model_name} ({entry.total_tokens} tokens, {entry.total_size_bytes / 1024:.1f}KB)"
        )
        return True

    def lookup_local(self, model_name: str, prompt_hash: str) -> KVCacheEntry | None:
        """查询本地 KV 缓存。"""
        cache_id = self._lookup_index.get((model_name, prompt_hash))
        if not cache_id:
            return None
        entry = self._local_cache.get(cache_id)
        if not entry:
            self._lookup_index.pop((model_name, prompt_hash), None)
            return None
        if entry.is_expired:
            self._local_cache.pop(entry.cache_id, None)
            self._local_size_bytes -= entry.total_size_bytes
            self._lookup_index.pop((model_name, prompt_hash), None)
            return None
        entry.access_count += 1
        if entry.shards:
            entry.shards[-1].last_access = time.time()
        self._local_cache.move_to_end(entry.cache_id)
        return entry

    def lookup_prefix(self, model_name: str, prefix: str) -> list[KVCacheEntry]:
        """按前缀匹配查询 KV 缓存（用于缓存复用）。"""
        matches = []
        for entry in self._local_cache.values():
            if entry.model_name == model_name and entry.prompt_prefix.startswith(prefix) and not entry.is_expired:
                matches.append(entry)
        return matches

    def _evict(self, needed_bytes: int) -> None:
        """LRU 淘汰缓存 (单线程, 无需显式锁)。"""
        max_bytes = self.max_local_cache_mb * 1024 * 1024
        if needed_bytes > max_bytes:
            return
        while self._local_cache and self._local_size_bytes + needed_bytes > max_bytes:
            cache_id, entry = self._local_cache.popitem(last=False)
            self._local_size_bytes -= entry.total_size_bytes
            self._lookup_index.pop((entry.model_name, entry.prompt_hash), None)
            logger.debug(f"KV 缓存淘汰: {cache_id} ({entry.total_size_bytes / 1024:.1f}KB)")

    # ── 远程缓存查询 ──

    async def lookup_remote(
        self,
        model_name: str,
        prompt_hash: str,
        nodes: list[str],
    ) -> tuple[KVCacheEntry, str] | None:
        """查询远程节点 KV 缓存。"""
        client = await self._get_http_client(self.max_remote_lookup_ms / 1000)

        for node_id in nodes:
            try:
                safe_node = sanitize_node_url_part(node_id)
                resp = await client.post(
                    f"http://{safe_node}:11445/api/kv/lookup",
                    json={
                        "model_name": model_name,
                        "prompt_hash": prompt_hash,
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("found"):
                        entry = self._deserialize_entry(data["entry"])
                        return entry, node_id
            except Exception as e:
                logger.debug(f"远程 KV 查询失败 {node_id}: {e}")

        return None

    async def transfer_from_remote(
        self,
        cache_id: str,
        source_node: str,
        target_node: str,
    ) -> bool:
        """跨节点传输 KV 缓存。"""
        try:
            client = await self._get_http_client(30.0)
            safe_source = sanitize_node_url_part(source_node)
            resp = await client.post(
                f"http://{safe_source}:11445/api/kv/transfer",
                json={
                    "cache_id": cache_id,
                    "target_node": target_node,
                    "compress": self.enable_compression,
                    "protocol": KV_SYNC_PROTOCOL,
                },
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"KV 传输失败 {source_node} → {target_node}: {e}")
            return False

    def sync_to_cluster(self, cache_id: str, model_name: str, source_node_id: str) -> KVCacheSyncMessage:
        """生成 FMP 协议 KV 缓存同步消息。"""
        entry = self._local_cache.get(cache_id)
        if not entry:
            logger.warning(f"KV 缓存同步: cache_id={cache_id} 未找到本地缓存")
            size_mb = 0.0
        else:
            size_mb = entry.total_size_bytes / (1024 * 1024)

        sync_msg = KVCacheSyncMessage(
            cache_id=cache_id,
            model_name=model_name,
            source_node_id=source_node_id,
            size_mb=size_mb,
            protocol=KV_SYNC_PROTOCOL,
        )
        logger.info(
            f"M9-04 FMP KV 缓存同步消息: cache_id={cache_id} "
            f"model={model_name} source={source_node_id} "
            f"size={size_mb:.1f}MB protocol={sync_msg.protocol}"
        )
        return sync_msg

    # ── 缓存预热 ──

    async def warm_cache(
        self,
        model_name: str,
        prompts: list[str],
        nodes: list[str],
    ) -> dict[str, Any]:
        """预加载高频 prompt 的 KV 缓存到多节点。"""
        results = {"success": 0, "failed": 0, "details": []}
        client = await self._get_http_client(60.0)

        for prompt in prompts:
            prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]
            for node_id in nodes:
                try:
                    safe_node = sanitize_node_url_part(node_id)
                    resp = await client.post(
                        f"http://{safe_node}:11445/api/kv/warm",
                        json={
                            "model_name": model_name,
                            "prompt": prompt,
                            "prompt_hash": prompt_hash,
                        },
                    )
                    if resp.status_code == 200:
                        results["success"] += 1
                        results["details"].append(
                            {
                                "node": node_id,
                                "prompt": prompt[:50],
                                "status": "ok",
                            }
                        )
                    else:
                        results["failed"] += 1
                except Exception as e:
                    results["failed"] += 1
                    logger.warning(f"缓存预热失败 {node_id}: {e}")

        logger.info(f"KV 缓存预热: {results['success']} 成功, {results['failed']} 失败")
        return results

    # ── 缓存统计 ──

    def get_stats(self) -> dict[str, Any]:
        """获取 KV 缓存统计。"""
        total_shards = sum(len(e.shards) for e in self._local_cache.values())
        return {
            "local_entries": len(self._local_cache),
            "local_size_mb": round(self._local_size_bytes / (1024 * 1024), 1),
            "local_max_mb": self.max_local_cache_mb,
            "total_shards": total_shards,
            "remote_indexed_nodes": len(self._remote_cache_index),
            "warm_cache_entries": len(self._warm_cache),
            "compression_enabled": self.enable_compression,
        }

    def _deserialize_entry(self, data: dict) -> KVCacheEntry:
        shards = []
        for s in data.get("shards", []):
            filtered = {k: v for k, v in s.items() if k in ALLOWED_SHARD_KEYS}
            shards.append(KVShard(**filtered))
        return KVCacheEntry(
            cache_id=data["cache_id"],
            model_name=data["model_name"],
            prompt_hash=data["prompt_hash"],
            prompt_prefix=data.get("prompt_prefix", ""),
            shards=shards,
            total_tokens=data.get("total_tokens", 0),
            total_size_bytes=data.get("total_size_bytes", 0),
            created_at=data.get("created_at", time.time()),
            ttl_seconds=data.get("ttl_seconds", 3600.0),
            access_count=data.get("access_count", 0),
        )


class KVCacheWarmScheduler:
    """KV 缓存预热调度器 — 自动预热高频 prompt。"""

    def __init__(self, manager: KVSharingManager):
        self.manager = manager
        self._hot_prompts: dict[str, int] = {}
        self._max_hot_prompts = 1000
        self._running = False

    def record_prompt(self, prompt: str) -> None:
        """记录 prompt 使用频率。"""
        key = prompt[:100]
        self._hot_prompts[key] = self._hot_prompts.get(key, 0) + 1
        if len(self._hot_prompts) > self._max_hot_prompts:
            sorted_items = sorted(self._hot_prompts.items(), key=lambda x: x[1])
            self._hot_prompts = dict(sorted_items[len(sorted_items) // 2 :])

    def get_hot_prompts(self, threshold: int = 3, max_count: int = 10) -> list[str]:
        """获取高频 prompt 列表。"""
        sorted_prompts = sorted(
            self._hot_prompts.items(),
            key=lambda x: -x[1],
        )
        return [p for p, c in sorted_prompts if c >= threshold][:max_count]

    async def start(self, interval: int = 300, nodes: list[str] | None = None) -> None:
        """启动预热调度。"""
        self._running = True
        while self._running:
            await asyncio.sleep(interval)
            hot = self.get_hot_prompts()
            if hot and nodes:
                await self.manager.warm_cache("default", hot, nodes)
                logger.info(f"自动预热: {len(hot)} 个 prompt")

    def stop(self) -> None:
        self._running = False

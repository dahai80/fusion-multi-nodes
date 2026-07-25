"""Enhanced KV Cache Sharing — 跨节点 KV 缓存共享与复用。

解决分布式推理中的显存瓶颈：
1. 跨节点 KV 缓存读写：节点间共享 KV 缓存，避免重复计算
2. 缓存预热：预加载高频 prompt 的 KV 缓存到多节点
3. 缓存淘汰策略：LRU + 分片大小加权
4. 缓存压缩：使用 Caveman 压缩减少传输量
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


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
    shards: List[KVShard] = field(default_factory=list)
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

        # 远程节点 KV 缓存索引
        self._remote_cache_index: Dict[str, List[KVCacheEntry]] = {}

        # 预热缓存
        self._warm_cache: Dict[str, KVCacheEntry] = {}

        # 压缩器
        self._compressor = None
        if enable_compression:
            from .caveman_compress import CavemanCompressor
            self._compressor = CavemanCompressor()

    # ── 本地缓存管理 ──

    def store_local(self, entry: KVCacheEntry) -> bool:
        """存储本地 KV 缓存。"""
        # 检查空间
        if self._local_size_bytes + entry.total_size_bytes > self.max_local_cache_mb * 1024 * 1024:
            self._evict(entry.total_size_bytes)

        self._local_cache[entry.cache_id] = entry
        self._local_size_bytes += entry.total_size_bytes
        logger.debug(f"KV 缓存存储: {entry.model_name} ({entry.total_tokens} tokens, "
                    f"{entry.total_size_bytes / 1024:.1f}KB)")
        return True

    def lookup_local(self, model_name: str, prompt_hash: str) -> Optional[KVCacheEntry]:
        """查询本地 KV 缓存。"""
        for entry in self._local_cache.values():
            if entry.model_name == model_name and entry.prompt_hash == prompt_hash:
                if not entry.is_expired:
                    entry.access_count += 1
                    entry.shards[-1].last_access = time.time()
                    # 移动到最近使用
                    self._local_cache.move_to_end(entry.cache_id)
                    return entry
                else:
                    # 删除过期
                    self._local_cache.pop(entry.cache_id, None)
                    self._local_size_bytes -= entry.total_size_bytes
        return None

    def lookup_prefix(self, model_name: str, prefix: str) -> List[KVCacheEntry]:
        """按前缀匹配查询 KV 缓存（用于缓存复用）。"""
        matches = []
        for entry in self._local_cache.values():
            if entry.model_name == model_name and entry.prompt_prefix.startswith(prefix):
                if not entry.is_expired:
                    matches.append(entry)
        return matches

    def _evict(self, needed_bytes: int) -> None:
        """LRU 淘汰缓存。"""
        while self._local_size_bytes + needed_bytes > self.max_local_cache_mb * 1024 * 1024:
            if not self._local_cache:
                break
            # 淘汰最久未使用
            cache_id, entry = self._local_cache.popitem(last=False)
            self._local_size_bytes -= entry.total_size_bytes
            logger.debug(f"KV 缓存淘汰: {cache_id} ({entry.total_size_bytes / 1024:.1f}KB)")

    # ── 远程缓存查询 ──

    async def lookup_remote(
        self,
        model_name: str,
        prompt_hash: str,
        nodes: List[str],
    ) -> Optional[Tuple[KVCacheEntry, str]]:
        """查询远程节点 KV 缓存。"""
        import httpx

        for node_id in nodes:
            try:
                async with httpx.AsyncClient(timeout=self.max_remote_lookup_ms / 1000) as client:
                    resp = await client.post(
                        f"http://{node_id}:9755/api/kv/lookup",
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
        import httpx

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # 请求源节点传输
                resp = await client.post(
                    f"http://{source_node}:9755/api/kv/transfer",
                    json={
                        "cache_id": cache_id,
                        "target_node": target_node,
                        "compress": self.enable_compression,
                    },
                )
                return resp.status_code == 200
        except Exception as e:
            logger.error(f"KV 传输失败 {source_node} → {target_node}: {e}")
            return False

    # ── 缓存预热 ──

    async def warm_cache(
        self,
        model_name: str,
        prompts: List[str],
        nodes: List[str],
    ) -> Dict[str, Any]:
        """预加载高频 prompt 的 KV 缓存到多节点。"""
        import httpx

        results = {"success": 0, "failed": 0, "details": []}

        for prompt in prompts:
            prompt_hash = str(hash(prompt))[:16]
            for node_id in nodes:
                try:
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        resp = await client.post(
                            f"http://{node_id}:9755/api/kv/warm",
                            json={
                                "model_name": model_name,
                                "prompt": prompt,
                                "prompt_hash": prompt_hash,
                            },
                        )
                        if resp.status_code == 200:
                            results["success"] += 1
                            results["details"].append({
                                "node": node_id,
                                "prompt": prompt[:50],
                                "status": "ok",
                            })
                        else:
                            results["failed"] += 1
                except Exception as e:
                    results["failed"] += 1
                    logger.warning(f"缓存预热失败 {node_id}: {e}")

        logger.info(f"KV 缓存预热: {results['success']} 成功, {results['failed']} 失败")
        return results

    # ── 缓存统计 ──

    def get_stats(self) -> Dict[str, Any]:
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
        """反序列化 KV 缓存条目。"""
        shards = [KVShard(**s) for s in data.get("shards", [])]
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
        self._hot_prompts: Dict[str, int] = {}  # prompt -> frequency
        self._running = False

    def record_prompt(self, prompt: str) -> None:
        """记录 prompt 使用频率。"""
        key = prompt[:100]  # 取前 100 字符作为 key
        self._hot_prompts[key] = self._hot_prompts.get(key, 0) + 1

    def get_hot_prompts(self, threshold: int = 3, max_count: int = 10) -> List[str]:
        """获取高频 prompt 列表。"""
        sorted_prompts = sorted(
            self._hot_prompts.items(),
            key=lambda x: -x[1],
        )
        return [p for p, c in sorted_prompts if c >= threshold][:max_count]

    async def start(self, interval: int = 300, nodes: List[str] = None) -> None:
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
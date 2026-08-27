"""Enhanced KV Cache Sharing — 跨节点 KV 缓存共享与复用。

解决分布式推理中的显存瓶颈：
1. 跨节点 KV 缓存读写：节点间共享 KV 缓存，避免重复计算
2. 缓存预热：预加载高频 prompt 的 KV 缓存到多节点
3. 缓存淘汰策略：LRU + 分片大小加权
4. 缓存压缩：使用 Caveman 压缩减少传输量
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from fusion_multi_node.protocol import KVCacheSyncMessage
from fusion_multi_node.security.mtls import client_kwargs as mtls_client_kwargs
from fusion_multi_node.security.mtls import scheme as mtls_scheme
from fusion_multi_node.utils.auth import is_safe_outbound_host, sanitize_node_url_part

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

# GAP-7 (#33): KVShard.tensor 序列化键 — 张量负载 base64 (Caveman 压缩后)。
# ALLOWED_SHARD_KEYS 仅含元数据, tensor 单独经 _serialize_entry/_deserialize_entry 处理
# (base64 编码 + 压缩标记), 不进 ALLOWED_SHARD_KEYS 白名单 (避免 getattr 直传 bytes)。
TENSOR_KEY = "tensor"
COMPRESS_METHOD_KEY = "tensor_compress"


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
    # GAP-7 (#33): 张量负载 — 跨节点 KV 传输的真实张量字节。
    # 默认 None (纯元数据分片, 向后兼容); 合成/真张量后端填入 (Caveman 压缩后裸 bytes)。
    # 序列化时经 base64 编码进 JSON, 反序列化时 base64 解码回 bytes。
    tensor: bytes | None = None


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
        cluster_token: str = "",
        persist_path: str | None = None,
        transport: Any = None,
    ):
        self.max_local_cache_mb = max_local_cache_mb
        self.max_remote_lookup_ms = max_remote_lookup_ms
        self.enable_compression = enable_compression
        # 跨节点 KV HTTP 调用 (lookup/transfer/warm) 需过对端 BearerAuthMiddleware。
        # 缺 token → 全部 401 (生产 agent 默认鉴权)。由 AgentServer 透传集群共享 token。
        self._cluster_token = cluster_token
        # GAP-7 (#33): KV 张量传输后端 — 合成默认 / MLX env-gated。ctor 注入 (测试可换 stub)。
        if transport is None:
            from .kv_tensor_transport import get_kv_transport

            transport = get_kv_transport()
        self._transport = transport

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

        # P1-9: 磁盘持久化 — agent 重启可恢复/预热本地 KV 缓存 (审计 §6.3)。
        # 全内存 OrderedDict 重启即失。save() 落盘 JSON, load() 启动恢复。
        # 默认 ~/.fusion/multi-node/kv_cache.json (与 tasks.json/election_state.json 同域)。
        self._persist_path: Path = (
            Path(persist_path) if persist_path else Path.home() / ".fusion" / "multi-node" / "kv_cache.json"
        )
        self._dirty: bool = False

    async def _get_http_client(self, timeout: float = 30.0) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=timeout, **mtls_client_kwargs())
        return self._http_client

    def _auth_headers(self) -> dict[str, str]:
        """跨节点 KV HTTP 调用鉴权头 — Bearer 集群共享 token。"""
        if self._cluster_token:
            return {"Authorization": f"Bearer {self._cluster_token}"}
        return {}

    async def close(self) -> None:
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None
        # GAP-7 (#33): 关张量传输后端 (MLX 后端持有 httpx 客户端)。
        if self._transport is not None:
            try:
                await self._transport.close()
            except Exception as e:
                logger.debug(f"KV 张量后端关闭失败 (忽略): {e}")

    # ── P1-9 磁盘持久化 ──

    def save(self, path: str | None = None) -> bool:
        """落盘本地 KV 缓存 (审计 §6.3)。原子写: tmp + os.replace。跳过已过期条目。"""
        save_path = Path(path) if path else self._persist_path
        try:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            entries = []
            for entry in self._local_cache.values():
                if entry.is_expired:
                    continue
                entries.append(self._serialize_entry(entry))
            data = {
                "saved_at": time.time(),
                "entry_count": len(entries),
                "entries": entries,
            }
            tmp = save_path.with_suffix(save_path.suffix + ".tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, save_path)
            self._dirty = False
            logger.info(f"P1-9 KV 缓存落盘: {save_path} ({len(entries)} 条)")
            return True
        except Exception as e:
            logger.error(f"P1-9 KV 缓存落盘失败: {e}")
            return False

    def load(self, path: str | None = None) -> int:
        """启动恢复本地 KV 缓存 (审计 §6.3)。跳过已过期条目, 不覆盖已存在 cache_id。"""
        load_path = Path(path) if path else self._persist_path
        if not load_path.exists():
            logger.info(f"P1-9 KV 缓存恢复: 文件不存在 {load_path}, 跳过")
            return 0
        try:
            data = json.loads(load_path.read_text(encoding="utf-8"))
            entries = data.get("entries", [])
            restored = 0
            for item in entries:
                if item.get("cache_id") in self._local_cache:
                    continue
                entry = self._deserialize_entry(item)
                if entry.is_expired:
                    continue
                if self.store_local(entry):
                    restored += 1
            self._dirty = False
            logger.info(f"P1-9 KV 缓存恢复: {load_path} ({restored}/{len(entries)} 条)")
            return restored
        except Exception as e:
            logger.error(f"P1-9 KV 缓存恢复失败: {e}")
            return 0

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
        self._dirty = True
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
            self._dirty = True
            return None
        entry.access_count += 1
        if entry.shards:
            entry.shards[-1].last_access = time.time()
        self._local_cache.move_to_end(entry.cache_id)
        return entry

    def lookup_local_by_id(self, cache_id: str) -> KVCacheEntry | None:
        """按 cache_id 查本地 KV 缓存 (跨节点传输用)。"""
        entry = self._local_cache.get(cache_id)
        if entry is None:
            return None
        if entry.is_expired:
            self._local_cache.pop(entry.cache_id, None)
            self._local_size_bytes -= entry.total_size_bytes
            self._lookup_index.pop((entry.model_name, entry.prompt_hash), None)
            return None
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
            self._dirty = True
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
                # H3 (AR #24): 出站 SSRF 守卫 — 挡云元数据/链路本地等恶意 host
                if not is_safe_outbound_host(safe_node):
                    logger.warning(f"远程 KV 查询跳过非安全对端: {node_id!r}")
                    continue
                resp = await client.post(
                    f"{mtls_scheme()}://{safe_node}:11458/api/kv/lookup",
                    json={
                        "model_name": model_name,
                        "prompt_hash": prompt_hash,
                    },
                    headers=self._auth_headers(),
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
        """从 source_node 拉取 KV 缓存并本地存储 (target = 本节点)。

        推模型: POST source_node /api/kv/transfer → 源节点回传序列化 entry →
        本节点 store_local。源节点路由只查本地回传, 不再二次回调 (避免递归)。
        source_node 须为纯主机名 (路由器拼 :11458), 非 host:port。
        """
        try:
            client = await self._get_http_client(30.0)
            safe_source = sanitize_node_url_part(source_node)
            # H3 (AR #24): 出站 SSRF 守卫 — 挡云元数据/链路本地等恶意 source host
            if not is_safe_outbound_host(safe_source):
                logger.warning(f"KV 传输跳过非安全源节点: {source_node!r}")
                return False
            resp = await client.post(
                f"{mtls_scheme()}://{safe_source}:11458/api/kv/transfer",
                json={
                    "cache_id": cache_id,
                    "target_node": target_node,
                    "compress": self.enable_compression,
                    "protocol": KV_SYNC_PROTOCOL,
                },
                headers=self._auth_headers(),
            )
            if resp.status_code != 200:
                logger.warning(f"KV 传输失败 {source_node} → {target_node}: HTTP {resp.status_code}")
                return False
            data = resp.json()
            entry = self._deserialize_entry(data["entry"])
            stored = self.store_local(entry)
            if not stored:
                logger.warning(f"KV 传输 store_local 失败 {source_node} → {target_node}: cache_id={cache_id}")
                return False
            logger.info(f"KV 传输成功 {source_node} → {target_node}: cache_id={cache_id}")
            return True
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
                    # H3 (AR #24): 出站 SSRF 守卫 — 挡云元数据/链路本地等恶意 host
                    if not is_safe_outbound_host(safe_node):
                        logger.warning(f"缓存预热跳过非安全对端: {node_id!r}")
                        results["failed"] += 1
                        continue
                    resp = await client.post(
                        f"{mtls_scheme()}://{safe_node}:11458/api/kv/warm",
                        json={
                            "model_name": model_name,
                            "prompt": prompt,
                            "prompt_hash": prompt_hash,
                        },
                        headers=self._auth_headers(),
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
            "tensor_backend": getattr(self._transport, "name", "unknown"),
        }

    # ── GAP-7 (#33) 张量级 KV 跨节点导出/导入 ──

    async def export_bundle(self, cache_id: str, model_name: str) -> dict[str, Any] | None:
        """源节点导出 KV 缓存 bundle (含张量) 供跨节点传输。

        查本地缓存 → 经 transport 后端为各分片产出张量 → 序列化含 tensor 的 bundle。
        返回 None = 本地无此 cache_id。
        """
        entry = self.lookup_local_by_id(cache_id)
        if entry is None:
            logger.warning(f"GAP-7 KV 导出: cache_id={cache_id} 本地未找到")
            return None
        node_id = self._node_id_for_export(entry)
        # 为缺张量的分片经后端产出 (合成/MLX); 已有张量的分片直传 (避免重复生成)。
        for shard in entry.shards:
            if shard.tensor is None:
                tensor = await self._transport.export_tensor(cache_id, shard.model_name, node_id)
                if tensor is not None:
                    shard.tensor = tensor
                    shard.is_compressed = self.enable_compression
                    # 张量字节并入 size_bytes (传输计费)
                    shard.size_bytes += len(tensor)
        # 张量并入条目总字节 (传输计费 + 目标内存预算)
        self._recompute_entry_size(entry)
        bundle = self._serialize_entry(entry)
        logger.info(
            f"GAP-7 KV 导出 bundle: cache_id={cache_id} model={model_name} "
            f"shards={len(entry.shards)} size={entry.total_size_bytes}B backend={getattr(self._transport, 'name', '?')}"
        )
        return bundle

    async def import_bundle(self, bundle: dict[str, Any]) -> bool:
        """目标节点导入 KV 缓存 bundle (含张量) 并本地存储。

        反序列化 → 经 transport 后端消费张量 (MLX 装本地引擎 / 合成 no-op) → store_local
        (LRU + max_local_cache_mb 硬预算门控)。返回 False = 预算超限或解析失败。
        """
        try:
            entry = self._deserialize_entry(bundle)
        except Exception as e:
            logger.error(f"GAP-7 KV 导入反序列化失败: {e}")
            return False
        node_id = self._node_id_for_export(entry)
        # 经后端消费张量 (MLX 装本地引擎; 合成 no-op) — 不阻塞存储。
        for shard in entry.shards:
            if shard.tensor is not None:
                await self._transport.import_tensor(entry.cache_id, shard.model_name, shard.tensor, node_id)
        stored = self.store_local(entry)
        if not stored:
            logger.warning(
                f"GAP-7 KV 导入 store_local 拒绝 (预算超限): cache_id={entry.cache_id} size={entry.total_size_bytes}B"
            )
            return False
        logger.info(
            f"GAP-7 KV 导入成功: cache_id={entry.cache_id} model={entry.model_name} "
            f"shards={len(entry.shards)} size={entry.total_size_bytes}B"
        )
        return True

    # ── P0-3 (审计 §4.3): KV 张量流式二进制协议 — 替代 base64+JSON 单 POST 全量物化 ──
    # 旧 _serialize_entry (base64 进 JSON) 把整 bundle 物化进内存, 500MB 张量 → JSON 解析峰值 1.5GB。
    # 流式协议: 头部 JSON 元数据 (无张量) + 各分片原始张量字节顺序拼接, 逐块产/消, 不物化整 bundle。
    # 格式:
    #   8B magic b"FMUKVT01" + 4B big-endian uint32 metadata_len + metadata JSON bytes
    #   metadata = {"entry": <entry 字段无 shards>, "shards": [{<shard 字段无 tensor>, "tensor_len": N}]}
    #   随后顺序各 shard 的 tensor_len 原始字节 (len=0 则无负载, 旧对端降级 JSON)。
    KV_STREAM_MAGIC = b"FMUKVT01"
    KV_STREAM_VERSION = 1

    async def export_stream(self, cache_id: str, model_name: str):
        """源节点流式导出 KV 缓存 (含张量) — async generator 逐块产字节, 不物化整 bundle。

        元数据头 JSON (无张量) 先 yield, 再逐分片经后端产张量后按块 yield 原始字节。
        比旧 export_bundle (base64+JSON 全量物化) 峰值内存大幅下降, 真 8B 模型 (200-500MB) 可行。
        """
        entry = self.lookup_local_by_id(cache_id)
        if entry is None:
            logger.warning(f"P0-3 KV 流式导出: cache_id={cache_id} 本地未找到")
            return
        node_id = self._node_id_for_export(entry)
        # 先逐分片经后端产张量 (缺则合成/MLX), 收集元数据 + 张量字节 (张量小可一次产, 大则优化为分块)
        shard_metas = []
        tensors = []
        for shard in entry.shards:
            if shard.tensor is None:
                tensor = await self._transport.export_tensor(cache_id, shard.model_name, node_id)
                if tensor is not None:
                    shard.tensor = tensor
                    shard.is_compressed = self.enable_compression
                    shard.size_bytes += len(tensor)
            t = shard.tensor or b""
            shard_d = {k: getattr(shard, k) for k in ALLOWED_SHARD_KEYS if hasattr(shard, k)}
            shard_d["tensor_len"] = len(t)
            shard_d[COMPRESS_METHOD_KEY] = "caveman" if shard.is_compressed else "none"
            shard_metas.append(shard_d)
            tensors.append(t)
        self._recompute_entry_size(entry)
        metadata = {
            "version": self.KV_STREAM_VERSION,
            "entry": {
                "cache_id": entry.cache_id,
                "model_name": entry.model_name,
                "prompt_hash": entry.prompt_hash,
                "prompt_prefix": entry.prompt_prefix,
                "total_tokens": entry.total_tokens,
                "total_size_bytes": entry.total_size_bytes,
                "created_at": entry.created_at,
                "ttl_seconds": entry.ttl_seconds,
                "access_count": entry.access_count,
            },
            "shards": shard_metas,
        }
        meta_bytes = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
        # 头: magic + 元数据长度 (big-endian uint32) + 元数据
        yield self.KV_STREAM_MAGIC + len(meta_bytes).to_bytes(4, "big") + meta_bytes
        # 逐分片张量字节 (大张量可分块, 此处整块 yield — 合成/中等张量足够; 真大张量优化留上游 #650)
        for t in tensors:
            if t:
                yield t
        logger.info(
            f"P0-3 KV 流式导出: cache_id={cache_id} shards={len(entry.shards)} "
            f"size={entry.total_size_bytes}B backend={getattr(self._transport, 'name', '?')}"
        )

    async def import_stream(self, header_and_meta: bytes, tensor_body_aiter):
        """目标节点流式导入 — 消费 export_stream 产出的字节流并本地存储。

        header_and_meta: 已读入的 magic+长度+元数据 (调用方先读头部确定元数据长度)。
        tensor_body_aiter: 剩余张量字节 async iterator (逐块产), 按元数据 shard 顺序消费。
        返回 True = 已存 (store_local 预算门控); False = 解析失败或预算超限。
        """
        if not header_and_meta.startswith(self.KV_STREAM_MAGIC):
            logger.warning("P0-3 KV 流式导入: magic 头不匹配, 拒绝")
            return False
        try:
            meta_len = int.from_bytes(header_and_meta[8:12], "big")
            meta_bytes = header_and_meta[12 : 12 + meta_len]
            metadata = json.loads(meta_bytes.decode("utf-8"))
        except Exception as e:
            logger.error(f"P0-3 KV 流式导入: 元数据解析失败: {e}")
            return False
        shard_metas = metadata.get("shards", [])
        # 按顺序逐分片从 tensor_body_aiter 读 tensor_len 字节, 流式不物化全部
        shard_tensors = []
        async_gen = tensor_body_aiter.__aiter__() if hasattr(tensor_body_aiter, "__aiter__") else tensor_body_aiter
        try:
            for sm in shard_metas:
                need = int(sm.get("tensor_len", 0))
                if need <= 0:
                    shard_tensors.append(None)
                    continue
                got = bytearray()
                while len(got) < need:
                    try:
                        chunk = await async_gen.__anext__()
                    except StopAsyncIteration:
                        break
                    if not chunk:
                        continue
                    got.extend(chunk)
                shard_tensors.append(bytes(got[:need]) if len(got) >= need else None)
        except Exception as e:
            logger.error(f"P0-3 KV 流式导入: 张量流消费失败: {e}")
            return False
        # 重建 entry + 经后端装张量 (MLX 装本地引擎 / 合成 no-op) + store_local
        shards = []
        for sm, t in zip(shard_metas, shard_tensors):
            filtered = {k: v for k, v in sm.items() if k in ALLOWED_SHARD_KEYS}
            if t is not None:
                filtered["tensor"] = t
                filtered["is_compressed"] = sm.get(COMPRESS_METHOD_KEY) == "caveman"
            shards.append(KVShard(**filtered))
        entry_meta = metadata.get("entry", {})
        entry = KVCacheEntry(
            cache_id=entry_meta.get("cache_id", ""),
            model_name=entry_meta.get("model_name", ""),
            prompt_hash=entry_meta.get("prompt_hash", ""),
            prompt_prefix=entry_meta.get("prompt_prefix", ""),
            shards=shards,
            total_tokens=entry_meta.get("total_tokens", 0),
            total_size_bytes=entry_meta.get("total_size_bytes", 0),
            created_at=entry_meta.get("created_at", time.time()),
            ttl_seconds=entry_meta.get("ttl_seconds", 3600.0),
            access_count=entry_meta.get("access_count", 0),
        )
        node_id = self._node_id_for_export(entry)
        for shard in entry.shards:
            if shard.tensor is not None:
                await self._transport.import_tensor(entry.cache_id, shard.model_name, shard.tensor, node_id)
        stored = self.store_local(entry)
        if not stored:
            logger.warning(
                f"P0-3 KV 流式导入 store_local 拒绝 (预算超限): cache_id={entry.cache_id} "
                f"size={entry.total_size_bytes}B"
            )
            return False
        logger.info(
            f"P0-3 KV 流式导入成功: cache_id={entry.cache_id} shards={len(entry.shards)} size={entry.total_size_bytes}B"
        )
        return True

    def _node_id_for_export(self, entry: KVCacheEntry) -> str:
        """取条目归属节点 id (分片 node_id 优先, 否则首分片, 否则空)。"""
        for shard in entry.shards:
            if shard.node_id:
                return shard.node_id
        return entry.shards[0].node_id if entry.shards else ""

    def _recompute_entry_size(self, entry: KVCacheEntry) -> None:
        """重算条目总字节 (张量并入后)。"""
        total = 0
        for shard in entry.shards:
            # size_bytes 已含张量 (export_bundle 并入), 此处汇总
            total += shard.size_bytes
        entry.total_size_bytes = total

    def _serialize_entry(self, entry: KVCacheEntry) -> dict:
        """序列化 KV 缓存条目供跨节点传输。

        GAP-7 (#33): 张量负载随分片传输 — base64 编码 (Caveman 压缩后) 进 JSON。
        无 tensor 的分片 (纯元数据) 不写 tensor 键, 向后兼容旧对端。
        """
        shards_out = []
        for s in entry.shards:
            shard_d = {k: getattr(s, k) for k in ALLOWED_SHARD_KEYS if hasattr(s, k)}
            if s.tensor is not None:
                tensor_b64 = base64.b64encode(s.tensor).decode("ascii")
                shard_d[TENSOR_KEY] = tensor_b64
                shard_d[COMPRESS_METHOD_KEY] = "caveman" if s.is_compressed else "none"
            shards_out.append(shard_d)
        return {
            "cache_id": entry.cache_id,
            "model_name": entry.model_name,
            "prompt_hash": entry.prompt_hash,
            "prompt_prefix": entry.prompt_prefix,
            "total_tokens": entry.total_tokens,
            "total_size_bytes": entry.total_size_bytes,
            "created_at": entry.created_at,
            "ttl_seconds": entry.ttl_seconds,
            "access_count": entry.access_count,
            "shards": shards_out,
        }

    def _deserialize_entry(self, data: dict) -> KVCacheEntry:
        shards = []
        for s in data.get("shards", []):
            filtered = {k: v for k, v in s.items() if k in ALLOWED_SHARD_KEYS}
            # GAP-7 (#33): 张量负载解码 — base64 → bytes。压缩标记 is_compressed 已在元数据。
            tensor_b64 = s.get(TENSOR_KEY)
            if tensor_b64:
                try:
                    filtered["tensor"] = base64.b64decode(tensor_b64)
                except Exception as e:
                    logger.warning(f"KV 张量解码失败, 降级为无张量分片: {e}")
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

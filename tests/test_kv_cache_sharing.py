"""KV cache sharing coverage tests."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from fusion_multi_node.distributed_mlx.kv_cache_sharing import (
    KVCacheEntry,
    KVCacheWarmScheduler,
    KVShard,
    KVSharingManager,
)


def _make_entry(
    cache_id="c1",
    model_name="test-model",
    prompt_hash="hash1",
    prompt_prefix="Hello",
    total_tokens=100,
    total_size_bytes=1024,
    ttl_seconds=3600.0,
):
    return KVCacheEntry(
        cache_id=cache_id,
        model_name=model_name,
        prompt_hash=prompt_hash,
        prompt_prefix=prompt_prefix,
        total_tokens=total_tokens,
        total_size_bytes=total_size_bytes,
        created_at=time.time(),
        ttl_seconds=ttl_seconds,
        shards=[
            KVShard(
                shard_id="s1",
                model_name=model_name,
                layer_index=0,
                node_id="node_1",
                token_count=total_tokens,
                size_bytes=total_size_bytes,
                created_at=time.time(),
            )
        ],
    )


def _make_mock_client(mock_resp=None, side_effect=None):
    mock_client = AsyncMock()
    if side_effect:
        mock_client.post = AsyncMock(side_effect=side_effect)
    else:
        mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=mock_client)


class TestKVShard:
    def test_basic(self):
        shard = KVShard(
            shard_id="s0",
            model_name="test",
            layer_index=0,
            node_id="n1",
            token_count=100,
            size_bytes=1024,
            created_at=time.time(),
        )
        assert shard.shard_id == "s0"
        assert shard.access_count == 0
        assert not shard.is_compressed


class TestKVCacheEntry:
    def test_is_expired_false(self):
        e = _make_entry()
        assert e.is_expired is False

    def test_is_expired_true(self):
        e = _make_entry()
        e.created_at = time.time() - 7200
        e.ttl_seconds = 3600
        assert e.is_expired is True

    def test_defaults(self):
        e = KVCacheEntry(cache_id="c1", model_name="m", prompt_hash="h", prompt_prefix="p")
        assert e.total_tokens == 0
        assert e.total_size_bytes == 0
        assert e.access_count == 0
        assert e.shards == []


class TestKVSharingManagerInit:
    def test_init_defaults(self):
        m = KVSharingManager()
        assert m.max_local_cache_mb == 4096.0
        assert m.enable_compression is True
        assert m._compressor is not None

    def test_init_no_compression(self):
        m = KVSharingManager(enable_compression=False)
        assert m.enable_compression is False
        assert m._compressor is None


class TestKVSharingManagerLocal:
    def test_store_local(self):
        m = KVSharingManager()
        assert m.store_local(_make_entry()) is True
        assert len(m._local_cache) == 1

    def test_lookup_local(self):
        m = KVSharingManager()
        m.store_local(_make_entry())
        found = m.lookup_local("test-model", "hash1")
        assert found is not None
        assert found.access_count == 1

    def test_lookup_local_missing(self):
        m = KVSharingManager()
        assert m.lookup_local("test", "missing") is None

    def test_lookup_local_expired(self):
        m = KVSharingManager()
        e = _make_entry(ttl_seconds=0.01)
        m.store_local(e)
        time.sleep(0.02)
        assert m.lookup_local("test-model", "hash1") is None

    def test_lookup_prefix(self):
        m = KVSharingManager()
        m.store_local(_make_entry(prompt_prefix="hello_world"))
        matches = m.lookup_prefix("test-model", "hello")
        assert len(matches) == 1

    def test_lookup_prefix_no_match(self):
        m = KVSharingManager()
        assert m.lookup_prefix("test-model", "xxx") == []

    def test_lru_eviction(self):
        m = KVSharingManager(max_local_cache_mb=0.001)
        for i in range(10):
            m.store_local(_make_entry(cache_id=f"e{i}", total_size_bytes=500))
        assert m._local_size_bytes <= 0.001 * 1024 * 1024

    def test_evict_empty_cache(self):
        m = KVSharingManager(max_local_cache_mb=0.001)
        m._evict(500)
        assert len(m._local_cache) == 0


class TestKVSharingManagerRemoteLookup:
    @pytest.mark.asyncio
    async def test_lookup_remote_success(self):
        m = KVSharingManager(enable_compression=False)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "found": True,
            "entry": {
                "cache_id": "r1",
                "model_name": "test-model",
                "prompt_hash": "hash1",
                "prompt_prefix": "Hello",
                "shards": [],
                "total_tokens": 50,
                "total_size_bytes": 512,
                "created_at": time.time(),
                "ttl_seconds": 3600.0,
                "access_count": 0,
            },
        }
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.lookup_remote("test-model", "hash1", ["node_1"])
        assert result is not None
        entry, node_id = result
        assert entry.cache_id == "r1"
        assert node_id == "node_1"

    @pytest.mark.asyncio
    async def test_lookup_remote_not_found(self):
        m = KVSharingManager(enable_compression=False)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"found": False}
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.lookup_remote("test-model", "hash1", ["node_1"])
        assert result is None

    @pytest.mark.asyncio
    async def test_lookup_remote_exception(self):
        m = KVSharingManager(enable_compression=False)
        mock_ac_class = _make_mock_client(side_effect=Exception("connection error"))
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.lookup_remote("test-model", "hash1", ["node_1"])
        assert result is None

    @pytest.mark.asyncio
    async def test_lookup_remote_http_error(self):
        m = KVSharingManager(enable_compression=False)
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.lookup_remote("test-model", "hash1", ["node_1"])
        assert result is None

    @pytest.mark.asyncio
    async def test_lookup_remote_unreachable(self):
        m = KVSharingManager()
        result = await m.lookup_remote("test", "h1", ["192.0.2.1"])
        assert result is None


class TestKVSharingManagerTransfer:
    @pytest.mark.asyncio
    async def test_transfer_from_remote_success(self):
        m = KVSharingManager(enable_compression=False)
        # 推模型: 源节点回传 {entry: 序列化 KVCacheEntry}, 本节点反序列化 + store_local。
        entry = _make_entry(cache_id="c1")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"entry": m._serialize_entry(entry)}
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.transfer_from_remote("c1", "node_1", "node_2")
        assert result is True
        assert m.lookup_local_by_id("c1") is not None

    @pytest.mark.asyncio
    async def test_transfer_from_remote_http_fail(self):
        m = KVSharingManager(enable_compression=False)
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.transfer_from_remote("c1", "node_1", "node_2")
        assert result is False

    @pytest.mark.asyncio
    async def test_transfer_from_remote_exception(self):
        m = KVSharingManager(enable_compression=False)
        mock_ac_class = _make_mock_client(side_effect=Exception("timeout"))
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.transfer_from_remote("c1", "node_1", "node_2")
        assert result is False

    @pytest.mark.asyncio
    async def test_transfer_from_remote_unreachable(self):
        m = KVSharingManager(enable_compression=False)
        mock_ac_class = _make_mock_client(side_effect=httpx.ConnectError("unreachable"))
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.transfer_from_remote("c1", "192.0.2.1", "192.0.2.2")
        assert result is False


class TestKVSharingManagerWarmCache:
    @pytest.mark.asyncio
    async def test_warm_cache_success(self):
        m = KVSharingManager(enable_compression=False)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            results = await m.warm_cache("test-model", ["prompt1", "prompt2"], ["node_1"])
        assert results["success"] == 2
        assert results["failed"] == 0

    @pytest.mark.asyncio
    async def test_warm_cache_partial_failure(self):
        m = KVSharingManager(enable_compression=False)
        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.status_code = 200 if call_count % 2 == 1 else 500
            return resp

        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_ac_class = MagicMock(return_value=mock_client)

        with patch("httpx.AsyncClient", mock_ac_class):
            results = await m.warm_cache("test-model", ["p1"], ["node_1", "node_2"])
        assert results["success"] >= 1
        assert results["failed"] >= 1

    @pytest.mark.asyncio
    async def test_warm_cache_exception(self):
        m = KVSharingManager(enable_compression=False)
        mock_ac_class = _make_mock_client(side_effect=Exception("connection error"))
        with patch("httpx.AsyncClient", mock_ac_class):
            results = await m.warm_cache("test-model", ["p1"], ["node_1"])
        assert results["failed"] == 1

    @pytest.mark.asyncio
    async def test_warm_cache_unreachable(self):
        m = KVSharingManager(enable_compression=False)
        mock_ac_class = _make_mock_client(side_effect=httpx.ConnectError("unreachable"))
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await m.warm_cache("test", ["hello"], ["192.0.2.1"])
        assert result["failed"] >= 1


class TestKVSharingManagerDeserialize:
    def test_deserialize_entry(self):
        m = KVSharingManager(enable_compression=False)
        data = {
            "cache_id": "d1",
            "model_name": "model-x",
            "prompt_hash": "h1",
            "prompt_prefix": "prefix",
            "shards": [
                {
                    "shard_id": "s1",
                    "model_name": "model-x",
                    "layer_index": 0,
                    "node_id": "n1",
                    "token_count": 10,
                    "size_bytes": 100,
                    "created_at": time.time(),
                }
            ],
            "total_tokens": 10,
            "total_size_bytes": 100,
            "created_at": time.time(),
            "ttl_seconds": 3600.0,
            "access_count": 5,
        }
        entry = m._deserialize_entry(data)
        assert entry.cache_id == "d1"
        assert len(entry.shards) == 1
        assert entry.access_count == 5


class TestKVSharingManagerStats:
    def test_get_stats(self):
        m = KVSharingManager(enable_compression=False)
        m.store_local(_make_entry(total_size_bytes=1024 * 1024))
        stats = m.get_stats()
        assert stats["local_entries"] == 1
        assert stats["local_size_mb"] > 0
        assert stats["compression_enabled"] is False


class TestKVCacheWarmScheduler:
    def test_record_prompt(self):
        m = KVSharingManager(enable_compression=False)
        s = KVCacheWarmScheduler(m)
        s.record_prompt("Hello world")
        assert "Hello world" in s._hot_prompts

    def test_get_hot_prompts(self):
        m = KVSharingManager(enable_compression=False)
        s = KVCacheWarmScheduler(m)
        for _ in range(5):
            s.record_prompt("hot prompt")
        for _ in range(2):
            s.record_prompt("cold prompt")
        hot = s.get_hot_prompts(threshold=3)
        assert "hot prompt" in hot
        assert "cold prompt" not in hot

    def test_get_hot_prompts_max_count(self):
        m = KVSharingManager(enable_compression=False)
        s = KVCacheWarmScheduler(m)
        for i in range(20):
            for _ in range(5):
                s.record_prompt(f"prompt_{i}")
        hot = s.get_hot_prompts(threshold=3, max_count=5)
        assert len(hot) == 5

    def test_stop(self):
        m = KVSharingManager(enable_compression=False)
        s = KVCacheWarmScheduler(m)
        s._running = True
        s.stop()
        assert s._running is False

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        m = KVSharingManager(enable_compression=False)
        s = KVCacheWarmScheduler(m)
        task = asyncio.create_task(s.start(interval=0.1, nodes=["n1"]))
        await asyncio.sleep(0.05)
        s.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert s._running is False

"""GAP-7 (#33) S1 单元测试 — KVShard 张量字段 + 序列化 round-trip + 传输后端。

覆盖:
- KVShard.tensor 字段 round-trip (serialize → deserialize 字节完整)
- 无张量分片向后兼容 (旧元数据 bundle 无 tensor 键 → tensor=None)
- SyntheticKVTransport 确定性 (同种子同张量) + import no-op 返 True
- get_kv_transport 工厂 env 选后端
"""

from __future__ import annotations

import asyncio

from fusion_multi_node.distributed_mlx.kv_cache_sharing import (
    KVCacheEntry,
    KVShard,
    KVSharingManager,
)
from fusion_multi_node.distributed_mlx.kv_tensor_transport import (
    MLXKVTransport,
    SyntheticKVTransport,
    get_kv_transport,
)


def _make_entry_with_tensor(tensor: bytes | None) -> KVCacheEntry:
    import time

    return KVCacheEntry(
        cache_id="c-tensor",
        model_name="llama-1b",
        prompt_hash="hash-tensor",
        prompt_prefix="Hello",
        total_tokens=32,
        total_size_bytes=1024,
        created_at=time.time(),
        ttl_seconds=3600.0,
        shards=[
            KVShard(
                shard_id="s0",
                model_name="llama-1b",
                layer_index=0,
                node_id="node-a",
                token_count=32,
                size_bytes=512,
                created_at=time.time(),
                tensor=tensor,
                is_compressed=tensor is not None,
            )
        ],
    )


class TestKVShardTensorSerialize:
    def test_tensor_round_trip(self):
        # 张量字节经 serialize (base64) → deserialize 回原始 bytes, 完整保留。
        mgr = KVSharingManager(cluster_token="tok", transport=SyntheticKVTransport())
        original = b"\x00\x01\x02\xff" * 128  # 512B 含高位字节
        entry = _make_entry_with_tensor(original)
        serialized = mgr._serialize_entry(entry)
        restored = mgr._deserialize_entry(serialized)
        assert restored.shards[0].tensor == original
        assert restored.shards[0].is_compressed is True

    def test_no_tensor_backward_compat(self):
        # 旧 bundle 无 tensor 键 → deserialize 得 tensor=None (向后兼容)。
        mgr = KVSharingManager(cluster_token="tok", transport=SyntheticKVTransport())
        old_bundle = {
            "cache_id": "c-old",
            "model_name": "llama-1b",
            "prompt_hash": "h",
            "prompt_prefix": "Hi",
            "total_tokens": 8,
            "total_size_bytes": 256,
            "created_at": 0.0,
            "ttl_seconds": 3600.0,
            "shards": [
                {
                    "shard_id": "s0",
                    "model_name": "llama-1b",
                    "layer_index": 0,
                    "node_id": "node-a",
                    "token_count": 8,
                    "size_bytes": 256,
                    "created_at": 0.0,
                }
            ],
        }
        restored = mgr._deserialize_entry(old_bundle)
        assert restored.shards[0].tensor is None

    def test_serialize_without_tensor_omits_key(self):
        # 无张量分片 serialize 不写 tensor 键 (避免旧对端读未知键)。
        mgr = KVSharingManager(cluster_token="tok", transport=SyntheticKVTransport())
        entry = _make_entry_with_tensor(None)
        serialized = mgr._serialize_entry(entry)
        assert "tensor" not in serialized["shards"][0]


class TestSyntheticKVTransport:
    def test_export_deterministic(self):
        # 同 cache_id+model+node 恒产生同字节。
        t = SyntheticKVTransport(tensor_size=512)

        async def _run():
            a = await t.export_tensor("c1", "llama-1b", "node-a")
            b = await t.export_tensor("c1", "llama-1b", "node-a")
            return a, b

        a, b = asyncio.run(_run())
        assert a is not None and a == b
        assert len(a) == 512

    def test_export_different_seed_differs(self):
        # 不同 node_id → 不同种子 → 不同张量。
        t = SyntheticKVTransport(tensor_size=256)

        async def _run():
            return await t.export_tensor("c1", "llama-1b", "node-a"), await t.export_tensor(
                "c1", "llama-1b", "node-b"
            )

        a, b = asyncio.run(_run())
        assert a != b

    def test_import_noop_returns_true(self):
        t = SyntheticKVTransport()

        async def _run():
            return await t.import_tensor("c1", "llama-1b", b"x" * 100, "node-a")

        assert asyncio.run(_run()) is True


class TestGetKVTransport:
    def test_default_synthetic(self, monkeypatch):
        monkeypatch.delenv("FUSION_KV_TENSOR_BACKEND", raising=False)
        t = get_kv_transport()
        assert isinstance(t, SyntheticKVTransport)

    def test_env_mlx(self, monkeypatch):
        monkeypatch.setenv("FUSION_KV_TENSOR_BACKEND", "mlx")
        t = get_kv_transport()
        assert isinstance(t, MLXKVTransport)

    def test_explicit_backend_overrides_env(self, monkeypatch):
        monkeypatch.setenv("FUSION_KV_TENSOR_BACKEND", "mlx")
        t = get_kv_transport("synthetic")
        assert isinstance(t, SyntheticKVTransport)


class TestExportImportBundle:
    def test_export_bundle_attaches_tensor(self):
        # export_bundle: 无张量分片经后端产出张量并并入 bundle。
        mgr = KVSharingManager(
            cluster_token="tok",
            transport=SyntheticKVTransport(tensor_size=256),
        )
        mgr.store_local(_make_entry_with_tensor(None))

        async def _run():
            return await mgr.export_bundle("c-tensor", "llama-1b")

        bundle = asyncio.run(_run())
        assert bundle is not None
        assert "tensor" in bundle["shards"][0]
        assert bundle["shards"][0]["tensor_compress"] == "caveman"

    def test_import_bundle_stores_with_tensor(self):
        # import_bundle: bundle 含张量 → 反序列化 → store_local → 查回张量完整。
        mgr_src = KVSharingManager(
            cluster_token="tok",
            transport=SyntheticKVTransport(tensor_size=256),
        )
        mgr_dst = KVSharingManager(
            cluster_token="tok",
            transport=SyntheticKVTransport(),
        )
        mgr_src.store_local(_make_entry_with_tensor(None))

        async def _run():
            bundle = await mgr_src.export_bundle("c-tensor", "llama-1b")
            assert bundle is not None
            ok = await mgr_dst.import_bundle(bundle)
            return ok

        ok = asyncio.run(_run())
        assert ok is True
        restored = mgr_dst.lookup_local_by_id("c-tensor")
        assert restored is not None
        assert restored.shards[0].tensor is not None
        assert len(restored.shards[0].tensor) == 256

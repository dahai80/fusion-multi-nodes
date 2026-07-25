"""CavemanCompressor and CavemanManager detailed coverage tests."""

import struct
import zlib

import pytest

from fusion_multi_node.distributed_mlx.caveman_compress import (
    CavemanCompressor,
    CavemanManager,
    CompressStats,
)


class TestCompressStats:
    def test_defaults(self):
        s = CompressStats()
        assert s.original_bytes == 0
        assert s.compressed_bytes == 0
        assert s.ratio == 0.0
        assert s.method == ""
        assert s.time_ms == 0.0

    def test_custom(self):
        s = CompressStats(original_bytes=100, compressed_bytes=50, ratio=0.5, method="zlib")
        assert s.method == "zlib"


class TestCavemanCompressorBuildDictionary:
    def test_build_dictionary(self):
        c = CavemanCompressor(dictionary_size=8)
        tokens = [10, 20, 30, 40, 10, 20, 10]
        c.build_dictionary(tokens)
        assert len(c._dictionary) > 0
        assert len(c._reverse_dict) > 0
        for token in set(tokens):
            assert token in c._dictionary

    def test_build_dictionary_large_index(self):
        c = CavemanCompressor(dictionary_size=70000)
        tokens = list(range(70000))
        c.build_dictionary(tokens)
        assert len(c._dictionary) == 70000
        for i in range(65536, 70000):
            expected_code = struct.pack(">I", i)
            assert c._dictionary[i] == expected_code
            assert c._reverse_dict[expected_code] == i


class TestCavemanCompressorDecompress:
    def test_decompress_zlib(self):
        c = CavemanCompressor()
        data = b"hello world " * 50
        compressed = zlib.compress(data, level=3)
        result = c.decompress(compressed, "zlib")
        assert result == data

    def test_decompress_diff(self):
        c = CavemanCompressor()
        data = bytes(range(256))
        compressed, _ = c.compress(data, method="diff")
        result = c.decompress(compressed, "diff")
        assert result == data

    def test_decompress_dict_roundtrip(self):
        c = CavemanCompressor()
        tokens = [100, 200, 300, 400]
        c.build_dictionary(tokens)
        original = b"".join(struct.pack(">I", t) for t in tokens)
        compressed = c._dict_compress(original)
        result = c.decompress(compressed, "dict")
        assert result == original

    def test_decompress_unknown_method_returns_data(self):
        c = CavemanCompressor()
        data = b"some data"
        result = c.decompress(data, "unknown")
        assert result == data


class TestCavemanCompressorSelectMethod:
    def test_small_data_selects_dict(self):
        c = CavemanCompressor()
        assert c._select_method(b"short") == "dict"

    def test_repeated_pattern_selects_diff(self):
        c = CavemanCompressor()
        data = b"AAAAAAAA" * 20
        assert c._select_method(data) == "diff"

    def test_large_no_repeat_selects_zlib(self):
        c = CavemanCompressor()
        import os
        data = os.urandom(256)
        method = c._select_method(data)
        assert method in ("zlib", "diff")


class TestCavemanCompressorCompressInternal:
    def test_compress_unknown_method_returns_data(self):
        c = CavemanCompressor()
        data = b"hello"
        result = c._compress(data, "unknown")
        assert result == data


class TestCavemanCompressorDictCompress:
    def test_no_dictionary_returns_data(self):
        c = CavemanCompressor()
        data = b"some raw data"
        assert c._dict_compress(data) == data

    def test_dict_compress_with_matching_tokens(self):
        c = CavemanCompressor()
        tokens = [0x41414141]
        c.build_dictionary(tokens)
        data = struct.pack(">I", 0x41414141)
        result = c._dict_compress(data)
        assert len(result) < len(data)

    def test_dict_compress_mixed_data(self):
        c = CavemanCompressor()
        tokens = [42]
        c.build_dictionary(tokens)
        data = struct.pack(">I", 42) + b"AB"
        result = c._dict_compress(data)
        assert isinstance(result, bytes)
        assert len(result) > 0


class TestCavemanCompressorDictDecompress:
    def test_no_reverse_dict_returns_data(self):
        c = CavemanCompressor()
        data = b"raw bytes"
        assert c._dict_decompress(data) == data

    def test_dict_decompress_roundtrip(self):
        c = CavemanCompressor()
        tokens = [100, 200]
        c.build_dictionary(tokens)
        original = struct.pack(">I", 100) + struct.pack(">I", 200)
        compressed = c._dict_compress(original)
        decompressed = c._dict_decompress(compressed)
        assert decompressed == original

    def test_dict_decompress_single_byte_passthrough(self):
        c = CavemanCompressor()
        tokens = [100]
        c.build_dictionary(tokens)
        data = b"\x01"
        result = c._dict_decompress(data)
        assert result == b"\x01"


class TestCavemanCompressorDiffCompress:
    def test_less_than_2_bytes_returns_data(self):
        c = CavemanCompressor()
        assert c._diff_compress(b"A") == b"A"
        assert c._diff_compress(b"") == b""

    def test_diff_compress_decompress_roundtrip(self):
        c = CavemanCompressor()
        data = bytes([10, 20, 30, 40, 50])
        compressed = c._diff_compress(data)
        decompressed = c._diff_decompress(compressed)
        assert decompressed == data

    def test_diff_decompress_single_byte(self):
        c = CavemanCompressor()
        data = zlib.compress(b"X", level=1)
        result = c._diff_decompress(data)
        assert result == b"X"

    def test_diff_decompress_empty_after_zlib(self):
        c = CavemanCompressor()
        data = zlib.compress(b"", level=1)
        result = c._diff_decompress(data)
        assert result == b""


class TestCavemanCompressorRepeatedPattern:
    def test_short_data_no_pattern(self):
        c = CavemanCompressor()
        assert c._has_repeated_pattern(b"ABCDE") is False

    def test_repeated_pattern_detected(self):
        c = CavemanCompressor()
        data = b"AAAA" * 100
        assert c._has_repeated_pattern(data) is True

    def test_high_entropy_no_repeat(self):
        c = CavemanCompressor()
        data = bytes(i * 37 % 256 for i in range(256)) * 2
        result = c._has_repeated_pattern(data)
        assert isinstance(result, bool)


class TestCavemanCompressorStatsProperty:
    def test_stats_property(self):
        c = CavemanCompressor()
        data = b"hello world " * 50
        c.compress(data, method="zlib")
        assert c.stats.original_bytes == len(data)
        assert c.stats.compressed_bytes > 0
        assert c.stats.method == "zlib"

    def test_reset_stats(self):
        c = CavemanCompressor()
        c.compress(b"test data " * 10, method="zlib")
        assert c.stats.original_bytes > 0
        c.reset_stats()
        assert c.stats.original_bytes == 0
        assert c.stats.compressed_bytes == 0


class TestCavemanCompressorAutoMethod:
    def test_auto_dict(self):
        c = CavemanCompressor()
        data = b"hi"
        compressed, stats = c.compress(data, method="auto")
        assert stats.method == "dict"
        decompressed = c.decompress(compressed, "dict")
        assert decompressed == data

    def test_auto_diff(self):
        c = CavemanCompressor()
        data = b"AAAAAAAA" * 50
        compressed, stats = c.compress(data, method="auto")
        assert stats.method == "diff"
        decompressed = c.decompress(compressed, "diff")
        assert decompressed == data

    def test_auto_zlib(self):
        c = CavemanCompressor()
        import os
        data = os.urandom(256)
        compressed, stats = c.compress(data, method="auto")
        decompressed = c.decompress(compressed, stats.method)
        assert decompressed == data


class TestCavemanManager:
    @pytest.mark.asyncio
    async def test_compress_tensor_default_link(self):
        m = CavemanManager()
        data = b"test data " * 100
        compressed, method, stats = await m.compress_tensor(data)
        assert method == "zlib"
        assert stats.original_bytes == len(data)

    @pytest.mark.asyncio
    async def test_decompress_tensor(self):
        m = CavemanManager()
        data = b"test data " * 100
        compressed, method, _ = await m.compress_tensor(data, link_type="ethernet_1g")
        result = await m.decompress_tensor(compressed, method)
        assert result == data

    def test_compression_ratio_no_data(self):
        m = CavemanManager()
        assert m.get_compression_ratio() == 1.0

    @pytest.mark.asyncio
    async def test_compression_ratio_with_data(self):
        m = CavemanManager()
        data = b"test data " * 100
        await m.compress_tensor(data, link_type="ethernet_1g")
        ratio = m.get_compression_ratio()
        assert 0 < ratio < 1.0

    @pytest.mark.asyncio
    async def test_get_stats(self):
        m = CavemanManager()
        data = b"test data " * 100
        await m.compress_tensor(data, link_type="ethernet_1g")
        stats = m.get_stats()
        assert stats["total_original_bytes"] > 0
        assert stats["total_compressed_bytes"] > 0
        assert stats["overall_ratio"] > 0
        assert stats["savings_bytes"] > 0
        assert stats["savings_percent"] > 0

    @pytest.mark.asyncio
    async def test_compress_tensor_thunderbolt(self):
        m = CavemanManager()
        data = b"test data " * 50
        compressed, method, stats = await m.compress_tensor(data, link_type="thunderbolt_5")
        assert method == "dict"

    @pytest.mark.asyncio
    async def test_compress_tensor_100m(self):
        m = CavemanManager()
        data = b"AAAAAAAA" * 200
        compressed, method, stats = await m.compress_tensor(data, link_type="ethernet_100m")
        assert method == "diff"

    def test_unknown_link_type(self):
        m = CavemanManager()
        config = m.get_compression_config("nonexistent")
        assert config["method"] == "zlib"

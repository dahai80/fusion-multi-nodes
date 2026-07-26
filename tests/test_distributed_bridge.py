"""Distributed MLX Bridge 测试。"""


import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch

from fusion_multi_node.distributed_mlx.distributed_bridge import (
    DistributedMLXBridge,
    DistConfig,
    DistMode,
    ModelShard,
)


class TestDistMode:
    def test_values(self):
        assert DistMode.PIPELINE.value == "pipeline"
        assert DistMode.DATA.value == "data"
        assert DistMode.TENSOR.value == "tensor"


class TestModelShard:
    def test_basic(self):
        shard = ModelShard(shard_id=0, total_shards=2, layers=[0, 1, 2], node_id="n1")
        assert shard.shard_id == 0
        assert shard.status == "pending"
        assert shard.memory_mb == 0.0


class TestDistConfig:
    def test_defaults(self):
        cfg = DistConfig()
        assert cfg.mode == DistMode.PIPELINE
        assert cfg.num_nodes == 1
        assert cfg.caveman_compress is True

    def test_custom(self):
        cfg = DistConfig(mode=DistMode.DATA, model_name="llama", num_nodes=4)
        assert cfg.mode == DistMode.DATA
        assert cfg.model_name == "llama"


class TestDistributedMLXBridge:
    def test_init(self):
        bridge = DistributedMLXBridge()
        assert bridge._shards == {}
        assert bridge._active_pipelines == {}

    @pytest.mark.asyncio
    async def test_shard_model(self):
        bridge = DistributedMLXBridge()

        async def mock_get_config(model_name):
            return {"num_hidden_layers": 32, "memory_mb": 4096}

        bridge._get_model_config = mock_get_config
        shards = await bridge.shard_model("test-model", 4)
        assert len(shards) == 4
        assert all(s.total_shards == 4 for s in shards)
        total_layers = sum(len(s.layers) for s in shards)
        assert total_layers == 32

    @pytest.mark.asyncio
    async def test_shard_model_uneven(self):
        bridge = DistributedMLXBridge()

        async def mock_get_config(model_name):
            return {"num_hidden_layers": 33, "memory_mb": 4096}

        bridge._get_model_config = mock_get_config
        shards = await bridge.shard_model("test-model", 3)
        assert len(shards) == 3
        total_layers = sum(len(s.layers) for s in shards)
        assert total_layers == 33

    @pytest.mark.asyncio
    async def test_shard_model_single(self):
        bridge = DistributedMLXBridge()

        async def mock_get_config(model_name):
            return {"num_hidden_layers": 16, "memory_mb": 2048}

        bridge._get_model_config = mock_get_config
        shards = await bridge.shard_model("tiny", 1)
        assert len(shards) == 1
        assert len(shards[0].layers) == 16

    @pytest.mark.asyncio
    async def test_shard_model_custom_strategy(self):
        bridge = DistributedMLXBridge()

        async def mock_get_config(model_name):
            return {"num_hidden_layers": 32, "memory_mb": 4096}

        bridge._get_model_config = mock_get_config
        shards = await bridge.shard_model("test-model", 4, strategy="custom")
        assert len(shards) == 4

    @pytest.mark.asyncio
    async def test_load_shard_out_of_range(self):
        bridge = DistributedMLXBridge()
        shards = [ModelShard(shard_id=0, total_shards=1, layers=[0], node_id="")]
        bridge._shards["test"] = shards
        ok = await bridge.load_shard("test", 5, "n1")
        assert not ok

    @pytest.mark.asyncio
    async def test_load_shard_no_shards(self):
        bridge = DistributedMLXBridge()
        ok = await bridge.load_shard("missing", 0, "n1")
        assert not ok

    @pytest.mark.asyncio
    async def test_load_shard_connection_error(self):
        bridge = DistributedMLXBridge()
        shards = [ModelShard(shard_id=0, total_shards=1, layers=[0], node_id="")]
        bridge._shards["test"] = shards

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=Exception("Connection refused"))

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            ok = await bridge.load_shard("test", 0, "n1")
            assert not ok
            assert shards[0].status == "failed"

    @pytest.mark.asyncio
    async def test_load_shard_success_mock(self):
        bridge = DistributedMLXBridge()
        shards = [ModelShard(shard_id=0, total_shards=1, layers=[0], node_id="")]
        bridge._shards["test"] = shards

        mock_resp = MagicMock()
        mock_resp.status_code = 200

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            ok = await bridge.load_shard("test", 0, "n1")
            assert ok is True
            assert shards[0].status == "loaded"

    @pytest.mark.asyncio
    async def test_load_shard_bad_status_mock(self):
        bridge = DistributedMLXBridge()
        shards = [ModelShard(shard_id=0, total_shards=1, layers=[0], node_id="")]
        bridge._shards["test"] = shards

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Error"

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            ok = await bridge.load_shard("test", 0, "n1")
            assert ok is False

    @pytest.mark.asyncio
    async def test_pipeline_inference_success_mock(self):
        bridge = DistributedMLXBridge()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"output": "result_text"}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await bridge.pipeline_inference("test", "hello", ["n1"])
            assert "output" in result
            assert result["output"] == "result_text"
            assert result["nodes"] == 1

    @pytest.mark.asyncio
    async def test_pipeline_inference_failure_mock(self):
        bridge = DistributedMLXBridge()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=Exception("connection failed"))

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await bridge.pipeline_inference("test", "hello", ["n1"])
            assert "error" in result
            pid = result["pipeline_id"]
            assert bridge._active_pipelines[pid]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_pipeline_inference_multi_node_mock(self):
        bridge = DistributedMLXBridge()

        call_count = 0

        def make_resp():
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"output": f"step_{call_count}"}
            return resp

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=lambda *a, **kw: make_resp())

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await bridge.pipeline_inference("test", "hello", ["n1", "n2"])
            assert result["nodes"] == 2
            assert result["output"] == "step_2"

    @pytest.mark.asyncio
    async def test_data_parallel_inference_success_mock(self):
        bridge = DistributedMLXBridge()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "response"}}],
            "usage": {"total_tokens": 10},
        }

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            results = await bridge.data_parallel_inference(
                "test", ["prompt1"], ["n1"],
            )
            assert len(results) == 1
            assert "content" in results[0]
            assert results[0]["node_id"] == "n1"

    @pytest.mark.asyncio
    async def test_data_parallel_inference_mixed_failure_mock(self):
        bridge = DistributedMLXBridge()

        call_count = 0

        async def mock_post(url, json=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {
                    "choices": [{"message": {"content": "ok"}}],
                    "usage": {},
                }
                return resp
            raise Exception("node down")

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = mock_post

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            results = await bridge.data_parallel_inference(
                "test", ["p1", "p2"], ["n1", "n2"],
            )
            assert len(results) == 2
            assert results[0]["content"] == "ok"
            assert "error" in results[1]

    @pytest.mark.asyncio
    async def test_data_parallel_inference_connection_error_mock(self):
        bridge = DistributedMLXBridge()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=Exception("connection refused"))

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            results = await bridge.data_parallel_inference(
                "test", ["p1", "p2"], ["n1"],
            )
            assert len(results) == 2
            assert all("error" in r for r in results)

    @pytest.mark.asyncio
    async def test_single_inference_mock(self):
        bridge = DistributedMLXBridge()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "hello"}}],
            "usage": {"total_tokens": 5},
        }

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await bridge._single_inference("n1", "test", "hi", 8000)
            assert result["content"] == "hello"
            assert result["node_id"] == "n1"

    @pytest.mark.asyncio
    async def test_get_model_config_fallback(self):
        bridge = DistributedMLXBridge()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(side_effect=Exception("no server"))

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            config = await bridge._get_model_config("nonexistent")
            assert config["num_hidden_layers"] == 32
            assert config["memory_mb"] == 4096

    @pytest.mark.asyncio
    async def test_get_model_config_success_mock(self):
        bridge = DistributedMLXBridge()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"num_hidden_layers": 48, "memory_mb": 8192}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            config = await bridge._get_model_config("test-model")
            assert config["num_hidden_layers"] == 48

    @pytest.mark.asyncio
    async def test_get_model_config_server_error_mock(self):
        bridge = DistributedMLXBridge()

        mock_resp = MagicMock()
        mock_resp.status_code = 404

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            config = await bridge._get_model_config("nonexistent")
            assert config["num_hidden_layers"] == 32

    @pytest.mark.asyncio
    async def test_sync_weights_success_mock(self):
        bridge = DistributedMLXBridge()

        mock_resp = MagicMock()
        mock_resp.status_code = 200

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            ok = await bridge.sync_weights("test", "n1", ["n2"])
            assert ok is True

    @pytest.mark.asyncio
    async def test_sync_weights_server_error_mock(self):
        bridge = DistributedMLXBridge()

        mock_resp = MagicMock()
        mock_resp.status_code = 500

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            ok = await bridge.sync_weights("test", "n1", ["n2"])
            assert ok is False

    @pytest.mark.asyncio
    async def test_sync_weights_connection_error_mock(self):
        bridge = DistributedMLXBridge()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=Exception("timeout"))

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            ok = await bridge.sync_weights("test", "n1", ["n2"])
            assert ok is False

    @pytest.mark.asyncio
    async def test_sync_weights_multiple_targets_mock(self):
        bridge = DistributedMLXBridge()

        call_count = 0

        async def mock_post(url, json=None):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.status_code = 200 if call_count == 1 else 500
            return resp

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = mock_post

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            ok = await bridge.sync_weights("test", "n1", ["n2", "n3"])
            assert ok is False

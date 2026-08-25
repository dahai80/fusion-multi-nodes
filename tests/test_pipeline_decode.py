"""H1 PIPELINE decode 单元测试 — mock httpx, 不依赖真 fusion-mlx。

上游 fusion-mlx /distributed/decode 端点 (issue #630) 未落地 → 调用必 404。
本测验覆盖:
1. FusionMLXBackend.decode POST /distributed/decode 带 Bearer 鉴权头 (mock client)
2. DistributedMLXBridge.pipeline_inference decode 404 → fallback decoded=False + 返隐藏状态
3. pipeline_inference decode 200 → decoded=True 取 output
4. pipeline_inference decode 异常 → fallback decoded=False (不 crash)
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from fusion_multi_node.agent.node_agent import FusionMLXBackend
from fusion_multi_node.distributed_mlx.distributed_bridge import DistributedMLXBridge

logger = logging.getLogger(__name__)


def _mock_resp(status_code: int, json_body: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    return resp


class TestFusionMLXBackendDecode:
    """FusionMLXBackend.decode — POST /distributed/decode 鉴权 + 返值。"""

    @pytest.mark.asyncio
    async def test_decode_sends_bearer_auth(self):
        backend = FusionMLXBackend(base_url="http://127.0.0.1:11432", api_key="dahai168")
        mock_client = AsyncMock()
        mock_client.is_closed = False
        mock_client.post = AsyncMock(
            return_value=_mock_resp(200, {"output": "tok", "shard_id": "0"})
        )
        with patch.object(backend, "_get_client", AsyncMock(return_value=mock_client)):
            result = await backend.decode(shard_id="0", hidden_states="hs-base64", max_tokens=1)

        mock_client.post.assert_awaited_once()
        call_kwargs = mock_client.post.call_args
        url = call_kwargs.args[0]
        headers = call_kwargs.kwargs["headers"]
        body = call_kwargs.kwargs["json"]
        assert url == "http://127.0.0.1:11432/distributed/decode"
        assert headers["Authorization"] == "Bearer dahai168"
        assert body == {"shard_id": "0", "hidden_states": "hs-base64", "max_tokens": 1}
        assert result == {"output": "tok", "shard_id": "0"}
        logger.info("H1 decode 单测: Bearer 鉴权头 + body + 返值 通过")

    @pytest.mark.asyncio
    async def test_decode_no_api_key_omits_auth(self, monkeypatch):
        monkeypatch.delenv("FUSION_MLX_API_KEY", raising=False)
        backend = FusionMLXBackend(base_url="http://127.0.0.1:11432")
        mock_client = AsyncMock()
        mock_client.is_closed = False
        mock_client.post = AsyncMock(return_value=_mock_resp(200, {"output": "x"}))
        with patch.object(backend, "_get_client", AsyncMock(return_value=mock_client)):
            await backend.decode(shard_id="s1", hidden_states="hs", max_tokens=2)
        headers = mock_client.post.call_args.kwargs["headers"]
        assert "Authorization" not in headers
        assert headers["Content-Type"] == "application/json"

    @pytest.mark.asyncio
    async def test_decode_404_raises_for_status(self):
        backend = FusionMLXBackend(base_url="http://127.0.0.1:11432", api_key="k")
        mock_client = AsyncMock()
        mock_client.is_closed = False
        resp404 = _mock_resp(404)
        err = httpx.HTTPStatusError("404", request=MagicMock(), response=resp404)
        resp404.raise_for_status = MagicMock(side_effect=err)
        mock_client.post = AsyncMock(return_value=resp404)
        with patch.object(backend, "_get_client", AsyncMock(return_value=mock_client)):
            with pytest.raises(httpx.HTTPStatusError):
                await backend.decode(shard_id="0", hidden_states="hs")


class TestPipelineDecodeFallback:
    """DistributedMLXBridge.pipeline_inference — decode 端点未就绪 fallback。

    node_chain 经 forward 链 (/distributed/pipeline_step) 跑通后, 末段调 decode。
    #630 未落地 → 404/异常 → decoded=False, output=hidden_states (末段 forward 输出)。
    """

    @pytest.mark.asyncio
    async def test_pipeline_decode_404_returns_fallback(self):
        bridge = DistributedMLXBridge()
        mock_client = AsyncMock()
        mock_client.is_closed = False

        # forward 链: 单节点 pipeline_step 返 output=hidden_out
        forward_resp = _mock_resp(200, {"output": "hidden_out"})
        # decode: 404 (上游 #630 未落地)
        decode_resp_404 = _mock_resp(404)
        mock_client.post = AsyncMock(side_effect=[forward_resp, decode_resp_404])

        with patch.object(bridge, "_get_http_client", AsyncMock(return_value=mock_client)):
            result = await bridge.pipeline_inference(
                model_name="llama-1b",
                prompt="hello",
                node_chain=["127.0.0.1"],
                fusion_mlx_port=11434,
            )

        assert result["pipeline_id"].startswith("pipe_")
        assert result["decoded"] is False
        assert result["output"] == "hidden_out"
        assert result["nodes"] == 1
        logger.info("H1 pipeline decode 404 fallback 通过: decoded=False, output=hidden_states")

    @pytest.mark.asyncio
    async def test_pipeline_decode_200_returns_decoded(self):
        bridge = DistributedMLXBridge()
        mock_client = AsyncMock()
        mock_client.is_closed = False

        forward_resp = _mock_resp(200, {"output": "hidden_final"})
        decode_resp_ok = _mock_resp(200, {"output": "decoded_token_str"})
        mock_client.post = AsyncMock(side_effect=[forward_resp, decode_resp_ok])

        with patch.object(bridge, "_get_http_client", AsyncMock(return_value=mock_client)):
            result = await bridge.pipeline_inference(
                model_name="llama-1b",
                prompt="hello",
                node_chain=["10.0.0.2"],
                fusion_mlx_port=11434,
            )

        assert result["decoded"] is True
        assert result["output"] == "decoded_token_str"
        assert result["nodes"] == 1
        logger.info("H1 pipeline decode 200 真解码通过: decoded=True")

    @pytest.mark.asyncio
    async def test_pipeline_decode_exception_returns_fallback(self):
        bridge = DistributedMLXBridge()
        mock_client = AsyncMock()
        mock_client.is_closed = False

        forward_resp = _mock_resp(200, {"output": "hidden_after"})
        mock_client.post = AsyncMock(
            side_effect=[forward_resp, httpx.ConnectError("decode endpoint refused")]
        )

        with patch.object(bridge, "_get_http_client", AsyncMock(return_value=mock_client)):
            result = await bridge.pipeline_inference(
                model_name="llama-1b",
                prompt="hello",
                node_chain=["10.0.0.3"],
                fusion_mlx_port=11434,
            )

        assert result["decoded"] is False
        assert result["output"] == "hidden_after"
        logger.info("H1 pipeline decode 异常 fallback 通过: decoded=False, 不 crash")

    @pytest.mark.asyncio
    async def test_pipeline_decode_body_has_shard_id_and_max_tokens(self):
        bridge = DistributedMLXBridge()
        mock_client = AsyncMock()
        mock_client.is_closed = False

        # 2 节点: forward 链 2 次 (各返 output) + decode 1 次 = 共 3 次 post
        forward_resp0 = _mock_resp(200, {"output": "hs_inter"})
        forward_resp1 = _mock_resp(200, {"output": "hs_final"})
        decode_resp = _mock_resp(200, {"output": "tok"})
        mock_client.post = AsyncMock(side_effect=[forward_resp0, forward_resp1, decode_resp])

        with patch.object(bridge, "_get_http_client", AsyncMock(return_value=mock_client)):
            await bridge.pipeline_inference(
                model_name="m",
                prompt="p",
                node_chain=["10.0.0.4", "10.0.0.5"],
                fusion_mlx_port=11434,
            )

        decode_call = mock_client.post.call_args_list[-1]
        body = decode_call.kwargs["json"]
        assert body["shard_id"] == 1  # 末段 shard_id = len(node_chain)-1
        assert body["hidden_states"] == "hs_final"
        assert body["max_tokens"] == 1

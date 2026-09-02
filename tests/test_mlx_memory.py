"""#64: fetch_mlx_memory 显存采集测试 — mock httpx, 验解析 + 容错。"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from fusion_multi_node.agent.mlx_memory import fetch_mlx_memory, fetch_mlx_memory_sync


def _ok_response(payload: dict) -> httpx.Response:
    req = httpx.Request("GET", "http://x/v1/health")
    return httpx.Response(200, json=payload, request=req)


@pytest.mark.asyncio
async def test_fetch_mlx_memory_parses_memory_block():
    payload = {
        "memory": {
            "mlx_active_bytes": 2 * 1024**3,
            "mlx_cache_bytes": 1 * 1024**3,
            "mlx_peak_bytes": 3 * 1024**3,
            "total_bytes": 16 * 1024**3,
            "oom_risk": False,
        }
    }
    client = AsyncMock()
    client.get = AsyncMock(return_value=_ok_response(payload))
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    with patch("fusion_multi_node.agent.mlx_memory.httpx.AsyncClient", return_value=client):
        res = await fetch_mlx_memory("http://127.0.0.1:11434", api_key="k")
    assert res is not None
    assert res["active_gb"] == 2.0
    assert res["cache_gb"] == 1.0
    assert res["total_gb"] == 16.0
    assert res["oom_risk"] is False
    # api_key 传入时 Authorization 头存在
    _, kwargs = client.get.call_args
    assert kwargs["headers"]["Authorization"] == "Bearer k"


@pytest.mark.asyncio
async def test_fetch_mlx_memory_no_key_omits_auth_header():
    payload = {"memory": {"mlx_active_bytes": 0, "total_bytes": 0}}
    client = AsyncMock()
    client.get = AsyncMock(return_value=_ok_response(payload))
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    with patch("fusion_multi_node.agent.mlx_memory.httpx.AsyncClient", return_value=client):
        res = await fetch_mlx_memory("http://127.0.0.1:11434")
    assert res is not None
    _, kwargs = client.get.call_args
    assert "Authorization" not in kwargs["headers"]


@pytest.mark.asyncio
async def test_fetch_mlx_memory_non_200_returns_none():
    req = httpx.Request("GET", "http://x/v1/health")
    client = AsyncMock()
    client.get = AsyncMock(return_value=httpx.Response(404, request=req))
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    with patch("fusion_multi_node.agent.mlx_memory.httpx.AsyncClient", return_value=client):
        res = await fetch_mlx_memory("http://127.0.0.1:11434", api_key="k")
    assert res is None


@pytest.mark.asyncio
async def test_fetch_mlx_memory_missing_memory_block_returns_none():
    payload = {"status": "ok"}
    client = AsyncMock()
    client.get = AsyncMock(return_value=_ok_response(payload))
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    with patch("fusion_multi_node.agent.mlx_memory.httpx.AsyncClient", return_value=client):
        res = await fetch_mlx_memory("http://127.0.0.1:11434")
    assert res is None


@pytest.mark.asyncio
async def test_fetch_mlx_memory_connect_error_returns_none():
    client = AsyncMock()
    client.get = AsyncMock(side_effect=httpx.ConnectError("refused"))
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    with patch("fusion_multi_node.agent.mlx_memory.httpx.AsyncClient", return_value=client):
        res = await fetch_mlx_memory("http://127.0.0.1:11434")
    assert res is None


def test_fetch_mlx_memory_sync_parses():
    payload = {"memory": {"mlx_active_bytes": 4 * 1024**3, "total_bytes": 8 * 1024**3}}
    client = MagicMock()
    client.get = MagicMock(return_value=_ok_response(payload))
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=None)
    with patch("fusion_multi_node.agent.mlx_memory.httpx.Client", return_value=client):
        res = fetch_mlx_memory_sync("http://127.0.0.1:11434", api_key="k")
    assert res is not None
    assert res["active_gb"] == 4.0
    assert res["total_gb"] == 8.0

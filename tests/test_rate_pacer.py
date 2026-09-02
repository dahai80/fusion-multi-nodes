"""GAP-6 客户端限流适配测试 — 429 退避重试。

验证:
(a) 非 429 响应直接返回 (不退避)。
(b) 429 间歇后非 429 → 重试到成功。
(c) 429 持续 → 耗尽预算抛 RateLimitExhausted。
(d) Retry-After 头解析 (秒数 / HTTP-date / 缺失回落)。
(e) PacerConfig 退避确定性 (指数, 上限, 无随机)。
(f) FusionMLXBackend.chat 429 退避重试到成功。
(g) FusionMLXBackend.chat 429 耗尽 → RateLimitExhausted (非 HTTPStatusError)。
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock

import httpx
import pytest

from fusion_multi_node.agent.node_agent import FusionMLXBackend
from fusion_multi_node.agent.rate_pacer import (
    PacerConfig,
    RateLimitExhausted,
    dispatch_with_pacing,
    parse_retry_after,
)


def _resp(status: int, retry_after: str | None = None) -> httpx.Response:
    headers = httpx.Headers({"retry-after": retry_after} if retry_after else {})
    return httpx.Response(status, headers=headers, text="body")


class TestPacerConfig:
    def test_backoff_exponential_capped(self):
        p = PacerConfig(initial_backoff=0.5, max_backoff=5.0)
        assert p.next_backoff(0) == 0.5
        assert p.next_backoff(1) == 1.0
        assert p.next_backoff(2) == 2.0
        assert p.next_backoff(3) == 4.0
        assert p.next_backoff(4) == 5.0  # cap
        assert p.next_backoff(10) == 5.0  # still cap

    def test_defaults(self):
        p = PacerConfig()
        assert p.max_retries == 3
        assert p.budget_seconds == 10.0
        assert p.initial_backoff > 0


class TestParseRetryAfter:
    def test_seconds(self):
        r = _resp(429, "2.5")
        assert parse_retry_after(r) == 2.5

    def test_int_seconds(self):
        r = _resp(429, "3")
        assert parse_retry_after(r) == 3.0

    def test_missing_header_fallback(self):
        r = _resp(429)
        assert parse_retry_after(r) == 1.0

    def test_garbage_fallback(self):
        r = _resp(429, "not-a-date-or-number")
        assert parse_retry_after(r) == 1.0

    def test_negative_clamped(self):
        r = _resp(429, "-5")
        assert parse_retry_after(r) == 0.0


class TestDispatchWithPacing:
    @pytest.mark.asyncio
    async def test_non_429_returns_immediately(self):
        calls = []

        async def send():
            calls.append(time.time())
            return _resp(200)

        p = PacerConfig(max_retries=3, initial_backoff=0.01)
        resp = await dispatch_with_pacing(send, p)
        assert resp.status_code == 200
        assert len(calls) == 1  # no retry

    @pytest.mark.asyncio
    async def test_429_then_success_retries(self):
        responses = [_resp(429, "0.01"), _resp(429, "0.01"), _resp(200)]
        idx = {"i": 0}

        async def send():
            r = responses[idx["i"]]
            idx["i"] += 1
            return r

        p = PacerConfig(max_retries=3, initial_backoff=0.01, budget_seconds=5.0)
        resp = await dispatch_with_pacing(send, p)
        assert resp.status_code == 200
        assert idx["i"] == 3  # 2 retries then success

    @pytest.mark.asyncio
    async def test_429_exhausted_raises(self):
        async def send():
            return _resp(429, "0.01")

        p = PacerConfig(max_retries=2, initial_backoff=0.01, budget_seconds=5.0)
        with pytest.raises(RateLimitExhausted) as ei:
            await dispatch_with_pacing(send, p)
        assert ei.value.attempts == 3  # 1 + 2 retries
        assert ei.value.last_status == 429

    @pytest.mark.asyncio
    async def test_429_budget_cutoff_raises_before_max(self):
        # budget 极小 → 一次 429 后剩余预算耗尽, 不耗满 max_retries
        async def send():
            return _resp(429, "100")  # Retry-After 100s

        p = PacerConfig(max_retries=5, initial_backoff=0.01, budget_seconds=0.05)
        with pytest.raises(RateLimitExhausted):
            await dispatch_with_pacing(send, p)

    @pytest.mark.asyncio
    async def test_5xx_not_retried(self):
        # 非 429 错误码原样返回, 不退避重试
        calls = []

        async def send():
            calls.append(1)
            return _resp(500)

        p = PacerConfig(max_retries=3, initial_backoff=0.01)
        resp = await dispatch_with_pacing(send, p)
        assert resp.status_code == 500
        assert len(calls) == 1


class TestFusionMLXBackendPacing:
    @pytest.mark.asyncio
    async def test_chat_429_retry_then_success(self, monkeypatch):
        # 模拟 client.post: 首次 429, 二次 200 (含真实 JSON body)
        ok_body = {"choices": [{"message": {"content": "hello"}}], "usage": {}}
        ok_resp = httpx.Response(200, json=ok_body)
        ok_resp._request = httpx.Request("POST", "http://test/v1/chat/completions")
        responses = [
            _resp(429, "0.01"),
            ok_resp,
        ]
        idx = {"i": 0}

        def make_resp(*args, **kwargs):
            r = responses[idx["i"]]
            idx["i"] += 1
            return r

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=make_resp)
        backend = FusionMLXBackend(base_url="http://test", api_key="k")
        monkeypatch.setattr(backend, "_get_client", AsyncMock(return_value=mock_client))
        backend._pacer = PacerConfig(max_retries=3, initial_backoff=0.01, budget_seconds=5.0)

        data = await backend.chat(model="m", messages=[{"role": "user", "content": "hi"}])
        assert idx["i"] == 2  # retried once
        assert data["choices"][0]["message"]["content"] == "hello"

    @pytest.mark.asyncio
    async def test_chat_429_exhausted_raises_rate_limit(self, monkeypatch):
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_resp(429, "0.01"))
        backend = FusionMLXBackend(base_url="http://test", api_key="k")
        monkeypatch.setattr(backend, "_get_client", AsyncMock(return_value=mock_client))
        backend._pacer = PacerConfig(max_retries=1, initial_backoff=0.01, budget_seconds=5.0)

        with pytest.raises(RateLimitExhausted):
            await backend.chat(model="m", messages=[{"role": "user", "content": "hi"}])

    @pytest.mark.asyncio
    async def test_health_probe_carries_api_key_bearer(self, monkeypatch):
        # issue #60: FusionMLXBackend.health() 探 /v1/models 须带 Bearer api_key —
        # fusion-mlx 启用鉴权时无头恒 401, health() 恒 False 误判底座不健康。
        mock_resp = AsyncMock()
        mock_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        backend = FusionMLXBackend(base_url="http://mlx-host:11434", api_key="fg-admin-key")
        monkeypatch.setattr(backend, "_get_client", AsyncMock(return_value=mock_client))
        assert await backend.health() is True
        _, kwargs = mock_client.get.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer fg-admin-key"

    @pytest.mark.asyncio
    async def test_health_probe_no_key_omits_auth_header(self, monkeypatch):
        # 无 api_key → 不发 Authorization 头 (回退匿名探测, 兼容未启鉴权的 fusion-mlx)。
        monkeypatch.delenv("FUSION_MLX_API_KEY", raising=False)
        mock_resp = AsyncMock()
        mock_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        backend = FusionMLXBackend(base_url="http://mlx-host:11434")
        monkeypatch.setattr(backend, "_get_client", AsyncMock(return_value=mock_client))
        assert await backend.health() is True
        _, kwargs = mock_client.get.call_args
        # _dist_headers 在无 key 时不含 Authorization (仅 Content-Type)。
        assert "Authorization" not in kwargs["headers"]

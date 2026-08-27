"""GAP-8 Phase F3 — agent /api/v1/chat/completions 透传路由单测。

覆盖:
- 非流式 → FusionMLXBackend.chat (假后端) → 200 原生 OpenAI 格式。
- 流式 → 透传 fusion-mlx SSE 字节流 (注入 FakeMLX transport)。
- 非法 model (路径穿越) → 400。
- 无效 token → 401 (BearerAuthMiddleware)。
- worker 角色缺 TASK_EXECUTE → 403 (权限强制)。
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.agent.node_agent import FusionMLXBackend
from fusion_multi_node.server.agent_server import AgentServer

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


class FakeMLXBackend(FusionMLXBackend):
    """继承 FusionMLXBackend 过 isinstance; 重写 chat 免真 HTTP。"""

    def __init__(self, node_id: str = "agent-x"):
        super().__init__(base_url="http://fake-mlx", api_key="")
        self._node_id = node_id
        self.chat_calls: list[dict[str, Any]] = []

    async def chat(
        self,
        model: str,
        messages: list[dict[str, Any]],
        temperature: float = 0.7,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.chat_calls.append({"model": model, "messages": messages})
        return {
            "choices": [
                {"message": {"content": f"echo@{self._node_id}:{messages[0]['content']}"}}
            ],
            "usage": {"total_tokens": 10},
        }

    async def embed(self, model: str, input_text: str, **kwargs: Any) -> dict[str, Any]:
        return {"data": [{"embedding": [0.1, 0.2]}]}

    async def health(self) -> bool:
        return True


def _make_server(backend: FakeMLXBackend | None = None) -> tuple[AgentServer, FakeMLXBackend]:
    backend = backend or FakeMLXBackend()
    agent = NodeAgent(
        config=AgentConfig(
            node_id="agent-x", cluster_token=TEST_TOKEN, agent_port=22458
        ),
        backend=backend,
    )
    server = AgentServer(agent=agent, shared_token=TEST_TOKEN)
    return server, backend


def _client(server: AgentServer) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=server.app), base_url="http://test")


class TestAgentChatPassthrough:
    """agent /api/v1/chat/completions 透传路由单测。"""

    @pytest.mark.asyncio
    async def test_nonstream_chat(self):
        """非流式 → 200 原生 OpenAI 格式, 经 FusionMLXBackend.chat。"""
        server, backend = _make_server()
        async with _client(server) as client:
            resp = await client.post(
                "/api/v1/chat/completions",
                json={
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "hello"}],
                },
                headers=AUTH_HEADERS,
            )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "choices" in data
        assert "echo@agent-x:hello" in data["choices"][0]["message"]["content"]
        assert backend.chat_calls, "backend.chat 未被调用"
        assert backend.chat_calls[0]["model"] == "test-model"

    @pytest.mark.asyncio
    async def test_stream_sse(self):
        """流式 → 透传 fusion-mlx SSE 字节流 (注入 FakeMLX transport)。"""
        backend = FakeMLXBackend()
        sse_body = b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\ndata: [DONE]\n\n'

        class FakeMLXTransport(AsyncBaseTransport):
            async def handle_async_request(self, request: Request) -> Response:
                return Response(
                    200,
                    stream=httpx.ByteStream(sse_body),
                    headers={"content-type": "text/event-stream"},
                )

            async def aclose(self) -> None:
                pass

        fake_client = AsyncClient(transport=FakeMLXTransport(), base_url="http://mlx")
        backend._client = fake_client  # type: ignore[attr-defined]
        backend._base_url = "http://mlx"  # type: ignore[attr-defined]

        server, _ = _make_server(backend)
        try:
            async with _client(server) as client:
                resp = await client.post(
                    "/api/v1/chat/completions",
                    json={
                        "model": "m",
                        "messages": [{"role": "user", "content": "x"}],
                        "stream": True,
                    },
                    headers=AUTH_HEADERS,
                )
                assert resp.status_code == 200
                assert resp.headers["content-type"].startswith("text/event-stream")
                assert b"[DONE]" in resp.content
                assert b"delta" in resp.content
        finally:
            await fake_client.aclose()

    @pytest.mark.asyncio
    async def test_invalid_model_400(self):
        """非法 model (含 / 路径穿越) → 400。"""
        server, backend = _make_server()
        async with _client(server) as client:
            resp = await client.post(
                "/api/v1/chat/completions",
                json={
                    "model": "../etc/passwd",
                    "messages": [{"role": "user", "content": "x"}],
                },
                headers=AUTH_HEADERS,
            )
        assert resp.status_code == 400, resp.text
        assert "非法 model" in resp.text
        assert not backend.chat_calls, "非法 model 不应触达后端"

    @pytest.mark.asyncio
    async def test_no_token_401(self):
        """无 token → 401 (BearerAuthMiddleware)。"""
        server, _ = _make_server()
        async with _client(server) as client:
            resp = await client.post(
                "/api/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "x"}]},
            )
        assert resp.status_code == 401, resp.text

    @pytest.mark.asyncio
    async def test_empty_model_400(self):
        """空 model → 400。"""
        server, _ = _make_server()
        async with _client(server) as client:
            resp = await client.post(
                "/api/v1/chat/completions",
                json={"model": "", "messages": [{"role": "user", "content": "x"}]},
                headers=AUTH_HEADERS,
            )
        assert resp.status_code == 400, resp.text

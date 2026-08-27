"""GAP-8 Phase F3 — /v1/chat/completions 轻量代理 + 租户在途配额。

覆盖:
- USER 令牌非流式 → 200 原生 OpenAI 格式, 经 select_nodes 路由到选中节点 agent。
- 集群令牌亦放行 (内部调用, 无租户配额 gate)。
- VIEWER 令牌 → 403 (无 chat:complete 权限)。
- 租户在途配额满 (tenant_max_concurrent=1) → 第二并发 429 + 审计 chat_quota_exceeded。
- 无可用节点 → 503。
- 审计 actor=已认证 user_id, action=chat, node_id=选中节点。
- 流式 stream=true → SSE 透传 (text/event-stream)。
- 非法 model (路径穿越) → 400。

真实栈: master + agent 真 FastAPI app, master 派发经 PortRoutingTransport 按端口路由到
agent ASGITransport (真 FastAPI 栈, 免真 TCP)。agent 注入 FakeInferenceBackend (免真模型)。
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.agent.node_agent import FusionMLXBackend
from fusion_multi_node.master import ClusterMaster
from fusion_multi_node.security.audit_log import reset_audit_logger
from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-cluster-token"
CLUSTER_AUTH = {"Authorization": f"Bearer {TEST_TOKEN}"}
AGENT_PORT_A = 22445


class FakeInferenceBackend(FusionMLXBackend):
    """假推理后端 — 继承 FusionMLXBackend 过 isinstance 关, 重写 chat 返固定 OpenAI 格式。

    非流式: 重写 chat (免真 HTTP)。
    流式: route 走 _get_client + _base_url (测试注入 FakeMLX transport)。
    """

    def __init__(self, node_id: str):
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
        self.chat_calls.append(
            {"model": model, "messages": messages, "node_id": self._node_id}
        )
        return {
            "choices": [
                {
                    "message": {
                        "content": f"echo@{self._node_id}:{messages[0]['content']}"
                    }
                }
            ],
            "usage": {"total_tokens": 10},
        }

    async def embed(self, model: str, input_text: str, **kwargs: Any) -> dict[str, Any]:
        return {"data": [{"embedding": [0.1, 0.2]}]}

    async def health(self) -> bool:
        return True


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 agent ASGI app (真 FastAPI 栈, 免真 TCP)。"""

    def __init__(self, port_to_app: dict[int, Any]):
        self._port_to_app = port_to_app
        self._clients: dict[int, AsyncClient] = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
            for p, app in port_to_app.items()
        }

    async def handle_async_request(self, request: Request) -> Response:
        port = request.url.port
        client = self._clients.get(port)
        if client is None:
            return Response(404, text=f"no agent for port {port}")
        # stream=True — 保留响应 body 可逐块读 (SSE 透传场景), 不预读吞流。
        resp = await client.send(request, stream=True)
        return resp

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_agent_server(node_id: str, port: int) -> tuple[AgentServer, FakeInferenceBackend]:
    backend = FakeInferenceBackend(node_id)
    agent = NodeAgent(
        config=AgentConfig(node_id=node_id, cluster_token=TEST_TOKEN, agent_port=port),
        backend=backend,
    )
    server = AgentServer(agent=agent, shared_token=TEST_TOKEN)
    return server, backend


def _register_node(client: AsyncClient, node_id: str, port: int) -> Any:
    payload = {
        "node_id": node_id,
        "hostname": f"mac-{node_id}",
        "ip_address": "127.0.0.1",
        "port": port,
        "arch": "arm64",
        "total_memory_gb": 64.0,
        "available_memory_gb": 48.0,
        "cpu_cores": 12,
        "gpu_cores": 30,
    }
    return client.post("/api/nodes/register", json=payload, headers=CLUSTER_AUTH)


class TestChatProxy:
    """/v1/chat/completions 代理集成测试。"""

    @pytest.fixture
    async def cluster(self, tmp_path, monkeypatch):
        # user store + 审计隔离 (F2 同款)
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()

        # SSRF 放行 127.0.0.1 (测试作用域, 不动生产)
        monkeypatch.setattr(
            "fusion_multi_node.server.master_server.is_safe_peer_host",
            lambda host: True,
        )
        monkeypatch.setattr(
            "fusion_multi_node.server.master_server.build_safe_url",
            lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
        )
        monkeypatch.setattr(
            "fusion_multi_node.server.master_server.mtls_scheme",
            lambda: "http",
        )
        monkeypatch.setattr(
            "fusion_multi_node.server.master_server.mtls_client_kwargs",
            lambda: {},
        )

        server_a, backend_a = _make_agent_server("agent-a", AGENT_PORT_A)
        port_to_app = {AGENT_PORT_A: server_a.app}
        routing_transport = PortRoutingTransport(port_to_app)

        master = ClusterMaster(heartbeat_timeout=60.0)
        master_server = MasterServer(master=master, shared_token=TEST_TOKEN)
        master_server._approval_manager = None

        # 派发 httpx 走端口路由 transport (master chat 路由复用 _get_dispatch_http)
        async def _fake_dispatch_http():
            return AsyncClient(transport=routing_transport, timeout=10.0)

        monkeypatch.setattr(master, "_get_dispatch_http", _fake_dispatch_http)
        master._dispatch_token = TEST_TOKEN

        # 预建用户 (USER 可推理, VIEWER 不可)
        store = master_server._user_store
        assert store is not None
        store.create_user("alice", UserRole.USER)
        store.create_user("viewer1", UserRole.VIEWER)
        alice_token = store.issue_token("alice")
        viewer_token = store.issue_token("viewer1")

        master_app = master_server.app
        try:
            yield {
                "master": master,
                "master_server": master_server,
                "master_app": master_app,
                "server_a": server_a,
                "backend_a": backend_a,
                "routing_transport": routing_transport,
                "alice_token": alice_token,
                "viewer_token": viewer_token,
                "audit_path": str(tmp_path / "audit.log"),
            }
        finally:
            await master.stop()
            await routing_transport.aclose()

    async def _register(self, client: AsyncClient) -> None:
        r = await _register_node(client, "agent-a", AGENT_PORT_A)
        assert r.status_code == 200

    @staticmethod
    def _read_audit(audit_path: str) -> list[dict[str, Any]]:
        entries = []
        try:
            with open(audit_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        entries.append(json.loads(line))
        except FileNotFoundError:
            pass
        return entries

    @pytest.mark.asyncio
    async def test_user_nonstream_routed(self, cluster):
        """USER 令牌非流式 → 200 OpenAI 格式, 路由到 agent-a, content 嵌 node_id。"""
        async with AsyncClient(
            transport=ASGITransport(app=cluster["master_app"]), base_url="http://test"
        ) as client:
            await self._register(client)
            resp = await client.post(
                "/v1/chat/completions",
                json={
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "hello"}],
                },
                headers={"Authorization": f"Bearer {cluster['alice_token']}"},
            )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "choices" in data
        assert "echo@agent-a:hello" in data["choices"][0]["message"]["content"]
        assert cluster["backend_a"].chat_calls, "agent chat 未被调用"
        assert cluster["backend_a"].chat_calls[0]["model"] == "test-model"

    @pytest.mark.asyncio
    async def test_cluster_token_routed(self, cluster):
        """集群令牌亦放行 (内部调用, 无租户 gate) → 200。"""
        async with AsyncClient(
            transport=ASGITransport(app=cluster["master_app"]), base_url="http://test"
        ) as client:
            await self._register(client)
            resp = await client.post(
                "/v1/chat/completions",
                json={
                    "model": "m",
                    "messages": [{"role": "user", "content": "hi"}],
                },
                headers=CLUSTER_AUTH,
            )
        assert resp.status_code == 200, resp.text
        assert "echo@agent-a:hi" in resp.json()["choices"][0]["message"]["content"]

    @pytest.mark.asyncio
    async def test_viewer_forbidden(self, cluster):
        """VIEWER 令牌无 chat:complete 权限 → 403。"""
        async with AsyncClient(
            transport=ASGITransport(app=cluster["master_app"]), base_url="http://test"
        ) as client:
            await self._register(client)
            resp = await client.post(
                "/v1/chat/completions",
                json={
                    "model": "m",
                    "messages": [{"role": "user", "content": "x"}],
                },
                headers={"Authorization": f"Bearer {cluster['viewer_token']}"},
            )
        assert resp.status_code == 403, resp.text

    @pytest.mark.asyncio
    async def test_no_nodes_503(self, cluster):
        """无可用节点 (未注册) → 503。"""
        async with AsyncClient(
            transport=ASGITransport(app=cluster["master_app"]), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                json={
                    "model": "m",
                    "messages": [{"role": "user", "content": "x"}],
                },
                headers={"Authorization": f"Bearer {cluster['alice_token']}"},
            )
        assert resp.status_code == 503, resp.text
        assert "无可用推理节点" in resp.text

    @pytest.mark.asyncio
    async def test_quota_429(self, cluster):
        """租户在途配额满 (tenant_max_concurrent=1) → 第二并发 429 + 审计 chat_quota_exceeded。"""
        master = cluster["master"]
        master.configure_scheduling(tenant_max_concurrent=1)

        # 第一请求挂住占用槽 (sleep), 第二请求并发 → 配额满 429。
        real_chat = cluster["backend_a"].chat
        first_started = asyncio.Event()

        async def _slow_chat(*args, **kwargs):
            first_started.set()
            await asyncio.sleep(0.3)
            return await real_chat(*args, **kwargs)

        cluster["backend_a"].chat = _slow_chat  # type: ignore[assignment]

        async with AsyncClient(
            transport=ASGITransport(app=cluster["master_app"]), base_url="http://test"
        ) as client:
            await self._register(client)
            headers = {"Authorization": f"Bearer {cluster['alice_token']}"}
            # 并发两请求: gather 真并发调度, 第一挂住后第二命中配额 gate。
            r1_task = asyncio.ensure_future(client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "a"}]},
                headers=headers,
            ))
            await first_started.wait()
            resp2 = await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "b"}]},
                headers=headers,
            )
            resp1 = await r1_task
        assert resp1.status_code == 200, resp1.text
        assert resp2.status_code == 429, resp2.text
        assert "配额已满" in resp2.text
        # 审计记录 chat_quota_exceeded
        entries = self._read_audit(cluster["audit_path"])
        quota_denies = [e for e in entries if e.get("action") == "chat_quota_exceeded"]
        assert quota_denies, "缺 chat_quota_exceeded 审计"
        assert quota_denies[0]["actor"] == "alice"

    @pytest.mark.asyncio
    async def test_audit_actor_chat(self, cluster):
        """审计 actor=已认证 user_id, action=chat, node_id=选中节点。"""
        async with AsyncClient(
            transport=ASGITransport(app=cluster["master_app"]), base_url="http://test"
        ) as client:
            await self._register(client)
            resp = await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "x"}]},
                headers={"Authorization": f"Bearer {cluster['alice_token']}"},
            )
        assert resp.status_code == 200
        entries = self._read_audit(cluster["audit_path"])
        chats = [e for e in entries if e.get("action") == "chat"]
        assert chats, "缺 chat 审计记录"
        chat = chats[0]
        assert chat["actor"] == "alice"
        assert chat["node_id"] == "agent-a"
        assert chat["result"] == "ok"

    @pytest.mark.asyncio
    async def test_stream_sse(self, cluster):
        """流式 stream=true → SSE 透传 (text/event-stream), 透传 agent SSE 字节。"""
        # agent FakeInferenceBackend.chat 非流式; 流式走 backend._get_client 直连 fusion-mlx。
        # 测试内注入 FakeMLX transport 到 agent backend client, 返固定 SSE。
        backend = cluster["backend_a"]
        sse_body = b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\ndata: [DONE]\n\n'

        class FakeMLXTransport(AsyncBaseTransport):
            async def handle_async_request(self, request: Request) -> Response:
                # stream=ByteStream — body 可逐块读 (aiter_raw), content= 会预吞流。
                return Response(
                    200,
                    stream=httpx.ByteStream(sse_body),
                    headers={"content-type": "text/event-stream"},
                )

            async def aclose(self) -> None:
                pass

        fake_client = AsyncClient(transport=FakeMLXTransport(), base_url="http://mlx")
        backend._client = fake_client  # type: ignore[attr-defined]
        # _base_url 非流式走 chat; 流式 route 拼 {backend._base_url}/v1/chat/completions
        backend._base_url = "http://mlx"  # type: ignore[attr-defined]

        try:
            async with AsyncClient(
                transport=ASGITransport(app=cluster["master_app"]), base_url="http://test"
            ) as client:
                await self._register(client)
                resp = await client.post(
                    "/v1/chat/completions",
                    json={
                        "model": "m",
                        "messages": [{"role": "user", "content": "x"}],
                        "stream": True,
                    },
                    headers={"Authorization": f"Bearer {cluster['alice_token']}"},
                )
                assert resp.status_code == 200
                assert resp.headers["content-type"].startswith("text/event-stream")
                body = resp.content
                assert b"[DONE]" in body
                assert b'delta' in body
        finally:
            await fake_client.aclose()

    @pytest.mark.asyncio
    async def test_slot_released_after_request(self, cluster):
        """请求完成后槽释放 (非流式) → 后续请求不因前请求占槽 429。"""
        master = cluster["master"]
        master.configure_scheduling(tenant_max_concurrent=1)
        async with AsyncClient(
            transport=ASGITransport(app=cluster["master_app"]), base_url="http://test"
        ) as client:
            await self._register(client)
            headers = {"Authorization": f"Bearer {cluster['alice_token']}"}
            r1 = await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "a"}]},
                headers=headers,
            )
            r2 = await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "b"}]},
                headers=headers,
            )
        assert r1.status_code == 200 and r2.status_code == 200, (r1.text, r2.text)
        # 槽应归零
        assert master._inflight_chat.get("alice", 0) == 0

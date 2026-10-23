"""#84 E2E — agent-studio 跨节点会话 resume 的集群侧链路 (验收场景)。

真实链: MasterServer 真 FastAPI (session-store 路由 + sandbox 委派头) →
ClusterMaster 真调度 → PortRoutingTransport → 两 AgentServer 真 FastAPI (FakeBackend
离线推理, 会话存储为纯数据不依赖模型)。覆盖 issue #84 验收:
- 会话在"节点 A"侧 checkpoint 到集群存储 → "节点 B"侧水化 (transcript 一致);
- `sandbox: multi-node:<node>` 委派 (X-Fusion-Target-Node) 只在指定节点执行;
- 执行状态按 run_id 可查 (X-Fusion-Run-Id → /api/v1/runs/{run_id});
- B 侧回写后 A 侧可读到更新 (双向共享存储)。
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.agent.node_agent import InferenceBackend
from fusion_multi_node.master import ClusterMaster, TaskStatus
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer
from fusion_multi_node.storage import ClusterSessionStore

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}
AGENT_PORT_A = 22461
AGENT_PORT_B = 22462


class FakeBackend(InferenceBackend):
    def __init__(self):
        self.call_count = 0

    async def chat(self, model, messages, temperature=0.7, max_tokens=4096, **kwargs):
        self.call_count += 1
        return {"choices": [{"message": {"content": f"reply-{self.call_count}"}}], "usage": {"total_tokens": 1}}

    async def embed(self, model, input_text, **kwargs):
        self.call_count += 1
        return {"data": [{"embedding": [0.1]}]}

    async def health(self):
        return True


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 agent ASGI app (复用数据并行 E2E 约定)。"""

    def __init__(self, port_to_app: dict[int, object]):
        self._port_to_app = port_to_app
        self._clients: dict[int, AsyncClient] = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test") for p, app in port_to_app.items()
        }

    async def handle_async_request(self, request: Request) -> Response:
        port = request.url.port
        client = self._clients.get(port)
        if client is None:
            return Response(404, text=f"no agent for port {port}")
        return await client.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=dict(request.headers),
        )

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_agent_server(node_id: str, port: int, backend: FakeBackend) -> AgentServer:
    agent = NodeAgent(
        config=AgentConfig(
            node_id=node_id,
            cluster_token=TEST_TOKEN,
            agent_port=port,
        ),
        backend=backend,
    )
    return AgentServer(agent=agent, shared_token=TEST_TOKEN)


def _register_node(client: AsyncClient, node_id: str, port: int):
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
    return client.post("/api/nodes/register", json=payload, headers=AUTH_HEADERS)


async def _drain(master: ClusterMaster, timeout_s: float = 30.0) -> None:
    for _ in range(int(timeout_s / 0.05)):
        await asyncio.sleep(0.05)
        if (
            all(
                t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT)
                for t in master.tasks.values()
            )
            and not master._pending_queue
        ):
            return
    await asyncio.sleep(0.1)


class TestSessionResumeE2E:
    @pytest.fixture
    async def cluster(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.is_safe_peer_host",
            lambda host: True,
        )
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.build_safe_url",
            lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
        )
        backend_a = FakeBackend()
        backend_b = FakeBackend()
        server_a = _make_agent_server("agent-a", AGENT_PORT_A, backend_a)
        server_b = _make_agent_server("agent-b", AGENT_PORT_B, backend_b)
        routing_transport = PortRoutingTransport({AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app})

        master = ClusterMaster(heartbeat_timeout=60.0)
        master_server = MasterServer(master=master, shared_token=TEST_TOKEN)
        master_server._approval_manager = None
        master_server.session_store = ClusterSessionStore(str(tmp_path / "session-store"))

        async def _fake_dispatch_http():
            return AsyncClient(transport=routing_transport, timeout=30.0)

        monkeypatch.setattr(master, "_get_dispatch_http", _fake_dispatch_http)
        master._dispatch_token = TEST_TOKEN

        try:
            yield {
                "master": master,
                "master_server": master_server,
                "backend_a": backend_a,
                "backend_b": backend_b,
                "routing_transport": routing_transport,
            }
        finally:
            await master.stop()
            await routing_transport.aclose()

    async def test_cross_node_session_resume_and_delegation(self, cluster):
        master_app = cluster["master_server"].app
        master = cluster["master"]
        transport = ASGITransport(app=master_app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            assert (await _register_node(c, "agent-a", AGENT_PORT_A)).status_code == 200
            assert (await _register_node(c, "agent-b", AGENT_PORT_B)).status_code == 200

            # 1. "节点 A" 侧 (laptop-a) checkpoint 会话 transcript 到集群存储
            session_a = {
                "id": "sess-e2e",
                "title": "cross-node resume",
                "messages": [
                    {"role": "user", "content": "hello"},
                    {"role": "assistant", "content": "hi"},
                ],
                "metadata": {"origin_node": "laptop-a"},
            }
            put = await c.put("/api/v1/session-store/sess-e2e", json=session_a, headers=AUTH_HEADERS)
            assert put.status_code == 200

            # 2. "节点 B" 侧水化 — transcript 与 A 侧一致
            hydrated = await c.get("/api/v1/session-store/sess-e2e", headers=AUTH_HEADERS)
            assert hydrated.status_code == 200
            assert hydrated.json()["session"]["messages"] == session_a["messages"]

            # 3. sandbox 委派: 指定 agent-b 执行 (X-Fusion-Target-Node) + run-id 登记
            submit = await c.post(
                "/api/tasks/submit",
                json={
                    "name": "agent-studio-run",
                    "mode": "data",
                    "model_name": "test-model",
                    "prompt": "continue session",
                    "timeout_seconds": 60.0,
                },
                headers={
                    **AUTH_HEADERS,
                    "X-Fusion-Target-Node": "agent-b",
                    "X-Fusion-Run-Id": "run-e2e-1",
                },
            )
            assert submit.status_code == 200
            task_id = submit.json()["task_id"]
            assert submit.json()["assigned_nodes"] == ["agent-b"]

            # 4. 任务只在 agent-b 执行 (硬绑定, 不落 agent-a)
            await _drain(master)
            task = master.tasks[task_id]
            assert task.status == TaskStatus.COMPLETED
            assert task.assigned_nodes == ["agent-b"]
            assert cluster["backend_b"].call_count == 1
            assert cluster["backend_a"].call_count == 0

            # 5. run 状态按 run_id 可查 (attach/resume 链路)
            run = await c.get("/api/v1/runs/run-e2e-1", headers=AUTH_HEADERS)
            assert run.status_code == 200
            assert run.json()["task"]["task_id"] == task_id
            assert run.json()["task"]["status"] == "completed"

            # 6. B 侧执行后回写增量消息, A 侧可读 (双向共享)
            session_b = dict(hydrated.json()["session"])
            session_b["messages"].append({"role": "assistant", "content": "reply-1"})
            put_b = await c.put("/api/v1/session-store/sess-e2e", json=session_b, headers=AUTH_HEADERS)
            assert put_b.status_code == 200
            reread = await c.get("/api/v1/session-store/sess-e2e", headers=AUTH_HEADERS)
            assert len(reread.json()["session"]["messages"]) == 3

    async def test_delegation_to_unavailable_node_fails_visibly(self, cluster):
        """pinned 节点下线 → 提交 409, 绝不静默回落到其它节点 (sandbox 语义)。"""
        master_app = cluster["master_server"].app
        transport = ASGITransport(app=master_app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            assert (await _register_node(c, "agent-a", AGENT_PORT_A)).status_code == 200
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data", "model_name": "m"},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "agent-b"},
            )
            assert resp.status_code == 404
            assert cluster["backend_a"].call_count == 0

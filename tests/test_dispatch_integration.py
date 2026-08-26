"""P5 真实多节点派发集成测试 — submit→dispatch→COMPLETED 全链路。

覆盖 P1 修复的真实派发循环: MasterServer /api/tasks/submit → ClusterMaster 后台派发
→ httpx POST /api/execute → AgentServer 真实 FastAPI 路由 → NodeAgent.execute_task
→ 注入 FakeInferenceBackend (免真模型) → 回填 COMPLETED + result。

真实栈: 两个 AgentServer 真 FastAPI app (含 Bearer 鉴权/路由/execute_task),
MasterServer 真 FastAPI app。派发 httpx 经自定义 async transport 按端口路由到对应
agent ASGITransport (真 FastAPI 栈执行, 免真 TCP 端口抖动, 复用既有 ASGI 测试约定)。
SSRF 守卫 is_safe_peer_host 测试内 monkeypatch 放行 127.0.0.1 (仅测试作用域,
不削弱生产 SSRF 防护)。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.agent.node_agent import InferenceBackend
from fusion_multi_node.master import ClusterMaster, ClusterTask, ParallelMode, TaskStatus
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}

# Agent 占用端口 (测试内 transport 按端口路由, 不真实监听)
AGENT_PORT_A = 21445
AGENT_PORT_B = 21446


class FakeInferenceBackend(InferenceBackend):
    """假推理后端 — 返回固定 chat 响应, 免真模型加载。

    content 内嵌 node_id 供测试断言派发到达哪一节点。
    """

    def __init__(self, node_id: str):
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
        self.chat_calls.append({"model": model, "messages": messages, "node_id": self._node_id})
        return {
            "choices": [{"message": {"content": f"echo@{self._node_id}:{messages[0]['content']}"}}],
            "usage": {"total_tokens": 10},
        }

    async def embed(self, model: str, input_text: str, **kwargs: Any) -> dict[str, Any]:
        return {"data": [{"embedding": [0.1, 0.2]}]}

    async def health(self) -> bool:
        return True


class PortRoutingTransport(AsyncBaseTransport):
    """async transport — 按 URL 端口路由到对应 agent ASGI app。

    Master 派发建 URL 为 http://127.0.0.1:<port>/api/execute; 本 transport 解析端口,
    转交对应 agent ASGITransport 执行 (真 FastAPI 栈), 无匹配端口返回 404。
    """

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
        return await client.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=dict(request.headers),
        )

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_agent_server(node_id: str) -> tuple[AgentServer, FakeInferenceBackend]:
    """构造真实 AgentServer + 注入 FakeInferenceBackend 的 NodeAgent。"""
    backend = FakeInferenceBackend(node_id)
    agent = NodeAgent(
        config=AgentConfig(node_id=node_id, cluster_token=TEST_TOKEN, agent_port=AGENT_PORT_A),
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
    return client.post("/api/nodes/register", json=payload, headers=AUTH_HEADERS)


class TestDispatchIntegration:
    """真实多节点派发全链路集成测试。"""

    @pytest.fixture
    async def cluster(self, monkeypatch):
        """起 master + 两 agent, 派发走真 ASGI 路由, SSRF 测试内放行 127.0.0.1。"""
        # SSRF 测试放行 — 仅测试作用域, 不动生产防护
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.is_safe_peer_host",
            lambda host: True,
        )
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.build_safe_url",
            lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
        )

        server_a, backend_a = _make_agent_server("agent-a")
        server_b, backend_b = _make_agent_server("agent-b")
        port_to_app = {AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app}
        routing_transport = PortRoutingTransport(port_to_app)

        master = ClusterMaster(heartbeat_timeout=60.0)
        master_server = MasterServer(master=master, shared_token=TEST_TOKEN)
        master_server._approval_manager = None

        # 派发 httpx 客户端走端口路由 transport (真 ASGI 栈, 免真 TCP)
        async def _fake_dispatch_http():
            return AsyncClient(transport=routing_transport, timeout=10.0)

        monkeypatch.setattr(master, "_get_dispatch_http", _fake_dispatch_http)
        # 派发 token 用测试固定 token — 与 agent BearerAuthMiddleware 同源
        # (load_or_create_token 读真实集群 token 文件, 与 TEST_TOKEN 不一致 → 401)
        master._dispatch_token = TEST_TOKEN

        master_app = master_server.app
        try:
            yield {
                "master": master,
                "master_server": master_server,
                "master_app": master_app,
                "server_a": server_a,
                "server_b": server_b,
                "backend_a": backend_a,
                "backend_b": backend_b,
                "routing_transport": routing_transport,
            }
        finally:
            await master.stop()
            await routing_transport.aclose()

    async def _wait_status(
        self, client: AsyncClient, task_id: str, target: str, timeout_s: float = 5.0
    ) -> dict[str, Any]:
        """轮询任务直到进入 target 状态 (COMPLETED/FAILED) 或超时。"""
        for _ in range(int(timeout_s * 20)):
            resp = await client.get(f"/api/tasks/{task_id}", headers=AUTH_HEADERS)
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] == target:
                return data
            if data["status"] in ("FAILED", "COMPLETED", "TIMEOUT"):
                return data
            await asyncio.sleep(0.05)
        return data

    async def _register_both(self, client: AsyncClient) -> None:
        r1 = await _register_node(client, "agent-a", AGENT_PORT_A)
        r2 = await _register_node(client, "agent-b", AGENT_PORT_B)
        assert r1.status_code == 200 and r2.status_code == 200

    async def _drain_dispatch(self, master: ClusterMaster, timeout_s: float = 5.0) -> None:
        """等所有派发后台任务结束 (派发是 fire-and-forget asyncio.Task)。"""
        deadline_iters = int(timeout_s * 20)
        for _ in range(deadline_iters):
            pending = [t for t in master._dispatch_tasks.values() if not t.done()]
            if not pending:
                return
            await asyncio.sleep(0.05)

    @pytest.mark.asyncio
    async def test_data_parallel_two_node_dispatch(self, cluster):
        """DATA 并行两节点 — model_shards=[{},{}] 选两节点, 并发派发, 回填 COMPLETED + 两节点输出。

        证明 P1 派发全链路: assign_task → _trigger_dispatch → httpx POST /api/execute
        → AgentServer 真路由 → NodeAgent.execute_task → FakeBackend → _finalize_task。
        两节点都派发到达, active_tasks 派发后回降 0。
        """
        master = cluster["master"]
        backend_a = cluster["backend_a"]
        backend_b = cluster["backend_b"]

        transport = ASGITransport(app=cluster["master_app"])
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await self._register_both(client)

            # DATA + 两 shard → select_nodes count=2 → 选两节点
            task = ClusterTask(
                task_id="task-data-2node",
                name="integration-data",
                mode=ParallelMode.DATA,
                model_name="qwen-3b",
                model_shards=[{"id": "s0"}, {"id": "s1"}],
                task_type="inference",
                params={"prompt": "hello", "messages": [], "max_tokens": 64, "temperature": 0.7},
            )
            ok = await master.assign_task(task)
            assert ok
            assert task.assigned_nodes == ["agent-a", "agent-b"]

            await self._drain_dispatch(master)

            final = await master.get_task(task.task_id)

        assert final.status == TaskStatus.COMPLETED, f"期望 COMPLETED 实得 {final.status}: {final.error}"
        assert final.error == ""
        # 两节点各收到一次 chat
        assert len(backend_a.chat_calls) == 1
        assert len(backend_b.chat_calls) == 1
        assert backend_a.chat_calls[0]["messages"][0]["content"] == "hello"
        assert backend_b.chat_calls[0]["messages"][0]["content"] == "hello"
        # result 聚合两节点
        assert final.result["node_count"] == 2
        assert len(final.result["outputs"]) == 2
        contents = {o["content"] for o in final.result["outputs"]}
        assert contents == {"echo@agent-a:hello", "echo@agent-b:hello"}
        # active_tasks 派发后回降 0
        assert master.nodes["agent-a"].active_tasks == 0
        assert master.nodes["agent-b"].active_tasks == 0

    @pytest.mark.asyncio
    async def test_http_submit_route_dispatches(self, cluster):
        """HTTP /api/tasks/submit 路由也连派发链 — 单节点 DATA, 走真 FastAPI submit 路由。

        submit 路由默认 model_shards=[] → count=1 → 单节点 (路由未暴露 shard 字段,
        多节点须经 model_shards, 见 test_data_parallel_two_node_dispatch)。此处验证
        HTTP 提交入口同样触达真实派发, 非仅 assign_task 内存态。
        """
        master = cluster["master"]
        backend_a = cluster["backend_a"]

        transport = ASGITransport(app=cluster["master_app"])
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await self._register_both(client)

            resp = await client.post(
                "/api/tasks/submit",
                json={
                    "name": "http-submit",
                    "mode": "data",
                    "model_name": "qwen-3b",
                    "task_type": "inference",
                    "prompt": "ping",
                },
                headers=AUTH_HEADERS,
            )
            assert resp.status_code == 200, resp.text
            task_id = resp.json()["task_id"]
            assert len(resp.json()["assigned_nodes"]) == 1

            await self._drain_dispatch(master)
            final = await master.get_task(task_id)

        assert final.status == TaskStatus.COMPLETED, f"期望 COMPLETED 实得 {final.status}: {final.error}"
        assigned = final.assigned_nodes[0]
        hit_backend = backend_a if assigned == "agent-a" else cluster["backend_b"]
        assert len(hit_backend.chat_calls) == 1
        assert hit_backend.chat_calls[0]["messages"][0]["content"] == "ping"
        assert final.result["node_count"] == 1
        assert master.nodes[assigned].active_tasks == 0


class FailingInferenceBackend(InferenceBackend):
    """假推理后端 — chat 永远抛异常, 模拟 agent 内部错误 (OOM/坏模型)。

    execute_task 捕获后返 {"error": ...} → master _dispatch_to_node 走 logic_fail 路径
    (200+ok+result.error) → _dispatch_data 聚合为 PARTIAL (一成功一失败)。
    """

    async def chat(self, model, messages, temperature=0.7, max_tokens=4096, **kwargs):
        raise RuntimeError("simulated backend failure")

    async def embed(self, model, input_text, **kwargs):
        raise RuntimeError("simulated backend failure")

    async def health(self):
        return False


class TestPartialSuccess:
    """P3-29 (审计 §5.9): DATA 并行部分节点成功 → PARTIAL 终态, 保留部分结果。"""

    @pytest.fixture
    async def cluster_partial(self, monkeypatch):
        # SSRF 测试放行 — 仅测试作用域
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.is_safe_peer_host",
            lambda host: True,
        )
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.build_safe_url",
            lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
        )

        # agent-a 成功, agent-b 失败 → PARTIAL
        ok_backend = FakeInferenceBackend("agent-a")
        agent_a = NodeAgent(
            config=AgentConfig(node_id="agent-a", cluster_token=TEST_TOKEN, agent_port=AGENT_PORT_A),
            backend=ok_backend,
        )
        server_a = AgentServer(agent=agent_a, shared_token=TEST_TOKEN)

        fail_backend = FailingInferenceBackend()
        agent_b = NodeAgent(
            config=AgentConfig(node_id="agent-b", cluster_token=TEST_TOKEN, agent_port=AGENT_PORT_B),
            backend=fail_backend,
        )
        server_b = AgentServer(agent=agent_b, shared_token=TEST_TOKEN)

        port_to_app = {AGENT_PORT_A: server_a.app, AGENT_PORT_B: server_b.app}
        routing_transport = PortRoutingTransport(port_to_app)

        master = ClusterMaster(heartbeat_timeout=60.0)
        master_server = MasterServer(master=master, shared_token=TEST_TOKEN)
        master_server._approval_manager = None

        async def _fake_dispatch_http():
            return AsyncClient(transport=routing_transport, timeout=10.0)

        monkeypatch.setattr(master, "_get_dispatch_http", _fake_dispatch_http)
        master._dispatch_token = TEST_TOKEN

        try:
            yield {
                "master": master,
                "ok_backend": ok_backend,
                "routing_transport": routing_transport,
            }
        finally:
            await master.stop()
            await routing_transport.aclose()

    @pytest.mark.asyncio
    async def test_data_parallel_partial_success(self, cluster_partial):
        """DATA 并行: agent-a 成功 + agent-b 失败 → PARTIAL, 保留 agent-a 的 output。

        证明 P3-29 部分成功语义: 不浪费已成功节点工作, 不整任务 FAILED, 客户端可取
        result.outputs 的部分结果。全失败才 FAILED (逻辑错误不重试)。
        """
        master = cluster_partial["master"]
        ok_backend = cluster_partial["ok_backend"]
        master_server = MasterServer(master=master, shared_token=TEST_TOKEN)
        master_server._approval_manager = None

        transport = ASGITransport(app=master_server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r1 = await _register_node(client, "agent-a", AGENT_PORT_A)
            r2 = await _register_node(client, "agent-b", AGENT_PORT_B)
            assert r1.status_code == 200 and r2.status_code == 200

            task = ClusterTask(
                task_id="task-partial",
                name="partial-test",
                mode=ParallelMode.DATA,
                model_name="qwen-3b",
                model_shards=[{"id": "s0"}, {"id": "s1"}],
                task_type="inference",
                params={"prompt": "hello", "messages": [], "max_tokens": 64, "temperature": 0.7},
            )
            ok = await master.assign_task(task)
            assert ok
            assert task.assigned_nodes == ["agent-a", "agent-b"]

            await self._drain_dispatch(master)
            final = await master.get_task(task.task_id)

        assert final.status == TaskStatus.PARTIAL, (
            f"期望 PARTIAL 实得 {final.status}: {final.error}"
        )
        # 保留成功节点的部分结果
        assert len(final.result["outputs"]) == 1
        assert final.result["outputs"][0]["content"] == "echo@agent-a:hello"
        assert len(final.result["errors"]) == 1
        assert "agent-b" in final.result["errors"][0]
        # 成功节点确实执行
        assert len(ok_backend.chat_calls) == 1
        # active_tasks 回降
        assert master.nodes["agent-a"].active_tasks == 0
        assert master.nodes["agent-b"].active_tasks == 0

    async def _drain_dispatch(self, master: ClusterMaster, timeout_s: float = 5.0) -> None:
        deadline_iters = int(timeout_s * 20)
        for _ in range(deadline_iters):
            pending = [t for t in master._dispatch_tasks.values() if not t.done()]
            if not pending:
                return
            await asyncio.sleep(0.05)

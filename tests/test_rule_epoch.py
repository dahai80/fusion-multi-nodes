"""issue #52 原语 2 — 规则纪元广播测试。

3 类:
- TestRuleEpochMaster — 初始 0 / advance 增 / 查询 / advance 端点 ADMIN-only /
  get 端点 USER+VIEWER 可读 / receive 拒回退 / receive 等于幂等。
- TestRuleEpochBroadcast — advance 广播到 worker / agent 存 epoch /
  跳 unsafe peer / state_sync_loop 周期补漏 / standby 收。
- TestRuleEpochUserRbac — 用户令牌拒 advance / 集群令牌 ok。
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

from fusion_multi_node.master import ClusterMaster, NodeInfo
from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-epoch-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}
M_PORT = 11452
AG_PORT = 11458


class PortRoutingTransport(AsyncBaseTransport):
    """按 URL 端口路由到对应 ASGI app。"""

    def __init__(self, port_to_app: dict[int, object]):
        self._port_to_app = port_to_app
        self._clients = {
            p: AsyncClient(transport=ASGITransport(app=app), base_url="http://test") for p, app in port_to_app.items()
        }

    async def handle_async_request(self, request: Request) -> Response:
        port = request.url.port
        client = self._clients.get(port)
        if client is None:
            return Response(404, text=f"no app for port {port}")
        return await client.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=dict(request.headers),
        )

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()


def _make_master(tmp_path, host="127.0.0.1", port=M_PORT) -> ClusterMaster:
    m = ClusterMaster(host=host, port=port, heartbeat_timeout=60.0)
    m._task_store_path = tmp_path / f"tasks-{port}.json"
    m._election_state_path = tmp_path / f"election-{port}.json"
    m._dispatch_token = TEST_TOKEN
    return m


def _make_server(tmp_path, monkeypatch, *, users=None, token=TEST_TOKEN) -> tuple[MasterServer, dict]:
    monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
    monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
    monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
    from fusion_multi_node.security.audit_log import reset_audit_logger

    reset_audit_logger()
    server = MasterServer(shared_token=token)
    server._approval_manager = None
    tokens = {}
    if users:
        store = server._user_store
        for uid, role in users.items():
            store.create_user(uid, role)
            tokens[uid] = store.issue_token(uid)
    return server, tokens


def _client(server):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://test")


async def _register_node(master: ClusterMaster, node_id="n1", ip="10.0.0.1", port=AG_PORT):
    await master.register_node(
        NodeInfo(
            node_id=node_id,
            hostname="mac1",
            ip_address=ip,
            port=port,
            total_memory_gb=64.0,
            available_memory_gb=48.0,
            cpu_cores=12,
            gpu_cores=30,
            max_tasks=4,
        )
    )


class TestRuleEpochMaster:
    @pytest.mark.asyncio
    async def test_initial_epoch_zero(self, tmp_path):
        m = _make_master(tmp_path)
        assert await m.get_rule_epoch() == 0
        await m.stop()

    @pytest.mark.asyncio
    async def test_advance_increments(self, tmp_path):
        m = _make_master(tmp_path)
        await m.advance_rule_epoch("rule-v2")
        await m.advance_rule_epoch("rule-v3")
        assert await m.get_rule_epoch() == 2
        await m.stop()

    @pytest.mark.asyncio
    async def test_query_endpoint(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        await server.master.advance_rule_epoch("init")
        async with _client(server) as c:
            resp = await c.get("/api/v1/rules/epoch", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert resp.json()["epoch"] == 1
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_advance_endpoint_admin_only(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"root": UserRole.ADMIN, "bob": UserRole.USER})
        async with _client(server) as c:
            r_admin = await c.post(
                "/api/v1/rules/epoch/advance",
                json={"reason": "r"},
                headers={"Authorization": f"Bearer {tok['root']}"},
            )
            r_user = await c.post(
                "/api/v1/rules/epoch/advance",
                json={"reason": "r"},
                headers={"Authorization": f"Bearer {tok['bob']}"},
            )
        assert r_admin.status_code == 200, "ADMIN 可推进"
        assert r_admin.json()["epoch"] == 1
        assert r_user.status_code == 403, "USER 无 user:manage → 403"
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_get_endpoint_user_viewer_readable(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.USER, "v": UserRole.VIEWER})
        async with _client(server) as c:
            r_u = await c.get("/api/v1/rules/epoch", headers={"Authorization": f"Bearer {tok['bob']}"})
            r_v = await c.get("/api/v1/rules/epoch", headers={"Authorization": f"Bearer {tok['v']}"})
        assert r_u.status_code == 200 and r_v.status_code == 200, "cluster:stats → USER/VIEWER 可读"
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_receive_rejects_rollback(self, tmp_path):
        m = _make_master(tmp_path)
        await m.advance_rule_epoch("bump")  # epoch=1
        r = await m.receive_rule_epoch(0, "x")  # 回退 → 拒
        assert r["status"] == "rejected"
        assert await m.get_rule_epoch() == 1, "回退不改本地"
        await m.stop()

    @pytest.mark.asyncio
    async def test_receive_equal_idempotent(self, tmp_path):
        m = _make_master(tmp_path)
        await m.advance_rule_epoch("bump")  # epoch=1
        r = await m.receive_rule_epoch(1, "x")  # 等于 → 幂等 ok
        assert r["status"] == "ok"
        await m.stop()


class TestRuleEpochBroadcast:
    @pytest.mark.asyncio
    async def test_advance_broadcasts_to_worker(self, tmp_path, monkeypatch):
        # master advance → 广播到 worker node (实际指向 agent server) → agent 存 epoch。
        monkeypatch.setattr("fusion_multi_node.master.cluster_master.is_safe_peer_host", lambda h: True)
        monkeypatch.setattr(
            "fusion_multi_node.master.cluster_master.build_safe_url",
            lambda scheme, host, port, path: f"{scheme}://{host}:{port}{path}",
        )
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "agent-audit.log"))
        reset_audit_logger()
        agent = AgentServer(shared_token=TEST_TOKEN)
        m = _make_master(tmp_path)
        server = MasterServer(master=m, shared_token=TEST_TOKEN)
        server._approval_manager = None

        routing = PortRoutingTransport({AG_PORT: agent.app})

        async def _fake_http():
            return AsyncClient(transport=routing, timeout=5.0)

        monkeypatch.setattr(m, "_get_dispatch_http", _fake_http)

        await _register_node(m, "n1", "127.0.0.1", AG_PORT)
        await m.advance_rule_epoch("broadcast-test")  # 广播到 n1@AG_PORT
        await asyncio.sleep(0.1)  # best-effort 异步广播
        assert agent._rule_epoch == 1, "agent 须存收到的纪元"
        await m.stop()
        await routing.aclose()

    @pytest.mark.asyncio
    async def test_skips_unsafe_peer(self, tmp_path, monkeypatch):
        # unsafe peer (is_safe_peer_host=False) 不入广播目标。
        monkeypatch.setattr("fusion_multi_node.master.cluster_master.is_safe_peer_host", lambda h: False)
        m = _make_master(tmp_path)
        await _register_node(m, "evil", "169.254.1.1", AG_PORT)
        targets = await m._build_epoch_targets()
        assert targets == [], "unsafe peer 须跳过"
        await m.stop()

    @pytest.mark.asyncio
    async def test_receive_on_standby(self, tmp_path):
        # standby (非 leader) 接收纪元广播 → 接受超前纪元。
        m = _make_master(tmp_path, port=11453)
        m.setup_election(
            node_id="standby-1",
            priority=1,
            known_nodes=[{"node_id": "leader", "priority": 10, "ip_address": "127.0.0.1", "port": M_PORT}],
        )
        m._is_leader = False
        r = await m.receive_rule_epoch(5, "leader")
        assert r["status"] == "ok"
        assert await m.get_rule_epoch() == 5
        await m.stop()

    @pytest.mark.asyncio
    async def test_non_leader_advance_rejected(self, tmp_path):
        m = _make_master(tmp_path, port=11453)
        m.setup_election(
            node_id="standby-1",
            priority=1,
            known_nodes=[{"node_id": "leader", "priority": 10, "ip_address": "127.0.0.1", "port": M_PORT}],
        )
        m._is_leader = False
        with pytest.raises(RuntimeError):
            await m.advance_rule_epoch("forbidden")
        await m.stop()


class TestRuleEpochUserRbac:
    @pytest.mark.asyncio
    async def test_user_token_denied_advance(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.USER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/rules/epoch/advance",
                json={"reason": "x"},
                headers={"Authorization": f"Bearer {tok['bob']}"},
            )
        assert resp.status_code == 403
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_cluster_token_advance_ok(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post("/api/v1/rules/epoch/advance", json={"reason": "internal"}, headers=AUTH_HEADERS)
        assert resp.status_code == 200
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_receive_epoch_endpoint_internal(self, tmp_path, monkeypatch):
        # /api/rules/epoch 接收端 (CLUSTER_INTERNAL) — 集群令牌 ok, 用户令牌 403。
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.ADMIN})
        async with _client(server) as c:
            r_cl = await c.post("/api/rules/epoch", json={"epoch": 7, "source": "x"}, headers=AUTH_HEADERS)
            r_usr = await c.post(
                "/api/rules/epoch",
                json={"epoch": 8, "source": "x"},
                headers={"Authorization": f"Bearer {tok['alice']}"},
            )
        assert r_cl.status_code == 200, "集群令牌接收端 ok"
        assert r_usr.status_code == 403, "用户令牌调集群内部接收端 → 403"
        await server.master.stop()

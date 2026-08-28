"""issue #52 原语 3 — confirm 中继 + MAC + RBAC 测试。

3 类:
- TestConfirmRelayMac — POST 存 / 坏 MAC 401 / 缺 MAC 401 / 幂等覆盖 / GET 聚合 /
  GET 按 epoch 过滤 / GET 空。
- TestConfirmRelayRbac — 用户令牌拒 POST / 拒 GET / 集群令牌 ok。
- TestConfirmRelayHelper — post_confirm 往返 / 坏 MAC 401。
"""

from __future__ import annotations

import httpx
import pytest

from fusion_multi_node.master import ClusterMaster
from fusion_multi_node.security.cluster_key import canonical_json, derive_confirm_relay_key, mac_payload, post_confirm
from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-confirm-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


def _make_master(tmp_path, port=11452) -> ClusterMaster:
    m = ClusterMaster(host="127.0.0.1", port=port, heartbeat_timeout=60.0)
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
    # receive_confirm 经 master._get_dispatch_token() 派生 MAC 密钥 — 须注入同 token,
    # 否则读 token 文件 (与 shared_token 异) → MAC 不匹配 401。
    server.master._dispatch_token = token
    tokens = {}
    if users:
        store = server._user_store
        for uid, role in users.items():
            store.create_user(uid, role)
            tokens[uid] = store.issue_token(uid)
    return server, tokens


def _client(server):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://test")


def _good_mac(confirm_id, node_id, action, epoch, ts):
    """用同 cluster_token 派生密钥构合法 MAC (与 master.receive_confirm 同源)。"""
    key = derive_confirm_relay_key(TEST_TOKEN)
    payload = {"confirm_id": confirm_id, "node_id": node_id, "action": action, "epoch": epoch, "ts": ts}
    return mac_payload(key, canonical_json(payload))


def _confirm_payload(confirm_id="c1", node_id="n1", action="apply_rule", epoch=1, ts="2026-08-28T00:00:00Z"):
    return {
        "confirm_id": confirm_id,
        "node_id": node_id,
        "action": action,
        "epoch": epoch,
        "ts": ts,
        "mac": _good_mac(confirm_id, node_id, action, epoch, ts),
    }


class TestConfirmRelayMac:
    @pytest.mark.asyncio
    async def test_post_stores(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post("/api/confirm", json=_confirm_payload(), headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        confirms = await server.master.get_confirms()
        assert len(confirms) == 1
        assert confirms[0]["confirm_id"] == "c1"
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_bad_mac_401(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        payload = _confirm_payload()
        payload["mac"] = "0" * 64  # 坏 MAC
        async with _client(server) as c:
            resp = await c.post("/api/confirm", json=payload, headers=AUTH_HEADERS)
        assert resp.status_code == 401
        assert await server.master.get_confirms() == [], "坏 MAC 不存"
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_missing_mac_401(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        payload = _confirm_payload()
        del payload["mac"]
        async with _client(server) as c:
            resp = await c.post("/api/confirm", json=payload, headers=AUTH_HEADERS)
        assert resp.status_code == 422, "Pydantic mac:str 缺字段 → 422 (FastAPI 校验先于 handler)"
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_idempotent_overwrites(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await c.post("/api/confirm", json=_confirm_payload(action="v1"), headers=AUTH_HEADERS)
            await c.post("/api/confirm", json=_confirm_payload(action="v2"), headers=AUTH_HEADERS)
        confirms = await server.master.get_confirms()
        assert len(confirms) == 1, "同 (confirm_id, node_id) 覆盖不重复"
        assert confirms[0]["action"] == "v2"
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_get_aggregates(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await c.post("/api/confirm", json=_confirm_payload("c1", "n1"), headers=AUTH_HEADERS)
            await c.post("/api/confirm", json=_confirm_payload("c2", "n2"), headers=AUTH_HEADERS)
            resp = await c.get("/api/v1/confirms", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 2
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_get_filter_by_epoch(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await c.post("/api/confirm", json=_confirm_payload("c1", "n1", epoch=1), headers=AUTH_HEADERS)
            await c.post("/api/confirm", json=_confirm_payload("c2", "n2", epoch=2), headers=AUTH_HEADERS)
            resp = await c.get("/api/v1/confirms?epoch=2", headers=AUTH_HEADERS)
        body = resp.json()
        assert body["count"] == 1
        assert body["confirms"][0]["epoch"] == 2
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_get_empty(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.get("/api/v1/confirms", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        assert resp.json()["count"] == 0
        assert resp.json()["confirms"] == []
        await server.master.stop()


class TestConfirmRelayRbac:
    @pytest.mark.asyncio
    async def test_user_token_denied_post(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.ADMIN})
        async with _client(server) as c:
            resp = await c.post(
                "/api/confirm",
                json=_confirm_payload(),
                headers={"Authorization": f"Bearer {tok['alice']}"},
            )
        assert resp.status_code == 403, "/api/confirm = CLUSTER_INTERNAL → 用户令牌拒"
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_user_token_denied_get(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.ADMIN})
        async with _client(server) as c:
            resp = await c.get("/api/v1/confirms", headers={"Authorization": f"Bearer {tok['alice']}"})
        assert resp.status_code == 403, "/api/v1/confirms = CLUSTER_INTERNAL → 用户令牌拒"
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_cluster_token_ok(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            r_post = await c.post("/api/confirm", json=_confirm_payload(), headers=AUTH_HEADERS)
            r_get = await c.get("/api/v1/confirms", headers=AUTH_HEADERS)
        assert r_post.status_code == 200 and r_get.status_code == 200
        await server.master.stop()


class TestConfirmRelayHelper:
    @pytest.mark.asyncio
    async def test_post_confirm_roundtrip(self, tmp_path, monkeypatch):
        # post_confirm 助手经 ASGI transport 往返 master /api/confirm。
        server, _ = _make_server(tmp_path, monkeypatch)
        # 助手用真 httpx.AsyncClient — monkeypatch 经 transport 路由到 master app。
        # post_confirm 内惰性 import build_safe_url — 须 patch 源模块 auth 属性才拦得住。
        monkeypatch.setattr(
            "fusion_multi_node.utils.auth.build_safe_url",
            lambda scheme, host, port, path: f"http://{host}:{port}{path}",
        )
        from httpx import ASGITransport

        # patch 前绑真 AsyncClient — 否则 _FakeAsyncClient.__init__ 调被 patch 的名 = 无限递归。
        real_async_client = httpx.AsyncClient
        routing = ASGITransport(app=server.app)

        class _FakeAsyncClient:
            def __init__(self, *a, **kw):
                self._c = real_async_client(transport=routing, base_url="http://test", timeout=5.0)

            async def __aenter__(self):
                await self._c.__aenter__()
                return self._c

            async def __aexit__(self, *a):
                await self._c.__aexit__(*a)

        monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)
        result = await post_confirm(
            "127.0.0.1",
            11452,
            TEST_TOKEN,
            confirm_id="helper-c1",
            node_id="n1",
            action="apply_rule",
            epoch=3,
            ts="2026-08-28T01:00:00Z",
        )
        assert result["status"] == "ok"
        confirms = await server.master.get_confirms()
        assert any(c["confirm_id"] == "helper-c1" for c in confirms)
        await server.master.stop()

    @pytest.mark.asyncio
    async def test_post_confirm_bad_mac_rejected(self, tmp_path, monkeypatch):
        # 助手用错 token 派生密钥 → MAC 与 master 不匹配 → 401 → status=error code=401。
        server, _ = _make_server(tmp_path, monkeypatch)
        # post_confirm 内惰性 import build_safe_url — patch 源模块 auth 属性。
        monkeypatch.setattr(
            "fusion_multi_node.utils.auth.build_safe_url",
            lambda scheme, host, port, path: f"http://{host}:{port}{path}",
        )
        from httpx import ASGITransport

        real_async_client = httpx.AsyncClient
        routing = ASGITransport(app=server.app)

        class _FakeAsyncClient:
            def __init__(self, *a, **kw):
                self._c = real_async_client(transport=routing, base_url="http://test", timeout=5.0)

            async def __aenter__(self):
                await self._c.__aenter__()
                return self._c

            async def __aexit__(self, *a):
                await self._c.__aexit__(*a)

        monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)
        result = await post_confirm(
            "127.0.0.1",
            11452,
            "wrong-token",  # 错 token → 派生密钥异 → MAC 坏
            confirm_id="bad-c1",
            node_id="n1",
            action="apply_rule",
            epoch=1,
            ts="2026-08-28T01:00:00Z",
        )
        assert result["status"] == "error"
        assert result["code"] == 401
        await server.master.stop()

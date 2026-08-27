"""F5 (GAP-8 Phase F5): 令牌轮换 — 用户多活 + 集群 previous-active 重叠窗。

覆盖:
- 用户轮换: rotate 签新令牌旧令牌保留 (多活), 两者均通过鉴权; revoke 旧令牌 → 旧令牌 401 新令牌 200。
- 集群令牌滚动重启: FUSION_CLUSTER_TOKEN_PREVIOUS 设旧令牌 → current + previous 均接受 (200);
  未设 → previous 令牌 401 (单令牌行为不变)。
- 出站派发用 current 令牌 (非 previous) — 滚动重启期全节点先接受旧值, 再轮换 current。
"""

from __future__ import annotations

import httpx

from fusion_multi_node.security.audit_log import reset_audit_logger
from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.server.master_server import MasterServer


def _make_user_server(tmp_path, monkeypatch, *, users=None):
    monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
    monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
    monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
    reset_audit_logger()
    server = MasterServer(shared_token="cluster-tok")
    store = server._user_store
    assert store is not None
    tokens = {}
    for uid, role in (users or {}).items():
        store.create_user(uid, role)
        tokens[uid] = store.issue_token(uid)
    return server, tokens


def _client(server):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://test")


class TestUserTokenRotation:
    async def test_rotate_keeps_old_valid(self, tmp_path, monkeypatch):
        server, tok = _make_user_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        store = server._user_store
        old = tok["alice"]
        new = store.rotate_user_token("alice")
        async with _client(server) as c:
            r_old = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {old}"})
            r_new = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {new}"})
        assert r_old.status_code == 200
        assert r_new.status_code == 200

    async def test_revoke_old_after_rotate(self, tmp_path, monkeypatch):
        server, tok = _make_user_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        store = server._user_store
        old = tok["alice"]
        new = store.rotate_user_token("alice")
        old_tid = store.get_user("alice").tokens[0].tid
        assert store.revoke_token("alice", old_tid) is True
        async with _client(server) as c:
            r_old = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {old}"})
            r_new = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {new}"})
        assert r_old.status_code == 401
        assert r_new.status_code == 200

    async def test_rotate_route_returns_new_keeps_old(self, tmp_path, monkeypatch):
        server, tok = _make_user_server(tmp_path, monkeypatch, users={"admin": UserRole.ADMIN, "alice": UserRole.USER})
        old = tok["alice"]
        async with _client(server) as c:
            r = await c.post(
                "/api/v1/users/alice/tokens/rotate",
                json={"label": "rotated"},
                headers={"Authorization": f"Bearer {tok['admin']}"},
            )
            new = r.json()["token"]
            r_old = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {old}"})
            r_new = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {new}"})
        assert r.status_code == 200
        assert r.json()["token_shown_once"] is True
        assert r_old.status_code == 200
        assert r_new.status_code == 200


def _make_cluster_server(tmp_path, monkeypatch, *, current, previous=None):
    monkeypatch.setenv("FUSION_CLUSTER_TOKEN", current)
    if previous is not None:
        monkeypatch.setenv("FUSION_CLUSTER_TOKEN_PREVIOUS", previous)
    else:
        monkeypatch.delenv("FUSION_CLUSTER_TOKEN_PREVIOUS", raising=False)
    monkeypatch.delenv("FUSION_USERS_FILE", raising=False)
    monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
    monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
    reset_audit_logger()
    server = MasterServer(shared_token=current)
    return server


class TestClusterTokenRotation:
    async def test_previous_accepted_in_overlap(self, tmp_path, monkeypatch):
        current = "new-cluster-secret"
        previous = "old-cluster-secret"
        server = _make_cluster_server(tmp_path, monkeypatch, current=current, previous=previous)
        async with _client(server) as c:
            r_cur = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {current}"})
            r_prev = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {previous}"})
        assert r_cur.status_code == 200
        assert r_prev.status_code == 200

    async def test_previous_rejected_without_env(self, tmp_path, monkeypatch):
        current = "only-cluster-secret"
        server = _make_cluster_server(tmp_path, monkeypatch, current=current, previous=None)
        async with _client(server) as c:
            r_cur = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {current}"})
            r_prev = await c.get("/api/v1/nodes", headers={"Authorization": "Bearer stale-secret"})
        assert r_cur.status_code == 200
        assert r_prev.status_code == 401

    async def test_previous_equal_current_disabled(self, tmp_path, monkeypatch):
        # _PREVIOUS == current → 不开重叠窗 (避免误配双值一致)。
        # 此时另一不匹配令牌应 401 (未开额外接受口), current 仍 200。
        current = "same-secret"
        server = _make_cluster_server(tmp_path, monkeypatch, current=current, previous=current)
        async with _client(server) as c:
            r_cur = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {current}"})
            r_other = await c.get("/api/v1/nodes", headers={"Authorization": "Bearer other-secret"})
        assert r_cur.status_code == 200
        assert r_other.status_code == 401


class TestOutboundDispatchToken:
    async def test_dispatch_uses_current_not_previous(self, tmp_path, monkeypatch):
        # 出站派发 header 须用 current (load_or_create_token 读 FUSION_CLUSTER_TOKEN),
        # 非 previous — 滚动重启期对端已先接受旧值, 但本端出站始终发 current。
        from fusion_multi_node.master import ClusterMaster

        current = "cur-dispatch-secret"
        previous = "prev-dispatch-secret"
        monkeypatch.setenv("FUSION_CLUSTER_TOKEN", current)
        monkeypatch.setenv("FUSION_CLUSTER_TOKEN_PREVIOUS", previous)
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        master = ClusterMaster(heartbeat_timeout=60.0)
        token = master._get_dispatch_token()
        assert token == current
        assert token != previous

        # previous 令牌对入站仍有效 (中间件重叠窗) — 端到端确认出/入两端语义分离
        server = MasterServer(master=master, shared_token=current)
        async with _client(server) as c:
            r_prev = await c.get("/api/v1/nodes", headers={"Authorization": f"Bearer {previous}"})
        assert r_prev.status_code == 200

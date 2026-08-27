"""GAP-8 Phase F2 — 用户管理 CRUD (ADMIN-only)。

覆盖:
- ADMIN 建/查/删/改角色/签发/吊销/轮换用户与令牌。
- 非 ADMIN (USER/VIEWER) → 403。
- 集群令牌 → 403 (用户管理须 ADMIN 用户令牌)。
- 令牌明文仅签发/轮换时返回一次, 审计日志不含明文。
- 持久化跨重启 (同文件重载 UserStore 保留用户/令牌)。
- 自删拒绝; GET detail 不返回 token_hash/salt。
"""

from __future__ import annotations

import httpx

from fusion_multi_node.security.audit_log import AuditLogger, reset_audit_logger
from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.security.user_store import UserStore
from fusion_multi_node.server.master_server import MasterServer


def _make_server(tmp_path, monkeypatch):
    """建带 user_store 的 master + 预建 ADMIN root + USER alice, 返 (server, tokens)。"""
    monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
    monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
    monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
    reset_audit_logger()
    server = MasterServer(shared_token="cluster-tok")
    store = server._user_store
    assert store is not None
    store.create_user("root", UserRole.ADMIN)
    store.create_user("alice", UserRole.USER)
    tokens = {
        "root": store.issue_token("root", label="admin"),
        "alice": store.issue_token("alice"),
    }
    return server, tokens


def _client(server):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://test")


def _hdr(tok):
    return {"Authorization": f"Bearer {tok}"}


class TestUserCrudAdmin:
    async def test_create_user(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users",
                json={"user_id": "bob", "role": "user"},
                headers=_hdr(tok["root"]),
            )
        assert resp.status_code == 201
        assert resp.json() == {"status": "ok", "user_id": "bob", "role": "user"}

    async def test_list_users_no_hashes(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.get("/api/v1/users", headers=_hdr(tok["root"]))
        assert resp.status_code == 200
        body = resp.json()
        ids = {u["user_id"] for u in body["users"]}
        assert {"root", "alice"} <= ids
        # list_users 不含敏感哈希
        assert "token_hash" not in body["users"][0]
        assert "salt" not in body["users"][0]

    async def test_get_user_detail_no_hash(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.get("/api/v1/users/alice", headers=_hdr(tok["root"]))
        assert resp.status_code == 200
        body = resp.json()
        assert body["user_id"] == "alice"
        assert body["role"] == "user"
        # detail 不返回 token_hash/salt, 仅令牌元信息
        assert "token_hash" not in body
        assert "salt" not in body
        assert len(body["tokens"]) >= 1
        assert "tid" in body["tokens"][0]
        assert "label" in body["tokens"][0]

    async def test_delete_user(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.delete("/api/v1/users/alice", headers=_hdr(tok["root"]))
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    async def test_self_delete_rejected(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.delete("/api/v1/users/root", headers=_hdr(tok["root"]))
        assert resp.status_code == 400
        assert "不可删除自身" in resp.json()["detail"]

    async def test_update_role(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.put(
                "/api/v1/users/alice/role",
                json={"role": "admin"},
                headers=_hdr(tok["root"]),
            )
        assert resp.status_code == 200
        assert resp.json()["role"] == "admin"

    async def test_create_duplicate_rejected(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users",
                json={"user_id": "alice", "role": "user"},
                headers=_hdr(tok["root"]),
            )
        assert resp.status_code == 400
        assert "已存在" in resp.json()["detail"]

    async def test_create_bad_role_rejected(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users",
                json={"user_id": "x", "role": "superuser"},
                headers=_hdr(tok["root"]),
            )
        assert resp.status_code == 400
        assert "非法角色" in resp.json()["detail"]


class TestTokenIssueRevokeRotate:
    async def test_issue_token_shown_once(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users/alice/tokens",
                json={"label": "laptop"},
                headers=_hdr(tok["root"]),
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["token_shown_once"] is True
        assert body["token"].startswith("fmu_alice_")
        # 签发的令牌可用
        async with _client(server) as c:
            ok = await c.post(
                "/api/tasks/submit",
                json={"name": "t1"},
                headers=_hdr(body["token"]),
            )
        assert ok.status_code in (200, 202)

    async def test_issue_token_not_in_audit(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await c.post(
                "/api/v1/users/alice/tokens",
                json={"label": "laptop"},
                headers=_hdr(tok["root"]),
            )
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        issues = [e for e in al.read() if e["action"] == "token_issue"]
        assert len(issues) == 1
        # 审计 detail 不含明文令牌 (防日志泄露)
        assert "fmu_alice_" not in issues[0]["detail"]
        assert "token=" not in issues[0]["detail"]

    async def test_revoke_token(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        # 取 alice 现有令牌 tid
        rec = server._user_store.get_user("alice")
        tid = rec.tokens[0].tid
        async with _client(server) as c:
            resp = await c.delete(
                f"/api/v1/users/alice/tokens/{tid}",
                headers=_hdr(tok["root"]),
            )
        assert resp.status_code == 200
        # 吊销后令牌失效 → 401
        async with _client(server) as c:
            denied = await c.post(
                "/api/tasks/submit",
                json={"name": "t1"},
                headers=_hdr(tok["alice"]),
            )
        assert denied.status_code == 401

    async def test_rotate_token_keeps_old(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        old = tok["alice"]
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users/alice/tokens/rotate",
                json={"label": "rotated"},
                headers=_hdr(tok["root"]),
            )
        assert resp.status_code == 200
        new = resp.json()["token"]
        assert new.startswith("fmu_alice_")
        assert new != old
        # 多活: 旧令牌仍有效
        async with _client(server) as c:
            old_ok = await c.post(
                "/api/tasks/submit",
                json={"name": "t1"},
                headers=_hdr(old),
            )
            new_ok = await c.post(
                "/api/tasks/submit",
                json={"name": "t2"},
                headers=_hdr(new),
            )
        assert old_ok.status_code in (200, 202)
        assert new_ok.status_code in (200, 202)

    async def test_issue_unknown_user_404(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users/ghost/tokens",
                json={},
                headers=_hdr(tok["root"]),
            )
        assert resp.status_code == 404


class TestUserCrudAuthz:
    async def test_user_cannot_manage(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users",
                json={"user_id": "x", "role": "user"},
                headers=_hdr(tok["alice"]),
            )
        assert resp.status_code == 403

    async def test_cluster_token_cannot_manage(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users",
                json={"user_id": "x", "role": "user"},
                headers=_hdr("cluster-tok"),
            )
        assert resp.status_code == 403
        assert "ADMIN" in resp.json()["detail"]
        # 审计记录集群令牌越权
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        denies = [e for e in al.read() if e["action"] == "permission_deny"]
        assert any("集群令牌" in e["detail"] for e in denies)

    async def test_no_user_store_503(self, tmp_path, monkeypatch):
        # 不设 FUSION_USERS_FILE, 单租户零配置 → user_store None → 503
        monkeypatch.delenv("FUSION_USERS_FILE", raising=False)
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()
        # conftest autouse 已重定向 HOME, 无文件 → load_user_store None
        server = MasterServer(shared_token="cluster-tok")
        assert server._user_store is None
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/users",
                json={"user_id": "x", "role": "user"},
                headers=_hdr("cluster-tok"),
            )
        assert resp.status_code == 503


class TestPersistenceAcrossRestart:
    async def test_reload_preserves_users_tokens(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch)
        store = server._user_store
        # 建新用户 + 签令牌
        store.create_user("carol", UserRole.VIEWER)
        carol_tok = store.issue_token("carol", label="phone")
        users_file = str(store.path)
        # 模拟重启: 新 UserStore 同文件
        reloaded = UserStore(path=users_file)
        assert reloaded.get_user("root") is not None
        assert reloaded.get_user("carol") is not None
        assert reloaded.get_user("carol").role == UserRole.VIEWER
        # 令牌持久化 — 校验明文仍有效
        uid, role = reloaded.validate(carol_tok)
        assert uid == "carol"
        assert role == UserRole.VIEWER

"""GAP-8 Phase F2 — per-user RBAC + 防伪造审计。

覆盖:
- USER 令牌可提交/取消任务, 不可 migrate/degrade/manage users。
- VIEWER 令牌只读, 提交/取消 → 403。
- 用户令牌提交 → task.user=已认证 user_id (忽略客户端 req.user, 防伪造审计 actor)。
- 集群令牌路径不变 (node-RBAC, req.user 自声明)。
- 审计 actor=已认证 user_id (非伪造 req.user)。
"""

from __future__ import annotations

import httpx

from fusion_multi_node.security.audit_log import AuditLogger, reset_audit_logger
from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.server.master_server import MasterServer


def _make_server(tmp_path, monkeypatch, *, users=None):
    """建带 user_store 的 master + 预建用户, 返 (server, {user_id: token})。"""
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


class TestUserRbacTaskSubmit:
    async def test_user_can_submit(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t1", "model_name": "m"},
                headers={"Authorization": f"Bearer {tok['alice']}"},
            )
        assert resp.status_code in (200, 202)
        assert "task_id" in resp.json()

    async def test_viewer_cannot_submit(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t1"},
                headers={"Authorization": f"Bearer {tok['bob']}"},
            )
        assert resp.status_code == 403
        assert "权限不足" in resp.json()["detail"]

    async def test_viewer_cannot_cancel(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/ghost/cancel",
                json={"reason": "x"},
                headers={"Authorization": f"Bearer {tok['bob']}"},
            )
        # VIEWER 无 task:cancel → 403 (先于 404 任务不存在)
        assert resp.status_code == 403

    async def test_user_cannot_migrate(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/ghost/migrate",
                headers={"Authorization": f"Bearer {tok['alice']}"},
            )
        # USER 无 task:migrate → 403 (先于 node-RBAC)
        assert resp.status_code == 403

    async def test_admin_can_migrate(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"root": UserRole.ADMIN})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/ghost/migrate",
                headers={"Authorization": f"Bearer {tok['root']}"},
            )
        # ADMIN 有 task:migrate → 过用户层鉴权; 任务不存在 → 500 (migrate_task 失败)
        assert resp.status_code in (500, 404)

    async def test_user_cannot_degrade(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/ghost/degrade",
                headers={"Authorization": f"Bearer {tok['alice']}"},
            )
        assert resp.status_code == 403

    async def test_admin_can_degrade(self, tmp_path, monkeypatch):
        server, tok = _make_server(tmp_path, monkeypatch, users={"root": UserRole.ADMIN})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/ghost/degrade",
                headers={"Authorization": f"Bearer {tok['root']}"},
            )
        # ADMIN 有 task:degrade → 过鉴权; 任务不存在 → 400
        assert resp.status_code in (400, 404)


class TestTamperProofAudit:
    async def test_task_user_is_authenticated(self, tmp_path, monkeypatch):
        """用户令牌提交 → task.user=已认证 user_id, 忽略伪造 req.user。"""
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/submit",
                # 客户端伪造 user="rootadmin" — 须被忽略, 实际 task.user=alice
                json={"name": "t1", "user": "rootadmin"},
                headers={"Authorization": f"Bearer {tok['alice']}"},
            )
        assert resp.status_code in (200, 202)
        task_id = resp.json()["task_id"]
        task = await server.master.get_task(task_id)
        assert task is not None
        assert task.user == "alice"  # 已认证身份, 非 "rootadmin"

    async def test_audit_actor_is_authenticated(self, tmp_path, monkeypatch):
        """审计日志 actor=已认证 user_id, 非伪造 req.user。"""
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        async with _client(server) as c:
            await c.post(
                "/api/tasks/submit",
                json={"name": "t1", "user": "evil"},
                headers={"Authorization": f"Bearer {tok['alice']}"},
            )
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        submits = [e for e in al.read() if e["action"] == "task_submit"]
        assert len(submits) == 1
        assert submits[0]["actor"] == "alice"  # 非 "evil"

    async def test_user_deny_audited(self, tmp_path, monkeypatch):
        """VIEWER 提交被拒 → 审计 permission_deny, actor=viewer。"""
        server, tok = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            await c.post(
                "/api/tasks/submit",
                json={"name": "t1"},
                headers={"Authorization": f"Bearer {tok['bob']}"},
            )
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        denies = [e for e in al.read() if e["action"] == "permission_deny"]
        assert len(denies) == 1
        assert denies[0]["actor"] == "bob"
        assert "/api/tasks/submit" in denies[0]["detail"]


class TestClusterTokenBypass:
    async def test_cluster_token_submit_uses_req_user(self, tmp_path, monkeypatch):
        """集群令牌 (内部可信) → task.user=req.user (自声明), 用户层鉴权不拦。"""
        server, _ = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t1", "user": "internal-cli"},
                headers={"Authorization": "Bearer cluster-tok"},
            )
        assert resp.status_code in (200, 202)
        task_id = resp.json()["task_id"]
        task = await server.master.get_task(task_id)
        assert task.user == "internal-cli"  # 集群令牌保留 req.user

    async def test_cluster_token_list_nodes_ok(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.USER})
        async with _client(server) as c:
            resp = await c.get("/api/nodes", headers={"Authorization": "Bearer cluster-tok"})
        assert resp.status_code == 200


class TestP15P16RbacFailClosed:
    """P1-5 (审计 §3.4) RBAC fail-closed: 未登记路径拒用户令牌。
    P1-6: 集群内部路由 (CLUSTER_INTERNAL) 拒用户令牌, 仅 cluster_token 可达;
    用户面新增路由 (config/metrics/observability-export) 登记鉴权。"""

    async def test_unregistered_path_denies_user_token(self, tmp_path, monkeypatch):
        # P1-5: 未登记路由 (/api/foobar) → 用户令牌 fail-closed 拒 (旧 fail-open 放行)。
        from fusion_multi_node.security.permission import check_user_path_access

        assert check_user_path_access(UserRole.ADMIN, "/api/foobar-unknown", "GET") is False
        assert check_user_path_access(UserRole.VIEWER, "/api/foobar-unknown", "POST") is False

    async def test_exempt_paths_allowed(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.permission import check_user_path_access

        assert check_user_path_access(UserRole.VIEWER, "/api/health", "GET") is True
        assert check_user_path_access(UserRole.VIEWER, "/docs", "GET") is True

    async def test_cluster_internal_route_denies_all_user_roles(self, tmp_path, monkeypatch):
        # P1-6: /api/ha/vote 等 CLUSTER_INTERNAL — 用户令牌任何角色皆拒。
        from fusion_multi_node.security.permission import check_user_path_access

        for role in (UserRole.ADMIN, UserRole.USER, UserRole.VIEWER):
            assert check_user_path_access(role, "/api/ha/vote", "POST") is False
            assert check_user_path_access(role, "/api/ha/sync-state", "POST") is False
            assert check_user_path_access(role, "/api/nodes/register", "POST") is False

    async def test_admin_can_config_reload(self, tmp_path, monkeypatch):
        # P1-6: /api/v1/config/reload 登记 user:manage — ADMIN 可过鉴权 (未注 ClusterConfig → 503, 非 403)。
        server, toks = _make_server(tmp_path, monkeypatch, users={"root": UserRole.ADMIN})
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/config/reload",
                headers={"Authorization": f"Bearer {toks['root']}"},
            )
        assert resp.status_code != 403, "ADMIN 应过 config/reload 用户层鉴权"

    async def test_viewer_cannot_config_reload(self, tmp_path, monkeypatch):
        # P1-6: VIEWER 无 user:manage → config/reload 拒 403。
        server, toks = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/v1/config/reload",
                headers={"Authorization": f"Bearer {toks['bob']}"},
            )
        assert resp.status_code == 403

    async def test_user_token_denied_ha_vote(self, tmp_path, monkeypatch):
        # P1-6: 用户令牌调集群内部 /api/ha/vote → 403 (CLUSTER_INTERNAL 仅 cluster_token)。
        server, toks = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.ADMIN})
        async with _client(server) as c:
            resp = await c.post(
                "/api/ha/vote",
                json={"term": 1, "candidate_id": "m1", "candidate_priority": 1},
                headers={"Authorization": f"Bearer {toks['alice']}"},
            )
        assert resp.status_code == 403

    async def test_viewer_can_read_metrics(self, tmp_path, monkeypatch):
        # P1-6: /api/v1/metrics 登记 cluster:stats — VIEWER 可读 (非 403)。
        server, toks = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/metrics",
                headers={"Authorization": f"Bearer {toks['bob']}"},
            )
        assert resp.status_code == 200


class TestP2_8DynamicSubpathAllOps:
    """P2-8 (审计 §3.4): F2 动态子路径全 op — 写 op 各自权限, 读 op 继承父 task:list。
    旧 F2 仅覆盖 cancel/migrate/degrade, GET 读子路径 (progress/timeline) 无登记。"""

    async def test_viewer_can_read_task_detail(self, tmp_path, monkeypatch):
        # GET /api/tasks/{id} 详情 → task:list (VIEWER 可读) → 非 403 (任务不存在→404)。
        server, toks = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            resp = await c.get(
                "/api/tasks/ghost",
                headers={"Authorization": f"Bearer {toks['bob']}"},
            )
        assert resp.status_code != 403, "VIEWER 读任务详情被拒 (应 task:list 放行)"

    async def test_viewer_can_read_progress(self, tmp_path, monkeypatch):
        # GET /api/v1/tasks/{id}/progress → task:list (VIEWER 可读) → 非 403。
        server, toks = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/tasks/ghost/progress",
                headers={"Authorization": f"Bearer {toks['bob']}"},
            )
        assert resp.status_code != 403, "VIEWER 读 progress 被拒 (应 task:list 放行)"

    async def test_viewer_can_read_timeline(self, tmp_path, monkeypatch):
        # GET /api/v1/tasks/{id}/timeline → task:list (VIEWER 可读) → 非 403。
        server, toks = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/tasks/ghost/timeline",
                headers={"Authorization": f"Bearer {toks['bob']}"},
            )
        assert resp.status_code != 403, "VIEWER 读 timeline 被拒 (应 task:list 放行)"

    async def test_unknown_write_op_fail_closed(self, tmp_path, monkeypatch):
        # POST 未知尾部 op (/api/tasks/{id}/result) → fail-closed 403 (非白名单不继承)。
        from fusion_multi_node.security.permission import check_user_path_access

        assert check_user_path_access(UserRole.ADMIN, "/api/tasks/ghost/result", "POST") is False
        assert check_user_path_access(UserRole.USER, "/api/tasks/ghost/retry", "POST") is False

    async def test_write_op_still_enforced(self, tmp_path, monkeypatch):
        # POST cancel 仍走 task:cancel — VIEWER 无 → 403 (回归保护, P2-8 未放松写权限)。
        server, toks = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER})
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/ghost/cancel",
                json={"reason": "x"},
                headers={"Authorization": f"Bearer {toks['bob']}"},
            )
        assert resp.status_code == 403

"""GAP-2 / GAP-8 企业级安全修复测试 (复审计 2026-08-26)。

覆盖:
- GAP-2 mTLS fail-closed: 开启但证书不全 → raise, 不静默回退明文。
- GAP-8 审计日志: 安全动作 (鉴权失败/权限拒绝/注册/审批/任务提交) 写 JSONL, 字段完整。
- GAP-8 权限强制校验默认开: FUSION_PERMISSION_ENFORCE 默认 "1", 缺 X-Node-Id → 403。
"""

from __future__ import annotations

import httpx
import pytest

from fusion_multi_node.security import mtls as mtls_mod
from fusion_multi_node.security.audit_log import AuditLogger
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer

# ── GAP-2: mTLS fail-closed ──


class TestMTLSFailClosed:
    """开启但证书路径不全 → raise, 不回退明文。"""

    def _enable_no_certs(self, monkeypatch):
        monkeypatch.setenv("FUSION_MTLS_ENABLED", "1")
        monkeypatch.delenv("FUSION_MTLS_CA_CERT", raising=False)
        monkeypatch.delenv("FUSION_MTLS_NODE_CERT", raising=False)
        monkeypatch.delenv("FUSION_MTLS_NODE_KEY", raising=False)
        mtls_mod._ENABLED = True

    def _clear(self, monkeypatch):
        for k in ("FUSION_MTLS_ENABLED", "FUSION_MTLS_CA_CERT", "FUSION_MTLS_NODE_CERT", "FUSION_MTLS_NODE_KEY"):
            monkeypatch.delenv(k, raising=False)
        mtls_mod._ENABLED = False

    def test_server_ssl_context_raises_on_missing_certs(self, monkeypatch):
        self._enable_no_certs(monkeypatch)
        with pytest.raises(RuntimeError, match="证书路径不全"):
            mtls_mod.server_ssl_context()
        self._clear(monkeypatch)

    def test_client_ssl_context_raises_on_missing_certs(self, monkeypatch):
        self._enable_no_certs(monkeypatch)
        with pytest.raises(RuntimeError, match="证书路径不全"):
            mtls_mod.client_ssl_context()
        self._clear(monkeypatch)

    def test_server_ssl_kwargs_raises_on_missing_certs(self, monkeypatch):
        self._enable_no_certs(monkeypatch)
        with pytest.raises(RuntimeError, match="证书路径不全"):
            mtls_mod.server_ssl_kwargs()
        self._clear(monkeypatch)

    def test_client_kwargs_raises_on_missing_certs(self, monkeypatch):
        self._enable_no_certs(monkeypatch)
        with pytest.raises(RuntimeError, match="证书路径不全"):
            mtls_mod.client_kwargs()
        self._clear(monkeypatch)

    def test_certs_available_false_when_enabled_and_incomplete(self, monkeypatch):
        self._enable_no_certs(monkeypatch)
        assert mtls_mod.certs_available() is False
        self._clear(monkeypatch)

    def test_certs_available_true_when_disabled(self, monkeypatch):
        self._clear(monkeypatch)
        assert mtls_mod.certs_available() is True

    def test_disabled_returns_none_not_raise(self, monkeypatch):
        """mTLS 关 → 各 helper 返回 None/{}, 不 raise (明文合法)。"""
        self._clear(monkeypatch)
        assert mtls_mod.server_ssl_context() is None
        assert mtls_mod.client_ssl_context() is None
        assert mtls_mod.server_ssl_kwargs() == {}
        assert mtls_mod.client_kwargs() == {}

    def test_full_certs_build_context_not_raise(self, monkeypatch, tmp_path):
        """开启 + 证书齐全 → 正常构建, 不 raise (回归: fail-closed 不误伤合法配置)。"""
        ca_dir = tmp_path / "ca"
        ca_cert, ca_key = mtls_mod.provision_cluster(str(ca_dir))
        cert, key = mtls_mod.provision_node("n1", "worker", ca_cert, ca_key, str(tmp_path / "n1"))
        monkeypatch.setenv("FUSION_MTLS_ENABLED", "1")
        monkeypatch.setenv("FUSION_MTLS_CA_CERT", ca_cert)
        monkeypatch.setenv("FUSION_MTLS_NODE_CERT", cert)
        monkeypatch.setenv("FUSION_MTLS_NODE_KEY", key)
        mtls_mod._ENABLED = True
        assert mtls_mod.certs_available() is True
        assert mtls_mod.server_ssl_context() is not None
        assert mtls_mod.client_ssl_context() is not None
        assert mtls_mod.server_ssl_kwargs()["ssl_certfile"] == cert
        self._clear(monkeypatch)


# ── GAP-8: 审计日志 ──


class TestAuditLogger:
    """AuditLogger 追加写 JSONL, 字段完整, 写失败降级。"""

    def test_log_writes_jsonl_line(self, tmp_path):
        log_path = tmp_path / "audit.log"
        al = AuditLogger(log_path=str(log_path))
        al.log(actor="master", action="task_submit", path="/api/tasks/submit", method="POST", result="ok", detail="t1")
        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 1
        import json

        ev = json.loads(lines[0])
        assert ev["actor"] == "master"
        assert ev["action"] == "task_submit"
        assert ev["path"] == "/api/tasks/submit"
        assert ev["result"] == "ok"
        assert ev["detail"] == "t1"
        assert "ts" in ev

    def test_log_appends_multiple(self, tmp_path):
        log_path = tmp_path / "audit.log"
        al = AuditLogger(log_path=str(log_path))
        for i in range(5):
            al.log(action="register", detail=f"n{i}")
        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 5

    def test_read_returns_all_events(self, tmp_path):
        log_path = tmp_path / "audit.log"
        al = AuditLogger(log_path=str(log_path))
        al.log(action="a1")
        al.log(action="a2")
        events = al.read()
        assert len(events) == 2
        assert events[0]["action"] == "a1"
        assert events[1]["action"] == "a2"

    def test_read_empty_when_missing(self, tmp_path):
        al = AuditLogger(log_path=str(tmp_path / "nope.log"))
        assert al.read() == []

    def test_write_failure_does_not_raise(self, tmp_path):
        """写失败 (路径不可写) → 降级 warning, 不抛 (审计日志不拖垮主路径)。"""
        # 用一个已存在文件当目录 → open 失败
        blocker = tmp_path / "blocker"
        blocker.write_text("x")
        al = AuditLogger(log_path=str(blocker / "audit.log"))
        # 不应抛
        al.log(action="should_not_raise")
        assert not (blocker / "audit.log").exists()

    def test_env_override_path(self, monkeypatch, tmp_path):
        env_path = tmp_path / "env_audit.log"
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(env_path))
        al = AuditLogger()
        assert al.path == env_path
        al.log(action="x")
        assert env_path.exists()


# ── GAP-8: 鉴权失败写审计 (BearerAuthMiddleware) ──


class TestAuthFailAudit:
    """缺 token / token 不匹配 → 401 + 写 auth_fail 审计条目。"""

    async def test_missing_token_writes_auth_fail(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        server = AgentServer()
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post("/api/execute", json={"task_type": "inference", "model_name": "m", "prompt": "p"})
        assert resp.status_code == 401
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        events = al.read()
        fails = [e for e in events if e["action"] == "auth_fail"]
        assert len(fails) == 1
        assert fails[0]["result"] == "denied"
        assert "Bearer" in fails[0]["detail"]

    async def test_bad_token_writes_auth_fail(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        server = AgentServer()
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/execute",
                json={"task_type": "inference", "model_name": "m", "prompt": "p"},
                headers={"Authorization": "Bearer wrong-token"},
            )
        assert resp.status_code == 401
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        events = al.read()
        fails = [e for e in events if e["action"] == "auth_fail"]
        assert len(fails) == 1
        assert "不匹配" in fails[0]["detail"]


# ── GAP-8 (Phase F1): 用户令牌双令牌分流 ──


class TestUserTokenAuth:
    """fmu_ 用户令牌在 master 用户面路由通过; 错误令牌 401+审计; cluster 令牌不变;
    agent 路由拒 fmu_ 前缀。"""

    async def test_user_token_accepted_on_master(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger
        from fusion_multi_node.security.permission import UserRole

        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()
        server = MasterServer(shared_token="cluster-tok")
        store = server._user_store
        assert store is not None
        store.create_user("alice", UserRole.USER)
        tok = store.issue_token("alice")
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/nodes", headers={"Authorization": f"Bearer {tok}"})
        assert resp.status_code == 200

    async def test_wrong_user_token_401_audit(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger
        from fusion_multi_node.security.permission import UserRole

        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()
        server = MasterServer(shared_token="cluster-tok")
        server._user_store.create_user("alice", UserRole.USER)
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/nodes", headers={"Authorization": "Bearer fmu_alice_wrong"})
        assert resp.status_code == 401
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        fails = [e for e in al.read() if e["action"] == "auth_fail"]
        assert len(fails) == 1
        assert "用户令牌校验失败" in fails[0]["detail"]

    async def test_cluster_token_still_accepted_with_user_store(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger
        from fusion_multi_node.security.permission import UserRole

        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()
        server = MasterServer(shared_token="cluster-tok")
        server._user_store.create_user("alice", UserRole.USER)
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/nodes", headers={"Authorization": "Bearer cluster-tok"})
        assert resp.status_code == 200

    async def test_user_token_rejected_on_agent(self, tmp_path, monkeypatch):
        """集群内部流量不携带用户令牌 — agent 路由拒 fmu_ 前缀。"""
        from fusion_multi_node.security.audit_log import reset_audit_logger
        from fusion_multi_node.security.permission import UserRole
        from fusion_multi_node.security.user_store import UserStore

        # agent 无 user_store (默认); 即便有, agent 路由也不该接受用户令牌
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()
        # 先建用户拿令牌 (经独立 store), agent 不注入 user_store
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        tok = store.issue_token("alice")
        server = AgentServer()
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/tasks", headers={"Authorization": f"Bearer {tok}"})
        assert resp.status_code == 401
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        fails = [e for e in al.read() if e["action"] == "auth_fail"]
        assert any("节点路由" in e["detail"] for e in fails)

    async def test_no_user_store_cluster_token_unchanged(self, tmp_path, monkeypatch):
        """无 FUSION_USERS_FILE / users.json → load_user_store 返回 None → 纯 cluster_token。"""
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.delenv("FUSION_USERS_FILE", raising=False)
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()
        server = MasterServer(shared_token="cluster-tok")
        assert server._user_store is None
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/api/nodes", headers={"Authorization": "Bearer cluster-tok"})
            bad = await c.get("/api/nodes", headers={"Authorization": "Bearer fmu_alice_x"})
        assert resp.status_code == 200
        # fmu_ 令牌无 user_store → 落显式拒分支 (用户令牌不可用于节点路由)
        assert bad.status_code == 401

    async def test_bootstrap_admin_env(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        monkeypatch.setenv("FUSION_BOOTSTRAP_ADMIN", "rootadmin")
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()
        server = MasterServer(shared_token="cluster-tok")
        store = server._user_store
        assert store is not None
        assert not store.is_empty()
        assert store.get_user("rootadmin") is not None
        assert store.get_user("rootadmin").role.value == "admin"


# ── GAP-8: 权限强制校验默认开 ──


class TestPermissionEnforceDefault:
    """FUSION_PERMISSION_ENFORCE 默认 "1" → 缺 X-Node-Id → 403 + 审计。"""

    async def test_enforce_default_on_rejects_missing_node_id(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        # 不设 FUSION_PERMISSION_ENFORCE → 默认 "1" 强制。须清掉 conftest 设的 "0"。
        monkeypatch.delenv("FUSION_PERMISSION_ENFORCE", raising=False)
        monkeypatch.delenv("FUSION_MTLS_ENABLED", raising=False)
        mtls_mod._ENABLED = False
        server = AgentServer()
        assert server._permission_enforce is True
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/execute",
                json={"task_type": "inference", "model_name": "m", "prompt": "p"},
                headers={"Authorization": f"Bearer {server._shared_token}"},
            )
        assert resp.status_code == 403
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        events = al.read()
        denies = [e for e in events if e["action"] == "permission_deny"]
        assert len(denies) == 1
        assert "X-Node-Id" in denies[0]["detail"]

    async def test_enforce_off_passes_missing_node_id(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        monkeypatch.delenv("FUSION_MTLS_ENABLED", raising=False)
        mtls_mod._ENABLED = False
        server = AgentServer()
        assert server._permission_enforce is False
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/execute",
                json={"task_type": "inference", "model_name": "m", "prompt": "p"},
                headers={"Authorization": f"Bearer {server._shared_token}"},
            )
        # 不 403 (兼容模式缺 header 放行); 可能 500 (无真推理) 但不 403。
        assert resp.status_code != 403

    async def test_enforce_on_master_header_passes(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        monkeypatch.delenv("FUSION_PERMISSION_ENFORCE", raising=False)
        monkeypatch.delenv("FUSION_MTLS_ENABLED", raising=False)
        mtls_mod._ENABLED = False
        server = AgentServer()
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/execute",
                json={"task_type": "inference", "model_name": "m", "prompt": "p"},
                headers={
                    "Authorization": f"Bearer {server._shared_token}",
                    "X-Node-Id": "master",
                    "X-Node-Role": "master",
                },
            )
        # master 角色有权 → 不 403 (可能 500 无真推理)。
        assert resp.status_code != 403


# ── GAP-8: master 路由审计 (register/approve/reject/task_submit) ──


class TestMasterRouteAudit:
    """master 安全路由写审计条目。"""

    @pytest.fixture
    def _master(self, tmp_path, monkeypatch):
        from fusion_multi_node.security.audit_log import reset_audit_logger

        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        monkeypatch.delenv("FUSION_MTLS_ENABLED", raising=False)
        mtls_mod._ENABLED = False
        server = MasterServer()
        server._approval_manager = None  # 测试 escape hatch
        return server, str(tmp_path / "audit.log")

    async def test_register_writes_audit(self, _master):
        server, log_path = _master
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.post(
                "/api/nodes/register",
                json={"node_id": "w1", "hostname": "h", "ip_address": "127.0.0.1", "port": 11458},
                headers={"Authorization": f"Bearer {server._shared_token}"},
            )
        assert resp.status_code == 200
        al = AuditLogger(log_path=log_path)
        events = al.read()
        regs = [e for e in events if e["action"] == "register" and e["result"] == "ok"]
        assert len(regs) == 1
        assert regs[0]["node_id"] == "w1"

    async def test_task_submit_writes_audit(self, _master):
        server, log_path = _master
        transport = httpx.ASGITransport(app=server.app)
        # 先注册一个节点供派发
        await server.master.register_node(
            __import__("fusion_multi_node.master.cluster_master", fromlist=["NodeInfo", "NodeStatus"]).NodeInfo(
                node_id="n1",
                hostname="h",
                ip_address="127.0.0.1",
                port=11458,
                status=__import__("fusion_multi_node.master.cluster_master", fromlist=["NodeStatus"]).NodeStatus.ONLINE,
                last_heartbeat=0.0,
            )
        )
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data", "model_name": "m", "prompt": "p"},
                headers={"Authorization": f"Bearer {server._shared_token}"},
            )
        # 可能 200 或 503 (派发失败), 但审计应记录 submit
        al = AuditLogger(log_path=log_path)
        events = al.read()
        subs = [e for e in events if e["action"] == "task_submit"]
        assert len(subs) == 1
        assert "task_id=" in subs[0]["detail"]

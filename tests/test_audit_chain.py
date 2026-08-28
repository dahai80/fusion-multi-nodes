"""issue #52 原语 1 — 审计链 HMAC + 链段拉取端点测试。

3 类:
- TestAuditChainLocal — seq 单调 / prev_hash 链接 / mac 验证 / 篡改检出 / 实例重置 / 降级无 token / 向后兼容旧记录。
- TestAuditChainEndpoint — master 链字段 / since_seq 过滤 / 基线记录 / 用户令牌 403 / 集群令牌 200。
- TestAuditChainAgentEndpoint — agent node_id / 链字段。
"""

from __future__ import annotations

import httpx

from fusion_multi_node.security.audit_log import AuditLogger, reset_audit_logger
from fusion_multi_node.security.cluster_key import canonical_json, derive_audit_chain_key, verify_mac
from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer
from fusion_multi_node.utils.auth import load_or_create_token


def _client(server):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://test")


def _make_server(tmp_path, monkeypatch, *, users=None):
    monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
    monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
    monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
    reset_audit_logger()
    server = MasterServer(shared_token="cluster-tok")
    store = server._user_store
    tokens = {}
    for uid, role in (users or {}).items():
        store.create_user(uid, role)
        tokens[uid] = store.issue_token(uid)
    return server, tokens


class TestAuditChainLocal:
    def test_seq_monotonic(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        al.log(action="a1")
        al.log(action="a2")
        al.log(action="a3")
        recs = al.read()
        seqs = [r["seq"] for r in recs if "seq" in r]
        assert seqs == [1, 2, 3], "seq 须单调递增"

    def test_prev_hash_links(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        al.log(action="a1")
        al.log(action="a2")
        recs = al.read()
        # 第二条 prev_hash = 第一条完整记录 sha256 — 重算校验
        import hashlib

        expected_prev = hashlib.sha256(canonical_json(recs[0])).hexdigest()
        assert recs[1]["prev_hash"] == expected_prev, "prev_hash 须链接含 mac 的完整前序记录"

    def test_mac_verifies(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        # AuditLogger 惰性从 cluster_token 派生 chain_key — 用同 token 重派生验签。
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        al.log(action="verify_me")
        rec = al.read()[0]
        key = derive_audit_chain_key(load_or_create_token())
        payload = {k: v for k, v in rec.items() if k != "mac"}
        assert verify_mac(key, canonical_json(payload), rec["mac"]) is True

    def test_tamper_detected(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        al.log(action="a1")
        al.log(action="a2")
        al.log(action="a3")
        recs = al.read()
        # 篡改第一条 action → 重算 mac 不匹配 (mac 字段仍旧) → verify False
        recs[0]["action"] = "tampered"
        key = derive_audit_chain_key(load_or_create_token())
        payload = {k: v for k, v in recs[0].items() if k != "mac"}
        assert verify_mac(key, canonical_json(payload), recs[0]["mac"]) is False, "篡改字段须断 mac"

    def test_instance_reset_clears_chain_state(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit1.log"))
        reset_audit_logger()
        al1 = AuditLogger(log_path=str(tmp_path / "audit1.log"))
        al1.log(action="a1")
        assert al1._seq == 1
        # 新实例 (reset 后) seq 归零 — 不同进程不共享链状态
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit2.log"))
        reset_audit_logger()
        al2 = AuditLogger(log_path=str(tmp_path / "audit2.log"))
        al2.log(action="fresh")
        rec = al2.read()[0]
        assert rec["seq"] == 1, "新实例 seq 从 1 起"

    def test_degrade_without_token(self, tmp_path, monkeypatch):
        # 派生失败 (cluster_token 不可读) → 降级无链字段, 审计不丢事件。
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        al = AuditLogger(log_path=str(tmp_path / "audit.log"))
        # 强制 chain_key 派生失败 — monkeypatch _ensure_chain_key 抛
        monkeypatch.setattr(al, "_ensure_chain_key", lambda: (_ for _ in ()).throw(RuntimeError("no token")))
        al.log(action="degraded")
        rec = al.read()[0]
        assert "seq" not in rec and "mac" not in rec, "降级记录无链字段"
        assert rec["action"] == "degraded", "事件仍写入 (审计不丢事件契约)"

    def test_backward_compat_baseline_records(self, tmp_path, monkeypatch):
        # 旧记录缺链字段 → read() 返基线 dict, guard 视为未验证基线, 不报错。
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
        reset_audit_logger()
        import json

        log_path = tmp_path / "audit.log"
        log_path.write_text(json.dumps({"ts": "old", "action": "legacy", "actor": "x"}) + "\n")
        al = AuditLogger(log_path=str(log_path))
        recs = al.read()
        assert len(recs) == 1
        assert recs[0]["action"] == "legacy"
        assert "seq" not in recs[0], "旧记录无 seq — 基线兼容"


class TestAuditChainEndpoint:
    async def test_chain_fields_present(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        server._audit.log(action="ep1")
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/audit/chain",
                headers={"Authorization": "Bearer cluster-tok"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["node_id"] == "master"
        assert len(body["records"]) == 1
        assert "seq" in body["records"][0] and "mac" in body["records"][0]

    async def test_since_seq_filter(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        server._audit.log(action="a1")
        server._audit.log(action="a2")
        server._audit.log(action="a3")
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/audit/chain?since_seq=2",
                headers={"Authorization": "Bearer cluster-tok"},
            )
        assert resp.status_code == 200
        seqs = [r.get("seq") for r in resp.json()["records"] if "seq" in r]
        assert all(s >= 2 for s in seqs), "since_seq 过滤 seq>=N"
        assert 2 in seqs and 3 in seqs

    async def test_baseline_records_returned(self, tmp_path, monkeypatch):
        # 缺 seq 基线记录 (since_seq 过滤时一律返 — guard 需基线)。
        server, _ = _make_server(tmp_path, monkeypatch)
        # 手写一条无 seq 旧记录
        import json

        with open(server._audit.path, "a") as f:
            f.write(json.dumps({"ts": "old", "action": "legacy"}) + "\n")
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/audit/chain?since_seq=5",
                headers={"Authorization": "Bearer cluster-tok"},
            )
        assert resp.status_code == 200
        actions = [r.get("action") for r in resp.json()["records"]]
        assert "legacy" in actions, "缺 seq 基线记录须返 (since_seq 不滤)"

    async def test_user_token_denied(self, tmp_path, monkeypatch):
        # audit/chain = CLUSTER_INTERNAL → 用户令牌任何角色 403。
        server, tok = _make_server(tmp_path, monkeypatch, users={"alice": UserRole.ADMIN})
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/audit/chain",
                headers={"Authorization": f"Bearer {tok['alice']}"},
            )
        assert resp.status_code == 403

    async def test_cluster_token_ok(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/audit/chain",
                headers={"Authorization": "Bearer cluster-tok"},
            )
        assert resp.status_code == 200


class TestAuditChainAgentEndpoint:
    async def test_agent_node_id_and_chain(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "agent-audit.log"))
        monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
        reset_audit_logger()
        server = AgentServer(shared_token="cluster-tok")
        server.agent.config.node_id = "agent-node-7"
        server._audit.log(action="agent_evt")
        async with _client(server) as c:
            resp = await c.get(
                "/api/v1/audit/chain",
                headers={"Authorization": "Bearer cluster-tok"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["node_id"] == "agent-node-7"
        assert len(body["records"]) == 1
        assert "mac" in body["records"][0]

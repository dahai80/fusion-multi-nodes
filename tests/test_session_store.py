"""#84 集群侧共享会话存储 + sandbox 委派头 — 单元 + HTTP API 覆盖。

- ClusterSessionStore: put/get/list/delete/fork + id 路径段校验 + list 剥离 messages。
- /api/v1/session-store/*: CRUD/fork + Bearer 鉴权 + 用户 RBAC (VIEWER 只读)。
- X-Fusion-Target-Node: 硬绑定校验 (未注册 404 / 离线 409 / draining 409 / 非法段 400 /
  pipeline 400 / exclude 冲突 400), 派发只落 pinned 节点。
- X-Fusion-Run-Id: run→task 索引 + GET /api/v1/runs/{run_id} 查询。
"""

from __future__ import annotations

import httpx
import pytest

from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeStatus, ParallelMode, TaskStatus
from fusion_multi_node.security.audit_log import reset_audit_logger
from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.server.master_server import MasterServer
from fusion_multi_node.storage import ClusterSessionStore

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


def _session(sid: str, messages: int = 2) -> dict:
    return {
        "id": sid,
        "title": f"session {sid}",
        "messages": [{"role": "user", "content": f"m{i}"} for i in range(messages)],
        "metadata": {"origin_node": "laptop-a"},
    }


class TestClusterSessionStoreUnit:
    def test_put_get_roundtrip(self, tmp_path):
        store = ClusterSessionStore(str(tmp_path / "store"))
        stored = store.put_session(_session("sess-1", messages=3))
        assert stored["id"] == "sess-1"
        assert stored["updated_at"] > 0
        got = store.get_session("sess-1")
        assert got is not None and len(got["messages"]) == 3

    def test_get_missing_returns_none(self, tmp_path):
        store = ClusterSessionStore(str(tmp_path / "store"))
        assert store.get_session("ghost") is None

    def test_list_strips_messages_and_sorts(self, tmp_path):
        store = ClusterSessionStore(str(tmp_path / "store"))
        store.put_session(_session("a"))
        store.put_session(_session("b"))
        rows = store.list_sessions(limit=10)
        assert {r["id"] for r in rows} == {"a", "b"}
        assert all("messages" not in r for r in rows)
        # 最近写的排前
        assert rows[0]["id"] == "b"

    def test_delete(self, tmp_path):
        store = ClusterSessionStore(str(tmp_path / "store"))
        store.put_session(_session("a"))
        assert store.delete_session("a") is True
        assert store.get_session("a") is None
        assert store.delete_session("a") is False

    def test_fork_copies_and_marks_parent(self, tmp_path):
        store = ClusterSessionStore(str(tmp_path / "store"))
        store.put_session(_session("parent", messages=4))
        forked = store.fork_session("parent", title="branch")
        assert forked is not None
        assert forked["id"] != "parent"
        assert forked["title"] == "branch"
        assert forked["metadata"]["forked_from"] == "parent"
        assert len(forked["messages"]) == 4

    def test_fork_missing_returns_none(self, tmp_path):
        store = ClusterSessionStore(str(tmp_path / "store"))
        assert store.fork_session("ghost") is None

    @pytest.mark.parametrize("bad", ["", "a/b", "a\\b", "..", "a..b", "."])
    def test_path_traversal_rejected(self, tmp_path, bad):
        store = ClusterSessionStore(str(tmp_path / "store"))
        assert store.get_session(bad) is None
        with pytest.raises(ValueError):
            store.put_session({"id": bad})
        assert store.delete_session(bad) is False

    def test_dir_env_override(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_MULTINODE_SESSION_STORE_DIR", str(tmp_path / "envdir"))
        store = ClusterSessionStore()
        store.put_session(_session("env-1"))
        assert (tmp_path / "envdir" / "env-1.json").exists()

    def test_configured_dir_wins_over_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_MULTINODE_SESSION_STORE_DIR", str(tmp_path / "envdir"))
        store = ClusterSessionStore(str(tmp_path / "cfgdir"))
        store.put_session(_session("cfg-1"))
        assert (tmp_path / "cfgdir" / "cfg-1.json").exists()


def _make_server(tmp_path, monkeypatch, *, users=None):
    monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
    monkeypatch.setenv("FUSION_AUDIT_LOG", str(tmp_path / "audit.log"))
    monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
    reset_audit_logger()
    server = MasterServer(master=ClusterMaster(heartbeat_timeout=60.0), shared_token=TEST_TOKEN)
    server._approval_manager = None
    server.session_store = ClusterSessionStore(str(tmp_path / "store"))
    tokens = {}
    if users:
        store = server._user_store
        assert store is not None
        for uid, role in users.items():
            store.create_user(uid, role)
            tokens[uid] = store.issue_token(uid)
    return server, tokens


def _client(server) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://test")


def _register_node(client, node_id="n1"):
    payload = {
        "node_id": node_id,
        "hostname": f"mac-{node_id}",
        "ip_address": "127.0.0.1",
        "port": 11458,
        "arch": "arm64",
        "total_memory_gb": 64.0,
        "available_memory_gb": 48.0,
        "cpu_cores": 12,
        "gpu_cores": 30,
    }
    return client.post("/api/nodes/register", json=payload, headers=AUTH_HEADERS)


class TestSessionStoreApi:
    async def test_put_get_list_delete_fork_flow(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.put("/api/v1/session-store/sess-1", json=_session("sess-1"), headers=AUTH_HEADERS)
            assert resp.status_code == 200
            assert resp.json()["session"]["id"] == "sess-1"

            got = await c.get("/api/v1/session-store/sess-1", headers=AUTH_HEADERS)
            assert got.status_code == 200
            assert len(got.json()["session"]["messages"]) == 2

            listing = await c.get("/api/v1/session-store", headers=AUTH_HEADERS)
            assert listing.status_code == 200
            assert listing.json()["total"] == 1
            assert "messages" not in listing.json()["sessions"][0]

            forked = await c.post("/api/v1/session-store/sess-1/fork", headers=AUTH_HEADERS)
            assert forked.status_code == 200
            assert forked.json()["session"]["metadata"]["forked_from"] == "sess-1"

            deleted = await c.delete("/api/v1/session-store/sess-1", headers=AUTH_HEADERS)
            assert deleted.status_code == 200
            missing = await c.get("/api/v1/session-store/sess-1", headers=AUTH_HEADERS)
            assert missing.status_code == 404

    async def test_get_missing_404(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.get("/api/v1/session-store/ghost", headers=AUTH_HEADERS)
        assert resp.status_code == 404

    async def test_put_id_mismatch_400(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.put("/api/v1/session-store/a", json=_session("b"), headers=AUTH_HEADERS)
        assert resp.status_code == 400

    async def test_bad_session_id_400(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            # 穿越段经 HTTP 客户端/路由层均不可达: %2F 多段不命中单段路由 → 404,
            # ".." 被 URL 规范化吞掉 → 404。存储层拒绝已在单测覆盖 (path_traversal_rejected)。
            assert (await c.get("/api/v1/session-store/a%2Fb", headers=AUTH_HEADERS)).status_code == 404
            assert (
                await c.put("/api/v1/session-store/..", json=_session(".."), headers=AUTH_HEADERS)
            ).status_code == 404
            assert not list((tmp_path / "store").glob("*.json"))

    async def test_auth_required(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.get("/api/v1/session-store")
        assert resp.status_code in (401, 403)

    async def test_viewer_read_ok_write_denied(self, tmp_path, monkeypatch):
        server, toks = _make_server(tmp_path, monkeypatch, users={"bob": UserRole.VIEWER, "amy": UserRole.USER})
        vh = {"Authorization": f"Bearer {toks['bob']}"}
        uh = {"Authorization": f"Bearer {toks['amy']}"}
        async with _client(server) as c:
            await c.put("/api/v1/session-store/sess-1", json=_session("sess-1"), headers=uh)
            read = await c.get("/api/v1/session-store/sess-1", headers=vh)
            assert read.status_code == 200
            write = await c.put("/api/v1/session-store/sess-2", json=_session("sess-2"), headers=vh)
            assert write.status_code == 403
            fork = await c.post("/api/v1/session-store/sess-1/fork", headers=vh)
            assert fork.status_code == 403
            delete = await c.delete("/api/v1/session-store/sess-1", headers=vh)
            assert delete.status_code == 403
            user_write = await c.put("/api/v1/session-store/sess-2", json=_session("sess-2"), headers=uh)
            assert user_write.status_code == 200


class TestSandboxTargetNode:
    async def test_submit_pinned_dispatches_to_target(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            assert (await _register_node(c, "n1")).status_code == 200
            assert (await _register_node(c, "n2")).status_code == 200
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "delegated", "mode": "data", "model_name": "m", "timeout_seconds": 60},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "n2"},
            )
            assert resp.status_code == 200
            task_id = resp.json()["task_id"]
            task = server.master.tasks[task_id]
            assert task.pinned_node_id == "n2"
            assert task.preferred_node_id == "n2"
            assert task.assigned_nodes == ["n2"]

    async def test_submit_unregistered_node_404(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data"},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "ghost"},
            )
        assert resp.status_code == 404

    async def test_submit_offline_node_409(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await _register_node(c, "n1")
            server.master.nodes["n1"].status = NodeStatus.OFFLINE
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data"},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "n1"},
            )
        assert resp.status_code == 409

    async def test_submit_draining_node_409(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await _register_node(c, "n1")
            await server.master.set_node_draining("n1", True)
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data"},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "n1"},
            )
        assert resp.status_code == 409

    async def test_submit_banned_node_409(self, tmp_path, monkeypatch):
        import time as _time

        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await _register_node(c, "n1")
            server.master._banned_nodes["n1"] = _time.time() + 300.0
            assert server.master.is_node_banned("n1")
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data"},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "n1"},
            )
        assert resp.status_code == 409

    async def test_submit_bad_segment_400(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data"},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "a/b"},
            )
        assert resp.status_code == 400

    async def test_submit_pipeline_pinned_400(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await _register_node(c, "n1")
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "pipeline", "model_shards": []},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "n1"},
            )
        assert resp.status_code == 400

    async def test_submit_exclude_conflict_400(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await _register_node(c, "n1")
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data", "exclude_nodes": ["n1"]},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "n1"},
            )
        assert resp.status_code == 400

    async def test_v1_submit_pinned_supported(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await _register_node(c, "n1")
            resp = await c.post(
                "/api/v1/tasks/submit",
                json={"name": "t", "mode": "data", "model_name": "m"},
                headers={**AUTH_HEADERS, "X-Fusion-Target-Node": "n1"},
            )
            assert resp.status_code == 200
            task = server.master.tasks[resp.json()["task_id"]]
            assert task.assigned_nodes == ["n1"]


class TestRunIdIndex:
    async def test_run_id_lookup(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            await _register_node(c, "n1")
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "run-1", "mode": "data", "model_name": "m"},
                headers={**AUTH_HEADERS, "X-Fusion-Run-Id": "run-abc"},
            )
            assert resp.status_code == 200
            task_id = resp.json()["task_id"]
            got = await c.get("/api/v1/runs/run-abc", headers=AUTH_HEADERS)
            assert got.status_code == 200
            assert got.json()["task"]["task_id"] == task_id

    async def test_run_id_unknown_404(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.get("/api/v1/runs/ghost", headers=AUTH_HEADERS)
        assert resp.status_code == 404

    async def test_run_id_bad_segment_400(self, tmp_path, monkeypatch):
        server, _ = _make_server(tmp_path, monkeypatch)
        async with _client(server) as c:
            resp = await c.post(
                "/api/tasks/submit",
                json={"name": "t", "mode": "data"},
                headers={**AUTH_HEADERS, "X-Fusion-Run-Id": "../etc"},
            )
        assert resp.status_code == 400

    async def test_run_id_persist_and_restore(self, tmp_path, monkeypatch):
        master = ClusterMaster(heartbeat_timeout=60.0)
        await master.register_run_id("run-x", "task-x")
        master.tasks["task-x"] = ClusterTask(
            task_id="task-x", name="t", mode=ParallelMode.DATA, status=TaskStatus.PENDING
        )
        snap = master._persist_tasks_locked()
        master._write_task_store(snap)
        master2 = ClusterMaster(heartbeat_timeout=60.0)
        restored = await master2._restore_tasks()
        assert restored == 1
        assert master2.get_task_by_run_id("run-x") is not None


class TestPinSchedulingUnit:
    async def test_select_nodes_pinned_only_target(self):
        import time as _time

        from fusion_multi_node.master import NodeInfo

        master = ClusterMaster(heartbeat_timeout=60.0)
        for nid in ("n1", "n2"):
            master.nodes[nid] = NodeInfo(
                node_id=nid,
                hostname=f"mac-{nid}",
                ip_address="127.0.0.1",
                port=11458,
                arch="arm64",
                total_memory_gb=64.0,
                available_memory_gb=48.0,
                cpu_cores=12,
                gpu_cores=30,
                status=NodeStatus.ONLINE,
                last_heartbeat=_time.time(),
            )
        nodes = await master.select_nodes(ParallelMode.DATA, pinned_node_id="n2")
        assert [n.node_id for n in nodes] == ["n2"]

    async def test_select_nodes_pinned_unavailable_empty(self):
        master = ClusterMaster(heartbeat_timeout=60.0)
        nodes = await master.select_nodes(ParallelMode.DATA, pinned_node_id="ghost")
        assert nodes == []

    async def test_select_nodes_pinned_excluded_empty(self):
        import time as _time

        from fusion_multi_node.master import NodeInfo

        master = ClusterMaster(heartbeat_timeout=60.0)
        master.nodes["n1"] = NodeInfo(
            node_id="n1",
            hostname="mac-n1",
            ip_address="127.0.0.1",
            port=11458,
            arch="arm64",
            total_memory_gb=64.0,
            available_memory_gb=48.0,
            cpu_cores=12,
            gpu_cores=30,
            status=NodeStatus.ONLINE,
            last_heartbeat=_time.time(),
        )
        nodes = await master.select_nodes(ParallelMode.DATA, pinned_node_id="n1", exclude_nodes=["n1"])
        assert nodes == []

    async def test_retry_keeps_pinned_out_of_exclude(self):
        master = ClusterMaster(heartbeat_timeout=60.0)
        task = ClusterTask(
            task_id="t1",
            name="t",
            mode=ParallelMode.DATA,
            status=TaskStatus.RUNNING,
            pinned_node_id="n1",
            assigned_nodes=["n1", "n2"],
        )
        master._enqueue_retry(task)
        # pinned 节点不并入 exclude (重试仍可派回 n1), n2 正常并入
        assert "n1" not in task.exclude_nodes
        assert "n2" in task.exclude_nodes
        assert task.status == TaskStatus.PENDING

"""M2-05 / M3-03 / M4-04 / M4-05 / M5-04 / M6 / M8 / M9 / M10 / M1 / M9-04 / M6-04 — 新功能测试。"""

from __future__ import annotations

import asyncio
import tempfile
import time

import pytest

from fusion_multi_node.master.cluster_master import (
    ClusterMaster,
    ClusterTask,
    NodeInfo,
    NodeStatus,
    ParallelMode,
)
from fusion_multi_node.master.load_metrics import (
    STRATEGY_WEIGHTS,
    LoadMetrics,
    LoadRouter,
    RoutingStrategy,
    RoutingWeights,
)
from fusion_multi_node.master.task_sharding import (
    MergedResult,
    ShardingStrategy,
    ShardingType,
    ShardMerger,
    TaskShard,
    TaskSharder,
)
from fusion_multi_node.storage.shard_replication import (
    ReplicationConfig,
    ShardReplicator,
)
from fusion_multi_node.storage.storage_volume import (
    StorageVolume,
    VolumeSpec,
    VolumeType,
)

# ── M3-03 Master Election ──


class TestMasterElection:
    def test_election_state_enum(self):
        from fusion_multi_node.master.election import ElectionState

        assert ElectionState.FOLLOWER.value == "follower"
        assert ElectionState.CANDIDATE.value == "candidate"
        assert ElectionState.LEADER.value == "leader"

    def test_vote_request_response(self):
        from fusion_multi_node.master.election import VoteRequest, VoteResponse

        req = VoteRequest(term=1, candidate_id="node-1", candidate_priority=5)
        assert req.term == 1
        resp = VoteResponse(term=1, vote_granted=True, voter_id="node-2")
        assert resp.vote_granted is True

    def test_election_init(self):
        from fusion_multi_node.master.election import ElectionState, MasterElection

        e = MasterElection(node_id="node-1", priority=5, known_nodes=["node-2", "node-3"])
        assert e.node_id == "node-1"
        assert e.priority == 5
        assert e.state == ElectionState.FOLLOWER
        assert len(e._known_nodes) == 2

    @pytest.mark.asyncio
    async def test_election_start_stop(self):
        from fusion_multi_node.master.election import MasterElection

        e = MasterElection(node_id="node-1", priority=1)
        await e.start()
        assert e._running is True
        await e.stop()
        assert e._running is False

    @pytest.mark.asyncio
    async def test_handle_vote_request(self):
        from fusion_multi_node.master.election import MasterElection, VoteRequest

        e = MasterElection(node_id="node-1", priority=3)
        req = VoteRequest(term=1, candidate_id="node-2", candidate_priority=5)
        resp = await e.handle_vote_request(req)
        assert resp.vote_granted is True
        assert resp.voter_id == "node-1"

    @pytest.mark.asyncio
    async def test_receive_heartbeat(self):
        from fusion_multi_node.master.election import MasterElection

        e = MasterElection(node_id="node-1", priority=3)
        await e.receive_heartbeat("leader-1", term=1)
        assert e._leader_id == "leader-1"
        assert e.current_term == 1

    def test_get_state(self):
        from fusion_multi_node.master.election import MasterElection

        e = MasterElection(node_id="node-1", priority=5, known_nodes=["node-2"])
        state = e.get_state()
        assert state["node_id"] == "node-1"
        assert state["priority"] == 5
        assert "node-2" in state["known_nodes"]

    def test_add_remove_known_node(self):
        from fusion_multi_node.master.election import MasterElection

        e = MasterElection(node_id="node-1")
        e.add_known_node("node-2")
        assert "node-2" in e._known_nodes
        e.remove_known_node("node-2")
        assert "node-2" not in e._known_nodes

    @pytest.mark.asyncio
    async def test_election_loop_survives_start_election_failure(self):
        # P0-1: 选举循环逐次异常隔离 — _start_election 抛异常不杀循环。
        import fusion_multi_node.master.election as el
        from fusion_multi_node.master.election import MasterElection

        e = MasterElection(node_id="node-1", priority=1, known_nodes=["node-2"])
        calls = {"n": 0}

        async def boom():
            calls["n"] += 1
            raise RuntimeError("模拟选举失败")

        orig_start = e._start_election
        e._start_election = boom
        # 强制选举超时: _last_heartbeat 置远过去, _election_timeout 置极小。
        import time as _time

        e._last_heartbeat = _time.time() - 1000
        e._election_timeout = 0.001
        orig_sleep = el.asyncio.sleep

        async def fast_sleep(_d):
            await orig_sleep(0)

        el.asyncio.sleep = fast_sleep
        try:
            await e.start()
            await orig_sleep(0.05)
            assert e._running is True, "选举循环不应被异常杀死"
            assert e._task is not None and not e._task.done()
            assert calls["n"] >= 1
        finally:
            el.asyncio.sleep = orig_sleep
            e._start_election = orig_start
            await e.stop()


# ── M4-04 Task Auto-degradation & M5-04 Cancel ──


class TestClusterTaskNewFields:
    def test_task_new_fields(self):
        from fusion_multi_node.master.cluster_master import ClusterTask, ParallelMode

        task = ClusterTask(
            task_id="t1",
            name="test",
            mode=ParallelMode.DATA,
            required_capability="inference",
            preferred_node_id="node-1",
            priority=5,
        )
        assert task.required_capability == "inference"
        assert task.preferred_node_id == "node-1"
        assert task.priority == 5
        assert task.degradation_count == 0
        assert task.sub_tasks == []
        assert task.cancel_reason == ""

    @pytest.mark.asyncio
    async def test_cancel_task(self):
        from fusion_multi_node.master.cluster_master import (
            ClusterMaster,
            ClusterTask,
            ParallelMode,
            TaskStatus,
        )

        master = ClusterMaster()
        task = ClusterTask(
            task_id="t-cancel",
            name="cancel-test",
            mode=ParallelMode.DATA,
            status=TaskStatus.RUNNING,
            started_at=time.time(),
        )
        master.tasks[task.task_id] = task
        ok = await master.cancel_task("t-cancel", reason="test cancel")
        assert ok is True
        assert master.tasks["t-cancel"].status == TaskStatus.CANCELLED
        assert master.tasks["t-cancel"].cancel_reason == "test cancel"

    @pytest.mark.asyncio
    async def test_cancel_task_with_sub_tasks(self):
        from fusion_multi_node.master.cluster_master import (
            ClusterMaster,
            ClusterTask,
            ParallelMode,
            TaskStatus,
        )

        master = ClusterMaster()
        sub = ClusterTask(
            task_id="sub-1",
            name="sub",
            mode=ParallelMode.DATA,
            status=TaskStatus.RUNNING,
            started_at=time.time(),
        )
        parent = ClusterTask(
            task_id="parent-1",
            name="parent",
            mode=ParallelMode.DATA,
            status=TaskStatus.RUNNING,
            started_at=time.time(),
            sub_tasks=["sub-1"],
        )
        master.tasks["sub-1"] = sub
        master.tasks["parent-1"] = parent
        ok = await master.cancel_task("parent-1", reason="cancel parent", cancel_sub_tasks=True)
        assert ok is True
        assert master.tasks["sub-1"].status == TaskStatus.CANCELLED
        assert "父任务取消" in master.tasks["sub-1"].cancel_reason

    def test_model_degradation_chain(self):
        from fusion_multi_node.master.cluster_master import ClusterMaster

        chain = ClusterMaster.MODEL_DEGRADATION_CHAIN
        assert chain["70b"] == "32b"
        assert chain["32b"] == "13b"
        assert chain["8b"] == "3b"

    def test_extract_model_size(self):
        from fusion_multi_node.master.cluster_master import ClusterMaster

        master = ClusterMaster()
        assert master._extract_model_size("llama-70b-chat") == "70b"
        assert master._extract_model_size("qwen-8b") == "8b"
        assert master._extract_model_size("unknown") == ""

    def test_extract_model_size_boundary_r7(self):
        # R7: 130b 不应命中 13b, 33b 不应命中 3b, 30b 不应命中 3b
        from fusion_multi_node.master.cluster_master import ClusterMaster

        master = ClusterMaster()
        assert master._extract_model_size("meta-llama-130b") == ""
        assert master._extract_model_size("qwen-33b") == ""
        assert master._extract_model_size("model-30b-chat") == ""
        assert master._extract_model_size("llama-13b") == "13b"
        assert master._extract_model_size("qwen-3b") == "3b"
        # 130b 不被误判为 vram_first, 内存估算走默认 (不按 13b 低估)
        from fusion_multi_node.master import ClusterTask, ParallelMode

        task_130 = ClusterTask(task_id="t130", name="n", mode=ParallelMode.DATA, model_name="llama-130b")
        assert master._is_vram_first(task_130) is False
        task_13 = ClusterTask(task_id="t13", name="n", mode=ParallelMode.DATA, model_name="llama-13b")
        assert master._is_vram_first(task_13) is True
        # 130b 估算 == 默认 (未被 13b=12GB 低估)
        mem_130 = master._estimate_memory(task_130)
        mem_unknown = master._estimate_memory(
            ClusterTask(task_id="tu", name="n", mode=ParallelMode.DATA, model_name="unknown-xyz")
        )
        assert mem_130 == mem_unknown


# ── M6 Security ──


class TestPermissionManager:
    def test_assign_role(self):
        from fusion_multi_node.security.permission import NodeRole, PermissionManager

        pm = PermissionManager()
        pm.assign_role("node-1", NodeRole.MASTER)
        assert pm.get_role("node-1") == NodeRole.MASTER

    def test_master_permissions(self):
        from fusion_multi_node.security.permission import (
            NodeRole,
            Permission,
            PermissionManager,
        )

        pm = PermissionManager()
        pm.assign_role("master-1", NodeRole.MASTER)
        assert pm.has_permission("master-1", Permission.TASK_SUBMIT) is True
        assert pm.has_permission("master-1", Permission.NODE_REGISTER) is True
        # master 派发 execute 到 agent → 须有 TASK_EXECUTE (#80 mTLS 细粒度权限)
        assert pm.has_permission("master-1", Permission.TASK_EXECUTE) is True

    def test_worker_permissions(self):
        from fusion_multi_node.security.permission import (
            NodeRole,
            Permission,
            PermissionManager,
        )

        pm = PermissionManager()
        pm.assign_role("worker-1", NodeRole.WORKER)
        assert pm.has_permission("worker-1", Permission.TASK_EXECUTE) is True
        assert pm.has_permission("worker-1", Permission.HARDWARE_READ) is True
        assert pm.has_permission("worker-1", Permission.TASK_SUBMIT) is False
        assert pm.has_permission("worker-1", Permission.CLUSTER_STATS) is False

    def test_check_path_access(self):
        from fusion_multi_node.security.permission import NodeRole, PermissionManager

        pm = PermissionManager()
        pm.assign_role("worker-1", NodeRole.WORKER)
        assert pm.check_path_access("worker-1", "/api/execute", "POST") is True
        assert pm.check_path_access("worker-1", "/api/tasks/submit", "POST") is False
        assert pm.check_path_access("worker-1", "/api/health", "GET") is True

    def test_unknown_node_denied(self):
        from fusion_multi_node.security.permission import Permission, PermissionManager

        pm = PermissionManager()
        assert pm.has_permission("unknown", Permission.TASK_SUBMIT) is False

    def test_remove_assignment(self):
        from fusion_multi_node.security.permission import NodeRole, PermissionManager

        pm = PermissionManager()
        pm.assign_role("node-1", NodeRole.MASTER)
        pm.remove_assignment("node-1")
        assert pm.get_role("node-1") is None

    def test_get_permissions(self):
        from fusion_multi_node.security.permission import NodeRole, PermissionManager

        pm = PermissionManager()
        pm.assign_role("master-1", NodeRole.MASTER)
        perms = pm.get_permissions("master-1")
        assert len(perms) > 5


class TestNodeApproval:
    def test_request_join(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager()
        req = mgr.request_join(
            node_id="node-1",
            hostname="mac-1",
            ip_address="192.168.1.10",
            port=11458,
        )
        assert req.node_id == "node-1"
        assert req.status.value == "pending"

    def test_approve(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager()
        mgr.request_join("node-1", "mac-1", "192.168.1.10", 11458)
        mgr.approve("node-1", approved_by="admin")
        assert mgr.is_approved("node-1") is True

    def test_reject(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager()
        mgr.request_join("node-1", "mac-1", "192.168.1.10", 11458)
        mgr.reject("node-1", reason="untrusted")
        assert mgr.is_approved("node-1") is False

    def test_auto_approve_patterns(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager(auto_approve_patterns=["192.168."])
        mgr.request_join("node-1", "mac-1", "192.168.1.10", 11458)
        assert mgr.is_approved("node-1") is True

    def test_auto_approve_cidr_precision(self):
        # CIDR "172.16.0.0/12" 精确匹配私网 172.16-31, 不应放过公网 172.1.2.3。
        # 旧 "172." 子串会同时匹配二者 (过匹配公网)。
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager(auto_approve_patterns=["172.16.0.0/12"])
        mgr.request_join("priv", "mac-priv", "172.16.1.5", 11458)
        assert mgr.is_approved("priv") is True
        mgr.request_join("pub", "mac-pub", "172.1.2.3", 11458)
        assert mgr.is_approved("pub") is False
        # 通配兼容 (旧 "192.168.*" 配置仍生效)。
        mgr2 = NodeApprovalManager(auto_approve_patterns=["192.168.*"])
        mgr2.request_join("wcard", "mac-w", "192.168.1.10", 11458)
        assert mgr2.is_approved("wcard") is True

    def test_revoke(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager(auto_approve_patterns=["192.168.*"])
        mgr.request_join("node-1", "mac-1", "192.168.1.10", 11458)
        mgr.revoke_approval("node-1")
        assert mgr.is_approved("node-1") is False


class TestWorkerSandbox:
    def test_sandbox_config_defaults(self):
        from fusion_multi_node.security.sandbox import SandboxConfig

        config = SandboxConfig()
        assert config.max_cpu_seconds == 300
        assert config.max_memory_mb == 8192

    def test_check_path_access(self):
        from fusion_multi_node.security.sandbox import SandboxConfig, WorkerSandbox

        config = SandboxConfig(allowed_paths=["/tmp", "/data"], read_only_paths=["/models"])
        sandbox = WorkerSandbox(config=config)
        assert sandbox.check_path_access("/tmp/output", write=True) is True
        assert sandbox.check_path_access("/models/model.bin", write=False) is True
        assert sandbox.check_path_access("/models/model.bin", write=True) is False
        assert sandbox.check_path_access("/etc/passwd", write=False) is False

    def test_check_network_access(self):
        from fusion_multi_node.security.sandbox import SandboxConfig, WorkerSandbox

        config = SandboxConfig(allowed_network_hosts=["api.openai.com", "internal"])
        sandbox = WorkerSandbox(config=config)
        assert sandbox.check_network_access("api.openai.com") is True
        assert sandbox.check_network_access("sub.internal") is True
        assert sandbox.check_network_access("evil.com") is False

    def test_check_network_no_whitelist(self):
        from fusion_multi_node.security.sandbox import SandboxConfig, WorkerSandbox

        sandbox = WorkerSandbox(config=SandboxConfig(allowed_network_hosts=[]))
        assert sandbox.check_network_access("any-host.com") is True

    def test_filter_environment(self):
        from fusion_multi_node.security.sandbox import WorkerSandbox

        sandbox = WorkerSandbox()
        env = {
            "HOME": "/Users/test",
            "PATH": "/usr/bin",
            "SECRET_KEY": "super-secret",
            "FUSION_NODE_ID": "node-1",
            "AWS_ACCESS_KEY": "AKIA1234567890ABCDEF",
        }
        filtered = sandbox.filter_environment(env)
        assert "HOME" in filtered
        assert "PATH" in filtered
        assert "FUSION_NODE_ID" in filtered
        assert "SECRET_KEY" not in filtered
        assert "AWS_ACCESS_KEY" not in filtered


class TestDataScrubber:
    def test_scrub_phone(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        text, hits = scrubber.scrub_text("请联系 13912345678")
        assert "13912345678" not in text
        assert "phone_cn" in hits

    def test_scrub_email(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        text, hits = scrubber.scrub_text("email: test@example.com")
        assert "test@example.com" not in text
        assert "email" in hits

    def test_scrub_dict(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        data = {"user": "张三", "phone": "13912345678", "note": "call me"}
        result, hits = scrubber.scrub_dict(data)
        assert "13912345678" not in result["phone"]
        assert "phone_cn" in hits

    def test_scrub_api_key(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        text, hits = scrubber.scrub_text('api_key = "sk-abcdefghijklmnopqrstuvwxyz123456"')
        assert "sk-abcdefghijklmnopqrstuvwxyz123456" not in text
        assert "api_key" in hits

    def test_scrub_openai_token(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        text, hits = scrubber.scrub_text("bearer sk-abcd1234efgh5678ijkl9012mnop3456")
        assert "sk-abcd1234efgh5678ijkl9012mnop3456" not in text
        assert "openai_key" in hits

    def test_scrub_github_pat(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        pat = "ghp_0123456789abcdefghijklmnopqrstuvwxyz"  # ghp_ + 36 chars
        text, hits = scrubber.scrub_text(f"token={pat}")
        assert pat not in text
        assert "github_pat" in hits

    def test_scrub_slack_token(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        text, hits = scrubber.scrub_text("hook xoxb-1234567890-abcdefghij")
        assert "xoxb-1234567890-abcdefghij" not in text
        assert "slack_token" in hits

    def test_scrub_jwt(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4f"
        text, hits = scrubber.scrub_text(f"Authorization: Bearer {jwt}")
        assert jwt not in text
        assert "jwt_token" in hits

    def test_scrub_phone_cjk_adjacent(self):
        # 中文字符紧贴手机号, \b 在 Unicode \w 下失效 → 用数字边界 (?<!\d)(?!\d) 才脱敏
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        text, hits = scrubber.scrub_text("用户手机13800138000，邮箱test@example.com")
        assert "13800138000" not in text
        assert "phone_cn" in hits

    def test_scrub_phone_not_in_longer_digits(self):
        # 13 位数字串不应误匹配 11 位手机号 (数字边界拒绝更长串子串)
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        text, hits = scrubber.scrub_text("订单号 1391234567890")
        assert "1391234567890" in text
        assert "phone_cn" not in hits

    def test_custom_rule(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber, ScrubRule

        scrubber = DataScrubber(custom_rules=[ScrubRule(name="test_id", pattern=r"ID-\d{6}", replacement="[ID]")])
        text, hits = scrubber.scrub_text("Your ID-123456 is ready")
        assert "ID-123456" not in text
        assert "test_id" in hits

    def test_active_rules(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        assert scrubber.rule_count >= 7
        assert "phone_cn" in scrubber.active_rules

    def test_no_hits(self):
        from fusion_multi_node.security.data_scrubber import DataScrubber

        scrubber = DataScrubber()
        _text, hits = scrubber.scrub_text("hello world")
        assert hits == []

    def test_data_isolation_symlink_bypass(self):
        # 符号链接指向 .fusion/master 子文件, realpath+commonpath 应拦截 (AR审计 P2)
        import os
        import tempfile

        from fusion_multi_node.security.data_isolation import DataIsolationPolicy

        policy = DataIsolationPolicy()
        with tempfile.TemporaryDirectory() as tmp:
            master_dir = os.path.join(tmp, ".fusion", "master")
            os.makedirs(master_dir)
            secret = os.path.join(master_dir, "secret.db")
            open(secret, "w").close()
            link = os.path.join(tmp, "link_to_secret.db")
            os.symlink(secret, link)
            # 直连 + 符号链接都应判为 Master 专有
            assert policy.is_master_only(secret) is True
            assert policy.is_master_only(link) is True

    def test_data_isolation_clean_path_allowed(self):
        from fusion_multi_node.security.data_isolation import DataIsolationPolicy

        policy = DataIsolationPolicy()
        assert policy.is_master_only("/tmp/normal/model.mlx") is False


# ── M8 Log Store & Diagnosis ──


class TestLogStore:
    def test_store_and_query(self):
        from fusion_multi_node.observability.log_store import LogStore, StoredLog

        store = LogStore(persist_to_disk=False)
        store.store(
            StoredLog(
                timestamp=time.time(),
                level="error",
                source="master",
                message="节点心跳超时",
                node_id="node-1",
            )
        )
        store.store(StoredLog(timestamp=time.time(), level="info", source="agent", message="任务完成"))
        results = store.query(level="error")
        assert len(results) == 1
        assert results[0].node_id == "node-1"

    def test_query_by_node(self):
        from fusion_multi_node.observability.log_store import LogStore, StoredLog

        store = LogStore(persist_to_disk=False)
        store.store(
            StoredLog(
                timestamp=time.time(),
                level="info",
                source="a",
                message="m1",
                node_id="n1",
            )
        )
        store.store(
            StoredLog(
                timestamp=time.time(),
                level="info",
                source="a",
                message="m2",
                node_id="n2",
            )
        )
        results = store.query(node_id="n1")
        assert len(results) == 1

    def test_query_by_keyword(self):
        from fusion_multi_node.observability.log_store import LogStore, StoredLog

        store = LogStore(persist_to_disk=False)
        store.store(
            StoredLog(
                timestamp=time.time(),
                level="error",
                source="a",
                message="OOM out of memory",
            )
        )
        results = store.query(keyword="memory")
        assert len(results) == 1

    def test_export_json(self):
        import json

        from fusion_multi_node.observability.log_store import LogStore, StoredLog

        store = LogStore(persist_to_disk=False)
        store.store(StoredLog(timestamp=time.time(), level="info", source="a", message="test"))
        data = json.loads(store.export_json())
        assert len(data) == 1

    def test_export_csv(self):
        from fusion_multi_node.observability.log_store import LogStore, StoredLog

        store = LogStore(persist_to_disk=False)
        store.store(StoredLog(timestamp=time.time(), level="info", source="a", message="test"))
        csv_text = store.export_csv()
        assert "timestamp" in csv_text
        assert "test" in csv_text

    def test_export_text(self):
        from fusion_multi_node.observability.log_store import LogStore, StoredLog

        store = LogStore(persist_to_disk=False)
        store.store(
            StoredLog(
                timestamp=time.time(),
                level="error",
                source="master",
                message="fail",
                node_id="n1",
            )
        )
        text = store.export_text()
        assert "ERROR" in text
        assert "fail" in text

    def test_get_stats(self):
        from fusion_multi_node.observability.log_store import LogStore, StoredLog

        store = LogStore(persist_to_disk=False)
        store.store(StoredLog(timestamp=time.time(), level="info", source="a", message="m1"))
        store.store(StoredLog(timestamp=time.time(), level="error", source="b", message="m2"))
        stats = store.get_stats()
        assert stats["total_entries"] == 2


class TestFaultDiagnoser:
    def test_diagnose_heartbeat_timeout(self):
        from fusion_multi_node.observability.log_store import (
            FaultDiagnoser,
            LogStore,
            StoredLog,
        )

        store = LogStore(persist_to_disk=False)
        diagnoser = FaultDiagnoser()
        store.store(
            StoredLog(
                timestamp=time.time(),
                level="warning",
                source="master",
                message="节点心跳超时: node-1",
                node_id="node-1",
            )
        )
        store.store(
            StoredLog(
                timestamp=time.time(),
                level="warning",
                source="master",
                message="节点心跳超时: node-2",
                node_id="node-2",
            )
        )
        results = diagnoser.diagnose(store._entries)
        assert len(results) >= 1
        names = [r.pattern for r in results]
        assert "node_heartbeat_timeout" in names

    def test_diagnose_oom(self):
        from fusion_multi_node.observability.log_store import FaultDiagnoser, StoredLog

        diagnoser = FaultDiagnoser()
        logs = [
            StoredLog(
                timestamp=time.time(),
                level="error",
                source="agent",
                message="任务执行失败: OOM",
            )
        ]
        results = diagnoser.diagnose(logs)
        assert any(r.pattern == "task_execution_failure" for r in results)

    def test_analyze_frequency(self):
        from fusion_multi_node.observability.log_store import FaultDiagnoser, StoredLog

        diagnoser = FaultDiagnoser()
        logs = [
            StoredLog(timestamp=time.time(), level="info", source="master", message="a"),
            StoredLog(timestamp=time.time(), level="info", source="master", message="b"),
            StoredLog(timestamp=time.time(), level="info", source="agent", message="c"),
        ]
        freq = diagnoser.analyze_frequency(logs, group_by="source")
        assert freq["master"] == 2
        assert freq["agent"] == 1


# ── M9 Storage ──


class TestStorageVolume:
    def test_create_and_delete_volume(self):
        from fusion_multi_node.storage.storage_volume import StorageVolume, VolumeSpec

        with tempfile.TemporaryDirectory() as tmpdir:
            sv = StorageVolume(base_dir=tmpdir)
            spec = VolumeSpec(name="test-vol", volume_type=VolumeType.LOCAL)
            assert sv.create_volume(spec) is True
            info = sv.get_volume_info("test-vol")
            assert info is not None
            assert info.name == "test-vol"
            assert sv.delete_volume("test-vol") is True

    def test_write_read_file(self):
        from fusion_multi_node.storage.storage_volume import StorageVolume, VolumeSpec

        with tempfile.TemporaryDirectory() as tmpdir:
            sv = StorageVolume(base_dir=tmpdir)
            spec = VolumeSpec(name="data-vol", volume_type=VolumeType.LOCAL)
            sv.create_volume(spec)
            assert sv.write_file("data-vol", "test.txt", b"hello world") is True
            data = sv.read_file("data-vol", "test.txt")
            assert data == b"hello world"

    def test_delete_file(self):
        from fusion_multi_node.storage.storage_volume import StorageVolume, VolumeSpec

        with tempfile.TemporaryDirectory() as tmpdir:
            sv = StorageVolume(base_dir=tmpdir)
            sv.create_volume(VolumeSpec(name="vol1"))
            sv.write_file("vol1", "a.txt", b"data")
            assert sv.delete_file("vol1", "a.txt") is True
            assert sv.read_file("vol1", "a.txt") is None

    def test_list_files(self):
        from fusion_multi_node.storage.storage_volume import StorageVolume, VolumeSpec

        with tempfile.TemporaryDirectory() as tmpdir:
            sv = StorageVolume(base_dir=tmpdir)
            sv.create_volume(VolumeSpec(name="v2"))
            sv.write_file("v2", "a.txt", b"a")
            sv.write_file("v2", "sub/b.txt", b"b")
            files = sv.list_files("v2")
            assert len(files) == 2

    def test_list_volumes(self):
        from fusion_multi_node.storage.storage_volume import StorageVolume, VolumeSpec

        with tempfile.TemporaryDirectory() as tmpdir:
            sv = StorageVolume(base_dir=tmpdir)
            sv.create_volume(VolumeSpec(name="v1"))
            sv.create_volume(VolumeSpec(name="v2"))
            vols = sv.list_volumes()
            assert len(vols) == 2


class TestShardReplicator:
    def test_assign_replicas(self):
        from fusion_multi_node.storage.shard_replication import ShardReplicator

        replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
        nodes = [{"node_id": "n1"}, {"node_id": "n2"}, {"node_id": "n3"}]
        replicas = replicator.assign_replicas("shard-1", "/models/llama.bin", 1024, nodes)
        assert len(replicas) == 2

    def test_get_healthy_replica(self):
        from fusion_multi_node.storage.shard_replication import ShardReplicator

        replicator = ShardReplicator()
        nodes = [{"node_id": "n1"}]
        replicator.assign_replicas("s1", "file.bin", 100, nodes)
        healthy = replicator.get_healthy_replica("s1")
        assert healthy is not None

    def test_mark_failed(self):
        from fusion_multi_node.storage.shard_replication import ShardReplicator

        replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
        nodes = [{"node_id": "n1"}, {"node_id": "n2"}]
        replicator.assign_replicas("s1", "f", 100, nodes)
        replicator.mark_replica_failed("s1", "n1")
        replicas = replicator.get_replicas("s1")
        assert any(r.status == "failed" for r in replicas)

    def test_under_replicated(self):
        from fusion_multi_node.storage.shard_replication import ShardReplicator

        replicator = ShardReplicator(config=ReplicationConfig(replication_factor=3))
        nodes = [{"node_id": "n1"}]
        replicator.assign_replicas("s1", "f", 100, nodes)
        assert "s1" in replicator.get_under_replicated()


class TestShardQuorumE9:
    # AR审计 E9: quorum_read/quorum_write 无 storage_volume 时降级到内存自欺
    # (所有副本读/写同一 self._shard_data dict → consistent 恒 True / success 恒 True)。
    # 修后: 无 storage_volume 一律拒绝, success=False, error=no_storage_volume。

    def test_quorum_write_refuses_without_storage_volume(self):
        from fusion_multi_node.storage.shard_replication import (
            ReplicationConfig,
            ShardReplicator,
        )

        replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
        replicator.assign_replicas("q-w", "f", 10, [{"node_id": "n1"}, {"node_id": "n2"}])
        result = replicator.quorum_write("q-w", b"payload", storage_volume=None)
        assert result["success"] is False
        assert result["error"] == "no_storage_volume"

    def test_quorum_read_refuses_without_storage_volume(self):
        from fusion_multi_node.storage.shard_replication import (
            ReplicationConfig,
            ShardReplicator,
        )

        replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
        replicator.assign_replicas("q-r", "f", 10, [{"node_id": "n1"}, {"node_id": "n2"}])
        replicator.register_shard_data("q-r", b"payload")
        result = replicator.quorum_read("q-r", storage_volume=None)
        assert result["success"] is False
        assert result["error"] == "no_storage_volume"

    def test_quorum_write_succeeds_with_mock_storage_volume(self):
        from fusion_multi_node.storage.shard_replication import (
            ReplicationConfig,
            ShardReplicator,
        )

        class _MockVolume:
            def __init__(self):
                self.store = {}

            def write_file(self, volume_name, file_path, data):
                self.store[(volume_name, file_path)] = data
                return True

            def read_file(self, volume_name, file_path):
                return self.store.get((volume_name, file_path))

        volume = _MockVolume()
        replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
        replicator._local_node_id = "n1"
        replicator.assign_replicas("q-ok", "f", 10, [{"node_id": "n1"}, {"node_id": "n2"}])
        result = replicator.quorum_write("q-ok", b"payload", storage_volume=volume)
        # n1 本地写入成功 (storage_volume 非 None), n2 远端无 FMP → _sync_local 走 n2
        # 但 n2 != local → 无 fmp_interface → is_remote False → _sync_local 写 n2 副本
        assert result["success"] is True
        assert result["quorum"] == 1

    def test_quorum_read_succeeds_with_mock_storage_volume(self):
        from fusion_multi_node.storage.shard_replication import (
            ReplicationConfig,
            ShardReplicator,
        )

        class _MockVolume:
            def __init__(self, data):
                self.store = {}

            def read_file(self, volume_name, file_path):
                return b"payload"

            def write_file(self, volume_name, file_path, data):
                return True

        volume = _MockVolume(b"payload")
        replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
        replicator.assign_replicas("q-rok", "f", 10, [{"node_id": "n1"}, {"node_id": "n2"}])
        result = replicator.quorum_read("q-rok", storage_volume=volume)
        assert result["success"] is True
        assert result["data"] == b"payload"

    def test_quorum_write_no_replicas_still_refused(self):
        from fusion_multi_node.storage.shard_replication import (
            ReplicationConfig,
            ShardReplicator,
        )

        replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
        # 无 storage_volume 优先于 no_replicas 校验
        result = replicator.quorum_write("q-none", b"x", storage_volume=None)
        assert result["success"] is False
        assert result["error"] == "no_storage_volume"


class TestDistributedKVStorePartition:
    # AR审计硬伤4: fmp_server._on_kv_get 调 get_entry(key, partition), 原签名只 1 参
    # → inbound KV_GET 必 TypeError。增可选 partition 后须校验分区匹配。

    def test_get_entry_no_partition_returns_any(self):
        from fusion_multi_node.storage.kv_store import DistributedKVStore

        kv = DistributedKVStore(data_dir="")
        kv.put("k1", "v1", partition="alpha")
        entry = kv.get_entry("k1")
        assert entry is not None
        assert entry.value == "v1"

    def test_get_entry_partition_match(self):
        from fusion_multi_node.storage.kv_store import DistributedKVStore

        kv = DistributedKVStore(data_dir="")
        kv.put("k2", "v2", partition="alpha")
        entry = kv.get_entry("k2", partition="alpha")
        assert entry is not None
        assert entry.value == "v2"

    def test_get_entry_partition_mismatch_returns_none(self):
        from fusion_multi_node.storage.kv_store import DistributedKVStore

        kv = DistributedKVStore(data_dir="")
        kv.put("k3", "v3", partition="alpha")
        entry = kv.get_entry("k3", partition="beta")
        assert entry is None

    def test_get_entry_missing_key_returns_none(self):
        from fusion_multi_node.storage.kv_store import DistributedKVStore

        kv = DistributedKVStore(data_dir="")
        assert kv.get_entry("nope", partition="alpha") is None


class TestShardFmpSyncHonesty:
    # AR审计硬伤4: _sync_via_fmp 原 fire-and-forget (ensure_future 不 await) 却返
    # success=True/checksum_verified=True → quorum 写保证虚构。修后: 仅同步 await
    # 的 send 可称 success, fire-and-forget 路径返 success=False + "未确认" 日志。

    def test_fire_and_forget_marks_unconfirmed(self):
        # 事件循环内调用 → 走 ensure_future 分支 → success=False, bytes=0
        from fusion_multi_node.storage.shard_replication import (
            ReplicationConfig,
            ShardReplicator,
        )

        replicator = ShardReplicator(
            config=ReplicationConfig(replication_factor=1),
            fmp_interface=_FakeFmpInterface(),
        )
        replicator._local_node_id = "self"
        replicator.register_shard_data("shard-x", b"payload")
        replicator.assign_replicas("shard-x", "f", 7, [{"node_id": "remote"}])

        async def _run():
            # 事件循环内: ensure_future 分支
            return replicator.sync_to_node("shard-x", "remote")

        import asyncio

        result = asyncio.run(_run())
        assert result.success is False
        assert result.bytes_transferred == 0
        assert result.checksum_verified is False

    def test_sync_no_loop_marks_delivered(self):
        # sync_to_node 同步顶层调用 (无 running loop) → asyncio.run 分支 → success=True
        from fusion_multi_node.storage.shard_replication import (
            ReplicationConfig,
            ShardReplicator,
        )

        replicator = ShardReplicator(
            config=ReplicationConfig(replication_factor=1),
            fmp_interface=_FakeFmpInterface(),
        )
        replicator._local_node_id = "self"
        replicator.register_shard_data("shard-y", b"payload")
        replicator.assign_replicas("shard-y", "f", 7, [{"node_id": "remote"}])

        result = replicator.sync_to_node("shard-y", "remote")
        assert result.success is True
        assert result.bytes_transferred == 7
        assert result.checksum_verified is False  # 无应用层 ACK, 永不声称校验通过


class _FakeFmpInterface:
    # _sync_via_fmp 访问 _fmp_interface._conn_mgr.send_to
    class _ConnMgr:
        async def send_to(self, node_id, msg):
            return True

    _conn_mgr = _ConnMgr()


class TestCheckpointManager:
    def test_save_and_load(self):
        from fusion_multi_node.storage.checkpoint import (
            CheckpointEntry,
            CheckpointManager,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(checkpoint_dir=tmpdir, max_checkpoints=10)
            entry = CheckpointEntry(
                checkpoint_id="cp-1",
                task_id="task-1",
                node_id="node-1",
                step=5,
                state_data={"tokens": 100},
            )
            assert mgr.save(entry) is True
            loaded = mgr.load("cp-1")
            assert loaded is not None
            assert loaded.step == 5

    def test_load_latest(self):
        from fusion_multi_node.storage.checkpoint import (
            CheckpointEntry,
            CheckpointManager,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(checkpoint_dir=tmpdir)
            mgr.save(CheckpointEntry(checkpoint_id="cp-1", task_id="t1", node_id="n1", step=3))
            mgr.save(CheckpointEntry(checkpoint_id="cp-2", task_id="t1", node_id="n1", step=7))
            latest = mgr.load_latest("t1")
            assert latest is not None
            assert latest.step == 7

    def test_delete(self):
        from fusion_multi_node.storage.checkpoint import (
            CheckpointEntry,
            CheckpointManager,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(checkpoint_dir=tmpdir)
            mgr.save(CheckpointEntry(checkpoint_id="cp-1", task_id="t1", node_id="n1", step=1))
            assert mgr.delete("cp-1") is True
            assert mgr.load("cp-1") is None

    def test_list_by_task(self):
        from fusion_multi_node.storage.checkpoint import (
            CheckpointEntry,
            CheckpointManager,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = CheckpointManager(checkpoint_dir=tmpdir)
            mgr.save(CheckpointEntry(checkpoint_id="cp-1", task_id="t1", node_id="n1", step=1))
            mgr.save(CheckpointEntry(checkpoint_id="cp-2", task_id="t1", node_id="n1", step=2))
            mgr.save(CheckpointEntry(checkpoint_id="cp-3", task_id="t2", node_id="n1", step=1))
            entries = mgr.list_by_task("t1")
            assert len(entries) == 2


# ── M10 Autoscaler ──


class TestAutoscaler:
    def test_config_defaults(self):
        from fusion_multi_node.autoscaler.autoscaler import (
            AutoscalerConfig,
            ScalePolicy,
        )

        config = AutoscalerConfig()
        assert config.min_nodes == 1
        assert config.max_nodes == 10
        assert config.policy == ScalePolicy.BALANCED

    def test_policy_enum(self):
        from fusion_multi_node.autoscaler.autoscaler import ScalePolicy

        assert ScalePolicy.CONSERVATIVE.value == "conservative"
        assert ScalePolicy.AGGRESSIVE.value == "aggressive"

    def test_autoscaler_init(self):
        from fusion_multi_node.autoscaler.autoscaler import Autoscaler, ScalePolicy

        scaler = Autoscaler(policy=ScalePolicy.AGGRESSIVE)
        assert scaler.config.policy == ScalePolicy.AGGRESSIVE
        assert scaler.config.scale_up_threshold == 0.6

    @pytest.mark.asyncio
    async def test_evaluate_noop(self):
        from fusion_multi_node.autoscaler.autoscaler import Autoscaler, ScaleAction

        scaler = Autoscaler(
            get_cluster_state=lambda: {
                "nodes": [
                    {
                        "status": "online",
                        "active_tasks": 1,
                        "max_tasks": 4,
                        "last_heartbeat": time.time(),
                    }
                ],
                "tasks": [],
            }
        )
        action = await scaler.evaluate()
        assert action == ScaleAction.NOOP

    @pytest.mark.asyncio
    async def test_evaluate_scale_up(self):
        from fusion_multi_node.autoscaler.autoscaler import Autoscaler, ScaleAction

        scale_up_called = False

        def on_scale_up(count):
            nonlocal scale_up_called
            scale_up_called = True

        nodes = [
            {
                "status": "online",
                "active_tasks": 4,
                "max_tasks": 4,
                "last_heartbeat": time.time(),
                "node_id": "n1",
            }
        ]
        tasks = [{"status": "pending"}, {"status": "pending"}]

        scaler = Autoscaler(
            config=None,
            on_scale_up=on_scale_up,
            get_cluster_state=lambda: {"nodes": nodes, "tasks": tasks},
        )
        scaler._last_action_time = 0  # bypass cooldown
        action = await scaler.evaluate()
        assert action == ScaleAction.SCALE_UP
        assert scale_up_called is True

    def test_get_stats(self):
        from fusion_multi_node.autoscaler.autoscaler import Autoscaler

        scaler = Autoscaler()
        stats = scaler.get_stats()
        assert "policy" in stats
        assert "config" in stats

    @pytest.mark.asyncio
    async def test_update_config_preserves_cooldown(self):
        # AR审计硬伤4: update_config 原清零 _last_action_time → 热重载即绕过冷却门。
        # 修后: 首次扩容设冷却, update_config 后立即再 evaluate 须被冷却拦截 (NOOP)。
        from fusion_multi_node.autoscaler.autoscaler import (
            Autoscaler,
            AutoscalerConfig,
            ScaleAction,
        )

        scale_count = 0

        def on_scale_up(count):
            nonlocal scale_count
            scale_count += 1

        nodes = [
            {
                "status": "online",
                "active_tasks": 4,
                "max_tasks": 4,
                "last_heartbeat": time.time(),
                "node_id": "n1",
            }
        ]
        tasks = [{"status": "pending"}, {"status": "pending"}]
        state = lambda: {"nodes": nodes, "tasks": tasks}  # noqa: E731

        scaler = Autoscaler(
            config=AutoscalerConfig(cooldown_seconds=120.0),
            on_scale_up=on_scale_up,
            get_cluster_state=state,
        )
        scaler._last_action_time = 0  # 首次 bypass 冷却以触发扩容
        first = await scaler.evaluate()
        assert first == ScaleAction.SCALE_UP
        assert scale_count == 1
        # 热更新配置 — 修前会清零冷却, 修后保留
        scaler.update_config(AutoscalerConfig(cooldown_seconds=120.0))
        second = await scaler.evaluate()
        # 冷却期内须 NOOP, 不再次扩容
        assert second == ScaleAction.NOOP
        assert scale_count == 1


# ── M1-05 Manual Join ──


class TestManualJoin:
    def test_manual_join_manager(self):
        from fusion_multi_node.discovery.manual_join import ManualJoinManager

        mgr = ManualJoinManager(cluster_secret="test-secret", auto_approve=True)
        result = mgr.handle_join_request(
            {
                "node_id": "node-1",
                "hostname": "mac-1",
                "ip_address": "192.168.1.10",
                "port": 11458,
                "cluster_secret": "test-secret",
            }
        )
        assert result["status"] == "ok"
        assert result["node_id"] == "node-1"

    def test_manual_join_wrong_secret(self):
        from fusion_multi_node.discovery.manual_join import ManualJoinManager

        mgr = ManualJoinManager(cluster_secret="secret", auto_approve=True)
        result = mgr.handle_join_request(
            {
                "node_id": "node-1",
                "cluster_secret": "wrong",
            }
        )
        assert result["status"] == "error"

    def test_manual_join_no_node_id(self):
        from fusion_multi_node.discovery.manual_join import ManualJoinManager

        mgr = ManualJoinManager()
        result = mgr.handle_join_request({})
        assert result["status"] == "error"

    def test_join_history(self):
        from fusion_multi_node.discovery.manual_join import ManualJoinManager

        mgr = ManualJoinManager()
        mgr.handle_join_request({"node_id": "n1", "hostname": "h1", "ip_address": "1.1.1.1", "port": 11458})
        history = mgr.get_join_history()
        assert len(history) == 1
        assert mgr.join_count == 1

    def test_join_request_response_dataclass(self):
        from fusion_multi_node.discovery.manual_join import JoinRequest, JoinResponse

        req = JoinRequest(node_id="n1", hostname="h1", ip_address="1.1.1.1", port=11458)
        assert req.node_id == "n1"
        resp = JoinResponse(success=True, master_host="1.2.3.4", master_port=11452)
        assert resp.success is True

    # P1-8 (审计 §3.3): 集群密钥常量时间比较 — 空 secret 拒非空 req_secret (防绕过)。
    def test_manual_join_empty_req_secret_rejected(self):
        from fusion_multi_node.discovery.manual_join import ManualJoinManager

        mgr = ManualJoinManager(cluster_secret="real-secret", auto_approve=True)
        result = mgr.handle_join_request({"node_id": "node-1", "cluster_secret": ""})
        assert result["status"] == "error"
        assert "密钥" in result["detail"]

    # P1-9 (审计 §3.3): join URL 协议随 mTLS 开关 (mtls.scheme), 默认 http, mTLS 开则 https。
    def test_manual_join_client_url_uses_mtls_scheme(self, monkeypatch):
        from fusion_multi_node.discovery.manual_join import ManualJoinClient
        from fusion_multi_node.security import mtls

        monkeypatch.delenv("FUSION_MTLS_ENABLED", raising=False)
        assert mtls.scheme() == "http"
        client = ManualJoinClient(node_id="n1", cluster_secret="s")
        # monkeypatch client.post 捕 URL 验协议前缀。
        captured = {}

        class _FakeClient:
            is_closed = False

            async def post(self, url, json=None):
                captured["url"] = url
                return _FakeResp()

            async def get(self, url):
                captured["health_url"] = url
                return _FakeResp()

            async def aclose(self):
                pass

        class _FakeResp:
            status_code = 200

            def json(self):
                # join 返 {status,node_id}; verify_master (/api/health) 返 {role:master}。
                return {"status": "ok", "node_id": "n1", "role": "master"}

        client._client = _FakeClient()

        async def _run():
            result = await client.join("192.168.1.10", 11452)
            assert result.success is True
            assert captured["url"].startswith("http://"), "默认无 mTLS 应走 http"
            ok = await client.verify_master("192.168.1.10", 11452)
            assert ok is True
            assert captured["health_url"].startswith("http://")

        asyncio.run(_run())


# ── P1-3 (审计 §3.7) HTTP 派发路径 PII 可选脱敏 ──


class TestHttpPiiScrub:
    def test_config_default_off(self):
        # P1-3: 默认 False (LAN 明文), 校验器接纳 bool。
        from fusion_multi_node.config import ClusterConfig

        cfg = ClusterConfig(config_path="/tmp/_test_pii_cfg_default.json")
        assert cfg.get("security.http_pii_scrub") is False

    def test_master_scrub_off_by_default(self):
        # P1-3: 无 config 注入 → _is_http_pii_scrub_enabled 返 False (明文)。
        from fusion_multi_node.master import ClusterMaster

        master = ClusterMaster()
        assert master._is_http_pii_scrub_enabled() is False
        payload = {"prompt": "联系 13912345678", "messages": []}
        out = master._scrub_payload_text_fields(payload)
        assert out["prompt"] == "联系 13912345678", "默认关不应脱敏"

    def test_master_scrub_on_when_config_enabled(self, tmp_path):
        # P1-3: config security.http_pii_scrub=True → dispatch payload prompt/messages 脱敏。
        import json

        from fusion_multi_node.config import ClusterConfig
        from fusion_multi_node.master import ClusterMaster

        cfg_path = tmp_path / "pii_cfg.json"
        cfg_path.write_text(json.dumps({"security": {"http_pii_scrub": True}}))
        cfg = ClusterConfig(config_path=str(cfg_path))
        assert cfg.get("security.http_pii_scrub") is True
        master = ClusterMaster()
        master._cluster_config = cfg
        assert master._is_http_pii_scrub_enabled() is True
        payload = {
            "prompt": "我的手机 13912345678, 邮箱 a@b.com",
            "messages": [{"role": "user", "content": "sk-1234567890abcdefghijklmnopqrst"}],
        }
        out = master._scrub_payload_text_fields(payload)
        assert "13912345678" not in out["prompt"], "prompt 手机号应脱敏"
        assert "a@b.com" not in out["prompt"], "prompt 邮箱应脱敏"
        assert "***OPENAIKEY***" in out["messages"][0]["content"], "messages sk- key 应脱敏"

    def test_warm_cache_scrub_env_gated(self, tmp_path, monkeypatch):
        # P1-3: warm_cache (agent 侧 KVSharingManager) env FUSION_HTTP_PII_SCRUB=1 → prompt 脱敏。
        import asyncio

        from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager

        captured = {}

        class _FakeResp:
            status_code = 200

        class _FakeClient:
            is_closed = False

            async def post(self, url, json=None, headers=None):
                captured["json"] = json
                return _FakeResp()

            async def aclose(self):
                pass

        mgr = KVSharingManager(persist_path=str(tmp_path / "kv.json"))
        mgr._http_client = _FakeClient()

        async def _run():
            await mgr.warm_cache("m", ["请联系 13912345678"], ["127.0.0.1"])

        monkeypatch.setenv("FUSION_HTTP_PII_SCRUB", "1")
        asyncio.run(_run())
        assert "13912345678" not in captured["json"]["prompt"], "env 开启 warm prompt 应脱敏"
        assert "***PHONE***" in captured["json"]["prompt"]


# ── M4-01 LoadMetrics + LoadRouter Tests ──


class TestLoadMetrics:
    def test_defaults(self):
        m = LoadMetrics()
        assert m.uma_used_ratio == 0.0
        assert m.cpu_percent == 0.0
        assert m.metal_util == 0.0
        assert m.task_queue_len == 0
        assert m.net_rtt_ms == 0.0
        assert m.timestamp > 0

    def test_uma_available_ratio(self):
        m = LoadMetrics(uma_used_ratio=0.3)
        assert abs(m.uma_available_ratio - 0.7) < 0.001

    def test_is_stale(self):
        m = LoadMetrics(timestamp=time.time() - 60)
        assert m.is_stale
        m2 = LoadMetrics(timestamp=time.time())
        assert not m2.is_stale

    def test_to_dict_roundtrip(self):
        m = LoadMetrics(
            uma_used_ratio=0.5,
            cpu_percent=30.0,
            metal_util=0.1,
            task_queue_len=3,
            net_rtt_ms=10.0,
            node_id="n1",
        )
        d = m.to_dict()
        m2 = LoadMetrics.from_dict(d)
        assert m2.uma_used_ratio == 0.5
        assert m2.cpu_percent == 30.0
        assert m2.node_id == "n1"

    def test_post_init_timestamp(self):
        m = LoadMetrics()
        assert m.timestamp > 0


class TestLoadRouter:
    def _make_router(self):
        router = LoadRouter(strategy=RoutingStrategy.BALANCED)
        router.update_metrics(
            "n1",
            LoadMetrics(
                uma_used_ratio=0.2,
                cpu_percent=20.0,
                metal_util=0.1,
                task_queue_len=1,
                net_rtt_ms=5.0,
            ),
        )
        router.update_metrics(
            "n2",
            LoadMetrics(
                uma_used_ratio=0.8,
                cpu_percent=90.0,
                metal_util=0.9,
                task_queue_len=7,
                net_rtt_ms=80.0,
            ),
        )
        router.update_metrics(
            "n3",
            LoadMetrics(
                uma_used_ratio=0.4,
                cpu_percent=40.0,
                metal_util=0.3,
                task_queue_len=2,
                net_rtt_ms=15.0,
            ),
        )
        return router

    def test_compute_score_high(self):
        router = LoadRouter()
        m = LoadMetrics(
            uma_used_ratio=0.1,
            cpu_percent=10.0,
            metal_util=0.1,
            task_queue_len=0,
            net_rtt_ms=1.0,
        )
        score = router.compute_score(m)
        assert score > 0.8

    def test_compute_score_low(self):
        router = LoadRouter()
        m = LoadMetrics(
            uma_used_ratio=0.9,
            cpu_percent=95.0,
            metal_util=0.9,
            task_queue_len=8,
            net_rtt_ms=90.0,
        )
        score = router.compute_score(m)
        assert score < 0.2

    def test_select_best(self):
        router = self._make_router()
        result = router.select_best(["n1", "n2", "n3"])
        assert result is not None
        assert result.node_id == "n1"
        assert result.score > 0.5

    def test_select_best_preferred_bonus(self):
        router = self._make_router()
        result = router.select_best(["n1", "n2", "n3"], preferred_node_id="n3")
        assert result is not None
        assert result.node_id == "n1"
        assert result.breakdown.get("preferred_bonus") is None or result.node_id == "n1"
        result_n3 = router.select_best(["n3", "n2"], preferred_node_id="n3")
        assert result_n3.node_id == "n3"

    def test_select_best_no_candidates(self):
        router = LoadRouter()
        result = router.select_best(["nonexistent"])
        assert result is None

    def test_select_n(self):
        router = self._make_router()
        results = router.select_n(["n1", "n2", "n3"], count=2)
        assert len(results) == 2
        assert results[0].score >= results[1].score

    def test_select_n_required_uma(self):
        router = self._make_router()
        results = router.select_n(["n1", "n2", "n3"], count=3, required_uma_ratio=0.5)
        assert all(r.metrics.uma_available_ratio >= 0.5 for r in results)

    def test_set_strategy(self):
        router = LoadRouter()
        router.set_strategy(RoutingStrategy.VRAM_FIRST)
        assert router.strategy == RoutingStrategy.VRAM_FIRST

    def test_remove_node(self):
        router = self._make_router()
        router.remove_node("n1")
        assert router.get_metrics("n1") is None

    def test_stale_metrics_skipped(self):
        router = LoadRouter()
        stale = LoadMetrics(uma_used_ratio=0.1, cpu_percent=10.0, timestamp=time.time() - 60)
        router.update_metrics("n_stale", stale)
        router._metrics["n_stale"].timestamp = time.time() - 60
        result = router.select_best(["n_stale"])
        assert result is None

    def test_score_breakdown(self):
        router = LoadRouter()
        m = LoadMetrics(
            uma_used_ratio=0.5,
            cpu_percent=50.0,
            metal_util=0.5,
            task_queue_len=4,
            net_rtt_ms=50.0,
        )
        bd = router.score_breakdown(m)
        assert "uma" in bd
        assert "cpu" in bd
        assert "total" in bd
        assert abs(bd["total"] - router.compute_score(m)) < 0.001

    def test_cluster_load_summary(self):
        router = self._make_router()
        summary = router.get_cluster_load_summary()
        assert summary["node_count"] == 3
        assert "avg_score" in summary
        assert summary["strategy"] == "balanced"


class TestRoutingWeights:
    def test_default_valid(self):
        w = RoutingWeights()
        assert w.validate()

    def test_vram_first_valid(self):
        assert STRATEGY_WEIGHTS[RoutingStrategy.VRAM_FIRST].validate()

    def test_locality_first_valid(self):
        assert STRATEGY_WEIGHTS[RoutingStrategy.LOCALITY_FIRST].validate()

    def test_low_latency_valid(self):
        assert STRATEGY_WEIGHTS[RoutingStrategy.LOW_LATENCY].validate()

    def test_invalid_weights(self):
        w = RoutingWeights(
            uma_weight=0.5,
            cpu_weight=0.5,
            metal_weight=0.5,
            queue_weight=0.5,
            net_weight=0.5,
        )
        assert not w.validate()


class TestLoadRouterIntegration:
    def test_cluster_master_has_load_router(self):
        cm = ClusterMaster()
        assert hasattr(cm, "load_router")
        assert isinstance(cm.load_router, LoadRouter)

    def test_register_node_syncs_metrics(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(
                node_id="n1",
                hostname="mac1",
                ip_address="192.168.1.10",
                port=11458,
                total_memory_gb=16.0,
                available_memory_gb=12.0,
                active_tasks=1,
                network_rtt_ms=5.0,
            )
            await cm.register_node(info)
            m = cm.load_router.get_metrics("n1")
            assert m is not None
            assert abs(m.uma_used_ratio - 0.25) < 0.01

        asyncio.run(_test())

    def test_update_node_load(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(
                node_id="n1",
                hostname="mac1",
                ip_address="192.168.1.10",
                port=11458,
                total_memory_gb=16.0,
                available_memory_gb=12.0,
            )
            await cm.register_node(info)
            metrics = LoadMetrics(
                uma_used_ratio=0.6,
                cpu_percent=50.0,
                metal_util=0.3,
                task_queue_len=3,
                net_rtt_ms=10.0,
            )
            await cm.update_node_load("n1", metrics)
            m = cm.load_router.get_metrics("n1")
            assert m.uma_used_ratio == 0.6
            assert cm.nodes["n1"].active_tasks == 3

        asyncio.run(_test())

    def test_unregister_removes_from_router(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(node_id="n1", hostname="mac1", ip_address="192.168.1.10", port=11458)
            await cm.register_node(info)
            assert cm.load_router.get_metrics("n1") is not None
            await cm.unregister_node("n1")
            assert cm.load_router.get_metrics("n1") is None

        asyncio.run(_test())

    def test_stats_includes_load_summary(self):
        async def _test():
            cm = ClusterMaster()
            stats = await cm.get_stats()
            assert "load_summary" in stats
            assert stats["load_summary"]["node_count"] == 0

        asyncio.run(_test())


# ── M4-02/03 本地强制门控 + VRAM优先调度 Tests ──


class TestLocalForceGate:
    def test_lightweight_model_forced_local(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(
                node_id="local",
                hostname="mac1",
                ip_address="127.0.0.1",
                port=11458,
                total_memory_gb=16.0,
                available_memory_gb=14.0,
            )
            await cm.register_node(info)
            task = ClusterTask(
                task_id="t1",
                name="tiny",
                mode=ParallelMode.DATA,
                model_name="qwen-0.5b",
                preferred_node_id="local",
            )
            ok = await cm.assign_task(task)
            assert ok
            assert task.assigned_nodes == ["local"]

        asyncio.run(_test())

    def test_1b_model_forced_local(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(
                node_id="local",
                hostname="mac1",
                ip_address="127.0.0.1",
                port=11458,
                total_memory_gb=16.0,
                available_memory_gb=14.0,
            )
            await cm.register_node(info)
            task = ClusterTask(
                task_id="t2",
                name="small",
                mode=ParallelMode.DATA,
                model_name="qwen-1b",
                preferred_node_id="local",
            )
            ok = await cm.assign_task(task)
            assert ok
            assert task.assigned_nodes == ["local"]

        asyncio.run(_test())

    def test_local_force_fallback_when_preferred_unavailable(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(
                node_id="remote",
                hostname="mac2",
                ip_address="192.168.1.11",
                port=11458,
                total_memory_gb=32.0,
                available_memory_gb=28.0,
            )
            await cm.register_node(info)
            task = ClusterTask(
                task_id="t3",
                name="tiny",
                mode=ParallelMode.DATA,
                model_name="qwen-0.5b",
                preferred_node_id="missing_node",
            )
            ok = await cm.assign_task(task)
            assert ok
            assert "remote" in task.assigned_nodes

        asyncio.run(_test())

    def test_large_model_not_forced_local(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(
                node_id="local",
                hostname="mac1",
                ip_address="127.0.0.1",
                port=11458,
                total_memory_gb=64.0,
                available_memory_gb=60.0,
            )
            await cm.register_node(info)
            task = ClusterTask(
                task_id="t4",
                name="big",
                mode=ParallelMode.DATA,
                model_name="llama-70b",
                preferred_node_id="local",
            )
            ok = await cm.assign_task(task)
            assert ok
            assert "local" in task.assigned_nodes

        asyncio.run(_test())


class TestVRAMFirstScheduling:
    def test_vram_first_strategy_for_large_model(self):
        async def _test():
            cm = ClusterMaster()
            low_vram = NodeInfo(
                node_id="n_low",
                hostname="low",
                ip_address="10.0.0.1",
                port=11458,
                total_memory_gb=16.0,
                available_memory_gb=14.0,
            )
            high_vram = NodeInfo(
                node_id="n_high",
                hostname="high",
                ip_address="10.0.0.2",
                port=11458,
                total_memory_gb=64.0,
                available_memory_gb=58.0,
            )
            await cm.register_node(low_vram)
            await cm.register_node(high_vram)
            task = ClusterTask(
                task_id="t_big",
                name="big_model",
                mode=ParallelMode.DATA,
                model_name="llama-32b",
            )
            ok = await cm.assign_task(task)
            assert ok
            assert "n_high" in task.assigned_nodes

        asyncio.run(_test())

    def test_strategy_restored_after_vram_first(self):
        async def _test():
            cm = ClusterMaster()
            assert cm.load_router.strategy == RoutingStrategy.BALANCED
            info = NodeInfo(
                node_id="n1",
                hostname="mac1",
                ip_address="10.0.0.1",
                port=11458,
                total_memory_gb=64.0,
                available_memory_gb=60.0,
            )
            await cm.register_node(info)
            task = ClusterTask(
                task_id="t_big",
                name="big",
                mode=ParallelMode.DATA,
                model_name="llama-70b",
            )
            await cm.assign_task(task)
            assert cm.load_router.strategy == RoutingStrategy.BALANCED

        asyncio.run(_test())

    def test_small_model_uses_balanced_strategy(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(
                node_id="n1",
                hostname="mac1",
                ip_address="10.0.0.1",
                port=11458,
                total_memory_gb=16.0,
                available_memory_gb=14.0,
            )
            await cm.register_node(info)
            task = ClusterTask(
                task_id="t_small",
                name="small",
                mode=ParallelMode.DATA,
                model_name="qwen-3b",
            )
            await cm.assign_task(task)
            assert cm.load_router.strategy == RoutingStrategy.BALANCED

        asyncio.run(_test())


# ── M5-01/02/05 Task Sharding Tests ──


class TestShardingType:
    def test_enum_values(self):
        assert ShardingType.INFERENCE.value == "inference"
        assert ShardingType.AST.value == "ast"
        assert ShardingType.VECTORIZE.value == "vectorize"


class TestShardingStrategy:
    def test_enum_values(self):
        assert ShardingStrategy.BY_FILE.value == "by_file"
        assert ShardingStrategy.BY_DOCUMENT.value == "by_document"
        assert ShardingStrategy.BY_BATCH.value == "by_batch"


class TestTaskSharder:
    def _make_file_items(self, n=20):
        return [{"file_path": f"file_{i % 5}.py", "content": f"code_{i}"} for i in range(n)]

    def _make_inference_items(self, n=30):
        return [{"text": f"prompt_{i}"} for i in range(n)]

    def _make_doc_items(self, n=25):
        return [{"document_id": f"doc_{i % 3}", "text": f"text_{i}"} for i in range(n)]

    def test_by_batch_inference(self):
        sharder = TaskSharder()
        items = self._make_inference_items(30)
        shards = sharder.create_shards("t1", ShardingType.INFERENCE, items, shard_size=10)
        assert len(shards) == 3
        assert all(s.sharding_type == ShardingType.INFERENCE for s in shards)
        assert shards[0].total_shards == 3

    def test_by_file_ast(self):
        sharder = TaskSharder()
        items = self._make_file_items(20)
        shards = sharder.create_shards("t2", ShardingType.AST, items)
        assert len(shards) >= 1
        assert all(s.sharding_type == ShardingType.AST for s in shards)

    def test_by_document_vectorize(self):
        sharder = TaskSharder()
        items = self._make_doc_items(25)
        shards = sharder.create_shards("t3", ShardingType.VECTORIZE, items)
        assert len(shards) >= 1
        assert all(s.sharding_type == ShardingType.VECTORIZE for s in shards)

    def test_empty_items(self):
        sharder = TaskSharder()
        shards = sharder.create_shards("t4", ShardingType.INFERENCE, [])
        assert shards == []

    def test_max_shards_limit(self):
        sharder = TaskSharder(max_shards=4)
        items = self._make_inference_items(100)
        shards = sharder.create_shards("t5", ShardingType.INFERENCE, items, shard_size=1)
        assert len(shards) <= 4

    def test_get_shards(self):
        sharder = TaskSharder()
        items = self._make_inference_items(10)
        sharder.create_shards("t6", ShardingType.INFERENCE, items, shard_size=5)
        assert len(sharder.get_shards("t6")) == 2
        assert sharder.get_shards("nonexistent") == []

    def test_get_shard(self):
        sharder = TaskSharder()
        items = self._make_inference_items(10)
        sharder.create_shards("t7", ShardingType.INFERENCE, items, shard_size=10)
        s = sharder.get_shard("t7_shard_0")
        assert s is not None
        assert s.shard_index == 0

    def test_update_shard_status(self):
        sharder = TaskSharder()
        items = self._make_inference_items(5)
        sharder.create_shards("t8", ShardingType.INFERENCE, items)
        ok = sharder.update_shard_status("t8_shard_0", "completed", {"results": ["ok"]})
        assert ok
        s = sharder.get_shard("t8_shard_0")
        assert s.status == "completed"
        assert s.result == {"results": ["ok"]}

    def test_update_shard_status_nonexistent(self):
        sharder = TaskSharder()
        assert not sharder.update_shard_status("bad_id", "completed")


class TestTaskShard:
    def test_auto_timestamp(self):
        s = TaskShard(
            shard_id="s1",
            parent_task_id="t1",
            sharding_type=ShardingType.INFERENCE,
            shard_index=0,
            total_shards=1,
        )
        assert s.created_at > 0


class TestShardMerger:
    def _make_completed_shards(self, sharding_type, count=3):
        shards = []
        for i in range(count):
            s = TaskShard(
                shard_id=f"s{i}",
                parent_task_id="t1",
                sharding_type=sharding_type,
                shard_index=i,
                total_shards=count,
            )
            s.status = "completed"
            s.result = {"results": [f"result_{i}"]}
            shards.append(s)
        return shards

    def test_merge_inference(self):
        shards = self._make_completed_shards(ShardingType.INFERENCE, 3)
        merger = ShardMerger()
        result = merger.merge("t1", shards)
        assert result.success_count == 3
        assert result.fail_count == 0
        assert result.is_complete
        assert result.success_rate == 1.0
        assert "results" in result.data

    def test_merge_with_failures(self):
        shards = self._make_completed_shards(ShardingType.INFERENCE, 3)
        shards[1].status = "failed"
        shards[1].error = "timeout"
        merger = ShardMerger()
        result = merger.merge("t1", shards)
        assert result.success_count == 2
        assert result.fail_count == 1
        assert len(result.errors) == 1

    def test_merge_ast(self):
        shards = self._make_completed_shards(ShardingType.AST, 2)
        for s in shards:
            s.result = {"tree": {"type": "module", "name": f"file_{s.shard_index}"}}
        merger = ShardMerger()
        result = merger.merge("t1", shards)
        assert result.sharding_type == ShardingType.AST
        assert "trees" in result.data

    def test_merge_vectorize(self):
        shards = self._make_completed_shards(ShardingType.VECTORIZE, 2)
        for s in shards:
            s.result = {"embeddings": [0.1, 0.2], "token_count": 100}
        merger = ShardMerger()
        result = merger.merge("t1", shards)
        assert "embeddings" in result.data
        assert result.data["total_tokens"] == 200

    def test_merge_empty(self):
        merger = ShardMerger()
        result = merger.merge("t1", [])
        assert result.total_shards == 0


class TestMergedResult:
    def test_is_complete(self):
        r = MergedResult(
            task_id="t1",
            sharding_type=ShardingType.INFERENCE,
            total_shards=3,
            success_count=2,
            fail_count=1,
        )
        assert r.is_complete

    def test_not_complete(self):
        r = MergedResult(
            task_id="t1",
            sharding_type=ShardingType.INFERENCE,
            total_shards=3,
            success_count=2,
            fail_count=0,
        )
        assert not r.is_complete

    def test_success_rate(self):
        r = MergedResult(
            task_id="t1",
            sharding_type=ShardingType.INFERENCE,
            total_shards=4,
            success_count=3,
            fail_count=1,
        )
        assert abs(r.success_rate - 0.75) < 0.01


# ── M9-02/03/05 Storage Enhancement Tests ──


class TestCapacityMonitoring:
    def test_capacity_report_unlimited(self):
        sv = StorageVolume(base_dir="/tmp/test_cap_ul")
        sv.create_volume(VolumeSpec(name="unlimited"))
        report = sv.get_capacity_report("unlimited")
        assert report is not None
        assert report.total_mb == 0
        assert not report.needs_eviction

    def test_capacity_report_with_limit(self):
        sv = StorageVolume(base_dir="/tmp/test_cap_lim")
        sv.create_volume(VolumeSpec(name="limited", size_limit_mb=1))
        sv.write_file("limited", "test.txt", b"hello")
        report = sv.get_capacity_report("limited")
        assert report is not None
        assert report.total_mb == 1
        assert report.used_mb > 0

    def test_check_all_capacities(self):
        sv = StorageVolume(base_dir="/tmp/test_cap_all")
        sv.create_volume(VolumeSpec(name="v1", size_limit_mb=10))
        sv.create_volume(VolumeSpec(name="v2", size_limit_mb=20))
        reports = sv.check_all_capacities()
        assert len(reports) == 2

    def test_eviction_triggers_on_full(self):
        sv = StorageVolume(base_dir="/tmp/test_evict")
        sv.create_volume(VolumeSpec(name="ev", size_limit_mb=1))
        sv.write_file("ev", "old.txt", b"x" * 500000)
        sv.write_file("ev", "new.txt", b"y" * 500000)
        info = sv.get_volume_info("ev")
        assert info.file_count == 2
        report = sv.get_capacity_report("ev")
        if report and report.usage_ratio >= sv.EVICTION_HIGH_WATERMARK:
            assert report.needs_eviction


class TestLRUEviction:
    def test_lru_eviction_frees_space(self):
        sv = StorageVolume(base_dir="/tmp/test_lru")
        sv.create_volume(VolumeSpec(name="lru", size_limit_mb=1))
        sv.write_file("lru", "a.txt", b"a" * 400000)
        time.sleep(0.01)
        sv.write_file("lru", "b.txt", b"b" * 400000)
        # Try writing a large file that triggers eviction
        sv.write_file("lru", "c.txt", b"c" * 400000)
        # Either eviction happened or write failed
        info = sv.get_volume_info("lru")
        assert info.used_bytes <= 1024 * 1024


class TestShardDistribution:
    def test_distribute_shard(self):
        sv = StorageVolume(base_dir="/tmp/test_dist")
        sv.create_volume(VolumeSpec(name="models"))
        ok = sv.distribute_shard(
            shard_id="shard_001",
            shard_data=b"model_weight_data",
            target_volume="models",
            shard_path="llama/shard_001.bin",
            node_id="worker-1",
        )
        assert ok
        dist = sv.get_shard_distribution("shard_001")
        assert len(dist) == 1
        assert dist[0]["node_id"] == "worker-1"

    def test_verify_shard(self):
        sv = StorageVolume(base_dir="/tmp/test_verify")
        sv.create_volume(VolumeSpec(name="models"))
        data = b"model_weight_data_12345"
        sv.distribute_shard("shard_002", data, "models", "llama/shard_002.bin", "w1")
        assert sv.verify_shard("models", "llama/shard_002.bin", len(data))
        assert not sv.verify_shard("models", "llama/shard_002.bin", 999)


class TestShardReplicatorSync:
    def test_register_and_sync(self):
        sv = StorageVolume(base_dir="/tmp/test_repl_sync")
        sv.create_volume(VolumeSpec(name="models"))
        replicator = ShardReplicator()
        data = b"shard_data_content"
        replicator.register_shard_data("s1", data)
        replicator.assign_replicas(
            "s1",
            "model/shard.bin",
            len(data),
            [{"node_id": "n1"}, {"node_id": "n2"}],
        )
        result = replicator.sync_to_node("s1", "n1", storage_volume=sv)
        assert result.success
        assert result.bytes_transferred == len(data)
        assert result.checksum_verified

    def test_sync_no_data_registered(self):
        replicator = ShardReplicator()
        result = replicator.sync_to_node("missing", "n1")
        assert not result.success
        assert "未注册" in result.error

    def test_sync_all_replicas(self):
        sv = StorageVolume(base_dir="/tmp/test_repl_all")
        sv.create_volume(VolumeSpec(name="models"))
        replicator = ShardReplicator()
        data = b"all_shard_data"
        replicator.register_shard_data("s2", data)
        replicator.assign_replicas(
            "s2",
            "model/s2.bin",
            len(data),
            [{"node_id": "n1"}, {"node_id": "n2"}],
        )
        results = replicator.sync_all_replicas("s2", storage_volume=sv)
        assert len(results) == 2
        assert all(r.success for r in results)

    def test_repair_replica(self):
        sv = StorageVolume(base_dir="/tmp/test_repl_repair")
        sv.create_volume(VolumeSpec(name="models"))
        replicator = ShardReplicator()
        data = b"repair_data"
        replicator.register_shard_data("s3", data)
        replicator.assign_replicas(
            "s3",
            "model/s3.bin",
            len(data),
            [{"node_id": "n1"}],
        )
        replicator.mark_replica_failed("s3", "n1")
        result = replicator.repair_replica("s3", "n1", storage_volume=sv)
        assert result.success
        replica = replicator.get_replicas("s3")[0]
        assert replica.status == "active"

    def test_get_shard_data(self):
        replicator = ShardReplicator()
        replicator.register_shard_data("s4", b"data4")
        assert replicator.get_shard_data("s4") == b"data4"
        assert replicator.get_shard_data("missing") is None

    def test_stats_include_cached_data(self):
        replicator = ShardReplicator()
        replicator.register_shard_data("s5", b"data5")
        stats = replicator.get_stats()
        assert stats["cached_shard_data"] == 1


# ── M9-04 FMP Protocol Messages ──


class TestFMPMessage:
    def test_fmp_create(self):
        from fusion_multi_node.protocol import FMPMessage, PayloadType

        msg = FMPMessage.create(
            source_id="node1",
            target_id="node2",
            payload_type=PayloadType.HEARTBEAT,
            payload={"status": "ok"},
        )
        assert msg.message_id.startswith("fmp_")
        assert msg.link.source_id == "node1"
        assert msg.link.target_id == "node2"
        assert msg.business.payload_type == PayloadType.HEARTBEAT
        assert not msg.encrypted

    def test_fmp_link_layer_forward(self):
        from fusion_multi_node.protocol import FMPLinkLayer

        link = FMPLinkLayer(source_id="a", target_id="b", max_hops=3)
        assert link.can_forward()
        link.forward("c")
        assert link.hop_count == 1
        assert "c" in link.trace

    def test_fmp_link_layer_max_hops(self):
        from fusion_multi_node.protocol import FMPLinkLayer

        link = FMPLinkLayer(source_id="a", target_id="b", max_hops=1)
        link.forward("c")
        assert not link.can_forward()
        with pytest.raises(ValueError):
            link.forward("d")

    def test_fmp_business_round(self):
        from fusion_multi_node.protocol import FMPBusinessLayer, PayloadType

        biz = FMPBusinessLayer(
            payload_type=PayloadType.HEARTBEAT,
            payload=b"test",
            max_rounds=2,
        )
        assert biz.can_next_round()
        biz.next_round()
        assert biz.round_number == 1
        assert biz.can_next_round()
        biz.next_round()
        assert not biz.can_next_round()

    def test_fmp_serialize_deserialize(self):
        from fusion_multi_node.protocol import FMPMessage, PayloadType

        msg = FMPMessage.create(
            source_id="s1",
            target_id="t1",
            payload_type=PayloadType.TASK_ASSIGN,
            payload={"task_id": "xyz"},
        )
        raw = msg.serialize()
        assert isinstance(raw, bytes)
        restored = FMPMessage.deserialize(raw)
        assert restored.message_id == msg.message_id
        assert restored.link.source_id == "s1"
        assert restored.business.payload_type == PayloadType.TASK_ASSIGN

    def test_fmp_dict_roundtrip(self):
        from fusion_multi_node.protocol import FMPMessage, PayloadType

        msg = FMPMessage.create(
            source_id="s2",
            target_id="t2",
            payload_type=PayloadType.KV_LOOKUP,
            payload={"key": "val"},
        )
        d = msg.to_dict()
        restored = FMPMessage.from_dict(d)
        assert restored.message_id == msg.message_id
        assert restored.link.source_id == "s2"

    def test_fmp_deserialize_invalid_magic(self):
        from fusion_multi_node.protocol import FMPMessage

        with pytest.raises(ValueError, match="MAGIC"):
            FMPMessage.deserialize(b"\x00\x00\x00\x00" + b"\x00" * 8 + b"{}")

    def test_fmp_deserialize_too_short(self):
        from fusion_multi_node.protocol import FMPMessage

        with pytest.raises(ValueError, match="过短"):
            FMPMessage.deserialize(b"\x00\x00")


class TestFMPCrypto:
    def test_generate_key(self):
        from fusion_multi_node.protocol import FMPCrypto

        key = FMPCrypto.generate_key()
        assert len(key) == 32

    def test_encrypt_decrypt_roundtrip(self):
        from fusion_multi_node.protocol import FMPCrypto

        key = FMPCrypto.generate_key()
        crypto = FMPCrypto(key=key)
        plaintext = b"hello fusion cluster"
        encrypted = crypto.encrypt(plaintext)
        assert encrypted != plaintext
        decrypted = crypto.decrypt(encrypted)
        assert decrypted == plaintext

    def test_encrypt_with_aad(self):
        from fusion_multi_node.protocol import FMPCrypto

        key = FMPCrypto.generate_key()
        crypto = FMPCrypto(key=key)
        plaintext = b"secure data"
        aad = b"node1:node2"
        encrypted = crypto.encrypt(plaintext, aad=aad)
        decrypted = crypto.decrypt(encrypted, aad=aad)
        assert decrypted == plaintext

    def test_no_key_raises(self):
        from fusion_multi_node.protocol import FMPCrypto

        crypto = FMPCrypto()
        with pytest.raises(RuntimeError):
            crypto.encrypt(b"data")

    def test_invalid_key_length(self):
        from fusion_multi_node.protocol import FMPCrypto

        with pytest.raises(ValueError):
            FMPCrypto(key=b"short")

    def test_encrypt_decrypt_message(self):
        from fusion_multi_node.protocol import FMPCrypto, FMPMessage, PayloadType

        msg = FMPMessage.create(
            source_id="s1",
            target_id="t1",
            payload_type=PayloadType.CHAT_COMPLETION,
            payload={"prompt": "hello"},
        )
        key = FMPCrypto.generate_key()
        crypto = FMPCrypto(key=key)
        encrypted_msg = crypto.encrypt_message(msg)
        assert encrypted_msg.encrypted
        decrypted_msg = crypto.decrypt_message(encrypted_msg)
        assert not decrypted_msg.encrypted


class TestKVCacheSyncMessage:
    def test_create_sync_message(self):
        from fusion_multi_node.protocol import KVCacheSyncMessage

        msg = KVCacheSyncMessage(
            cache_id="cache_123",
            model_name="llama-3b",
            source_node_id="node1",
            size_mb=128.5,
        )
        assert msg.protocol == "fmp"
        assert msg.cache_id == "cache_123"

    def test_sync_message_dict_roundtrip(self):
        from fusion_multi_node.protocol import KVCacheSyncMessage

        msg = KVCacheSyncMessage(
            cache_id="cache_456",
            model_name="qwen-7b",
            source_node_id="node2",
            size_mb=256.0,
        )
        d = msg.to_dict()
        restored = KVCacheSyncMessage.from_dict(d)
        assert restored.cache_id == "cache_456"
        assert restored.size_mb == 256.0

    def test_sync_kv_cache_in_master(self):
        # GAP-7 (#33): sync_kv_cache 实现张量跨节点传输 — 源 export → 目标 import → 返 True。
        # 注入两 agent ASGI (源持有缓存) + 路由 transport, master 编排两端 HTTP。
        async def _test():
            from httpx import ASGITransport, AsyncBaseTransport, AsyncClient, Request, Response

            from fusion_multi_node.agent import AgentConfig, NodeAgent
            from fusion_multi_node.distributed_mlx.kv_cache_sharing import (
                KVCacheEntry as AgentKVEntry,
            )
            from fusion_multi_node.distributed_mlx.kv_cache_sharing import (
                KVShard,
            )
            from fusion_multi_node.distributed_mlx.kv_tensor_transport import SyntheticKVTransport
            from fusion_multi_node.server.agent_server import AgentServer

            cm = ClusterMaster()
            from fusion_multi_node.master.cluster_master import KVCacheEntry

            # 注册 master 级 KV 条目 (源节点 n-src)
            entry = KVCacheEntry(
                cache_id="c1",
                model_name="llama-3b",
                node_id="n-src",
                created_at=time.time(),
                size_mb=0.1,
            )
            await cm.register_kv_cache(entry)

            # 注册两个在线节点 (源 + 目标)
            from fusion_multi_node.master.cluster_master import NodeInfo, NodeStatus

            for nid, port in (("n-src", 33057), ("n-tgt", 33058)):
                ni = NodeInfo(
                    node_id=nid,
                    hostname=nid,
                    ip_address="127.0.0.1",
                    port=port,
                    status=NodeStatus.ONLINE,
                )
                async with cm._nodes_lock:
                    cm.nodes[nid] = ni

            # 两 agent server — 源预存缓存, 共享合成张量后端
            from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager

            kv_src = KVSharingManager(
                cluster_token="test-cluster-token",
                transport=SyntheticKVTransport(tensor_size=128),
            )
            kv_src.store_local(
                AgentKVEntry(
                    cache_id="c1",
                    model_name="llama-3b",
                    prompt_hash="h1",
                    prompt_prefix="Hi",
                    total_tokens=16,
                    total_size_bytes=256,
                    created_at=time.time(),
                    ttl_seconds=3600.0,
                    shards=[
                        KVShard(
                            shard_id="s0",
                            model_name="llama-3b",
                            layer_index=0,
                            node_id="n-src",
                            token_count=16,
                            size_bytes=256,
                            created_at=time.time(),
                            tensor=None,
                            is_compressed=False,
                        )
                    ],
                )
            )
            agent_src = NodeAgent(
                config=AgentConfig(node_id="n-src", cluster_token="test-cluster-token", agent_port=33057),
            )
            server_src = AgentServer(agent=agent_src, kv_manager=kv_src, shared_token="test-cluster-token")
            server_src._rate_limiter._max = 100000
            kv_tgt = KVSharingManager(
                cluster_token="test-cluster-token",
                transport=SyntheticKVTransport(),
            )
            agent_tgt = NodeAgent(
                config=AgentConfig(node_id="n-tgt", cluster_token="test-cluster-token", agent_port=33058),
            )
            server_tgt = AgentServer(agent=agent_tgt, kv_manager=kv_tgt, shared_token="test-cluster-token")
            server_tgt._rate_limiter._max = 100000

            class _Route(AsyncBaseTransport):
                def __init__(self):
                    self._c = {
                        33057: AsyncClient(transport=ASGITransport(app=server_src.app), base_url="http://t"),
                        33058: AsyncClient(transport=ASGITransport(app=server_tgt.app), base_url="http://t"),
                    }

                async def handle_async_request(self, request: Request) -> Response:
                    c = self._c.get(request.url.port)
                    if c is None:
                        return Response(404)
                    return await c.request(
                        request.method,
                        str(request.url),
                        content=request.content,
                        headers=dict(request.headers),
                    )

                async def aclose(self):
                    for c in self._c.values():
                        await c.aclose()

            route = _Route()
            cm._dispatch_http = AsyncClient(transport=route, timeout=30.0)
            cm._dispatch_token = "test-cluster-token"

            # SSRF 守卫拦 127.0.0.1 — 测试作用域放行 (与现有 dispatch E2E 一致)
            # build_safe_url 调 utils.auth.is_safe_peer_host, master 调 cluster_master.is_safe_peer_host — 两处同放行
            from fusion_multi_node.master import cluster_master as _cm_mod
            from fusion_multi_node.utils import auth as _auth_mod

            _orig_safe_cm = _cm_mod.is_safe_peer_host
            _orig_safe_auth = _auth_mod.is_safe_peer_host
            _cm_mod.is_safe_peer_host = lambda host: True
            _auth_mod.is_safe_peer_host = lambda host: True
            try:
                result = await cm.sync_kv_cache("c1", "llama-3b", "n-src", 0.1, target_node_id="n-tgt")
            finally:
                _cm_mod.is_safe_peer_host = _orig_safe_cm
                _auth_mod.is_safe_peer_host = _orig_safe_auth
            await route.aclose()
            await cm._dispatch_http.aclose()
            assert result is True
            # 目标本地查回张量
            got = kv_tgt.lookup_local_by_id("c1")
            assert got is not None and got.shards[0].tensor is not None

        asyncio.run(_test())

    def test_sync_kv_cache_missing_entry(self):
        async def _test():
            cm = ClusterMaster()
            result = await cm.sync_kv_cache("missing", "model", "n1", 50.0)
            assert not result

        asyncio.run(_test())


# ── M1-02 device_model + uma_size_gb in NodeInfo and mDNS ──


class TestDeviceModelUMA:
    def test_nodeinfo_has_device_model(self):
        info = NodeInfo(
            node_id="n1",
            hostname="mac1",
            ip_address="192.168.1.1",
            port=11458,
            device_model="Apple M2 Ultra",
            uma_size_gb=192.0,
        )
        assert info.device_model == "Apple M2 Ultra"
        assert info.uma_size_gb == 192.0

    def test_nodeinfo_defaults(self):
        info = NodeInfo(
            node_id="n2",
            hostname="mac2",
            ip_address="192.168.1.2",
            port=11458,
        )
        assert info.device_model == ""
        assert info.uma_size_gb == 0.0

    def test_register_node_with_device_model(self):
        async def _test():
            cm = ClusterMaster()
            info = NodeInfo(
                node_id="n1",
                hostname="mac1",
                ip_address="192.168.1.1",
                port=11458,
                device_model="Apple M3 Max",
                uma_size_gb=128.0,
                total_memory_gb=128.0,
                available_memory_gb=64.0,
            )
            await cm.register_node(info)
            node = cm.nodes["n1"]
            assert node.device_model == "Apple M3 Max"
            assert node.uma_size_gb == 128.0

        asyncio.run(_test())

    def test_mdns_register_passes_device_model(self):
        from unittest.mock import MagicMock, patch

        from fusion_multi_node.discovery import MDNSDiscovery

        mdns = MDNSDiscovery(node_id="test-master")
        props = {
            "role": "master",
            "device_model": "Apple M2 Ultra",
            "uma_size_gb": "192.0",
        }
        mock_zc = MagicMock()
        mock_si_cls = MagicMock()
        with (
            patch("zeroconf.Zeroconf", return_value=mock_zc),
            patch("zeroconf.ServiceInfo", mock_si_cls),
        ):
            ok = mdns.register(port=11452, properties=props)
            assert ok is True
            assert mdns._registered is True
            # device_model / uma_size_gb 透传到 ServiceInfo.properties
            call_kwargs = mock_si_cls.call_args.kwargs
            passed_props = call_kwargs.get("properties", {})
            assert passed_props.get("device_model") == "Apple M2 Ultra"
            assert passed_props.get("uma_size_gb") == "192.0"
            assert passed_props.get("role") == "master"
            mock_zc.register_service.assert_called_once()
            mdns.unregister()
            mock_zc.close.assert_called_once()

    def test_heartbeat_interval_default_3s(self):
        from fusion_multi_node.config.config import ClusterConfig

        assert ClusterConfig.DEFAULT_CONFIG["cluster"]["heartbeat_interval"] == 3.0


# ── M10 Task Migration and Rebalance ──


class TestTaskMigration:
    def test_migrate_running_task(self):
        async def _test():
            cm = ClusterMaster()
            n1 = NodeInfo(
                node_id="n1",
                hostname="mac1",
                ip_address="10.0.0.1",
                port=11458,
                total_memory_gb=64.0,
                available_memory_gb=32.0,
                status=NodeStatus.ONLINE,
            )
            n2 = NodeInfo(
                node_id="n2",
                hostname="mac2",
                ip_address="10.0.0.2",
                port=11458,
                total_memory_gb=64.0,
                available_memory_gb=48.0,
                status=NodeStatus.ONLINE,
            )
            await cm.register_node(n1)
            await cm.register_node(n2)

            task = ClusterTask(
                task_id="t1",
                name="inference",
                mode=ParallelMode.DATA,
                model_name="llama-3b",
            )
            ok = await cm.assign_task(task)
            assert ok
            assert task.status.value == "running"

            ok = await cm.migrate_task("t1")
            assert ok
            assert task.status.value == "running"
            assert len(task.assigned_nodes) > 0

        asyncio.run(_test())

    def test_migrate_nonexistent_task(self):
        async def _test():
            cm = ClusterMaster()
            ok = await cm.migrate_task("nonexistent")
            assert not ok

        asyncio.run(_test())

    def test_migrate_non_running_task(self):
        async def _test():
            from fusion_multi_node.master.cluster_master import TaskStatus

            cm = ClusterMaster()
            n1 = NodeInfo(
                node_id="n1",
                hostname="mac1",
                ip_address="10.0.0.1",
                port=11458,
                total_memory_gb=64.0,
                available_memory_gb=32.0,
                status=NodeStatus.ONLINE,
            )
            await cm.register_node(n1)
            task = ClusterTask(
                task_id="t1",
                name="pending",
                mode=ParallelMode.DATA,
            )
            task.status = TaskStatus.PENDING
            cm.tasks["t1"] = task
            ok = await cm.migrate_task("t1")
            assert not ok

        asyncio.run(_test())


class TestKVSharingManagerFMP:
    def test_sync_to_cluster(self):
        from fusion_multi_node.distributed_mlx.kv_cache_sharing import (
            KVCacheEntry as KVEntry,
        )
        from fusion_multi_node.distributed_mlx.kv_cache_sharing import (
            KVShard,
            KVSharingManager,
        )

        manager = KVSharingManager(enable_compression=False)
        entry = KVEntry(
            cache_id="c1",
            model_name="llama-3b",
            prompt_hash="abc123",
            prompt_prefix="hello",
            shards=[
                KVShard(
                    shard_id="s1",
                    model_name="llama-3b",
                    layer_index=0,
                    node_id="n1",
                    token_count=100,
                    size_bytes=4096,
                    created_at=time.time(),
                )
            ],
            total_tokens=100,
            total_size_bytes=4096,
            created_at=time.time(),
        )
        manager.store_local(entry)
        sync_msg = manager.sync_to_cluster("c1", "llama-3b", "n1")
        assert sync_msg.cache_id == "c1"
        assert sync_msg.protocol == "fmp"
        assert sync_msg.size_mb > 0

    def test_sync_to_cluster_missing(self):
        from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager

        manager = KVSharingManager(enable_compression=False)
        sync_msg = manager.sync_to_cluster("missing", "model", "n1")
        assert sync_msg.size_mb == 0.0

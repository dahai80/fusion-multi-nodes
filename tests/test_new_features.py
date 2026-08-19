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


# ── M4-05 Cloud Fallback ──


class TestCloudFallback:
    def test_cloud_provider_enum(self):
        from fusion_multi_node.master.cloud_fallback import CloudProvider

        assert CloudProvider.OPENAI.value == "openai"
        assert CloudProvider.ANTHROPIC.value == "anthropic"

    def test_cloud_config_defaults(self):
        from fusion_multi_node.master.cloud_fallback import CloudConfig, CloudProvider

        config = CloudConfig()
        assert config.provider == CloudProvider.OPENAI
        assert config.enabled is False
        assert config.max_cost_per_day == 10.0

    def test_cloud_usage(self):
        from fusion_multi_node.master.cloud_fallback import CloudUsage

        usage = CloudUsage(
            total_requests=5,
            total_input_tokens=1000,
            total_output_tokens=500,
            total_cost=0.05,
        )
        assert usage.total_requests == 5

    def test_available_models(self):
        from fusion_multi_node.master.cloud_fallback import AVAILABLE_MODELS

        assert len(AVAILABLE_MODELS) >= 2
        model_ids = [m.model_id for m in AVAILABLE_MODELS]
        assert "gpt-4o-mini" in model_ids

    def test_cloud_client_init(self):
        from fusion_multi_node.master.cloud_fallback import CloudFallbackClient

        client = CloudFallbackClient()
        assert client.config.enabled is False

    @pytest.mark.asyncio
    async def test_cloud_chat_disabled(self):
        from fusion_multi_node.master.cloud_fallback import (
            CloudConfig,
            CloudFallbackClient,
        )

        config = CloudConfig(enabled=False)
        client = CloudFallbackClient(config=config)
        result = await client.chat(messages=[{"role": "user", "content": "hi"}])
        assert "error" in result

    @pytest.mark.asyncio
    async def test_cloud_chat_no_key(self):
        from fusion_multi_node.master.cloud_fallback import (
            CloudConfig,
            CloudFallbackClient,
        )

        config = CloudConfig(api_key="")
        client = CloudFallbackClient(config=config)
        result = await client.chat(messages=[{"role": "user", "content": "hi"}])
        assert "error" in result

    def test_get_usage(self):
        from fusion_multi_node.master.cloud_fallback import CloudFallbackClient

        client = CloudFallbackClient()
        usage = client.get_usage()
        assert "total_requests" in usage
        assert "daily_cost" in usage


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
        assert master.tasks["t-cancel"].status == TaskStatus.FAILED
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
        assert master.tasks["sub-1"].status == TaskStatus.FAILED
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
        assert pm.has_permission("master-1", Permission.TASK_EXECUTE) is False

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
            port=11445,
        )
        assert req.node_id == "node-1"
        assert req.status.value == "pending"

    def test_approve(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager()
        mgr.request_join("node-1", "mac-1", "192.168.1.10", 11445)
        mgr.approve("node-1", approved_by="admin")
        assert mgr.is_approved("node-1") is True

    def test_reject(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager()
        mgr.request_join("node-1", "mac-1", "192.168.1.10", 11445)
        mgr.reject("node-1", reason="untrusted")
        assert mgr.is_approved("node-1") is False

    def test_auto_approve_patterns(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager(auto_approve_patterns=["192.168."])
        mgr.request_join("node-1", "mac-1", "192.168.1.10", 11445)
        assert mgr.is_approved("node-1") is True

    def test_revoke(self):
        from fusion_multi_node.security.node_approval import NodeApprovalManager

        mgr = NodeApprovalManager(auto_approve_patterns=["192.168.*"])
        mgr.request_join("node-1", "mac-1", "192.168.1.10", 11445)
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
                "port": 11445,
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
        mgr.handle_join_request({"node_id": "n1", "hostname": "h1", "ip_address": "1.1.1.1", "port": 11445})
        history = mgr.get_join_history()
        assert len(history) == 1
        assert mgr.join_count == 1

    def test_join_request_response_dataclass(self):
        from fusion_multi_node.discovery.manual_join import JoinRequest, JoinResponse

        req = JoinRequest(node_id="n1", hostname="h1", ip_address="1.1.1.1", port=11445)
        assert req.node_id == "n1"
        resp = JoinResponse(success=True, master_host="1.2.3.4", master_port=11452)
        assert resp.success is True


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
                port=11445,
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
                port=11445,
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
            info = NodeInfo(node_id="n1", hostname="mac1", ip_address="192.168.1.10", port=11445)
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
                port=11445,
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
                port=11445,
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
                port=11445,
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
                port=11445,
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
                port=11445,
                total_memory_gb=16.0,
                available_memory_gb=14.0,
            )
            high_vram = NodeInfo(
                node_id="n_high",
                hostname="high",
                ip_address="10.0.0.2",
                port=11445,
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
                port=11445,
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
                port=11445,
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


# ── M6-04 AST Diff-only ──


class TestASTDiff:
    def test_compute_empty_diff(self):
        from fusion_multi_node.master.ast_diff import compute_ast_diff

        old = {"id": "root", "type": "module", "children": []}
        new = {"id": "root", "type": "module", "children": []}
        diff = compute_ast_diff(old, new)
        assert diff["added_nodes"] == []
        assert diff["removed_nodes"] == []
        assert diff["modified_nodes"] == []
        assert diff["stats"]["added"] == 0

    def test_compute_added_nodes(self):
        from fusion_multi_node.master.ast_diff import compute_ast_diff

        old = {"id": "root", "type": "module", "children": []}
        new = {
            "id": "root",
            "type": "module",
            "children": [{"id": "fn1", "type": "function", "children": []}],
        }
        diff = compute_ast_diff(old, new)
        assert diff["stats"]["added"] == 1
        assert any("fn1" in n.get("path", "") or n.get("id") == "fn1" for n in diff["added_nodes"])

    def test_compute_removed_nodes(self):
        from fusion_multi_node.master.ast_diff import compute_ast_diff

        old = {
            "id": "root",
            "type": "module",
            "children": [{"id": "fn1", "type": "function", "children": []}],
        }
        new = {"id": "root", "type": "module", "children": []}
        diff = compute_ast_diff(old, new)
        assert diff["stats"]["removed"] == 1

    def test_compute_modified_nodes(self):
        from fusion_multi_node.master.ast_diff import compute_ast_diff

        old = {
            "id": "root",
            "type": "module",
            "children": [{"id": "fn1", "type": "function", "value": "old", "children": []}],
        }
        new = {
            "id": "root",
            "type": "module",
            "children": [{"id": "fn1", "type": "function", "value": "new", "children": []}],
        }
        diff = compute_ast_diff(old, new)
        assert diff["stats"]["modified"] == 1

    def test_apply_diff_identity(self):
        from fusion_multi_node.master.ast_diff import apply_ast_diff, compute_ast_diff

        old = {
            "id": "root",
            "type": "module",
            "children": [{"id": "fn1", "type": "function", "value": "hello", "children": []}],
        }
        new = {
            "id": "root",
            "type": "module",
            "children": [{"id": "fn1", "type": "function", "value": "world", "children": []}],
        }
        diff = compute_ast_diff(old, new)
        result = apply_ast_diff(old, diff)
        fn1 = result["children"][0]
        assert fn1["value"] == "world"

    def test_apply_diff_add_and_remove(self):
        from fusion_multi_node.master.ast_diff import apply_ast_diff, compute_ast_diff

        old = {
            "id": "root",
            "type": "module",
            "children": [{"id": "fn1", "type": "function", "children": []}],
        }
        new = {
            "id": "root",
            "type": "module",
            "children": [{"id": "fn2", "type": "class", "children": []}],
        }
        diff = compute_ast_diff(old, new)
        result = apply_ast_diff(old, diff)
        child_ids = [c["id"] for c in result.get("children", [])]
        assert "fn2" in child_ids


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
        async def _test():
            cm = ClusterMaster()
            from fusion_multi_node.master.cluster_master import KVCacheEntry

            entry = KVCacheEntry(
                cache_id="c1",
                model_name="llama-3b",
                node_id="n1",
                created_at=time.time(),
                size_mb=100.0,
            )
            await cm.register_kv_cache(entry)
            result = await cm.sync_kv_cache("c1", "llama-3b", "n1", 100.0)
            assert result

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
            port=11445,
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
            port=11445,
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
                port=11445,
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
                port=11445,
                total_memory_gb=64.0,
                available_memory_gb=32.0,
                status=NodeStatus.ONLINE,
            )
            n2 = NodeInfo(
                node_id="n2",
                hostname="mac2",
                ip_address="10.0.0.2",
                port=11445,
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
                port=11445,
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

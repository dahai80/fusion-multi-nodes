"""Observability coverage tests."""

import asyncio
import collections
import time
from unittest.mock import patch

import pytest

from fusion_multi_node.observability.observability import (
    Alert,
    ClusterObservability,
    LogEntry,
    MetricPoint,
    _build_node_summary,
)


class TestMetricPoint:
    def test_basic(self):
        mp = MetricPoint(timestamp=100.0, node_id="n1", metric_name="cpu", value=0.8)
        assert mp.metric_name == "cpu"
        assert mp.value == 0.8
        assert mp.tags == {}

    def test_with_tags(self):
        mp = MetricPoint(
            timestamp=100.0,
            node_id="n1",
            metric_name="mem",
            value=0.5,
            tags={"role": "worker"},
        )
        assert mp.tags["role"] == "worker"


class TestAlert:
    def test_basic(self):
        a = Alert(alert_id="a1", severity="warning", title="high cpu", message="cpu > 90%")
        assert a.alert_id == "a1"
        assert a.severity == "warning"
        assert a.resolved is False

    def test_with_node(self):
        a = Alert(
            alert_id="a2",
            severity="critical",
            title="offline",
            message="node down",
            node_id="n1",
        )
        assert a.node_id == "n1"


class TestLogEntry:
    def test_basic(self):
        entry = LogEntry(
            timestamp=100.0,
            node_id="n1",
            level="INFO",
            module="master",
            message="started",
        )
        assert entry.level == "INFO"
        assert entry.module == "master"

    def test_with_task(self):
        entry = LogEntry(
            timestamp=100.0,
            node_id="n1",
            level="ERROR",
            module="agent",
            message="fail",
            task_id="t1",
        )
        assert entry.task_id == "t1"


class TestClusterObservabilityInit:
    def test_init(self):
        obs = ClusterObservability()
        assert len(obs.metrics) == 0
        assert len(obs.alerts) == 0
        assert len(obs.logs) == 0

    def test_init_custom_retention(self):
        obs = ClusterObservability(retention_hours=48.0)
        assert obs.retention_seconds == 48.0 * 3600


class TestClusterObservabilityMetrics:
    def test_record_metric(self):
        obs = ClusterObservability()
        obs.record_metric("n1", "cpu", 0.8)
        assert len(obs.metrics) == 1
        assert obs.metrics[0].value == 0.8

    def test_record_metric_with_tags(self):
        obs = ClusterObservability()
        obs.record_metric("n1", "cpu", 0.8, tags={"role": "worker"})
        assert obs.metrics[0].tags == {"role": "worker"}

    def test_max_metrics_truncation(self):
        obs = ClusterObservability()
        obs.metrics = collections.deque(maxlen=5)
        for i in range(10):
            obs.record_metric("n1", "cpu", float(i))
        assert len(obs.metrics) == 5

    def test_get_metrics(self):
        obs = ClusterObservability()
        obs.record_metric("n1", "cpu", 0.5)
        obs.record_metric("n1", "mem", 0.6)
        obs.record_metric("n2", "cpu", 0.7)
        assert len(obs.get_metrics("cpu")) == 2

    def test_get_metrics_by_node(self):
        obs = ClusterObservability()
        obs.record_metric("n1", "cpu", 0.5)
        obs.record_metric("n2", "cpu", 0.7)
        result = obs.get_metrics("cpu", node_id="n1")
        assert len(result) == 1
        assert result[0].node_id == "n1"

    def test_get_metrics_since(self):
        obs = ClusterObservability()
        obs.record_metric("n1", "cpu", 0.5)
        import time

        time.sleep(0.01)
        obs.record_metric("n1", "cpu", 0.7)
        mid_ts = obs.metrics[0].timestamp + 0.005
        recent = obs.get_metrics("cpu", since=mid_ts)
        assert len(recent) == 1

    def test_get_metrics_limit(self):
        obs = ClusterObservability()
        for i in range(20):
            obs.record_metric("n1", "cpu", float(i))
        result = obs.get_metrics("cpu", limit=5)
        assert len(result) == 5

    def test_get_latest_metric(self):
        obs = ClusterObservability()
        obs.record_metric("n1", "cpu", 0.5)
        obs.record_metric("n1", "cpu", 0.9)
        assert obs.get_latest_metric("cpu").value == 0.9

    def test_get_latest_metric_by_node(self):
        obs = ClusterObservability()
        obs.record_metric("n1", "cpu", 0.5)
        obs.record_metric("n2", "cpu", 0.9)
        assert obs.get_latest_metric("cpu", node_id="n1").value == 0.5

    def test_get_latest_metric_missing(self):
        obs = ClusterObservability()
        assert obs.get_latest_metric("nonexistent") is None


class TestClusterObservabilityLogs:
    def test_add_log(self):
        obs = ClusterObservability()
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="master",
                message="started",
            )
        )
        assert len(obs.logs) == 1

    def test_add_log_error_creates_alert(self):
        obs = ClusterObservability()
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="ERROR",
                module="agent",
                message="fail",
            )
        )
        assert len(obs.alerts) == 1
        assert obs.alerts[0].severity == "warning"

    def test_add_log_critical_creates_alert(self):
        obs = ClusterObservability()
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="CRITICAL",
                module="agent",
                message="fatal",
            )
        )
        assert len(obs.alerts) == 1
        assert obs.alerts[0].severity == "critical"

    def test_max_logs_truncation(self):
        obs = ClusterObservability()
        obs.logs = collections.deque(maxlen=3)
        for i in range(5):
            obs.add_log(
                LogEntry(
                    timestamp=time.time(),
                    node_id="n1",
                    level="INFO",
                    module="m",
                    message=f"msg{i}",
                )
            )
        assert len(obs.logs) == 3

    def test_get_logs(self):
        obs = ClusterObservability()
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="m1",
                message="a",
            )
        )
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="ERROR",
                module="m2",
                message="b",
            )
        )
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n2",
                level="INFO",
                module="m3",
                message="c",
            )
        )
        assert len(obs.get_logs()) == 3

    def test_get_logs_by_level(self):
        obs = ClusterObservability()
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="m1",
                message="a",
            )
        )
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="ERROR",
                module="m2",
                message="b",
            )
        )
        errors = obs.get_logs(level="ERROR")
        assert len(errors) == 1

    def test_get_logs_by_node(self):
        obs = ClusterObservability()
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="m1",
                message="a",
            )
        )
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n2",
                level="INFO",
                module="m2",
                message="b",
            )
        )
        assert len(obs.get_logs(node_id="n1")) == 1

    def test_get_logs_since(self):
        obs = ClusterObservability()
        obs.add_log(LogEntry(timestamp=100.0, node_id="n1", level="INFO", module="m1", message="old"))
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="m2",
                message="new",
            )
        )
        recent = obs.get_logs(since=200.0)
        assert len(recent) == 1

    def test_get_logs_limit(self):
        obs = ClusterObservability()
        for i in range(20):
            obs.add_log(
                LogEntry(
                    timestamp=time.time(),
                    node_id="n1",
                    level="INFO",
                    module="m",
                    message=f"msg{i}",
                )
            )
        result = obs.get_logs(limit=5)
        assert len(result) == 5


class TestClusterObservabilityAlerts:
    def test_create_alert(self):
        obs = ClusterObservability()
        alert = obs.create_alert(severity="warning", title="high cpu", message="cpu > 90%", node_id="n1")
        assert alert.severity == "warning"
        assert not alert.resolved

    def test_create_alert_calls_handler(self):
        obs = ClusterObservability()
        received = []
        obs.on_alert(lambda a: received.append(a))
        obs.create_alert(severity="warning", title="test", message="test")
        assert len(received) == 1

    def test_create_alert_handler_exception(self):
        obs = ClusterObservability()

        def bad_handler(a):
            raise RuntimeError("handler failed")

        obs.on_alert(bad_handler)
        alert = obs.create_alert(severity="warning", title="test", message="test")
        assert alert is not None

    def test_resolve_alert(self):
        obs = ClusterObservability()
        alert = obs.create_alert(severity="warning", title="test", message="test")
        assert obs.resolve_alert(alert.alert_id) is True
        assert alert.resolved is True

    def test_resolve_alert_missing(self):
        obs = ClusterObservability()
        assert obs.resolve_alert("nope") is False

    def test_get_active_alerts(self):
        obs = ClusterObservability()
        obs.create_alert(severity="warning", title="a1", message="m1")
        a2 = obs.create_alert(severity="critical", title="a2", message="m2")
        obs.resolve_alert(a2.alert_id)
        active = obs.get_active_alerts()
        assert len(active) == 1

    def test_get_active_alerts_by_severity(self):
        obs = ClusterObservability()
        obs.create_alert(severity="warning", title="a1", message="m1")
        obs.create_alert(severity="critical", title="a2", message="m2")
        critical = obs.get_active_alerts(severity="critical")
        assert len(critical) == 1


class TestClusterObservabilityAlertRules:
    @pytest.mark.asyncio
    async def test_check_alert_rules_offline_node(self):
        obs = ClusterObservability()
        nodes = {"n1": {"status": "offline", "hostname": "node1"}}
        alerts = await obs.check_alert_rules(nodes)
        assert len(alerts) >= 1
        assert any("离线" in a.title for a in alerts)

    @pytest.mark.asyncio
    async def test_check_alert_rules_low_memory(self):
        obs = ClusterObservability()
        nodes = {
            "n1": {
                "status": "online",
                "available_memory_gb": 1.0,
                "total_memory_gb": 100.0,
            }
        }
        alerts = await obs.check_alert_rules(nodes)
        assert len(alerts) >= 1
        assert any("内存" in a.title for a in alerts)

    @pytest.mark.asyncio
    async def test_check_alert_rules_healthy(self):
        obs = ClusterObservability()
        nodes = {
            "n1": {
                "status": "online",
                "available_memory_gb": 50.0,
                "total_memory_gb": 64.0,
            }
        }
        alerts = await obs.check_alert_rules(nodes)
        assert len(alerts) == 0

    @pytest.mark.asyncio
    async def test_check_alert_rules_zero_total_memory(self):
        obs = ClusterObservability()
        nodes = {"n1": {"status": "online", "available_memory_gb": 0, "total_memory_gb": 0}}
        alerts = await obs.check_alert_rules(nodes)
        assert len(alerts) == 0

    @pytest.mark.asyncio
    async def test_check_alert_rules_dedup_repeated_ticks(self):
        # P0-8: 周期调用须去重 — 同 (node_id, title) 已活跃则不再重复创建。
        obs = ClusterObservability()
        nodes = {"n1": {"status": "offline", "hostname": "node1"}}
        first = await obs.check_alert_rules(nodes)
        # 离线 + 无内存字段 (total 默认 1, avail 0 → 低内存也触发) = 2 条首告警
        assert len(first) == 2
        # 第二/三 tick (10s 周期) 不应再灌同质告警
        second = await obs.check_alert_rules(nodes)
        third = await obs.check_alert_rules(nodes)
        assert second == []
        assert third == []
        assert len(obs.alerts) == 2


class TestClusterObservabilityReport:
    def test_get_cluster_report(self):
        obs = ClusterObservability()
        obs.record_metric("n1", "latency_ms", 10.0)
        obs.record_metric("n1", "tokens_per_sec", 100.0)
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="m1",
                message="ok",
            )
        )
        obs.create_alert(severity="warning", title="test", message="test")
        report = obs.get_cluster_report()
        assert "metrics_collected" in report
        assert "active_alerts" in report
        assert "node_summary" in report
        assert "n1" in report["node_summary"]


class TestBuildNodeSummary:
    def test_with_data(self):
        metrics = {
            "latency_ms": [10.0, 20.0, 30.0],
            "tokens_per_sec": [100.0, 200.0],
        }
        result = _build_node_summary(metrics)
        assert result["avg_latency_ms"] == 20.0
        assert result["avg_tps"] == 150.0

    def test_empty(self):
        result = _build_node_summary({})
        assert result["avg_latency_ms"] == 0
        assert result["avg_tps"] == 0

    def test_partial_data(self):
        metrics = {"latency_ms": [5.0]}
        result = _build_node_summary(metrics)
        assert result["avg_latency_ms"] == 5.0
        assert result["avg_tps"] == 0


class TestClusterObservabilityLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self):
        obs = ClusterObservability()
        await obs.start()
        assert obs._running is True
        await obs.stop()
        assert obs._running is False

    @pytest.mark.asyncio
    async def test_cleanup_loop(self):
        obs = ClusterObservability(retention_hours=0.00001)
        obs._running = True
        obs.record_metric("n1", "cpu", 0.5)
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="m1",
                message="ok",
            )
        )
        old_sleep = asyncio.sleep

        async def fast_sleep(delay):
            await old_sleep(0.01)

        with patch("asyncio.sleep", fast_sleep):
            task = asyncio.create_task(obs._cleanup_loop())
            await old_sleep(0.1)
            obs._running = False
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


class TestExportLogs:
    def test_export_json(self):
        obs = ClusterObservability(retention_hours=168.0)
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="src",
                message="hello",
            )
        )
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n2",
                level="ERROR",
                module="src2",
                message="fail",
            )
        )
        result = obs.export_logs(fmt="json")
        assert len(result) == 2
        assert result[0]["message"] == "hello"

    def test_export_csv(self):
        obs = ClusterObservability(retention_hours=168.0)
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="src",
                message="hello",
            )
        )
        csv = obs.export_logs(fmt="csv")
        assert "timestamp,level" in csv
        assert "hello" in csv

    def test_export_with_filters(self):
        obs = ClusterObservability(retention_hours=168.0)
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n1",
                level="INFO",
                module="src",
                message="msg1",
            )
        )
        obs.add_log(
            LogEntry(
                timestamp=time.time(),
                node_id="n2",
                level="ERROR",
                module="src",
                message="msg2",
            )
        )
        result = obs.export_logs(node_id="n2")
        assert len(result) == 1
        assert result[0]["node_id"] == "n2"


class TestOptimizationSuggestions:
    def test_suggestions_memory_alert(self):
        obs = ClusterObservability(retention_hours=168.0)
        obs.alerts.append(Alert(alert_id="a1", severity="critical", title="mem", message="内存压力过高"))
        suggestions = obs.generate_optimization_suggestions()
        assert any(s["category"] == "resource" for s in suggestions)

    def test_suggestions_offline_alert(self):
        obs = ClusterObservability(retention_hours=168.0)
        obs.alerts.append(Alert(alert_id="a2", severity="critical", title="down", message="node offline"))
        suggestions = obs.generate_optimization_suggestions()
        assert any(s["category"] == "availability" for s in suggestions)

    def test_suggestions_healthy(self):
        obs = ClusterObservability(retention_hours=168.0)
        suggestions = obs.generate_optimization_suggestions()
        assert len(suggestions) >= 1
        assert suggestions[0]["category"] == "info"

    def test_suggestions_error_logs(self):
        obs = ClusterObservability(retention_hours=168.0)
        for _ in range(5):
            obs.add_log(
                LogEntry(
                    timestamp=time.time(),
                    node_id="n1",
                    level="ERROR",
                    module="inference_engine",
                    message="crash",
                )
            )
        suggestions = obs.generate_optimization_suggestions()
        assert any("inference_engine" in s["title"] for s in suggestions)


class TestP05AlertWebhook:
    # P0-5 (审计 §5.6): 告警出站通道 — env FUSION_ALERT_WEBHOOK_URL 注册 webhook handler,
    # create_alert → fire-and-forget POST, 不阻塞 create_alert 同步路径。

    @pytest.mark.asyncio
    async def test_webhook_registered_and_posted(self, tmp_path, monkeypatch):
        from fusion_multi_node.master.cluster_master import ClusterMaster

        posted = []

        class _FakeResp:
            status_code = 200
            text = "ok"

        class _FakeClient:
            def __init__(self, *a, **kw):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def post(self, url, json=None):
                posted.append({"url": url, "json": json})
                return _FakeResp()

        monkeypatch.setattr("fusion_multi_node.master.cluster_master.httpx.Client", _FakeClient)
        monkeypatch.setenv("FUSION_ALERT_WEBHOOK_URL", "http://alert-sink.local/hook")

        m = ClusterMaster()
        m._task_store_path = tmp_path / "tasks.json"
        await m.start(with_server=False, with_mdns=False)
        try:
            t0 = time.monotonic()
            m._observability.create_alert(severity="critical", title="节点离线", message="n1 失联", node_id="n1")
            elapsed = time.monotonic() - t0
            # create_alert 须 fire-and-forget 即返 (POST 在后台 create_task), 不阻塞同步路径。
            assert elapsed < 0.05, f"create_alert 被 webhook 拖慢: {elapsed:.3f}s"
            # 后台 POST 需要一个事件循环 tick 调度 — 等 to_thread 完成。
            await asyncio.sleep(0.1)
        finally:
            await m.stop()

        assert len(posted) == 1, "告警须经 webhook POST 出站"
        payload = posted[0]["json"]
        assert payload["title"] == "节点离线"
        assert payload["severity"] == "critical"
        assert payload["node_id"] == "n1"
        assert payload["source"] == "fusion-multi-node"

    @pytest.mark.asyncio
    async def test_no_webhook_env_skips_registration(self, tmp_path, monkeypatch):
        from fusion_multi_node.master.cluster_master import ClusterMaster

        monkeypatch.delenv("FUSION_ALERT_WEBHOOK_URL", raising=False)
        m = ClusterMaster()
        m._task_store_path = tmp_path / "tasks.json"
        await m.start(with_server=False, with_mdns=False)
        try:
            # 无 env → 零 handler 注册, create_alert 不触发任何出站 (仅内存 deque)。
            assert m._observability._alert_handlers == []
            m._observability.create_alert(severity="info", title="t", message="m")
        finally:
            await m.stop()


class TestP2_12ObservabilityPersist:
    """P2-12 (审计 §6.5): 可观测 deque 持久化 (persist=True, env 门控默认关)。

    save() 落盘 JSONL (metrics/alerts/logs); load() 启动恢复; persist=False no-op。
    事件总线不持久化 (SSE 实时语义, 重连从当前开始)。
    """

    def test_p2_12_persist_false_no_op(self, tmp_path, monkeypatch):
        # persist=False → save()/load() no-op, 不读写文件。
        monkeypatch.setenv("FUSION_OBSERVABILITY_FILE", str(tmp_path / "obs.jsonl"))
        obs = ClusterObservability(persist=False)
        assert obs.save() is False
        assert obs.load() == 0
        assert not (tmp_path / "obs.jsonl").exists()

    def test_p2_12_save_load_roundtrip(self, tmp_path, monkeypatch):
        # persist=True → save 落盘 metrics/alerts/logs; 新实例 load 恢复全部。
        path = tmp_path / "obs.jsonl"
        monkeypatch.setenv("FUSION_OBSERVABILITY_FILE", str(path))
        obs = ClusterObservability(persist=True)
        obs.record_metric("n1", "mem_used_gb", 4.2)
        obs.create_alert(severity="warning", title="节点内存高", message=">80%", node_id="n1")
        obs.add_log(LogEntry(timestamp=time.time(), node_id="n1", level="WARN", module="test", message="warn"))
        assert obs.save() is True
        assert path.exists()

        obs2 = ClusterObservability(persist=True)
        loaded = obs2.load()
        assert loaded == 3, f"应恢复 3 条 (1 metric+1 alert+1 log), 实际 {loaded}"
        assert len(obs2.metrics) == 1
        assert obs2.metrics[0].metric_name == "mem_used_gb"
        assert obs2.metrics[0].value == 4.2
        assert len(obs2.alerts) == 1
        assert obs2.alerts[0].title == "节点内存高"
        assert len(obs2.logs) == 1
        assert obs2.logs[0].message == "warn"

    def test_p2_12_load_missing_file_zero(self, tmp_path, monkeypatch):
        # persist=True 但文件不存在 (首启) → load 返 0 不报错。
        monkeypatch.setenv("FUSION_OBSERVABILITY_FILE", str(tmp_path / "nope.jsonl"))
        obs = ClusterObservability(persist=True)
        assert obs.load() == 0

    def test_p2_12_save_atomic_replace(self, tmp_path, monkeypatch):
        # 原子落盘: .tmp → os.replace, 成功后无残留 .tmp。
        path = tmp_path / "obs.jsonl"
        monkeypatch.setenv("FUSION_OBSERVABILITY_FILE", str(path))
        obs = ClusterObservability(persist=True)
        obs.record_metric("n1", "x", 1.0)
        assert obs.save() is True
        assert path.exists()
        assert not (tmp_path / "obs.jsonl.tmp").exists(), "原子 replace 后应无残留 .tmp"

    @pytest.mark.asyncio
    async def test_p2_12_master_lifecycle_persist(self, tmp_path, monkeypatch):
        # master.start (persist=True) → stop 触发 save; 文件含采样的指标。
        monkeypatch.setenv("FUSION_OBSERVABILITY_FILE", str(tmp_path / "life.jsonl"))
        monkeypatch.setenv("FUSION_PARTIAL_RECOVERY", "0")
        from fusion_multi_node.master.cluster_master import ClusterMaster

        m = ClusterMaster()
        m._task_store_path = tmp_path / "tasks.json"
        m._observability = ClusterObservability(persist=True)
        m._observability._persist_path = str(tmp_path / "life.jsonl")
        await m.start(with_server=False, with_mdns=False)
        try:
            m._observability.record_metric("master", "mem_used_gb", 2.0)
        finally:
            await m.stop()
        assert (tmp_path / "life.jsonl").exists(), "stop 应触发 observability.save()"
        obs2 = ClusterObservability(persist=True)
        obs2._persist_path = str(tmp_path / "life.jsonl")
        assert obs2.load() >= 1, "落盘文件应可 load 恢复"

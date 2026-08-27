"""H3 Master 任务持久化 + 启动恢复测试。

验证崩溃自愈: RUNNING/PENDING 任务落盘 → 新 Master 启动恢复 → RUNNING 置 PENDING 重派。
终态任务不落盘。无文件时启动恢复 0 任务。
No external API callers — internal unit tests.
"""

import asyncio
import json
import time
from pathlib import Path

import pytest

from fusion_multi_node.master.cluster_master import (
    ClusterMaster,
    ClusterTask,
    NodeInfo,
    ParallelMode,
    TaskStatus,
)
from fusion_multi_node.observability.observability import ClusterObservability


def _master_with_store(tmp_path: Path) -> ClusterMaster:
    m = ClusterMaster()
    m._task_store_path = tmp_path / "tasks.json"
    return m


def _task(tid: str, status: TaskStatus = TaskStatus.PENDING) -> ClusterTask:
    return ClusterTask(
        task_id=tid,
        name=f"task-{tid}",
        mode=ParallelMode.PIPELINE,
        model_name="test-model",
        status=status,
        created_at=time.time(),
    )


class TestPersistRestore:
    @pytest.mark.asyncio
    async def test_empty_start_no_file(self, tmp_path):
        m = _master_with_store(tmp_path)
        restored = await m._restore_tasks()
        assert restored == 0
        assert len(m.tasks) == 0

    @pytest.mark.asyncio
    async def test_running_restored_as_pending(self, tmp_path):
        m = _master_with_store(tmp_path)
        await m.register_node(NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458))
        t = _task("t-run", TaskStatus.RUNNING)
        t.assigned_nodes = ["n1"]
        async with m._tasks_lock:
            m.tasks["t-run"] = t
            snap = m._persist_tasks_locked()
        # P1-11: 落盘在锁释放后。
        m._write_task_store(snap)
        assert (tmp_path / "tasks.json").exists()

        m2 = _master_with_store(tmp_path)
        restored = await m2._restore_tasks()
        assert restored == 1
        assert m2.tasks["t-run"].status == TaskStatus.PENDING
        assert m2.tasks["t-run"].task_id == "t-run"
        assert m2.tasks["t-run"].model_name == "test-model"

    @pytest.mark.asyncio
    async def test_migrated_restored_as_pending(self, tmp_path):
        m = _master_with_store(tmp_path)
        t = _task("t-mig", TaskStatus.MIGRATED)
        async with m._tasks_lock:
            m.tasks["t-mig"] = t
            snap = m._persist_tasks_locked()
        m._write_task_store(snap)
        m2 = _master_with_store(tmp_path)
        await m2._restore_tasks()
        assert m2.tasks["t-mig"].status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_pending_restored_as_pending(self, tmp_path):
        m = _master_with_store(tmp_path)
        t = _task("t-pend")
        async with m._tasks_lock:
            m.tasks["t-pend"] = t
            snap = m._persist_tasks_locked()
        m._write_task_store(snap)
        m2 = _master_with_store(tmp_path)
        await m2._restore_tasks()
        assert m2.tasks["t-pend"].status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_terminal_not_persisted(self, tmp_path):
        m = _master_with_store(tmp_path)
        for st in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
            async with m._tasks_lock:
                m.tasks[f"t-{st.value}"] = _task(f"t-{st.value}", st)
                snap = m._persist_tasks_locked()
            m._write_task_store(snap)
        m2 = _master_with_store(tmp_path)
        restored = await m2._restore_tasks()
        assert restored == 0
        assert len(m2.tasks) == 0

    @pytest.mark.asyncio
    async def test_mixed_only_non_terminal_restored(self, tmp_path):
        m = _master_with_store(tmp_path)
        async with m._tasks_lock:
            m.tasks["t-done"] = _task("t-done", TaskStatus.COMPLETED)
            m.tasks["t-fail"] = _task("t-fail", TaskStatus.FAILED)
            m.tasks["t-run"] = _task("t-run", TaskStatus.RUNNING)
            m.tasks["t-pend"] = _task("t-pend", TaskStatus.PENDING)
            snap = m._persist_tasks_locked()
        m._write_task_store(snap)
        m2 = _master_with_store(tmp_path)
        restored = await m2._restore_tasks()
        assert restored == 2
        assert set(m2.tasks.keys()) == {"t-run", "t-pend"}
        assert m2.tasks["t-run"].status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_atomic_write_no_tmp_leftover(self, tmp_path):
        m = _master_with_store(tmp_path)
        async with m._tasks_lock:
            m.tasks["t1"] = _task("t1")
            snap = m._persist_tasks_locked()
        m._write_task_store(snap)
        assert (tmp_path / "tasks.json").exists()
        assert not (tmp_path / "tasks.json.tmp").exists()

    @pytest.mark.asyncio
    async def test_corrupt_file_recovers_zero(self, tmp_path):
        (tmp_path / "tasks.json").write_text("not valid json {{{")
        m = _master_with_store(tmp_path)
        restored = await m._restore_tasks()
        assert restored == 0
        assert len(m.tasks) == 0

    @pytest.mark.asyncio
    async def test_persist_file_shape(self, tmp_path):
        m = _master_with_store(tmp_path)
        async with m._tasks_lock:
            m.tasks["t1"] = _task("t1")
            snap = m._persist_tasks_locked()
        m._write_task_store(snap)
        data = json.loads((tmp_path / "tasks.json").read_text())
        assert "tasks" in data
        assert "saved_at" in data
        assert data["tasks"][0]["task_id"] == "t1"
        assert data["tasks"][0]["status"] == "pending"
        assert data["tasks"][0]["mode"] == "pipeline"
        assert "spec" not in data["tasks"][0]

    @pytest.mark.asyncio
    async def test_start_stop_persists_and_restores(self, tmp_path):
        m = _master_with_store(tmp_path)
        await m.start(with_server=False, with_mdns=False)
        async with m._tasks_lock:
            m.tasks["t-survive"] = _task("t-survive", TaskStatus.RUNNING)
            snap = m._persist_tasks_locked()
        m._write_task_store(snap)
        await m.stop()
        assert (tmp_path / "tasks.json").exists()

        m2 = _master_with_store(tmp_path)
        restored = await m2._restore_tasks()
        assert restored == 1
        assert m2.tasks["t-survive"].status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_fsync_runs_outside_tasks_lock(self, tmp_path):
        # P1-11 (审计 §4.2): 落盘 (含 os.fsync) 须在 _tasks_lock 释放后执行, 不持锁 fsync。
        m = _master_with_store(tmp_path)
        async with m._tasks_lock:
            m.tasks["t1"] = _task("t1", TaskStatus.RUNNING)
        lock_state_at_write = {}

        real_write = m._write_task_store

        def spy_write(pending):
            lock_state_at_write["locked"] = m._tasks_lock.locked()
            return real_write(pending)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(m, "_write_task_store", spy_write)
            await m._persist_tasks()
        assert lock_state_at_write["locked"] is False, "落盘须在 _tasks_lock 释放后 (不持锁 fsync)"
        assert (tmp_path / "tasks.json").exists()

    @pytest.mark.asyncio
    async def test_fsync_does_not_block_event_loop(self, tmp_path):
        # P0-4 (审计): _write_task_store 含 os.fsync 同步阻塞 — 移 asyncio.to_thread 后
        # 落盘期间事件循环须可推进 (并行 asyncio.sleep 计时器不受慢盘拖累)。
        m = _master_with_store(tmp_path)
        async with m._tasks_lock:
            m.tasks["t1"] = _task("t1", TaskStatus.RUNNING)

        real_write = m._write_task_store

        def slow_write(pending):
            # 模拟慢盘 fsync: 同步阻塞 80ms (若在事件循环线程, 会拖垮并发协程)
            import time as _t

            _t.sleep(0.08)
            return real_write(pending)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(m, "_write_task_store", slow_write)
            t0 = time.monotonic()
            persist_task = asyncio.create_task(m._persist_tasks())
            # 并行计时器: 40ms sleep — 若 fsync 阻塞事件循环, 此 sleep 会被 80ms 慢盘推迟到 ~80ms+ 才完成
            timer_task = asyncio.create_task(asyncio.sleep(0.04))
            await timer_task
            timer_elapsed = time.monotonic() - t0
            await persist_task
        # 计时器应在 ~40ms 完成 (容忍调度抖动), 不被 80ms 慢盘拖到 ~80ms — 证明 fsync 移出事件循环。
        assert timer_elapsed < 0.07, f"事件循环被 fsync 阻塞: 计时器 {timer_elapsed:.3f}s 应 < 0.07s"
        assert (tmp_path / "tasks.json").exists()

    @pytest.mark.asyncio
    async def test_persist_failure_emits_alert_and_metric(self, tmp_path):
        # P1-15 (审计 §5.6): _write_task_store 落盘失败须发 critical 告警 + 失败指标, 不静默吞。
        m = _master_with_store(tmp_path)
        m._observability = ClusterObservability()
        # 指向不可写路径触发 open() 失败 (父目录是文件 → mkdir 或 open 抛)
        bad_path = tmp_path / "blocker"
        bad_path.write_text("x")
        m._task_store_path = bad_path / "tasks.json"
        m._write_task_store([{"task_id": "t1", "status": "running"}])
        alerts = m._observability.get_active_alerts()
        assert any(a.title == "H3 任务持久化失败" and a.severity == "critical" for a in alerts)
        latest = m._observability.get_latest_metric("task_persist_failed", "cluster")
        assert latest is not None and latest.value == 1.0

    @pytest.mark.asyncio
    async def test_retry_count_survives_persist_restore(self, tmp_path):
        # P2-26 (审计 §5.7): _retry_count 是动态属性, asdict 不序列化 → 崩溃重启归零
        # → 允许额外重试超 _max_retry_attempts。显式序列化+恢复须闭环。
        m = _master_with_store(tmp_path)
        t = _task("t-retry", TaskStatus.RUNNING)
        t._retry_count = 2
        async with m._tasks_lock:
            m.tasks["t-retry"] = t
            snap = m._persist_tasks_locked()
        m._write_task_store(snap)
        # 落盘 JSON 含 _retry_count
        data = json.loads((tmp_path / "tasks.json").read_text())
        assert data["tasks"][0]["_retry_count"] == 2
        # 新 Master 恢复: 重试预算保留, 不归零
        m2 = _master_with_store(tmp_path)
        restored = await m2._restore_tasks()
        assert restored == 1
        assert m2.tasks["t-retry"].status == TaskStatus.PENDING
        assert getattr(m2.tasks["t-retry"], "_retry_count", 0) == 2

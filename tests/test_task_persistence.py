"""H3 Master 任务持久化 + 启动恢复测试。

验证崩溃自愈: RUNNING/PENDING 任务落盘 → 新 Master 启动恢复 → RUNNING 置 PENDING 重派。
终态任务不落盘。无文件时启动恢复 0 任务。
No external API callers — internal unit tests.
"""

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
        await m.register_node(
            NodeInfo(node_id="n1", hostname="mac1", ip_address="10.0.0.1", port=11458)
        )
        t = _task("t-run", TaskStatus.RUNNING)
        t.assigned_nodes = ["n1"]
        async with m._tasks_lock:
            m.tasks["t-run"] = t
            m._persist_tasks_locked()
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
            m._persist_tasks_locked()
        m2 = _master_with_store(tmp_path)
        await m2._restore_tasks()
        assert m2.tasks["t-mig"].status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_pending_restored_as_pending(self, tmp_path):
        m = _master_with_store(tmp_path)
        t = _task("t-pend")
        async with m._tasks_lock:
            m.tasks["t-pend"] = t
            m._persist_tasks_locked()
        m2 = _master_with_store(tmp_path)
        await m2._restore_tasks()
        assert m2.tasks["t-pend"].status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_terminal_not_persisted(self, tmp_path):
        m = _master_with_store(tmp_path)
        for st in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
            async with m._tasks_lock:
                m.tasks[f"t-{st.value}"] = _task(f"t-{st.value}", st)
                m._persist_tasks_locked()
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
            m._persist_tasks_locked()
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
            m._persist_tasks_locked()
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
            m._persist_tasks_locked()
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
            m._persist_tasks_locked()
        await m.stop()
        assert (tmp_path / "tasks.json").exists()

        m2 = _master_with_store(tmp_path)
        restored = await m2._restore_tasks()
        assert restored == 1
        assert m2.tasks["t-survive"].status == TaskStatus.PENDING

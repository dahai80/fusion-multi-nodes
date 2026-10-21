"""#82 常驻节点调度宿主测试 — spec 校验/next_run 确定性/持久化 roundtrip/触发循环。"""

import asyncio
import time

import pytest

from fusion_multi_node.agent.scheduling import (
    _MAX_RUNS_PER_SCHEDULE,
    ScheduleHost,
    ScheduleRun,
    ScheduleSpec,
)


def _spec(**kw) -> ScheduleSpec:
    base = {
        "schedule_id": "sched-test",
        "kind": "interval",
        "every_seconds": 3600,
    }
    base.update(kw)
    return ScheduleSpec(**base)


class _Recorder:
    def __init__(self):
        self.tasks: list[dict] = []
        self.fail = False

    async def __call__(self, task: dict) -> dict:
        self.tasks.append(task)
        if self.fail:
            return {"error": "boom"}
        return {"outputs": [f"ran {task['task_id']}"]}


def test_interval_next_run_deterministic():
    spec = _spec(every_seconds=120)
    assert spec.next_run_at(1000.0) == 1120.0


def test_daily_future_today():
    spec = _spec(kind="daily", at="23:00")
    lt = time.localtime(time.time())
    now = time.mktime(lt[:3] + (10, 0, 0, 0, 0, -1))
    nxt = spec.next_run_at(now)
    got = time.localtime(nxt)
    assert (got.tm_hour, got.tm_min) == (23, 0)
    assert got.tm_mday == lt.tm_mday


def test_daily_past_moves_tomorrow():
    spec = _spec(kind="daily", at="03:00")
    lt = time.localtime(time.time())
    now = time.mktime(lt[:3] + (23, 30, 0, 0, 0, -1))
    nxt = spec.next_run_at(now)
    got = time.localtime(nxt)
    assert (got.tm_hour, got.tm_min) == (3, 0)
    assert got.tm_mday != lt.tm_mday


def test_weekly_same_day_future():
    spec = _spec(kind="weekly", at="09:00", weekday=time.localtime(time.time()).tm_wday)
    lt = time.localtime(time.time())
    now = time.mktime(lt[:3] + (8, 0, 0, 0, 0, -1))
    nxt = spec.next_run_at(now)
    got = time.localtime(nxt)
    assert (got.tm_hour, got.tm_min) == (9, 0)
    assert got.tm_mday == lt.tm_mday


def test_weekly_past_moves_next_week():
    wd = time.localtime(time.time()).tm_wday
    spec = _spec(kind="weekly", at="09:00", weekday=wd)
    lt = time.localtime(time.time())
    now = time.mktime(lt[:3] + (10, 0, 0, 0, 0, -1))
    nxt = spec.next_run_at(now)
    got = time.localtime(nxt)
    assert got.tm_wday == wd
    assert 6 * 86400 <= nxt - now < 7.2 * 86400


def test_validation_errors(tmp_path):
    tmp_path_unused = tmp_path / "x.json"
    with pytest.raises(ValueError):
        _spec(kind="cron").validate()
    with pytest.raises(ValueError):
        _spec(task_type="plugin").validate()
    with pytest.raises(ValueError):
        _spec(every_seconds=30).validate()
    with pytest.raises(ValueError):
        _spec(kind="daily", at="25:00").validate()
    with pytest.raises(ValueError):
        _spec(kind="daily").validate()
    with pytest.raises(ValueError):
        _spec(kind="weekly", at="09:00", weekday=7).validate()
    with pytest.raises(ValueError):
        host = ScheduleHost(resident=True, executor=_Recorder(), store_path=tmp_path_unused)
        host.register(_spec(params={"bad": object()}))


def test_register_list_get_remove_roundtrip(tmp_path):
    store = tmp_path / "schedules.json"
    host = ScheduleHost(resident=True, executor=_Recorder(), store_path=store)
    saved = host.register(_spec())
    assert saved["schedule_id"] == "sched-test"
    assert len(host.list_schedules()) == 1
    detail = host.get("sched-test")
    assert detail is not None and detail["next_run_at"] > time.time()
    assert host.remove("sched-test") is True
    assert host.get("sched-test") is None
    assert host.remove("sched-test") is False


def test_register_generates_id_when_missing(tmp_path):
    host = ScheduleHost(resident=True, executor=_Recorder(), store_path=tmp_path / "s.json")
    saved = host.register(_spec(schedule_id=""))
    assert saved["schedule_id"].startswith("sched-")


def test_register_not_resident_denied(tmp_path):
    host = ScheduleHost(resident=False, executor=_Recorder(), store_path=tmp_path / "s.json")
    with pytest.raises(PermissionError):
        host.register(_spec())


def test_persist_restore_roundtrip(tmp_path):
    store = tmp_path / "s.json"
    host = ScheduleHost(resident=True, executor=_Recorder(), store_path=store)
    host.register(_spec())
    host.register(_spec(schedule_id="sched-daily", kind="daily", at="23:30"))
    host2 = ScheduleHost(resident=True, executor=_Recorder(), store_path=store)
    restored = host2.load()
    assert restored == 2
    assert host2.get("sched-test") is not None
    assert host2.get("sched-daily") is not None
    # interval 重启后从 now+every 重排 (不补跑)
    nxt = host2.get("sched-test")["next_run_at"]
    assert nxt > time.time() + 3500


def test_load_corrupt_file_starts_empty(tmp_path):
    store = tmp_path / "s.json"
    store.write_text("{corrupt")
    host = ScheduleHost(resident=True, executor=_Recorder(), store_path=store)
    assert host.load() == 0


def test_set_enabled_pause(tmp_path):
    host = ScheduleHost(resident=True, executor=_Recorder(), store_path=tmp_path / "s.json")
    host.register(_spec())
    saved = host.set_enabled("sched-test", False)
    assert saved["enabled"] is False
    assert host.get("sched-test")["enabled"] is False
    assert host.get("sched-test")["next_run_at"] == 0.0


async def test_fire_loop_executes_and_records(tmp_path):
    store = tmp_path / "s.json"
    rec = _Recorder()
    host = ScheduleHost(resident=True, executor=rec, store_path=store, tick_seconds=0.05)
    host.register(_spec(schedule_id="sched-fire", every_seconds=3600))
    host._next_run["sched-fire"] = time.time() - 1
    host.start()
    try:
        for _ in range(40):
            await asyncio.sleep(0.05)
            if rec.tasks:
                break
        assert len(rec.tasks) == 1
        t = rec.tasks[0]
        assert t["task_id"] == "sched-fire-000001"
        assert t["type"] == "inference"
        detail = host.get("sched-fire")
        runs = detail["runs"]
        assert len(runs) == 1 and runs[0]["status"] == "completed"
        assert runs[0]["result"]["outputs"][0] == "ran sched-fire-000001"
        assert detail["next_run_at"] > time.time() + 3500
    finally:
        await host.stop()


async def test_fire_failure_recorded(tmp_path):
    store = tmp_path / "s.json"
    rec = _Recorder()
    rec.fail = True
    host = ScheduleHost(resident=True, executor=rec, store_path=store, tick_seconds=0.05)
    host.register(_spec(schedule_id="sched-fail", every_seconds=3600))
    host._next_run["sched-fail"] = time.time() - 1
    host.start()
    try:
        for _ in range(40):
            await asyncio.sleep(0.05)
            if host.get("sched-fail")["runs"]:
                break
        runs = host.get("sched-fail")["runs"]
        assert runs[0]["status"] == "failed"
        assert runs[0]["error"] == "boom"
    finally:
        await host.stop()


async def test_disabled_schedule_not_fired(tmp_path):
    rec = _Recorder()
    host = ScheduleHost(resident=True, executor=rec, store_path=tmp_path / "s.json", tick_seconds=0.05)
    host.register(_spec(schedule_id="sched-off", every_seconds=3600, enabled=False))
    host._next_run["sched-off"] = time.time() - 1
    host.start()
    try:
        await asyncio.sleep(0.25)
        assert rec.tasks == []
    finally:
        await host.stop()


async def test_run_history_capped(tmp_path):
    rec = _Recorder()
    host = ScheduleHost(resident=True, executor=rec, store_path=tmp_path / "s.json", tick_seconds=0.05)
    host.register(_spec(schedule_id="sched-cap", every_seconds=3600))
    for i in range(_MAX_RUNS_PER_SCHEDULE + 5):
        host._seq["sched-cap"] = i + 1
        run = ScheduleRun(seq=i + 1, task_id=f"sched-cap-{i + 1:06d}", status="completed", started_at=time.time())
        host._runs["sched-cap"].append(run)
        if len(host._runs["sched-cap"]) > _MAX_RUNS_PER_SCHEDULE:
            del host._runs["sched-cap"][:-_MAX_RUNS_PER_SCHEDULE]
    assert len(host._runs["sched-cap"]) == _MAX_RUNS_PER_SCHEDULE
    await host.stop()

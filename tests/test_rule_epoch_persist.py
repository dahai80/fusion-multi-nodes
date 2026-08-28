"""v0.14.0 item 7 — 规则纪元/confirm 持久化测试。

覆盖:
- advance 后重启 (新 ClusterMaster 实例) 恢复纪元值。
- confirm 存入重启恢复。
- 坏盘容错 (缺文件 → 默认 0 / 坏 JSON → 默认 0 不抛)。
- 节流脏标 (高频 advance 第二次 defer, _persist_loop 兜底)。
- HA standby receive_synced_state 接收纪元/confirm。
"""

from __future__ import annotations

import json
import time

import pytest

from fusion_multi_node.master import ClusterMaster

TEST_TOKEN = "test-epoch-persist-token"
M_PORT = 11452
AG_PORT = 11458


def _make_master(tmp_path, host="127.0.0.1", port=M_PORT) -> ClusterMaster:
    m = ClusterMaster(host=host, port=port, heartbeat_timeout=60.0)
    m._task_store_path = tmp_path / f"tasks-{port}.json"
    m._election_state_path = tmp_path / f"election-{port}.json"
    m._rule_epoch_path = tmp_path / f"rule_epoch-{port}.json"
    m._dispatch_token = TEST_TOKEN
    return m


class TestRuleEpochPersist:
    @pytest.mark.asyncio
    async def test_advance_persisted_across_restart(self, tmp_path):
        # 第一实例推进纪元 → 落盘
        m1 = _make_master(tmp_path)
        epoch = await m1.advance_rule_epoch("rule change A")
        assert epoch == 1
        # 即时落盘 (距上次 >= throttle, 首次必即时)
        assert m1._rule_epoch_path.exists()
        data = json.loads(m1._rule_epoch_path.read_text())
        assert data["rule_epoch"] == 1

        # 第二实例重启 → 恢复
        m2 = _make_master(tmp_path)
        m2._load_rule_epoch_state()
        assert await m2.get_rule_epoch() == 1

    @pytest.mark.asyncio
    async def test_confirm_persisted_across_restart(self, tmp_path):
        m1 = _make_master(tmp_path)
        # 推进纪元 (confirm 依赖 epoch) — 首次即时落盘
        await m1.advance_rule_epoch("init")
        # 确认前重置节流时间戳 → confirm 即时落盘 (非节流 defer)
        m1._last_epoch_persist_ts = 0.0
        # 存 confirm — 需合法 MAC (经 cluster_key derive)
        from fusion_multi_node.security.cluster_key import canonical_json, derive_confirm_relay_key, mac_payload

        token = m1._get_dispatch_token()
        key = derive_confirm_relay_key(token)
        record = {
            "confirm_id": "c1",
            "node_id": "n1",
            "action": "guard_check",
            "epoch": 1,
            "ts": "2026-08-28T00:00:00Z",
        }
        mac = mac_payload(key, canonical_json(record))
        res = await m1.receive_confirm(
            confirm_id="c1",
            node_id="n1",
            action="guard_check",
            epoch=1,
            ts="2026-08-28T00:00:00Z",
            mac=mac,
        )
        assert res["status"] == "ok"

        # 第二实例重启 → 恢复 confirm
        m2 = _make_master(tmp_path)
        m2._load_rule_epoch_state()
        confirms = await m2.get_confirms()
        assert any(c["confirm_id"] == "c1" and c["node_id"] == "n1" for c in confirms)
        assert await m2.get_rule_epoch() == 1

    @pytest.mark.asyncio
    async def test_missing_file_defaults_zero(self, tmp_path):
        m = _make_master(tmp_path)
        # 无文件 → 默认 0/空, 不抛
        m._load_rule_epoch_state()
        assert await m.get_rule_epoch() == 0
        assert await m.get_confirms() == []

    @pytest.mark.asyncio
    async def test_corrupt_json_defaults_zero(self, tmp_path):
        m = _make_master(tmp_path)
        m._rule_epoch_path.write_text("{not valid json")
        # 坏 JSON → 默认 0/空, 不抛 (容错 Rule 12)
        m._load_rule_epoch_state()
        assert await m.get_rule_epoch() == 0
        assert await m.get_confirms() == []

    @pytest.mark.asyncio
    async def test_throttle_defers_second_write(self, tmp_path):
        m = _make_master(tmp_path)
        # 模拟刚写过 (last_ts 置近) → advance 应节流 defer (代码用 time.time)
        m._last_epoch_persist_ts = time.time()
        await m.advance_rule_epoch("first fast")
        # 节流期: 设脏标不即时落盘新值
        assert m._rule_epoch_dirty is True

    @pytest.mark.asyncio
    async def test_persist_loop_flushes_dirty(self, tmp_path):
        m = _make_master(tmp_path)
        m._last_epoch_persist_ts = time.time()
        await m.advance_rule_epoch("deferred")
        assert m._rule_epoch_dirty is True
        # 手动触发持久化兜底逻辑 (_persist_loop 单轮)
        m._rule_epoch_dirty = False
        m._last_epoch_persist_ts = 0.0
        await m._persist_rule_epoch_async()
        assert m._rule_epoch_path.exists()
        data = json.loads(m._rule_epoch_path.read_text())
        assert data["rule_epoch"] == 1

    @pytest.mark.asyncio
    async def test_stop_final_persist(self, tmp_path):
        m = _make_master(tmp_path)
        m._running = True
        await m.advance_rule_epoch("before stop")
        # 模拟 stop 的最终落盘片段 (不跑完整 stop 避启 server)
        await m._persist_rule_epoch_async()
        data = json.loads(m._rule_epoch_path.read_text())
        assert data["rule_epoch"] == 1


class TestRuleEpochHASync:
    @pytest.mark.asyncio
    async def test_standby_receives_epoch_via_sync(self, tmp_path):
        # leader 推进纪元
        leader = _make_master(tmp_path, port=11460)
        await leader.advance_rule_epoch("leader rule")
        assert await leader.get_rule_epoch() == 1

        # standby 接收 state sync payload (含 rule_epoch_state)
        snapshot = leader._build_rule_epoch_snapshot()
        state = {"rule_epoch_state": snapshot, "saved_at": 0.0}
        standby = _make_master(tmp_path, port=11461)
        assert await standby.get_rule_epoch() == 0
        await standby.receive_synced_state(state)
        assert await standby.get_rule_epoch() == 1

    @pytest.mark.asyncio
    async def test_standby_receives_confirms_via_sync(self, tmp_path):
        leader = _make_master(tmp_path, port=11462)
        await leader.advance_rule_epoch("init")
        from fusion_multi_node.security.cluster_key import canonical_json, derive_confirm_relay_key, mac_payload

        key = derive_confirm_relay_key(leader._get_dispatch_token())
        record = {
            "confirm_id": "cx",
            "node_id": "nx",
            "action": "check",
            "epoch": 1,
            "ts": "2026-08-28T00:00:00Z",
        }
        mac = mac_payload(key, canonical_json(record))
        await leader.receive_confirm("cx", "nx", "check", 1, "2026-08-28T00:00:00Z", mac)

        snapshot = leader._build_rule_epoch_snapshot()
        state = {"rule_epoch_state": snapshot, "saved_at": 0.0}
        standby = _make_master(tmp_path, port=11463)
        await standby.receive_synced_state(state)
        confirms = await standby.get_confirms()
        assert any(c["confirm_id"] == "cx" for c in confirms)

    @pytest.mark.asyncio
    async def test_standby_rollback_protection(self, tmp_path):
        # standby 已有更高纪元 → 同步低纪元不回退
        standby = _make_master(tmp_path, port=11464)
        await standby.receive_rule_epoch(5, "prior leader")
        state = {"rule_epoch_state": {"rule_epoch": 2, "confirms": {}}, "saved_at": 0.0}
        await standby.receive_synced_state(state)
        assert await standby.get_rule_epoch() == 5

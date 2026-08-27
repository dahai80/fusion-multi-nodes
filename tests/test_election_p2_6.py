"""P2-6 (审计 §5.10): 选举 _lock 持锁内 await HTTP + fsync 移锁外 单元测试。

验证锁内快照 + 锁外 I/O 范式:
- _start_election: send_vote_request (HTTP) + _persist_state_async (fsync) 不持 _lock
- handle_vote_request: _persist_state_async 不持 _lock
- receive_heartbeat: _persist_state_async 不持 _lock
- _broadcast_heartbeat: send_heartbeat (HTTP) 不持 _lock
"""

from __future__ import annotations

from pathlib import Path

from fusion_multi_node.master.election import (
    ElectionState,
    MasterElection,
    VoteRequest,
    VoteResponse,
)


def _make_election(tmp_path: Path, send_vote=None, send_heartbeat=None) -> MasterElection:
    e = MasterElection(
        node_id="node-1",
        priority=5,
        known_nodes=["node-2", "node-3"],
        send_vote_request=send_vote,
        send_heartbeat=send_heartbeat,
        state_path=tmp_path / "election.json",
    )
    return e


class TestP2_6ElectionLockOutsideIO:
    """P2-6: HTTP/fsync 移锁外 — I/O 执行期间 _lock.locked() 须为 False。"""

    async def test_p2_6_start_election_http_and_fsync_outside_lock(self, tmp_path):
        # send_vote 回调 + persist 期间 _lock 不应持锁。
        persist_locked: list[bool] = []
        vote_locked: list[bool] = []

        async def fake_persist():
            persist_locked.append(e._lock.locked())

        async def fake_vote(req, peer):
            vote_locked.append(e._lock.locked())
            return VoteResponse(term=req.term, vote_granted=True, voter_id=peer)

        e = _make_election(tmp_path, send_vote=fake_vote)
        e._persist_state_async = fake_persist  # 替换锁外落盘
        e.state = ElectionState.FOLLOWER
        # 已知 2 对端 + 自身 = 3, 多数 2; 两票均授 → 胜出 (走完 _become_leader)。
        await e._start_election()

        assert e.state == ElectionState.LEADER
        # 两次拉票期间锁均释放; 一次 persist 期间锁释放。
        assert len(vote_locked) == 2 and all(not lk for lk in vote_locked), f"P2-6 拉票持锁: {vote_locked}"
        assert len(persist_locked) >= 1 and all(not lk for lk in persist_locked), f"P2-6 落盘持锁: {persist_locked}"

    async def test_p2_6_handle_vote_request_persist_outside_lock(self, tmp_path):
        persist_locked: list[bool] = []

        async def fake_persist():
            persist_locked.append(e._lock.locked())

        e = _make_election(tmp_path)
        e._persist_state_async = fake_persist
        # term 跃迁 (>0) 触发 need_persist, 投票亦 need_persist。
        req = VoteRequest(term=1, candidate_id="node-2", candidate_priority=5)
        resp = await e.handle_vote_request(req)

        assert resp.vote_granted is True
        assert len(persist_locked) >= 1 and all(not lk for lk in persist_locked), f"P2-6 投票落盘持锁: {persist_locked}"

    async def test_p2_6_receive_heartbeat_persist_outside_lock(self, tmp_path):
        persist_locked: list[bool] = []

        async def fake_persist():
            persist_locked.append(e._lock.locked())

        e = _make_election(tmp_path)
        e._persist_state_async = fake_persist
        await e.receive_heartbeat(leader_id="node-2", term=3)

        assert e.current_term == 3
        assert e._leader_id == "node-2"
        assert len(persist_locked) >= 1 and all(not lk for lk in persist_locked), f"P2-6 心跳落盘持锁: {persist_locked}"

    async def test_p2_6_broadcast_heartbeat_http_outside_lock(self, tmp_path):
        heartbeat_locked: list[bool] = []

        async def fake_heartbeat(peer):
            heartbeat_locked.append(e._lock.locked())

        e = _make_election(tmp_path, send_heartbeat=fake_heartbeat)
        # 锁外推心跳 — peers 快照由调用方传入。
        await e._broadcast_heartbeat(["node-2", "node-3"])

        assert len(heartbeat_locked) == 2 and all(not lk for lk in heartbeat_locked), (
            f"P2-6 心跳推送持锁: {heartbeat_locked}"
        )

    async def test_p2_6_persist_actually_writes_disk(self, tmp_path):
        # 落盘移锁外后仍须真正写盘 (语义不变, 仅不持锁)。
        e = _make_election(tmp_path)
        e.current_term = 7
        e.voted_for = "node-2"
        await e._persist_state_async()
        assert e._state_path.exists()
        import json

        data = json.loads(e._state_path.read_text())
        assert data["current_term"] == 7
        assert data["voted_for"] == "node-2"

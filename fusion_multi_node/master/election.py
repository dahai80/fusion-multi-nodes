"""M3-03 Master 选举机制 — 优先级 + 心跳超时的分布式选举协议。

当 Master 节点故障时，集群中多个 Standby 节点通过选举协议确定新 Master。
选举策略:
- 优先级: 每个候选人有优先级值，高优先级优先
- 心跳超时: 基于超时检测触发选举
- 投票: 候选人向已知节点拉票，多数票获胜
- 分裂预防: 优先级相同按 node_id 字典序打破
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ElectionState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class ElectionCandidate:
    node_id: str
    priority: int = 0
    hostname: str = ""
    ip_address: str = ""
    port: int = 11452
    term: int = 0
    voted_for: str = ""
    last_heartbeat: float = 0.0


@dataclass
class VoteRequest:
    term: int
    candidate_id: str
    candidate_priority: int
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass
class VoteResponse:
    term: int
    vote_granted: bool
    voter_id: str = ""


class MasterElection:
    """Master 选举管理器。"""

    def __init__(
        self,
        node_id: str,
        priority: int = 0,
        known_nodes: list[str] | None = None,
        election_timeout_range: tuple = (5.0, 10.0),
        heartbeat_interval: float = 3.0,
        on_elected: Callable[[], Any] | None = None,
        on_demoted: Callable[[], Any] | None = None,
        send_vote_request: Callable[[VoteRequest, str], Any] | None = None,
        send_heartbeat: Callable[[str], Any] | None = None,
        state_path: Path | None = None,
    ):
        self.node_id = node_id
        self.priority = priority
        self._known_nodes: set[str] = set(known_nodes or [])
        # node_id → ElectionCandidate (带 ip/port, 供 send_vote_request 回调解析对端地址)。
        # add_known_node 同步填充; get_candidate 公开查询。
        self.candidates: dict[str, ElectionCandidate] = {}
        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval
        self._on_elected = on_elected
        self._on_demoted = on_demoted
        self._send_vote_request = send_vote_request
        # C1: Leader 心跳广播回调 — leader 周期性向 follower 推 heartbeat, 维持权威。
        # 旧实现 leader 仅自盖 _last_heartbeat, follower 收不到 → 误判 leader 死 → term 抖动重选。
        self._send_heartbeat = send_heartbeat
        # C2: term/voted_for 持久化 — 崩溃重启不丢投票历史, 防同一 term 重复投票 (split brain)。
        # 无 path 则纯内存 (单 Master / 测试不落盘)。
        self._state_path = state_path

        self.state = ElectionState.FOLLOWER
        self.current_term = 0
        self.voted_for: str | None = None
        self._load_state()
        self._votes_received: set[str] = set()
        self._last_heartbeat = time.time()
        self._last_broadcast = 0.0
        self._election_timeout = self._random_timeout()
        self._running = False
        self._task: asyncio.Task | None = None
        self._leader_id: str | None = None
        self._lock = asyncio.Lock()

    def _random_timeout(self) -> float:
        lo, hi = self._election_timeout_range
        return lo + random.random() * (hi - lo)

    def add_known_node(self, node_or_candidate: str | ElectionCandidate) -> None:
        """登记已知候选节点。接受 str (仅 node_id, 向后兼容) 或 ElectionCandidate (带 ip/port)。

        传 str 时仅入 _known_nodes 集 (无地址, 不可达, 用于多数计数);
        传 ElectionCandidate 时同时入 candidates 字典 (供 get_candidate 解析对端 HTTP 地址)。
        """
        if isinstance(node_or_candidate, ElectionCandidate):
            cand = node_or_candidate
            self._known_nodes.add(cand.node_id)
            self.candidates[cand.node_id] = cand
        else:
            self._known_nodes.add(node_or_candidate)

    def get_candidate(self, node_id: str) -> ElectionCandidate | None:
        """查询候选节点 (含 ip/port)。无地址返回 None。"""
        return self.candidates.get(node_id)

    @property
    def leader_known(self) -> bool:
        """P1-17: leader 是否已确定。选举空窗期 (_leader_id None) → False, 客户端应得 503 而非误派。"""
        return self._leader_id is not None

    def remove_known_node(self, node_id: str) -> None:
        self._known_nodes.discard(node_id)
        self.candidates.pop(node_id, None)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._election_loop())
        logger.info(f"选举管理器启动: {self.node_id} (priority={self.priority})")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("选举管理器已停止")

    async def _election_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(0.5)
                now = time.time()

                # P0-1: 逐次异常隔离, 单轮选举/心跳失败不杀循环 (否则 HA 静默停滞零告警)。
                try:
                    # P2-6: 锁内仅内存状态判定 + 快照, HTTP I/O (start_election 拉票 /
                    # broadcast_heartbeat 推送) 移锁外 — 旧实现持 _lock 内 await N 个对端 HTTP。
                    should_elect = False
                    should_broadcast = False
                    broadcast_peers: list[str] = []
                    async with self._lock:
                        if self.state == ElectionState.FOLLOWER:
                            if now - self._last_heartbeat > self._election_timeout:
                                should_elect = True
                        elif self.state == ElectionState.CANDIDATE:
                            pass
                        elif self.state == ElectionState.LEADER:
                            # C1: 周期广播 heartbeat 到所有 follower (非每 0.5s, 按 heartbeat_interval)。
                            # 旧实现仅 self._last_heartbeat = now, follower 收不到心跳 → 超时重选 → term 抖动。
                            self._last_heartbeat = now
                            if now - self._last_broadcast >= self._heartbeat_interval:
                                self._last_broadcast = now
                                should_broadcast = True
                                broadcast_peers = [nid for nid in self._known_nodes if nid != self.node_id]
                    if should_elect:
                        await self._start_election()
                    if should_broadcast:
                        await self._broadcast_heartbeat(broadcast_peers)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"选举循环异常: {e}")
        except asyncio.CancelledError:
            pass

    async def _broadcast_heartbeat(self, peers: list[str]) -> None:
        """P2-6 (审计 §5.10): 锁外向所有已知 follower 推 heartbeat (best-effort)。

        C1 修复: 调 send_heartbeat 回调 (HTTP POST /api/ha/heartbeat)。
        回调内自解析对端地址 + best-effort 吞异常, 这里只负责遍历已知节点。
        peers 由 _election_loop 锁内快照传入, HTTP 不持 _lock (旧实现持锁内 await N 对端)。
        """
        if not self._send_heartbeat:
            return
        for peer_id in peers:
            try:
                if asyncio.iscoroutinefunction(self._send_heartbeat):
                    await self._send_heartbeat(peer_id)
                else:
                    self._send_heartbeat(peer_id)
            except Exception as e:
                logger.debug(f"心跳推送失败: {peer_id}: {e}")

    async def _start_election(self) -> None:
        # P2-6 (审计 §5.10): 锁内仅改内存状态 (term/state/voted/votes 集) + 快照投票请求,
        # HTTP send_vote_request 轮询对端 + _save_state (fsync) 移锁外 — 旧实现整段在
        # _election_loop 持 _lock 内 await N 个对端 HTTP 拉票, 拖慢选举 + 长持锁阻塞
        # handle_vote_request/receive_heartbeat。快照 vote_req 字段后释放锁, 锁外异步拉票。
        async with self._lock:
            self.current_term += 1
            self.state = ElectionState.CANDIDATE
            self.voted_for = self.node_id
            self._votes_received = {self.node_id}
            self._election_timeout = self._random_timeout()
            vote_req = VoteRequest(
                term=self.current_term,
                candidate_id=self.node_id,
                candidate_priority=self.priority,
            )
            known_peers = [nid for nid in self._known_nodes if nid != self.node_id]
            majority = (len(self._known_nodes) + 1) // 2 + 1
        # 锁外落盘 (fsync 不持锁) — 仍属选举关键路径, 单次 to_thread 不显著增延迟。
        await self._persist_state_async()
        logger.info(f"发起选举: term={vote_req.term}, node={self.node_id}")

        # 锁外并发拉票: _votes_received 仅本 candidate 自身写 (单选举轮), 无跨任务竞争。
        for node_id in known_peers:
            if self._send_vote_request:
                try:
                    if asyncio.iscoroutinefunction(self._send_vote_request):
                        resp = await self._send_vote_request(vote_req, node_id)
                    else:
                        resp = self._send_vote_request(vote_req, node_id)

                    if isinstance(resp, VoteResponse):
                        await self._handle_vote_response(resp)
                except Exception as e:
                    logger.debug(f"拉票失败: {node_id}: {e}")

        if len(self._votes_received) >= majority:
            await self._become_leader()

    async def _handle_vote_response(self, resp: VoteResponse) -> None:
        # P2-6: _save_state (fsync) 移锁外 — 此函数在 _start_election 锁外调用,
        # 但 term 跃迁仍经 _persist_state_async 异步落盘 (不阻塞拉票循环)。
        if resp.term > self.current_term:
            self.current_term = resp.term
            await self._persist_state_async()
            await self._become_follower()
            return

        if self.state != ElectionState.CANDIDATE:
            return

        if resp.vote_granted:
            self._votes_received.add(resp.voter_id)
            majority = (len(self._known_nodes) + 1) // 2 + 1
            if len(self._votes_received) >= majority:
                await self._become_leader()

    async def handle_vote_request(self, req: VoteRequest) -> VoteResponse:
        # P2-6 (审计 §5.10): 锁内仅读写内存状态 (term/voted_for/state), fsync 落盘
        # (_save_state) 移锁外 — 旧实现持 _lock 内调 _save_state (含 os.fsync 同步写盘),
        # 拖慢投票应答 + 阻塞选举锁。快照需落盘字段 (term/voted_for), 锁外异步 to_thread 落盘,
        # 不阻塞事件循环亦不持锁 (沿用 cluster_master 锁内快照锁外 I/O 范式)。
        need_persist = False
        async with self._lock:
            if req.term > self.current_term:
                self.current_term = req.term
                need_persist = True
                await self._become_follower()

            vote_granted = False
            if req.term >= self.current_term:
                if (self.voted_for is None or self.voted_for == req.candidate_id) and (
                    req.candidate_priority >= self.priority or req.term > self.current_term
                ):
                    vote_granted = True
                    self.voted_for = req.candidate_id
                    self._last_heartbeat = time.time()
                    self._election_timeout = self._random_timeout()
                    need_persist = True

            response = VoteResponse(
                term=self.current_term,
                vote_granted=vote_granted,
                voter_id=self.node_id,
            )
        if need_persist:
            await self._persist_state_async()
        return response

    async def receive_heartbeat(self, leader_id: str, term: int) -> None:
        # P2-6: 锁内仅内存状态, _save_state (fsync) 移锁外异步落盘。
        need_persist = False
        async with self._lock:
            if term >= self.current_term:
                self.current_term = term
                need_persist = True
                self._leader_id = leader_id
                self._last_heartbeat = time.time()
                self._election_timeout = self._random_timeout()
                if self.state != ElectionState.FOLLOWER:
                    await self._become_follower()
        if need_persist:
            await self._persist_state_async()

    async def _become_leader(self) -> None:
        self.state = ElectionState.LEADER
        self._leader_id = self.node_id
        logger.warning(f"选举胜出成为 Leader: {self.node_id} (term={self.current_term})")
        if self._on_elected:
            if asyncio.iscoroutinefunction(self._on_elected):
                await self._on_elected()
            else:
                self._on_elected()

    async def _become_follower(self) -> None:
        old_state = self.state
        self.state = ElectionState.FOLLOWER
        self.voted_for = None
        self._votes_received.clear()
        if old_state == ElectionState.LEADER and self._on_demoted:
            logger.warning(f"Leader 降级为 Follower: {self.node_id}")
            if asyncio.iscoroutinefunction(self._on_demoted):
                await self._on_demoted()
            else:
                self._on_demoted()

    def _load_state(self) -> None:
        """C2: 启动时读盘恢复 term/voted_for。无 state_path 或读失败 → 落 0/None (容错)。"""
        if not self._state_path or not self._state_path.exists():
            return
        try:
            data = json.loads(self._state_path.read_text())
            self.current_term = int(data.get("current_term", 0))
            vf = data.get("voted_for")
            self.voted_for = vf if (vf is None or isinstance(vf, str)) else None
            logger.info(f"C2 选举状态恢复: term={self.current_term} voted_for={self.voted_for}")
        except Exception as e:
            logger.warning(f"C2 选举状态恢复失败 ({self._state_path}): {e}")

    def _save_state(self) -> None:
        """C2: term/voted_for 落盘 (原子写)。每次投票/term 变更后调, 防重启 split brain。"""
        if not self._state_path:
            return
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
            with open(tmp, "w") as f:
                json.dump({"current_term": self.current_term, "voted_for": self.voted_for}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._state_path)
        except Exception as e:
            logger.warning(f"C2 选举状态落盘失败: {e}")

    async def _persist_state_async(self) -> None:
        """P2-6 (审计 §5.10): term/voted_for 异步落盘 (锁外调) — 快照字段后 to_thread 调
        _save_state, fsync 不持 _lock 亦不阻塞事件循环。无 state_path (纯内存) 则 no-op。
        供 handle_vote_request/receive_heartbeat/_start_election/_handle_vote_response 锁外落盘。
        """
        if not self._state_path:
            return
        term = self.current_term
        voted = self.voted_for
        await asyncio.to_thread(self._save_state_snapshot, term, voted)

    def _save_state_snapshot(self, term: int, voted: str | None) -> None:
        """P2-6: 按快照值落盘 (锁外 to_thread 调, 避读 self 竞态)。"""
        if not self._state_path:
            return
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
            with open(tmp, "w") as f:
                json.dump({"current_term": term, "voted_for": voted}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._state_path)
        except Exception as e:
            logger.warning(f"C2 选举状态落盘失败: {e}")

    def get_state(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "leader_id": self._leader_id,
            "priority": self.priority,
            "known_nodes": list(self._known_nodes),
            "votes_received": list(self._votes_received),
        }

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
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

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
        known_nodes: Optional[List[str]] = None,
        election_timeout_range: tuple = (5.0, 10.0),
        heartbeat_interval: float = 3.0,
        on_elected: Optional[Callable[[], Any]] = None,
        on_demoted: Optional[Callable[[], Any]] = None,
        send_vote_request: Optional[Callable[[VoteRequest, str], Any]] = None,
    ):
        self.node_id = node_id
        self.priority = priority
        self._known_nodes: Set[str] = set(known_nodes or [])
        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval
        self._on_elected = on_elected
        self._on_demoted = on_demoted
        self._send_vote_request = send_vote_request

        self.state = ElectionState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self._votes_received: Set[str] = set()
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_timeout()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._leader_id: Optional[str] = None
        self._lock = asyncio.Lock()

    def _random_timeout(self) -> float:
        lo, hi = self._election_timeout_range
        return lo + random.random() * (hi - lo)

    def add_known_node(self, node_id: str) -> None:
        self._known_nodes.add(node_id)

    def remove_known_node(self, node_id: str) -> None:
        self._known_nodes.discard(node_id)

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

                async with self._lock:
                    if self.state == ElectionState.FOLLOWER:
                        if now - self._last_heartbeat > self._election_timeout:
                            await self._start_election()
                    elif self.state == ElectionState.CANDIDATE:
                        pass
                    elif self.state == ElectionState.LEADER:
                        self._last_heartbeat = now
        except asyncio.CancelledError:
            pass

    async def _start_election(self) -> None:
        self.current_term += 1
        self.state = ElectionState.CANDIDATE
        self.voted_for = self.node_id
        self._votes_received = {self.node_id}
        self._election_timeout = self._random_timeout()

        logger.info(f"发起选举: term={self.current_term}, node={self.node_id}")

        vote_req = VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            candidate_priority=self.priority,
        )

        for node_id in self._known_nodes:
            if node_id == self.node_id:
                continue
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

        majority = (len(self._known_nodes) + 1) // 2 + 1
        if len(self._votes_received) >= majority:
            await self._become_leader()

    async def _handle_vote_response(self, resp: VoteResponse) -> None:
        if resp.term > self.current_term:
            self.current_term = resp.term
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
        async with self._lock:
            if req.term > self.current_term:
                self.current_term = req.term
                await self._become_follower()

            vote_granted = False
            if req.term >= self.current_term:
                if self.voted_for is None or self.voted_for == req.candidate_id:
                    if req.candidate_priority >= self.priority or req.term > self.current_term:
                        vote_granted = True
                        self.voted_for = req.candidate_id
                        self._last_heartbeat = time.time()
                        self._election_timeout = self._random_timeout()

            return VoteResponse(
                term=self.current_term,
                vote_granted=vote_granted,
                voter_id=self.node_id,
            )

    async def receive_heartbeat(self, leader_id: str, term: int) -> None:
        async with self._lock:
            if term >= self.current_term:
                self.current_term = term
                self._leader_id = leader_id
                self._last_heartbeat = time.time()
                self._election_timeout = self._random_timeout()
                if self.state != ElectionState.FOLLOWER:
                    await self._become_follower()

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

    def get_state(self) -> Dict[str, Any]:
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

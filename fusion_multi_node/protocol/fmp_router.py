"""FMP 消息路由器。

职责:
- 消息路由: 根据 target_id 投递到正确连接
- hop_count 校验: 超限直接拦截
- 多轮对话管理: round_id + round_number 追踪
- 与 CircuitBreaker 集成
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from .fmp_connection import FMPConnectionManager
from .fmp_message import (
    FMPMessage,
    PayloadType,
    MAX_ROUNDS,
)

logger = logging.getLogger(__name__)


@dataclass
class RoundInfo:
    """多轮对话追踪。"""
    round_id: str
    source_id: str
    target_id: str
    current_round: int = 0
    max_rounds: int = MAX_ROUNDS
    started_at: float = 0.0
    last_active: float = 0.0

    def can_next(self) -> bool:
        return self.current_round < self.max_rounds


class FMPRouter:
    """FMP 消息路由器。"""

    def __init__(
        self,
        local_node_id: str = "",
        connection_manager: Optional[FMPConnectionManager] = None,
        on_local_message: Optional[Callable[[FMPMessage], None]] = None,
    ):
        self.local_node_id = local_node_id
        self._conn_mgr = connection_manager
        self._on_local_message = on_local_message
        self._rounds: Dict[str, RoundInfo] = {}
        self._blocked_nodes: set = set()
        self._stats = {
            "routed": 0,
            "dropped_hop": 0,
            "dropped_round": 0,
            "dropped_blocked": 0,
            "local_delivered": 0,
        }

    async def route(self, msg: FMPMessage) -> bool:
        """路由消息。"""
        self.cleanup_stale_rounds()
        link = msg.link

        # 检查目标是否被屏蔽
        if link.target_id in self._blocked_nodes:
            self._stats["dropped_blocked"] += 1
            logger.warning(f"消息被屏蔽: target={link.target_id}")
            return False

        # hop_count 校验
        if not link.can_forward():
            self._stats["dropped_hop"] += 1
            logger.warning(f"消息跳数超限: hop_count={link.hop_count}")
            return False

        # 多轮对话校验
        business = msg.business
        if business.round_id:
            round_info = self._rounds.get(business.round_id)
            if round_info and not round_info.can_next():
                self._stats["dropped_round"] += 1
                logger.warning(f"消息轮次超限: round={round_info.current_round}/{round_info.max_rounds}")
                return False

        # 本地投递
        if link.target_id == self.local_node_id:
            self._stats["local_delivered"] += 1
            if self._on_local_message:
                self._on_local_message(msg)
            return True

        # 远程转发
        if not self._conn_mgr:
            logger.error("无连接管理器，无法转发")
            return False

        # 增加 hop_count
        link.forward(self.local_node_id)

        # 更新多轮对话
        if business.round_id:
            round_info = self._rounds.get(business.round_id)
            if round_info:
                round_info.current_round += 1
                round_info.last_active = time.time()

        ok = await self._conn_mgr.send_to(link.target_id, msg)
        if ok:
            self._stats["routed"] += 1
        return ok

    async def send(
        self,
        target_id: str,
        payload_type: PayloadType,
        payload: Any,
        round_id: str = "",
    ) -> bool:
        """便捷发送接口。"""
        msg = FMPMessage.create(
            source_id=self.local_node_id,
            target_id=target_id,
            payload_type=payload_type,
            payload=payload,
            round_id=round_id,
        )
        return await self.route(msg)

    def register_round(self, round_id: str, source_id: str, target_id: str, max_rounds: int = MAX_ROUNDS) -> None:
        self._rounds[round_id] = RoundInfo(
            round_id=round_id,
            source_id=source_id,
            target_id=target_id,
            max_rounds=max_rounds,
            started_at=time.time(),
            last_active=time.time(),
        )

    def block_node(self, node_id: str) -> None:
        self._blocked_nodes.add(node_id)
        logger.info(f"节点已屏蔽: {node_id}")

    def unblock_node(self, node_id: str) -> None:
        self._blocked_nodes.discard(node_id)
        logger.info(f"节点已解除屏蔽: {node_id}")

    def cleanup_stale_rounds(self, max_age: float = 3600.0) -> int:
        now = time.time()
        stale = [rid for rid, r in self._rounds.items() if now - r.last_active > max_age]
        for rid in stale:
            del self._rounds[rid]
        if stale:
            logger.info(f"清理过期对话: {len(stale)} 个")
        return len(stale)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "local_node_id": self.local_node_id,
            "active_rounds": len(self._rounds),
            "blocked_nodes": list(self._blocked_nodes),
            **self._stats,
        }

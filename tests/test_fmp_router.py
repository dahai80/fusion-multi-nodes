"""FMP Router 测试。"""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from fusion_multi_node.protocol.fmp_message import (
    MAX_HOP_COUNT,
    MAX_ROUNDS,
    FMPMessage,
    PayloadType,
)
from fusion_multi_node.protocol.fmp_router import FMPRouter, RoundInfo


def _make_msg(
    source_id: str = "node_a",
    target_id: str = "node_b",
    payload_type: PayloadType = PayloadType.HEARTBEAT,
    hop_count: int = 0,
    round_id: str = "",
) -> FMPMessage:
    msg = FMPMessage.create(
        source_id=source_id,
        target_id=target_id,
        payload_type=payload_type,
        payload={"text": "hello"},
    )
    msg.link.hop_count = hop_count
    if round_id:
        msg.business.round_id = round_id
    return msg


class TestRoundInfo:
    def test_can_next_under_limit(self):
        ri = RoundInfo(round_id="r1", source_id="a", target_id="b", current_round=3, max_rounds=10)
        assert ri.can_next() is True

    def test_can_next_at_limit(self):
        ri = RoundInfo(round_id="r1", source_id="a", target_id="b", current_round=10, max_rounds=10)
        assert ri.can_next() is False

    def test_default_max_rounds(self):
        ri = RoundInfo(round_id="r1", source_id="a", target_id="b")
        assert ri.max_rounds == MAX_ROUNDS


class TestFMPRouterInit:
    def test_default_init(self):
        router = FMPRouter(local_node_id="node_a")
        assert router.local_node_id == "node_a"
        stats = router.get_stats()
        assert stats["local_node_id"] == "node_a"
        assert stats["routed"] == 0
        assert stats["dropped_hop"] == 0
        assert stats["dropped_round"] == 0
        assert stats["dropped_blocked"] == 0
        assert stats["local_delivered"] == 0
        assert stats["active_rounds"] == 0
        assert stats["blocked_nodes"] == []

    def test_init_with_callback(self):
        cb = MagicMock()
        router = FMPRouter(local_node_id="n1", on_local_message=cb)
        assert router._on_local_message is cb


class TestFMPRouterRoute:
    @pytest.mark.asyncio
    async def test_blocked_node_dropped(self):
        router = FMPRouter(local_node_id="node_a")
        router.block_node("node_b")
        msg = _make_msg(target_id="node_b")
        result = await router.route(msg)
        assert result is False
        stats = router.get_stats()
        assert stats["dropped_blocked"] == 1

    @pytest.mark.asyncio
    async def test_hop_count_exceeded(self):
        router = FMPRouter(local_node_id="node_a")
        msg = _make_msg(hop_count=MAX_HOP_COUNT)
        result = await router.route(msg)
        assert result is False
        stats = router.get_stats()
        assert stats["dropped_hop"] == 1

    @pytest.mark.asyncio
    async def test_round_limit_exceeded(self):
        router = FMPRouter(local_node_id="node_a")
        router.register_round("round_1", "node_a", "node_b", max_rounds=2)
        router._rounds["round_1"].current_round = 2
        msg = _make_msg(round_id="round_1")
        result = await router.route(msg)
        assert result is False
        stats = router.get_stats()
        assert stats["dropped_round"] == 1

    @pytest.mark.asyncio
    async def test_local_delivery_with_callback(self):
        cb = MagicMock()
        router = FMPRouter(local_node_id="node_b", on_local_message=cb)
        msg = _make_msg(target_id="node_b")
        result = await router.route(msg)
        assert result is True
        cb.assert_called_once_with(msg)
        stats = router.get_stats()
        assert stats["local_delivered"] == 1

    @pytest.mark.asyncio
    async def test_local_delivery_without_callback(self):
        router = FMPRouter(local_node_id="node_b")
        msg = _make_msg(target_id="node_b")
        result = await router.route(msg)
        assert result is True
        stats = router.get_stats()
        assert stats["local_delivered"] == 1

    @pytest.mark.asyncio
    async def test_remote_forward_no_conn_mgr(self):
        router = FMPRouter(local_node_id="node_a")
        msg = _make_msg(target_id="node_b")
        result = await router.route(msg)
        assert result is False

    @pytest.mark.asyncio
    async def test_remote_forward_success(self):
        conn_mgr = MagicMock()
        conn_mgr.send_to = AsyncMock(return_value=True)
        router = FMPRouter(
            local_node_id="node_a",
            connection_manager=conn_mgr,
        )
        msg = _make_msg(target_id="node_b")
        result = await router.route(msg)
        assert result is True
        assert msg.link.hop_count == 1
        assert "node_a" in msg.link.trace
        conn_mgr.send_to.assert_called_once_with("node_b", msg)
        stats = router.get_stats()
        assert stats["routed"] == 1

    @pytest.mark.asyncio
    async def test_remote_forward_failure(self):
        conn_mgr = MagicMock()
        conn_mgr.send_to = AsyncMock(return_value=False)
        router = FMPRouter(
            local_node_id="node_a",
            connection_manager=conn_mgr,
        )
        msg = _make_msg(target_id="node_b")
        result = await router.route(msg)
        assert result is False

    @pytest.mark.asyncio
    async def test_remote_forward_updates_round(self):
        conn_mgr = MagicMock()
        conn_mgr.send_to = AsyncMock(return_value=True)
        router = FMPRouter(
            local_node_id="node_a",
            connection_manager=conn_mgr,
        )
        router.register_round("round_1", "node_a", "node_b", max_rounds=5)
        msg = _make_msg(target_id="node_b", round_id="round_1")
        before_round = router._rounds["round_1"].current_round
        await router.route(msg)
        after_round = router._rounds["round_1"].current_round
        assert after_round == before_round + 1


class TestFMPRouterSend:
    @pytest.mark.asyncio
    async def test_send_creates_and_routes(self):
        router = FMPRouter(local_node_id="node_b")
        result = await router.send(
            target_id="node_b",
            payload_type=PayloadType.HEARTBEAT,
            payload={"msg": "hi"},
        )
        assert result is True
        stats = router.get_stats()
        assert stats["local_delivered"] == 1

    @pytest.mark.asyncio
    async def test_send_with_round_id(self):
        router = FMPRouter(local_node_id="node_b")
        result = await router.send(
            target_id="node_b",
            payload_type=PayloadType.CHAT_COMPLETION,
            payload={"text": "test"},
            round_id="round_1",
        )
        assert result is True


class TestFMPRouterBlockNode:
    def test_block_node(self):
        router = FMPRouter(local_node_id="n1")
        router.block_node("bad_node")
        assert "bad_node" in router._blocked_nodes
        stats = router.get_stats()
        assert "bad_node" in stats["blocked_nodes"]

    def test_unblock_node(self):
        router = FMPRouter(local_node_id="n1")
        router.block_node("bad_node")
        router.unblock_node("bad_node")
        assert "bad_node" not in router._blocked_nodes
        stats = router.get_stats()
        assert "bad_node" not in stats["blocked_nodes"]

    def test_unblock_nonexistent(self):
        router = FMPRouter(local_node_id="n1")
        router.unblock_node("nonexistent")
        assert "nonexistent" not in router._blocked_nodes


class TestFMPRouterRegisterRound:
    def test_register_round(self):
        router = FMPRouter(local_node_id="n1")
        router.register_round("r1", "node_a", "node_b", max_rounds=5)
        assert "r1" in router._rounds
        ri = router._rounds["r1"]
        assert ri.source_id == "node_a"
        assert ri.target_id == "node_b"
        assert ri.max_rounds == 5
        assert ri.started_at > 0
        assert ri.last_active > 0


class TestFMPRouterCleanupStaleRounds:
    def test_cleanup_stale(self):
        router = FMPRouter(local_node_id="n1")
        router.register_round("r1", "a", "b")
        router._rounds["r1"].last_active = time.time() - 7200
        count = router.cleanup_stale_rounds(max_age=3600)
        assert count == 1
        assert "r1" not in router._rounds

    def test_cleanup_no_stale(self):
        router = FMPRouter(local_node_id="n1")
        router.register_round("r1", "a", "b")
        count = router.cleanup_stale_rounds(max_age=3600)
        assert count == 0
        assert "r1" in router._rounds


class TestFMPRouterGetStats:
    def test_get_stats(self):
        conn_mgr = MagicMock()
        router = FMPRouter(
            local_node_id="node_a",
            connection_manager=conn_mgr,
        )
        router.block_node("bad_node")
        router.register_round("r1", "a", "b")
        stats = router.get_stats()
        assert stats["local_node_id"] == "node_a"
        assert stats["active_rounds"] == 1
        assert "bad_node" in stats["blocked_nodes"]
        assert "routed" in stats
        assert "dropped_hop" in stats
        assert "dropped_round" in stats
        assert "dropped_blocked" in stats
        assert "local_delivered" in stats

"""FMP 连接管理测试。"""

import asyncio
import pytest

from fusion_multi_node.protocol.fmp_connection import FMPConnection, FMPConnectionManager, ConnectionInfo
from fusion_multi_node.protocol.fmp_message import FMPMessage, PayloadType, FMPCrypto


class TestConnectionInfo:
    def test_basic(self):
        info = ConnectionInfo(node_id="n1", host="10.0.1.5", port=9753)
        assert info.node_id == "n1"
        assert not info.is_alive
        assert info.uptime == 0.0

    def test_uptime_when_alive(self):
        import time
        info = ConnectionInfo(
            node_id="n1", host="10.0.1.5", port=9753,
            is_alive=True, connected_at=time.time(),
        )
        assert info.uptime > 0


class TestFMPConnection:
    def test_init(self):
        conn = FMPConnection(node_id="n1", host="localhost", port=9753)
        assert conn.info.node_id == "n1"
        assert not conn.is_connected

    @pytest.mark.asyncio
    async def test_connect_fails_invalid_host(self):
        conn = FMPConnection(node_id="n1", host="192.0.2.1", port=19999)
        ok = await conn.connect()
        assert not ok

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(self):
        conn = FMPConnection(node_id="n1", host="localhost", port=9753)
        await conn.disconnect()
        assert not conn.is_connected


class TestFMPConnectionManager:
    def test_init(self):
        mgr = FMPConnectionManager(local_node_id="master")
        assert mgr.local_node_id == "master"

    @pytest.mark.asyncio
    async def test_add_connection_fails(self):
        mgr = FMPConnectionManager(local_node_id="master")
        conn = await mgr.add_connection("n1", "192.0.2.1", 19999)
        assert not conn.is_connected

    @pytest.mark.asyncio
    async def test_remove_nonexistent(self):
        mgr = FMPConnectionManager(local_node_id="master")
        await mgr.remove_connection("n1")  # should not raise

    @pytest.mark.asyncio
    async def test_get_connection_none(self):
        mgr = FMPConnectionManager(local_node_id="master")
        assert mgr.get_connection("n1") is None

    @pytest.mark.asyncio
    async def test_send_to_no_connection(self):
        mgr = FMPConnectionManager(local_node_id="master")
        msg = FMPMessage.create("m1", "n1", PayloadType.HEARTBEAT, {"ok": True})
        ok = await mgr.send_to("n1", msg)
        assert not ok

    @pytest.mark.asyncio
    async def test_close_all(self):
        mgr = FMPConnectionManager(local_node_id="master")
        await mgr.close_all()

    @pytest.mark.asyncio
    async def test_stats(self):
        mgr = FMPConnectionManager(local_node_id="master")
        stats = mgr.get_stats()
        assert stats["local_node_id"] == "master"
        assert stats["connections"] == {}

    @pytest.mark.asyncio
    async def test_with_crypto(self):
        key = FMPCrypto.generate_key()
        crypto = FMPCrypto(key=key)
        mgr = FMPConnectionManager(local_node_id="master", crypto=crypto)
        assert mgr._crypto is not None

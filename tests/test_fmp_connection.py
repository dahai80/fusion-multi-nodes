"""FMP 连接管理测试 — 完整覆盖。"""

import asyncio
import struct
import time

import pytest

from fusion_multi_node.protocol.circuit_breaker import CircuitState
from fusion_multi_node.protocol.fmp_connection import (
    ConnectionInfo,
    FMPConnection,
    FMPConnectionManager,
    DEFAULT_RECONNECT_INTERVAL,
)
from fusion_multi_node.protocol.fmp_message import (
    FMPCrypto,
    FMPMessage,
    PayloadType,
)

INVALID_HOST = "127.0.0.1"
INVALID_PORT = 19999


@pytest.fixture(autouse=True)
def cancel_lingering_tasks():
    yield
    try:
        loop = asyncio.get_event_loop()
        for task in asyncio.all_tasks(loop):
            if task is not asyncio.current_task():
                task.cancel()
    except RuntimeError:
        pass


def fmp_echo_handler(received_list=None):
    async def handler(reader, writer):
        while True:
            try:
                len_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=30.0)
                msg_len = int.from_bytes(len_bytes, "big")
                data = await asyncio.wait_for(reader.readexactly(msg_len), timeout=30.0)
                if received_list is not None:
                    try:
                        msg = FMPMessage.deserialize(data)
                        received_list.append(msg)
                    except Exception:
                        pass
            except asyncio.IncompleteReadError:
                break
            except asyncio.TimeoutError:
                continue
            except Exception:
                break
        try:
            writer.close()
        except Exception:
            pass
    return handler


def fmp_send_and_hold_handler(messages_to_send, hold_timeout=5.0):
    async def handler(reader, writer):
        try:
            for msg in messages_to_send:
                serialized = msg.serialize()
                writer.write(len(serialized).to_bytes(4, "big") + serialized)
                await writer.drain()
            while True:
                try:
                    data = await asyncio.wait_for(reader.read(4096), timeout=hold_timeout)
                    if not data:
                        break
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    break
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass
    return handler


class TestConnectionInfo:
    def test_basic(self):
        info = ConnectionInfo(node_id="n1", host="10.0.1.5", port=9753)
        assert info.node_id == "n1"
        assert not info.is_alive
        assert info.uptime == 0.0

    def test_uptime_when_alive(self):
        info = ConnectionInfo(
            node_id="n1", host="10.0.1.5", port=9753,
            is_alive=True, connected_at=time.time(),
        )
        assert info.uptime > 0

    def test_uptime_zero_when_not_alive(self):
        info = ConnectionInfo(
            node_id="n1", host="10.0.1.5", port=9753,
            is_alive=False, connected_at=time.time(),
        )
        assert info.uptime == 0.0


class TestFMPConnection:
    def test_init(self):
        conn = FMPConnection(node_id="n1", host="localhost", port=9753)
        assert conn.info.node_id == "n1"
        assert not conn.is_connected
        assert conn._reconnect_interval == DEFAULT_RECONNECT_INTERVAL

    @pytest.mark.asyncio
    async def test_connect_fails_refused(self):
        conn = FMPConnection(node_id="n1", host=INVALID_HOST, port=INVALID_PORT)
        ok = await conn.connect()
        assert not ok
        assert conn._circuit_breaker._failure_count >= 1

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(self):
        conn = FMPConnection(node_id="n1", host="localhost", port=9753)
        await conn.disconnect()
        assert not conn.is_connected

    @pytest.mark.asyncio
    async def test_connect_success_and_disconnect(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
            ok = await conn.connect()
            assert ok
            assert conn.is_connected
            assert conn.info.is_alive
            assert conn.info.connected_at > 0
            assert conn.info.last_active > 0
            assert conn._circuit_breaker.state == CircuitState.CLOSED
            assert conn._running is True

            await conn.disconnect()
            assert not conn.is_connected
            assert not conn.info.is_alive
            assert conn._reader is None
            assert conn._writer is None
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_send_message(self):
        received = []
        server = await asyncio.start_server(fmp_echo_handler(received), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
            ok = await conn.connect()
            assert ok

            msg = FMPMessage.create(
                source_id="s1", target_id="t1",
                payload_type=PayloadType.HEARTBEAT,
                payload={"status": "ok"},
            )
            sent = await conn.send(msg)
            assert sent

            await asyncio.sleep(0.2)
            assert len(received) == 1
            assert received[0].message_id == msg.message_id

            await conn.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_send_when_not_connected(self):
        conn = FMPConnection(node_id="n1", host="localhost", port=9753)
        msg = FMPMessage.create("s1", "t1", PayloadType.HEARTBEAT, {"ok": True})
        ok = await conn.send(msg)
        assert not ok

    @pytest.mark.asyncio
    async def test_send_circuit_breaker_open(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
            ok = await conn.connect()
            assert ok

            for _ in range(10):
                conn._circuit_breaker.record_failure()
            assert conn._circuit_breaker.state == CircuitState.OPEN

            msg = FMPMessage.create("s1", "t1", PayloadType.HEARTBEAT, {"ok": True})
            ok = await conn.send(msg)
            assert not ok

            await conn.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_send_exception_sets_alive_false(self):
        conn = FMPConnection(node_id="n1", host="localhost", port=9753)
        conn._running = False
        conn.info.is_alive = True

        fake_writer = type("FakeWriter", (), {
            "is_closing": lambda self: False,
            "write": lambda self, data: None,
            "drain": lambda self: (_ for _ in ()).throw(ConnectionError("broken")),
        })()
        conn._writer = fake_writer

        msg = FMPMessage.create("s1", "t1", PayloadType.HEARTBEAT, {"ok": True})
        ok = await conn.send(msg)
        assert not ok
        assert not conn.info.is_alive

    @pytest.mark.asyncio
    async def test_connect_with_retry_success(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
            conn._reconnect_interval = 0.01
            ok = await conn.connect_with_retry(max_retries=3)
            assert ok
            await conn.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_connect_with_retry_all_fail(self):
        conn = FMPConnection(node_id="n1", host=INVALID_HOST, port=INVALID_PORT)
        conn._reconnect_interval = 0.01
        ok = await conn.connect_with_retry(max_retries=2)
        assert not ok

    @pytest.mark.asyncio
    async def test_read_loop_receives_and_callbacks(self):
        client_messages = []

        def on_msg(msg):
            client_messages.append(msg)

        msg_to_send = FMPMessage.create(
            source_id="srv", target_id="cli",
            payload_type=PayloadType.HEARTBEAT,
            payload={"ping": "pong"},
        )

        server = await asyncio.start_server(
            fmp_send_and_hold_handler([msg_to_send], hold_timeout=5.0),
            "127.0.0.1", 0,
        )
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(
                node_id="cli", host="127.0.0.1", port=port,
                on_message=on_msg,
            )
            ok = await conn.connect()
            assert ok

            for _ in range(20):
                if client_messages:
                    break
                await asyncio.sleep(0.05)

            assert len(client_messages) >= 1
            assert client_messages[0].business.payload_type == PayloadType.HEARTBEAT

            await conn.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_disconnect_writer_raises(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
            ok = await conn.connect()
            assert ok

            real_writer = conn._writer
            real_writer.close()

            await conn.disconnect()
            assert not conn.info.is_alive
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_disconnect_wait_closed_raises(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
            ok = await conn.connect()
            assert ok


            async def bad_wait_closed():
                raise OSError("broken pipe")

            conn._writer.wait_closed = bad_wait_closed
            await conn.disconnect()
            assert not conn.info.is_alive
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_auto_reconnect_on_read_loop_exit(self):
        auto_reconnect_called = False
        original_auto_reconnect = FMPConnection._auto_reconnect

        async def mock_auto_reconnect(self):
            nonlocal auto_reconnect_called
            auto_reconnect_called = True
            self._running = False

        async def handler(reader, writer):
            try:
                writer.close()
            except Exception:
                pass

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            FMPConnection._auto_reconnect = mock_auto_reconnect
            try:
                conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
                conn._reconnect_interval = 0.01
                ok = await conn.connect()
                assert ok

                await asyncio.sleep(0.3)
                assert auto_reconnect_called
            finally:
                FMPConnection._auto_reconnect = original_auto_reconnect

            await conn.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_is_connected_writer_closing(self):
        conn = FMPConnection(node_id="n1", host="localhost", port=9753)
        conn.info.is_alive = True
        assert not conn.is_connected

    @pytest.mark.asyncio
    async def test_read_loop_invalid_msg_length(self):
        auto_reconnect_called = False
        original_auto_reconnect = FMPConnection._auto_reconnect

        async def mock_auto_reconnect(self):
            nonlocal auto_reconnect_called
            auto_reconnect_called = True
            self._running = False

        async def handler(reader, writer):
            try:
                await asyncio.sleep(0.05)
                writer.write(struct.pack(">I", 0))
                await writer.drain()
                while True:
                    try:
                        data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
                        if not data:
                            break
                    except asyncio.TimeoutError:
                        continue
                    except Exception:
                        break
            except Exception:
                pass
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            FMPConnection._auto_reconnect = mock_auto_reconnect
            try:
                conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
                ok = await conn.connect()
                assert ok

                await asyncio.sleep(0.3)
                assert not conn.info.is_alive
                assert auto_reconnect_called

                await conn.disconnect()
            finally:
                FMPConnection._auto_reconnect = original_auto_reconnect
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_read_loop_oversized_msg_length(self):
        auto_reconnect_called = False
        original_auto_reconnect = FMPConnection._auto_reconnect

        async def mock_auto_reconnect(self):
            nonlocal auto_reconnect_called
            auto_reconnect_called = True
            self._running = False

        async def handler(reader, writer):
            try:
                await asyncio.sleep(0.05)
                writer.write(struct.pack(">I", 20_000_000))
                await writer.drain()
                while True:
                    try:
                        data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
                        if not data:
                            break
                    except asyncio.TimeoutError:
                        continue
                    except Exception:
                        break
            except Exception:
                pass
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            FMPConnection._auto_reconnect = mock_auto_reconnect
            try:
                conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
                ok = await conn.connect()
                assert ok

                await asyncio.sleep(0.3)
                assert not conn.info.is_alive
                assert auto_reconnect_called
                await conn.disconnect()
            finally:
                FMPConnection._auto_reconnect = original_auto_reconnect
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_read_loop_exception(self):
        auto_reconnect_called = False
        original_auto_reconnect = FMPConnection._auto_reconnect

        async def mock_auto_reconnect(self):
            nonlocal auto_reconnect_called
            auto_reconnect_called = True
            self._running = False

        async def handler(reader, writer):
            try:
                await asyncio.sleep(0.05)
                writer.write(b"\x00\x00\x00\x04@@@@@@@@")
                await writer.drain()
                while True:
                    try:
                        data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
                        if not data:
                            break
                    except asyncio.TimeoutError:
                        continue
                    except Exception:
                        break
            except Exception:
                pass
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            FMPConnection._auto_reconnect = mock_auto_reconnect
            try:
                conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
                ok = await conn.connect()
                assert ok

                await asyncio.sleep(0.3)
                assert auto_reconnect_called
                await conn.disconnect()
            finally:
                FMPConnection._auto_reconnect = original_auto_reconnect
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_send_with_crypto(self):
        key = FMPCrypto.generate_key()
        crypto = FMPCrypto(key=key)
        received = []

        server = await asyncio.start_server(fmp_echo_handler(received), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(
                node_id="n1", host="127.0.0.1", port=port,
                crypto=crypto,
            )
            ok = await conn.connect()
            assert ok

            msg = FMPMessage.create("s1", "t1", PayloadType.HEARTBEAT, {"ok": True})
            sent = await conn.send(msg)
            assert sent

            await asyncio.sleep(0.2)
            assert len(received) == 1
            assert received[0].encrypted is True

            await conn.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_auto_reconnect_actual(self):
        reconnect_attempts = 0
        original_auto_reconnect = FMPConnection._auto_reconnect

        async def tracking_auto_reconnect(self):
            nonlocal reconnect_attempts
            while self._running:
                reconnect_attempts += 1
                if await self.connect():
                    return
                self._running = False

        FMPConnection._auto_reconnect = tracking_auto_reconnect

        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            conn = FMPConnection(node_id="n1", host="127.0.0.1", port=port)
            conn._reconnect_interval = 0.01
            ok = await conn.connect()
            assert ok

            conn._writer.close()
            await asyncio.sleep(0.5)
            assert reconnect_attempts >= 1

            await conn.disconnect()
        finally:
            FMPConnection._auto_reconnect = original_auto_reconnect
            server.close()
            await server.wait_closed()


class TestFMPConnectionManager:
    def test_init(self):
        mgr = FMPConnectionManager(local_node_id="master")
        assert mgr.local_node_id == "master"

    def test_init_default_node_id(self):
        mgr = FMPConnectionManager()
        assert mgr.local_node_id.startswith("node_")

    @pytest.mark.asyncio
    async def test_add_connection_fails(self):
        original_init = FMPConnection.__init__

        def fast_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            self._reconnect_interval = 0.01

        FMPConnection.__init__ = fast_init
        try:
            mgr = FMPConnectionManager(local_node_id="master")
            conn = await mgr.add_connection("n1", INVALID_HOST, INVALID_PORT)
            assert not conn.is_connected
        finally:
            FMPConnection.__init__ = original_init

    @pytest.mark.asyncio
    async def test_add_connection_success(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            mgr = FMPConnectionManager(local_node_id="master")
            conn = await mgr.add_connection("n1", "127.0.0.1", port)
            assert conn.is_connected

            conn2 = await mgr.add_connection("n1", "127.0.0.1", port)
            assert conn2 is conn

            await mgr.close_all()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_add_connection_reconnects_dead(self):
        original_init = FMPConnection.__init__
        original_auto_reconnect = FMPConnection._auto_reconnect

        def fast_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            self._reconnect_interval = 0.01

        async def no_auto_reconnect(self):
            self._running = False

        FMPConnection.__init__ = fast_init
        FMPConnection._auto_reconnect = no_auto_reconnect

        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            mgr = FMPConnectionManager(local_node_id="master")
            conn = await mgr.add_connection("n1", "127.0.0.1", port)
            assert conn.is_connected

            await conn.disconnect()
            assert not conn.is_connected

            conn2 = await mgr.add_connection("n1", "127.0.0.1", port)
            assert conn2 is not conn
            assert conn2.is_connected

            await mgr.close_all()
        finally:
            FMPConnection.__init__ = original_init
            FMPConnection._auto_reconnect = original_auto_reconnect
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_remove_nonexistent(self):
        mgr = FMPConnectionManager(local_node_id="master")
        await mgr.remove_connection("n1")

    @pytest.mark.asyncio
    async def test_remove_existing(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            mgr = FMPConnectionManager(local_node_id="master")
            conn = await mgr.add_connection("n1", "127.0.0.1", port)
            assert conn.is_connected

            await mgr.remove_connection("n1")
            assert mgr.get_connection("n1") is None
            assert not conn.is_connected
        finally:
            server.close()
            await server.wait_closed()

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
    async def test_send_to_connected(self):
        received = []
        server = await asyncio.start_server(fmp_echo_handler(received), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            mgr = FMPConnectionManager(local_node_id="master")
            conn = await mgr.add_connection("n1", "127.0.0.1", port)
            assert conn.is_connected

            msg = FMPMessage.create("m1", "n1", PayloadType.HEARTBEAT, {"ok": True})
            ok = await mgr.send_to("n1", msg)
            assert ok

            await asyncio.sleep(0.2)
            assert len(received) == 1

            await mgr.close_all()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_broadcast(self):
        received_by = {"n1": [], "n2": []}

        server1 = await asyncio.start_server(fmp_echo_handler(received_by["n1"]), "127.0.0.1", 0)
        port1 = server1.sockets[0].getsockname()[1]
        server2 = await asyncio.start_server(fmp_echo_handler(received_by["n2"]), "127.0.0.1", 0)
        port2 = server2.sockets[0].getsockname()[1]

        try:
            mgr = FMPConnectionManager(local_node_id="master")
            await mgr.add_connection("n1", "127.0.0.1", port1)
            await mgr.add_connection("n2", "127.0.0.1", port2)

            msg = FMPMessage.create("m", "all", PayloadType.HEARTBEAT, {"broadcast": True})
            results = await mgr.broadcast(msg)
            assert "n1" in results
            assert "n2" in results
            assert results["n1"]
            assert results["n2"]

            await asyncio.sleep(0.2)
            assert len(received_by["n1"]) == 1
            assert len(received_by["n2"]) == 1

            await mgr.close_all()
        finally:
            server1.close()
            await server1.wait_closed()
            server2.close()
            await server2.wait_closed()

    @pytest.mark.asyncio
    async def test_broadcast_skips_disconnected(self):
        mgr = FMPConnectionManager(local_node_id="master")
        dead_conn = FMPConnection(node_id="dead", host=INVALID_HOST, port=INVALID_PORT)
        dead_conn._running = False
        mgr._connections["dead"] = dead_conn

        msg = FMPMessage.create("m", "all", PayloadType.HEARTBEAT, {"ok": True})
        results = await mgr.broadcast(msg)
        assert "dead" not in results

    @pytest.mark.asyncio
    async def test_close_all(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            mgr = FMPConnectionManager(local_node_id="master")
            await mgr.add_connection("n1", "127.0.0.1", port)
            await mgr.close_all()
            assert len(mgr._connections) == 0
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_stats(self):
        server = await asyncio.start_server(fmp_echo_handler(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            mgr = FMPConnectionManager(local_node_id="master")
            await mgr.add_connection("n1", "127.0.0.1", port)

            stats = mgr.get_stats()
            assert stats["local_node_id"] == "master"
            assert "n1" in stats["connections"]
            conn_stats = stats["connections"]["n1"]
            assert conn_stats["host"] == "127.0.0.1"
            assert conn_stats["port"] == port
            assert "circuit_breaker" in conn_stats
            assert "uptime" in conn_stats

            await mgr.close_all()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_with_crypto(self):
        key = FMPCrypto.generate_key()
        crypto = FMPCrypto(key=key)
        mgr = FMPConnectionManager(local_node_id="master", crypto=crypto)
        assert mgr._crypto is not None

    @pytest.mark.asyncio
    async def test_with_on_message_callback(self):
        received = []

        def on_msg(msg):
            received.append(msg)

        mgr = FMPConnectionManager(local_node_id="master", on_message=on_msg)
        assert mgr._on_message is not None

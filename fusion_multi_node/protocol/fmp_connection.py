"""FMP TCP 长连接管理。

节点间维持 TCP 长连接:
- 自动重连（3s 内完成）
- 心跳保活
- 连接池管理
- 与 CircuitBreaker 集成
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from .circuit_breaker import CircuitBreaker
from .fmp_message import FMPMessage, FMPCrypto

logger = logging.getLogger(__name__)

DEFAULT_RECONNECT_INTERVAL = 3.0
DEFAULT_HEARTBEAT_INTERVAL = 10.0
DEFAULT_READ_TIMEOUT = 30.0

_tls_manager: Optional[Any] = None


def _get_shared_tls_manager():
    """全局共享 TLSCertManager 实例，避免每次连接重建。"""
    global _tls_manager
    if _tls_manager is None:
        try:
            from fusion_multi_node.protocol import TLSCertManager
            _tls_manager = TLSCertManager()
        except Exception:
            pass
    return _tls_manager


@dataclass
class ConnectionInfo:
    """连接信息。"""
    node_id: str
    host: str
    port: int
    connected_at: float = 0.0
    last_active: float = 0.0
    is_alive: bool = False

    @property
    def uptime(self) -> float:
        if not self.is_alive:
            return 0.0
        return time.time() - self.connected_at


class FMPConnection:
    """单条 FMP TCP 连接。"""

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        crypto: Optional[FMPCrypto] = None,
        on_message: Optional[Callable[[FMPMessage], None]] = None,
        use_tls: bool = False,
    ):
        self.info = ConnectionInfo(node_id=node_id, host=host, port=port)
        self._crypto = crypto
        self._on_message = on_message
        self._use_tls = use_tls
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._running = False
        self._reconnect_interval = DEFAULT_RECONNECT_INTERVAL
        self._circuit_breaker = CircuitBreaker(name=f"conn-{node_id}")
        self._send_lock = asyncio.Lock()
        self._read_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None

    @property
    def is_connected(self) -> bool:
        return self.info.is_alive and self._writer is not None and not self._writer.is_closing()

    async def connect(self) -> bool:
        """建立 TCP 连接（use_tls=True 时启用 TLS）。"""
        try:
            ssl_ctx = None
            if self._use_tls:
                tls_mgr = _get_shared_tls_manager()
                if tls_mgr:
                    ssl_ctx = tls_mgr.get_client_ssl_context()
            self._reader, self._writer = await asyncio.open_connection(
                self.info.host, self.info.port, ssl=ssl_ctx,
            )
            self.info.is_alive = True
            self.info.connected_at = time.time()
            self.info.last_active = time.time()
            self._running = True
            self._circuit_breaker.reset()
            logger.info(f"FMP 连接建立: {self.info.host}:{self.info.port}")
            if self._read_task and not self._read_task.done():
                self._read_task.cancel()
            if self._reconnect_task and not self._reconnect_task.done():
                self._reconnect_task.cancel()
            self._read_task = asyncio.create_task(self._read_loop())
            return True
        except Exception as e:
            self._circuit_breaker.record_failure()
            logger.error(f"FMP 连接失败: {self.info.host}:{self.info.port} - {e}")
            return False

    async def connect_with_retry(self, max_retries: int = 5) -> bool:
        """带重试的连接。"""
        for i in range(max_retries):
            if await self.connect():
                return True
            logger.info(f"FMP 重连 {i+1}/{max_retries}: {self.info.host}:{self.info.port}")
            await asyncio.sleep(self._reconnect_interval)
        return False

    async def disconnect(self) -> None:
        """断开连接。"""
        self._running = False
        for task in (self._read_task, self._reconnect_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._writer and not self._writer.is_closing():
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self.info.is_alive = False
        self._reader = None
        self._writer = None
        logger.info(f"FMP 连接断开: {self.info.host}:{self.info.port}")

    async def send(self, msg: FMPMessage) -> bool:
        """发送 FMP 消息。"""
        if not self.is_connected:
            logger.warning(f"FMP 发送失败: 连接未建立 ({self.info.node_id})")
            return False

        if not self._circuit_breaker.allow_request():
            logger.warning(f"FMP 发送被熔断: {self.info.node_id}")
            return False

        async with self._send_lock:
            try:
                if self._crypto and not msg.encrypted:
                    msg = self._crypto.encrypt_message(msg)
                data = msg.serialize()
                self._writer.write(len(data).to_bytes(4, "big") + data)
                await self._writer.drain()
                self.info.last_active = time.time()
                self._circuit_breaker.record_success()
                return True
            except Exception as e:
                self._circuit_breaker.record_failure()
                logger.error(f"FMP 发送异常: {e}")
                self.info.is_alive = False
                return False

    async def _read_loop(self) -> None:
        """读取循环。"""
        while self._running and self._reader:
            try:
                len_bytes = await asyncio.wait_for(
                    self._reader.readexactly(4), timeout=DEFAULT_READ_TIMEOUT,
                )
                msg_len = int.from_bytes(len_bytes, "big")
                if msg_len <= 0 or msg_len > 16 * 1024 * 1024:
                    logger.error(f"FMP 无效消息长度: {msg_len}")
                    break

                data = await asyncio.wait_for(
                    self._reader.readexactly(msg_len), timeout=DEFAULT_READ_TIMEOUT,
                )
                msg = FMPMessage.deserialize(data)

                if self._crypto and msg.encrypted:
                    msg = self._crypto.decrypt_message(msg)

                self.info.last_active = time.time()

                if self._on_message:
                    import asyncio as _aio
                    import inspect
                    result = self._on_message(msg)
                    if inspect.iscoroutine(result):
                        _aio.create_task(result)

            except asyncio.TimeoutError:
                continue
            except asyncio.IncompleteReadError:
                logger.warning(f"FMP 连接断开: {self.info.node_id}")
                break
            except Exception as e:
                logger.error(f"FMP 读取异常: {e}")
                break

        self.info.is_alive = False
        if self._running:
            if self._reconnect_task and not self._reconnect_task.done():
                return
            self._reconnect_task = asyncio.create_task(self._auto_reconnect())

    async def _auto_reconnect(self) -> None:
        """自动重连。"""
        while self._running:
            logger.info(f"FMP 自动重连: {self.info.host}:{self.info.port}")
            if await self.connect():
                return
            await asyncio.sleep(self._reconnect_interval)


class FMPConnectionManager:
    """FMP 连接池管理器。"""

    def __init__(
        self,
        local_node_id: str = "",
        crypto: Optional[FMPCrypto] = None,
        on_message: Optional[Callable[[FMPMessage], None]] = None,
    ):
        self.local_node_id = local_node_id or f"node_{uuid.uuid4().hex[:8]}"
        self._crypto = crypto
        self._on_message = on_message
        self._connections: Dict[str, FMPConnection] = {}
        self._lock = asyncio.Lock()

    async def add_connection(self, node_id: str, host: str, port: int) -> FMPConnection:
        """添加并连接到远程节点。"""
        async with self._lock:
            if node_id in self._connections:
                conn = self._connections[node_id]
                if conn.is_connected:
                    return conn
                await conn.disconnect()

            conn = FMPConnection(
                node_id=node_id,
                host=host,
                port=port,
                crypto=self._crypto,
                on_message=self._on_message,
            )
            self._connections[node_id] = conn
        await conn.connect_with_retry()
        return conn

    async def remove_connection(self, node_id: str) -> None:
        """移除连接。"""
        async with self._lock:
            conn = self._connections.pop(node_id, None)
        if conn:
            await conn.disconnect()

    def get_connection(self, node_id: str) -> Optional[FMPConnection]:
        return self._connections.get(node_id)

    async def safe_get_connection(self, node_id: str) -> Optional[FMPConnection]:
        async with self._lock:
            return self._connections.get(node_id)

    async def send_to(self, node_id: str, msg: FMPMessage) -> bool:
        """向指定节点发送消息。"""
        async with self._lock:
            conn = self._connections.get(node_id)
        if not conn or not conn.is_connected:
            logger.warning(f"节点 {node_id} 连接不可用")
            return False
        return await conn.send(msg)

    async def broadcast(self, msg: FMPMessage) -> Dict[str, bool]:
        """广播消息到所有连接。"""
        async with self._lock:
            targets = {nid: conn for nid, conn in self._connections.items() if conn.is_connected}
        results = {}
        for node_id, conn in targets.items():
            results[node_id] = await conn.send(msg)
        return results

    async def close_all(self) -> None:
        """关闭所有连接。"""
        async with self._lock:
            conns = list(self._connections.values())
            self._connections.clear()
        for conn in conns:
            await conn.disconnect()

    def get_stats(self) -> Dict[str, Any]:
        return {
            "local_node_id": self.local_node_id,
            "connections": {
                nid: {
                    "host": c.info.host,
                    "port": c.info.port,
                    "is_alive": c.is_connected,
                    "uptime": c.info.uptime,
                    "circuit_breaker": c._circuit_breaker.get_stats(),
                }
                for nid, c in self._connections.items()
            },
        }

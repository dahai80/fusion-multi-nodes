"""FMP TCP 服务器 — 接受入站 FMP 连接，按 PayloadType 分发消息。

每个节点启动时运行 FMPServer 监听指定端口，接受其他节点的
FMPConnection 连入，并将收到的消息分发到已注册的 handler。
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

from ..utils.auth import is_safe_path_segment
from .fmp_message import FMPCrypto, FMPMessage, PayloadType

logger = logging.getLogger(__name__)

DEFAULT_FMP_PORT = 11446
MAX_INBOUND_CONNECTIONS = 64


class FMPServer:
    """FMP TCP 服务器 — 监听端口，接受入站连接并分发消息。"""

    def __init__(
        self,
        node_id: str = "",
        host: str = "127.0.0.1",
        port: int = DEFAULT_FMP_PORT,
        crypto: FMPCrypto | None = None,
    ):
        self.node_id = node_id or f"server_{uuid.uuid4().hex[:8]}"
        self.host = host
        self.port = port
        self._crypto = crypto
        self._server: asyncio.AbstractServer | None = None
        self._running = False
        self._handlers: dict[PayloadType, Callable[[FMPMessage], Any]] = {}
        self._default_handler: Callable[[FMPMessage], Any] | None = None
        self._peers: dict[str, _InboundPeer] = {}
        self._lock = asyncio.Lock()
        self._msg_counter = 0
        self._fmp_sender: Callable | None = None

    def register_handler(
        self,
        payload_type: PayloadType,
        handler: Callable[[FMPMessage], Any],
    ) -> None:
        """注册消息处理器。handler 可以是 sync 或 async 函数。"""
        self._handlers[payload_type] = handler
        logger.info(f"FMP handler 注册: {payload_type.value}")

    def set_default_handler(self, handler: Callable[[FMPMessage], Any]) -> None:
        """设置默认处理器 — 处理无匹配 handler 的消息。"""
        self._default_handler = handler

    async def start(self) -> bool:
        """启动 TCP 服务器。"""
        if self._running:
            logger.warning("FMPServer 已在运行")
            return True
        try:
            self._server = await asyncio.start_server(
                self._on_connect,
                self.host,
                self.port,
            )
            self._running = True
            logger.info(f"FMPServer 启动: {self.host}:{self.port} node={self.node_id}")
            return True
        except Exception as e:
            logger.error(f"FMPServer 启动失败: {e}")
            return False

    async def stop(self) -> None:
        """停止服务器，关闭所有入站连接。"""
        self._running = False
        async with self._lock:
            peers = list(self._peers.values())
            self._peers.clear()
        for peer in peers:
            await peer.close()
        if self._server:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None
        logger.info("FMPServer 已停止")

    async def _on_connect(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """入站连接回调 — 为每个连接创建 _InboundPeer。"""
        if len(self._peers) >= MAX_INBOUND_CONNECTIONS:
            logger.warning(f"入站连接超限 ({MAX_INBOUND_CONNECTIONS})，拒绝新连接")
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return

        peer_id = f"in_{uuid.uuid4().hex[:8]}"
        peer = _InboundPeer(
            peer_id=peer_id,
            reader=reader,
            writer=writer,
            crypto=self._crypto,
            on_message=self._dispatch,
        )
        async with self._lock:
            self._peers[peer_id] = peer
        logger.info(f"入站连接: {peer_id} from {writer.get_extra_info('peername')}")

        try:
            await peer.read_loop()
        except Exception as e:
            logger.debug(f"入站连接异常: {peer_id} - {e}")
        finally:
            async with self._lock:
                self._peers.pop(peer_id, None)
            logger.info(f"入站连接断开: {peer_id}")

    async def _dispatch(self, msg: FMPMessage) -> None:
        """按 PayloadType 分发消息到注册的 handler。"""
        self._msg_counter += 1
        ptype = msg.business.payload_type
        handler = self._handlers.get(ptype, self._default_handler)
        if not handler:
            logger.debug(f"无 handler: {ptype.value} msg={msg.message_id}")
            return
        try:
            import asyncio as _aio
            import inspect

            result = handler(msg)
            if inspect.iscoroutine(result):
                _aio.create_task(result)
        except Exception as e:
            logger.error(f"handler 异常: {ptype.value} - {e}")

    def get_stats(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "running": self._running,
            "peers": len(self._peers),
            "msg_counter": self._msg_counter,
            "handlers": [pt.value for pt in self._handlers],
        }

    def register_shard_handler(self, storage_volume: Any = None) -> None:
        """注册分片同步处理器 — 收到 SHARD_SYNC 后写入本地存储卷。"""

        def _on_shard_sync(msg: FMPMessage) -> None:
            try:
                import base64
                import hashlib
                import os

                payload = msg.business.payload_as_json()
                shard_id = payload["shard_id"]
                volume_name = payload.get("volume_name", "models")
                file_path = payload["file_path"]
                data = base64.b64decode(payload["data_b64"])
                checksum = payload.get("checksum", "")

                if not is_safe_path_segment(shard_id):
                    logger.error(f"SHARD_SYNC 非法 shard_id: {shard_id!r}")
                    return
                if not is_safe_path_segment(volume_name):
                    logger.error(f"SHARD_SYNC 非法 volume_name: {volume_name!r}")
                    return
                # file_path 允许多段相对路径, 但禁绝对路径/穿越/盘符/空字节
                if not file_path or "\x00" in file_path:
                    logger.error(f"SHARD_SYNC 非法 file_path: {file_path!r}")
                    return
                if file_path.startswith("/") or (":" in file_path.split("/")[0] and "\\" not in file_path):
                    logger.error(f"SHARD_SYNC 拒绝对/盘符路径: {file_path!r}")
                    return
                norm = os.path.normpath(file_path)
                if norm.startswith("..") or "/.." in norm or norm == "..":
                    logger.error(f"SHARD_SYNC 路径穿越被拒: {file_path!r}")
                    return
                for seg in norm.split("/"):
                    if not is_safe_path_segment(seg):
                        logger.error(f"SHARD_SYNC 路径段非法: {seg!r} (in {file_path!r})")
                        return
                file_path = norm

                if checksum and hashlib.sha256(data).hexdigest() != checksum:
                    logger.error(f"SHARD_SYNC 校验失败: {shard_id}")
                    return

                if storage_volume is not None:
                    ok = storage_volume.write_file(volume_name, file_path, data)
                    if ok:
                        logger.info(f"SHARD_SYNC 写入成功: {shard_id} → {volume_name}/{file_path}")
                    else:
                        logger.error(f"SHARD_SYNC 写入失败: {shard_id}")
                else:
                    logger.warning(f"SHARD_SYNC 无存储卷，分片 {shard_id} 丢弃")

            except Exception as e:
                logger.error(f"SHARD_SYNC 处理异常: {e}")

        self.register_handler(PayloadType.SHARD_SYNC, _on_shard_sync)

    def register_data_sync_handler(
        self,
        on_ast_sync: Any = None,
        on_text_sync: Any = None,
    ) -> None:
        """注册 DATA_SYNC 处理器 — 收到 AST差分+脱敏 消息后还原并回调。"""

        def _on_data_sync(msg: FMPMessage) -> None:
            try:
                from ..security.secure_transfer import SecureTransferPipeline

                payload = msg.business.payload_as_json()
                pipeline = SecureTransferPipeline()
                transfer_type = payload.get("type", "")
                if transfer_type == "ast_diff_scrubbed":
                    base_ast = payload.get("base_ast", {})
                    result = pipeline.apply_transfer(base_ast, payload)
                    if on_ast_sync:
                        on_ast_sync(result, msg.link.source_id)
                    logger.info(f"DATA_SYNC AST还原完成 from={msg.link.source_id}")
                elif transfer_type == "text_scrubbed":
                    text = payload.get("text", "")
                    if on_text_sync:
                        on_text_sync(text, msg.link.source_id)
                else:
                    logger.warning(f"DATA_SYNC 未知类型: {transfer_type}")
            except Exception as e:
                logger.error(f"DATA_SYNC 处理异常: {e}")

        self.register_handler(PayloadType.DATA_SYNC, _on_data_sync)

    def register_kv_handler(self, kv_store: Any = None) -> None:
        """注册 KV 存储处理器 — 收到 KV_GET/KV_PUT 后操作本地 KVStore。"""

        def _on_kv_get(msg: FMPMessage) -> None:
            if not self._fmp_sender or not kv_store:
                return
            try:
                payload = msg.business.payload_as_json()
                key = payload["key"]
                partition = payload.get("partition", "default")
                entry = kv_store.get_entry(key, partition)
                resp_msg = FMPMessage.create(
                    source_id=self.node_id,
                    target_id=msg.link.source_id,
                    payload_type=PayloadType.KV_GET_RESP,
                    payload={
                        "request_id": payload.get("request_id", ""),
                        "key": key,
                        "found": entry is not None,
                        "value": entry.value if entry else None,
                        "partition": partition,
                    },
                )
                asyncio.ensure_future(self._fmp_sender(msg.link.source_id, resp_msg))
            except Exception as e:
                logger.error(f"KV_GET 处理异常: {e}")

        def _on_kv_put(msg: FMPMessage) -> None:
            if not self._fmp_sender or not kv_store:
                return
            try:
                payload = msg.business.payload_as_json()
                key = payload["key"]
                value = payload["value"]
                partition = payload.get("partition", "default")
                ttl = payload.get("ttl")
                kv_store.put(key, value, partition=partition, ttl_seconds=ttl)
                ack_msg = FMPMessage.create(
                    source_id=self.node_id,
                    target_id=msg.link.source_id,
                    payload_type=PayloadType.KV_PUT_ACK,
                    payload={
                        "request_id": payload.get("request_id", ""),
                        "key": key,
                        "success": True,
                        "partition": partition,
                    },
                )
                asyncio.ensure_future(self._fmp_sender(msg.link.source_id, ack_msg))
            except Exception as e:
                logger.error(f"KV_PUT 处理异常: {e}")

        self.register_handler(PayloadType.KV_GET, _on_kv_get)
        self.register_handler(PayloadType.KV_PUT, _on_kv_put)

    def set_fmp_sender(self, sender: Callable) -> None:
        """设置消息发送回调 — 用于 handler 回复消息。"""
        self._fmp_sender = sender


class _InboundPeer:
    """入站连接 — 从 TCP 流读取 FMP 消息并回调。"""

    def __init__(
        self,
        peer_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        crypto: FMPCrypto | None = None,
        on_message: Callable[[FMPMessage], Any] | None = None,
    ):
        self.peer_id = peer_id
        self._reader = reader
        self._writer = writer
        self._crypto = crypto
        self._on_message = on_message
        self.connected_at = time.time()
        self.last_active = time.time()
        self._alive = True

    @property
    def is_alive(self) -> bool:
        return self._alive and not self._writer.is_closing()

    async def read_loop(self) -> None:
        """持续读取 FMP 消息帧。"""
        while self._alive:
            try:
                len_bytes = await asyncio.wait_for(
                    self._reader.readexactly(4),
                    timeout=30.0,
                )
                msg_len = int.from_bytes(len_bytes, "big")
                if msg_len <= 0 or msg_len > 16 * 1024 * 1024:
                    logger.error(f"入站帧长度无效: {msg_len}")
                    break

                data = await asyncio.wait_for(
                    self._reader.readexactly(msg_len),
                    timeout=30.0,
                )
                msg = FMPMessage.deserialize(data)

                if self._crypto and msg.encrypted:
                    msg = self._crypto.decrypt_message(msg)

                self.last_active = time.time()

                if self._on_message:
                    self._on_message(msg)

            except TimeoutError:
                continue
            except asyncio.IncompleteReadError:
                break
            except Exception as e:
                logger.debug(f"入站读取异常: {self.peer_id} - {e}")
                break

        self._alive = False

    async def close(self) -> None:
        self._alive = False
        if not self._writer.is_closing():
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass

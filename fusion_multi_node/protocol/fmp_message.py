"""FMP 三层协议消息定义 + AES-GCM 加密。

三层结构:
- LinkLayer: 链路层（路由、hop_count、源/目标）
- BusinessLayer: 业务层（payload 类型、序列化数据）
- ControlLayer: 控制层（心跳、ACK、流控）

加密封装:
- AES-GCM 全报文加密
- 支持预共享密钥模式
"""

from __future__ import annotations

import json
import logging
import os
import struct
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

FMP_MAGIC = b"\x46\x4D\x50\x01"
FMP_HEADER_SIZE = 12
MAX_HOP_COUNT = 3
MAX_ROUNDS = 10
FMP_VERSION = 1
FMP_MAX_PAYLOAD_SIZE = 16 * 1024 * 1024


class PayloadType(Enum):
    HEARTBEAT = "heartbeat"
    REGISTER = "register"
    TASK_ASSIGN = "task_assign"
    TASK_RESULT = "task_result"
    KV_TRANSFER = "kv_transfer"
    KV_LOOKUP = "kv_lookup"
    FAULT_REPORT = "fault_report"
    CHAT_COMPLETION = "chat_completion"
    EMBEDDING = "embedding"
    CONTROL = "control"
    ACK = "ack"
    NACK = "nack"


class ControlType(Enum):
    HEARTBEAT = "heartbeat"
    ACK = "ack"
    NACK = "nack"
    FLOW_CONTROL = "flow_control"
    DISCONNECT = "disconnect"


@dataclass
class FMPLinkLayer:
    """链路层 — 路由与跳数控制。"""
    source_id: str
    target_id: str
    hop_count: int = 0
    max_hops: int = MAX_HOP_COUNT
    trace: list = field(default_factory=list)

    def can_forward(self) -> bool:
        return self.hop_count < self.max_hops

    def forward(self, next_node: str) -> None:
        if not self.can_forward():
            raise ValueError(f"hop_count={self.hop_count} 已达上限 {self.max_hops}")
        self.hop_count += 1
        self.trace.append(next_node)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "hop_count": self.hop_count,
            "max_hops": self.max_hops,
            "trace": self.trace,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FMPLinkLayer:
        return cls(
            source_id=d["source_id"],
            target_id=d["target_id"],
            hop_count=d.get("hop_count", 0),
            max_hops=d.get("max_hops", MAX_HOP_COUNT),
            trace=d.get("trace", []),
        )


@dataclass
class FMPBusinessLayer:
    """业务层 — payload 类型与序列化数据。"""
    payload_type: PayloadType
    payload: bytes
    round_id: str = ""
    round_number: int = 0
    max_rounds: int = MAX_ROUNDS

    def can_next_round(self) -> bool:
        return self.round_number < self.max_rounds

    def next_round(self) -> None:
        if not self.can_next_round():
            raise ValueError(f"round_number={self.round_number} 已达上限 {self.max_rounds}")
        self.round_number += 1

    def payload_as_json(self) -> Any:
        return json.loads(self.payload.decode("utf-8"))

    @classmethod
    def from_json_payload(cls, payload_type: PayloadType, data: Any, **kwargs) -> FMPBusinessLayer:
        return cls(
            payload_type=payload_type,
            payload=json.dumps(data).encode("utf-8"),
            **kwargs,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "payload_type": self.payload_type.value,
            "payload": self.payload.decode("utf-8", errors="replace"),
            "round_id": self.round_id,
            "round_number": self.round_number,
            "max_rounds": self.max_rounds,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FMPBusinessLayer:
        return cls(
            payload_type=PayloadType(d["payload_type"]),
            payload=d["payload"].encode("utf-8") if isinstance(d["payload"], str) else d["payload"],
            round_id=d.get("round_id", ""),
            round_number=d.get("round_number", 0),
            max_rounds=d.get("max_rounds", MAX_ROUNDS),
        )


@dataclass
class FMPControlLayer:
    """控制层 — 心跳、ACK、流控。"""
    control_type: ControlType
    sequence: int = 0
    timestamp: float = 0.0
    ack_seq: int = 0
    flow_window: int = 64

    def to_dict(self) -> Dict[str, Any]:
        return {
            "control_type": self.control_type.value,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "ack_seq": self.ack_seq,
            "flow_window": self.flow_window,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FMPControlLayer:
        return cls(
            control_type=ControlType(d["control_type"]),
            sequence=d.get("sequence", 0),
            timestamp=d.get("timestamp", 0.0),
            ack_seq=d.get("ack_seq", 0),
            flow_window=d.get("flow_window", 64),
        )


@dataclass
class FMPMessage:
    """FMP 完整消息 — 三层封装。"""
    message_id: str
    link: FMPLinkLayer
    business: FMPBusinessLayer
    control: FMPControlLayer
    encrypted: bool = False

    @classmethod
    def create(
        cls,
        source_id: str,
        target_id: str,
        payload_type: PayloadType,
        payload: Any,
        control_type: ControlType = ControlType.HEARTBEAT,
        round_id: str = "",
    ) -> FMPMessage:
        msg_id = f"fmp_{uuid.uuid4().hex[:12]}"
        link = FMPLinkLayer(source_id=source_id, target_id=target_id)
        if isinstance(payload, bytes):
            raw = payload
        else:
            raw = json.dumps(payload).encode("utf-8")
        business = FMPBusinessLayer(
            payload_type=payload_type,
            payload=raw,
            round_id=round_id or msg_id,
        )
        control = FMPControlLayer(
            control_type=control_type,
            sequence=uuid.uuid4().int >> 96,
            timestamp=time.time(),
        )
        return cls(message_id=msg_id, link=link, business=business, control=control)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "link": self.link.to_dict(),
            "business": self.business.to_dict(),
            "control": self.control.to_dict(),
            "encrypted": self.encrypted,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FMPMessage:
        return cls(
            message_id=d["message_id"],
            link=FMPLinkLayer.from_dict(d["link"]),
            business=FMPBusinessLayer.from_dict(d["business"]),
            control=FMPControlLayer.from_dict(d["control"]),
            encrypted=d.get("encrypted", False),
        )

    def serialize(self, use_msgpack: bool = False) -> bytes:
        """序列化为二进制帧: [MAGIC][VERSION][FLAGS][PAYLOAD_LEN][RESERVED][PAYLOAD]

        FLAGS: bit0=encrypted, bit1=msgpack(1)/json(0)
        """
        payload_dict = self.to_dict()
        flags = 0x01 if self.encrypted else 0x00

        if use_msgpack:
            try:
                import msgpack
                payload_bytes = msgpack.packb(payload_dict, use_bin_type=True)
                flags |= 0x02
            except ImportError:
                logger.debug("msgpack 未安装，回退 JSON 序列化")
                payload_bytes = json.dumps(payload_dict).encode("utf-8")
        else:
            payload_bytes = json.dumps(payload_dict).encode("utf-8")

        header = struct.pack(
            "!4sBBIH",
            FMP_MAGIC,
            FMP_VERSION,
            flags,
            len(payload_bytes),
            0,
        )
        return header + payload_bytes

    @classmethod
    def deserialize(cls, data: bytes) -> FMPMessage:
        """从二进制帧反序列化。"""
        if len(data) < FMP_HEADER_SIZE:
            raise ValueError(f"数据过短: {len(data)} < {FMP_HEADER_SIZE}")

        magic, version, flags, payload_len, _ = struct.unpack("!4sBBIH", data[:FMP_HEADER_SIZE])
        if magic != FMP_MAGIC:
            raise ValueError(f"无效 MAGIC: {magic}")
        if version != FMP_VERSION:
            raise ValueError(f"版本不匹配: {version}")
        if payload_len > FMP_MAX_PAYLOAD_SIZE:
            raise ValueError(f"payload 超限: {payload_len} > {FMP_MAX_PAYLOAD_SIZE}")

        expected_total = FMP_HEADER_SIZE + payload_len
        if len(data) < expected_total:
            raise ValueError(
                f"数据不足: 声明 {payload_len} 字节, 实际 {len(data) - FMP_HEADER_SIZE} 字节"
            )
        if len(data) > expected_total:
            logger.warning(f"帧含尾部数据: {len(data) - expected_total} 字节将被忽略")

        payload_bytes = data[FMP_HEADER_SIZE:expected_total]
        is_msgpack = bool(flags & 0x02)

        if is_msgpack:
            try:
                import msgpack
                d = msgpack.unpackb(payload_bytes, raw=False)
            except ImportError:
                raise ValueError("收到 msgpack 帧，但 msgpack 未安装")
        else:
            d = json.loads(payload_bytes.decode("utf-8"))

        d["encrypted"] = bool(flags & 0x01)
        return cls.from_dict(d)


class FMPCrypto:
    """AES-GCM 加密器。"""

    def __init__(self, key: Optional[bytes] = None):
        if key and len(key) != 32:
            raise ValueError("AES-256-GCM 需要 32 字节密钥")
        self._key = key
        self._aesgcm = None
        if key:
            try:
                from cryptography.hazmat.primitives.ciphers.aead import AESGCM
                self._aesgcm = AESGCM(key)
            except ImportError:
                pass

    @classmethod
    def generate_key(cls) -> bytes:
        return os.urandom(32)

    def encrypt(self, plaintext: bytes, aad: Optional[bytes] = None) -> bytes:
        if not self._aesgcm:
            raise RuntimeError("FMPCrypto: 加密密钥未设置，拒绝明文传输")
        nonce = os.urandom(12)
        ciphertext = self._aesgcm.encrypt(nonce, plaintext, aad)
        return nonce + ciphertext

    def decrypt(self, data: bytes, aad: Optional[bytes] = None) -> bytes:
        if not self._aesgcm:
            raise RuntimeError("FMPCrypto: 解密密钥未设置，拒绝明文处理")
        nonce = data[:12]
        ciphertext = data[12:]
        return self._aesgcm.decrypt(nonce, ciphertext, aad)

    def encrypt_message(self, msg: FMPMessage) -> FMPMessage:
        raw_payload = msg.business.payload
        aad = f"{msg.link.source_id}:{msg.link.target_id}".encode("utf-8")
        encrypted_payload = self.encrypt(raw_payload, aad=aad)
        encrypted_business = FMPBusinessLayer(
            payload_type=msg.business.payload_type,
            payload=encrypted_payload,
            round_id=msg.business.round_id,
            round_number=msg.business.round_number,
            max_rounds=msg.business.max_rounds,
        )
        return FMPMessage(
            message_id=msg.message_id,
            link=msg.link,
            business=encrypted_business,
            control=msg.control,
            encrypted=True,
        )

    def decrypt_message(self, msg: FMPMessage) -> FMPMessage:
        if not msg.encrypted:
            return msg
        encrypted_payload = msg.business.payload
        aad = f"{msg.link.source_id}:{msg.link.target_id}".encode("utf-8")
        decrypted_payload = self.decrypt(encrypted_payload, aad=aad)
        msg.business.payload = decrypted_payload
        msg.encrypted = False
        return msg

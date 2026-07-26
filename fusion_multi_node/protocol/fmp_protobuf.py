"""FMP Protobuf 三层协议定义。

三层结构:
- Envelope: 信封层（路由、hop_count、源/目标、序列号）
- Control: 控制层（心跳、ACK、流控、断连）
- Payload: 业务负载层（序列化数据 + 类型标记）

当 protobuf 不可用时自动降级到 JSON/msgpack 二进制帧。
"""

from __future__ import annotations

import logging
import struct
import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

FMP_PROTO_MAGIC = b"\x46\x4D\x50\x02"
FMP_PROTO_HEADER_SIZE = 12
FMP_PROTO_VERSION = 2
FMP_PROTO_MAX_PAYLOAD = 16 * 1024 * 1024

FLAG_ENCRYPTED = 0x01
FLAG_MSGPACK = 0x02
FLAG_PROTOBUF = 0x04
FLAG_COMPRESSED = 0x08


class PayloadType(IntEnum):
    HEARTBEAT = 0
    REGISTER = 1
    TASK_ASSIGN = 2
    TASK_RESULT = 3
    TASK_CANCEL = 4
    KV_TRANSFER = 5
    KV_LOOKUP = 6
    FAULT_REPORT = 7
    CHAT_COMPLETION = 8
    EMBEDDING = 9
    CONTROL = 10
    ACK = 11
    NACK = 12
    ELECTION = 13
    ELECTION_VOTE = 14
    STATE_SYNC = 15
    APPROVAL_REQUEST = 16
    APPROVAL_RESPONSE = 17
    DEGRADE_TASK = 18
    CLOUD_FALLBACK = 19
    DATA_SYNC = 24


class ControlCode(IntEnum):
    HEARTBEAT = 0
    ACK = 1
    NACK = 2
    FLOW_CONTROL = 3
    DISCONNECT = 4
    ELECTION_START = 5
    ELECTION_VOTE = 6
    STATE_SYNC_REQ = 7
    STATE_SYNC_RESP = 8


@dataclass
class FMPEnvelope:
    """信封层 — 路由与跳数控制。"""
    source_id: str
    target_id: str
    hop_count: int = 0
    max_hops: int = 3
    trace: List[str] = field(default_factory=list)
    message_id: str = ""
    timestamp: float = 0.0
    sequence: int = 0

    def __post_init__(self):
        if not self.message_id:
            self.message_id = f"env_{uuid.uuid4().hex[:12]}"
        if not self.timestamp:
            self.timestamp = time.time()

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
            "message_id": self.message_id,
            "timestamp": self.timestamp,
            "sequence": self.sequence,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FMPEnvelope:
        return cls(
            source_id=d["source_id"],
            target_id=d["target_id"],
            hop_count=d.get("hop_count", 0),
            max_hops=d.get("max_hops", 3),
            trace=d.get("trace", []),
            message_id=d.get("message_id", ""),
            timestamp=d.get("timestamp", 0.0),
            sequence=d.get("sequence", 0),
        )


@dataclass
class FMPControl:
    """控制层 — 心跳、ACK、流控。"""
    code: ControlCode
    sequence: int = 0
    timestamp: float = 0.0
    ack_seq: int = 0
    flow_window: int = 64
    reason: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code.value,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "ack_seq": self.ack_seq,
            "flow_window": self.flow_window,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FMPControl:
        return cls(
            code=ControlCode(d["code"]),
            sequence=d.get("sequence", 0),
            timestamp=d.get("timestamp", 0.0),
            ack_seq=d.get("ack_seq", 0),
            flow_window=d.get("flow_window", 64),
            reason=d.get("reason", ""),
        )


@dataclass
class FMPPayload:
    """业务负载层 — 类型标记 + 序列化数据。"""
    payload_type: PayloadType
    data: bytes
    round_id: str = ""
    round_number: int = 0
    max_rounds: int = 10
    compressed: bool = False

    def can_next_round(self) -> bool:
        return self.round_number < self.max_rounds

    def next_round(self) -> None:
        if not self.can_next_round():
            raise ValueError(f"round_number={self.round_number} 已达上限 {self.max_rounds}")
        self.round_number += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "payload_type": self.payload_type.value,
            "data": self.data.decode("utf-8", errors="replace"),
            "round_id": self.round_id,
            "round_number": self.round_number,
            "max_rounds": self.max_rounds,
            "compressed": self.compressed,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FMPPayload:
        raw = d["data"]
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        return cls(
            payload_type=PayloadType(d["payload_type"]),
            data=raw,
            round_id=d.get("round_id", ""),
            round_number=d.get("round_number", 0),
            max_rounds=d.get("max_rounds", 10),
            compressed=d.get("compressed", False),
        )


@dataclass
class FMPProtoMessage:
    """Protobuf 三层协议完整消息。"""
    envelope: FMPEnvelope
    control: FMPControl
    payload: FMPPayload
    encrypted: bool = False

    @classmethod
    def create(
        cls,
        source_id: str,
        target_id: str,
        payload_type: PayloadType,
        data: Any,
        control_code: ControlCode = ControlCode.HEARTBEAT,
        round_id: str = "",
    ) -> FMPProtoMessage:
        import json
        envelope = FMPEnvelope(source_id=source_id, target_id=target_id)
        if isinstance(data, bytes):
            raw = data
        else:
            raw = json.dumps(data).encode("utf-8")
        payload = FMPPayload(
            payload_type=payload_type,
            data=raw,
            round_id=round_id or envelope.message_id,
        )
        control = FMPControl(
            code=control_code,
            sequence=uuid.uuid4().int >> 96,
        )
        return cls(envelope=envelope, control=control, payload=payload)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "envelope": self.envelope.to_dict(),
            "control": self.control.to_dict(),
            "payload": self.payload.to_dict(),
            "encrypted": self.encrypted,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FMPProtoMessage:
        return cls(
            envelope=FMPEnvelope.from_dict(d["envelope"]),
            control=FMPControl.from_dict(d["control"]),
            payload=FMPPayload.from_dict(d["payload"]),
            encrypted=d.get("encrypted", False),
        )

    def serialize(self) -> bytes:
        import json
        payload_dict = self.to_dict()
        flags = FLAG_ENCRYPTED if self.encrypted else 0x00

        proto_data = _try_protobuf_encode(payload_dict)
        if proto_data is not None:
            flags |= FLAG_PROTOBUF
            payload_bytes = proto_data
        else:
            try:
                import msgpack
                payload_bytes = msgpack.packb(payload_dict, use_bin_type=True)
                flags |= FLAG_MSGPACK
            except ImportError:
                payload_bytes = json.dumps(payload_dict).encode("utf-8")

        header = struct.pack(
            "!4sBBIH",
            FMP_PROTO_MAGIC,
            FMP_PROTO_VERSION,
            flags,
            len(payload_bytes),
            0,
        )
        return header + payload_bytes

    @classmethod
    def deserialize(cls, data: bytes) -> FMPProtoMessage:
        if len(data) < FMP_PROTO_HEADER_SIZE:
            raise ValueError(f"数据过短: {len(data)} < {FMP_PROTO_HEADER_SIZE}")

        magic, version, flags, payload_len, _ = struct.unpack(
            "!4sBBIH", data[:FMP_PROTO_HEADER_SIZE]
        )
        if magic != FMP_PROTO_MAGIC:
            raise ValueError(f"无效 MAGIC: {magic}")
        if version != FMP_PROTO_VERSION:
            raise ValueError(f"版本不匹配: {version}")
        if payload_len > FMP_PROTO_MAX_PAYLOAD:
            raise ValueError(f"payload 超限: {payload_len}")

        expected = FMP_PROTO_HEADER_SIZE + payload_len
        if len(data) < expected:
            raise ValueError(f"数据不足: 声明 {payload_len}, 实际 {len(data) - FMP_PROTO_HEADER_SIZE}")

        payload_bytes = data[FMP_PROTO_HEADER_SIZE:expected]
        is_proto = bool(flags & FLAG_PROTOBUF)
        is_msgpack = bool(flags & FLAG_MSGPACK)

        if is_proto:
            d = _try_protobuf_decode(payload_bytes)
            if d is None:
                raise ValueError("Protobuf 解码失败")
        elif is_msgpack:
            try:
                import msgpack
                d = msgpack.unpackb(payload_bytes, raw=False)
            except ImportError:
                raise ValueError("收到 msgpack 帧但 msgpack 未安装")
        else:
            import json
            d = json.loads(payload_bytes.decode("utf-8"))

        d["encrypted"] = bool(flags & FLAG_ENCRYPTED)
        return cls.from_dict(d)


def _try_protobuf_encode(payload_dict: Dict[str, Any]) -> Optional[bytes]:
    """将消息字典编码为结构化 protobuf — 使用 FMPEnvelope/FMPControl/FMPPayload 字段。"""
    try:
        from fusion_multi_node.protocol import fmp_proto_pb2
        import json

        frame = fmp_proto_pb2.FMPFrame()

        env_data = payload_dict.get("envelope", {})
        env = frame.envelope
        env.source_id = env_data.get("source_id", "")
        env.target_id = env_data.get("target_id", "")
        env.hop_count = env_data.get("hop_count", 0)
        env.max_hops = env_data.get("max_hops", 3)
        for t in env_data.get("trace", []):
            env.trace.append(t)
        env.message_id = env_data.get("message_id", "")
        env.timestamp = env_data.get("timestamp", 0.0)
        env.sequence = env_data.get("sequence", 0)

        ctrl_data = payload_dict.get("control", {})
        ctrl = frame.control
        ctrl.code = ctrl_data.get("code", 0)
        ctrl.sequence = ctrl_data.get("sequence", 0)
        ctrl.timestamp = ctrl_data.get("timestamp", 0.0)
        ctrl.ack_seq = ctrl_data.get("ack_seq", 0)
        ctrl.flow_window = ctrl_data.get("flow_window", 64)
        ctrl.reason = ctrl_data.get("reason", "")

        pay_data = payload_dict.get("payload", {})
        pay = frame.payload
        pay.payload_type = pay_data.get("payload_type", 0)
        raw = pay_data.get("data", "")
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        pay.data = raw
        pay.round_id = pay_data.get("round_id", "")
        pay.round_number = pay_data.get("round_number", 0)
        pay.max_rounds = pay_data.get("max_rounds", 10)
        pay.compressed = pay_data.get("compressed", False)

        frame.encrypted = payload_dict.get("encrypted", False)
        frame.payload_json = ""

        return frame.SerializeToString()
    except ImportError:
        return None
    except Exception as e:
        logger.debug(f"Protobuf 编码失败: {e}")
        return None


def _try_protobuf_decode(data: bytes) -> Optional[Dict[str, Any]]:
    """从 protobuf 二进制解码为消息字典 — 使用结构化字段。"""
    try:
        from fusion_multi_node.protocol import fmp_proto_pb2
        import json

        frame = fmp_proto_pb2.FMPFrame()
        frame.ParseFromString(data)

        env = frame.envelope
        ctrl = frame.control
        pay = frame.payload

        result = {
            "envelope": {
                "source_id": env.source_id,
                "target_id": env.target_id,
                "hop_count": env.hop_count,
                "max_hops": env.max_hops,
                "trace": list(env.trace),
                "message_id": env.message_id,
                "timestamp": env.timestamp,
                "sequence": env.sequence,
            },
            "control": {
                "code": ctrl.code,
                "sequence": ctrl.sequence,
                "timestamp": ctrl.timestamp,
                "ack_seq": ctrl.ack_seq,
                "flow_window": ctrl.flow_window,
                "reason": ctrl.reason,
            },
            "payload": {
                "payload_type": pay.payload_type,
                "data": pay.data.decode("utf-8", errors="replace"),
                "round_id": pay.round_id,
                "round_number": pay.round_number,
                "max_rounds": pay.max_rounds,
                "compressed": pay.compressed,
            },
            "encrypted": frame.encrypted,
        }

        if frame.payload_json and not pay.data:
            return json.loads(frame.payload_json)

        return result
    except ImportError:
        return None
    except Exception as e:
        logger.debug(f"Protobuf 解码失败: {e}")
        return None

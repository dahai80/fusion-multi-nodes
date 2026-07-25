"""FMP 协议模块测试。"""

import json
import time

import pytest

from fusion_multi_node.protocol.fmp_message import (
    FMPMessage, FMPLinkLayer, FMPBusinessLayer, FMPControlLayer,
    FMPCrypto, PayloadType, ControlType,
    FMP_MAGIC, FMP_VERSION, MAX_HOP_COUNT, MAX_ROUNDS,
)
from fusion_multi_node.protocol.circuit_breaker import CircuitBreaker, CircuitState


class TestFMPLinkLayer:
    def test_basic(self):
        link = FMPLinkLayer(source_id="m1", target_id="n1")
        assert link.hop_count == 0
        assert link.max_hops == MAX_HOP_COUNT
        assert link.can_forward()

    def test_forward(self):
        link = FMPLinkLayer(source_id="m1", target_id="n1")
        link.forward("n2")
        assert link.hop_count == 1
        assert "n2" in link.trace

    def test_max_hops(self):
        link = FMPLinkLayer(source_id="m1", target_id="n1", max_hops=2)
        link.forward("n2")
        link.forward("n3")
        assert not link.can_forward()
        with pytest.raises(ValueError):
            link.forward("n4")

    def test_serialization(self):
        link = FMPLinkLayer(source_id="m1", target_id="n1")
        d = link.to_dict()
        link2 = FMPLinkLayer.from_dict(d)
        assert link2.source_id == "m1"
        assert link2.target_id == "n1"
        assert link2.hop_count == 0


class TestFMPBusinessLayer:
    def test_json_payload(self):
        biz = FMPBusinessLayer.from_json_payload(
            PayloadType.HEARTBEAT, {"status": "ok"},
        )
        data = biz.payload_as_json()
        assert data["status"] == "ok"

    def test_round_control(self):
        biz = FMPBusinessLayer(
            payload_type=PayloadType.CHAT_COMPLETION,
            payload=b"test",
            max_rounds=3,
        )
        assert biz.can_next_round()
        biz.next_round()
        biz.next_round()
        biz.next_round()
        assert not biz.can_next_round()
        with pytest.raises(ValueError):
            biz.next_round()

    def test_serialization(self):
        biz = FMPBusinessLayer.from_json_payload(
            PayloadType.TASK_ASSIGN, {"task_id": "t1"},
            round_id="r1", round_number=2,
        )
        d = biz.to_dict()
        biz2 = FMPBusinessLayer.from_dict(d)
        assert biz2.payload_type == PayloadType.TASK_ASSIGN
        assert biz2.round_id == "r1"
        assert biz2.round_number == 2


class TestFMPControlLayer:
    def test_basic(self):
        ctrl = FMPControlLayer(control_type=ControlType.ACK, sequence=42)
        assert ctrl.control_type == ControlType.ACK
        assert ctrl.sequence == 42

    def test_serialization(self):
        ctrl = FMPControlLayer(control_type=ControlType.FLOW_CONTROL, flow_window=32)
        d = ctrl.to_dict()
        ctrl2 = FMPControlLayer.from_dict(d)
        assert ctrl2.flow_window == 32


class TestFMPMessage:
    def test_create(self):
        msg = FMPMessage.create(
            source_id="master", target_id="node1",
            payload_type=PayloadType.HEARTBEAT,
            payload={"ts": 12345},
        )
        assert msg.link.source_id == "master"
        assert msg.link.target_id == "node1"
        assert msg.business.payload_type == PayloadType.HEARTBEAT
        assert not msg.encrypted

    def test_serialize_deserialize(self):
        msg = FMPMessage.create(
            source_id="m1", target_id="n1",
            payload_type=PayloadType.TASK_RESULT,
            payload={"result": "done"},
        )
        data = msg.serialize()
        assert data[:4] == FMP_MAGIC

        msg2 = FMPMessage.deserialize(data)
        assert msg2.message_id == msg.message_id
        assert msg2.link.source_id == "m1"
        assert msg2.business.payload_type == PayloadType.TASK_RESULT

    def test_round_trip_with_forward(self):
        msg = FMPMessage.create(
            source_id="m1", target_id="n3",
            payload_type=PayloadType.CHAT_COMPLETION,
            payload={"prompt": "hello"},
        )
        msg.link.forward("n2")
        data = msg.serialize()
        msg2 = FMPMessage.deserialize(data)
        assert msg2.link.hop_count == 1
        assert "n2" in msg2.link.trace

    def test_deserialize_invalid_magic(self):
        with pytest.raises(ValueError, match="无效 MAGIC"):
            FMPMessage.deserialize(b"\x00\x00\x00\x00" + b"\x00" * 20)


class TestFMPCrypto:
    def test_no_key_passthrough(self):
        crypto = FMPCrypto()
        plain = b"hello"
        assert crypto.encrypt(plain) == plain
        assert crypto.decrypt(plain) == plain

    def test_generate_key(self):
        key = FMPCrypto.generate_key()
        assert len(key) == 32

    def test_encrypt_decrypt_message(self):
        key = FMPCrypto.generate_key()
        crypto = FMPCrypto(key=key)
        msg = FMPMessage.create(
            source_id="m1", target_id="n1",
            payload_type=PayloadType.HEARTBEAT,
            payload={"status": "ok"},
        )
        original = msg.business.payload
        crypto.encrypt_message(msg)
        assert msg.encrypted
        crypto.decrypt_message(msg)
        assert not msg.encrypted
        assert msg.business.payload == original

    def test_invalid_key_length(self):
        with pytest.raises(ValueError):
            FMPCrypto(key=b"short")


class TestCircuitBreaker:
    def test_initial_state(self):
        cb = CircuitBreaker(name="test")
        assert cb.is_closed
        assert not cb.is_open

    def test_trips_on_failures(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_closed
        cb.record_failure()
        assert cb.is_open

    def test_blocks_when_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=10.0)
        cb.record_failure()
        cb.record_failure()
        assert not cb.allow_request()

    def test_half_open_after_timeout(self):
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=0.01)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open
        time.sleep(0.02)
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.allow_request()

    def test_closes_on_success_in_half_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=0.01)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.02)
        assert cb.state == CircuitState.HALF_OPEN
        cb.record_success()
        assert cb.is_closed

    def test_reopens_on_failure_in_half_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=0.01)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.02)
        _ = cb.state  # trigger HALF_OPEN
        cb.record_failure()
        assert cb.is_open

    def test_reset(self):
        cb = CircuitBreaker(name="test", failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        cb.reset()
        assert cb.is_closed

    def test_stats(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.allow_request()
        cb.record_failure()
        stats = cb.get_stats()
        assert stats["name"] == "test"
        assert stats["total_calls"] == 1
        assert stats["total_failures"] == 1

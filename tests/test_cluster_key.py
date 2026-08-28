"""issue #52 原语 — cluster_key HKDF 派生 + MAC 助手单元测试。

覆盖: 3 派生密钥 32 字节 / 域分离互异 / 确定性 / 不同 token 异 /
mac hex / verify 接受-拒篡改-拒空 / canonical 排序无空白中文不转义。
纯内存无 server 无 IO。
"""

from __future__ import annotations

from fusion_multi_node.security.cluster_key import (
    canonical_json,
    derive_audit_chain_key,
    derive_confirm_relay_key,
    derive_rule_epoch_key,
    mac_payload,
    verify_mac,
)


class TestKeyDerivation:
    def test_three_keys_32_bytes(self):
        tok = "cluster-secret-x"
        for k in (
            derive_audit_chain_key(tok),
            derive_rule_epoch_key(tok),
            derive_confirm_relay_key(tok),
        ):
            assert isinstance(k, bytes)
            assert len(k) == 32

    def test_domain_separation_distinct(self):
        tok = "cluster-secret-x"
        a = derive_audit_chain_key(tok)
        r = derive_rule_epoch_key(tok)
        c = derive_confirm_relay_key(tok)
        assert a != r != c != a, "域分离标签须产互异密钥"

    def test_deterministic_same_token(self):
        tok = "cluster-secret-x"
        assert derive_audit_chain_key(tok) == derive_audit_chain_key(tok)

    def test_different_token_different_key(self):
        assert derive_audit_chain_key("t1") != derive_audit_chain_key("t2")


class TestMacPayload:
    def test_mac_hex_string(self):
        key = derive_audit_chain_key("tok")
        mac = mac_payload(key, canonical_json({"a": 1}))
        assert isinstance(mac, str)
        assert len(mac) == 64, "SHA256 hex = 64 字符"
        int(mac, 16)  # 合法 hex

    def test_verify_accepts_valid(self):
        key = derive_confirm_relay_key("tok")
        canon = canonical_json({"confirm_id": "c1", "node": "n1"})
        mac = mac_payload(key, canon)
        assert verify_mac(key, canon, mac) is True

    def test_verify_rejects_tampered(self):
        key = derive_confirm_relay_key("tok")
        canon = canonical_json({"confirm_id": "c1"})
        mac = mac_payload(key, canon)
        bad_canon = canonical_json({"confirm_id": "c2"})
        assert verify_mac(key, bad_canon, mac) is False

    def test_verify_rejects_empty_mac(self):
        key = derive_audit_chain_key("tok")
        assert verify_mac(key, b"{}", "") is False
        assert verify_mac(key, b"{}", None) is False

    def test_verify_accepts_bytes_mac(self):
        # FMP/protobuf 路径可能传 bytes mac — 统一规整不抛 TypeError。
        key = derive_audit_chain_key("tok")
        canon = canonical_json({"x": 1})
        mac = mac_payload(key, canon).encode("ascii")
        assert verify_mac(key, canon, mac) is True


class TestCanonicalJson:
    def test_sorted_keys(self):
        out = canonical_json({"b": 2, "a": 1})
        assert out == b'{"a":1,"b":2}'

    def test_no_whitespace(self):
        out = canonical_json({"a": [1, 2], "b": {"c": 3}})
        assert b" " not in out

    def test_chinese_not_escaped(self):
        out = canonical_json({"actor": "管理员"})
        assert "管理员".encode() in out, "ensure_ascii=False 须保留中文"

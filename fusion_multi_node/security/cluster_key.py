"""集群 MAC 密钥派生 — 从 cluster_token 经 HKDF-SHA256 派生各原语独立 MAC 密钥。

issue #52 契约: 多节点暴露跨节点 TRANSPORT 原语供 fusion-guard 消费。
MAC 密钥不新增独立秘密 — cluster_token 已是集群成员根信源 (各节点同载, env 注入,
滚动重叠)。派生密钥 MAC 传递证明集群成员身份 (identity propagation 契约)。

域分离 info 标签 → 各原语独立密钥 (HKDF 输出独立性: 单标签泄露不扩散)。
轮换 cluster_token → 派生密钥同步轮换, guard 重新基线 (issue #52 明确)。

公开:
  derive_audit_chain_key / derive_rule_epoch_key / derive_confirm_relay_key — 3 派生密钥
  mac_payload / verify_mac — HMAC-SHA256 签名/验签 (常量时间)
  canonical_json — 规范 JSON (键排序 + 无空白 + ensure_ascii=False)
  post_confirm — agent/guard → master /api/confirm POST 助手
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging

logger = logging.getLogger(__name__)

# 域分离标签 — v1 后缀便于未来无冲突升级 (v2 标签 → 新密钥空间, 旧 MAC 失效)。
_AUDIT_CHAIN_INFO = b"fusion-multinode-audit-chain-v1"
_RULE_EPOCH_INFO = b"fusion-multinode-rule-epoch-v1"
_CONFIRM_RELAY_INFO = b"fusion-multinode-confirm-relay-v1"

_KEY_LEN = 32  # SHA256 → 32 字节


def _hkdf_derive(secret: str, info: bytes) -> bytes:
    """HKDF-SHA256 派生 — 复用 key_exchange.py 范式 (cryptography 库)。"""
    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    except ImportError as e:
        raise RuntimeError("HKDF 派生需要 cryptography 库") from e
    hkdf = HKDF(algorithm=hashes.SHA256(), length=_KEY_LEN, salt=None, info=info)
    return hkdf.derive(secret.encode("utf-8"))


def derive_audit_chain_key(cluster_token: str) -> bytes:
    return _hkdf_derive(cluster_token, _AUDIT_CHAIN_INFO)


def derive_rule_epoch_key(cluster_token: str) -> bytes:
    return _hkdf_derive(cluster_token, _RULE_EPOCH_INFO)


def derive_confirm_relay_key(cluster_token: str) -> bytes:
    return _hkdf_derive(cluster_token, _CONFIRM_RELAY_INFO)


def mac_payload(key: bytes, canonical: bytes) -> str:
    """HMAC-SHA256 → hex 字符串 (记录/响应携带, 常量时间比较在 verify_mac)。"""
    return hmac.new(key, canonical, hashlib.sha256).hexdigest()


def canonical_json(payload: dict) -> bytes:
    """规范 JSON — 键排序 + 无空白 + ensure_ascii=False (中文不转义)。

    签名输入须确定性: 键序/空白/转义不一致 → MAC 不匹配。guard 与多节点须同算法。
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def verify_mac(key: bytes, canonical: bytes, mac_hex) -> bool:
    """常量时间 MAC 校验 — 空串/缺字段 → False (不抛)。

    mac_hex 接受 str (hexdigest) 或 bytes (ASCII hex) — 调用方 JSON 反序列化后
    通常为 str, 但 FMP/protobuf 路径可能传 bytes, 统一规整避免 TypeError。
    """
    if not mac_hex:
        return False
    if isinstance(mac_hex, bytes):
        mac_hex = mac_hex.decode("ascii", errors="replace")
    expected = mac_payload(key, canonical)
    return hmac.compare_digest(expected, mac_hex)


async def post_confirm(
    master_host: str,
    master_port: int,
    cluster_token: str,
    *,
    confirm_id: str,
    node_id: str,
    action: str,
    epoch: int,
    ts: str,
) -> dict:
    """agent/guard → master /api/confirm POST 助手 — 构 MAC, 发, 返 ack。

    guard 编排调用 (符合层边界 — 多节点仅 TRANSPORT+KEY SCHEME, guard 实现消费)。
    agent 不自动 POST; guard 持 master 地址 + cluster_token 触发。
    """
    import httpx

    from fusion_multi_node.utils.auth import build_safe_url

    key = derive_confirm_relay_key(cluster_token)
    payload = {
        "confirm_id": confirm_id,
        "node_id": node_id,
        "action": action,
        "epoch": epoch,
        "ts": ts,
    }
    payload["mac"] = mac_payload(key, canonical_json(payload))
    url = build_safe_url("http", master_host, master_port, "/api/confirm")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, json=payload, headers={"Authorization": f"Bearer {cluster_token}"})
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"confirm POST 到 master HTTP {resp.status_code}: {resp.text[:200]}")
            return {"status": "error", "code": resp.status_code}
    except Exception as e:
        logger.warning(f"confirm POST 到 master 异常: {e}")
        return {"status": "error", "reason": str(e)}

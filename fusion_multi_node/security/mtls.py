"""集群 mTLS — 私有 CA + 节点叶证书, 集群内 HTTP 双向认证。

env 开关 (默认关, 不破坏现有 http + ASGITransport 测试):
  FUSION_MTLS_ENABLED=1            开启 mTLS (server 要求客户端证书, client 带证书)
  FUSION_MTLS_CA_CERT=<path>       集群 CA 证书 PEM (节点共享同一 CA)
  FUSION_MTLS_NODE_CERT=<path>     本节点叶证书 PEM
  FUSION_MTLS_NODE_KEY=<path>      本节点叶私钥 PEM
  FUSION_MTLS_NODE_ID=<id>         本节点 id (写入证书 CN)
  FUSION_MTLS_NODE_ROLE=<role>     本节点角色 master|worker (写入证书 O)

设计:
- 私有 CA 自签 (provision_cluster 一次性生成 ca.crt/ca.key)。
- 每节点叶证书由 CA 签发, CN=node_id, O=role。
- server ssl_context: 加载本节点叶证书, verify_mode=CERT_REQUIRED, 信任 CA →
  无证书客户端 TLS 握手失败 (uvicorn 层拒绝, 到不了 ASGI)。
- client ssl_context: 加载本节点叶证书, 校验对端证书链到 CA。
- 角色绑定: uvicorn 不把对端证书暴露进 ASGI scope (仅 client host:port),
  故运行时角色校验经 X-Node-Id header + PermissionManager 注册角色, 非证书 O 直读。
  传输层仍只放行 CA 签名节点 (非集群节点连不上)。
"""

from __future__ import annotations

import ipaddress
import logging
import os
import ssl
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_DIR = Path.home() / ".fusion" / "multi-node" / "tls"
# v0.14.0 item 4: 不再 import 时一次性缓存 — lazy 读 env, 供 configure_from_config 运行时翻默认。
# 旧 _ENABLED import-time 缓存导致 config 段 enabled=True 不生效 (silent security gap, Rule 12)。
_TRUTHY = ("1", "true", "yes")


def is_enabled() -> bool:
    """mTLS 是否开启 — lazy 读 env FUSION_MTLS_ENABLED (config 桥经 configure_from_config 写 env)。"""
    return os.environ.get("FUSION_MTLS_ENABLED", "").lower() in _TRUTHY


def configure_from_config(cfg: Any) -> None:
    """v0.14.0 item 4: config 段 → env 桥。CLI/服务器启动时 (config 加载后, uvicorn 前) 调。

    把 security.mtls 配置段写回 env (env 优先: 已设的非空 env 不覆盖, 兼容旧 env-only 部署)。
    写入后 is_enabled() 即时反映 (lazy, 无 import-time 缓存问题)。fail-closed 不变: enabled=True
    但证书路径不全 → server_ssl_kwargs/client_kwargs raise (不回退明文)。
    """
    mtls = cfg.get_mtls_config() if hasattr(cfg, "get_mtls_config") else {}
    if not mtls:
        return
    if mtls.get("enabled"):
        # 仅当 env 未设时写 (env 优先, 不覆盖显式 env 部署)。
        if not os.environ.get("FUSION_MTLS_ENABLED"):
            os.environ["FUSION_MTLS_ENABLED"] = "1"
    # 证书路径: config 非空且 env 未设时写回 (env 优先)。
    for cfg_key, env_name in (
        ("ca_cert", "FUSION_MTLS_CA_CERT"),
        ("node_cert", "FUSION_MTLS_NODE_CERT"),
        ("node_key", "FUSION_MTLS_NODE_KEY"),
        ("node_id", "FUSION_MTLS_NODE_ID"),
        ("node_role", "FUSION_MTLS_NODE_ROLE"),
    ):
        val = mtls.get(cfg_key, "")
        if val and not os.environ.get(env_name):
            os.environ[env_name] = str(val)
    logger.info(f"mTLS config 桥已应用: enabled={is_enabled()}")


def _env_path(name: str) -> str | None:
    v = os.environ.get(name)
    return v if v else None


def certs_available() -> bool:
    """mTLS 开启时三证书路径是否齐全 (CA + 本节点 cert + key)。

    GAP-2 修复 (复审计 2026-08-26): 旧实现证书不全时静默回退明文 (fail-open),
    默认部署零节点身份校验。现改 fail-closed — 开启但证书不全直接 raise, 不回退明文。
    """
    if not is_enabled():
        return True
    ca = _env_path("FUSION_MTLS_CA_CERT")
    cert = _env_path("FUSION_MTLS_NODE_CERT")
    key = _env_path("FUSION_MTLS_NODE_KEY")
    return bool(ca and cert and key)


def _require_certs() -> tuple[str, str, str]:
    """mTLS 开启时取证书路径, 不全则 raise (fail-closed, 不回退明文)。"""
    ca = _env_path("FUSION_MTLS_CA_CERT")
    cert = _env_path("FUSION_MTLS_NODE_CERT")
    key = _env_path("FUSION_MTLS_NODE_KEY")
    if not (ca and cert and key):
        raise RuntimeError(
            "mTLS 已开启 (FUSION_MTLS_ENABLED=1) 但证书路径不全 "
            "(FUSION_MTLS_CA_CERT/NODE_CERT/NODE_KEY); fail-closed 拒绝回退明文。"
            " 设全三路径或关闭 FUSION_MTLS_ENABLED。"
        )
    return ca, cert, key


def _build_ssl_context(verify_ca: str | None, cert: str | None, key: str | None) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if verify_ca else ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20")
    if cert and key:
        ctx.load_cert_chain(cert, key)
    return ctx


def server_ssl_context() -> ssl.SSLContext | None:
    if not is_enabled():
        return None
    ca, cert, key = _require_certs()
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20")
    ctx.load_cert_chain(cert, key)
    ctx.load_verify_locations(ca)
    ctx.verify_mode = ssl.CERT_REQUIRED
    logger.info(f"mTLS server 上下文就绪: 要求客户端证书, CA={ca}")
    return ctx


def client_ssl_context() -> ssl.SSLContext | None:
    if not is_enabled():
        return None
    ca, cert, key = _require_certs()
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20")
    ctx.load_cert_chain(cert, key)
    ctx.load_verify_locations(ca)
    # P2-1 (审计 §3.4): 叶证书现带 SAN (provision_node 传 ip 后 DNSName+IPAddress),
    # 故开 hostname 校验 — 防 MITM 改连非集群节点 (旧 check_hostname=False 放弃对端身份校验)。
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    logger.info(f"mTLS client 上下文就绪: 带本节点证书, 校验对端 CA={ca} (hostname 校验开)")
    return ctx


def client_kwargs() -> dict:
    """httpx.AsyncClient mTLS 参数 — 关时返回空 dict (调用方 **展开, 不改默认行为)。

    httpx verify 接受 ssl.SSLContext: 既校验对端证书链, 又用本 ctx 内 loaded cert_chain
    作客户端证书。故 verify=ctx 一参数同时满足双向认证。开启但证书不全 → raise (fail-closed)。
    """
    ctx = client_ssl_context()
    if ctx is None:
        return {}
    return {"verify": ctx}


def server_ssl_kwargs() -> dict:
    """uvicorn.Config mTLS 参数 — 关时返回空 dict。

    uvicorn.Config 不接受 ssl_context 对象, 取个体化 ssl_* 参数:
    ssl_certfile/ssl_keyfile (本节点叶证书), ssl_ca_certs (集群 CA),
    ssl_cert_reqs=CERT_REQUIRED (要求客户端证书)。开启但证书不全 → raise (fail-closed)。
    """
    if not is_enabled():
        return {}
    ca, cert, key = _require_certs()
    return {
        "ssl_certfile": cert,
        "ssl_keyfile": key,
        "ssl_ca_certs": ca,
        "ssl_cert_reqs": ssl.CERT_REQUIRED,
        "ssl_version": ssl.PROTOCOL_TLS_SERVER,
    }


def scheme() -> str:
    """集群内 URL 协议 — mTLS 开时 https, 否则 http。"""
    return "https" if is_enabled() else "http"


# ── 证书供给 (provision_cluster / provision_node) ──


def provision_cluster(ca_dir: str | None = None) -> tuple[str, str]:
    """生成集群私有 CA (ca.crt + ca.key)。一次性, 各节点共享 ca.crt。"""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    d = Path(ca_dir) if ca_dir else _DEFAULT_DIR
    d.mkdir(parents=True, exist_ok=True)
    ca_crt = d / "ca.crt"
    ca_key = d / "ca.key"
    if ca_crt.exists() and ca_key.exists():
        logger.info(f"集群 CA 已存在: {ca_crt}")
        return str(ca_crt), str(ca_key)

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "fusion-multi-node-cluster-ca"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "FusionCluster"),
        ]
    )
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(key, hashes.SHA256())
    )
    ca_key.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    ca_crt.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    os.chmod(ca_key, 0o600)
    os.chmod(ca_crt, 0o644)
    logger.info(f"集群私有 CA 已生成: {ca_crt}")
    return str(ca_crt), str(ca_key)


def provision_node(
    node_id: str,
    role: str,
    ca_cert: str,
    ca_key: str,
    out_dir: str | None = None,
    ip: str | None = None,
) -> tuple[str, str]:
    """用集群 CA 签发节点叶证书 (CN=node_id, O=role)。返回 (cert_path, key_path)。

    P2-1 (审计 §3.4): ip 非空时叶证书加 SubjectAlternativeName — DNSName(node_id) +
    IPAddress(ip), 使 client_ssl_context 可开 check_hostname=True (校验对端身份, 防 MITM)。
    旧叶证书无 SAN → check_hostname 被迫关 → 对端身份零校验。CA (provision_cluster) 无需 SAN。
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    d = Path(out_dir) if out_dir else _DEFAULT_DIR / node_id
    d.mkdir(parents=True, exist_ok=True)
    cert_path = d / "node.crt"
    key_path = d / "node.key"
    if cert_path.exists() and key_path.exists():
        return str(cert_path), str(key_path)

    ca_cert_obj = x509.load_pem_x509_certificate(Path(ca_cert).read_bytes())
    ca_key_obj = serialization.load_pem_private_key(Path(ca_key).read_bytes(), password=None)
    ca_name = ca_cert_obj.subject

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, node_id),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, role),
        ]
    )
    # P2-1: SAN 扩展 — DNSName(node_id) + IPAddress(ip) (ip 可解析时), critical=False。
    san_list = [x509.DNSName(node_id)]
    if ip:
        try:
            san_list.append(x509.IPAddress(ipaddress.ip_address(ip)))
        except ValueError:
            logger.warning(f"provision_node ip={ip!r} 非合法 IP, 仅写 DNSName SAN (无 IP SAN)")
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName(san_list), critical=False)
        .sign(ca_key_obj, hashes.SHA256())
    )
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    os.chmod(key_path, 0o600)
    os.chmod(cert_path, 0o644)
    logger.info(f"节点叶证书已签发: node_id={node_id} role={role} ip={ip} SAN={len(san_list)} → {cert_path}")
    return str(cert_path), str(key_path)

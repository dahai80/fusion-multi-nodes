"""ECDH 密钥交换 + TLS 自签名证书工具。

提供：
- ECDH 密钥交换: 节点间协商 AES-256 会话密钥
- TLS 自签名证书: 节点间加密通信
- 证书指纹 pinning: 集群内节点信任验证
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ECDHKeyExchange:
    """ECDH 密钥交换 — 协商 AES-256 会话密钥。"""

    def __init__(self):
        self._private_key: bytes | None = None
        self._public_key: bytes | None = None

    def generate_keypair(self) -> bytes:
        try:
            from cryptography.hazmat.primitives.asymmetric.x25519 import (
                X25519PrivateKey,
            )
        except ImportError:
            raise RuntimeError("ECDH 需要 cryptography 库")
        self._private_key = X25519PrivateKey.generate()
        self._public_key = self._private_key.public_key().public_bytes_raw()
        return self._public_key

    def compute_shared_secret(self, peer_public_key: bytes) -> bytes:
        if not self._private_key:
            raise RuntimeError("请先调用 generate_keypair()")
        try:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PublicKey
            from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        except ImportError:
            raise RuntimeError("ECDH 需要 cryptography 库")
        peer_pk = X25519PublicKey.from_public_bytes(peer_public_key)
        shared = self._private_key.exchange(peer_pk)
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b"fusion-multi-node-ecdh",
        )
        session_key = hkdf.derive(shared)
        logger.info("ECDH 会话密钥协商完成")
        return session_key

    @property
    def public_key(self) -> bytes | None:
        return self._public_key


class TLSCertManager:
    """TLS 自签名证书管理 — 生成节点间通信证书 + 指纹 pinning 信任。"""

    def __init__(self, cert_dir: str | None = None):
        self._cert_dir = Path(cert_dir) if cert_dir else Path.home() / ".fusion" / "multi-node" / "tls"
        self._cert_path = self._cert_dir / "node.crt"
        self._key_path = self._cert_dir / "node.key"
        self._ssl_context: Any | None = None
        self._client_ssl_context: Any | None = None
        self._pinned_fingerprints: set[str] = set()
        self._cert_fingerprint: str | None = None

    def ensure_certificates(self) -> tuple[str, str]:
        if self._cert_path.exists() and self._key_path.exists():
            return str(self._cert_path), str(self._key_path)
        return self._generate_self_signed()

    def _generate_self_signed(self) -> tuple[str, str]:
        try:
            from cryptography import x509
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.x509.oid import NameOID
        except ImportError:
            raise RuntimeError("TLS 证书生成需要 cryptography 库")

        self._cert_dir.mkdir(parents=True, exist_ok=True)

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, "fusion-multi-node"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "FusionCluster"),
            ]
        )
        now = datetime.now(UTC)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + timedelta(days=365))
            .add_extension(
                x509.SubjectAlternativeName(
                    [
                        x509.DNSName("localhost"),
                        x509.IPAddress(self._get_local_ip()),
                    ]
                ),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )

        key_pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)

        self._key_path.write_bytes(key_pem)
        self._cert_path.write_bytes(cert_pem)
        os.chmod(self._key_path, 0o600)
        os.chmod(self._cert_path, 0o644)

        # 清除缓存，重新加载
        self._ssl_context = None
        self._client_ssl_context = None

        logger.info(f"TLS 自签名证书已生成: {self._cert_dir}")
        return str(self._cert_path), str(self._key_path)

    def get_cert_fingerprint(self) -> str:
        """获取本节点证书 SHA-256 指纹。"""
        if self._cert_fingerprint:
            return self._cert_fingerprint
        cert_path, _ = self.ensure_certificates()
        try:
            from cryptography import x509
            from cryptography.hazmat.primitives import hashes

            cert_der = Path(cert_path).read_bytes()
            cert = x509.load_pem_x509_certificate(cert_der)
            fingerprint = cert.fingerprint(hashes.SHA256()).hex()
            self._cert_fingerprint = fingerprint
            return fingerprint
        except Exception as e:
            logger.error(f"获取证书指纹失败: {e}")
            return ""

    def pin_fingerprint(self, fingerprint: str) -> None:
        """添加受信任的节点证书指纹。"""
        self._pinned_fingerprints.add(fingerprint)
        self._client_ssl_context = None
        logger.info(f"证书指纹已 pin: {fingerprint[:16]}...")

    def _verify_pinned_cert(self, cert_der: bytes) -> bool:
        """验证对端证书是否在 pinned 指纹列表中。"""
        if not self._pinned_fingerprints:
            logger.warning("无 pinned 指纹，跳过证书验证")
            return True
        try:
            from cryptography import x509
            from cryptography.hazmat.primitives import hashes

            cert = x509.load_der_x509_certificate(cert_der)
            fp = cert.fingerprint(hashes.SHA256()).hex()
            if fp in self._pinned_fingerprints:
                return True
            logger.warning(f"证书指纹不在信任列表: {fp[:16]}...")
            return False
        except Exception as e:
            logger.error(f"证书验证异常: {e}")
            return False

    @staticmethod
    def _get_local_ip():
        import ipaddress
        import socket

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ipaddress.IPv4Address(ip)
        except Exception:
            return ipaddress.IPv4Address("127.0.0.1")

    def get_ssl_context(self):
        if self._ssl_context is not None:
            return self._ssl_context
        cert_path, key_path = self.ensure_certificates()
        import ssl

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert_path, key_path)
        ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20")
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        self._ssl_context = ctx
        return ctx

    def get_client_ssl_context(self):
        if self._client_ssl_context is not None:
            return self._client_ssl_context
        cert_path, key_path = self.ensure_certificates()
        import ssl

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_cert_chain(cert_path, key_path)
        ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20")
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        if not self._pinned_fingerprints:
            # 无 pinned 指纹时 fail-closed: 禁止返回可被 MITM 的"自签信任"上下文。
            # 调用方须先 pin_fingerprint(对端证书指纹) 再建立连接。
            raise RuntimeError(
                "TLS 客户端上下文要求 pinned 指纹: 先调用 pin_fingerprint() 配置对端证书指纹，"
                "禁止无 pin 的自签名信任回退 (MITM 风险)"
            )
        # 有 pinned 指纹: check_hostname=False (自签证书无对端域名匹配),
        # 但用 CERT_REQUIRED + DER 指纹回调做真实身份校验, 不信任任何 CA。
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        pinned = self._pinned_fingerprints

        def _verify_cb(conn, cert_der, errno, errdepth, ok):
            # cert_der 为对端证书 DER 二进制, 计算 SHA-256 与 pinned 比对
            fp = hashlib.sha256(cert_der).hexdigest()
            if fp in pinned:
                return True
            logger.warning(f"TLS 对端证书不在 pinned 列表: {fp[:16]}...")
            return False

        ctx.set_verify(ssl.VERIFY_PEER, _verify_cb)
        self._client_ssl_context = ctx
        logger.info(f"TLS 客户端上下文就绪: pinned={len(self._pinned_fingerprints)} 指纹")
        return ctx

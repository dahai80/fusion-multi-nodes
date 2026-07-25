"""ECDH 密钥交换 + TLS 自签名证书工具。

提供：
- ECDH 密钥交换: 节点间协商 AES-256 会话密钥
- TLS 自签名证书: 节点间加密通信
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class ECDHKeyExchange:
    """ECDH 密钥交换 — 协商 AES-256 会话密钥。"""

    def __init__(self):
        self._private_key: Optional[bytes] = None
        self._public_key: Optional[bytes] = None

    def generate_keypair(self) -> bytes:
        try:
            from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
        except ImportError:
            raise RuntimeError("ECDH 需要 cryptography 库")
        self._private_key = X25519PrivateKey.generate()
        self._public_key = self._private_key.public_key().public_bytes_raw()
        return self._public_key

    def compute_shared_secret(self, peer_public_key: bytes) -> bytes:
        if not self._private_key:
            raise RuntimeError("请先调用 generate_keypair()")
        try:
            from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
            from cryptography.hazmat.primitives import hashes
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
    def public_key(self) -> Optional[bytes]:
        return self._public_key


class TLSCertManager:
    """TLS 自签名证书管理 — 生成节点间通信证书。"""

    def __init__(self, cert_dir: Optional[str] = None):
        self._cert_dir = Path(cert_dir) if cert_dir else Path.home() / ".fusion" / "multi-node" / "tls"
        self._cert_path = self._cert_dir / "node.crt"
        self._key_path = self._cert_dir / "node.key"

    def ensure_certificates(self) -> Tuple[str, str]:
        if self._cert_path.exists() and self._key_path.exists():
            return str(self._cert_path), str(self._key_path)
        return self._generate_self_signed()

    def _generate_self_signed(self) -> Tuple[str, str]:
        try:
            from cryptography import x509
            from cryptography.x509.oid import NameOID
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            import datetime
        except ImportError:
            raise RuntimeError("TLS 证书生成需要 cryptography 库")

        self._cert_dir.mkdir(parents=True, exist_ok=True)

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "fusion-multi-node"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "FusionCluster"),
        ])
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.utcnow())
            .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName("localhost"),
                    x509.IPAddress(self._get_local_ip()),
                ]),
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

        logger.info(f"TLS 自签名证书已生成: {self._cert_dir}")
        return str(self._cert_path), str(self._key_path)

    @staticmethod
    def _get_local_ip():
        import socket
        import ipaddress
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ipaddress.IPv4Address(ip)
        except Exception:
            return ipaddress.IPv4Address("127.0.0.1")

    def get_ssl_context(self):
        cert_path, key_path = self.ensure_certificates()
        import ssl
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert_path, key_path)
        ctx.set_ciphers("ECDHE+AESGCM")
        return ctx

    def get_client_ssl_context(self):
        cert_path, key_path = self.ensure_certificates()
        import ssl
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_cert_chain(cert_path, key_path)
        ctx.load_verify_locations(cert_path)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.set_ciphers("ECDHE+AESGCM")
        return ctx

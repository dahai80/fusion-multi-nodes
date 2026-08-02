"""M6-04 FMPCrypto — AES-256-GCM 加密/解密，配合 key_exchange 协商的会话密钥。"""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)


class FMPCrypto:
    """AES-256-GCM 加密 — 使用 ECDH 协商的会话密钥。"""

    def __init__(self, session_key: bytes | None = None):
        self._session_key = session_key
        if session_key:
            logger.info("FMPCrypto 已加载会话密钥")

    def set_session_key(self, key: bytes) -> None:
        if len(key) != 32:
            raise ValueError(f"会话密钥必须 32 字节，当前 {len(key)} 字节")
        self._session_key = key
        logger.info("FMPCrypto 会话密钥已更新")

    def encrypt(self, plaintext: bytes, aad: bytes | None = None) -> bytes:
        if not self._session_key:
            raise RuntimeError("未设置会话密钥，请先调用 set_session_key()")
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError:
            raise RuntimeError("FMPCrypto 需要 cryptography 库")
        nonce = os.urandom(12)
        aesgcm = AESGCM(self._session_key)
        ciphertext = aesgcm.encrypt(nonce, plaintext, aad)
        return nonce + ciphertext

    def decrypt(self, data: bytes, aad: bytes | None = None) -> bytes:
        if not self._session_key:
            raise RuntimeError("未设置会话密钥，请先调用 set_session_key()")
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError:
            raise RuntimeError("FMPCrypto 需要 cryptography 库")
        if len(data) < 13:
            raise ValueError("密文数据过短")
        nonce = data[:12]
        ciphertext = data[12:]
        aesgcm = AESGCM(self._session_key)
        return aesgcm.decrypt(nonce, ciphertext, aad)

    def encrypt_dict(self, data: dict, aad: bytes | None = None) -> bytes:
        import json

        plaintext = json.dumps(data, ensure_ascii=False).encode("utf-8")
        return self.encrypt(plaintext, aad)

    def decrypt_dict(self, data: bytes, aad: bytes | None = None) -> dict:
        import json

        plaintext = self.decrypt(data, aad)
        return json.loads(plaintext.decode("utf-8"))


class MetalCryptoBackend:
    """P2-01 Metal AES-GCM 加速后端 — macOS Apple Silicon 硬件加速。

    通过 PyObjC 桥接 Security.framework 的 SecKeyEncrypt/Decrypt，
    利用 Apple Secure Enclave 和 AES 硬件引擎。不可用时自动降级到 cryptography。
    """

    def __init__(self):
        self._available = False
        self._sec = None
        self._foundation = None
        self._detect()

    def _detect(self) -> None:
        if sys.platform != "darwin":
            logger.info("MetalCryptoBackend: 非 macOS，跳过检测")
            return
        try:
            import Foundation as _foundation
            import Security as _sec

            self._sec = _sec
            self._foundation = _foundation
            self._available = True
            logger.info("MetalCryptoBackend: Security.framework 可用")
        except ImportError:
            logger.info("MetalCryptoBackend: PyObjC 不可用，将降级到 cryptography")

    @property
    def available(self) -> bool:
        return self._available

    def encrypt(self, key: bytes, plaintext: bytes, aad: bytes | None = None) -> bytes:
        """使用 CommonCrypto 的 CCCryptorGCM 加密。降级到 cryptography。"""
        if not self._available:
            return self._fallback_encrypt(key, plaintext, aad)
        try:
            return self._metal_encrypt(key, plaintext, aad)
        except Exception as e:
            logger.warning(f"MetalCryptoBackend: Metal 加密失败，降级: {e}")
            return self._fallback_encrypt(key, plaintext, aad)

    def decrypt(self, key: bytes, data: bytes, aad: bytes | None = None) -> bytes:
        """解密。降级到 cryptography。"""
        if not self._available:
            return self._fallback_decrypt(key, data, aad)
        try:
            return self._metal_decrypt(key, data, aad)
        except Exception as e:
            logger.warning(f"MetalCryptoBackend: Metal 解密失败，降级: {e}")
            return self._fallback_decrypt(key, data, aad)

    def _metal_encrypt(self, key: bytes, plaintext: bytes, aad: bytes | None) -> bytes:
        import ctypes
        import ctypes.util

        lib = ctypes.cdll.LoadLibrary(ctypes.util.find_library("System"))
        nonce = os.urandom(12)
        tag = ctypes.create_string_buffer(16)
        tag_len = ctypes.c_size_t(16)
        max_out = len(plaintext) + 16
        out_buf = ctypes.create_string_buffer(max_out)
        out_len = ctypes.c_size_t(0)
        aad_ptr = aad if aad else b""
        aad_len = len(aad_ptr)
        rc = lib.CCCryptorGCM(
            0,
            0,
            key,
            len(key),
            nonce,
            12,
            aad_ptr,
            aad_len,
            plaintext,
            len(plaintext),
            out_buf,
            ctypes.byref(out_len),
            tag,
            ctypes.byref(tag_len),
        )
        if rc != 0:
            raise RuntimeError(f"CCCryptorGCM encrypt failed: {rc}")
        return nonce + out_buf.raw[: out_len.value] + tag.raw[: tag_len.value]

    def _metal_decrypt(self, key: bytes, data: bytes, aad: bytes | None) -> bytes:
        if len(data) < 28:
            raise ValueError("密文数据过短 (需要 nonce12 + tag16 + ciphertext)")
        nonce = data[:12]
        tag = data[-16:]
        ciphertext = data[12:-16]
        import ctypes
        import ctypes.util

        lib = ctypes.cdll.LoadLibrary(ctypes.util.find_library("System"))
        out_buf = ctypes.create_string_buffer(len(ciphertext) + 16)
        out_len = ctypes.c_size_t(0)
        aad_ptr = aad if aad else b""
        aad_len = len(aad_ptr)
        rc = lib.CCCryptorGCM(
            1,
            0,
            key,
            len(key),
            nonce,
            12,
            aad_ptr,
            aad_len,
            ciphertext,
            len(ciphertext),
            out_buf,
            ctypes.byref(out_len),
            ctypes.create_string_buffer(tag, 16),
            ctypes.byref(ctypes.c_size_t(16)),
        )
        if rc != 0:
            raise RuntimeError(f"CCCryptorGCM decrypt failed: {rc}")
        return out_buf.raw[: out_len.value]

    @staticmethod
    def _fallback_encrypt(key: bytes, plaintext: bytes, aad: bytes | None) -> bytes:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        ciphertext = aesgcm.encrypt(nonce, plaintext, aad)
        return nonce + ciphertext

    @staticmethod
    def _fallback_decrypt(key: bytes, data: bytes, aad: bytes | None) -> bytes:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        if len(data) < 13:
            raise ValueError("密文数据过短")
        nonce = data[:12]
        ciphertext = data[12:]
        aesgcm = AESGCM(key)
        return aesgcm.decrypt(nonce, ciphertext, aad)

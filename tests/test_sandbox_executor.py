"""SandboxExecutor + MetalCryptoBackend 单元测试。"""

import os

import pytest

from fusion_multi_node.security.sandbox import SandboxConfig, SandboxExecutor


@pytest.fixture
def executor():
    return SandboxExecutor(SandboxConfig(allowed_paths=["/tmp"], execution_timeout=5))


def test_detect_backend(executor):
    assert executor.backend in ("sandbox-exec", "unshare", "python-resource")


def test_build_sbpl_profile(executor):
    profile = executor._build_sbpl_profile()
    assert "(version 1)" in profile
    assert "(deny default)" in profile
    assert 'subpath "/tmp"' in profile


def test_build_sbpl_profile_network():
    cfg = SandboxConfig(allowed_network_hosts=["example.com"])
    ex = SandboxExecutor(cfg)
    profile = ex._build_sbpl_profile()
    assert 'host "example.com"' in profile
    assert "(deny network-outbound)" in profile


def test_profile_path_cached(executor):
    p1 = executor._get_profile_path("task-1")
    p2 = executor._get_profile_path("task-1")
    assert p1 == p2


def test_cleanup_profile(executor):
    executor._get_profile_path("task-cleanup")
    executor.cleanup_profile("task-cleanup")
    assert "task-cleanup" not in executor._profile_cache


@pytest.mark.asyncio
async def test_execute_in_sandbox_echo(executor):
    result = await executor.execute_in_sandbox("t1", ["echo", "hello"])
    assert "exit_code" in result
    assert "stdout" in result
    assert "stderr" in result
    assert "task_id" in result
    if executor.backend == "python-resource":
        assert result["exit_code"] == 0
        assert "hello" in result["stdout"]


@pytest.mark.asyncio
async def test_execute_in_sandbox_timeout():
    cfg = SandboxConfig(execution_timeout=1)
    ex = SandboxExecutor(cfg)
    result = await ex.execute_in_sandbox("t2", ["sleep", "10"])
    assert result["exit_code"] != 0
    assert "超时" in result["stderr"] or result["exit_code"] in (-6, -9)


@pytest.mark.asyncio
async def test_execute_in_sandbox_not_found(executor):
    result = await executor.execute_in_sandbox("t3", ["nonexistent_cmd_xyz"])
    assert result["exit_code"] != 0


class TestMetalCryptoBackend:
    def test_detect(self):
        from fusion_multi_node.security.crypto import MetalCryptoBackend

        backend = MetalCryptoBackend()
        assert isinstance(backend.available, bool)

    def test_fallback_encrypt_decrypt(self):
        from fusion_multi_node.security.crypto import MetalCryptoBackend

        backend = MetalCryptoBackend()
        key = os.urandom(32)
        plaintext = b"hello metal aes-gcm"
        encrypted = backend._fallback_encrypt(key, plaintext, None)
        decrypted = backend._fallback_decrypt(key, encrypted, None)
        assert decrypted == plaintext

    def test_fallback_encrypt_decrypt_with_aad(self):
        from fusion_multi_node.security.crypto import MetalCryptoBackend

        backend = MetalCryptoBackend()
        key = os.urandom(32)
        plaintext = b"authenticated data"
        aad = b"extra-auth"
        encrypted = backend._fallback_encrypt(key, plaintext, aad)
        decrypted = backend._fallback_decrypt(key, encrypted, aad)
        assert decrypted == plaintext

    def test_encrypt_decrypt_via_api(self):
        from fusion_multi_node.security.crypto import MetalCryptoBackend

        backend = MetalCryptoBackend()
        key = os.urandom(32)
        plaintext = b"test via public api"
        encrypted = backend.encrypt(key, plaintext)
        decrypted = backend.decrypt(key, encrypted)
        assert decrypted == plaintext

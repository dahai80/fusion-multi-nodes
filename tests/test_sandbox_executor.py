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
    # CI (Linux 无 CAP_SYS_ADMIN) unshare 运行时失败 → 超时路径不可测, 跳过。
    cfg = SandboxConfig(execution_timeout=1)
    ex = SandboxExecutor(cfg)
    if ex.backend == "unshare":
        import subprocess as _sp

        probe = _sp.run(["unshare", "--pid", "--fork", "--mount-proc", "--", "true"], capture_output=True)
        if probe.returncode != 0:
            pytest.skip(f"unshare 不可用 (CI 无权限): {probe.stderr.decode().strip()}")
    result = await ex.execute_in_sandbox("t2", ["sleep", "10"])
    assert result["exit_code"] != 0
    assert "超时" in result["stderr"] or result["exit_code"] in (-6, -9)


@pytest.mark.asyncio
async def test_execute_in_sandbox_not_found(executor):
    result = await executor.execute_in_sandbox("t3", ["nonexistent_cmd_xyz"])
    assert result["exit_code"] != 0


@pytest.mark.asyncio
async def test_execute_in_sandbox_cwd_env(executor):
    # #79: cwd + env 透传 — pwd 输出指定目录, env 注入自定义变量。
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        result = await executor.execute_in_sandbox(
            "t-cwd",
            ["sh", "-c", "echo $PWD $FUSION_TEST_VAR"],
            cwd=td,
            env={"FUSION_TEST_VAR": "batch-ok"},
        )
        assert "success" in result
        if executor.backend == "python-resource":
            assert result["exit_code"] == 0
            assert result["success"] is True
            assert td in result["stdout"]
            assert "batch-ok" in result["stdout"]


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


class TestP2_9RlimitsSubprocessOnly:
    """P2-9 (审计 §6.2): apply_limits setrlimit 未调 — 子进程 rlimit 根本未应用。

    修复: SandboxConfig.enforce_rlimits + execute_in_sandbox preexec_fn (仅子进程)。
    验证: rlimit 加到子进程 (非主进程), 0=不限 no-op, AgentConfig knob 桥接。
    """

    @pytest.mark.asyncio
    async def test_rlimits_apply_to_subprocess(self):
        # enforce_rlimits=True + max_memory_mb=16 → 子进程 RLIMIT_AS=16MB。
        # 子进程尝试分配 64MB → 应 MemoryError/被限 (exit_code != 0, 非正常 0)。
        # 仅 python-resource 后端可测 (sandbox-exec/unshare 走 OS profile, 跳过)。
        cfg = SandboxConfig(max_memory_mb=16, enforce_rlimits=True, execution_timeout=10)
        ex = SandboxExecutor(cfg)
        if ex.backend != "python-resource":
            pytest.skip(f"非 python-resource 后端 ({ex.backend}), rlimit 路径不适用")
        # 子进程尝试分配超限内存: python -c 分配 64MB, 成功 exit 0, 失败 exit 1。
        probe_cmd = [
            "python3",
            "-c",
            "import sys; try:  bytearray(64*1024*1024);   sys.exit(0) except MemoryError:  sys.exit(1)",
        ]
        result = await ex.execute_in_sandbox("p2-9-mem", probe_cmd)
        assert result["exit_code"] != 0, f"16MB rlimit 未生效 (子进程分配 64MB 未被限): {result}"

    @pytest.mark.asyncio
    async def test_rlimits_disabled_no_op(self):
        # enforce_rlimits=False (默认) → 无 preexec_fn → 子进程无 rlimit → 分配 64MB 成功 exit 0。
        # 验证默认不误伤 (保既有行为, 0=不限)。
        cfg = SandboxConfig(max_memory_mb=16, enforce_rlimits=False, execution_timeout=10)
        ex = SandboxExecutor(cfg)
        if ex.backend != "python-resource":
            pytest.skip(f"非 python-resource 后端 ({ex.backend}), rlimit 路径不适用")
        probe_cmd = [
            "python3",
            "-c",
            "import sys; try:  bytearray(64*1024*1024);   sys.exit(0) except MemoryError:  sys.exit(1)",
        ]
        result = await ex.execute_in_sandbox("p2-9-nolimit", probe_cmd)
        assert result["exit_code"] == 0, f"enforce=False 仍误限 (子进程 64MB 被限): {result}"

    @pytest.mark.asyncio
    async def test_main_process_not_limited(self):
        # P2-9 关键约束: rlimit 仅子进程, 不碰主推理进程 (不误杀单长跑 agent)。
        # 验证: 跑带 rlimit 子进程后, 主进程仍能分配大块 (未被 setrlimit 影响)。
        cfg = SandboxConfig(max_memory_mb=16, enforce_rlimits=True, execution_timeout=10)
        ex = SandboxExecutor(cfg)
        if ex.backend != "python-resource":
            pytest.skip(f"非 python-resource 后端 ({ex.backend}), rlimit 路径不适用")
        await ex.execute_in_sandbox("p2-9-main", ["echo", "child-ran"])
        # 主进程尝试分配 64MB — 应成功 (主进程未受限)。
        try:
            _probe = bytearray(64 * 1024 * 1024)
            del _probe
        except MemoryError:
            pytest.fail("主进程被子进程 rlimit 误限 (应仅子进程)")

    def test_agent_config_knob_zero_no_enforce(self):
        # AgentConfig knob 默认 0 → build_subprocess_sandbox_config: enforce=False (不限)。
        from fusion_multi_node.agent.node_agent import AgentConfig, NodeAgent

        cfg = AgentConfig()
        assert cfg.task_mem_limit_mb == 0
        assert cfg.task_cpu_quota == 0
        agent = NodeAgent(config=cfg)
        sb_cfg = agent.build_subprocess_sandbox_config()
        assert sb_cfg.enforce_rlimits is False
        assert sb_cfg.max_memory_mb == 0
        assert sb_cfg.max_cpu_seconds == 0

    def test_agent_config_knob_positive_enforce(self):
        # AgentConfig knob >0 → enforce=True, 字段桥接对 (仅 mem/cpu, nproc/disk=0)。
        from fusion_multi_node.agent.node_agent import AgentConfig, NodeAgent

        cfg = AgentConfig(task_mem_limit_mb=512, task_cpu_quota=120)
        agent = NodeAgent(config=cfg)
        sb_cfg = agent.build_subprocess_sandbox_config()
        assert sb_cfg.enforce_rlimits is True
        assert sb_cfg.max_memory_mb == 512
        assert sb_cfg.max_cpu_seconds == 120
        # nproc/disk 不在 AgentConfig 暴露 → 传 0 (_apply_rlimits_in_child 跳过)。
        assert sb_cfg.max_processes == 0
        assert sb_cfg.max_disk_mb == 0

"""Test isolation — redirect HOME so no test touches real ~/.fusion.

ClusterMaster/ClusterConfig/AgentConfig resolve persistence paths via
Path.home() / ".fusion" / "multi-node" (tasks.json, election_state.json,
config.json, .cluster_token). Without isolation, a test that leaves a
non-terminal task (e.g. test_cancel_running_drains_queue → next-1 RUNNING)
persists it to the real store; the next test that starts a master restores
it, polluting active_tasks and breaking state-empty assertions (Rule 9:
test passing for wrong reason). This autouse fixture gives every test a
fresh throwaway HOME under tmp_path_factory, so the full suite is order-
independent and leaves the operator's ~/.fusion untouched.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> None:
    # Capture the real HOME BEFORE redirecting — docker compose plugin lives
    # in real ~/.docker/cli-plugins (OrbStack symlinks); we link it into the
    # throwaway HOME below so the container E2E keeps working.
    real_home = Path(os.environ["HOME"])
    real_docker = real_home / ".docker"

    home = tmp_path_factory.mktemp("fmn_home")
    monkeypatch.setenv("HOME", str(home))
    # XDG fallback in case any helper consults it.
    monkeypatch.setenv("XDG_CONFIG_HOME", str(home / ".config"))
    # GAP-8 (复审计 2026-08-26): 权限强制校验默认开 (FUSION_PERMISSION_ENFORCE=1 生产零信任)。
    # 现有 http 测试用 AUTH_HEADERS (无 X-Node-Id) → 强制模式会 403。测试隔离回退兼容模式,
    # 与旧 mTLS 关闭行为一致 (缺 header 放行)。test_mtls / test_agent_server 权限用例
    # 自行显式设 FUSION_PERMISSION_ENFORCE=1 验证强制模式。
    monkeypatch.setenv("FUSION_PERMISSION_ENFORCE", "0")
    # 审计日志走隔离 HOME 下的 audit.log — 不污染真实 ~/.fusion (与 token/tasks 同理)。
    monkeypatch.setenv("FUSION_AUDIT_LOG", str(home / ".fusion" / "multi-node" / "audit.log"))
    # 审计 logger 是模块级单例 (首次 get_audit_logger 缓存路径)。HOME/env 改后须 reset,
    # 否则后续测试复用首测路径 (其 tmp 已销毁) → 写失败。每测试重建 → 写入本测试 tmp。
    from fusion_multi_node.security.audit_log import reset_audit_logger

    reset_audit_logger()
    # Preserve docker CLI plugin discovery — redirecting HOME hides the real
    # ~/.docker, breaking docker compose (test_real_network_e2e.py). Link the
    # real ~/.docker into the throwaway HOME so ~/.fusion stays fully isolated
    # while docker compose keeps working.
    if real_docker.is_dir():
        (home / ".docker").symlink_to(real_docker)
    yield

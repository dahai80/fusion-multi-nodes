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
    # Preserve docker CLI plugin discovery — redirecting HOME hides the real
    # ~/.docker, breaking docker compose (test_real_network_e2e.py). Link the
    # real ~/.docker into the throwaway HOME so ~/.fusion stays fully isolated
    # while docker compose keeps working.
    if real_docker.is_dir():
        (home / ".docker").symlink_to(real_docker)
    yield

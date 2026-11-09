from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

BASH_SCRIPTS = [
    "deliver/ubuntu/bring-up.sh",
    "deliver/macos/bootstrap-guest.sh",
    "deliver/demo/orchestrate-demo.sh",
    "docker-entrypoint.sh",
]


def test_orchestrate_docs_bilingual():
    en = (REPO_ROOT / "deliver" / "demo" / "README.md").read_text()
    cn = (REPO_ROOT / "deliver" / "demo" / "README_CN.md").read_text()
    for token in ("orchestrate-demo.sh", "bootstrap-guest.sh", "/v1/chat/completions"):
        assert token in en and token in cn
    # Hidden-compute invariant: B never customer-facing.
    assert "never exposed" in en
    assert "绝不暴露" in cn


def test_orchestrate_targets_master_f3_proxy():
    text = (REPO_ROOT / "deliver" / "demo" / "orchestrate-demo.sh").read_text()
    assert "/v1/chat/completions" in text
    assert "FUSION_VERIFY_MODEL" in text
    # No mlx / model download logic on the orchestrating side.
    assert "HF_MIRROR" not in text


@pytest.mark.parametrize("rel", BASH_SCRIPTS)
def test_bash_scripts_parse(rel):
    path = REPO_ROOT / rel
    assert path.is_file(), f"missing delivery script: {rel}"
    result = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, f"{rel} bash -n failed: {result.stderr}"


def test_compose_has_web_service():
    # Textual checks (no yaml dep in CI test env); YAML validity is verified
    # locally and by `docker compose config` at build time.
    text = (REPO_ROOT / "docker-compose.yml").read_text()
    for name in ("master:", "agent:", "web:"):
        assert f"\n  {name}" in text, f"compose missing service: {name.rstrip(':')}"
    assert "FUSION_MULTINODE_ROLE: web" in text
    assert ":11472" in text  # published port (literal or via FUSION_WEB_PORT env default)


def test_compose_provides_fusion_core_build_context():
    # fusion-core is not on PyPI — every build must declare the extra context.
    text = (REPO_ROOT / "docker-compose.yml").read_text()
    assert text.count("additional_contexts:") == 3
    assert "FUSION_CORE_DIR" in text


def test_dockerfile_installs_fusion_core_from_context():
    # No github HTTPS at build time (pip git+ unreachable on restricted networks).
    text = (REPO_ROOT / "Dockerfile").read_text()
    assert "COPY --from=fusion-core" in text
    assert "git+https" not in text


def test_bring_up_resolves_fusion_core_and_env_conflicts():
    text = (REPO_ROOT / "deliver" / "ubuntu" / "bring-up.sh").read_text()
    assert "FUSION_CORE_DIR" in text
    assert "unset FUSION_CLIENT_KEY" in text


def test_entrypoint_web_role_serves_dist():
    text = (REPO_ROOT / "docker-entrypoint.sh").read_text()
    assert 'ROLE = "web"' in text or '[ "$ROLE" = "web" ]' in text
    assert "serve_dist.py" in text


def test_dockerfile_builds_web_dist():
    text = (REPO_ROOT / "Dockerfile").read_text()
    assert "oven/bun" in text
    assert "web/dist" in text
    assert "serve_dist.py" in text


def test_serve_dist_supports_bind_host_env():
    text = (REPO_ROOT / "web" / "serve_dist.py").read_text()
    assert "FUSION_MULTINODE_WEB_HOST" in text


def test_deliver_docs_bilingual():
    en = (REPO_ROOT / "deliver" / "README.md").read_text()
    cn = (REPO_ROOT / "deliver" / "README_CN.md").read_text()
    assert "bring-up.sh" in en and "bootstrap-guest.sh" in en
    assert "bring-up.sh" in cn and "bootstrap-guest.sh" in cn

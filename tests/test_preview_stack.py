from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PREVIEW = REPO_ROOT / "deliver" / "preview"


def test_preview_script_parses():
    result = subprocess.run(
        ["bash", "-n", str(PREVIEW / "preview-stack.sh")], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr


def test_preview_overlay_is_host_port_remap_only():
    # In-container ports must stay unchanged — base healthchecks probe fixed
    # in-container ports; changing them breaks `compose up --wait`.
    text = (PREVIEW / "docker-compose.preview.yml").read_text()
    assert "fusion-preview-master" in text
    assert ':-21452}:11452"' in text  # host-port remap (env-default form)
    assert ':-21472}:11472"' in text
    assert 'FUSION_MULTINODE_PORT: "21452"' not in text


def test_preview_agent_healthcheck_is_liveness():
    # Preview stacks must run without fusion-mlx — deep health (which requires
    # the inference backend) would mark the agent unhealthy.
    text = (PREVIEW / "docker-compose.preview.yml").read_text()
    assert "/api/health/deep" not in text
    assert "'http://127.0.0.1:11458/api/health'" in text


def test_seed_fixtures_deterministic_and_synthetic():
    text = (PREVIEW / "synthetic_seed.py").read_text()
    assert "[SYNTHETIC DEMO]" in text
    # Deterministic: no timestamps/randomness in fixtures.
    assert "time.time" not in text and "uuid" not in text and "random" not in text


def test_seed_function_idempotent_contract():
    spec = importlib.util.spec_from_file_location("synthetic_seed", PREVIEW / "synthetic_seed.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    ids = [s["id"] for s in mod.SYNTHETIC_SESSIONS]
    assert len(ids) == len(set(ids))
    assert all(i.startswith("preview-demo-") for i in ids)
    # count larger than fixture list is clamped, not an error
    assert min(10, len(mod.SYNTHETIC_SESSIONS)) == len(mod.SYNTHETIC_SESSIONS)


def test_preview_script_unsets_conflicting_env():
    text = (PREVIEW / "preview-stack.sh").read_text()
    assert "unset FUSION_CLIENT_KEY" in text


def test_ttl_watcher_uses_dedicated_state_dir():
    text = (PREVIEW / "preview-stack.sh").read_text()
    assert ".preview-stack" in text
    assert "down -v --remove-orphans" in text


def test_preview_docs_bilingual():
    en = (PREVIEW / "README.md").read_text()
    cn = (PREVIEW / "README_CN.md").read_text()
    for token in ("preview-stack.sh", "FUSION_PREVIEW_TTL_MINUTES"):
        assert token in en and token in cn
    # Notify hook documented EN-side; CN references the cowork email channel.
    assert "FUSION_PREVIEW_NOTIFY_CMD" in en
    assert "cow#404" in cn

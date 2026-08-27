"""P2-13 docker-compose 资源限制校验 — 解析 YAML 断言两服务含 mem_limit/cpus + deploy.resources.limits。

不依赖 docker daemon (CI 无 docker); 纯 YAML 解析。yaml 为传递依赖, 缺则 skip。
"""

from __future__ import annotations

from pathlib import Path

import pytest

_COMPOSE = Path(__file__).resolve().parent.parent / "docker-compose.yml"


def _load_compose():
    yaml = pytest.importorskip("yaml")
    with open(_COMPOSE) as f:
        return yaml.safe_load(f)


def _assert_resource_limits(svc: dict, name: str) -> None:
    # P2-13: compose v2 字段 — mem_limit + cpus 必须存在 (env 覆盖默认 4g/4)。
    assert "mem_limit" in svc, f"{name} 缺 mem_limit (P2-13 资源上限)"
    assert "cpus" in svc, f"{name} 缺 cpus (P2-13 资源上限)"
    assert "${" in str(svc["mem_limit"]), f"{name} mem_limit 须 env 可覆盖"
    assert "${" in str(svc["cpus"]), f"{name} cpus 须 env 可覆盖"
    # P2-13: compose v3 spec — deploy.resources.limits.memory + cpus。
    deploy = svc.get("deploy", {})
    limits = deploy.get("resources", {}).get("limits", {})
    assert "memory" in limits, f"{name} 缺 deploy.resources.limits.memory"
    assert "cpus" in limits, f"{name} 缺 deploy.resources.limits.cpus"


def test_master_has_resource_limits():
    compose = _load_compose()
    master = compose["services"]["master"]
    _assert_resource_limits(master, "master")


def test_agent_has_resource_limits():
    compose = _load_compose()
    agent = compose["services"]["agent"]
    _assert_resource_limits(agent, "agent")


def test_resource_limits_default_values():
    # P2-13: env 未覆盖时默认 4g/4 (Apple Silicon M 系), 与 .env.example 一致。
    compose = _load_compose()
    for name in ("master", "agent"):
        svc = compose["services"][name]
        assert ":-" in str(svc["mem_limit"]), f"{name} mem_limit 须带默认值 :-4g"
        assert "4g" in str(svc["mem_limit"]), f"{name} mem_limit 默认须为 4g"


def test_env_example_documents_resource_vars():
    # P2-13: .env.example 须文档化资源上限 env, 供 operator 调整。
    env = (_COMPOSE.parent / ".env.example").read_text()
    assert "FUSION_MASTER_MEM_LIMIT" in env, ".env.example 缺 FUSION_MASTER_MEM_LIMIT"
    assert "FUSION_AGENT_MEM_LIMIT" in env, ".env.example 缺 FUSION_AGENT_MEM_LIMIT"
    assert "FUSION_MASTER_CPUS" in env, ".env.example 缺 FUSION_MASTER_CPUS"
    assert "FUSION_AGENT_CPUS" in env, ".env.example 缺 FUSION_AGENT_CPUS"

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


# v0.14.0 企业生产 env 透传 — mTLS / 告警 / 多租户 bootstrap 经 compose env 注入 (默认空=关)。
_MTLS_ENV = (
    "FUSION_MTLS_ENABLED",
    "FUSION_MTLS_CA_CERT",
    "FUSION_MTLS_NODE_CERT",
    "FUSION_MTLS_NODE_KEY",
    "FUSION_MTLS_NODE_ID",
    "FUSION_MTLS_NODE_ROLE",
)


def test_compose_master_mtls_env_passthrough():
    # v0.14.0: master service 须透传 mTLS env (默认空=关, 生产显式设值开启)。
    compose = _load_compose()
    env = compose["services"]["master"]["environment"]
    for key in _MTLS_ENV:
        assert key in env, f"master environment 缺 {key} (v0.14.0 mTLS 透传)"
    assert "FUSION_ALERT_WEBHOOK_URL" in env, "master environment 缺 FUSION_ALERT_WEBHOOK_URL"
    assert "FUSION_BOOTSTRAP_ADMIN" in env, "master environment 缺 FUSION_BOOTSTRAP_ADMIN"


def test_compose_agent_mtls_env_passthrough():
    # v0.14.0: agent 亦须透传 mTLS env (集群双向认证, 与 master 同 CA)。
    compose = _load_compose()
    env = compose["services"]["agent"]["environment"]
    for key in _MTLS_ENV:
        assert key in env, f"agent environment 缺 {key} (v0.14.0 mTLS 透传)"


def test_compose_mtls_env_default_empty():
    # v0.14.0: mTLS env 须带 :- 空默认 (关), 不带弱硬编码 (fail-closed 由 mtls.py 保证, 非 compose 默认开)。
    compose = _load_compose()
    for name in ("master", "agent"):
        env = compose["services"][name]["environment"]
        val = env["FUSION_MTLS_ENABLED"]
        assert ":-" in str(val), f"{name} FUSION_MTLS_ENABLED 须带 :- 空默认 (关)"


def test_compose_mtls_volume_mount():
    # v0.14.0: mTLS 开启需证书文件挂载 — volumes 须含 tls 目录只读挂载。
    compose = _load_compose()
    for name in ("master", "agent"):
        svc = compose["services"][name]
        vols = svc.get("volumes", [])
        assert any("/tls" in str(v) for v in vols), f"{name} 缺 /tls 卷挂载 (v0.14.0 mTLS 证书)"


def test_env_example_documents_mtls_and_alert():
    # v0.14.0: .env.example 须文档化 mTLS + 告警 + bootstrap env (注释引导, 生产必配)。
    env = (_COMPOSE.parent / ".env.example").read_text()
    for key in _MTLS_ENV + ("FUSION_ALERT_WEBHOOK_URL", "FUSION_BOOTSTRAP_ADMIN"):
        assert key in env, f".env.example 缺 {key} (v0.14.0 企业生产文档)"


def test_plist_mtls_env_placeholders():
    # v0.14.0: launchd plist 模板须含 mTLS/alert/token/bootstrap 占位符 (start.sh install-launchd 渲染)。
    plist = (_COMPOSE.parent / "deploy" / "com.dahai80.fusion-multi-node.plist").read_text()
    for ph in (
        "@@MTLS_ENABLED@@",
        "@@MTLS_CA_CERT@@",
        "@@MTLS_NODE_CERT@@",
        "@@MTLS_NODE_KEY@@",
        "@@MTLS_NODE_ID@@",
        "@@MTLS_NODE_ROLE@@",
        "@@ALERT_WEBHOOK_URL@@",
        "@@CLUSTER_TOKEN@@",
        "@@BOOTSTRAP_ADMIN@@",
        "@@USERS_FILE@@",
    ):
        assert ph in plist, f"plist 模板缺占位符 {ph} (v0.14.0 env 透传)"


def test_startsh_renders_mtls_placeholders():
    # v0.14.0: start.sh install-launchd sed 须替换新占位符 (从 env 读, 默认空)。
    startsh = (_COMPOSE.parent / "start.sh").read_text()
    for ph in (
        "@@MTLS_ENABLED@@",
        "@@MTLS_CA_CERT@@",
        "@@MTLS_NODE_CERT@@",
        "@@MTLS_NODE_KEY@@",
        "@@MTLS_NODE_ID@@",
        "@@MTLS_NODE_ROLE@@",
        "@@ALERT_WEBHOOK_URL@@",
        "@@CLUSTER_TOKEN@@",
        "@@BOOTSTRAP_ADMIN@@",
        "@@USERS_FILE@@",
    ):
        assert ph in startsh, f"start.sh 缺 sed 替换 {ph} (v0.14.0 env 透传)"

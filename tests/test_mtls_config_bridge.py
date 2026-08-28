"""v0.14.0 item 4 — mTLS config 段 → env 桥 + lazy is_enabled 测试。

覆盖:
- configure_from_config 把 security.mtls 段写回 env (enabled + 证书路径)。
- env 优先: 已设的非空 env 不被 config 覆盖 (兼容旧 env-only 部署)。
- is_enabled lazy: config 写 env 后即时反映 (无 import-time 缓存问题)。
- fail-closed: enabled=True 但证书路径不全 → server_ssl_kwargs raise (不回退明文)。
- 空 config 段 / 无 get_mtls_config → no-op 不抛。
"""

from __future__ import annotations

import os

import pytest

from fusion_multi_node.config import ClusterConfig
from fusion_multi_node.security import mtls

_MTLS_ENVS = (
    "FUSION_MTLS_ENABLED",
    "FUSION_MTLS_CA_CERT",
    "FUSION_MTLS_NODE_CERT",
    "FUSION_MTLS_NODE_KEY",
    "FUSION_MTLS_NODE_ID",
    "FUSION_MTLS_NODE_ROLE",
)


@pytest.fixture(autouse=True)
def _snapshot_mtls_env():
    # configure_from_config 经 os.environ[...]= 原始写 env (非 monkeypatch),
    # monkeypatch 仅能回滚自身 setenv/delenv, 无法回滚 SUT 内裸写 → 跨测泄漏
    # (mTLS on 泄漏 → 后续真 bind 测试走 https + 缺证书 bind 失败)。
    # 故每测快照 + 还原整个 mTLS env 键集, 兜底 SUT 裸写。
    saved = {k: os.environ.get(k) for k in _MTLS_ENVS}
    for k in _MTLS_ENVS:
        os.environ.pop(k, None)
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _cfg(**mtls_overrides):
    """构造 ClusterConfig 带 security.mtls 覆盖 (写到 tmp config.json)。"""
    cfg = ClusterConfig()
    for k, v in mtls_overrides.items():
        cfg.set(f"security.mtls.{k}", v)
    return cfg


class TestMtlsConfigBridge:
    def test_disabled_by_default(self):
        assert mtls.is_enabled() is False
        assert mtls.server_ssl_kwargs() == {}
        assert mtls.client_kwargs() == {}

    def test_configure_writes_enabled_to_env(self):
        cfg = _cfg(enabled=True, ca_cert="/tls/ca.crt", node_cert="/tls/n.crt", node_key="/tls/n.key")
        mtls.configure_from_config(cfg)
        assert os.environ.get("FUSION_MTLS_ENABLED") == "1"
        assert mtls.is_enabled() is True
        assert os.environ.get("FUSION_MTLS_CA_CERT") == "/tls/ca.crt"
        assert os.environ.get("FUSION_MTLS_NODE_CERT") == "/tls/n.crt"
        assert os.environ.get("FUSION_MTLS_NODE_KEY") == "/tls/n.key"

    def test_env_priority_over_config(self, monkeypatch):
        # env 已设非空 → config 不覆盖 (兼容旧 env-only 部署)
        monkeypatch.setenv("FUSION_MTLS_ENABLED", "0")
        monkeypatch.setenv("FUSION_MTLS_CA_CERT", "/env/ca.crt")
        cfg = _cfg(enabled=True, ca_cert="/config/ca.crt", node_cert="/config/n.crt", node_key="/config/n.key")
        mtls.configure_from_config(cfg)
        # env 优先: enabled 保持 env 的 "0" (config 的 True 不覆盖)
        assert os.environ.get("FUSION_MTLS_ENABLED") == "0"
        assert os.environ.get("FUSION_MTLS_CA_CERT") == "/env/ca.crt"
        # 未设 env 的字段仍被 config 写入
        assert os.environ.get("FUSION_MTLS_NODE_CERT") == "/config/n.crt"

    def test_fail_closed_enabled_but_certs_missing(self):
        cfg = _cfg(enabled=True)  # 无证书路径
        mtls.configure_from_config(cfg)
        assert mtls.is_enabled() is True
        # fail-closed: 开但证书不全 → raise, 不回退明文 (GAP-2 不变)
        with pytest.raises(RuntimeError, match="证书路径不全"):
            mtls.server_ssl_kwargs()
        with pytest.raises(RuntimeError, match="证书路径不全"):
            mtls.client_kwargs()

    def test_empty_config_section_noop(self):
        # config 无 mtls 段 (空 dict) → no-op 不抛, is_enabled 不变
        cfg = ClusterConfig()
        # 默认段 enabled=False
        mtls.configure_from_config(cfg)
        assert mtls.is_enabled() is False

    def test_no_get_mtls_config_method(self):
        # 传无 get_mtls_config 的对象 → no-op 不抛 (hasattr 守卫)
        mtls.configure_from_config(object())  # type: ignore[arg-type]
        assert mtls.is_enabled() is False

    def test_cert_path_only_written_when_nonempty(self):
        # config 证书路径空串 → 不写 env (留旧值/空)
        cfg = _cfg(enabled=True, ca_cert="", node_cert="", node_key="")
        mtls.configure_from_config(cfg)
        assert os.environ.get("FUSION_MTLS_ENABLED") == "1"
        # 空串不写 env
        assert os.environ.get("FUSION_MTLS_CA_CERT") is None

    def test_scheme_reflects_lazy_state(self):
        assert mtls.scheme() == "http"
        cfg = _cfg(enabled=True, ca_cert="/tls/ca.crt", node_cert="/tls/n.crt", node_key="/tls/n.key")
        mtls.configure_from_config(cfg)
        assert mtls.scheme() == "https"

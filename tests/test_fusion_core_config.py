"""#89: fusion_core.config 单点解析接入后的语义回归。

显式参数优先; env 链 FUSION_CLIENT_KEY → FUSION_MLX_API_KEY (旧名 warn) → MLX_API_KEY;
dev 缺 key 返 "" + warning; FUSION_PROFILE=prod 缺 key RuntimeError (fail-fast)。
"""

import pytest

from fusion_multi_node.agent.node_agent import FusionMLXBackend
from fusion_multi_node.distributed_mlx.kv_tensor_transport import MLXKVTransport

_KEY_ENVS = ("FUSION_CLIENT_KEY", "FUSION_MLX_API_KEY", "MLX_API_KEY", "FUSION_PROFILE")


@pytest.fixture(autouse=True)
def _clean_key_env(monkeypatch):
    # 本机 shell 常驻 FUSION_CLIENT_KEY — 测试须显式清空, 隔离真实环境。
    for var in _KEY_ENVS:
        monkeypatch.delenv(var, raising=False)


def test_explicit_api_key_wins(monkeypatch):
    monkeypatch.setenv("FUSION_CLIENT_KEY", "from-env")
    backend = FusionMLXBackend(api_key="explicit")
    assert backend.api_key == "explicit"
    transport = MLXKVTransport(api_key="explicit")
    assert transport._api_key == "explicit"


def test_client_key_env_picked_up(monkeypatch):
    monkeypatch.setenv("FUSION_CLIENT_KEY", "fg-client-key")
    backend = FusionMLXBackend()
    assert backend.api_key == "fg-client-key"


def test_legacy_mlx_api_key_alias_with_warning(monkeypatch, caplog):
    monkeypatch.setenv("FUSION_MLX_API_KEY", "legacy-key")
    with caplog.at_level("WARNING", logger="fusion_core.config"):
        backend = FusionMLXBackend()
    assert backend.api_key == "legacy-key"
    assert any("deprecated" in r.message for r in caplog.records)


def test_dev_missing_key_returns_empty_with_warning(caplog):
    # _ANON_WARNED 进程级 once-only — 先清, 防随机顺序下前序用例已消费掉 warning。
    import fusion_core.config as _fc_config

    _fc_config._ANON_WARNED.clear()
    with caplog.at_level("WARNING", logger="fusion_core.config"):
        backend = FusionMLXBackend()
    assert backend.api_key == ""
    assert any("anonymous dev mode" in r.message for r in caplog.records)


def test_prod_missing_key_raises(monkeypatch):
    monkeypatch.setenv("FUSION_PROFILE", "prod")
    with pytest.raises(RuntimeError, match="FUSION_CLIENT_KEY not set"):
        FusionMLXBackend()
    with pytest.raises(RuntimeError, match="FUSION_CLIENT_KEY not set"):
        MLXKVTransport()


def test_prod_explicit_key_no_raise(monkeypatch):
    monkeypatch.setenv("FUSION_PROFILE", "prod")
    backend = FusionMLXBackend(api_key="explicit-in-prod")
    assert backend.api_key == "explicit-in-prod"


def test_service_port_multinode_env_name(monkeypatch):
    # cli._master_http 端口解析走 service_port("multinode", ...) — env 名须是 FUSION_MULTINODE_PORT。
    from fusion_core.config import service_port

    monkeypatch.setenv("FUSION_MULTINODE_PORT", "21552")
    assert service_port("multinode", 11452) == 21552
    monkeypatch.delenv("FUSION_MULTINODE_PORT")
    assert service_port("multinode", 11452) == 11452


def test_service_port_multinode_invalid_raises(monkeypatch):
    from fusion_core.config import service_port

    monkeypatch.setenv("FUSION_MULTINODE_PORT", "not-a-port")
    with pytest.raises(ValueError, match="FUSION_MULTINODE_PORT"):
        service_port("multinode", 11452)

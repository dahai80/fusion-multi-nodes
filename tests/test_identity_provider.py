"""#74 OPTIONAL fusion-identity 集成测试。

覆盖:
(a) IdentityProvider.verify_jwt — mock httpx, 解析 claims; revoked→None; 非200→None; 不可达→raise (fail-closed)。
(b) get_tenant_quota — 缓存命中; admin 接口; 失败→None。
(c) report_usage — best-effort, 失败不抛。
(d) get_identity_provider — env 未设→None; 已设→单例。
(e) 离线默认: 未启用 verify_jwt 返 None, get_tenant_quota 返 None。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from fusion_multi_node.security.identity_provider import (
    IdentityProvider,
    get_identity_provider,
    reset_identity_provider,
)


@pytest.fixture(autouse=True)
def _reset():
    reset_identity_provider()
    yield
    reset_identity_provider()


class TestVerifyJwt:
    def test_parses_claims(self):
        ip = IdentityProvider(base_url="http://id.test", service_token="svc")
        fake = MagicMock(status_code=200)
        fake.json.return_value = {"tid": "t1", "role": "user", "quota": {"concurrent": 8}}
        with patch("httpx.post", return_value=fake):
            claims = ip.verify_jwt("a.b.c")
        assert claims["tid"] == "t1"
        assert claims["role"] == "user"

    def test_revoked_returns_none(self):
        ip = IdentityProvider(base_url="http://id.test")
        fake = MagicMock(status_code=200)
        fake.json.return_value = {"tid": "t1", "role": "user", "revoked": True}
        with patch("httpx.post", return_value=fake):
            assert ip.verify_jwt("a.b.c") is None

    def test_tenant_status_revoked_returns_none(self):
        ip = IdentityProvider(base_url="http://id.test")
        fake = MagicMock(status_code=200)
        fake.json.return_value = {"tid": "t1", "tenant_status": "revoked"}
        with patch("httpx.post", return_value=fake):
            assert ip.verify_jwt("a.b.c") is None

    def test_non200_returns_none(self):
        ip = IdentityProvider(base_url="http://id.test")
        fake = MagicMock(status_code=401)
        with patch("httpx.post", return_value=fake):
            assert ip.verify_jwt("a.b.c") is None

    def test_unreachable_raises_fail_closed(self):
        ip = IdentityProvider(base_url="http://id.test")
        with patch("httpx.post", side_effect=ConnectionError("down")):
            with pytest.raises(ConnectionError):
                ip.verify_jwt("a.b.c")

    def test_disabled_returns_none(self):
        ip = IdentityProvider(base_url="")
        assert ip.enabled is False
        assert ip.verify_jwt("a.b.c") is None


class TestTenantQuota:
    def test_admin_endpoint(self):
        ip = IdentityProvider(base_url="http://id.test")
        fake = MagicMock(status_code=200)
        fake.json.return_value = {"concurrent": 5}
        with patch("httpx.get", return_value=fake):
            assert ip.get_tenant_quota("t1") == 5

    def test_cache_hit(self):
        ip = IdentityProvider(base_url="http://id.test")
        ip._quota_cache["t1"] = 7
        # 不应发 HTTP
        with patch("httpx.get") as m:
            assert ip.get_tenant_quota("t1") == 7
            m.assert_not_called()

    def test_failure_returns_none(self):
        ip = IdentityProvider(base_url="http://id.test")
        with patch("httpx.get", side_effect=ConnectionError("down")):
            assert ip.get_tenant_quota("t1") is None

    def test_disabled_returns_none(self):
        ip = IdentityProvider(base_url="")
        assert ip.get_tenant_quota("t1") is None


class TestReportUsage:
    def test_best_effort_no_raise(self):
        ip = IdentityProvider(base_url="http://id.test")
        with patch("httpx.post", side_effect=ConnectionError("down")):
            ip.report_usage("t1", "tasks_completed", 1, model="m", user_id="t1")  # 不抛

    def test_disabled_noop(self):
        ip = IdentityProvider(base_url="")
        with patch("httpx.post") as m:
            ip.report_usage("t1", "tasks_completed", 1)
            m.assert_not_called()


class TestGetIdentityProvider:
    def test_unset_env_returns_none(self, monkeypatch):
        monkeypatch.delenv("FUSION_IDENTITY_URL", raising=False)
        assert get_identity_provider() is None

    def test_set_env_singleton(self, monkeypatch):
        monkeypatch.setenv("FUSION_IDENTITY_URL", "http://id.test")
        monkeypatch.setenv("FUSION_IDENTITY_SERVICE_TOKEN", "svc")
        a = get_identity_provider()
        b = get_identity_provider()
        assert a is not None
        assert a is b
        assert a.enabled is True

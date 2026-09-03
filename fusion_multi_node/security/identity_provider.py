"""#74 OPTIONAL fusion-identity 集成 — 运维显式 opt-in 时启用, 离线默认不变。

fusion-identity 是 Fusion 生态的租户/鉴权服务 (签发 JWT + per-tenant 配额 + 用量)。
本模块是它的 OPTIONAL 客户端:

- 运维设置 FUSION_IDENTITY_URL → enabled=True, JWT 令牌经 /verify 校验, per-tenant 配额
  从 identity 拉取, 任务完成上报用量。identity 为权威 (opt-in 后 fail-closed: 不可达即拒)。
- 未设置 → get_identity_provider() 返 None, 所有行为退回本地 config + fmu_ store (离线默认)。

设计: 不退役 fmu_ store (UserStore)。JWT 与 fmu_ 共存 — JWT 令牌走本路径, fmu_ 走旧路径。
100% 本地/离线规则不破: identity 是 opt-in, 默认零配置离线。
"""

from __future__ import annotations

import logging
import os
import threading

logger = logging.getLogger(__name__)

_provider: IdentityProvider | None = None
_provider_lock = threading.Lock()


def _looks_like_jwt(token: str) -> bool:
    """粗判 JWT — 三段点分 (header.payload.signature)。不校验签名 (校验交 identity /verify)。"""
    return token.count(".") == 2 and not token.startswith("fmu_")


class IdentityProvider:
    """fusion-identity 可选客户端。

    enabled = bool(base_url)。env: FUSION_IDENTITY_URL, FUSION_IDENTITY_SERVICE_TOKEN。
    """

    def __init__(self, base_url: str = "", service_token: str = "", timeout: float = 3.0):
        self._base_url = base_url.rstrip("/")
        self._service_token = service_token
        self._timeout = timeout
        self._quota_cache: dict[str, int] = {}

    @property
    def enabled(self) -> bool:
        return bool(self._base_url)

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self._service_token:
            h["Authorization"] = f"Bearer {self._service_token}"
        return h

    def verify_jwt(self, token: str) -> dict | None:
        """POST /api/v1/auth/verify 校验 JWT → claims {tid, role, scopes, quota, tenant_status, revoked}。

        返回 None = 令牌无效/吊销/非 200。enabled 但网络错误 → raise (fail-closed,
        运维已 opt-in, identity 是权威, 不可达即拒, 不静默放行)。
        """
        if not self.enabled:
            return None
        import httpx

        url = f"{self._base_url}/api/v1/auth/verify"
        logger.info(f"#74 identity verify JWT ({url})")
        try:
            resp = httpx.post(url, json={"token": token}, headers=self._headers(), timeout=self._timeout)
        except Exception as e:
            logger.critical(f"#74 identity 不可达 (fail-closed 拒): {e}")
            raise
        if resp.status_code != 200:
            logger.warning(f"#74 identity verify 非 200: {resp.status_code}")
            return None
        try:
            claims = resp.json()
        except Exception as e:
            logger.warning(f"#74 identity verify 响应解析失败: {e}")
            return None
        if claims.get("revoked") or claims.get("tenant_status") == "revoked":
            logger.warning(f"#74 identity verify 令牌/租户已吊销: tid={claims.get('tid')}")
            return None
        return claims

    def get_tenant_quota(self, tid: str) -> int | None:
        """取 per-tenant 并发配额。优先 verify 缓存 claims.quota.concurrent, 回退 admin 接口。

        None → 调用方退回本地全局配额。enabled 关 → None。
        """
        if not self.enabled:
            return None
        if tid in self._quota_cache:
            return self._quota_cache[tid]
        import httpx

        url = f"{self._base_url}/api/v1/admin/tenants/{tid}/quota"
        try:
            resp = httpx.get(url, headers=self._headers(), timeout=self._timeout)
        except Exception as e:
            logger.warning(f"#74 identity 取配额失败 (退回本地): tid={tid} {e}")
            return None
        if resp.status_code != 200:
            logger.warning(f"#74 identity 取配额非 200: {resp.status_code} tid={tid}")
            return None
        try:
            q = resp.json().get("concurrent")
        except Exception:
            return None
        if isinstance(q, int) and q > 0:
            self._quota_cache[tid] = q
            return q
        return None

    def report_usage(
        self, tid: str, metric: str, value: int | float, model: str | None = None, user_id: str | None = None
    ) -> None:
        """POST /api/v1/tenants/{tid}/usage 上报用量。best-effort, 失败仅日志, 不阻塞调度。"""
        if not self.enabled:
            return
        import httpx

        url = f"{self._base_url}/api/v1/tenants/{tid}/usage"
        payload = {"metric": metric, "value": value}
        if model is not None:
            payload["model"] = model
        if user_id is not None:
            payload["user_id"] = user_id
        try:
            resp = httpx.post(url, json=payload, headers=self._headers(), timeout=self._timeout)
        except Exception as e:
            logger.warning(f"#74 identity 上报用量失败 (best-effort, 忽略): tid={tid} {e}")
            return
        if resp.status_code >= 400:
            logger.warning(f"#74 identity 上报用量非 2xx: {resp.status_code} tid={tid}")


def get_identity_provider() -> IdentityProvider | None:
    """单例 — env FUSION_IDENTITY_URL 未设 → None (离线默认)。

    运维显式 opt-in 后, 全进程共享同一实例 (含配额缓存)。
    """
    global _provider
    if _provider is not None:
        return _provider
    base_url = os.environ.get("FUSION_IDENTITY_URL", "").strip()
    if not base_url:
        return None
    service_token = os.environ.get("FUSION_IDENTITY_SERVICE_TOKEN", "").strip()
    with _provider_lock:
        if _provider is None:
            _provider = IdentityProvider(base_url=base_url, service_token=service_token)
            logger.info(f"#74 identity provider 启用 (opt-in): {base_url}")
    return _provider


def reset_identity_provider() -> None:
    """测试重置单例。"""
    global _provider
    with _provider_lock:
        _provider = None

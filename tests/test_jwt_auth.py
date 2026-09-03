"""#74 BearerAuthMiddleware JWT 分支测试。

jwt_verifier 注入后: JWT 令牌 → claims 注入 scope; 无效/吊销 → 401 fail-closed;
异常 → 401 fail-closed; fmu_ 令牌仍走旧路径; cluster_token 仍通过 (未注入 verifier 时默认)。
"""

from __future__ import annotations

from unittest.mock import MagicMock

from fusion_multi_node.utils.auth import BearerAuthMiddleware


class _CaptureApp:
    def __init__(self):
        self.scope = None

    async def __call__(self, scope, receive, send):
        self.scope = scope
        from starlette.responses import JSONResponse

        await JSONResponse(status_code=200, content={"ok": True})(scope, receive, send)


def _make_request(token: str, path: str = "/api/nodes"):
    async def app(scope, receive, send):
        pass

    app = BearerAuthMiddleware(app, shared_token="cluster-tok")
    capture = _CaptureApp()
    app.app = capture

    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [(b"authorization", f"Bearer {token}".encode())],
        "client": ("127.0.0.1", 1234),
    }

    sent = {}

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(msg):
        if msg["type"] == "http.response.start":
            sent["status"] = msg["status"]
        elif msg["type"] == "http.response.body":
            sent["body"] = msg.get("body", b"")

    return app, capture, scope, receive, send, sent


async def _run(token: str, jwt_verifier=None, path: str = "/api/nodes"):
    app, capture, scope, receive, send, sent = _make_request(token, path)
    app._jwt_verifier = jwt_verifier
    await app(scope, receive, send)
    return capture, sent


class TestJwtAuth:
    async def test_jwt_claims_injected(self):
        verifier = MagicMock(return_value={"tid": "t1", "role": "admin", "quota": {"concurrent": 5}})
        capture, sent = await _run("a.b.c", jwt_verifier=verifier)
        assert sent["status"] == 200
        assert capture.scope["user_id"] == "t1"
        assert capture.scope["user_role"] == "admin"
        assert capture.scope["tenant_quota"] == {"concurrent": 5}

    async def test_invalid_jwt_returns_401(self):
        verifier = MagicMock(return_value=None)
        capture, sent = await _run("a.b.c", jwt_verifier=verifier)
        assert sent["status"] == 401

    async def test_verifier_exception_fail_closed_401(self):
        verifier = MagicMock(side_effect=ConnectionError("identity down"))
        capture, sent = await _run("a.b.c", jwt_verifier=verifier)
        assert sent["status"] == 401

    async def test_fmu_token_still_works_with_verifier_set(self):
        # fmu_ 前缀 → 非 JWT, 跳过 verifier, 落 cluster_token 路径 (无 user_store → 显式拒)
        # 这里 fmu_ 令牌非 cluster_token → 401, 验证 verifier 未被调用
        verifier = MagicMock(return_value={"tid": "t1"})
        capture, sent = await _run("fmu_alice_secret", jwt_verifier=verifier)
        assert sent["status"] == 401
        verifier.assert_not_called()

    async def test_cluster_token_works_with_verifier_set(self):
        verifier = MagicMock(return_value={"tid": "t1"})
        capture, sent = await _run("cluster-tok", jwt_verifier=verifier)
        assert sent["status"] == 200
        verifier.assert_not_called()

    async def test_no_verifier_jwt_token_falls_to_cluster_token(self):
        # 未注入 verifier → JWT 形令牌落 cluster_token 路径, 非 cluster_token → 401
        capture, sent = await _run("a.b.c", jwt_verifier=None)
        assert sent["status"] == 401

    async def test_jwt_excluded_from_exempt_path(self):
        verifier = MagicMock()
        # exempt path → 中间件直接放行, verifier 不调
        app, capture, scope, receive, send, sent = _make_request("a.b.c", path="/api/health")
        app._jwt_verifier = verifier
        await app(scope, receive, send)
        assert sent["status"] == 200
        verifier.assert_not_called()

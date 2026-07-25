"""共享密钥 Bearer Token 认证中间件 + SSRF 防护。"""

from __future__ import annotations

import logging
import os
import re
import secrets
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_TOKEN_PATH = str(Path.home() / ".fusion" / "multi-node" / ".cluster_token")

SAFE_NODE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_\-]{0,63}$")


def generate_cluster_token() -> str:
    token = secrets.token_urlsafe(32)
    logger.info("集群共享密钥已生成")
    return token


def load_or_create_token(token_path: str = DEFAULT_TOKEN_PATH) -> str:
    path = Path(token_path)
    if path.exists():
        try:
            token = path.read_text().strip()
            if token:
                return token
        except Exception as e:
            logger.warning(f"读取集群密钥失败: {e}")
    token = generate_cluster_token()
    save_token(token, token_path)
    return token


def save_token(token: str, token_path: str = DEFAULT_TOKEN_PATH) -> None:
    path = Path(token_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(token)
    os.chmod(path, 0o600)
    logger.debug(f"集群密钥已保存: {token_path}")


def validate_node_id(node_id: str) -> bool:
    if not SAFE_NODE_ID_PATTERN.match(node_id):
        logger.warning(f"SSRF 防护: node_id 不合法: {node_id!r}")
        return False
    if ".." in node_id:
        logger.warning(f"SSRF 防护: node_id 包含路径遍历: {node_id!r}")
        return False
    return True


def sanitize_node_url_part(node_id: str) -> str:
    if not validate_node_id(node_id):
        raise ValueError(f"不合法的 node_id: {node_id!r}")
    return node_id


class BearerAuthMiddleware:
    """Bearer Token 认证中间件 — 纯 ASI 实现，避免 BaseHTTPMiddleware 问题。"""

    EXEMPT_PATHS = {"/api/health", "/docs", "/openapi.json", "/redoc", "/", "/favicon.ico"}

    def __init__(self, app, shared_token: str):
        self.app = app
        self._expected = shared_token

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in self.EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return

        # 提取 Authorization header
        auth_header = b""
        for name, value in scope.get("headers", []):
            if name == b"authorization":
                auth_header = value
                break

        if not auth_header.startswith(b"Bearer "):
            logger.warning(f"认证失败: 缺少 Bearer token ({path})")
            from starlette.responses import JSONResponse
            response = JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            await response(scope, receive, send)
            return

        token = auth_header[7:].decode("utf-8", errors="replace")
        if not secrets.compare_digest(token, self._expected):
            logger.warning(f"认证失败: token 不匹配 ({path})")
            from starlette.responses import JSONResponse
            response = JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)

"""共享密钥 Bearer Token 认证中间件 + SSRF 防护。"""

from __future__ import annotations

import ipaddress
import logging
import os
import re
import secrets
import socket
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_TOKEN_PATH = str(Path.home() / ".fusion" / "multi-node" / ".cluster_token")

SAFE_NODE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_\-]{0,63}$")

# SSRF 拒绝主机名（云元数据端点等），禁止出站请求
SSRF_BLOCKED_HOSTNAMES = frozenset(
    {
        "metadata.google.internal",
        "metadata",
        "169.254.169.254",
        "metadata.azure.com",
        "169.254.169.254.nip.io",
    }
)


def generate_cluster_token() -> str:
    token = secrets.token_urlsafe(32)
    logger.info("集群共享密钥已生成")
    return token


def load_or_create_token(token_path: str = DEFAULT_TOKEN_PATH) -> str:
    # 容器/多节点共享密钥: 优先读 FUSION_CLUSTER_TOKEN 环境变量。
    # 多容器部署 (docker compose) 经 env 注入同一 token, 避免各容器自生成不一致。
    env_token = os.environ.get("FUSION_CLUSTER_TOKEN", "").strip()
    if env_token:
        logger.info("集群共享密钥已从 FUSION_CLUSTER_TOKEN 环境变量加载")
        return env_token
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


def is_safe_path_segment(value: str) -> bool:
    """路径段安全校验 — 防 ../ 路径穿越与非法字符。

    仅允许字母数字 _ - 点，禁分隔符与穿越序列。
    """
    if not value or len(value) > 128:
        return False
    if "/" in value or "\\" in value or "\x00" in value:
        return False
    if value in (".", ".."):
        return False
    if value.startswith(".") and value not in (".",):
        # 允许点开头但不允许仅 ../ 序列；二次确认
        if ".." in value:
            return False
    return bool(re.match(r"^[a-zA-Z0-9_.\-]+$", value))


def is_safe_peer_host(host: str) -> bool:
    """出站对端主机 SSRF 防护 — 拒绝环回/链路本地/未指定/多播 + 元数据主机名。

    注意: 允许私网 (RFC1918)，集群节点跑在局域网。
    """
    if not host or not isinstance(host, str):
        return False
    host = host.strip()
    if not host:
        return False
    # 拒绝携带凭据/路径/查询的 URL 片段
    if "@" in host or "/" in host or "?" in host or "#" in host:
        return False
    if host.lower() in SSRF_BLOCKED_HOSTNAMES:
        logger.warning(f"SSRF 拦截: 元数据主机名 {host!r}")
        return False
    # 尝试按 IP 校验
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        if ip.is_loopback or ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved:
            logger.warning(f"SSRF 拦截: 受限 IP {host!r}")
            return False
        return True
    # 域名: 拒绝 localhost 与内嵌 IP 表示
    if host.lower() == "localhost":
        logger.warning(f"SSRF 拦截: localhost 主机名 {host!r}")
        return False
    if not re.match(r"^[a-zA-Z0-9._\-]+$", host):
        return False
    # 解析域名，若解析到受限 IP 则拒绝
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        logger.warning(f"SSRF 防护: 无法解析主机名 {host!r}")
        return False
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if ip.is_loopback or ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved:
            logger.warning(f"SSRF 拦截: 主机名 {host!r} 解析到受限 IP {addr!r}")
            return False
    return True


def validate_node_id(node_id: str) -> bool:
    """node_id 路径段安全校验（防路径穿越）。

    注: 本函数只做路径段过滤，不做 SSRF 主机校验。
    SSRF 校验请用 is_safe_peer_host。保留旧名向后兼容。
    """
    if not is_safe_path_segment(node_id):
        logger.warning(f"node_id 不合法: {node_id!r}")
        return False
    return True


def is_registerable_host(host: str) -> bool:
    """节点注册期主机校验 — 比 is_safe_peer_host 宽松: 允许 loopback + 私网 (单机/可信 LAN 部署)。

    拒绝: 元数据主机名、链路本地、未指定、多播、保留段、携带凭据/路径的 URL 片段。
    register_node 调用; 出站派发/cancel/KV 仍用更严的 is_safe_peer_host (拒 loopback)。

    H1 (AR 审计 #24): 注册时不校验 ip 致恶意节点入库, 出站路径才拦 — 入口补一道,
    挡掉云元数据/链路本地等明显恶意, 同时不破坏单机 127.0.0.1 + ASGI 测试。
    """
    if not host or not isinstance(host, str):
        return False
    host = host.strip()
    if not host:
        return False
    if "@" in host or "/" in host or "?" in host or "#" in host:
        return False
    if host.lower() in SSRF_BLOCKED_HOSTNAMES:
        logger.warning(f"注册拒绝: 元数据主机名 {host!r}")
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        # 允许 loopback (单机部署) + 私网; 拒链路本地/未指定/多播/保留
        if ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved:
            logger.warning(f"注册拒绝: 受限 IP {host!r}")
            return False
        return True
    if host.lower() == "localhost":
        return True
    if not re.match(r"^[a-zA-Z0-9._\-]+$", host):
        return False
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        logger.warning(f"注册拒绝: 无法解析主机名 {host!r}")
        return False
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            continue
        if ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved:
            logger.warning(f"注册拒绝: 主机名 {host!r} 解析到受限 IP {info[4][0]!r}")
            return False
    return True


def sanitize_node_url_part(node_id: str) -> str:
    if not validate_node_id(node_id):
        raise ValueError(f"不合法的 node_id: {node_id!r}")
    return node_id


def build_safe_url(scheme: str, host: str, port: int, path: str) -> str:
    """构建出站 URL — 对端主机走 SSRF 校验，path 走基础过滤。"""
    if not is_safe_peer_host(host):
        raise ValueError(f"不安全的对端主机: {host!r}")
    if not re.match(r"^/[a-zA-Z0-9._\-/]*$", path):
        raise ValueError(f"不安全的 path: {path!r}")
    return f"{scheme}://{host}:{int(port)}{path}"


def is_safe_outbound_host(host: str) -> bool:
    """轻量出站主机校验 — 不做 DNS 解析 (节点以 node_id 作 host 时不可解析, 仍合法)。

    拒绝: 元数据主机名、链路本地、未指定、多播、携带凭据/路径的 URL 片段、内嵌 IP 表示受限段。
    允许: loopback、私网、合法域名/标识符 (含不可解析 node_id)。

    用途: KV 跨节点路径 (lookup_remote/transfer/warm) — node_id 直接当 host,
    is_safe_peer_host 会 DNS 解析拒不可解析名 → 误杀测试/节点标识。此函数只挡
    明确恶意 (H3 AR #24), 不要求可解析。
    """
    if not host or not isinstance(host, str):
        return False
    host = host.strip()
    if not host:
        return False
    if "@" in host or "/" in host or "?" in host or "#" in host:
        return False
    if host.lower() in SSRF_BLOCKED_HOSTNAMES:
        logger.warning(f"SSRF 拦截 (出站): 元数据主机名 {host!r}")
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        if ip.is_loopback:
            return True  # 单机部署合法
        if ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved:
            logger.warning(f"SSRF 拦截 (出站): 受限 IP {host!r}")
            return False
        return True
    if host.lower() == "localhost":
        return True
    # 合法标识符/域名 (含不可解析 node_id) — 不强制 DNS 解析
    return bool(re.match(r"^[a-zA-Z0-9._\-]+$", host))


class BearerAuthMiddleware:
    """Bearer Token 认证中间件 — 纯 ASI 实现，避免 BaseHTTPMiddleware 问题。

    GAP-8 (Phase F1): 双令牌分流。user_store 注入后:
    - fmu_ 前缀 → 用户令牌路径: UserStore.validate → 注入 scope["user_id"]/["user_role"]。
    - 其余 → cluster_token 路径 (secrets.compare_digest, O(1), 不变)。
    user_store=None (单租户零配置) → 纯 cluster_token, 字节级等同旧行为。
    """

    EXEMPT_PATHS = {
        "/api/health",
        "/api/health/deep",
        "/health",
        "/health/deep",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/",
        "/favicon.ico",
    }

    USER_TOKEN_PREFIX = "fmu_"

    def __init__(self, app, shared_token: str, audit_logger=None, user_store=None):
        self.app = app
        self._expected = shared_token
        self._audit = audit_logger
        self._user_store = user_store

    def _client_ip(self, scope) -> str:
        client = scope.get("client")
        if client:
            return client[0]
        return ""

    def _audit_fail(self, scope, detail: str, actor: str = "") -> None:
        if self._audit is None:
            return
        self._audit.log(
            actor=actor or self._client_ip(scope) or "unknown",
            action="auth_fail",
            path=scope.get("path", ""),
            method=scope.get("method", ""),
            result="denied",
            detail=detail,
        )

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
            self._audit_fail(scope, "缺少 Bearer token")
            from starlette.responses import JSONResponse

            response = JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            await response(scope, receive, send)
            return

        token = auth_header[7:].decode("utf-8", errors="replace")

        # 用户令牌路径 (fmu_ 前缀) — 仅 user_store 注入时启用
        if self._user_store is not None and token.startswith(self.USER_TOKEN_PREFIX):
            result = self._user_store.validate(token)
            if result is None:
                logger.warning(f"认证失败: 用户令牌校验失败 ({path})")
                self._audit_fail(scope, "用户令牌校验失败")
                from starlette.responses import JSONResponse

                response = JSONResponse(status_code=401, content={"detail": "Unauthorized"})
                await response(scope, receive, send)
                return
            user_id, user_role = result
            # 注入 scope — handler 经 scope["user_id"]/["user_role"] 取身份
            scope["user_id"] = user_id
            scope["user_role"] = user_role.value
            logger.debug(f"用户令牌认证通过: user={user_id} role={user_role.value} ({path})")
            await self.app(scope, receive, send)
            return

        # fmu_ 前缀但本服务无 user_store (agent/未配 master) — 显式拒: 集群内部流量不携带用户令牌。
        # 落到下面 compare_digest 也会 401, 但显式分支给正确审计 detail (用户令牌误用于节点路由)。
        if token.startswith(self.USER_TOKEN_PREFIX):
            logger.warning(f"认证失败: 用户令牌不可用于本路由 ({path})")
            self._audit_fail(scope, "用户令牌不可用于节点路由")
            from starlette.responses import JSONResponse

            response = JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            await response(scope, receive, send)
            return

        # 集群令牌路径 (cluster_token) — 热路径, O(1)
        if not secrets.compare_digest(token, self._expected):
            logger.warning(f"认证失败: token 不匹配 ({path})")
            self._audit_fail(scope, "token 不匹配")
            from starlette.responses import JSONResponse

            response = JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)

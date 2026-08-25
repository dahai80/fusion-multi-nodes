"""M1-05 手动 IP 加入 — mDNS 失败时的 IP 直连回退机制。

提供 join_by_ip() 方法，允许 Agent 通过已知的 Master IP:Port 直接注册，
无需 mDNS 发现。同时提供 Master 端的 /api/join 端点。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass
class JoinRequest:
    """手动加入请求。"""

    node_id: str
    hostname: str
    ip_address: str
    port: int
    cluster_secret: str = ""
    capabilities: list[str] = None  # will default in __post_init__

    def __post_init__(self):
        if self.capabilities is None:
            self.capabilities = ["inference"]


@dataclass
class JoinResponse:
    """手动加入响应。"""

    success: bool
    master_host: str = ""
    master_port: int = 0
    node_id: str = ""
    error: str = ""
    token: str = ""


class ManualJoinClient:
    """手动加入客户端 — Agent 端使用。

    用法:
        client = ManualJoinClient(node_id="node-1")
        result = await client.join("192.168.1.100", 11452)
    """

    def __init__(self, node_id: str = "", cluster_secret: str = "", timeout: float = 10.0):
        self.node_id = node_id
        self._cluster_secret = cluster_secret
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def join(
        self,
        master_host: str,
        master_port: int = 11452,
        hostname: str = "",
        ip_address: str = "",
        agent_port: int = 11458,
        capabilities: list[str] | None = None,
    ) -> JoinResponse:
        """通过 IP 直连方式加入集群。"""
        import platform

        hostname = hostname or platform.node()
        ip_address = ip_address or self._get_local_ip()

        req = JoinRequest(
            node_id=self.node_id,
            hostname=hostname,
            ip_address=ip_address,
            port=agent_port,
            cluster_secret=self._cluster_secret,
            capabilities=capabilities or ["inference"],
        )

        try:
            client = await self._get_client()
            url = f"http://{master_host}:{master_port}/api/join"
            resp = await client.post(
                url,
                json={
                    "node_id": req.node_id,
                    "hostname": req.hostname,
                    "ip_address": req.ip_address,
                    "port": req.port,
                    "cluster_secret": req.cluster_secret,
                    "capabilities": req.capabilities,
                },
            )
            data = resp.json()

            if resp.status_code == 200 and data.get("status") == "ok":
                logger.info(f"手动加入成功: {master_host}:{master_port}")
                return JoinResponse(
                    success=True,
                    master_host=master_host,
                    master_port=master_port,
                    node_id=data.get("node_id", req.node_id),
                    token=data.get("token", ""),
                )
            else:
                error = data.get("detail", data.get("error", "未知错误"))
                logger.warning(f"手动加入失败: {error}")
                return JoinResponse(success=False, error=error)

        except httpx.ConnectError:
            error = f"无法连接 Master: {master_host}:{master_port}"
            logger.error(error)
            return JoinResponse(success=False, error=error)
        except Exception as e:
            error = f"手动加入异常: {e}"
            logger.error(error)
            return JoinResponse(success=False, error=error)

    async def verify_master(self, master_host: str, master_port: int = 11452) -> bool:
        """验证 Master 是否可达。"""
        try:
            client = await self._get_client()
            resp = await client.get(f"http://{master_host}:{master_port}/api/health")
            data = resp.json()
            return resp.status_code == 200 and data.get("role") == "master"
        except Exception:
            return False

    def _get_local_ip(self) -> str:
        import socket

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()


class ManualJoinManager:
    """手动加入管理器 — Master 端使用。

    管理 /api/join 请求的审批流程，与 NodeApprovalManager 集成。
    """

    def __init__(self, cluster_secret: str = "", auto_approve: bool = True):
        self._cluster_secret = cluster_secret
        self._auto_approve = auto_approve
        self._join_history: list[dict[str, Any]] = []
        self._max_history = 500

    def handle_join_request(self, request_data: dict[str, Any]) -> dict[str, Any]:
        """处理手动加入请求。

        返回:
            {"status": "ok", "node_id": ..., "token": ...} 或
            {"status": "error", "detail": ...}
        """
        node_id = request_data.get("node_id", "")
        if not node_id:
            return {"status": "error", "detail": "缺少 node_id"}

        # 集群密钥验证
        if self._cluster_secret:
            req_secret = request_data.get("cluster_secret", "")
            if req_secret != self._cluster_secret:
                logger.warning(f"手动加入密钥验证失败: {node_id}")
                return {"status": "error", "detail": "集群密钥验证失败"}

        # 记录加入历史
        self._join_history.append(
            {
                "node_id": node_id,
                "hostname": request_data.get("hostname", ""),
                "ip_address": request_data.get("ip_address", ""),
                "port": request_data.get("port", 0),
                "joined_at": time.time(),
                "auto_approved": self._auto_approve,
            }
        )

        # 清理历史
        if len(self._join_history) > self._max_history:
            self._join_history = self._join_history[-self._max_history :]

        logger.info(f"手动加入成功: {node_id} ({request_data.get('ip_address', '')}:{request_data.get('port', 0)})")

        result = {
            "status": "ok",
            "node_id": node_id,
            "auto_approved": self._auto_approve,
        }

        if not self._auto_approve:
            result["message"] = "等待管理员审批"

        return result

    def get_join_history(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._join_history[-limit:]

    @property
    def join_count(self) -> int:
        return len(self._join_history)

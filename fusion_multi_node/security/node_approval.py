"""M6-03 新节点接入审批 — 防止未授权节点自动加入集群。"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class ApprovalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


@dataclass
class ApprovalRequest:
    request_id: str
    node_id: str
    hostname: str
    ip_address: str
    port: int
    cluster_secret_hash: str
    requested_at: float = 0.0
    status: ApprovalStatus = ApprovalStatus.PENDING
    approved_by: str = ""
    approved_at: float = 0.0
    reject_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.requested_at:
            self.requested_at = time.time()


class NodeApprovalManager:
    """节点接入审批管理器。"""

    def __init__(
        self,
        auto_approve_patterns: list[str] | None = None,
        max_pending: int = 100,
        approval_ttl_seconds: float = 3600.0,
    ):
        self._pending: dict[str, ApprovalRequest] = {}
        self._approved: dict[str, ApprovalRequest] = {}
        self._rejected: dict[str, ApprovalRequest] = {}
        self._auto_patterns = auto_approve_patterns or []
        self._max_pending = max_pending
        self._approval_ttl = approval_ttl_seconds

    def request_join(
        self,
        node_id: str,
        hostname: str,
        ip_address: str,
        port: int,
        cluster_secret_hash: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> ApprovalRequest:
        if node_id in self._approved:
            logger.debug(f"节点已审批通过: {node_id}")
            return self._approved[node_id]
        if node_id in self._rejected:
            logger.debug(f"节点已被拒绝: {node_id}")
            return self._rejected[node_id]
        req = ApprovalRequest(
            request_id=f"apr_{node_id}",
            node_id=node_id,
            hostname=hostname,
            ip_address=ip_address,
            port=port,
            cluster_secret_hash=cluster_secret_hash,
            metadata=metadata or {},
        )
        if self._check_auto_approve(hostname, ip_address):
            req.status = ApprovalStatus.APPROVED
            req.approved_by = "auto"
            req.approved_at = time.time()
            self._approved[node_id] = req
            logger.info(f"节点自动审批通过: {node_id} ({hostname}/{ip_address})")
            return req
        self._pending[node_id] = req
        if len(self._pending) > self._max_pending:
            self._cleanup_expired_pending()
        logger.info(f"节点加入请求: {node_id} ({hostname}/{ip_address}), 等待审批")
        return req

    def approve(self, node_id: str, approved_by: str = "admin") -> bool:
        req = self._pending.pop(node_id, None)
        if not req:
            logger.warning(f"审批失败: 无待审批请求 {node_id}")
            return False
        req.status = ApprovalStatus.APPROVED
        req.approved_by = approved_by
        req.approved_at = time.time()
        self._approved[node_id] = req
        logger.info(f"节点审批通过: {node_id} (by {approved_by})")
        return True

    def reject(self, node_id: str, reason: str = "") -> bool:
        req = self._pending.pop(node_id, None)
        if not req:
            logger.warning(f"拒绝失败: 无待审批请求 {node_id}")
            return False
        req.status = ApprovalStatus.REJECTED
        req.reject_reason = reason
        self._rejected[node_id] = req
        logger.info(f"节点审批拒绝: {node_id} ({reason})")
        return True

    def is_approved(self, node_id: str) -> bool:
        return node_id in self._approved

    def get_pending(self) -> list[ApprovalRequest]:
        return list(self._pending.values())

    def get_approved(self) -> list[ApprovalRequest]:
        return list(self._approved.values())

    def revoke_approval(self, node_id: str) -> bool:
        req = self._approved.pop(node_id, None)
        if not req:
            return False
        req.status = ApprovalStatus.REJECTED
        req.reject_reason = "准入资格被撤销"
        self._rejected[node_id] = req
        logger.info(f"节点准入撤销: {node_id}")
        return True

    def _check_auto_approve(self, hostname: str, ip_address: str) -> bool:
        return any(pattern in hostname or pattern in ip_address for pattern in self._auto_patterns)

    def _cleanup_expired_pending(self) -> None:
        now = time.time()
        expired = [nid for nid, req in self._pending.items() if now - req.requested_at > self._approval_ttl]
        for nid in expired:
            del self._pending[nid]
        if expired:
            logger.info(f"清理过期审批请求: {len(expired)} 个")

"""M3-01 Master/Worker 权限隔离 — 基于角色的访问控制。

- MASTER: 全部管理 API（节点/任务/KV/集群统计）
- WORKER: 仅任务执行 + 本地 KV + 硬件信息
- 未授权 API 调用返回 403
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class NodeRole(Enum):
    MASTER = "master"
    WORKER = "worker"


class Permission(Enum):
    NODE_REGISTER = "node:register"
    NODE_LIST = "node:list"
    NODE_DELETE = "node:delete"
    NODE_HEARTBEAT = "node:heartbeat"
    TASK_SUBMIT = "task:submit"
    TASK_LIST = "task:list"
    TASK_CANCEL = "task:cancel"
    TASK_MIGRATE = "task:migrate"
    TASK_EXECUTE = "task:execute"
    KV_REGISTER = "kv:register"
    KV_FIND = "kv:find"
    KV_LOOKUP = "kv:lookup"
    KV_TRANSFER = "kv:transfer"
    CLUSTER_STATS = "cluster:stats"
    APPROVAL_LIST = "approval:list"
    APPROVAL_APPROVE = "approval:approve"
    HARDWARE_READ = "hardware:read"
    AUTOSCALER_MANAGE = "autoscaler:manage"


_ROLE_PERMISSIONS: dict[NodeRole, frozenset[Permission]] = {
    NodeRole.MASTER: frozenset(
        {
            Permission.NODE_REGISTER,
            Permission.NODE_LIST,
            Permission.NODE_DELETE,
            Permission.NODE_HEARTBEAT,
            Permission.TASK_SUBMIT,
            Permission.TASK_LIST,
            Permission.TASK_CANCEL,
            Permission.TASK_MIGRATE,
            Permission.KV_REGISTER,
            Permission.KV_FIND,
            Permission.CLUSTER_STATS,
            Permission.APPROVAL_LIST,
            Permission.APPROVAL_APPROVE,
            Permission.AUTOSCALER_MANAGE,
        }
    ),
    NodeRole.WORKER: frozenset(
        {
            Permission.NODE_HEARTBEAT,
            Permission.TASK_EXECUTE,
            Permission.KV_LOOKUP,
            Permission.KV_TRANSFER,
            Permission.HARDWARE_READ,
        }
    ),
}

_PATH_PERMISSION_MAP: dict[str, Permission] = {
    "/api/nodes/register": Permission.NODE_REGISTER,
    "/api/nodes": Permission.NODE_LIST,
    "/api/nodes/heartbeat": Permission.NODE_HEARTBEAT,
    "/api/tasks/submit": Permission.TASK_SUBMIT,
    "/api/tasks": Permission.TASK_LIST,
    "/api/tasks/cancel": Permission.TASK_CANCEL,
    "/api/tasks/migrate": Permission.TASK_MIGRATE,
    "/api/execute": Permission.TASK_EXECUTE,
    "/api/kv/register": Permission.KV_REGISTER,
    "/api/kv/find": Permission.KV_FIND,
    "/api/kv/lookup": Permission.KV_LOOKUP,
    "/api/kv/transfer": Permission.KV_TRANSFER,
    "/api/cluster/stats": Permission.CLUSTER_STATS,
    "/api/approval/list": Permission.APPROVAL_LIST,
    "/api/approval/approve": Permission.APPROVAL_APPROVE,
    "/api/hardware": Permission.HARDWARE_READ,
    "/api/autoscaler": Permission.AUTOSCALER_MANAGE,
}


@dataclass
class RoleAssignment:
    node_id: str
    role: NodeRole
    assigned_at: float = 0.0
    assigned_by: str = ""


class PermissionManager:
    """权限管理器 — 校验节点对 API 的访问权限。"""

    def __init__(self):
        self._assignments: dict[str, RoleAssignment] = {}

    def assign_role(self, node_id: str, role: NodeRole, assigned_by: str = "system") -> None:
        import time

        self._assignments[node_id] = RoleAssignment(
            node_id=node_id,
            role=role,
            assigned_at=time.time(),
            assigned_by=assigned_by,
        )
        logger.info(f"角色分配: {node_id} → {role.value} (by {assigned_by})")

    def get_role(self, node_id: str) -> NodeRole | None:
        assignment = self._assignments.get(node_id)
        return assignment.role if assignment else None

    def has_permission(self, node_id: str, permission: Permission) -> bool:
        assignment = self._assignments.get(node_id)
        if not assignment:
            return False
        return permission in _ROLE_PERMISSIONS.get(assignment.role, frozenset())

    def check_path_access(self, node_id: str, path: str, method: str = "GET") -> bool:
        if path in (
            "/api/health",
            "/docs",
            "/openapi.json",
            "/redoc",
            "/",
            "/favicon.ico",
        ):
            return True

        permission = _PATH_PERMISSION_MAP.get(path)
        if permission:
            return self.has_permission(node_id, permission)

        for api_path, perm in _PATH_PERMISSION_MAP.items():
            if path.startswith((api_path + "/", api_path)):
                return self.has_permission(node_id, perm)

        if method == "DELETE" and "/api/nodes/" in path:
            return self.has_permission(node_id, Permission.NODE_DELETE)
        if "/cancel" in path:
            return self.has_permission(node_id, Permission.TASK_CANCEL)
        if "/migrate" in path:
            return self.has_permission(node_id, Permission.TASK_MIGRATE)

        logger.warning(f"权限未定义路径: {path} ({method}), 拒绝访问")
        return False

    def get_permissions(self, node_id: str) -> list[Permission]:
        assignment = self._assignments.get(node_id)
        if not assignment:
            return []
        return list(_ROLE_PERMISSIONS.get(assignment.role, frozenset()))

    def remove_assignment(self, node_id: str) -> None:
        self._assignments.pop(node_id, None)
        logger.info(f"角色分配移除: {node_id}")

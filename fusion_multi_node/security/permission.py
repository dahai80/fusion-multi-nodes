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


class UserRole(Enum):
    """用户层角色 (GAP-8 Phase F) — 与 NodeRole 正交, 管多租户 per-user RBAC。

    ADMIN: 全部用户管理 (CRUD/签发/吊销令牌) + 任务操作
    USER:  任务提交/取消/查询 + 推理 (/v1/chat/completions), 不可管理用户
    VIEWER: 只读 (节点/任务列表/集群统计), 不可提交/取消/推理
    """

    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"


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
            Permission.TASK_EXECUTE,
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
    # F3 (#27): agent chat 透传路由 — 同 TASK_EXECUTE 权限 (集群内部 master 派发)。
    "/api/v1/chat/completions": Permission.TASK_EXECUTE,
    "/api/kv/register": Permission.KV_REGISTER,
    "/api/kv/find": Permission.KV_FIND,
    "/api/kv/lookup": Permission.KV_LOOKUP,
    "/api/kv/transfer": Permission.KV_TRANSFER,
    "/api/kv/warm": Permission.KV_TRANSFER,
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


# --- 用户层 RBAC (GAP-8 Phase F1) ---
# UserRole 权限映射 — 与 NodeRole 正交。用户令牌仅 master 用户面路由用,
# agent 路由拒 fmu_ 前缀 (集群内部流量从不携带用户凭据)。
_USER_PERMISSION = "user_perm"  # 占位, 实际用下面的集合
_USER_ROLE_PERMISSIONS: dict[UserRole, frozenset[str]] = {
    UserRole.ADMIN: frozenset(
        {
            "user:manage",
            "task:submit",
            "task:cancel",
            "task:list",
            "task:migrate",
            "task:degrade",
            "node:list",
            "cluster:stats",
            "observability:read",
            "chat:complete",
            "kv:read",
        }
    ),
    UserRole.USER: frozenset(
        {
            "task:submit",
            "task:cancel",
            "task:list",
            "node:list",
            "cluster:stats",
            "observability:read",
            "chat:complete",
            "kv:read",
        }
    ),
    UserRole.VIEWER: frozenset(
        {
            "task:list",
            "node:list",
            "cluster:stats",
            "observability:read",
        }
    ),
}

# 用户面路由 → 所需权限。master handler 用 check_user_path_access 鉴权。
# 注意: 用户管理路由 (CRUD/令牌) 仅 ADMIN; chat 路由 USER+。
_USER_PATH_PERMISSION_MAP: dict[tuple[str, str], str] = {
    # 任务操作
    ("POST", "/api/tasks/submit"): "task:submit",
    ("POST", "/api/v1/tasks/submit"): "task:submit",
    ("POST", "/api/tasks/cancel"): "task:cancel",
    ("POST", "/api/v1/tasks"): "task:cancel",
    ("GET", "/api/tasks"): "task:list",
    ("GET", "/api/v1/tasks"): "task:list",
    ("POST", "/api/tasks/migrate"): "task:migrate",
    ("POST", "/api/v1/tasks/migrate"): "task:migrate",
    ("POST", "/api/tasks/degrade"): "task:degrade",
    ("POST", "/api/v1/tasks/degrade"): "task:degrade",
    # 节点/集群只读
    ("GET", "/api/nodes"): "node:list",
    ("GET", "/api/v1/nodes"): "node:list",
    ("GET", "/api/cluster/stats"): "cluster:stats",
    ("GET", "/api/v1/cluster/stats"): "cluster:stats",
    # 观测
    ("GET", "/api/v1/observability/suggestions"): "observability:read",
    ("GET", "/api/v1/observability/alerts"): "observability:read",
    # P1-6 (审计 §3.4): 观测日志导出含他租户 task, 仅 ADMIN (user:manage 同级敏感)。
    ("GET", "/api/v1/observability/logs/export"): "observability:read",
    # P1-6: Prometheus 指标端点 — VIEWER 只读可读 (集群聚合 + 节点级, 无租户隔离泄露)。
    ("GET", "/api/v1/metrics"): "cluster:stats",
    ("GET", "/api/v1/nodes/metrics"): "cluster:stats",  # /{node_id}/metrics 前缀命中
    # P1-6: 配置热加载 / autoscaler 管理 — 仅 ADMIN。
    ("POST", "/api/v1/config/reload"): "user:manage",
    ("PUT", "/api/v1/autoscaler/config"): "user:manage",
    ("GET", "/api/v1/autoscaler/config"): "user:manage",
    # KV 读 (USER 可查本租户; 写走集群内部 cluster_token)。
    # GET /api/kv/find/{model_name} — path-param, 由下方前缀匹配命中。
    ("GET", "/api/kv/find"): "kv:read",
    ("GET", "/api/v1/kv/find"): "kv:read",
    # 推理代理
    ("POST", "/v1/chat/completions"): "chat:complete",
    # 用户管理 (仅 ADMIN)
    ("POST", "/api/v1/users"): "user:manage",
    ("GET", "/api/v1/users"): "user:manage",
    ("DELETE", "/api/v1/users"): "user:manage",
    ("POST", "/api/v1/users/tokens"): "user:manage",
    ("DELETE", "/api/v1/users/tokens"): "user:manage",
    # P1-6 (审计 §3.4): 集群内部路由 — 仅 cluster_token (leader/standby/agent 互信),
    # 用户令牌全拒。CLUSTER_INTERNAL sentinel 不在任何角色权限集 → 用户令牌必拒。
    # 集群令牌在 _enforce_user_rbac 提前返 "" (role is None) 不经此函数, 不受影响。
    ("POST", "/api/ha/vote"): "CLUSTER_INTERNAL",
    ("POST", "/api/ha/sync-tasks"): "CLUSTER_INTERNAL",
    ("POST", "/api/ha/sync-state"): "CLUSTER_INTERNAL",
    ("POST", "/api/ha/heartbeat"): "CLUSTER_INTERNAL",
    ("POST", "/api/nodes/register"): "CLUSTER_INTERNAL",
    ("POST", "/api/v1/nodes/register"): "CLUSTER_INTERNAL",
    ("POST", "/api/nodes/heartbeat"): "CLUSTER_INTERNAL",
    ("POST", "/api/sync/incremental"): "CLUSTER_INTERNAL",
    ("POST", "/api/join"): "CLUSTER_INTERNAL",
    ("POST", "/api/nodes/approve"): "CLUSTER_INTERNAL",
    ("POST", "/api/nodes/reject"): "CLUSTER_INTERNAL",
    ("POST", "/api/kv/register"): "CLUSTER_INTERNAL",
    ("POST", "/api/kv/sync"): "CLUSTER_INTERNAL",
}

# P1-5 (审计 §3.4): 健康检查/文档/根 — 任何令牌放行 (鉴权中间件已豁免, 此处双保险)。
# 集群内部纯节点级路由 (agent 侧) 不经 master user-RBAC, 不在此列。
_USER_EXEMPT_PATHS = frozenset(
    {
        "/api/health",
        "/health",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/",
        "/favicon.ico",
    }
)


def check_user_path_access(role: UserRole, path: str, method: str = "GET") -> bool:
    """用户层路径鉴权 — 查 (method, path) 所需权限, 验角色是否持有。

    P1-5 (审计 §3.4) fail-closed: 未登记路径不再默认放行 (旧 fail-open 可让用户令牌
    调任意未登记路由)。健康/文档豁免路径白名单放行; 其余未登记 → 拒 (用户令牌不该
    到达集群内部路由, 到达即拒, 交调用方提示用 cluster_token)。
    CLUSTER_INTERNAL sentinel = 仅 cluster_token, 用户令牌任何角色皆拒。
    """
    if path in _USER_EXEMPT_PATHS:
        return True
    perm = _USER_PATH_PERMISSION_MAP.get((method, path))
    if perm is None:
        # 路径前缀匹配 (带 id 的子路径, 如 /api/v1/users/<id>)
        for (m, p), pm in _USER_PATH_PERMISSION_MAP.items():
            if m == method and (path == p or path.startswith(p + "/")):
                perm = pm
                break
    # F2: 动态 task 子路径 /api/tasks/{task_id}/<op> — op 在尾部, 非固定前缀。
    # 前缀匹配够不到 (path=/api/tasks/ghost/cancel 不 startswith /api/tasks/cancel/)。
    # 按 task 父路径 + 尾部 op 联合判定: /api/tasks 分段后末段为 cancel/migrate/degrade 即命中。
    if perm is None and path.startswith("/api/tasks/"):
        tail = path.rsplit("/", 1)[-1]
        op_perm = {
            "cancel": "task:cancel",
            "migrate": "task:migrate",
            "degrade": "task:degrade",
        }.get(tail)
        if op_perm is not None:
            perm = op_perm
    if perm is None:
        # P1-5: fail-closed — 未登记路径拒用户令牌 (不再默认放行)。
        logger.warning(f"用户 RBAC 未登记路径, fail-closed 拒绝: {method} {path} role={role.value}")
        return False
    if perm == "CLUSTER_INTERNAL":
        # 仅 cluster_token 可达; 用户令牌任何角色皆拒。
        logger.warning(f"集群内部路由拒用户令牌: {method} {path} role={role.value}")
        return False
    allowed = _USER_ROLE_PERMISSIONS.get(role, frozenset())
    return perm in allowed

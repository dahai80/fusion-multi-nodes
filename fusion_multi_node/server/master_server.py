"""Master FastAPI 服务层 — 集群管理 HTTP API。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from starlette.responses import JSONResponse, StreamingResponse

from fusion_multi_node.master import (
    ClusterMaster,
    ClusterSyncManager,
    ClusterTask,
    KVCacheEntry,
    NodeInfo,
    NodeStatus,
    ParallelMode,
    TaskStatus,
)
from fusion_multi_node.master.load_metrics import LoadMetrics, RoutingStrategy
from fusion_multi_node.security.mtls import client_kwargs as mtls_client_kwargs
from fusion_multi_node.security.mtls import scheme as mtls_scheme
from fusion_multi_node.security.permission import NodeRole, PermissionManager, UserRole, check_user_path_access
from fusion_multi_node.utils.auth import (
    BearerAuthMiddleware,
    build_safe_url,
    is_safe_path_segment,
    is_safe_peer_host,
    load_or_create_token,
)

if TYPE_CHECKING:
    # P2-20 (审计 §6.8): /api/v1/config/reload 热加载 — 注入 ClusterConfig 类型提示。
    from fusion_multi_node.config import ClusterConfig

logger = logging.getLogger(__name__)

# P1-17 (审计 §6.7): 多节点协议版本兼容校验。
# master 接 agent 注册时比对 protocol_version — 低于 MIN 则拒 (schema 不匹配行为未定义),
# 并给降级指引 (须升级至 >= MIN)。空串 (旧客户端/直测) 放行 + warn (灰度期向后兼容)。
MIN_COMPAT_PROTOCOL_VERSION = "0.8.0"


def _parse_version(v: str) -> tuple[int, ...]:
    """解析 '0.8.7' → (0, 8, 7); 非法段当 0。空串 → ()。"""
    parts = []
    for seg in v.split("."):
        seg = seg.strip()
        if seg.isdigit():
            parts.append(int(seg))
        else:
            break
    return tuple(parts)


def _check_protocol_compat(agent_version: str) -> tuple[bool, str]:
    """返回 (compatible, detail)。compatible=False 时 detail 为拒因+降级指引。"""
    if not agent_version:
        return True, "agent 未上报 protocol_version (旧客户端), 放行 (灰度兼容)"
    av = _parse_version(agent_version)
    mn = _parse_version(MIN_COMPAT_PROTOCOL_VERSION)
    if not av:
        return True, f"agent protocol_version 非标准格式 ({agent_version!r}), 放行 (灰度兼容)"
    if av < mn:
        return False, (
            f"协议版本不兼容: agent {agent_version} < master 最低 {MIN_COMPAT_PROTOCOL_VERSION}。"
            f"请升级该节点 fusion-multi-node 至 >= {MIN_COMPAT_PROTOCOL_VERSION} 后重新加入集群"
        )
    return True, f"协议版本兼容: agent {agent_version} >= {MIN_COMPAT_PROTOCOL_VERSION}"


try:
    from importlib.metadata import version as _pkg_version

    _VERSION = _pkg_version("fusion-multi-node")
except Exception:
    _VERSION = "0.2.0"


# ── Pydantic 请求/响应模型 ──


class NodeRegisterRequest(BaseModel):
    node_id: str
    hostname: str
    ip_address: str
    port: int
    arch: str = "arm64"
    total_memory_gb: float = 0.0
    available_memory_gb: float = 0.0
    cpu_cores: int = 0
    gpu_cores: int = 0
    device_model: str = ""
    uma_size_gb: float = 0.0
    mlx_version: str = ""
    # P1-17 (审计 §6.7): 多节点协议版本 (fusion-multi-node __version__), master 比对兼容性。
    protocol_version: str = ""
    role: str = "worker"
    tags: list[str] = []
    active_tasks: int = 0
    max_tasks: int = 4
    network_rtt_ms: float = 0.0


class HeartbeatRequest(BaseModel):
    node_id: str
    total_memory_gb: float | None = None
    available_memory_gb: float | None = None
    active_tasks: int | None = None


class FaultReportRequest(BaseModel):
    node_id: str
    fault_type: str
    message: str


class TaskSubmitRequest(BaseModel):
    name: str
    mode: str = "data"
    model_name: str = ""
    model_id: str | None = None
    timeout_seconds: float = 300.0
    user: str = ""
    required_capability: str = ""
    preferred_node_id: str = ""
    # #31 重试节点规避: 硬黑名单 (绝不派发到列表内节点); 优先 preferred 健康节点
    exclude_nodes: list[str] = []
    priority: int = 0
    # P1 派发载荷 — 透传到 agent /api/execute (task_type + params)
    task_type: str = "inference"
    prompt: str = ""
    messages: list[dict[str, Any]] = []
    max_tokens: int = 2048
    temperature: float = 0.7


class TaskCancelRequest(BaseModel):
    reason: str = ""


# GAP-8 (Phase F2): 用户管理 CRUD 请求模型 — 仅 ADMIN 可调 (user:manage 权限)。
class UserCreateRequest(BaseModel):
    user_id: str
    role: str = "user"
    password: str = ""


class UserTokenIssueRequest(BaseModel):
    label: str = ""


class UserRoleUpdateRequest(BaseModel):
    role: str


class ChatCompletionsProxyRequest(BaseModel):
    # F3 (#27): master /v1/chat/completions 代理体 — 透传到 agent /api/v1/chat/completions。
    # 字段对齐 OpenAI chat 格式; extra 透传采样参数 (top_p/top_k/...)。
    model: str
    messages: list[dict[str, Any]] = []
    temperature: float = 0.7
    max_tokens: int = 4096
    stream: bool = False
    extra: dict[str, Any] = {}


class KVRegisterRequest(BaseModel):
    cache_id: str
    model_name: str
    node_id: str
    size_mb: float
    ttl_seconds: float = 3600.0


class LoadUpdateRequest(BaseModel):
    node_id: str
    uma_used_ratio: float = 0.0
    cpu_percent: float = 0.0
    metal_util: float = 0.0
    task_queue_len: int = 0
    net_rtt_ms: float = 0.0


class TaskResponse(BaseModel):
    task_id: str
    name: str
    mode: str
    model_name: str
    status: str
    assigned_nodes: list[str]
    created_at: float
    started_at: float
    completed_at: float
    error: str


class NodeResponse(BaseModel):
    node_id: str
    hostname: str
    ip_address: str
    port: int
    status: str
    total_memory_gb: float
    available_memory_gb: float
    cpu_cores: int
    gpu_cores: int
    device_model: str
    uma_size_gb: float
    active_tasks: int
    max_tasks: int
    score: float
    last_heartbeat: float


# ── Master Server ──


class MasterServer:
    """集群 Master HTTP 服务。"""

    def __init__(
        self,
        master: ClusterMaster | None = None,
        shared_token: str | None = None,
        config: ClusterConfig | None = None,
    ):
        self.master = master or ClusterMaster()
        self.app = FastAPI(title="Fusion Multi-Node Master", version=_VERSION)
        self._shared_token = shared_token or load_or_create_token()
        # P2-20 (审计 §6.8): 持有 ClusterConfig 供 /api/v1/config/reload 热重载。
        self._cluster_config = config
        # GAP-8 (Phase F1): 用户令牌存储 — 多租户 per-user 鉴权。load_user_store() 无文件/无 env
        # 时返回 None → 中间件回退纯 cluster_token (单租户零配置向后兼容)。FUSION_BOOTSTRAP_ADMIN
        # env 指定首启引导 ADMIN 用户名 (无用户库时自动创建并签发首个令牌, 记日志)。
        from fusion_multi_node.security.user_store import load_user_store

        self._user_store = load_user_store()
        bootstrap_admin = os.environ.get("FUSION_BOOTSTRAP_ADMIN", "").strip()
        if self._user_store is not None and bootstrap_admin and self._user_store.is_empty():
            token = self._user_store.bootstrap_admin(bootstrap_admin)
            if token:
                logger.warning(
                    f"首启引导 ADMIN 用户已创建: {bootstrap_admin} — 首个令牌已签发 (仅此一次显示, "
                    f"请妥善保存)。后续用户管理经 /api/v1/users API。"
                )
        # GAP-8: 审计日志 — 记节点注册/审批/鉴权失败/权限拒绝/任务提交取消等安全动作, 追加写 JSONL。
        # 须在 BearerAuthMiddleware 之前实例化 — 中间件经 audit_logger 参数引用。
        from fusion_multi_node.security.audit_log import get_audit_logger

        self._audit = get_audit_logger()
        self.app.add_middleware(
            BearerAuthMiddleware,
            shared_token=self._shared_token,
            audit_logger=self._audit,
            user_store=self._user_store,
        )
        # P2-22 (审计 §3.8): Master 无限流 → /api/nodes/register /api/join /api/ha/vote
        # /api/tasks/submit 无节流 → DoS + 审批队列 (max_pending=100) 耗尽。加全局限流。
        # 阈值高于 agent (集群内部流量: heartbeat 10s×N 节点 + 派发), 120 req/60s/IP。
        # 健康检查/指标/Prometheus 采集/SSE 高频或长连, 豁免避免误杀。
        from fusion_multi_node.server.agent_server import InMemoryRateLimiter, RateLimitMiddleware

        self._rate_limiter = InMemoryRateLimiter(max_requests=120, window_seconds=60.0)
        self.app.add_middleware(RateLimitMiddleware, limiter=self._rate_limiter)
        self._permission_manager = PermissionManager()
        self._permission_manager.assign_role("master", NodeRole.MASTER, "system")
        self._uvicorn_server: Any | None = None
        try:
            from fusion_multi_node.security.node_approval import NodeApprovalManager

            # FUSION_AUTO_APPROVE_PATTERNS: 逗号分隔的自动审批模式。
            # CIDR 优先 (如 "172.16.0.0/12" 精确匹配私网, 避免 "172." 子串过匹配公网);
            # 非 CIDR 回退子串/通配 (兼容旧 "192.168." / "192.168.*" 配置)。
            # 用于可信 LAN / 容器集群免审批自动加入 (生产: 仅对可信网段开放)。
            raw_patterns = os.environ.get("FUSION_AUTO_APPROVE_PATTERNS", "").strip()
            auto_patterns = [p.strip() for p in raw_patterns.split(",") if p.strip()]
            self._approval_manager = NodeApprovalManager(auto_approve_patterns=auto_patterns)
            if auto_patterns:
                logger.info(f"节点自动审批模式已启用: {auto_patterns}")
        except Exception:
            self._approval_manager = None
        # E1: ClusterSyncManager 一次性构造于 __init__, 接入 start()/stop() 生命周期。
        # 不再在路由内懒初始化 (避免 GET 读请求产生写副作用 + 并发首请求竞争 + 挂上去不 start 变死实例)。
        self._sync_manager = ClusterSyncManager(node_id="master")
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        async def _check_permission(node_id: str, path: str, method: str = "GET") -> bool:
            return self._permission_manager.check_path_access(node_id, path, method)

        # GAP-8 (Phase F2): 用户令牌身份解析 + per-user RBAC。
        # 中间件对 fmu_ 令牌注入 scope["user_id"]/["user_role"] (auth.py)。
        # _resolve_actor 取已认证 user_id (用户令牌) 或回退 X-Node-Id (集群令牌, 内部可信)。
        # _enforce_user_rbac 仅对用户令牌鉴权 (check_user_path_access); 集群令牌走 node-RBAC,
        # 用户层鉴权不适用 (内部流量无用户身份)。返回 actor: 用户令牌=user_id, 集群令牌=""。
        def _resolve_actor(request) -> str:
            scope = request.scope
            uid = scope.get("user_id")
            if uid:
                return uid
            # 集群令牌 — 无用户身份, 回退 X-Node-Id (内部调用方标识)
            for name, value in scope.get("headers", []):
                if name == b"x-node-id":
                    return value.decode("utf-8", errors="replace")
            return ""

        def _user_token_role(request) -> UserRole | None:
            scope = request.scope
            role_v = scope.get("user_role")
            if not role_v:
                return None
            try:
                return UserRole(role_v)
            except ValueError:
                return None

        def _enforce_user_rbac(request, path: str, method: str) -> str:
            """用户令牌鉴权关卡 — 返回已认证 actor (user_id)。

            集群令牌 (无 user_id) → 返回 "" 跳过用户层鉴权 (交 node-RBAC)。
            用户令牌但权限不足 → 抛 403 + 审计 permission_deny。
            """
            role = _user_token_role(request)
            if role is None:
                return ""  # 集群令牌, 非用户面调用
            actor = _resolve_actor(request)
            if not check_user_path_access(role, path, method):
                logger.warning(f"用户权限不足: user={actor} role={role.value} {method} {path}")
                self._audit.log(
                    actor=actor,
                    action="permission_deny",
                    path=path,
                    method=method,
                    result="denied",
                    detail=f"用户权限不足: role={role.value} {method} {path}",
                )
                raise HTTPException(status_code=403, detail=f"用户权限不足: {method} {path}")
            return actor

        @app.get("/api/health")
        @app.get("/health")
        async def health():
            # C11 (AR 审计 #24): liveness 不再恒 ok — 检本地依赖 (磁盘/内存/task-store 可写)。
            # 仍快 (无 HTTP 出站, 无锁), 供 start.sh/docker healthcheck 起 master。
            checks = self._liveness_checks()
            ok = all(checks.values())
            status = "ok" if ok else "degraded"
            logger.debug(f"master liveness: {status} checks={checks}")
            return {"status": status, "role": "master", "checks": checks}

        @app.get("/api/health/deep")
        @app.get("/health/deep")
        async def health_deep():
            # C11: readiness — liveness + 节点 quorum (≥1 ONLINE 节点)。
            # 编排器/LB 据此 drain 半坏 master: 本机健康但无可用节点 → 不 ready。
            checks = self._liveness_checks()
            online_nodes = [n for n in self.master.nodes.values() if n.status == NodeStatus.ONLINE]
            checks["node_quorum"] = len(online_nodes) > 0
            checks["online_nodes"] = len(online_nodes)
            ok = all(v for k, v in checks.items() if k != "online_nodes")
            status = "ok" if ok else "degraded"
            logger.info(f"master readiness: {status} checks={checks}")
            return {"status": status, "role": "master", "checks": checks}

        # ── 集群同步 API (Issue #5) ──

        @app.get("/api/models/{model_name}/manifest")
        async def get_model_manifest(model_name: str):
            manifest = self._sync_manager.get_manifest(model_name)
            return manifest.to_dict()

        @app.post("/api/sync/incremental")
        async def incremental_sync(req: dict):
            model_name = req.get("model_name", "")
            source_host = req.get("source_host", "")
            source_port = req.get("source_port", 11452)
            remote_manifest_data = req.get("remote_manifest", {})
            if not model_name or not source_host:
                raise HTTPException(status_code=400, detail="model_name and source_host required")
            from fusion_multi_node.master.cluster_sync import ModelManifest

            remote_manifest = ModelManifest.from_dict(remote_manifest_data) if remote_manifest_data else None
            if not remote_manifest:
                try:
                    import httpx

                    if not is_safe_peer_host(source_host):
                        raise HTTPException(status_code=400, detail=f"不安全对端主机: {source_host!r}")
                    if not is_safe_path_segment(model_name):
                        raise HTTPException(status_code=400, detail=f"非法 model_name: {model_name!r}")
                    client = httpx.AsyncClient(timeout=30.0, **mtls_client_kwargs())
                    url = build_safe_url(mtls_scheme(), source_host, source_port, f"/api/models/{model_name}/manifest")
                    resp = await client.get(url)
                    remote_manifest = ModelManifest.from_dict(resp.json())
                    await client.aclose()
                except HTTPException:
                    raise
                except Exception as e:
                    raise HTTPException(status_code=502, detail=f"获取远端 manifest 失败: {e}")
            result = await self._sync_manager.incremental_sync(model_name, remote_manifest, source_host, source_port)
            return result

        @app.get("/api/cluster/status")
        async def cluster_sync_status():
            return {"partition": self._sync_manager.get_cluster_status(), "sync_available": True}

        @app.get("/api/nodes/{node_id}/load")
        async def get_node_load(node_id: str):
            node = await self.master.get_node(node_id)
            if not node:
                raise HTTPException(status_code=404, detail=f"节点 {node_id} 不存在")
            # P1-10: collect_load_report 同步阻塞 (psutil.cpu_percent 100ms +
            # system_profiler 至 10s) — async handler 内须 to_thread 移出 event loop (审计 §4.1)。
            report = await asyncio.to_thread(self._sync_manager.collect_load_report)
            return report.to_dict()

        # M1-05 手动 IP 加入 — 走审批门，默认不自动注册
        @app.post("/api/join")
        async def manual_join(req: dict):
            from fusion_multi_node.discovery.manual_join import ManualJoinManager

            if not hasattr(self, "_join_manager"):
                cluster_secret = getattr(self.master, "_cluster_secret", "")
                self._join_manager = ManualJoinManager(cluster_secret=cluster_secret, auto_approve=False)
            result = self._join_manager.handle_join_request(req)
            if result.get("status") != "ok":
                raise HTTPException(status_code=400, detail=result.get("detail", "加入失败"))
            node_id = req.get("node_id", "")
            if not node_id or not is_safe_path_segment(node_id):
                raise HTTPException(status_code=400, detail="缺少或非法 node_id")
            # 仅当显式自动审批通过才注册；否则进入待审批，等待 /api/nodes/approve
            if result.get("auto_approved", False) and self._approval_manager is None:
                node = NodeInfo(
                    node_id=node_id,
                    hostname=req.get("hostname", ""),
                    ip_address=req.get("ip_address", ""),
                    port=req.get("port", 11458),
                    status=NodeStatus.ONLINE,
                    last_heartbeat=time.time(),
                )
                allowed = await self.master.register_node(node)
                if not allowed:
                    raise HTTPException(status_code=403, detail=f"节点 {node_id} 处于 ban 期, 拒绝加入")
            elif self._approval_manager is not None:
                self._approval_manager.request_join(
                    node_id=node_id,
                    hostname=req.get("hostname", ""),
                    ip_address=req.get("ip_address", ""),
                    port=req.get("port", 11458),
                )
                result["status"] = "ok"
                result["auto_approved"] = False
                result["message"] = "等待管理员审批"
            return result

        # ── 节点管理 ──

        @app.post("/api/nodes/register")
        async def register_node(req: NodeRegisterRequest):
            if not is_safe_path_segment(req.node_id):
                raise HTTPException(status_code=400, detail="非法 node_id")
            # P1-17 (审计 §6.7): 协议版本兼容校验 — 低于最低兼容版本拒注册并给降级指引。
            ok, detail = _check_protocol_compat(req.protocol_version)
            if not ok:
                logger.warning(f"节点注册拒 (协议不兼容): {req.node_id} — {detail}")
                raise HTTPException(status_code=400, detail=detail)
            if req.protocol_version == "":
                logger.info(f"节点 {req.node_id} {detail}")
            role = self._permission_manager.get_role(req.node_id)
            if role is not None and not await _check_permission(req.node_id, "/api/nodes/register", "POST"):
                self._audit.log(
                    actor=req.node_id,
                    action="permission_deny",
                    path="/api/nodes/register",
                    method="POST",
                    node_id=req.node_id,
                    result="denied",
                    detail="权限不足: node register",
                )
                raise HTTPException(status_code=403, detail="权限不足: node register")
            if self._approval_manager:
                approval = self._approval_manager.request_join(
                    node_id=req.node_id,
                    hostname=req.hostname,
                    ip_address=req.ip_address,
                    port=req.port,
                    metadata={
                        "total_memory_gb": req.total_memory_gb,
                        "available_memory_gb": req.available_memory_gb,
                        "max_tasks": req.max_tasks,
                        "cpu_cores": req.cpu_cores,
                        "gpu_cores": req.gpu_cores,
                        "arch": req.arch,
                        "device_model": req.device_model,
                        "uma_size_gb": req.uma_size_gb,
                        "mlx_version": req.mlx_version,
                        "tags": req.tags,
                    },
                )
                if approval.status.value != "approved":
                    logger.warning(f"节点注册被拒绝: {req.node_id} (未审批)")
                    self._audit.log(
                        actor=req.node_id,
                        action="register",
                        path="/api/nodes/register",
                        method="POST",
                        node_id=req.node_id,
                        result="denied",
                        detail=f"未通过审批, 当前状态: {approval.status.value}",
                    )
                    raise HTTPException(
                        status_code=403,
                        detail=f"节点 {req.node_id} 未通过审批，当前状态: {approval.status.value}",
                    )
            # 角色由 Master 决定，不信任 req.role 自声明 —— 远程注册节点恒为 WORKER
            assigned_role = NodeRole.WORKER
            node = NodeInfo(
                node_id=req.node_id,
                hostname=req.hostname,
                ip_address=req.ip_address,
                port=req.port,
                arch=req.arch,
                total_memory_gb=req.total_memory_gb,
                available_memory_gb=req.available_memory_gb,
                cpu_cores=req.cpu_cores,
                gpu_cores=req.gpu_cores,
                device_model=req.device_model,
                uma_size_gb=req.uma_size_gb,
                mlx_version=req.mlx_version,
                role=assigned_role.value,
                status=NodeStatus.ONLINE,
                tags=req.tags,
                active_tasks=req.active_tasks,
                max_tasks=req.max_tasks,
                network_rtt_ms=req.network_rtt_ms,
                last_heartbeat=time.time(),
            )
            allowed = await self.master.register_node(node)
            if not allowed:
                self._audit.log(
                    actor=req.node_id,
                    action="register",
                    path="/api/nodes/register",
                    method="POST",
                    node_id=req.node_id,
                    result="denied",
                    detail="处于 ban 期, 拒绝注册",
                )
                raise HTTPException(status_code=403, detail=f"节点 {req.node_id} 处于 ban 期, 拒绝注册")
            self._permission_manager.assign_role(req.node_id, assigned_role, "register")
            logger.info(f"节点注册: {req.node_id} ({req.ip_address}:{req.port}) role={assigned_role.value}")
            self._audit.log(
                actor=req.node_id,
                action="register",
                path="/api/nodes/register",
                method="POST",
                node_id=req.node_id,
                result="ok",
                detail=f"role={assigned_role.value} from {req.ip_address}:{req.port}",
            )
            return {"status": "ok", "node_id": req.node_id}

        @app.post("/api/nodes/approve")
        async def approve_node(req: dict):
            if not self._approval_manager:
                raise HTTPException(status_code=400, detail="审批管理器未启用")
            node_id = req.get("node_id", "")
            approved_by = req.get("approved_by", "admin")
            if not node_id:
                raise HTTPException(status_code=400, detail="缺少 node_id")
            ok = self._approval_manager.approve(node_id, approved_by)
            if not ok:
                raise HTTPException(status_code=404, detail=f"无待审批请求: {node_id}")
            logger.info(f"节点审批通过: {node_id} (by {approved_by})")
            self._audit.log(
                actor=approved_by,
                action="approve",
                path="/api/nodes/approve",
                method="POST",
                node_id=node_id,
                result="ok",
                detail=f"审批通过 by {approved_by}",
            )
            # 审批通过即注册入集群 — 否则 agent 只发心跳找不到节点, 永不上线。
            # approval_manager._approved 缓存了 join 时上报的 hostname/ip/port + metadata
            # (memory/max_tasks/cpu/...); 注册 NodeInfo 从 metadata 还原, 否则 0 内存 → 派发失败。
            approved_req = self._approval_manager._approved.get(node_id)
            if approved_req is not None and node_id not in self.master.nodes:
                md = approved_req.metadata or {}
                node = NodeInfo(
                    node_id=node_id,
                    hostname=approved_req.hostname,
                    ip_address=approved_req.ip_address,
                    port=approved_req.port,
                    arch=md.get("arch", ""),
                    total_memory_gb=md.get("total_memory_gb", 0.0),
                    available_memory_gb=md.get("available_memory_gb", 0.0),
                    cpu_cores=md.get("cpu_cores", 0),
                    gpu_cores=md.get("gpu_cores", 0),
                    device_model=md.get("device_model", ""),
                    uma_size_gb=md.get("uma_size_gb", 0.0),
                    mlx_version=md.get("mlx_version", ""),
                    role=NodeRole.WORKER.value,
                    status=NodeStatus.ONLINE,
                    tags=md.get("tags", []),
                    active_tasks=0,
                    max_tasks=md.get("max_tasks", 4),
                    last_heartbeat=time.time(),
                )
                allowed = await self.master.register_node(node)
                if not allowed:
                    raise HTTPException(status_code=403, detail=f"节点 {node_id} 处于 ban 期, 拒绝加入")
                logger.info(f"审批节点已注册入集群: {node_id} @ {approved_req.ip_address}:{approved_req.port}")
            return {"status": "ok", "node_id": node_id, "approved_by": approved_by}

        @app.post("/api/nodes/reject")
        async def reject_node(req: dict):
            if not self._approval_manager:
                raise HTTPException(status_code=400, detail="审批管理器未启用")
            node_id = req.get("node_id", "")
            reason = req.get("reason", "")
            if not node_id:
                raise HTTPException(status_code=400, detail="缺少 node_id")
            ok = self._approval_manager.reject(node_id, reason)
            if not ok:
                raise HTTPException(status_code=404, detail=f"无待审批请求: {node_id}")
            logger.info(f"节点审批拒绝: {node_id} (reason={reason})")
            self._audit.log(
                actor="master",
                action="reject",
                path="/api/nodes/reject",
                method="POST",
                node_id=node_id,
                result="ok",
                detail=f"审批拒绝 reason={reason}",
            )
            return {"status": "ok", "node_id": node_id, "rejected": True}

        @app.get("/api/nodes/pending")
        async def list_pending():
            if not self._approval_manager:
                return {"pending": []}
            pending = [
                {
                    "node_id": r.node_id,
                    "hostname": r.hostname,
                    "ip_address": r.ip_address,
                    "port": r.port,
                    "requested_at": r.requested_at,
                }
                for r in self._approval_manager._pending.values()
            ]
            return {"pending": pending}

        @app.post("/api/nodes/heartbeat")
        async def heartbeat(req: HeartbeatRequest):
            ok = await self.master.update_heartbeat(
                req.node_id,
                total_memory_gb=req.total_memory_gb,
                available_memory_gb=req.available_memory_gb,
                active_tasks=req.active_tasks,
            )
            if not ok:
                raise HTTPException(status_code=404, detail=f"节点 {req.node_id} 未注册")
            return {"status": "ok"}

        @app.post("/api/nodes/fault")
        async def report_fault(req: FaultReportRequest):
            ok = await self.master.report_fault(req.node_id, req.fault_type, req.message)
            if not ok:
                raise HTTPException(status_code=404, detail=f"节点 {req.node_id} 未注册")
            return {"status": "ok"}

        @app.post("/api/nodes/load")
        async def update_load(req: LoadUpdateRequest):
            metrics = LoadMetrics(
                uma_used_ratio=req.uma_used_ratio,
                cpu_percent=req.cpu_percent,
                metal_util=req.metal_util,
                task_queue_len=req.task_queue_len,
                net_rtt_ms=req.net_rtt_ms,
                node_id=req.node_id,
            )
            await self.master.update_node_load(req.node_id, metrics)
            return {"status": "ok"}

        @app.post("/api/routing/strategy")
        async def set_routing_strategy(strategy: str):
            try:
                rs = RoutingStrategy(strategy)
                self.master.load_router.set_strategy(rs)
                return {"status": "ok", "strategy": rs.value}
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效策略: {strategy}")

        @app.get("/api/routing/summary")
        async def routing_summary():
            return self.master.load_router.get_cluster_load_summary()

        @app.get("/api/nodes")
        async def list_nodes():
            online = await self.master.get_online_nodes()
            all_nodes = await self.master.snapshot_nodes()
            return {
                "total": len(all_nodes),
                "online": len(online),
                "nodes": [_node_to_resp(n) for n in all_nodes],
            }

        @app.get("/api/nodes/{node_id}")
        async def get_node(node_id: str):
            node = await self.master.get_node(node_id)
            if not node:
                raise HTTPException(status_code=404, detail=f"节点 {node_id} 不存在")
            return _node_to_resp(node)

        @app.delete("/api/nodes/{node_id}")
        async def unregister_node(node_id: str):
            if not await _check_permission("master", "/api/nodes/", "DELETE"):
                raise HTTPException(status_code=403, detail="权限不足: node delete")
            await self.master.unregister_node(node_id)
            return {"status": "ok"}

        # ── 任务管理 ──

        @app.post("/api/tasks/submit")
        async def submit_task(req: TaskSubmitRequest, request: Request):
            # GAP-8 (Phase F2): 用户令牌 → per-user RBAC + task.user=已认证 user_id (防伪造)。
            # 集群令牌 → node-RBAC (内部可信), task.user=req.user (内部调用方自声明)。
            user_actor = _enforce_user_rbac(request, "/api/tasks/submit", "POST")
            if not await _check_permission("master", "/api/tasks/submit", "POST"):
                self._audit.log(
                    actor="master",
                    action="permission_deny",
                    path="/api/tasks/submit",
                    method="POST",
                    result="denied",
                    detail="权限不足: task submit",
                )
                raise HTTPException(status_code=403, detail="权限不足: task submit")
            # HA standby 守卫: 选举已配置且非 leader → 拒绝提交
            if self.master._election is not None and not self.master._is_leader:
                raise HTTPException(status_code=503, detail="standby 模式, 非 leader 拒绝任务提交")
            mode = ParallelMode.PIPELINE if req.mode == "pipeline" else ParallelMode.DATA
            # F2: 用户令牌 → task.user 取已认证身份 (忽略客户端 req.user, 防伪造审计 actor)。
            # 集群令牌 → req.user (内部可信自声明, HA/CLI/agent 路径)。
            effective_user = user_actor if user_actor else req.user
            task = ClusterTask(
                task_id=f"task_{uuid.uuid4().hex[:12]}",
                name=req.name,
                mode=mode,
                model_name=req.model_name,
                model_id=req.model_id,
                timeout_seconds=req.timeout_seconds,
                user=effective_user,
                created_at=time.time(),
                required_capability=req.required_capability,
                preferred_node_id=req.preferred_node_id,
                # #31 重试节点规避: 透传硬黑名单
                exclude_nodes=list(req.exclude_nodes),
                priority=req.priority,
                task_type=req.task_type,
                params={
                    "prompt": req.prompt,
                    "messages": req.messages,
                    "max_tokens": req.max_tokens,
                    "temperature": req.temperature,
                },
            )
            self._audit.log(
                actor=effective_user or "unknown",
                action="task_submit",
                path="/api/tasks/submit",
                method="POST",
                node_id="",
                result="ok",
                detail=f"task_id={task.task_id} model={req.model_name} mode={req.mode}",
            )
            ok = await self.master.assign_task(task)
            if not ok:
                raise HTTPException(status_code=503, detail="可用节点不足，任务分配失败")
            # P1-H: 任务可能入优先级队列 (节点不足/配额满) → PENDING 状态返回 202。
            if task.status == TaskStatus.PENDING and task.task_id in {t.task_id for t in self.master._pending_queue}:
                resp = _task_to_resp(task)
                resp["queued"] = True
                return JSONResponse(status_code=202, content=resp)
            return _task_to_resp(task)

        @app.get("/api/tasks")
        async def list_tasks():
            all_tasks = await self.master.snapshot_tasks()
            return {
                "total": len(all_tasks),
                "tasks": [_task_to_resp(t) for t in all_tasks],
            }

        # P1-18 (审计 §5.5): 任务状态 SSE 推送 — 客户端无需轮询即知 FAILED/COMPLETED。
        # 必须注册在 /api/tasks/{task_id} 之前, 否则 "events" 被 path param 捕获。
        # BearerAuthMiddleware 已覆盖 /api/* (仅豁免 health/docs), SSE 鉴权同其它端点。
        @app.get("/api/tasks/events")
        async def task_events():
            async def event_stream():
                q = self.master.subscribe_task_events()
                logger.info("SSE /api/tasks/events 客户端连接")
                try:
                    yield 'event: ready\ndata: {"event":"ready"}\n\n'
                    while True:
                        try:
                            payload = await asyncio.wait_for(q.get(), timeout=15.0)
                        except TimeoutError:
                            yield ": keepalive\n\n"
                            continue
                        yield f"event: {payload['event']}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                except asyncio.CancelledError:
                    logger.info("SSE /api/tasks/events 客户端断开")
                    raise
                finally:
                    self.master.unsubscribe_task_events(q)

            return StreamingResponse(
                event_stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                    "Connection": "keep-alive",
                },
            )

        @app.get("/api/tasks/{task_id}")
        async def get_task(task_id: str):
            task = await self.master.get_task(task_id)
            if not task:
                raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
            return _task_to_resp(task)

        # F3 (#27): /v1/chat/completions 轻量代理 — 统一推理入口, 走 select_nodes 路由策略。
        # 非任务流水线 (同步直返, 不进 self.tasks/持久化/优先级队列)。
        # 流程: 用户令牌鉴权 (chat:complete; VIEWER→403) → 租户在途配额 (429 超限)
        # → select_nodes(DATA, count=1) → 转发选中节点 agent /api/v1/chat/completions
        # → agent FusionMLXBackend.chat → 原生 OpenAI 格式直返 / 流式透传。
        # 集群令牌亦放行 (内部调用, 无用户配额 gate) — 走 node-RBAC (master 全权)。
        @app.post("/v1/chat/completions")
        async def chat_completions_proxy(req: ChatCompletionsProxyRequest, request: Request):
            user_actor = _enforce_user_rbac(request, "/v1/chat/completions", "POST")
            # 租户在途配额: 仅用户令牌 gate (集群令牌无租户, 不限)。0=不限直接放行。
            if user_actor:
                if not await self.master.acquire_chat_slot(user_actor):
                    self._audit.log(
                        actor=user_actor,
                        action="chat_quota_exceeded",
                        path="/v1/chat/completions",
                        method="POST",
                        result="denied",
                        detail=f"租户在途推理配额满: user={user_actor}",
                    )
                    raise HTTPException(status_code=429, detail="租户推理并发配额已满, 稍后重试")

            # 槽已占 (用户令牌)。统一 try/finally 释放: 流式在 _relay 内释放 (生成器生命周期),
            # 非流式/异常在此处 finally 释放。stream_released 标记避免双重释放。
            stream_released = False
            try:
                # 节点选择: DATA 单节点, 复用现有路由策略 (负载/本地优先/熔断过滤)。
                nodes = await self.master.select_nodes(ParallelMode.DATA, count=1)
                if not nodes:
                    raise HTTPException(status_code=503, detail="无可用推理节点")
                node = nodes[0]
                # 出站 SSRF 守卫 — build_safe_url + is_safe_peer_host (与派发一致)。
                if not is_safe_peer_host(node.ip_address):
                    raise HTTPException(status_code=503, detail=f"节点 {node.node_id} 非安全对端")
                url = build_safe_url(mtls_scheme(), node.ip_address, node.port, "/api/v1/chat/completions")
                # 复用派发连接池 + token (测试 monkeypatch _get_dispatch_http 即可拦截出站)。
                client = await self.master._get_dispatch_http()
                token = self.master._get_dispatch_token()
                headers = {
                    "Authorization": f"Bearer {token}",
                    "X-Node-Id": "master",
                    "X-Node-Role": "master",
                }
                payload = {
                    "model": req.model,
                    "messages": req.messages,
                    "temperature": req.temperature,
                    "max_tokens": req.max_tokens,
                    "stream": req.stream,
                    "extra": req.extra,
                }
                self._audit.log(
                    actor=user_actor or "master",
                    action="chat",
                    path="/v1/chat/completions",
                    method="POST",
                    node_id=node.node_id,
                    result="ok",
                    detail=f"model={req.model} stream={req.stream}",
                )
                logger.info(
                    f"chat 代理转发: user={user_actor!r} → node={node.node_id} "
                    f"model={req.model} stream={req.stream}"
                )
                if req.stream:
                    # 流式: 透传 agent SSE 字节流到客户端。stream 生命周期绑生成器 (aiter_raw 消费时才取),
                    # 槽在 _relay finally 释放 (生成器结束/断开均触发)。
                    async def _relay():
                        try:
                            async with client.stream(
                                "POST", url, json=payload, headers=headers, timeout=None
                            ) as upstream:
                                async for chunk in upstream.aiter_raw():
                                    yield chunk
                        except Exception as e:
                            logger.error(f"chat 流式代理失败: {e}")
                            yield b'data: {"error":"internal"}\n\n'
                        finally:
                            if user_actor:
                                await self.master.release_chat_slot(user_actor)
                    stream_released = True  # 流式槽交 _relay 释放, 外层 finally 跳过
                    return StreamingResponse(_relay(), media_type="text/event-stream")
                # 非流式: 同步取 agent 响应, 原生 OpenAI 格式直返。
                resp = await client.post(url, json=payload, headers=headers, timeout=120.0)
                if resp.status_code == 429:
                    raise HTTPException(status_code=429, detail="推理后端限流, 稍后重试")
                if resp.status_code != 200:
                    raise HTTPException(status_code=502, detail=f"agent 推理失败: {resp.status_code} {resp.text[:200]}")
                return resp.json()
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"chat 代理失败: {e}")
                raise HTTPException(status_code=502, detail=f"推理代理失败: {str(e)[:200]}")
            finally:
                # 非流式/异常 (含 503/502) 释放槽; 流式槽已在 _relay finally 释放 (跳过防双重)。
                if user_actor and not stream_released:
                    await self.master.release_chat_slot(user_actor)

        @app.post("/api/tasks/{task_id}/cancel")
        async def cancel_task(task_id: str, req: TaskCancelRequest, request: Request):
            # F2: 用户令牌 per-user RBAC; VIEWER 无 task:cancel → 403。
            user_actor = _enforce_user_rbac(request, "/api/tasks/cancel", "POST")
            if not await _check_permission("master", "/api/tasks/cancel", "POST"):
                self._audit.log(
                    actor="master",
                    action="permission_deny",
                    path="/api/tasks/cancel",
                    method="POST",
                    result="denied",
                    detail="权限不足: task cancel",
                )
                raise HTTPException(status_code=403, detail="权限不足: task cancel")
            task = await self.master.get_task(task_id)
            if not task:
                raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
            # 取消前快照受影响节点 (cancel_task 会清空 assigned_nodes), 取消后通知各节点中止运行推理
            affected_nodes = []
            if task:
                affected_nodes = list(task.assigned_nodes)
                for sid in getattr(task, "sub_tasks", []) or []:
                    sub = await self.master.get_task(sid)
                    if sub:
                        affected_nodes.extend(sub.assigned_nodes)
            ok = await self.master.cancel_task(task_id, reason=req.reason, cancel_sub_tasks=True)
            if not ok:
                raise HTTPException(status_code=400, detail=f"任务 {task_id} 取消失败")
            self._audit.log(
                actor=user_actor or "master",
                action="task_cancel",
                path="/api/tasks/cancel",
                method="POST",
                node_id="",
                result="ok",
                detail=f"task_id={task_id} reason={req.reason}",
            )
            # R4: 传播取消到运行中节点 (真中止, 非假动作)。
            # 去重 (子任务 assigned_nodes 可能重叠), 单 AsyncClient 复用 + asyncio.gather 并发通知。
            unique_nodes = list(dict.fromkeys(affected_nodes))
            node_snapshots = {nid: await self.master.get_node(nid) for nid in unique_nodes}
            targets = [(nid, node) for nid, node in node_snapshots.items() if node]

            async def _notify(client, nid, node):
                try:
                    # H2 (AR #24): 出站 SSRF 守卫 — 不裸 f-string, 走 build_safe_url+is_safe_peer_host
                    if not is_safe_peer_host(node.ip_address):
                        logger.warning(f"取消通知跳过非安全对端: {nid} ({node.ip_address!r})")
                        return None
                    url = build_safe_url(mtls_scheme(), node.ip_address, node.port, "/api/tasks/cancel")
                    resp = await client.post(
                        url,
                        json={"task_id": task_id},
                        headers={
                            "Authorization": f"Bearer {self._shared_token}",
                            "X-Node-Id": "master",
                            "X-Node-Role": "master",
                        },
                    )
                    if resp.status_code == 200:
                        return nid
                    logger.warning(f"节点 {nid} 取消通知失败: {resp.status_code}")
                except Exception as e:
                    logger.warning(f"节点 {nid} 取消通知异常: {e}")
                return None

            import asyncio

            import httpx

            notified = []
            if targets:
                async with httpx.AsyncClient(timeout=5.0, **mtls_client_kwargs()) as client:
                    results = await asyncio.gather(*[_notify(client, nid, node) for nid, node in targets])
                    notified = [r for r in results if r]
            return {"status": "cancelled", "task_id": task_id, "notified_nodes": notified}

        # M4-04 任务降级
        @app.post("/api/tasks/{task_id}/degrade")
        async def degrade_task(task_id: str, request: Request):
            # F2: 用户令牌 per-user RBAC; USER 无 task:degrade → 403, ADMIN 有。
            _enforce_user_rbac(request, "/api/tasks/degrade", "POST")
            ok = await self.master.degrade_task(task_id)
            if not ok:
                raise HTTPException(status_code=400, detail=f"任务 {task_id} 降级失败")
            return {"status": "ok", "task_id": task_id}

        @app.post("/api/tasks/{task_id}/migrate")
        async def migrate_task(task_id: str, request: Request):
            # F2: 用户令牌 per-user RBAC; USER 无 task:migrate → 403, ADMIN 有。
            user_actor = _enforce_user_rbac(request, "/api/tasks/migrate", "POST")
            if not await _check_permission("master", "/api/tasks/migrate", "POST"):
                raise HTTPException(status_code=403, detail="权限不足: task migrate")
            ok = await self.master.migrate_task(task_id)
            if not ok:
                raise HTTPException(status_code=500, detail="任务迁移失败")
            self._audit.log(
                actor=user_actor or "master",
                action="task_migrate",
                path="/api/tasks/migrate",
                method="POST",
                node_id="",
                result="ok",
                detail=f"task_id={task_id}",
            )
            return {"status": "ok", "task_id": task_id}

        # ── KV 缓存 ──

        @app.post("/api/kv/register")
        async def register_kv(req: KVRegisterRequest):
            entry = KVCacheEntry(
                cache_id=req.cache_id,
                model_name=req.model_name,
                node_id=req.node_id,
                created_at=time.time(),
                size_mb=req.size_mb,
                ttl_seconds=req.ttl_seconds,
            )
            await self.master.register_kv_cache(entry)
            return {"status": "ok", "cache_id": req.cache_id}

        @app.get("/api/kv/find/{model_name}")
        async def find_kv(model_name: str):
            entry = await self.master.find_kv_cache(model_name)
            if not entry:
                raise HTTPException(status_code=404, detail=f"模型 {model_name} 无可用 KV 缓存")
            return {
                "cache_id": entry.cache_id,
                "model_name": entry.model_name,
                "node_id": entry.node_id,
                "size_mb": entry.size_mb,
                "access_count": entry.access_count,
            }

        # ── 集群统计 ──

        @app.get("/api/cluster/stats")
        async def cluster_stats():
            return await self.master.get_stats()

        # ── M7-06 监控 API ──

        @app.get("/api/v1/cluster/stats")
        async def v1_cluster_stats():
            stats = await self.master.get_stats()
            online = stats.get("online_nodes", 0)
            total = stats.get("total_nodes", 0)
            active = stats.get("active_tasks", 0)
            total_mem = stats.get("total_memory_gb", 0.0)
            avail_mem = stats.get("available_memory_gb", 0.0)
            return {
                "cluster": {
                    "online_nodes": online,
                    "total_nodes": total,
                    "active_tasks": active,
                    "total_memory_gb": round(total_mem, 2),
                    "available_memory_gb": round(avail_mem, 2),
                    "utilization": round(1.0 - avail_mem / max(total_mem, 0.01), 4) if total_mem > 0 else 0.0,
                },
                "tasks": {
                    "total": stats.get("total_tasks", 0),
                    "completed": stats.get("completed_tasks", 0),
                    "failed": stats.get("failed_tasks", 0),
                },
                "load_summary": stats.get("load_summary", {}),
            }

        # ── S2 Prometheus 监控指标端点 ──
        # 集群级聚合: 节点数/任务状态/KV 池/内存/派发延迟分位/重试次数。
        # 纯文本 0.0.4 exposition, 无外部依赖。Bearer 鉴权不豁免 (内部抓取携带 token)。

        @app.get("/api/v1/metrics")
        async def prometheus_metrics():
            from fastapi.responses import PlainTextResponse

            body = await self.master.get_prometheus_metrics()
            return PlainTextResponse(content=body, media_type="text/plain; version=0.0.4; charset=utf-8")

        @app.get("/api/v1/nodes/{node_id}/metrics")
        async def node_metrics(node_id: str):
            node = await self.master.get_node(node_id)
            if not node:
                raise HTTPException(status_code=404, detail=f"节点 {node_id} 不存在")
            load = self.master.load_router._node_loads.get(node_id)
            result = {
                "node_id": node_id,
                "status": node.status.value,
                "role": node.role,
                "score": node.score,
                "available_memory_gb": node.available_memory_gb,
                "total_memory_gb": node.total_memory_gb,
                "active_tasks": node.active_tasks,
                "max_tasks": node.max_tasks,
                "network_rtt_ms": node.network_rtt_ms,
            }
            if load:
                result["load_metrics"] = {
                    "uma_used_ratio": load.uma_used_ratio,
                    "cpu_percent": load.cpu_percent,
                    "metal_util": load.metal_util,
                    "task_queue_len": load.task_queue_len,
                    "net_rtt_ms": load.net_rtt_ms,
                }
            return result

        @app.get("/api/v1/tasks/{task_id}/progress")
        async def task_progress(task_id: str):
            task = await self.master.get_task(task_id)
            if not task:
                raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
            total_shards = len(task.sub_tasks) if task.sub_tasks else 1
            completed_shards = 0
            for stid in task.sub_tasks:
                st = await self.master.get_task(stid)
                if st and st.status == TaskStatus.COMPLETED:
                    completed_shards += 1
            progress = completed_shards / max(total_shards, 1)
            elapsed = time.time() - task.started_at if task.started_at > 0 else 0.0
            remaining = max(task.timeout_seconds - elapsed, 0.0) if task.started_at > 0 else task.timeout_seconds
            return {
                "task_id": task_id,
                "name": task.name,
                "status": task.status.value,
                "progress": round(progress, 3),
                "total_shards": total_shards,
                "completed_shards": completed_shards,
                "assigned_nodes": task.assigned_nodes,
                "elapsed_seconds": round(elapsed, 1),
                "remaining_seconds": round(remaining, 1),
                "model_name": task.model_name,
            }

        @app.get("/api/v1/tasks/{task_id}/timeline")
        async def task_timeline(task_id: str):
            task = await self.master.get_task(task_id)
            if not task:
                raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
            events = []
            if task.created_at > 0:
                events.append(
                    {
                        "timestamp": task.created_at,
                        "event": "created",
                        "detail": f"mode={task.mode.value}",
                    }
                )
            if task.started_at > 0:
                events.append(
                    {
                        "timestamp": task.started_at,
                        "event": "started",
                        "detail": f"nodes={task.assigned_nodes}",
                    }
                )
            if task.degraded_from_model:
                events.append(
                    {
                        "timestamp": task.started_at or task.created_at,
                        "event": "degraded",
                        "detail": f"{task.degraded_from_model} → {task.model_name}",
                    }
                )
            for sub_id in task.sub_tasks:
                sub = await self.master.get_task(sub_id)
                if sub:
                    if sub.started_at > 0:
                        events.append(
                            {
                                "timestamp": sub.started_at,
                                "event": "subtask_started",
                                "detail": f"sub_task={sub_id}",
                            }
                        )
                    if sub.completed_at > 0:
                        events.append(
                            {
                                "timestamp": sub.completed_at,
                                "event": "subtask_completed",
                                "detail": f"sub_task={sub_id}",
                            }
                        )
            if task.completed_at > 0:
                # P3-29: PARTIAL 单独标事件类型 (非 failed — 有部分结果)
                if task.status == TaskStatus.COMPLETED:
                    event_type = "completed"
                elif task.status == TaskStatus.PARTIAL:
                    event_type = "partial"
                else:
                    event_type = "failed"
                events.append(
                    {
                        "timestamp": task.completed_at,
                        "event": event_type,
                        "detail": task.error or "",
                    }
                )
            events.sort(key=lambda e: e["timestamp"])
            return {
                "task_id": task_id,
                "name": task.name,
                "status": task.status.value,
                "events": events,
            }

        # M10-04 Autoscaler 配置热更新
        # GAP-5 (审计 §7): autoscaler 模块未接线 (零实例化, _autoscaler 恒 None)。
        # 旧 GET 返回 {"enabled": False} — 歧义 ("禁用" vs "未实现")。改为显式 503 +
        # 明示未接线, 避免误读为已接但关闭。模块保留待迁移 (非生产路径)。
        @app.get("/api/v1/autoscaler/config")
        async def get_autoscaler_config():
            autoscaler = getattr(self.master, "_autoscaler", None)
            if not autoscaler:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Autoscaler 未接线 (not-wired): 模块存在但未实例化, 不构成调度路径。"
                        "详见 CLAUDE.md GAP-5 / 迁移计划。"
                    ),
                )
            cfg = autoscaler.config
            return {
                "enabled": True,
                "min_nodes": cfg.min_nodes,
                "max_nodes": cfg.max_nodes,
                "scale_up_threshold": cfg.scale_up_threshold,
                "scale_down_threshold": cfg.scale_down_threshold,
                "cooldown_seconds": cfg.cooldown_seconds,
                "idle_timeout_seconds": cfg.idle_timeout_seconds,
                "policy": cfg.policy.value,
                "check_interval": cfg.check_interval,
                "rebalance_threshold": cfg.rebalance_threshold,
            }

        @app.put("/api/v1/autoscaler/config")
        async def update_autoscaler_config(req: dict):
            from fusion_multi_node.autoscaler.autoscaler import (
                AutoscalerConfig,
                ScalePolicy,
            )

            autoscaler = getattr(self.master, "_autoscaler", None)
            if not autoscaler:
                raise HTTPException(
                    status_code=503,
                    detail="Autoscaler 未接线 (not-wired): 模块存在但未实例化。详见 CLAUDE.md GAP-5。",
                )
            if "policy" in req:
                try:
                    policy = ScalePolicy(req["policy"])
                    autoscaler.update_policy(policy)
                    return {
                        "status": "ok",
                        "action": "policy_updated",
                        "policy": policy.value,
                    }
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"无效策略: {req['policy']}")
            try:
                new_config = AutoscalerConfig(
                    min_nodes=req.get("min_nodes", autoscaler.config.min_nodes),
                    max_nodes=req.get("max_nodes", autoscaler.config.max_nodes),
                    scale_up_threshold=req.get("scale_up_threshold", autoscaler.config.scale_up_threshold),
                    scale_down_threshold=req.get("scale_down_threshold", autoscaler.config.scale_down_threshold),
                    cooldown_seconds=req.get("cooldown_seconds", autoscaler.config.cooldown_seconds),
                    idle_timeout_seconds=req.get("idle_timeout_seconds", autoscaler.config.idle_timeout_seconds),
                    policy=autoscaler.config.policy,
                    check_interval=req.get("check_interval", autoscaler.config.check_interval),
                    rebalance_threshold=req.get("rebalance_threshold", autoscaler.config.rebalance_threshold),
                )
                autoscaler.update_config(new_config)
                return {"status": "ok", "action": "config_updated"}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        # M8-02 日志导出
        @app.get("/api/v1/observability/logs/export")
        async def export_logs(fmt: str = "json", since: float = 0.0, node_id: str = ""):
            obs = getattr(self.master, "_observability", None)
            if not obs:
                raise HTTPException(status_code=503, detail="observability not initialized")
            try:
                result = obs.export_logs(fmt=fmt, since=since, node_id=node_id)
                if fmt == "csv":
                    from fastapi.responses import PlainTextResponse

                    return PlainTextResponse(content=result, media_type="text/csv")
                return {"logs": result, "count": len(result)}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        # M8-03 智能优化建议
        @app.get("/api/v1/observability/suggestions")
        async def get_optimization_suggestions():
            obs = getattr(self.master, "_observability", None)
            if not obs:
                return {"suggestions": [], "error": "observability not initialized"}
            try:
                suggestions = obs.generate_optimization_suggestions()
                return {"suggestions": suggestions}
            except Exception as e:
                return {"suggestions": [], "error": str(e)}

        # M8 活跃告警查询
        @app.get("/api/v1/observability/alerts")
        async def get_active_alerts(severity: str = ""):
            obs = getattr(self.master, "_observability", None)
            if not obs:
                return {"alerts": [], "error": "observability not initialized"}
            try:
                raw_alerts = obs.get_active_alerts(severity=severity)
                alerts = []
                for a in raw_alerts:
                    alerts.append(
                        {
                            "alert_id": a.alert_id,
                            "severity": a.severity,
                            "title": a.title,
                            "message": a.message,
                            "node_id": a.node_id,
                            "created_at": a.created_at,
                            "resolved": a.resolved,
                        }
                    )
                return {"alerts": alerts, "count": len(alerts)}
            except Exception as e:
                return {"alerts": [], "error": str(e)}

        # ── GAP-8 (Phase F2): 用户管理 CRUD ──
        # 仅 ADMIN (user:manage 权限, check_user_path_access 把关)。
        # 集群令牌无用户身份 → _enforce_user_rbac 返回 "" 不拦; 但用户管理是用户面能力,
        # 集群令牌 (内部 HA/CLI) 不该建用户 → 额外守卫: 无 user_store 或无 user 身份 → 403/503。
        # 令牌明文仅签发/轮换时返回一次 (与 UserStore 语义一致)。

        def _require_user_store(request: Request):
            """用户管理路由前置 — 须有 user_store + 用户令牌 (ADMIN) 身份。

            返回已认证 actor (ADMIN user_id)。无 user_store → 503; 非用户令牌 → 403。
            """
            if self._user_store is None:
                raise HTTPException(
                    status_code=503,
                    detail="用户管理未启用: 未配置 FUSION_USERS_FILE / users.json (单租户零配置模式)",
                )
            actor = _enforce_user_rbac(request, "/api/v1/users", "POST")
            if not actor:
                # 集群令牌调用户管理 — 内部流量无用户身份, 拒 (用户管理须 ADMIN 用户令牌)
                self._audit.log(
                    actor="",
                    action="permission_deny",
                    path="/api/v1/users",
                    method="POST",
                    result="denied",
                    detail="用户管理须 ADMIN 用户令牌, 集群令牌无权",
                )
                raise HTTPException(status_code=403, detail="用户管理须 ADMIN 用户令牌")
            return actor

        @app.post("/api/v1/users")
        async def create_user_route(req: UserCreateRequest, request: Request):
            admin = _require_user_store(request)
            try:
                role = UserRole(req.role)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"非法角色: {req.role!r} (admin/user/viewer)")
            try:
                self._user_store.create_user(req.user_id, role, req.password)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            logger.info(f"用户创建: {req.user_id} role={role.value} (by {admin})")
            self._audit.log(
                actor=admin,
                action="user_create",
                path="/api/v1/users",
                method="POST",
                result="ok",
                detail=f"user={req.user_id} role={role.value}",
            )
            return JSONResponse(
                status_code=201,
                content={"status": "ok", "user_id": req.user_id, "role": role.value},
            )

        @app.get("/api/v1/users")
        async def list_users_route(request: Request):
            if self._user_store is None:
                raise HTTPException(status_code=503, detail="用户管理未启用")
            _enforce_user_rbac(request, "/api/v1/users", "GET")
            return {"users": self._user_store.list_users(), "total": len(self._user_store.list_users())}

        @app.get("/api/v1/users/{user_id}")
        async def get_user_route(user_id: str, request: Request):
            if self._user_store is None:
                raise HTTPException(status_code=503, detail="用户管理未启用")
            _enforce_user_rbac(request, "/api/v1/users", "GET")
            if not is_safe_path_segment(user_id):
                raise HTTPException(status_code=400, detail="非法 user_id")
            rec = self._user_store.get_user(user_id)
            if rec is None:
                raise HTTPException(status_code=404, detail=f"用户 {user_id} 不存在")
            # 不返回 token_hash/salt (敏感), 仅元信息 + 令牌列表 (tid/label/created_at)
            return {
                "user_id": rec.user_id,
                "role": rec.role.value,
                "created_at": rec.created_at,
                "tokens": [
                    {"tid": t.tid, "label": t.label, "created_at": t.created_at}
                    for t in rec.tokens
                ],
            }

        @app.delete("/api/v1/users/{user_id}")
        async def delete_user_route(user_id: str, request: Request):
            admin = _require_user_store(request)
            if not is_safe_path_segment(user_id):
                raise HTTPException(status_code=400, detail="非法 user_id")
            if user_id == admin:
                raise HTTPException(status_code=400, detail="不可删除自身账户")
            ok = self._user_store.delete_user(user_id)
            if not ok:
                raise HTTPException(status_code=404, detail=f"用户 {user_id} 不存在")
            logger.info(f"用户删除: {user_id} (by {admin})")
            self._audit.log(
                actor=admin,
                action="user_delete",
                path="/api/v1/users",
                method="DELETE",
                result="ok",
                detail=f"user={user_id}",
            )
            return {"status": "ok", "user_id": user_id}

        @app.put("/api/v1/users/{user_id}/role")
        async def update_user_role_route(user_id: str, req: UserRoleUpdateRequest, request: Request):
            admin = _require_user_store(request)
            try:
                role = UserRole(req.role)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"非法角色: {req.role!r}")
            ok = self._user_store.set_role(user_id, role)
            if not ok:
                raise HTTPException(status_code=404, detail=f"用户 {user_id} 不存在")
            logger.info(f"用户角色变更: {user_id} → {role.value} (by {admin})")
            self._audit.log(
                actor=admin,
                action="user_role_update",
                path="/api/v1/users",
                method="PUT",
                result="ok",
                detail=f"user={user_id} role={role.value}",
            )
            return {"status": "ok", "user_id": user_id, "role": role.value}

        @app.post("/api/v1/users/{user_id}/tokens")
        async def issue_user_token_route(user_id: str, req: UserTokenIssueRequest, request: Request):
            admin = _require_user_store(request)
            if not is_safe_path_segment(user_id):
                raise HTTPException(status_code=400, detail="非法 user_id")
            try:
                token = self._user_store.issue_token(user_id, label=req.label)
            except KeyError:
                raise HTTPException(status_code=404, detail=f"用户 {user_id} 不存在")
            logger.info(f"令牌签发: user={user_id} label={req.label!r} (by {admin})")
            self._audit.log(
                actor=admin,
                action="token_issue",
                path="/api/v1/users",
                method="POST",
                result="ok",
                detail=f"user={user_id} label={req.label!r}",
            )
            # 明文仅此一次返回 — 不记审计 (防日志泄露令牌)
            return {"status": "ok", "user_id": user_id, "token": token, "token_shown_once": True}

        @app.delete("/api/v1/users/{user_id}/tokens/{tid}")
        async def revoke_user_token_route(user_id: str, tid: str, request: Request):
            admin = _require_user_store(request)
            ok = self._user_store.revoke_token(user_id, tid)
            if not ok:
                raise HTTPException(status_code=404, detail=f"令牌不存在: user={user_id} tid={tid}")
            logger.info(f"令牌吊销: user={user_id} tid={tid} (by {admin})")
            self._audit.log(
                actor=admin,
                action="token_revoke",
                path="/api/v1/users",
                method="DELETE",
                result="ok",
                detail=f"user={user_id} tid={tid}",
            )
            return {"status": "ok", "user_id": user_id, "tid": tid}

        @app.post("/api/v1/users/{user_id}/tokens/rotate")
        async def rotate_user_token_route(user_id: str, req: UserTokenIssueRequest, request: Request):
            admin = _require_user_store(request)
            if not is_safe_path_segment(user_id):
                raise HTTPException(status_code=400, detail="非法 user_id")
            try:
                token = self._user_store.rotate_user_token(user_id, label=req.label)
            except KeyError:
                raise HTTPException(status_code=404, detail=f"用户 {user_id} 不存在")
            logger.info(f"令牌轮换: user={user_id} (by {admin}) — 旧令牌保留, 需另调吊销")
            self._audit.log(
                actor=admin,
                action="token_rotate",
                path="/api/v1/users",
                method="POST",
                result="ok",
                detail=f"user={user_id} label={req.label!r}",
            )
            return {"status": "ok", "user_id": user_id, "token": token, "token_shown_once": True}

        # ── P2-20 配置热加载 (审计 §6.8) ──
        # 重读 config.json + 重应用运行时可调字段; 须重启字段 (端口/ha_config/mdns) 仅提示不生效。
        @app.post("/api/v1/config/reload")
        async def reload_config():
            cfg = self._cluster_config
            if cfg is None:
                raise HTTPException(
                    status_code=503,
                    detail="config 热加载未启用: MasterServer 未注入 ClusterConfig (启动未传 config)",
                )
            try:
                cfg.load()
            except Exception as e:
                logger.error(f"配置热加载失败: {e}")
                raise HTTPException(status_code=500, detail=f"配置重载失败: {e}") from e
            # 运行时可调字段重应用: 租户并发配额 (configure_scheduling)。
            tenant_max = cfg.get("scheduling.tenant_max_concurrent", 4)
            self.master.configure_scheduling(tenant_max)
            logger.info(f"配置热加载完成: tenant_max_concurrent={tenant_max}")
            return {
                "status": "ok",
                "reloaded": ["scheduling.tenant_max_concurrent"],
                "restart_required": ["cluster.master_host", "cluster.master_port", "ha_config", "mdns"],
                "config_path": cfg.config_path,
            }

        # ── HA 双 Master 选举 + 任务同步 ──
        # POST /api/ha/vote — 对端 master 拉票, 透传 ClusterMaster.handle_vote_request。
        # POST /api/ha/sync-tasks — leader 推送任务快照, standby 合并落盘。
        # 单 Master 模式 (无选举配置) 投票拒绝, 同步忽略。

        @app.post("/api/ha/vote")
        async def ha_vote(req: dict):
            from fusion_multi_node.master.election import VoteRequest, VoteResponse

            try:
                vote_req = VoteRequest(
                    term=int(req.get("term", 0)),
                    candidate_id=str(req.get("candidate_id", "")),
                    candidate_priority=int(req.get("candidate_priority", 0)),
                    last_log_index=int(req.get("last_log_index", 0)),
                    last_log_term=int(req.get("last_log_term", 0)),
                )
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"非法 VoteRequest: {e}")
            resp = await self.master.handle_vote_request(vote_req)
            if not isinstance(resp, VoteResponse):
                resp = VoteResponse(term=0, vote_granted=False, voter_id="")
            return {
                "term": resp.term,
                "vote_granted": resp.vote_granted,
                "voter_id": resp.voter_id,
            }

        @app.post("/api/ha/sync-tasks")
        async def ha_sync_tasks(req: dict):
            tasks = req.get("tasks", [])
            if not isinstance(tasks, list):
                raise HTTPException(status_code=400, detail="tasks 必须为列表")
            merged = await self.master.receive_synced_tasks(tasks)
            return {"status": "ok", "merged": merged}

        # GAP-1 (Phase C): leader 推送全状态 (nodes/kv/banned) 到 standby — always-on failover。
        @app.post("/api/ha/sync-state")
        async def ha_sync_state(req: dict):
            if not isinstance(req, dict):
                raise HTTPException(status_code=400, detail="sync-state payload 必须为对象")
            counts = await self.master.receive_synced_state(req)
            return {"status": "ok", "counts": counts}

        # C1: Leader→Follower 心跳 — 维持 leader 权威, 防 follower 超时误判重选。
        @app.post("/api/ha/heartbeat")
        async def ha_heartbeat(req: dict):
            leader_id = str(req.get("leader_id", ""))
            term = int(req.get("term", 0))
            if not leader_id:
                raise HTTPException(status_code=400, detail="缺少 leader_id")
            await self.master.handle_heartbeat(leader_id, term)
            return {"status": "ok"}

    def _liveness_checks(self) -> dict[str, Any]:
        """C11: 本地 liveness 检查 — 磁盘可写 / 内存充足 / task-store 可写。

        全本地无出站无锁, 供 /api/health (liveness) 与 /api/health/deep (readiness) 复用。
        失败项进 checks 字典返回 (key→bool), 上层据此定 status。
        """
        checks: dict[str, Any] = {}
        try:
            import psutil

            # 磁盘: task-store 所在分区剩余 > 512MB (绝对下限, 兼容 Mac APFS 容器占比失真)
            store_dir = self.master._task_store_path.parent
            if store_dir.exists():
                usage = psutil.disk_usage(str(store_dir))
                checks["disk_ok"] = usage.free > 512 * 1024 * 1024
            else:
                # 目录不存在视作可创建 — 写探针会建
                checks["disk_ok"] = True
            # 内存: 可用 > 256MB (master 自身常驻 + 任务簿)
            mem = psutil.virtual_memory()
            checks["mem_ok"] = mem.available > 256 * 1024 * 1024
        except Exception as e:
            logger.warning(f"liveness 资源采集失败: {e}")
            checks["disk_ok"] = True
            checks["mem_ok"] = True
        # task-store 可写: 原子写探针 (写即删, 不污染 tasks.json)
        try:
            self.master._task_store_path.parent.mkdir(parents=True, exist_ok=True)
            probe = self.master._task_store_path.with_suffix(".health_probe")
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            checks["task_store_writable"] = True
        except Exception as e:
            logger.warning(f"liveness task-store 写探针失败: {e}")
            checks["task_store_writable"] = False
        return checks

    async def start(self, host: str = "127.0.0.1", port: int = 11452, ssl_context=None) -> None:
        import uvicorn

        await self._sync_manager.start()
        ssl_kwargs = {}
        if ssl_context is None:
            from fusion_multi_node.security.mtls import server_ssl_kwargs

            ssl_kwargs = server_ssl_kwargs()
        elif isinstance(ssl_context, dict):
            ssl_kwargs = ssl_context
        config = uvicorn.Config(self.app, host=host, port=port, log_level="warning", **ssl_kwargs)
        self._uvicorn_server = uvicorn.Server(config)
        scheme = "https" if ssl_kwargs else "http"
        logger.info(f"Master 服务启动: {scheme}://{host}:{port}")
        try:
            await self._uvicorn_server.serve()
        except OSError as e:
            # 端口被占用 — 明确报冲突端口 (issue #25 同类: 同机服务撞端口难定位)。
            _CONFLICT = {
                11452: "fusion-multi-node Master (本服务)",
                11458: "fusion-multi-node Agent",
                11445: "fusion-comfyui",
                11432: "fusion-mlx / fusion-gateway",
                11434: "fusion-mlx (monorepo 默认)",
                11450: "fusion-multi-node mDNS",
                11446: "fusion-multi-node MCP/FMP",
            }
            who = _CONFLICT.get(port, "")
            hint = f" (与 {who} 默认端口冲突)" if who else ""
            logger.error(f"Master 端口 {port} bind 失败{hint}: {e}")
            raise OSError(f"端口 {port} 被占用{hint}, 原 OSError: {e}") from e

    async def stop(self) -> None:
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
        await self._sync_manager.stop()
        await self.master.stop()
        logger.info("Master 服务已停止")


def _node_to_resp(n: NodeInfo) -> dict[str, Any]:
    return {
        "node_id": n.node_id,
        "hostname": n.hostname,
        "ip_address": n.ip_address,
        "port": n.port,
        "status": n.status.value,
        "role": n.role,
        "total_memory_gb": n.total_memory_gb,
        "available_memory_gb": n.available_memory_gb,
        "cpu_cores": n.cpu_cores,
        "gpu_cores": n.gpu_cores,
        "device_model": n.device_model,
        "uma_size_gb": n.uma_size_gb,
        "active_tasks": n.active_tasks,
        "max_tasks": n.max_tasks,
        "score": n.score,
        "last_heartbeat": n.last_heartbeat,
    }


def _task_to_resp(t: ClusterTask) -> dict[str, Any]:
    return {
        "task_id": t.task_id,
        "name": t.name,
        "mode": t.mode.value,
        "model_name": t.model_name,
        "status": t.status.value,
        "assigned_nodes": t.assigned_nodes,
        "created_at": t.created_at,
        "started_at": t.started_at,
        "completed_at": t.completed_at,
        "error": t.error,
        "required_capability": t.required_capability,
        "priority": t.priority,
        "degraded_from_model": t.degraded_from_model,
        "degradation_count": t.degradation_count,
        "cancel_reason": t.cancel_reason,
        "sub_tasks": t.sub_tasks,
        "result": t.result,
    }

"""Master FastAPI 服务层 — 集群管理 HTTP API。"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from starlette.responses import JSONResponse

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
from fusion_multi_node.security.permission import NodeRole, PermissionManager
from fusion_multi_node.utils.auth import (
    BearerAuthMiddleware,
    build_safe_url,
    is_safe_path_segment,
    is_safe_peer_host,
    load_or_create_token,
)

logger = logging.getLogger(__name__)

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
    priority: int = 0
    # P1 派发载荷 — 透传到 agent /api/execute (task_type + params)
    task_type: str = "inference"
    prompt: str = ""
    messages: list[dict[str, Any]] = []
    max_tokens: int = 2048
    temperature: float = 0.7


class TaskCancelRequest(BaseModel):
    reason: str = ""


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

    def __init__(self, master: ClusterMaster | None = None, shared_token: str | None = None):
        self.master = master or ClusterMaster()
        self.app = FastAPI(title="Fusion Multi-Node Master", version=_VERSION)
        self._shared_token = shared_token or load_or_create_token()
        self.app.add_middleware(BearerAuthMiddleware, shared_token=self._shared_token)
        self._permission_manager = PermissionManager()
        self._permission_manager.assign_role("master", NodeRole.MASTER, "system")
        self._uvicorn_server: Any | None = None
        try:
            from fusion_multi_node.security.node_approval import NodeApprovalManager

            self._approval_manager = NodeApprovalManager()
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

        @app.get("/api/health")
        @app.get("/health")
        async def health():
            return {"status": "ok", "role": "master"}

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
                    url = build_safe_url(
                        mtls_scheme(), source_host, source_port, f"/api/models/{model_name}/manifest"
                    )
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
            report = self._sync_manager.collect_load_report()
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
            role = self._permission_manager.get_role(req.node_id)
            if role is not None and not await _check_permission(req.node_id, "/api/nodes/register", "POST"):
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
                raise HTTPException(status_code=403, detail=f"节点 {req.node_id} 处于 ban 期, 拒绝注册")
            self._permission_manager.assign_role(req.node_id, assigned_role, "register")
            logger.info(f"节点注册: {req.node_id} ({req.ip_address}:{req.port}) role={assigned_role.value}")
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
        async def submit_task(req: TaskSubmitRequest):
            if not await _check_permission("master", "/api/tasks/submit", "POST"):
                raise HTTPException(status_code=403, detail="权限不足: task submit")
            # HA standby 守卫: 选举已配置且非 leader → 拒绝提交
            if self.master._election is not None and not self.master._is_leader:
                raise HTTPException(status_code=503, detail="standby 模式, 非 leader 拒绝任务提交")
            mode = ParallelMode.PIPELINE if req.mode == "pipeline" else ParallelMode.DATA
            task = ClusterTask(
                task_id=f"task_{uuid.uuid4().hex[:12]}",
                name=req.name,
                mode=mode,
                model_name=req.model_name,
                model_id=req.model_id,
                timeout_seconds=req.timeout_seconds,
                user=req.user,
                created_at=time.time(),
                required_capability=req.required_capability,
                preferred_node_id=req.preferred_node_id,
                priority=req.priority,
                task_type=req.task_type,
                params={
                    "prompt": req.prompt,
                    "messages": req.messages,
                    "max_tokens": req.max_tokens,
                    "temperature": req.temperature,
                },
            )
            ok = await self.master.assign_task(task)
            if not ok:
                raise HTTPException(status_code=503, detail="可用节点不足，任务分配失败")
            # P1-H: 任务可能入优先级队列 (节点不足/配额满) → PENDING 状态返回 202。
            if task.status == TaskStatus.PENDING and task.task_id in {
                t.task_id for t in self.master._pending_queue
            }:
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

        @app.get("/api/tasks/{task_id}")
        async def get_task(task_id: str):
            task = await self.master.get_task(task_id)
            if not task:
                raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
            return _task_to_resp(task)

        @app.post("/api/tasks/{task_id}/cancel")
        async def cancel_task(task_id: str, req: TaskCancelRequest):
            if not await _check_permission("master", "/api/tasks/cancel", "POST"):
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
            # R4: 传播取消到运行中节点 (真中止, 非假动作)。
            # 去重 (子任务 assigned_nodes 可能重叠), 单 AsyncClient 复用 + asyncio.gather 并发通知。
            unique_nodes = list(dict.fromkeys(affected_nodes))
            node_snapshots = {nid: await self.master.get_node(nid) for nid in unique_nodes}
            targets = [(nid, node) for nid, node in node_snapshots.items() if node]

            async def _notify(client, nid, node):
                try:
                    resp = await client.post(
                        f"{mtls_scheme()}://{node.ip_address}:{node.port}/api/tasks/cancel",
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
        async def degrade_task(task_id: str):
            ok = await self.master.degrade_task(task_id)
            if not ok:
                raise HTTPException(status_code=400, detail=f"任务 {task_id} 降级失败")
            return {"status": "ok", "task_id": task_id}

        @app.post("/api/tasks/{task_id}/migrate")
        async def migrate_task(task_id: str):
            if not await _check_permission("master", "/api/tasks/migrate", "POST"):
                raise HTTPException(status_code=403, detail="权限不足: task migrate")
            ok = await self.master.migrate_task(task_id)
            if not ok:
                raise HTTPException(status_code=500, detail="任务迁移失败")
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
                event_type = "completed" if task.status == TaskStatus.COMPLETED else "failed"
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
        @app.get("/api/v1/autoscaler/config")
        async def get_autoscaler_config():
            autoscaler = getattr(self.master, "_autoscaler", None)
            if not autoscaler:
                return {"enabled": False}
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
                raise HTTPException(status_code=404, detail="Autoscaler 未启用")
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
        await self._uvicorn_server.serve()

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

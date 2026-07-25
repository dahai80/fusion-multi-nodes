"""Master FastAPI 服务层 — 集群管理 HTTP API。"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from fusion_multi_node.master import (
    ClusterMaster,
    ClusterTask,
    KVCacheEntry,
    NodeInfo,
    NodeStatus,
    ParallelMode,
    TaskStatus,
)
from fusion_multi_node.utils.auth import BearerAuthMiddleware, load_or_create_token

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
    mlx_version: str = ""
    tags: List[str] = []
    active_tasks: int = 0
    max_tasks: int = 4
    network_rtt_ms: float = 0.0


class HeartbeatRequest(BaseModel):
    node_id: str
    available_memory_gb: Optional[float] = None
    active_tasks: Optional[int] = None


class FaultReportRequest(BaseModel):
    node_id: str
    fault_type: str
    message: str


class TaskSubmitRequest(BaseModel):
    name: str
    mode: str = "data"
    model_name: str = ""
    timeout_seconds: float = 300.0
    user: str = ""


class TaskCancelRequest(BaseModel):
    reason: str = ""


class KVRegisterRequest(BaseModel):
    cache_id: str
    model_name: str
    node_id: str
    size_mb: float
    ttl_seconds: float = 3600.0


class TaskResponse(BaseModel):
    task_id: str
    name: str
    mode: str
    model_name: str
    status: str
    assigned_nodes: List[str]
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
    active_tasks: int
    max_tasks: int
    score: float
    last_heartbeat: float


# ── Master Server ──

class MasterServer:
    """集群 Master HTTP 服务。"""

    def __init__(self, master: Optional[ClusterMaster] = None, shared_token: Optional[str] = None):
        self.master = master or ClusterMaster()
        self.app = FastAPI(title="Fusion Multi-Node Master", version=_VERSION)
        self._shared_token = shared_token or load_or_create_token()
        self.app.add_middleware(BearerAuthMiddleware, shared_token=self._shared_token)
        self._uvicorn_server: Optional[Any] = None
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        @app.get("/api/health")
        async def health():
            return {"status": "ok", "role": "master"}

        # ── 节点管理 ──

        @app.post("/api/nodes/register")
        async def register_node(req: NodeRegisterRequest):
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
                mlx_version=req.mlx_version,
                status=NodeStatus.ONLINE,
                tags=req.tags,
                active_tasks=req.active_tasks,
                max_tasks=req.max_tasks,
                network_rtt_ms=req.network_rtt_ms,
                last_heartbeat=time.time(),
            )
            await self.master.register_node(node)
            logger.info(f"节点注册: {req.node_id} ({req.ip_address}:{req.port})")
            return {"status": "ok", "node_id": req.node_id}

        @app.post("/api/nodes/heartbeat")
        async def heartbeat(req: HeartbeatRequest):
            node = self.master.nodes.get(req.node_id)
            if not node:
                raise HTTPException(status_code=404, detail=f"节点 {req.node_id} 未注册")
            node.last_heartbeat = time.time()
            if req.available_memory_gb is not None:
                node.available_memory_gb = req.available_memory_gb
            if req.active_tasks is not None:
                node.active_tasks = req.active_tasks
            if node.status == NodeStatus.OFFLINE:
                node.status = NodeStatus.ONLINE
                logger.info(f"节点恢复上线: {req.node_id}")
            return {"status": "ok"}

        @app.post("/api/nodes/fault")
        async def report_fault(req: FaultReportRequest):
            logger.warning(f"节点故障上报: {req.node_id} [{req.fault_type}] {req.message}")
            node = self.master.nodes.get(req.node_id)
            if node:
                node.status = NodeStatus.ERROR
            return {"status": "ok"}

        @app.get("/api/nodes")
        async def list_nodes():
            online = await self.master.get_online_nodes()
            return {
                "total": len(self.master.nodes),
                "online": len(online),
                "nodes": [_node_to_resp(n) for n in self.master.nodes.values()],
            }

        @app.get("/api/nodes/{node_id}")
        async def get_node(node_id: str):
            node = self.master.nodes.get(node_id)
            if not node:
                raise HTTPException(status_code=404, detail=f"节点 {node_id} 不存在")
            return _node_to_resp(node)

        @app.delete("/api/nodes/{node_id}")
        async def unregister_node(node_id: str):
            await self.master.unregister_node(node_id)
            return {"status": "ok"}

        # ── 任务管理 ──

        @app.post("/api/tasks/submit")
        async def submit_task(req: TaskSubmitRequest):
            mode = ParallelMode.PIPELINE if req.mode == "pipeline" else ParallelMode.DATA
            task = ClusterTask(
                task_id=f"task_{int(time.time() * 1000)}",
                name=req.name,
                mode=mode,
                model_name=req.model_name,
                timeout_seconds=req.timeout_seconds,
                user=req.user,
                created_at=time.time(),
            )
            ok = await self.master.assign_task(task)
            if not ok:
                raise HTTPException(status_code=503, detail="可用节点不足，任务分配失败")
            return _task_to_resp(task)

        @app.get("/api/tasks")
        async def list_tasks():
            return {
                "total": len(self.master.tasks),
                "tasks": [_task_to_resp(t) for t in self.master.tasks.values()],
            }

        @app.get("/api/tasks/{task_id}")
        async def get_task(task_id: str):
            task = self.master.tasks.get(task_id)
            if not task:
                raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
            return _task_to_resp(task)

        @app.post("/api/tasks/{task_id}/cancel")
        async def cancel_task(task_id: str, req: TaskCancelRequest):
            task = self.master.tasks.get(task_id)
            if not task:
                raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
            if task.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
                raise HTTPException(status_code=400, detail=f"任务 {task_id} 无法取消（状态: {task.status.value}）")
            await self.master.complete_task(task_id, error=f"用户取消: {req.reason}")
            return {"status": "ok", "task_id": task_id}

        @app.post("/api/tasks/{task_id}/migrate")
        async def migrate_task(task_id: str):
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

    async def start(self, host: str = "127.0.0.1", port: int = 9753) -> None:
        import uvicorn
        config = uvicorn.Config(self.app, host=host, port=port, log_level="warning")
        self._uvicorn_server = uvicorn.Server(config)
        logger.info(f"Master 服务启动: {host}:{port}")
        await self._uvicorn_server.serve()

    async def stop(self) -> None:
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
        await self.master.stop()
        logger.info("Master 服务已停止")


def _node_to_resp(n: NodeInfo) -> Dict[str, Any]:
    return {
        "node_id": n.node_id,
        "hostname": n.hostname,
        "ip_address": n.ip_address,
        "port": n.port,
        "status": n.status.value,
        "total_memory_gb": n.total_memory_gb,
        "available_memory_gb": n.available_memory_gb,
        "cpu_cores": n.cpu_cores,
        "gpu_cores": n.gpu_cores,
        "active_tasks": n.active_tasks,
        "max_tasks": n.max_tasks,
        "score": n.score,
        "last_heartbeat": n.last_heartbeat,
    }


def _task_to_resp(t: ClusterTask) -> Dict[str, Any]:
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
    }

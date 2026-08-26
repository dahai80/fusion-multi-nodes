"""Agent FastAPI 服务层 — 节点任务执行与 KV 缓存 HTTP API。"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from fusion_multi_node.agent import NodeAgent
from fusion_multi_node.distributed_mlx import KVSharingManager
from fusion_multi_node.security.permission import NodeRole, PermissionManager
from fusion_multi_node.utils.auth import BearerAuthMiddleware, load_or_create_token

logger = logging.getLogger(__name__)

try:
    from importlib.metadata import version as _pkg_version

    _VERSION = _pkg_version("fusion-multi-node")
except Exception:
    _VERSION = "0.2.0"

ALLOWED_TASK_TYPES = {"inference", "embedding", "plugin", "model_sync", "pipeline_step"}
ALLOWED_EXTRA_KEYS = {"temperature", "top_p", "top_k", "repeat_penalty", "seed"}
# P3 pipeline_step 经 extra 透传的字段 (model_id/layer_range/hidden_states/input_ids/...)。
# hidden_states 为 b64.npy 字符串 (上游 /distributed/* 激活格式)。
PIPELINE_EXTRA_KEYS = {
    "model_id",
    "shard_index",
    "layer_range",
    "hidden_states",
    "input_ids",
    "position_ids",
}


class InMemoryRateLimiter:
    """简易内存速率限制器 — 按 IP 限制请求频率。"""

    _MAX_IP_ENTRIES = 10000
    _CLEANUP_INTERVAL = 100
    _TIME_CLEANUP_INTERVAL = 60.0

    def __init__(self, max_requests: int = 30, window_seconds: float = 60.0):
        self._max = max_requests
        self._window = window_seconds
        self._counts: dict[str, list[float]] = defaultdict(list)
        self._call_count = 0
        self._last_time_cleanup: float = time.time()

    def is_allowed(self, key: str) -> bool:
        now = time.time()
        timestamps = self._counts[key]
        cutoff = now - self._window
        self._counts[key] = [t for t in timestamps if t > cutoff]
        if len(self._counts[key]) >= self._max:
            return False
        self._counts[key].append(now)
        self._call_count += 1
        if self._call_count % self._CLEANUP_INTERVAL == 0:
            self._cleanup_stale(now)
        if now - self._last_time_cleanup >= self._TIME_CLEANUP_INTERVAL:
            self._cleanup_stale(now)
            self._last_time_cleanup = now
        return True

    def _cleanup_stale(self, now: float) -> None:
        cutoff = now - self._window
        stale_keys = [k for k, v in self._counts.items() if not v or v[-1] < cutoff]
        for k in stale_keys:
            del self._counts[k]
        if len(self._counts) > self._MAX_IP_ENTRIES:
            sorted_keys = sorted(
                self._counts,
                key=lambda k: self._counts[k][-1] if self._counts[k] else 0,
            )
            for k in sorted_keys[: len(self._counts) - self._MAX_IP_ENTRIES]:
                del self._counts[k]


class RateLimitMiddleware:
    """全局限流中间件 — 所有 API 端点统一限流。"""

    EXEMPT_PATHS = {
        "/api/health",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/",
        "/favicon.ico",
    }

    def __init__(self, app, limiter: InMemoryRateLimiter | None = None):
        self.app = app
        self._limiter = limiter or InMemoryRateLimiter()

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in self.EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return

        client_ip = "unknown"
        for name, value in scope.get("headers", []):
            if name == b"x-forwarded-for":
                client_ip = value.decode("utf-8", errors="replace").split(",")[0].strip()
                break
        if client_ip == "unknown":
            client = scope.get("client")
            if client:
                client_ip = client[0]

        if not self._limiter.is_allowed(client_ip):
            from starlette.responses import JSONResponse

            response = JSONResponse(status_code=429, content={"detail": "请求过于频繁"})
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)


# ── Pydantic 请求/响应模型 ──


class ExecuteRequest(BaseModel):
    task_type: str = "inference"
    model_name: str = ""
    prompt: str = ""
    messages: list[dict[str, Any]] = []
    max_tokens: int = 2048
    temperature: float = 0.7
    extra: dict[str, Any] = {}


class KGLookupRequest(BaseModel):
    model_name: str
    prompt_hash: str


class KVTransferRequest(BaseModel):
    cache_id: str
    target_node: str
    target_port: int = 11458


class KVWarmRequest(BaseModel):
    model_name: str
    prompt: str
    prompt_hash: str
    # token_count/size 仅记录用, 缺则估 0。
    total_tokens: int = 0
    total_size_bytes: int = 0


class TaskCancelRequest(BaseModel):
    task_id: str


class HealthResponse(BaseModel):
    status: str
    node_id: str
    uptime_seconds: float


# ── Agent Server ──


class AgentServer:
    """节点 Agent HTTP 服务。"""

    def __init__(
        self,
        agent: NodeAgent | None = None,
        kv_manager: KVSharingManager | None = None,
        shared_token: str | None = None,
    ):
        self.agent = agent or NodeAgent()
        self._shared_token = shared_token or load_or_create_token()
        # KVSharingManager 跨节点 HTTP 调用需过对端 Bearer 鉴权 — 透传集群共享 token。
        # 注入者自带 manager 时补齐 token (缺则 401)。
        if kv_manager is not None:
            self.kv_manager = kv_manager
            if not getattr(kv_manager, "_cluster_token", ""):
                kv_manager._cluster_token = self._shared_token
        else:
            self.kv_manager = KVSharingManager(cluster_token=self._shared_token)
        self.app = FastAPI(title="Fusion Multi-Node Agent", version=_VERSION)
        self._rate_limiter = InMemoryRateLimiter()
        self.app.add_middleware(BearerAuthMiddleware, shared_token=self._shared_token)
        self.app.add_middleware(RateLimitMiddleware, limiter=self._rate_limiter)
        # P1-G 细粒度权限 — 本 agent 维护调用方角色表。默认 master 为 MASTER 角色
        # (master 派发任务/取消到本节点须放行)。worker 节点角色由 register_caller 注入。
        # 仅 mTLS 开启时强制校验 (传输已证调用方=集群节点); 关时 X-Node-Id 缺失→放行 (兼容现有测试)。
        self._permission_manager = PermissionManager()
        self._permission_manager.assign_role("master", NodeRole.MASTER, "system")
        self._permission_enforce = False
        try:
            from fusion_multi_node.security.mtls import is_enabled

            self._permission_enforce = is_enabled()
        except Exception:
            self._permission_enforce = False
        self._uvicorn_server: Any | None = None
        self._started_at: float = 0.0
        # 本节点对外可寻址地址 — transfer_from_remote 据此回连本机拉缓存。
        # 默认 127.0.0.1, start() 时更新为实际监听 host。
        self._host: str = "127.0.0.1"
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        async def _check_permission(request: Request, path: str, method: str = "POST") -> None:
            """细粒度权限校验 — 从 X-Node-Id/X-Node-Role header 取调用方身份。

            强制模式 (mTLS 开): 缺 X-Node-Id → 403; 角色无权 → 403。
            兼容模式 (mTLS 关): 缺 header → 放行 (现有 http 测试/CLI 无 header);
              有 header 则按角色校验 (master 派发带 X-Node-Id=master)。
            """
            if not self._permission_enforce:
                node_id = request.headers.get("X-Node-Id", "")
                if not node_id:
                    return
            else:
                node_id = request.headers.get("X-Node-Id", "")
                if not node_id:
                    raise HTTPException(status_code=403, detail="缺少 X-Node-Id 身份头")
            role_hdr = request.headers.get("X-Node-Role", "")
            if role_hdr and self._permission_manager.get_role(node_id) is None:
                role = NodeRole.MASTER if role_hdr == "master" else NodeRole.WORKER
                self._permission_manager.assign_role(node_id, role, "header")
            if not self._permission_manager.check_path_access(node_id, path, method):
                logger.warning(f"权限拒绝: node={node_id} path={path} method={method}")
                raise HTTPException(status_code=403, detail=f"无权访问 {path}")

        @app.get("/api/health")
        @app.get("/health")
        async def health():
            return {"status": "ok"}

        # ── 任务执行 ──

        @app.post("/api/execute")
        async def execute_task(req: ExecuteRequest, request: Request):
            await _check_permission(request, "/api/execute", "POST")
            if req.task_type not in ALLOWED_TASK_TYPES:
                raise HTTPException(status_code=400, detail=f"不合法的任务类型: {req.task_type}")
            filtered_extra = {k: v for k, v in req.extra.items() if k in ALLOWED_EXTRA_KEYS}
            # P3: pipeline_step 字段经 extra 透传到 params (隐藏状态 b64.npy/层段)。
            pipeline_extra = {k: v for k, v in req.extra.items() if k in PIPELINE_EXTRA_KEYS}
            # 消费契约 (见 NodeAgent.execute_task docstring): {task_id, type, model, params}。
            # 旧实现下扁平键 (model_name/prompt/...) 与 _execute_inference 读取的
            # task["task_id"]/task.get("model")/task.get("params",{}) 错位 → KeyError + 空模型/空提示,
            # 因既有测试 mock execute_task 未触达真实消费端, 该契约 bug 长期潜伏。此处对齐。
            params = {
                "prompt": req.prompt,
                "messages": req.messages,
                "max_tokens": req.max_tokens,
                "temperature": req.temperature,
                **filtered_extra,
                **pipeline_extra,
            }
            task = {
                "task_id": "",
                "type": req.task_type,
                "model": req.model_name,
                "model_name": req.model_name,
                "params": params,
            }
            try:
                result = await self.agent.execute_task(task)
                return {"status": "ok", "result": result}
            except Exception as e:
                logger.error(f"任务执行失败: {e}")
                raise HTTPException(status_code=500, detail="内部错误")

        @app.post("/api/tasks/cancel")
        async def cancel_task(req: TaskCancelRequest, request: Request):
            # master 取消通知 → 本节点; worker 节点不应直接取消它节点任务
            await _check_permission(request, "/api/tasks/cancel", "POST")
            # 真取消: 中止运行中推理协程, 非假动作
            ok = await self.agent.cancel_task(req.task_id)
            if not ok:
                raise HTTPException(status_code=404, detail=f"无可取消任务: {req.task_id}")
            return {"status": "cancelled", "task_id": req.task_id}

        # ── KV 缓存 ──

        @app.post("/api/kv/lookup")
        async def kv_lookup(req: KGLookupRequest):
            # 契约对齐 lookup_remote: 回 {"found": True, "entry": 序列化 KVCacheEntry}。
            # 旧扁平 dict 无 "found"/"entry" 键 → lookup_remote 永远返回 None (跨节点复用静默失效)。
            entry = self.kv_manager.lookup_local(req.model_name, req.prompt_hash)
            if not entry:
                raise HTTPException(status_code=404, detail="KV 缓存未找到")
            return {"found": True, "entry": self.kv_manager._serialize_entry(entry)}

        @app.post("/api/kv/transfer")
        async def kv_transfer(req: KVTransferRequest):
            # 推模型: 源节点收到 transfer 请求 → 查本地缓存 → 回传序列化 entry。
            # 不再回调 transfer_from_remote (避免递归)。target_node 仅记录用。
            entry = self.kv_manager.lookup_local_by_id(req.cache_id)
            if entry is None:
                raise HTTPException(status_code=404, detail=f"KV 缓存未找到: {req.cache_id}")
            return {"status": "ok", "entry": self.kv_manager._serialize_entry(entry)}

        @app.post("/api/kv/warm")
        async def kv_warm(req: KVWarmRequest):
            # Worker 端本地预存 — Master/调度器经 KVSharingManager.warm_cache 跨节点分发,
            # 各 Worker 收到此请求只本地 store_local (不再二次远推, 否则递归)。
            # 契约对齐 manager.warm_cache 发送体: {model_name, prompt, prompt_hash}。
            import time

            from fusion_multi_node.distributed_mlx.kv_cache_sharing import (
                KVCacheEntry,
                KVShard,
            )

            entry = KVCacheEntry(
                cache_id=f"warm-{req.prompt_hash}",
                model_name=req.model_name,
                prompt_hash=req.prompt_hash,
                prompt_prefix=req.prompt[:100],
                total_tokens=req.total_tokens,
                total_size_bytes=req.total_size_bytes,
                created_at=time.time(),
                ttl_seconds=3600.0,
                shards=[
                    KVShard(
                        shard_id=f"ws-{req.prompt_hash}",
                        model_name=req.model_name,
                        layer_index=0,
                        node_id=self.agent.config.node_id,
                        token_count=req.total_tokens,
                        size_bytes=req.total_size_bytes,
                        created_at=time.time(),
                    )
                ],
            )
            stored = self.kv_manager.store_local(entry)
            return {"status": "ok" if stored else "skip", "warmed": 1 if stored else 0}

        @app.get("/api/kv/stats")
        async def kv_stats():
            return self.kv_manager.get_stats()

        # ── 硬件信息 ──

        @app.get("/api/hardware")
        async def hardware_info():
            info = self.agent.collect_hardware_info()
            return info

    async def start(self, host: str = "127.0.0.1", port: int = 11458, ssl_context=None) -> None:
        import uvicorn

        self._host = host
        ssl_kwargs = {}
        if ssl_context is None:
            from fusion_multi_node.security.mtls import server_ssl_kwargs

            ssl_kwargs = server_ssl_kwargs()
        elif isinstance(ssl_context, dict):
            ssl_kwargs = ssl_context
        config = uvicorn.Config(self.app, host=host, port=port, log_level="warning", **ssl_kwargs)
        self._uvicorn_server = uvicorn.Server(config)
        self._started_at = time.time()
        scheme = "https" if ssl_kwargs else "http"
        logger.info(f"Agent 服务启动: {scheme}://{host}:{port}")
        try:
            await self._uvicorn_server.serve()
        except OSError as e:
            # 端口被占用 (Address already in use) — 明确报冲突端口而非通用 bind 错误。
            # issue #25: 同机 fusion-comfyui (11445) / fusion-mlx (11432) / master (11452)
            # / mcp (11446) 撞端口时, 用户只看到通用 OSError, 难定位。
            _CONFLICT = {
                11445: "fusion-comfyui",
                11432: "fusion-mlx / fusion-gateway",
                11434: "fusion-mlx (monorepo 默认)",
                11452: "fusion-multi-node Master",
                11450: "fusion-multi-node mDNS",
                11446: "fusion-multi-node MCP/FMP",
            }
            who = _CONFLICT.get(port, "")
            hint = f" (与 {who} 默认端口冲突)" if who else ""
            logger.error(f"Agent 端口 {port} bind 失败{hint}: {e}")
            raise OSError(f"端口 {port} 被占用{hint}, 原 OSError: {e}") from e

    async def stop(self) -> None:
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
        logger.info("Agent 服务已停止")

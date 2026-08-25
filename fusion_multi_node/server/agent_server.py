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
    target_port: int = 11445


class KVWarmRequest(BaseModel):
    model_name: str
    prompts: list[str]
    # 目标节点列表; 不提供则仅本节点预存 (无远端推送)。
    nodes: list[str] = []


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
        self.kv_manager = kv_manager or KVSharingManager()
        self.app = FastAPI(title="Fusion Multi-Node Agent", version=_VERSION)
        self._shared_token = shared_token or load_or_create_token()
        self._rate_limiter = InMemoryRateLimiter()
        self.app.add_middleware(BearerAuthMiddleware, shared_token=self._shared_token)
        self.app.add_middleware(RateLimitMiddleware, limiter=self._rate_limiter)
        self._uvicorn_server: Any | None = None
        self._started_at: float = 0.0
        # 本节点对外可寻址地址 — transfer_from_remote 据此回连本机拉缓存。
        # 默认 127.0.0.1, start() 时更新为实际监听 host。
        self._host: str = "127.0.0.1"
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        @app.get("/api/health")
        @app.get("/health")
        async def health():
            return {"status": "ok"}

        # ── 任务执行 ──

        @app.post("/api/execute")
        async def execute_task(req: ExecuteRequest, request: Request):
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
        async def cancel_task(req: TaskCancelRequest):
            # 真取消: 中止运行中推理协程, 非假动作
            ok = await self.agent.cancel_task(req.task_id)
            if not ok:
                raise HTTPException(status_code=404, detail=f"无可取消任务: {req.task_id}")
            return {"status": "cancelled", "task_id": req.task_id}

        # ── KV 缓存 ──

        @app.post("/api/kv/lookup")
        async def kv_lookup(req: KGLookupRequest):
            entry = self.kv_manager.lookup_local(req.model_name, req.prompt_hash)
            if not entry:
                raise HTTPException(status_code=404, detail="KV 缓存未找到")
            return {
                "cache_id": entry.cache_id,
                "model_name": entry.model_name,
                "prompt_hash": entry.prompt_hash,
                "total_tokens": entry.total_tokens,
                "total_size_bytes": entry.total_size_bytes,
                "shards": [
                    {
                        "shard_id": s.shard_id,
                        "layer_index": s.layer_index,
                        "node_id": s.node_id,
                        "token_count": s.token_count,
                        "size_bytes": s.size_bytes,
                    }
                    for s in entry.shards
                ],
            }

        @app.post("/api/kv/transfer")
        async def kv_transfer(req: KVTransferRequest):
            # E2: source_node 用本节点真实可寻址地址, 非 "self"。
            # transfer_from_remote 向 source 回连拉缓存; "self" 解析为 0.0.0.0/失败。
            source_addr = f"{self._host}:{self.agent.config.agent_port}"
            ok = await self.kv_manager.transfer_from_remote(
                req.cache_id,
                source_node=source_addr,
                target_node=req.target_node,
            )
            if not ok:
                raise HTTPException(status_code=500, detail="KV 缓存传输失败")
            return {"status": "ok"}

        @app.post("/api/kv/warm")
        async def kv_warm(req: KVWarmRequest):
            # E7: nodes 由调用方提供 (Master/CLI 持有在线节点表)。
            # Agent 不知集群拓扑, 不可硬编码 []; 空 nodes = 仅本节点预存, 远端无推送。
            if not req.nodes:
                logger.warning("kv_warm: 未提供目标节点, 仅本节点预存, 无远端推送")
            result = await self.kv_manager.warm_cache(
                req.model_name,
                req.prompts,
                nodes=req.nodes,
            )
            warmed = result.get("success", 0)
            return {"status": "ok", "warmed": warmed, "success": warmed, "failed": result.get("failed", 0)}

        @app.get("/api/kv/stats")
        async def kv_stats():
            return self.kv_manager.get_stats()

        # ── 硬件信息 ──

        @app.get("/api/hardware")
        async def hardware_info():
            info = self.agent.collect_hardware_info()
            return info

    async def start(self, host: str = "127.0.0.1", port: int = 11445) -> None:
        import uvicorn

        self._host = host
        config = uvicorn.Config(self.app, host=host, port=port, log_level="warning")
        self._uvicorn_server = uvicorn.Server(config)
        self._started_at = time.time()
        logger.info(f"Agent 服务启动: {host}:{port}")
        await self._uvicorn_server.serve()

    async def stop(self) -> None:
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
        logger.info("Agent 服务已停止")

"""Agent FastAPI 服务层 — 节点任务执行与 KV 缓存 HTTP API。"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.distributed_mlx import KVCacheEntry, KVSharingManager, KVShard
from fusion_multi_node.utils.auth import BearerAuthMiddleware, load_or_create_token

logger = logging.getLogger(__name__)

ALLOWED_TASK_TYPES = {"inference", "embedding", "plugin"}
ALLOWED_EXTRA_KEYS = {"temperature", "top_p", "top_k", "repeat_penalty", "seed"}


class InMemoryRateLimiter:
    """简易内存速率限制器 — 按 IP 限制请求频率。"""

    def __init__(self, max_requests: int = 30, window_seconds: float = 60.0):
        self._max = max_requests
        self._window = window_seconds
        self._counts: Dict[str, List[float]] = defaultdict(list)

    def is_allowed(self, key: str) -> bool:
        now = time.time()
        timestamps = self._counts[key]
        cutoff = now - self._window
        self._counts[key] = [t for t in timestamps if t > cutoff]
        if len(self._counts[key]) >= self._max:
            return False
        self._counts[key].append(now)
        return True


# ── Pydantic 请求/响应模型 ──

ALLOWED_TASK_TYPES = {"inference", "embedding", "plugin"}
ALLOWED_EXTRA_KEYS = {"temperature", "top_p", "top_k", "repeat_penalty", "seed"}


class ExecuteRequest(BaseModel):
    task_type: str = "inference"
    model_name: str = ""
    prompt: str = ""
    messages: List[Dict[str, Any]] = []
    max_tokens: int = 2048
    temperature: float = 0.7
    extra: Dict[str, Any] = {}


class KGLookupRequest(BaseModel):
    model_name: str
    prompt_hash: str


class KVTransferRequest(BaseModel):
    cache_id: str
    target_node: str
    target_port: int = 9755


class KVWarmRequest(BaseModel):
    model_name: str
    prompts: List[str]


class HealthResponse(BaseModel):
    status: str
    node_id: str
    uptime_seconds: float


# ── Agent Server ──

class AgentServer:
    """节点 Agent HTTP 服务。"""

    def __init__(
        self,
        agent: Optional[NodeAgent] = None,
        kv_manager: Optional[KVSharingManager] = None,
        shared_token: Optional[str] = None,
    ):
        self.agent = agent or NodeAgent()
        self.kv_manager = kv_manager or KVSharingManager()
        self.app = FastAPI(title="Fusion Multi-Node Agent", version="0.1.0")
        self._shared_token = shared_token or load_or_create_token()
        self.app.add_middleware(BearerAuthMiddleware, shared_token=self._shared_token)
        self._uvicorn_server: Optional[Any] = None
        self._started_at: float = 0.0
        self._rate_limiter = InMemoryRateLimiter()
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        @app.get("/api/health")
        async def health():
            return HealthResponse(
                status="ok",
                node_id=self.agent.config.node_id,
                uptime_seconds=time.time() - self._started_at if self._started_at else 0.0,
            )

        # ── 任务执行 ──

        @app.post("/api/execute")
        async def execute_task(req: ExecuteRequest, request: Request):
            client_ip = request.client.host if request.client else "unknown"
            if not self._rate_limiter.is_allowed(client_ip):
                raise HTTPException(status_code=429, detail="请求过于频繁")
            if req.task_type not in ALLOWED_TASK_TYPES:
                raise HTTPException(status_code=400, detail=f"不合法的任务类型: {req.task_type}")
            filtered_extra = {k: v for k, v in req.extra.items() if k in ALLOWED_EXTRA_KEYS}
            task = {
                "type": req.task_type,
                "model_name": req.model_name,
                "prompt": req.prompt,
                "messages": req.messages,
                "max_tokens": req.max_tokens,
                "temperature": req.temperature,
                **filtered_extra,
            }
            try:
                result = self.agent.execute_task(task)
                return {"status": "ok", "result": result}
            except Exception as e:
                logger.error(f"任务执行失败: {e}")
                raise HTTPException(status_code=500, detail="内部错误")

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
            ok = self.kv_manager.transfer_from_remote(
                req.cache_id,
                source_node="self",
                target_node=req.target_node,
            )
            if not ok:
                raise HTTPException(status_code=500, detail="KV 缓存传输失败")
            return {"status": "ok"}

        @app.post("/api/kv/warm")
        async def kv_warm(req: KVWarmRequest):
            result = self.kv_manager.warm_cache(
                req.model_name,
                req.prompts,
                nodes=[],
            )
            return {"status": "ok", "warmed": result.get("warmed", 0)}

        @app.get("/api/kv/stats")
        async def kv_stats():
            return self.kv_manager.get_stats()

        # ── 硬件信息 ──

        @app.get("/api/hardware")
        async def hardware_info():
            info = self.agent.collect_hardware_info()
            return info

    async def start(self, host: str = "127.0.0.1", port: int = 9755) -> None:
        import uvicorn
        config = uvicorn.Config(self.app, host=host, port=port, log_level="warning")
        self._uvicorn_server = uvicorn.Server(config)
        self._started_at = time.time()
        logger.info(f"Agent 服务启动: {host}:{port}")
        await self._uvicorn_server.serve()

    async def stop(self) -> None:
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
        logger.info("Agent 服务已停止")

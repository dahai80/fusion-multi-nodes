"""Agent FastAPI 服务层 — 节点任务执行与 KV 缓存 HTTP API。"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.distributed_mlx import KVCacheEntry, KVSharingManager, KVShard

logger = logging.getLogger(__name__)


# ── Pydantic 请求/响应模型 ──

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
    ):
        self.agent = agent or NodeAgent()
        self.kv_manager = kv_manager or KVSharingManager()
        self.app = FastAPI(title="Fusion Multi-Node Agent", version="0.1.0")
        self._uvicorn_server: Optional[Any] = None
        self._started_at: float = 0.0
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
        async def execute_task(req: ExecuteRequest):
            task = {
                "type": req.task_type,
                "model_name": req.model_name,
                "prompt": req.prompt,
                "messages": req.messages,
                "max_tokens": req.max_tokens,
                "temperature": req.temperature,
                **req.extra,
            }
            try:
                result = self.agent.execute_task(task)
                return {"status": "ok", "result": result}
            except Exception as e:
                logger.error(f"任务执行失败: {e}")
                raise HTTPException(status_code=500, detail=str(e))

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

    async def start(self, host: str = "0.0.0.0", port: int = 9755) -> None:
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

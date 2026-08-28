"""Agent FastAPI 服务层 — 节点任务执行与 KV 缓存 HTTP API。"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from fusion_multi_node.agent import NodeAgent
from fusion_multi_node.distributed_mlx import KVSharingManager
from fusion_multi_node.security.permission import NodeRole, PermissionManager
from fusion_multi_node.utils.auth import BearerAuthMiddleware, is_safe_path_segment, load_or_create_token

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
    # P1-14 (审计 §5.3): task_id 供 agent 拒同 task_id 重复派发 (master 派发传真实 task_id)。
    task_id: str = ""
    task_type: str = "inference"
    model_name: str = ""
    prompt: str = ""
    messages: list[dict[str, Any]] = []
    max_tokens: int = 2048
    temperature: float = 0.7
    extra: dict[str, Any] = {}


class ChatCompletionsRequest(BaseModel):
    # F3 (#27): OpenAI 兼容 chat 透传体。透传到 fusion-mlx /v1/chat/completions,
    # 经 FusionMLXBackend.chat (429 退避 + api_key Bearer), 不经任务流水线。
    model: str
    messages: list[dict[str, Any]] = []
    temperature: float = 0.7
    max_tokens: int = 4096
    stream: bool = False
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


class KVExportRequest(BaseModel):
    # GAP-7 (#33): 源节点导出 KV 张量 bundle — 含分片张量 (base64) 供跨节点传输。
    cache_id: str
    model_name: str = ""


class KVImportRequest(BaseModel):
    # GAP-7 (#33): 目标节点导入 KV 张量 bundle — store_local 预算硬门 (max_local_cache_mb + LRU)。
    bundle: dict[str, Any]


class TaskCancelRequest(BaseModel):
    task_id: str


class HealthResponse(BaseModel):
    status: str
    node_id: str
    uptime_seconds: float


# issue #52 跨节点 guard 契约 — 共享 Pydantic 模型 (master + agent 同 schema)。
# 放 agent_server 侧 cycle-safe: master 已 import agent_server (master_server:403),
# agent 零 import master — master 反向 import 这两个模型不引入循环。


class V1AuditChainResponse(BaseModel):
    """原语 1 — 审计链段响应 (master + agent 同形)。guard 拉取后验链。"""

    node_id: str
    records: list[dict[str, Any]]
    fetched_at: str
    truncated: bool = False


class RuleEpochReceiveRequest(BaseModel):
    """原语 2 — 纪元广播接收端 (master 接 standby 广播 / agent 接 master 广播)。"""

    epoch: int
    source: str = ""


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
            # P1-9: 默认 manager 自带磁盘持久化路径 — agent 重启可恢复本地 KV 缓存 (审计 §6.3)。
            self.kv_manager = KVSharingManager(cluster_token=self._shared_token)
        self.app = FastAPI(title="Fusion Multi-Node Agent", version=_VERSION)
        self._rate_limiter = InMemoryRateLimiter()
        # GAP-8: 审计日志 — 记鉴权失败/权限拒绝等安全动作, 追加写 JSONL。
        # 须在 BearerAuthMiddleware 之前实例化 — 中间件经 audit_logger 参数引用。
        from fusion_multi_node.security.audit_log import get_audit_logger

        self._audit = get_audit_logger()
        self.app.add_middleware(BearerAuthMiddleware, shared_token=self._shared_token, audit_logger=self._audit)
        self.app.add_middleware(RateLimitMiddleware, limiter=self._rate_limiter)
        # P1-G 细粒度权限 — 本 agent 维护调用方角色表。默认 master 为 MASTER 角色
        # (master 派发任务/取消到本节点须放行)。worker 节点角色由 register_caller 注入。
        # GAP-8 (复审计 2026-08-26): 强制校验默认开 (FUSION_PERMISSION_ENFORCE 默认 "1"),
        # 不再仅随 mTLS。强制模式缺 X-Node-Id → 403 (生产零信任: 身份不可缺)。
        # 测试隔离: conftest.py autouse fixture 设 FUSION_PERMISSION_ENFORCE=0 回退兼容模式
        # (现有 http 测试/CLI 无 X-Node-Id 须放行)。mTLS 开 → 亦强制 (传输已证调用方=集群节点)。
        self._permission_manager = PermissionManager()
        self._permission_manager.assign_role("master", NodeRole.MASTER, "system")
        enforce_env = os.environ.get("FUSION_PERMISSION_ENFORCE", "1").strip().lower()
        self._permission_enforce = enforce_env in ("1", "true", "yes", "on")
        try:
            from fusion_multi_node.security.mtls import is_enabled

            if is_enabled():
                self._permission_enforce = True
        except Exception:
            pass
        self._uvicorn_server: Any | None = None
        self._started_at: float = 0.0
        # 本节点对外可寻址地址 — transfer_from_remote 据此回连本机拉缓存。
        # 默认 127.0.0.1, start() 时更新为实际监听 host。
        self._host: str = "127.0.0.1"
        # issue #52 原语 2 — 本节点规则纪元 (接收 master 广播存此, guard 读本地基线)。
        self._rule_epoch: int = 0
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        async def _check_permission(request: Request, path: str, method: str = "POST") -> None:
            """细粒度权限校验 — 从 X-Node-Id/X-Node-Role header 取调用方身份。

            强制模式 (默认 / mTLS 开): 缺 X-Node-Id → 403; 角色无权 → 403。
            兼容模式 (FUSION_PERMISSION_ENFORCE=0): 缺 header → 放行 (现有 http 测试/CLI 无 header);
              有 header 则按角色校验 (master 派发带 X-Node-Id=master)。
            """
            if not self._permission_enforce:
                node_id = request.headers.get("X-Node-Id", "")
                if not node_id:
                    return
            else:
                node_id = request.headers.get("X-Node-Id", "")
                if not node_id:
                    self._audit.log(
                        actor="unknown",
                        action="permission_deny",
                        path=path,
                        method=method,
                        result="denied",
                        detail="缺少 X-Node-Id 身份头",
                    )
                    raise HTTPException(status_code=403, detail="缺少 X-Node-Id 身份头")
            role_hdr = request.headers.get("X-Node-Role", "")
            if role_hdr and self._permission_manager.get_role(node_id) is None:
                role = NodeRole.MASTER if role_hdr == "master" else NodeRole.WORKER
                self._permission_manager.assign_role(node_id, role, "header")
            if not self._permission_manager.check_path_access(node_id, path, method):
                logger.warning(f"权限拒绝: node={node_id} path={path} method={method}")
                self._audit.log(
                    actor=node_id,
                    action="permission_deny",
                    path=path,
                    method=method,
                    node_id=node_id,
                    result="denied",
                    detail=f"无权访问 {path}",
                )
                raise HTTPException(status_code=403, detail=f"无权访问 {path}")

        @app.get("/api/health")
        @app.get("/health")
        async def health():
            # C11 (AR 审计 #24): liveness 检本地 — fusion-mlx 端口探测 + 资源, 无 HTTP 出站。
            checks: dict[str, Any] = {}
            try:
                import psutil

                # 磁盘: 剩余 > 512MB (绝对下限, 兼容 Mac APFS 容器全盘占比失真)
                checks["disk_ok"] = psutil.disk_usage("/").free > 512 * 1024 * 1024
                checks["mem_ok"] = psutil.virtual_memory().available > 256 * 1024 * 1024
            except Exception:
                checks["disk_ok"] = True
                checks["mem_ok"] = True
            # fusion-mlx 端口探测 (本地 socket, 非 HTTP — 快, 供 healthcheck)
            checks["fusion_mlx_port"] = bool(self.agent._check_service(self.agent.config.fusion_mlx_port))
            ok = all(checks.values())
            status = "ok" if ok else "degraded"
            logger.debug(f"agent liveness: {status} checks={checks}")
            return {"status": status, "role": "agent", "checks": checks}

        @app.get("/api/health/deep")
        @app.get("/health/deep")
        async def health_deep():
            # C11: readiness — liveness + 真 HTTP 探 fusion-mlx /v1/models。
            # 编排器/LB 据此判定 agent 是否真能推理 (端口开≠模型就绪)。
            checks: dict[str, Any] = {}
            try:
                import psutil

                checks["disk_ok"] = psutil.disk_usage("/").free > 512 * 1024 * 1024
                checks["mem_ok"] = psutil.virtual_memory().available > 256 * 1024 * 1024
            except Exception:
                checks["disk_ok"] = True
                checks["mem_ok"] = True
            checks["fusion_mlx_port"] = bool(self.agent._check_service(self.agent.config.fusion_mlx_port))
            fusion_mlx_ok = False
            try:
                import httpx

                url = (
                    getattr(self.agent._backend, "base_url", None)
                    or f"http://localhost:{self.agent.config.fusion_mlx_port}"
                )
                async with httpx.AsyncClient(timeout=2.0) as c:
                    resp = await c.get(f"{url}/v1/models")
                    fusion_mlx_ok = resp.status_code == 200
            except Exception as e:
                logger.debug(f"agent readiness fusion-mlx 探测失败: {e}")
            checks["fusion_mlx_ready"] = fusion_mlx_ok
            ok = all(checks.values())
            status = "ok" if ok else "degraded"
            logger.info(f"agent readiness: {status} checks={checks}")
            return {"status": status, "role": "agent", "checks": checks}

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
                # P1-14: 透传真实 task_id (master 派发带入), 空=直接调用无追踪 (agent 分配匿名 id)。
                "task_id": req.task_id,
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

        # F3 (#27): chat 透传 — master /v1/chat/completions 转发到此, 直调 fusion-mlx,
        # 保留原生 OpenAI 格式 (非 /api/execute 扁平 result), 支持流式。
        # 集群内部鉴权: cluster_token (master 派发) + X-Node-Id=master, node-RBAC TASK_EXECUTE。
        # fmu_ 用户令牌不应到达 agent (集群内部流量从不携带用户凭据), BearerAuthMiddleware 校验。
        @app.post("/api/v1/chat/completions")
        async def chat_completions(req: ChatCompletionsRequest, request: Request):
            await _check_permission(request, "/api/v1/chat/completions", "POST")
            from fusion_multi_node.agent.node_agent import FusionMLXBackend
            from fusion_multi_node.agent.rate_pacer import RateLimitExhausted

            backend = self.agent._backend
            if not isinstance(backend, FusionMLXBackend):
                raise HTTPException(status_code=503, detail="chat 透传需 FusionMLXBackend")
            if not req.model or not is_safe_path_segment(req.model):
                raise HTTPException(status_code=400, detail=f"非法 model: {req.model!r}")
            payload = {
                "model": req.model,
                "messages": req.messages,
                "temperature": req.temperature,
                "max_tokens": req.max_tokens,
                **{k: v for k, v in req.extra.items() if k in ALLOWED_EXTRA_KEYS},
            }
            if req.stream:
                payload["stream"] = True
                client = await backend._get_client()
                url = f"{backend._base_url}/v1/chat/completions"

                # 流式: 透传 fusion-mlx SSE 字节流到上游 (master StreamingResponse 再透传客户端)。
                async def _stream():
                    try:
                        req_ctx = client.stream("POST", url, json=payload, headers=backend._dist_headers())
                        async with req_ctx as upstream:
                            async for chunk in upstream.aiter_raw():
                                yield chunk
                    except Exception as e:
                        logger.error(f"chat 流式透传失败: {e}")
                        yield b'data: {"error":"internal"}\n\n'

                return StreamingResponse(_stream(), media_type="text/event-stream")
            try:
                data = await backend.chat(
                    model=req.model,
                    messages=req.messages,
                    temperature=req.temperature,
                    max_tokens=req.max_tokens,
                    **{k: v for k, v in req.extra.items() if k in ALLOWED_EXTRA_KEYS},
                )
            except RateLimitExhausted as e:
                logger.warning(f"chat 透传限流未恢复: {e}")
                raise HTTPException(status_code=429, detail="推理后端限流, 稍后重试")
            return data

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

        @app.post("/api/kv/export")
        async def kv_export(req: KVExportRequest):
            # GAP-7 (#33): 源节点导出含张量的 bundle — 经 transport 后端产出分片张量并入元数据。
            # 缓存不存在 → 404; 张量后端不可达 → bundle 仍含元数据 (张量缺, 目标降级存)。
            bundle = await self.kv_manager.export_bundle(req.cache_id, req.model_name)
            if bundle is None:
                raise HTTPException(status_code=404, detail=f"KV 缓存未找到: {req.cache_id}")
            logger.info(f"GAP-7 KV 张量导出: cache_id={req.cache_id} shards={len(bundle.get('shards', []))}")
            return {"status": "ok", "bundle": bundle}

        @app.post("/api/kv/import")
        async def kv_import(req: KVImportRequest):
            # GAP-7 (#33): 目标节点导入 bundle — import_bundle 反序列化 + 经后端装张量 + store_local 预算门。
            # 超预算/解析失败 → stored=False (不静默吞, 调用方据 False 决策)。
            try:
                stored = await self.kv_manager.import_bundle(req.bundle)
            except Exception as e:
                logger.warning(f"GAP-7 KV 张量导入异常: {e}")
                stored = False
            return {"status": "ok" if stored else "skip", "stored": 1 if stored else 0}

        @app.post("/api/kv/export-stream")
        async def kv_export_stream(req: KVExportRequest):
            # P0-3 (审计 §4.3): 流式二进制导出 — 替代 base64+JSON 全量物化 (峰值 1.5GB)。
            # 元数据头 JSON + 各分片原始张量字节拼接, StreamingResponse 逐块输出, 不物化整 bundle。
            # 旧对端走 /api/kv/export JSON (向后兼容); master sync_kv_cache 优先流式, 404 降级 JSON。
            gen = self.kv_manager.export_stream(req.cache_id, req.model_name)
            first = None
            try:
                first = await gen.__anext__()
            except StopAsyncIteration:
                raise HTTPException(status_code=404, detail=f"KV 缓存未找到: {req.cache_id}")

            async def stream_body():
                if first is not None:
                    yield first
                async for chunk in gen:
                    yield chunk

            return StreamingResponse(stream_body(), media_type="application/octet-stream")

        @app.post("/api/kv/import-stream")
        async def kv_import_stream(request: Request):
            # P0-3: 流式导入 — 读头部 magic+长度+元数据, 剩余张量字节流式消费不物化整 bundle。
            # 与 export_stream 配对。stored=1 已存 / stored=0 预算拒存或解析失败。
            body = await request.body()
            if not body.startswith(self.kv_manager.KV_STREAM_MAGIC):
                logger.warning("P0-3 KV 流式导入: 请求体 magic 头不匹配")
                return {"status": "skip", "stored": 0}
            meta_len = int.from_bytes(body[8:12], "big")
            header_and_meta = body[: 12 + meta_len]

            async def tensor_aiter():
                # 剩余张量字节按块产出 (已全量读入 body — 此处逻辑分块, 真流式需上游 #650 端点流式读)
                rest = body[12 + meta_len :]
                chunk_size = 65536
                for i in range(0, len(rest), chunk_size):
                    yield rest[i : i + chunk_size]

            try:
                stored = await self.kv_manager.import_stream(header_and_meta, tensor_aiter())
            except Exception as e:
                logger.warning(f"P0-3 KV 流式导入异常: {e}")
                stored = False
            return {"status": "ok" if stored else "skip", "stored": 1 if stored else 0}

        # ── 硬件信息 ──

        @app.get("/api/hardware")
        async def hardware_info():
            # P1-2 (审计 §4.5): collect_hardware_info 调 system_profiler/ipconfig (至 5s)
            # 同步阻塞事件循环 → 经 asyncio.to_thread 移出, 对齐 node_agent.report_hardware 范式。
            import asyncio

            info = await asyncio.to_thread(self.agent.collect_hardware_info)
            return info

        # ── issue #52 跨节点 guard 契约原语 ──

        @app.get("/api/v1/audit/chain")
        async def audit_chain(request: Request, since_seq: int = 0):
            # 原语 1: guard 拉本节点审计链段。since_seq 过滤 (缺 seq 基线记录一律返)。
            # node-RBAC: guard 持 cluster_token + X-Node-Id: master (MASTER 角色) 放行。
            await _check_permission(request, "/api/v1/audit/chain", "GET")
            records = self._audit.read()
            filtered = [r for r in records if r.get("seq", 0) >= since_seq or "seq" not in r]
            return V1AuditChainResponse(
                node_id=self.agent.config.node_id,
                records=filtered,
                fetched_at=datetime.now(UTC).isoformat(),
            )

        @app.post("/api/rules/epoch")
        async def receive_rule_epoch(request: Request, req: RuleEpochReceiveRequest):
            # 原语 2: 接收 master 规则纪元广播, 存本地供 guard 读基线。
            await _check_permission(request, "/api/rules/epoch", "POST")
            self._rule_epoch = req.epoch
            logger.info(f"接收规则纪元广播 → {req.epoch} (source={req.source})")
            return {"status": "ok", "epoch": self._rule_epoch}

    async def start(self, host: str = "127.0.0.1", port: int = 11458, ssl_context=None) -> None:
        import uvicorn

        self._host = host
        # P1-9: 启动恢复本地 KV 缓存 (审计 §6.3) — 落盘文件存在则读回预热。
        try:
            restored = self.kv_manager.load()
            if restored:
                logger.info(f"P1-9 Agent 启动恢复 KV 缓存: {restored} 条")
        except Exception as e:
            logger.warning(f"P1-9 Agent 启动恢复 KV 缓存失败 (不影响启动): {e}")
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
        # P1-9: 停服落盘本地 KV 缓存 (审计 §6.3) — 下次启动可恢复。
        # P3-2 (审计 §6.11): 落盘失败升 critical 告警 (旧仅 warning 易被停服日志淹没) +
        # 上报 master 故障 (best-effort, 不阻塞停服; report_fault 窗口计数在停服后失效,
        # 不误 ban 健康节点)。save() 返 bool False 或抛异常均判定失败。
        kv_save_ok = False
        try:
            kv_save_ok = self.kv_manager.save()
        except Exception as e:
            logger.critical(f"P3-2 Agent 停服落盘 KV 缓存异常 (critical 告警): {e}")
        if not kv_save_ok:
            logger.critical("P3-2 Agent 停服落盘 KV 缓存失败 (critical 告警) — 重启将丢失本地 KV, 须运维介入")
            # best-effort 上报 master (停服期网络/超时容忍, 失败不重试不阻塞)
            try:
                await self.agent.report_fault(
                    fault_type="kv_persist_failed",
                    message="Agent 停服 KV 缓存落盘失败, 重启将丢失本地 KV",
                )
            except Exception as e:
                logger.warning(f"P3-2 KV 落盘故障上报 master 失败 (best-effort): {e}")
        # P2-3 (审计 §6.3): 落盘后调 kv_manager.close() 关 httpx 客户端 + 张量传输后端,
        # 修资源泄漏 (旧 stop 仅 save 不 close → KVSharingManager._http_client + MLXKVTransport
        # 持有 httpx.AsyncClient 句柄泄漏)。close() 内已有 try/except 容错。
        try:
            await self.kv_manager.close()
        except Exception as e:
            logger.warning(f"P2-3 Agent 停服关 KV 资源失败: {e}")
        logger.info("Agent 服务已停止")

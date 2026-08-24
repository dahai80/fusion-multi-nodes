"""Distributed MLX 分布式算子桥 — 封装 mlx.distributed 底层 API。

提供统一并行接口：
- 流水线并行（Pipeline Parallelism）：大模型分层拆分到多节点
- 数据并行（Data Parallelism）：多节点完整加载同款模型
- 通信压缩（Caveman token compression）
- MoE 模型分布式路由

能力状态（2026-08-24 AR 审计 H1）：
- 数据并行（DATA）：可用。走 fusion-mlx `/v1/chat/completions`（已存在）。
- 流水线并行（PIPELINE）：未实现，依赖 fusion-mlx `/distributed/*` 端点，
  该端点当前不存在（上游 issue: dahai80/fusion-mlx#621）。现网调用必 404。
  本模块保留分片/步进代码骨架待上游就绪;调用方应先校验端点可用性或捕获
  RuntimeError。激活张量传输层亦缺失（现仅转发明文 prompt,非真实激活）。
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import httpx

from fusion_multi_node.utils.auth import sanitize_node_url_part

logger = logging.getLogger(__name__)


class DistMode(Enum):
    """分布式模式。"""

    PIPELINE = "pipeline"
    DATA = "data"
    TENSOR = "tensor"


@dataclass
class ModelShard:
    """模型分片定义。"""

    shard_id: int
    total_shards: int
    layers: list[int]
    node_id: str
    memory_mb: float = 0.0
    status: str = "pending"


@dataclass
class DistConfig:
    """分布式推理配置。"""

    mode: DistMode = DistMode.PIPELINE
    model_name: str = ""
    num_nodes: int = 1
    shard_strategy: str = "auto"  # auto | uniform | custom
    communication: str = "thunderbolt"  # thunderbolt | ethernet
    caveman_compress: bool = True
    timeout: float = 300.0


class DistributedMLXBridge:
    """分布式 MLX 算子桥 — 封装 mlx.distributed 通信原语。

    当前实现为调度层，底层通过 HTTP 调用 fusion-mlx 的分布式 API。
    """

    def __init__(self):
        self._shards: dict[str, list[ModelShard]] = {}
        self._active_pipelines: dict[str, dict[str, Any]] = {}
        self._http_client: httpx.AsyncClient | None = None
        self._max_pipelines = 100
        self._max_shards = 50

    async def _get_http_client(self, timeout: float = 300.0) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=timeout)
        return self._http_client

    async def close(self) -> None:
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    def _cleanup_completed_pipelines(self) -> None:
        completed = [pid for pid, p in self._active_pipelines.items() if p["status"] in ("completed", "failed")]
        for pid in completed:
            del self._active_pipelines[pid]
        if len(self._active_pipelines) > self._max_pipelines:
            oldest = next(iter(self._active_pipelines))
            del self._active_pipelines[oldest]

    async def shard_model(
        self,
        model_name: str,
        num_shards: int,
        strategy: str = "auto",
    ) -> list[ModelShard]:
        """将模型切分为分片。"""
        logger.info(f"模型分片: {model_name} → {num_shards} 片 (策略: {strategy})")

        # 获取模型配置
        config = await self._get_model_config(model_name)
        total_layers = config.get("num_hidden_layers", 32)

        # 计算每片层数
        layers_per_shard = max(1, total_layers // num_shards)
        shards = []

        for i in range(num_shards):
            start = i * layers_per_shard
            end = start + layers_per_shard if i < num_shards - 1 else total_layers
            shard = ModelShard(
                shard_id=i,
                total_shards=num_shards,
                layers=list(range(start, end)),
                node_id="",
                memory_mb=config.get("memory_mb", 0) / num_shards,
                status="pending",
            )
            shards.append(shard)

        self._shards[model_name] = shards
        if len(self._shards) > self._max_shards:
            oldest = next(iter(self._shards))
            del self._shards[oldest]
        logger.info(f"分片完成: {total_layers} 层 → {num_shards} 片 ({layers_per_shard} 层/片)")
        return shards

    async def load_shard(
        self,
        model_name: str,
        shard_id: int,
        node_id: str,
        fusion_mlx_port: int = 11432,
    ) -> bool:
        """在指定节点加载模型分片。"""
        shards = self._shards.get(model_name, [])
        if shard_id >= len(shards):
            logger.error(f"分片索引越界: {shard_id}/{len(shards)}")
            return False

        shard = shards[shard_id]
        shard.node_id = node_id

        try:
            payload = {
                "model": model_name,
                "shard_id": shard_id,
                "total_shards": shard.total_shards,
                "layers": shard.layers,
                "mode": "pipeline",
            }

            safe_node = sanitize_node_url_part(node_id)
            client = await self._get_http_client(300.0)
            resp = await client.post(
                f"http://{safe_node}:{fusion_mlx_port}/distributed/load_shard",
                json=payload,
            )
            if resp.status_code == 200:
                shard.status = "loaded"
                logger.info(f"分片加载成功: {model_name}[{shard_id}] @ {node_id}")
                return True
            else:
                logger.error(f"分片加载失败: {resp.text}")
                return False
        except Exception as e:
            logger.error(f"分片加载异常: {e}")
            shard.status = "failed"
            return False

    async def pipeline_inference(
        self,
        model_name: str,
        prompt: str,
        node_chain: list[str],
        fusion_mlx_port: int = 11432,
    ) -> dict[str, Any]:
        """流水线并行推理。"""
        pipeline_id = f"pipe_{model_name}_{len(self._active_pipelines)}"
        self._active_pipelines[pipeline_id] = {
            "model": model_name,
            "nodes": node_chain,
            "status": "running",
            "started_at": time.time(),
        }

        logger.info(f"流水线推理: {pipeline_id} ({len(node_chain)} 节点)")

        current_input = prompt
        client = await self._get_http_client(300.0)

        for i, node_id in enumerate(node_chain):
            payload = {
                "model": model_name,
                "input": current_input,
                "pipeline_id": pipeline_id,
                "shard_id": i,
                "total_shards": len(node_chain),
                "mode": "pipeline",
            }

            try:
                safe_node = sanitize_node_url_part(node_id)
                resp = await client.post(
                    f"http://{safe_node}:{fusion_mlx_port}/distributed/pipeline_step",
                    json=payload,
                )
                resp.raise_for_status()
                data = resp.json()
                current_input = data.get("output", current_input)
                logger.debug(f"流水线步骤 {i + 1}/{len(node_chain)} 完成 @ {node_id}")
            except Exception as e:
                logger.error(f"流水线步骤 {i + 1} 失败: {e}")
                self._active_pipelines[pipeline_id]["status"] = "failed"
                return {"error": "流水线步骤执行失败", "pipeline_id": pipeline_id}

        self._active_pipelines[pipeline_id]["status"] = "completed"
        self._cleanup_completed_pipelines()
        return {
            "pipeline_id": pipeline_id,
            "output": current_input,
            "nodes": len(node_chain),
        }

    async def data_parallel_inference(
        self,
        model_name: str,
        prompts: list[str],
        nodes: list[str],
        fusion_mlx_port: int = 11432,
    ) -> list[dict[str, Any]]:
        """数据并行推理 — 负载感知分配。

        优先分配给活跃任务数最少的节点，而非简单轮询。
        """
        load: dict[str, int] = dict.fromkeys(nodes, 0)
        tasks: list[asyncio.Task] = []
        assigned: list[str] = []

        for prompt in prompts:
            min_node = min(load, key=load.get)
            load[min_node] += 1
            assigned.append(min_node)
            tasks.append(asyncio.create_task(self._single_inference(min_node, model_name, prompt, fusion_mlx_port)))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed = []
        for node_id, r in zip(assigned, results):
            if isinstance(r, Exception):
                logger.error(f"数据并行推理失败 @ 节点 {node_id}: {type(r).__name__}: {r}")
                processed.append({"node_id": node_id, "error": f"推理执行失败: {r}"})
            else:
                processed.append(r)

        ok_count = sum(1 for r in processed if "error" not in r)
        logger.info(
            f"数据并行推理完成: {len(prompts)} 请求 → {len(nodes)} 节点, 成功={ok_count}, 分配: {load}"
        )
        return processed

    async def _single_inference(
        self,
        node_id: str,
        model_name: str,
        prompt: str,
        port: int,
    ) -> dict[str, Any]:
        """单节点推理。"""
        payload = {
            "model": model_name,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4096,
        }
        safe_node = sanitize_node_url_part(node_id)
        client = await self._get_http_client(300.0)
        resp = await client.post(
            f"http://{safe_node}:{port}/v1/chat/completions",
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        try:
            choices = data["choices"]
            content = choices[0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as e:
            raise RuntimeError(
                f"节点 {node_id} 返回非 OpenAI chat 格式 (缺 choices[0].message.content): {str(data)[:200]}"
            ) from e
        return {
            "node_id": node_id,
            "content": content,
            "usage": data.get("usage", {}),
        }

    async def _get_model_config(self, model_name: str) -> dict[str, Any]:
        """获取模型配置（通过 fusion-mlx API）。

        失败直接抛错, 不静默回退默认值 — 否则按错误层数切分, 产线静默损坏。
        """
        client = await self._get_http_client(10.0)
        _mlx_base = os.environ.get("FUSION_MLX_URL") or "http://localhost:11432"
        try:
            resp = await client.get(f"{_mlx_base}/v1/models/{model_name}")
        except Exception as e:
            raise RuntimeError(f"获取模型配置失败({model_name}): fusion-mlx 不可达: {e}") from e
        if resp.status_code != 200:
            raise RuntimeError(
                f"获取模型配置失败({model_name}): fusion-mlx 返回 {resp.status_code}: {resp.text[:200]}"
            )
        return resp.json()

    async def sync_weights(
        self,
        model_name: str,
        source_node: str,
        target_nodes: list[str],
        port: int = 11432,
    ) -> bool:
        """跨节点同步模型权重。"""
        success = True
        client = await self._get_http_client(600.0)
        for target in target_nodes:
            try:
                payload = {
                    "model": model_name,
                    "source": source_node,
                    "target": target,
                }
                safe_source = sanitize_node_url_part(source_node)
                resp = await client.post(
                    f"http://{safe_source}:{port}/distributed/sync_weights",
                    json=payload,
                )
                if resp.status_code != 200:
                    success = False
                    logger.error(f"权重同步失败: {source_node} → {target}")
            except Exception as e:
                success = False
                logger.error(f"权重同步异常: {e}")
        return success

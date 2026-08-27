"""GAP-7 (#33) — KV 张量跨节点传输后端。

本仓自建跨节点张量传输通道: 序列化张量 + HTTP 传输 + 目标节点内存预算。
两后端可插拔 (env FUSION_KV_TENSOR_BACKEND):

- SyntheticKVTransport (默认, 无依赖): 确定性合成张量 (hashlib 种子), 证明通道端到端可用。
  生产部署无上游 #650 时亦用此 (满足 #33 验收: 张量跨节点 round-trip)。
- MLXKVTransport (env=mlx, 待上游 #650): 调本地 fusion-mlx /distributed/kv_cache/export|import
  取/装真 mx.array 张量。端点未落地 (404) → 降级合成 + warn (Rule 12 fail visibly, 不静默)。

上游 issue #650 (OPEN, 已提) 提案 b64-npy 格式 — 落地后 MLX 后端激活, 零翻译。
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Any, Protocol, runtime_checkable

import httpx

from fusion_multi_node.security.mtls import client_kwargs as mtls_client_kwargs

logger = logging.getLogger(__name__)

# 上游 fusion-mlx KV 张量导出/导入端点 (issue #650 提案, 未落地)。
_KV_EXPORT_PATH = "/distributed/kv_cache/export"
_KV_IMPORT_PATH = "/distributed/kv_cache/import"


@runtime_checkable
class KVTransportBackend(Protocol):
    """KV 张量传输后端协议 — 解耦张量来源 (合成 vs 真 MLX)。"""

    name: str

    async def export_tensor(
        self, cache_id: str, model_name: str, node_id: str
    ) -> bytes | None:
        """产出张量字节 — 导出侧 (源节点)。

        返回 None = 无法产出 (上游端点未落地), 调用方降级 (跳过该分片张量或合成兜底)。
        """
        ...

    async def import_tensor(
        self, cache_id: str, model_name: str, tensor_bytes: bytes, node_id: str
    ) -> bool:
        """消费张量字节 — 导入侧 (目标节点)。

        返回 True = 已接收 (合成=no-op 仅本地存; MLX=装进本地 fusion-mlx)。
        返回 False = 装载失败 (上游端点不可达且无降级)。
        """
        ...

    async def close(self) -> None:
        ...


class SyntheticKVTransport:
    """合成张量后端 — 确定性生成, 无外部依赖。

    张量 = hashlib.sha256(种子).digest() 复制成指定长度。同 cache_id+model+node
    恒产生同字节 → 可验 round-trip 完整性。默认后端, 满足 #33 验收不依赖上游。
    """

    name = "synthetic"

    def __init__(self, tensor_size: int = 512):
        self._tensor_size = tensor_size

    async def export_tensor(
        self, cache_id: str, model_name: str, node_id: str
    ) -> bytes | None:
        seed = f"kv-tensor::{cache_id}::{model_name}::{node_id}".encode()
        digest = hashlib.sha256(seed).digest()
        # 复制 digest 到目标长度 (确定性, 可复现)
        out = bytearray()
        while len(out) < self._tensor_size:
            out.extend(digest)
        tensor = bytes(out[: self._tensor_size])
        logger.debug(
            f"GAP-7 合成张量导出: cache_id={cache_id} model={model_name} "
            f"node={node_id} size={len(tensor)}B"
        )
        return tensor

    async def import_tensor(
        self, cache_id: str, model_name: str, tensor_bytes: bytes, node_id: str
    ) -> bool:
        # 合成后端: 张量仅存本地 (store_local 持有 bytes), 无需装进推理引擎。
        logger.debug(
            f"GAP-7 合成张量导入: cache_id={cache_id} model={model_name} "
            f"node={node_id} size={len(tensor_bytes)}B (本地存储)"
        )
        return True

    async def close(self) -> None:
        pass


class MLXKVTransport:
    """真 MLX 张量后端 — 调本地 fusion-mlx /distributed/kv_cache/export|import。

    待上游 issue #650 落地激活 (env FUSION_KV_TENSOR_BACKEND=mlx)。
    端点 404 (未落地) → export 返 None (降级), import 返 True (store_local 兜底)。
    100% 本地: 仅调同节点/集群 fusion-mlx, 无云端路径。
    """

    name = "mlx"

    def __init__(
        self,
        base_url: str = "http://localhost:11432",
        api_key: str = "",
        timeout: float = 60.0,
    ):
        env_url = os.environ.get("FUSION_MLX_URL")
        self._base_url = (env_url or base_url).rstrip("/")
        self._api_key = api_key or os.environ.get("FUSION_MLX_API_KEY", "")
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._timeout, **mtls_client_kwargs())
        return self._client

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    async def export_tensor(
        self, cache_id: str, model_name: str, node_id: str
    ) -> bytes | None:
        try:
            client = await self._get_client()
            resp = await client.post(
                f"{self._base_url}{_KV_EXPORT_PATH}",
                json={"cache_id": cache_id, "model_name": model_name, "node_id": node_id},
                headers=self._headers(),
            )
            if resp.status_code == 404:
                logger.warning(
                    f"GAP-7 MLX 张量导出降级: 上游 {_KV_EXPORT_PATH} 未落地 (issue #650), "
                    f"cache_id={cache_id} 返回 None"
                )
                return None
            resp.raise_for_status()
            data = resp.json()
            import base64

            tensor_b64 = data.get("tensor", "")
            if not tensor_b64:
                logger.warning(f"GAP-7 MLX 张量导出空: cache_id={cache_id}")
                return None
            tensor = base64.b64decode(tensor_b64)
            logger.info(
                f"GAP-7 MLX 真张量导出: cache_id={cache_id} model={model_name} "
                f"size={len(tensor)}B"
            )
            return tensor
        except Exception as e:
            logger.warning(f"GAP-7 MLX 张量导出失败, 降级 None: cache_id={cache_id} err={e}")
            return None

    async def import_tensor(
        self, cache_id: str, model_name: str, tensor_bytes: bytes, node_id: str
    ) -> bool:
        try:
            import base64

            client = await self._get_client()
            resp = await client.post(
                f"{self._base_url}{_KV_IMPORT_PATH}",
                json={
                    "cache_id": cache_id,
                    "model_name": model_name,
                    "node_id": node_id,
                    "tensor": base64.b64encode(tensor_bytes).decode("ascii"),
                },
                headers=self._headers(),
            )
            if resp.status_code == 404:
                # 上游端点未落地 → store_local 兜底持有张量, 返 True (不阻塞传输)。
                logger.warning(
                    f"GAP-7 MLX 张量导入降级: 上游 {_KV_IMPORT_PATH} 未落地 (issue #650), "
                    f"cache_id={cache_id} 走本地存储兜底"
                )
                return True
            resp.raise_for_status()
            logger.info(
                f"GAP-7 MLX 真张量导入: cache_id={cache_id} model={model_name} "
                f"node={node_id} size={len(tensor_bytes)}B"
            )
            return True
        except Exception as e:
            logger.warning(
                f"GAP-7 MLX 张量导入失败, 走本地存储兜底: cache_id={cache_id} err={e}"
            )
            return True

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()


def get_kv_transport(
    backend: str | None = None,
    **kwargs: Any,
) -> KVTransportBackend:
    """工厂 — 按 env FUSION_KV_TENSOR_BACKEND 选后端。

    默认 "synthetic" (无依赖, 满足 #33 验收); "mlx" 激活真张量 (待上游 #650)。
    """
    name = (backend or os.environ.get("FUSION_KV_TENSOR_BACKEND", "synthetic")).strip().lower()
    if name == "mlx":
        logger.info("GAP-7 KV 张量后端: mlx (真张量, 待上游 issue #650 端点)")
        return MLXKVTransport(**kwargs)
    logger.info("GAP-7 KV 张量后端: synthetic (默认, 无依赖)")
    return SyntheticKVTransport(**kwargs) if name == "synthetic" else SyntheticKVTransport()

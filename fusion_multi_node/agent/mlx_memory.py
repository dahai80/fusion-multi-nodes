"""fusion-mlx Metal 显存采集 — 经 GET /v1/health 抓 MLX 活动内存。

issue #64: Apple Silicon 统一内存无独立显存, 真实 GPU/Metal 占用须从底座
mx.metal.get_active_memory() 派生。多节点不引 mlx 依赖 (非声明依赖), 改走
本地 loopback HTTP 抓 fusion-mlx /v1/health 返回的 memory 块。
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_GB = 1024**3


async def fetch_mlx_memory(base_url: str, api_key: str = "", timeout: float = 2.0) -> dict[str, Any] | None:
    """抓 fusion-mlx /v1/health memory 块, 返回 {active_gb, cache_gb, peak_gb, total_gb, oom_risk}。

    离线安全: 连接错误/超时/非 200 一律返 None (调用方回落 0.0, 不拖垮心跳)。
    api_key 空 → 不发 Authorization 头 (匿名探测, 兼容未启鉴权底座)。
    """
    url = f"{base_url.rstrip('/')}/v1/health"
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                logger.debug(f"fetch_mlx_memory 非 200: status={resp.status_code} url={url}")
                return None
            data = resp.json()
        mem = data.get("memory") if isinstance(data, dict) else None
        if not isinstance(mem, dict):
            logger.debug(f"fetch_mlx_memory memory 块缺失: url={url}")
            return None
        active_b = float(mem.get("mlx_active_bytes", 0) or 0)
        cache_b = float(mem.get("mlx_cache_bytes", 0) or 0)
        peak_b = float(mem.get("mlx_peak_bytes", 0) or 0)
        total_b = float(mem.get("total_bytes", 0) or 0)
        # oom_risk: fusion-mlx 实测放顶层 (非 memory 块内), 回退读顶层。
        oom_risk = mem.get("oom_risk")
        if oom_risk is None and isinstance(data, dict):
            oom_risk = data.get("oom_risk")
        logger.debug(
            f"fetch_mlx_memory ok: active={active_b / _GB:.2f}GB cache={cache_b / _GB:.2f}GB "
            f"total={total_b / _GB:.2f}GB oom_risk={oom_risk}"
        )
        return {
            "active_gb": round(active_b / _GB, 3),
            "cache_gb": round(cache_b / _GB, 3),
            "peak_gb": round(peak_b / _GB, 3),
            "total_gb": round(total_b / _GB, 3),
            "oom_risk": oom_risk,
        }
    except Exception as e:
        logger.debug(f"fetch_mlx_memory 失败 (底座未运行或不可达): url={url} err={e}")
        return None


def fetch_mlx_memory_sync(base_url: str, api_key: str = "", timeout: float = 2.0) -> dict[str, Any] | None:
    """同步版本 — 供 collect_load_report (to_thread 内) 复用同一逻辑。"""
    url = f"{base_url.rstrip('/')}/v1/health"
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(url, headers=headers)
            if resp.status_code != 200:
                logger.debug(f"fetch_mlx_memory_sync 非 200: status={resp.status_code} url={url}")
                return None
            data = resp.json()
        mem = data.get("memory") if isinstance(data, dict) else None
        if not isinstance(mem, dict):
            return None
        active_b = float(mem.get("mlx_active_bytes", 0) or 0)
        cache_b = float(mem.get("mlx_cache_bytes", 0) or 0)
        peak_b = float(mem.get("mlx_peak_bytes", 0) or 0)
        total_b = float(mem.get("total_bytes", 0) or 0)
        oom_risk = mem.get("oom_risk")
        if oom_risk is None and isinstance(data, dict):
            oom_risk = data.get("oom_risk")
        return {
            "active_gb": round(active_b / _GB, 3),
            "cache_gb": round(cache_b / _GB, 3),
            "peak_gb": round(peak_b / _GB, 3),
            "total_gb": round(total_b / _GB, 3),
            "oom_risk": oom_risk,
        }
    except Exception as e:
        logger.debug(f"fetch_mlx_memory_sync 失败: url={url} err={e}")
        return None

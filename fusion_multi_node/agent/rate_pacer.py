"""GAP-6 客户端限流适配 — 429 退避重试。

fusion-mlx 上游 --rate-limit 限流 (issue #635 已修: --rate-limit 0 真正关闭,
默认即关; 显式设上限值时仍会返 429)。本模块在 agent→fusion-mlx HTTP 调用层
拦截 429: 读 Retry-After 头, 指数退避 sleep, 在任务超时预算内重试。
预算耗尽仍 429 → 上抛 RateLimitExhausted 供调用方归类为瞬时失败 (可重试),
而非逻辑错误 (不该 ban 节点)。

旧缺陷: FusionMLXBackend.chat 直接 raise_for_status → 429 一律 HTTPStatusError
→ agent 包成 {"error":...} → master 归 logic_fail → report_fault 累计 ban 节点
(健康节点被限流却拉黑 300s, GAP-6 审计 §7)。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)


class RateLimitExhausted(Exception):
    """429 重试预算耗尽 — 调用方应归类为瞬时失败 (可重试), 非 ban 信号。"""

    def __init__(self, last_status: int, retry_after: float, attempts: int) -> None:
        self.last_status = last_status
        self.retry_after = retry_after
        self.attempts = attempts
        super().__init__(f"fusion-mlx 限流未恢复 (429): 重试 {attempts} 次, 末次 Retry-After={retry_after:.1f}s")


@dataclass
class PacerConfig:
    """限流退避参数 — 确定性 (Rule 5), 无随机。"""

    max_retries: int = 3
    initial_backoff: float = 0.5
    max_backoff: float = 5.0
    # 单次调用总重试预算上限 — 不超过任务级超时, 防无限阻塞。
    budget_seconds: float = 10.0

    def next_backoff(self, attempt: int) -> float:
        """指数退避: initial * 2^attempt, 上限 max_backoff。确定性无 jitter。"""
        return min(self.initial_backoff * (2**attempt), self.max_backoff)


def parse_retry_after(resp: httpx.Response) -> float:
    """解析 Retry-After 头 — 秒数或 HTTP-date。缺/非法回落 1.0s。"""
    raw = resp.headers.get("retry-after", "").strip()
    if not raw:
        return 1.0
    # 秒数形式
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    # HTTP-date 形式
    try:
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(raw)
        epoch = time.mktime(dt.timetuple())
        wait = max(0.0, epoch - time.time())
        return wait if wait > 0 else 1.0
    except Exception:
        return 1.0


async def dispatch_with_pacing(
    send_request,
    pacer: PacerConfig,
) -> httpx.Response:
    """包一次 HTTP 发送, 429 时按 pacer 退避重试, 预算内返回非 429 响应。

    send_request: async callable () -> httpx.Response (每次调用发一次请求)。
    返回首个非 429 响应。预算耗尽仍 429 → raise RateLimitExhausted。
    其他状态码 (含 5xx/401) 原样返回, 交调用方处理。
    """
    deadline = time.time() + pacer.budget_seconds
    last_retry_after = 0.0
    for attempt in range(pacer.max_retries + 1):
        resp = await send_request()
        if resp.status_code != 429:
            return resp
        last_retry_after = parse_retry_after(resp)
        # 末次不 sleep, 直接抛
        if attempt >= pacer.max_retries:
            break
        wait = min(last_retry_after, pacer.next_backoff(attempt))
        # 不超预算
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        wait = min(wait, remaining)
        logger.warning(
            f"fusion-mlx 限流 (429), 退避 {wait:.2f}s 后重试 "
            f"(attempt {attempt + 1}/{pacer.max_retries}, Retry-After={last_retry_after:.1f}s)"
        )
        await asyncio.sleep(wait)
    raise RateLimitExhausted(429, last_retry_after, pacer.max_retries + 1)

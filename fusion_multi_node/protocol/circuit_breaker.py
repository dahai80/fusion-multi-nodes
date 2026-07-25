"""Circuit Breaker 熔断器。

防止故障节点拖垮整个集群:
- CLOSED: 正常状态，请求放行
- OPEN: 熔断状态，请求直接拒绝
- HALF_OPEN: 半开状态，放行探测请求

参数:
- failure_threshold: 连续失败次数阈值（默认 5）
- recovery_timeout: 熔断恢复超时（默认 30s）
- half_open_max: 半开状态最大放行数（默认 1）
"""

from __future__ import annotations

import logging
import time
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """熔断器 — 保护集群免受故障节点拖垮。"""

    def __init__(
        self,
        name: str = "default",
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max: int = 1,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max = half_open_max

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float = 0.0
        self._half_open_count = 0
        self._total_calls = 0
        self._total_failures = 0

    @property
    def state(self) -> CircuitState:
        """当前状态（可能触发状态转换）。"""
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._half_open_count = 0
                logger.info(f"熔断器 [{self.name}] OPEN → HALF_OPEN")
        return self._state

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED

    def allow_request(self) -> bool:
        """是否允许请求通过。"""
        self._total_calls += 1
        current = self.state

        if current == CircuitState.CLOSED:
            return True
        elif current == CircuitState.HALF_OPEN:
            if self._half_open_count < self.half_open_max:
                self._half_open_count += 1
                return True
            return False
        else:
            return False

    def record_success(self) -> None:
        """记录成功。"""
        self._success_count += 1
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            logger.info(f"熔断器 [{self.name}] HALF_OPEN → CLOSED (探测成功)")
        elif self._state == CircuitState.CLOSED:
            self._failure_count = 0

    def record_failure(self) -> None:
        """记录失败。"""
        self._failure_count += 1
        self._total_failures += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            logger.info(f"熔断器 [{self.name}] HALF_OPEN → OPEN (探测失败)")
        elif self._state == CircuitState.CLOSED:
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                logger.warning(f"熔断器 [{self.name}] CLOSED → OPEN (连续 {self._failure_count} 次失败)")

    def reset(self) -> None:
        """手动重置熔断器。"""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_count = 0
        logger.info(f"熔断器 [{self.name}] 已重置")

    def get_stats(self) -> dict:
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "total_calls": self._total_calls,
            "total_failures": self._total_failures,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }

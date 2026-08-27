"""M4-05 云端 API 回退 — LiteLLM 风格 OpenAI/Anthropic 集成。

⚠️ AR审计 P2 违规: 直接调用外部云端API，违背"本地算力基座/断网可用"定位。
整改方案: 此模块为可选插件，默认禁用(enabled=False)。
云端回退应由 fusion-gateway 负责，后续版本将迁移。

当本地集群资源不足时，自动回退到云端 API:
- OpenAI GPT 系列
- Anthropic Claude 系列
- 统一的 OpenAI-compatible 接口
- 成本控制与配额管理
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# P1-4 (审计 §3.2): 调度路径已切断 (v0.8.2), 模块仅迁移债保留待迁 fusion-gateway #106。
# import-time 禁用守卫 — 默认拒绝导入, 显式 env FUSION_CLOUD_FALLBACK_ENABLED=1 才放行。
# 防止误引用重新接回云端调度路径 (违 "100% 本地/离线" 定位)。测试独立验证模块逻辑时设该 env。
if os.environ.get("FUSION_CLOUD_FALLBACK_ENABLED") != "1":
    raise ImportError(
        "cloud_fallback 已迁移 fusion-gateway #106, 调度路径已切断; "
        "需 FUSION_CLOUD_FALLBACK_ENABLED=1 显式启用 (仅独立验证用)"
    )


class CloudProvider(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"


@dataclass
class CloudModel:
    provider: CloudProvider
    model_id: str
    display_name: str
    context_length: int = 4096
    cost_per_1k_input: float = 0.0
    cost_per_1k_output: float = 0.0


@dataclass
class CloudConfig:
    provider: CloudProvider = CloudProvider.OPENAI
    api_key: str = ""
    base_url: str = ""
    model: str = "gpt-3.5-turbo"
    max_tokens: int = 4096
    temperature: float = 0.7
    timeout: float = 120.0
    max_cost_per_day: float = 10.0
    enabled: bool = False


@dataclass
class CloudUsage:
    total_requests: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost: float = 0.0
    last_request_at: float = 0.0


AVAILABLE_MODELS: list[CloudModel] = [
    CloudModel(CloudProvider.OPENAI, "gpt-4o-mini", "GPT-4o Mini", 128000, 0.00015, 0.0006),
    CloudModel(CloudProvider.OPENAI, "gpt-4o", "GPT-4o", 128000, 0.0025, 0.01),
    CloudModel(
        CloudProvider.ANTHROPIC,
        "claude-3-5-haiku-20241022",
        "Claude 3.5 Haiku",
        200000,
        0.001,
        0.005,
    ),
    CloudModel(
        CloudProvider.ANTHROPIC,
        "claude-sonnet-4-20250514",
        "Claude Sonnet 4",
        200000,
        0.003,
        0.015,
    ),
]


class CloudFallbackClient:
    """云端 API 回退客户端。"""

    def __init__(self, config: CloudConfig | None = None):
        self.config = config or CloudConfig()
        self._client: httpx.AsyncClient | None = None
        self._usage = CloudUsage()
        self._daily_cost = 0.0
        self._daily_reset = time.time()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.config.timeout)
        return self._client

    async def chat(
        self,
        messages: list[dict[str, Any]],
        model: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> dict[str, Any]:
        if not self.config.enabled:
            return {"error": "云端回退未启用"}
        if not self.config.api_key:
            return {"error": "缺少 API Key"}
        if self._check_daily_limit():
            return {"error": "已达到每日成本上限"}

        model = model or self.config.model
        temperature = temperature if temperature is not None else self.config.temperature
        max_tokens = max_tokens or self.config.max_tokens

        try:
            if self.config.provider == CloudProvider.OPENAI:
                result = await self._call_openai(messages, model, temperature, max_tokens)
            elif self.config.provider == CloudProvider.ANTHROPIC:
                result = await self._call_anthropic(messages, model, temperature, max_tokens)
            else:
                return {"error": f"不支持的云端提供商: {self.config.provider}"}

            self._update_usage(result)
            return result

        except Exception as e:
            logger.error(f"云端 API 调用失败: {e}")
            return {"error": str(e)}

    async def _call_openai(
        self,
        messages: list[dict[str, Any]],
        model: str,
        temperature: float,
        max_tokens: int,
    ) -> dict[str, Any]:
        base_url = self.config.base_url or "https://api.openai.com/v1"
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

        client = await self._get_client()
        resp = await client.post(f"{base_url}/chat/completions", json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def _call_anthropic(
        self,
        messages: list[dict[str, Any]],
        model: str,
        temperature: float,
        max_tokens: int,
    ) -> dict[str, Any]:
        base_url = self.config.base_url or "https://api.anthropic.com/v1"

        system_msg = ""
        user_messages = []
        for msg in messages:
            if msg.get("role") == "system":
                system_msg = msg.get("content", "")
            else:
                user_messages.append(msg)

        payload = {
            "model": model,
            "messages": user_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if system_msg:
            payload["system"] = system_msg

        headers = {
            "x-api-key": self.config.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }

        client = await self._get_client()
        resp = await client.post(f"{base_url}/messages", json=payload, headers=headers)
        resp.raise_for_status()

        data = resp.json()
        content = ""
        if data.get("content"):
            for block in data["content"]:
                if block.get("type") == "text":
                    content += block.get("text", "")

        return {
            "id": data.get("id", ""),
            "model": data.get("model", model),
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": data.get("stop_reason", "stop"),
                }
            ],
            "usage": {
                "prompt_tokens": data.get("usage", {}).get("input_tokens", 0),
                "completion_tokens": data.get("usage", {}).get("output_tokens", 0),
                "total_tokens": data.get("usage", {}).get("input_tokens", 0)
                + data.get("usage", {}).get("output_tokens", 0),
            },
            "provider": "anthropic",
        }

    def _check_daily_limit(self) -> bool:
        now = time.time()
        if now - self._daily_reset > 86400:
            self._daily_cost = 0.0
            self._daily_reset = now
        return self._daily_cost >= self.config.max_cost_per_day

    def _update_usage(self, result: dict[str, Any]) -> None:
        usage = result.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)

        self._usage.total_requests += 1
        self._usage.total_input_tokens += input_tokens
        self._usage.total_output_tokens += output_tokens
        self._usage.last_request_at = time.time()

        model_info = next((m for m in AVAILABLE_MODELS if m.model_id == result.get("model", "")), None)
        if model_info:
            cost = (
                input_tokens / 1000 * model_info.cost_per_1k_input
                + output_tokens / 1000 * model_info.cost_per_1k_output
            )
            self._usage.total_cost += cost
            self._daily_cost += cost

        logger.info(f"云端 API 使用: +{input_tokens}in/{output_tokens}out tokens, 日消费=${self._daily_cost:.4f}")

    def get_usage(self) -> dict[str, Any]:
        return {
            "total_requests": self._usage.total_requests,
            "total_input_tokens": self._usage.total_input_tokens,
            "total_output_tokens": self._usage.total_output_tokens,
            "total_cost": round(self._usage.total_cost, 4),
            "daily_cost": round(self._daily_cost, 4),
            "daily_limit": self.config.max_cost_per_day,
        }

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

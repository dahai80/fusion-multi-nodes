"""M6-04 传输数据脱敏 — 在节点间传输前自动擦除敏感数据。

- 正则匹配模式脱敏（手机号/身份证/邮箱/密钥等）
- 自定义规则
- JSON 深度遍历
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from re import Pattern
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ScrubRule:
    """脱敏规则。"""

    name: str
    pattern: str
    replacement: str = "***"
    description: str = ""
    enabled: bool = True

    def compile(self) -> Pattern:
        return re.compile(self.pattern)


DEFAULT_RULES: list[ScrubRule] = [
    ScrubRule(
        name="phone_cn",
        pattern=r"(?<!\d)1[3-9]\d{9}(?!\d)",
        replacement="***PHONE***",
        description="中国大陆手机号",
    ),
    ScrubRule(
        name="id_card_cn",
        pattern=r"(?<!\d)[1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx](?!\d)",
        replacement="***IDCARD***",
        description="中国身份证号",
    ),
    ScrubRule(
        name="email",
        pattern=r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        replacement="***EMAIL***",
        description="邮箱地址",
    ),
    ScrubRule(
        name="api_key",
        pattern=r"(?:api[_-]?key|apikey|secret[_-]?key|access[_-]?token|bearer)\s*[:=]\s*['\"]?[\w\-]{20,}['\"]?",
        replacement="***APIKEY***",
        description="API密钥/Token",
    ),
    ScrubRule(
        name="openai_key",
        pattern=r"\bsk-[a-zA-Z0-9]{20,}\b",
        replacement="***OPENAIKEY***",
        description="OpenAI API密钥",
    ),
    ScrubRule(
        name="github_pat",
        pattern=r"\bghp_[a-zA-Z0-9]{36}\b",
        replacement="***GITHUBPAT***",
        description="GitHub Personal Access Token",
    ),
    ScrubRule(
        name="slack_token",
        pattern=r"\bxox[baprs]-[a-zA-Z0-9-]{10,}\b",
        replacement="***SLACKTOKEN***",
        description="Slack Token (bot/user/app)",
    ),
    ScrubRule(
        name="jwt_token",
        pattern=r"\beyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\b",
        replacement="***JWT***",
        description="JWT Token (eyJ 三段式)",
    ),
    ScrubRule(
        name="aws_key",
        pattern=r"\bAKIA[0-9A-Z]{16}\b",
        replacement="***AWSKEY***",
        description="AWS Access Key",
    ),
    ScrubRule(
        name="private_key",
        pattern=r"-----BEGIN (?:RSA |EC |DSA )?PRIVATE KEY-----",
        replacement="***PRIVATEKEY***",
        description="PEM私钥头",
    ),
    ScrubRule(
        name="credit_card",
        pattern=r"\b4\d{12}(?:\d{3})?\b|\b5[1-5]\d{14}\b|\b3[47]\d{13}\b",
        replacement="***CARD***",
        description="信用卡号",
    ),
]


class DataScrubber:
    """数据脱敏器 — 在传输前自动擦除敏感信息。"""

    def __init__(
        self,
        rules: list[ScrubRule] | None = None,
        custom_rules: list[ScrubRule] | None = None,
    ):
        self._rules: list[ScrubRule] = list(rules or DEFAULT_RULES)
        if custom_rules:
            self._rules.extend(custom_rules)
        self._compiled: list[tuple[ScrubRule, Pattern]] = []
        self._recompile()

    def _recompile(self) -> None:
        self._compiled = [(rule, rule.compile()) for rule in self._rules if rule.enabled]
        logger.info(f"脱敏规则编译完成: {len(self._compiled)} 条生效")

    def add_rule(self, rule: ScrubRule) -> None:
        self._rules.append(rule)
        if rule.enabled:
            self._compiled.append((rule, rule.compile()))
        logger.info(f"添加脱敏规则: {rule.name}")

    def remove_rule(self, name: str) -> None:
        self._rules = [r for r in self._rules if r.name != name]
        self._recompile()
        logger.info(f"移除脱敏规则: {name}")

    def scrub_text(self, text: str) -> tuple[str, list[str]]:
        """脱敏纯文本，返回 (脱敏后文本, 命中规则列表)。"""
        hits: list[str] = []
        result = text
        for rule, pattern in self._compiled:
            if pattern.search(result):
                hits.append(rule.name)
                result = pattern.sub(rule.replacement, result)
        if hits:
            logger.debug(f"脱敏命中: {hits}")
        return result, hits

    def scrub_dict(self, data: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        """深度遍历字典并脱敏所有字符串值。"""
        all_hits: list[str] = []
        return self._scrub_dict_recursive(data, all_hits), all_hits

    def _scrub_dict_recursive(self, data: Any, hits: list[str]) -> Any:
        if isinstance(data, str):
            scrubbed, rule_hits = self.scrub_text(data)
            hits.extend(rule_hits)
            return scrubbed
        elif isinstance(data, dict):
            return {k: self._scrub_dict_recursive(v, hits) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._scrub_dict_recursive(item, hits) for item in data]
        return data

    def scrub_value(self, value: Any) -> tuple[Any, list[str]]:
        """脱敏任意值（自动判断类型）。"""
        if isinstance(value, str):
            return self.scrub_text(value)
        elif isinstance(value, dict):
            return self.scrub_dict(value)
        elif isinstance(value, list):
            all_hits: list[str] = []
            result = [self._scrub_dict_recursive(item, all_hits) for item in value]
            return result, all_hits
        return value, []

    @property
    def active_rules(self) -> list[str]:
        return [rule.name for rule, _ in self._compiled]

    @property
    def rule_count(self) -> int:
        return len(self._compiled)

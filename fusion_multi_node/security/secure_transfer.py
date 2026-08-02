"""M6-04 安全传输管线 — AST差分 + 数据脱敏 串联。

发送端: old_ast → compute_ast_diff → DataScrubber.scrub_dict → 传输脱敏diff
接收端: 收diff → apply_ast_diff → 还原完整AST
"""

from __future__ import annotations

import json
import logging
from typing import Any

from ..master.ast_diff import apply_ast_diff, compute_ast_diff
from .data_scrubber import DataScrubber

logger = logging.getLogger(__name__)


class SecureTransferPipeline:
    """安全传输管线 — AST差分提取变更 → PII脱敏 → 传输。"""

    def __init__(self, scrubber: DataScrubber | None = None):
        self._scrubber = scrubber or DataScrubber()

    def prepare_transfer(
        self,
        old_ast: dict[str, Any],
        new_ast: dict[str, Any],
    ) -> dict[str, Any]:
        diff = compute_ast_diff(old_ast, new_ast)
        scrubbed_diff, hits = self._scrubber.scrub_dict(diff)
        old_size = len(json.dumps(old_ast, ensure_ascii=False))
        diff_size = len(json.dumps(diff, ensure_ascii=False))
        scrubbed_size = len(json.dumps(scrubbed_diff, ensure_ascii=False))
        reduction_ratio = 1.0 - (scrubbed_size / old_size) if old_size > 0 else 0.0
        logger.info(
            f"安全传输准备: diff_size={diff_size} scrubbed_size={scrubbed_size} "
            f"reduction={reduction_ratio:.2%} scrubbed_rules={hits}"
        )
        return {
            "type": "ast_diff_scrubbed",
            "diff": scrubbed_diff,
            "scrubbed_rules": hits,
            "stats": {
                "original_size": old_size,
                "diff_size": diff_size,
                "scrubbed_size": scrubbed_size,
                "reduction_ratio": round(reduction_ratio, 4),
            },
        }

    def apply_transfer(
        self,
        base_ast: dict[str, Any],
        transfer_data: dict[str, Any],
    ) -> dict[str, Any]:
        transfer_type = transfer_data.get("type", "")
        if transfer_type != "ast_diff_scrubbed":
            logger.error(f"未知传输类型: {transfer_type}")
            return base_ast
        diff = transfer_data.get("diff", {})
        result = apply_ast_diff(base_ast, diff)
        logger.info(
            f"安全传输还原: scrubbed_rules={transfer_data.get('scrubbed_rules', [])} "
            f"stats={transfer_data.get('stats', {})}"
        )
        return result

    def prepare_text_transfer(self, text: str) -> tuple[str, list[str]]:
        scrubbed, hits = self._scrubber.scrub_text(text)
        logger.info(f"文本脱敏传输: hits={hits}")
        return scrubbed, hits

    def prepare_dict_transfer(self, data: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        scrubbed, hits = self._scrubber.scrub_dict(data)
        logger.info(f"字典脱敏传输: hits={hits}")
        return scrubbed, hits

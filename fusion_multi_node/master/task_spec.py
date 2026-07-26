"""M3-05 TaskSpec — 任务规格数据类，分离任务定义与运行时状态。"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class TaskSpec:
    """任务规格 — 定义"做什么"，与运行时状态无关。"""
    name: str
    model_name: str = ""
    mode: str = "data"
    timeout_seconds: float = 300.0
    user: str = ""
    required_capability: str = ""
    preferred_node_id: str = ""
    priority: TaskPriority = TaskPriority.NORMAL
    model_shards: List[Dict[str, Any]] = field(default_factory=list)
    input_data: Optional[Dict[str, Any]] = None
    parameters: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "model_name": self.model_name,
            "mode": self.mode,
            "timeout_seconds": self.timeout_seconds,
            "user": self.user,
            "required_capability": self.required_capability,
            "preferred_node_id": self.preferred_node_id,
            "priority": self.priority.value,
            "model_shards": self.model_shards,
            "input_data": self.input_data,
            "parameters": self.parameters,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TaskSpec:
        priority_val = data.get("priority", 1)
        if isinstance(priority_val, int):
            priority = TaskPriority(priority_val)
        elif isinstance(priority_val, str):
            try:
                priority = TaskPriority[priority_val]
            except KeyError:
                priority = TaskPriority.NORMAL
        else:
            priority = TaskPriority.NORMAL
        return cls(
            name=data.get("name", ""),
            model_name=data.get("model_name", ""),
            mode=data.get("mode", "data"),
            timeout_seconds=data.get("timeout_seconds", 300.0),
            user=data.get("user", ""),
            required_capability=data.get("required_capability", ""),
            preferred_node_id=data.get("preferred_node_id", ""),
            priority=priority,
            model_shards=data.get("model_shards", []),
            input_data=data.get("input_data"),
            parameters=data.get("parameters", {}),
        )

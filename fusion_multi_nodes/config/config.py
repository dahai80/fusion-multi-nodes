"""Fusion-Multi-Node 配置管理。"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from ..master import NodeInfo, ParallelMode
from ..agent import AgentConfig


class ClusterConfig:
    """集群全局配置管理。"""

    DEFAULT_CONFIG = {
        "cluster": {
            "name": "fusion-cluster",
            "master_host": "0.0.0.0",
            "master_port": 9753,
            "discovery_port": 9754,
            "agent_port": 9755,
            "mcp_port": 9756,
            "heartbeat_timeout": 15.0,
            "heartbeat_interval": 5.0,
            "report_interval": 15.0,
        },
        "parallel": {
            "default_mode": "pipeline",
            "pipeline_timeout": 300.0,
            "data_parallel_timeout": 120.0,
            "caveman_compress": True,
            "communication": "auto",
        },
        "mlx": {
            "fusion_mlx_port": 8000,
            "fusion_kb_port": 11434,
            "fusion_desk_port": 9000,
            "model_hub_port": 11435,
        },
        "mcp": {
            "enabled": True,
            "token_budget": 10_000_000,
            "tool_timeout": 60.0,
        },
        "observability": {
            "retention_hours": 24.0,
            "alert_enabled": True,
            "log_level": "info",
        },
    }

    def __init__(self, config_path: str = ""):
        self.config_path = config_path or str(
            Path.home() / ".fusion" / "multi-node" / "config.json"
        )
        self._data: Dict[str, Any] = {}
        self.load()

    def load(self) -> None:
        """加载配置。"""
        path = Path(self.config_path)
        if path.exists():
            try:
                with open(path) as f:
                    user_config = json.load(f)
                # 合并默认配置
                self._data = self._merge(self.DEFAULT_CONFIG, user_config)
            except Exception as e:
                print(f"加载配置失败: {e}")
                self._data = dict(self.DEFAULT_CONFIG)
        else:
            self._data = dict(self.DEFAULT_CONFIG)
            self.save()

    def save(self) -> None:
        """保存配置。"""
        path = Path(self.config_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置项。"""
        parts = key.split(".")
        data = self._data
        for part in parts:
            if isinstance(data, dict):
                data = data.get(part)
            else:
                return default
        return data if data is not None else default

    def set(self, key: str, value: Any) -> None:
        """设置配置项。"""
        parts = key.split(".")
        data = self._data
        for part in parts[:-1]:
            if part not in data:
                data[part] = {}
            data = data[part]
        data[parts[-1]] = value
        self.save()

    def _merge(self, base: Dict, override: Dict) -> Dict:
        """递归合并字典。"""
        result = dict(base)
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge(result[key], value)
            else:
                result[key] = value
        return result

    def to_node_agent_config(self) -> AgentConfig:
        """转换为 NodeAgent 配置。"""
        return AgentConfig(
            master_host=self.get("cluster.master_host"),
            master_port=self.get("cluster.master_port"),
            agent_port=self.get("cluster.agent_port"),
            fusion_desk_port=self.get("mlx.fusion_desk_port"),
            fusion_mlx_port=self.get("mlx.fusion_mlx_port"),
            heartbeat_interval=float(self.get("cluster.heartbeat_interval", 5.0)),
            report_interval=float(self.get("cluster.report_interval", 15.0)),
        )
"""Fusion-Multi-Node 配置管理。"""

from __future__ import annotations

import copy
import json
import logging
import os
from pathlib import Path
from typing import Any

from ..agent import AgentConfig


class ClusterConfig:
    """集群全局配置管理。"""

    DEFAULT_CONFIG = {
        "cluster": {
            "name": "fusion-cluster",
            "master_host": "127.0.0.1",
            "master_port": 11452,
            "discovery_port": 11450,
            "agent_port": 11445,
            "mcp_port": 11446,
            "heartbeat_timeout": 15.0,
            "heartbeat_interval": 3.0,
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
            "fusion_mlx_port": 11432,
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
        self.config_path = config_path or str(Path.home() / ".fusion" / "multi-node" / "config.json")
        self._data: dict[str, Any] = {}
        self.load()

    # v0.6.5 旧端口 → 当前默认端口映射（自动迁移）
    _STALE_PORT_MAP = {
        9753: 11452,  # master
        9754: 11450,  # discovery
        9755: 11445,  # agent
        9756: 11446,  # mcp
        8000: 11432,  # fusion_mlx（旧误用值）
    }
    # 端口键 → 期望默认值（用于校验迁移结果）
    _PORT_KEYS = {
        "cluster.master_port": 11452,
        "cluster.discovery_port": 11450,
        "cluster.agent_port": 11445,
        "cluster.mcp_port": 11446,
        "mlx.fusion_mlx_port": 11432,
    }

    def load(self) -> None:
        """加载配置。"""
        path = Path(self.config_path)
        if path.exists():
            try:
                with open(path) as f:
                    user_config = json.load(f)
                # 合并默认配置（深拷贝，避免 set() 污染类级 DEFAULT_CONFIG）
                self._data = self._merge(self.DEFAULT_CONFIG, user_config)
            except Exception as e:
                logging.getLogger(__name__).error(f"加载配置失败: {e}")
                self._data = copy.deepcopy(self.DEFAULT_CONFIG)
        else:
            self._data = copy.deepcopy(self.DEFAULT_CONFIG)
            self.save()
        # 自动迁移 v0.6.5 旧端口 + 清理测试残留字段
        if self._migrate_stale_ports():
            self.save()
            logging.getLogger(__name__).warning("配置已自动迁移 v0.6.5 旧端口到当前默认（%s）", self.config_path)

    def _migrate_stale_ports(self) -> bool:
        """检测并迁移旧端口；清理 cluster 下的测试残留字段。返回是否发生变更。"""
        changed = False
        cluster = self._data.get("cluster")
        if isinstance(cluster, dict):
            for key, default_val in self._PORT_KEYS.items():
                parts = key.split(".")
                cur = self._data
                for p in parts[:-1]:
                    cur = cur.get(p) if isinstance(cur, dict) else None
                if isinstance(cur, dict):
                    val = cur.get(parts[-1])
                    if val in self._STALE_PORT_MAP:
                        cur[parts[-1]] = self._STALE_PORT_MAP[val]
                        changed = True
            # master_host 0.0.0.0 → 127.0.0.1（默认绑定本机，避免外部暴露）
            host = cluster.get("master_host")
            if host == "0.0.0.0":
                cluster["master_host"] = "127.0.0.1"
                changed = True
            # 清理测试残留字段（custom_field / nested 等）
            for stale_key in list(cluster.keys()):
                if stale_key not in self.DEFAULT_CONFIG["cluster"] and stale_key in ("custom_field", "nested"):
                    del cluster[stale_key]
                    changed = True
        return changed

    def save(self) -> None:
        """保存配置 — 原子写: temp + os.replace, 防崩溃中途截断。"""
        path = Path(self.config_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(path.parent, 0o700)
        except OSError:
            pass
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        os.replace(tmp, path)

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

    def _merge(self, base: dict, override: dict) -> dict:
        """递归合并字典（深拷贝 base，避免共享嵌套引用污染 DEFAULT_CONFIG）。"""
        result = copy.deepcopy(base)
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
            heartbeat_interval=float(self.get("cluster.heartbeat_interval", 3.0)),
            report_interval=float(self.get("cluster.report_interval", 15.0)),
        )

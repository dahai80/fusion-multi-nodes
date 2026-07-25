"""Fusion-Multi-Node 工具函数。"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from urllib.parse import quote


def setup_logger(
    name: str = "fusion_multi_node",
    level: int = logging.INFO,
    verbose: bool = False,
) -> logging.Logger:
    """配置日志系统。"""
    if verbose:
        level = logging.DEBUG

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    if verbose:
        fmt = "[%(asctime)s] %(levelname)-8s %(name)s:%(lineno)d - %(message)s"
    else:
        fmt = "%(levelname)-8s %(message)s"
    handler.setFormatter(logging.Formatter(fmt, datefmt="%H:%M:%S"))
    logger.addHandler(handler)

    return logger


def get_data_dir() -> Path:
    """获取数据目录。"""
    data_dir = Path.home() / ".fusion" / "multi-node"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_log_dir() -> Path:
    """获取日志目录。"""
    log_dir = get_data_dir() / "logs"
    log_dir.mkdir(exist_ok=True)
    return log_dir


_HOST_RE = re.compile(r"^[a-zA-Z0-9._-]+$")


def build_url(host: str, port: int, path: str, scheme: str = "http") -> str:
    """构建 HTTP URL — 统一校验 host/port/path，防止拼接注入。

    - host 仅允许字母/数字/._-
    - port 范围 1-65535
    - path 必须以 / 开头，自动 URL-encode 路径段
    """
    if not _HOST_RE.match(host):
        raise ValueError(f"非法 host: {host!r}")
    if not (1 <= port <= 65535):
        raise ValueError(f"非法 port: {port}")
    if not path.startswith("/"):
        path = "/" + path
    encoded = "/".join(quote(seg, safe="") for seg in path.split("/"))
    return f"{scheme}://{host}:{port}{encoded}"
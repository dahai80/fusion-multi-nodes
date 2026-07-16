"""Fusion-Multi-Node 工具函数。"""

from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logger(
    name: str = "fusion_multi_nodes",
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
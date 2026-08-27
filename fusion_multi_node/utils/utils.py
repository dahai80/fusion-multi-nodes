"""Fusion-Multi-Node 工具函数。"""

from __future__ import annotations

import logging
import os
import re
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import quote


def setup_logger(
    name: str = "fusion_multi_node",
    level: int = logging.INFO,
    verbose: bool = False,
) -> logging.Logger:
    """配置日志系统。

    P1-16 (审计 §6.4): 默认仅控制台 (StreamHandler, 兼容测试 1 handler 断言)。
    设环境变量 FUSION_MULTINODE_LOG_FILE 时追加 RotatingFileHandler (10MB×5 上限),
    落盘应用日志有界, 崩溃重启循环下不再无界填盘。start.sh / launchd plist 设此 env。
    """
    if verbose:
        level = logging.DEBUG

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    if verbose:
        fmt = "[%(asctime)s] %(levelname)-8s %(name)s:%(lineno)d - %(message)s"
    else:
        fmt = "%(levelname)-8s %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%H:%M:%S")

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    log_file = os.environ.get("FUSION_MULTINODE_LOG_FILE")
    if log_file:
        try:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = RotatingFileHandler(
                log_path,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(level)
            logger.addHandler(file_handler)
        except OSError as e:
            # 落盘 handler 建失败不阻断启动 — 仍返回控制台 logger, 启动后可见此告警。
            sys.stderr.write(f"[setup_logger] RotatingFileHandler 建失败 ({log_file}): {e}\n")
            sys.stderr.flush()
    else:
        # P1-27 (审计 §6.4): 未配日志文件 + 命令行直起 (非 start.sh/launchd/docker, 三者均设 LOG_FILE env)
        # → 仅 stdout 落盘, 崩溃栈无处可查。提示运维设 env 或用受管启动方式。
        sys.stderr.write(
            "[setup_logger] 未配 FUSION_MULTINODE_LOG_FILE, 日志仅输出 stdout 不落盘; "
            "生产建议用 start.sh / launchd / docker-compose (均自动设此 env), 或手动 export 该 env\n"
        )
        sys.stderr.flush()

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

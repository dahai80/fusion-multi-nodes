"""审计日志 — 追加写 JSONL, 记录所有安全相关动作 (GAP-8, 复审计 2026-08-26)。

企业级商业生产发布阻塞项: 旧实现无审计日志 — 节点注册/审批/鉴权失败/权限拒绝/任务提交取消
等敏感动作无留痕, 事故不可溯源。本模块补齐: append-only JSONL 落盘 `~/.fusion/multi-node/audit.log`,
每行一条事件, 字段固定。

字段契约:
  ts       ISO8601 带时区时间戳
  actor    动作发起方 (node_id / "master" / "unknown" / ip)
  action   动作类型 (register/join/approve/reject/auth_fail/permission_deny/task_submit/task_cancel/config_reload)
  path     请求路径
  method   HTTP 方法
  node_id  动作目标节点 (注册/审批对象; 任务动作可空)
  result   ok | denied | error
  detail   人类可读补充 (拒因 / 角色等)

设计约束:
- 追加写 (open "a"), 进程内 threading.Lock 串行化, 不丢条目。
- 写失败只 log warning 不 raise — 审计日志不应拖垮主请求路径 (Rule 12 但安全日志降级,
  因 audit.log 不可用不应让鉴权通过/拒绝本身失败)。
- 路径经 FUSION_AUDIT_LOG 环境变量可覆盖 (测试隔离用)。
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path.home() / ".fusion" / "multi-node" / "audit.log"
_lock = threading.Lock()
_default: AuditLogger | None = None


def get_audit_logger() -> AuditLogger:
    """模块级单例 — 进程内复用同一 writer (同一 lock 串行写同一文件)。"""
    global _default
    if _default is None:
        with _lock:
            if _default is None:
                _default = AuditLogger()
    return _default


def reset_audit_logger() -> None:
    """测试用 — 清单例 (测试改 FUSION_AUDIT_LOG 后须 reset 才生效)。"""
    global _default
    with _lock:
        _default = None


class AuditLogger:
    """追加写 JSONL 审计日志。线程安全, 写失败降级不抛。"""

    def __init__(self, log_path: str | None = None):
        env_path = os.environ.get("FUSION_AUDIT_LOG", "").strip()
        if log_path:
            self._path = Path(log_path)
        elif env_path:
            self._path = Path(env_path)
        else:
            self._path = _DEFAULT_PATH
        self._write_lock = threading.Lock()
        # P1-7 (审计 §3.5): 写失败本地计数器 — 达阈值 (3 次) 日志升级 error 告警运维。
        # AuditLogger 无 observability 句柄 (独立模块), 用本地计数 + 日志级别升级 (同 P1-22 范式)。
        self._write_fail_count = 0
        self._WRITE_FAIL_ALERT_THRESHOLD = 3

    @property
    def path(self) -> Path:
        return self._path

    def log(
        self,
        *,
        actor: str = "unknown",
        action: str,
        path: str = "",
        method: str = "",
        node_id: str = "",
        result: str = "ok",
        detail: str = "",
    ) -> None:
        """写一条审计事件。绝不抛 — 失败只 warning。"""
        event = {
            "ts": datetime.now(UTC).isoformat(),
            "actor": actor,
            "action": action,
            "path": path,
            "method": method,
            "node_id": node_id,
            "result": result,
            "detail": detail,
        }
        line = json.dumps(event, ensure_ascii=False)
        try:
            with self._write_lock:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open("a", encoding="utf-8") as f:
                    f.write(line + "\n")
            # P1-7: 写成功清计数 (恢复正常)。
            self._write_fail_count = 0
        except Exception as e:
            # P1-7 (审计 §3.5): 写失败不 raise (不拖垮鉴权主路径), 但计数 + 达阈值升级 error。
            # 首两次 warning (降级可恢复), 第三次起 error (运维须介入, 磁盘满/权限丢)。
            self._write_fail_count += 1
            if self._write_fail_count >= self._WRITE_FAIL_ALERT_THRESHOLD:
                logger.error(
                    f"审计日志写入连续失败 {self._write_fail_count} 次 (磁盘满/权限丢?), 审计链降级中: {e} event={line}"
                )
            else:
                logger.warning(f"审计日志写入失败 (降级, 不影响主路径): {e} event={line}")

    def read(self) -> list[dict]:
        """读全部事件 — 测试校验用。损坏行跳过。"""
        if not self._path.exists():
            return []
        events: list[dict] = []
        try:
            with self._write_lock:
                with self._path.open("r", encoding="utf-8") as f:
                    for raw in f:
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            events.append(json.loads(raw))
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            # P1-7: 读失败同计数 + 阈值升级 error (审计读链降级, 排障/合规取证受阻)。
            self._write_fail_count += 1
            if self._write_fail_count >= self._WRITE_FAIL_ALERT_THRESHOLD:
                logger.error(f"审计日志读取连续失败 {self._write_fail_count} 次: {e}")
            else:
                logger.warning(f"审计日志读取失败: {e}")
        return events

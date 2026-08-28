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

import hashlib
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
        # issue #52 原语 1 — HMAC 链段: 每条记录追加 seq/prev_hash/mac, 篡改任前序字段断下条链。
        # 惰性派生 chain_key — 单例在中间件构造时 cluster_token 可能尚未注入 (env 延迟), 首次 log 才取。
        self._seq = 0
        self._prev_hash = ""
        self._chain_key: bytes | None = None
        self._chain_key_loaded = False

    @property
    def path(self) -> Path:
        return self._path

    def _ensure_chain_key(self) -> None:
        """惰性派生 HMAC 链密钥 — 从 cluster_token 经 HKDF (cluster_key 模块)。

        首次 log 时取 token; 失败 (token 未就绪/库缺) 降级 _chain_key=None
        → 该条及后续无链字段, guard 视为未验证基线 (审计不丢事件优先)。
        token 轮换后进程需重建单例 (reset_audit_logger) 才重派生 — v0.13.0 已知限制。
        """
        if self._chain_key_loaded:
            return
        try:
            from fusion_multi_node.security.cluster_key import derive_audit_chain_key
            from fusion_multi_node.utils.auth import load_or_create_token

            token = load_or_create_token()
            self._chain_key = derive_audit_chain_key(token)
        except Exception as e:
            # 降级: 无链字段, guard 视为基线。不 raise (审计不丢事件契约)。
            logger.warning(f"审计链密钥派生失败, 降级无链字段: {e}")
            self._chain_key = None
        finally:
            self._chain_key_loaded = True

    def _chain_payload(self, record: dict) -> bytes:
        """规范 JSON over (record 减 mac) — MAC 签名输入, 须确定性。"""
        from fusion_multi_node.security.cluster_key import canonical_json

        payload = {k: v for k, v in record.items() if k != "mac"}
        return canonical_json(payload)

    def _canonical_full(self, record: dict) -> bytes:
        """规范 JSON over 完整记录 (含 mac) — prev_hash 滚动锚点用。

        与 _chain_payload 同算法 (canonical_json), 但含 mac — guard 验证:
        sha256(_canonical_full(record)) == 下条 prev_hash。
        """
        from fusion_multi_node.security.cluster_key import canonical_json

        return canonical_json(record)

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
        # issue #52 原语 1 — HMAC 链段: seq 单调, prev_hash = 含 mac 的完整前序记录 sha256,
        # mac = HMAC-SHA256 over (record 减 mac)。链计算失败降级写无链字段 (审计不丢事件优先)。
        # 篡改任前序字段 → 重算 mac 不匹配 + 下条 prev_hash 断链, guard 双重检出。
        try:
            self._ensure_chain_key()
            if self._chain_key is not None:
                from fusion_multi_node.security.cluster_key import mac_payload

                self._seq += 1
                event["seq"] = self._seq
                event["prev_hash"] = self._prev_hash
                event["mac"] = mac_payload(self._chain_key, self._chain_payload(event))
                # prev_hash 滚动 = 含 mac 的完整记录 sha256 (下条链接锚点)。
                self._prev_hash = hashlib.sha256(self._canonical_full(event)).hexdigest()
        except Exception as e:
            # 降级: 该条无链字段, guard 视为基线。seq/prev_hash 不前进 (下条重新尝试)。
            logger.warning(f"审计链字段计算失败, 降级无链字段: {e}")
            event.pop("seq", None)
            event.pop("prev_hash", None)
            event.pop("mac", None)
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

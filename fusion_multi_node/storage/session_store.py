"""#84 集群侧共享会话存储 — agent-studio 跨节点 resume 的 transcript/checkpoint 载体。

Master 为唯一权威存储 (source of truth): 文件落盘 + HTTP API (/api/v1/session-store/*),
任一节点上的消费者经集群 registry 直连 master 水化/回写。语义对齐 fusion-agent-studio
SessionStore 适配器 (list/get/put/delete/fork), SQLite 默认路径不受影响 (消费侧自选后端)。
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_STORE_DIR = os.path.join(os.path.expanduser("~"), ".fusion", "multi-node", "session-store")


def resolve_store_dir(configured_dir: str = "") -> str:
    """存储目录优先级: 显式配置 > env FUSION_MULTINODE_SESSION_STORE_DIR > 默认路径。"""
    d = (configured_dir or "").strip() or os.environ.get("FUSION_MULTINODE_SESSION_STORE_DIR", "").strip()
    return d or DEFAULT_STORE_DIR


class ClusterSessionStore:
    """文件级会话存储 — 每会话一 JSON 文件, 原子 tmp+replace 落盘, 会话 id 作路径段强校验。"""

    def __init__(self, base_dir: str = ""):
        self.base_dir = Path(resolve_store_dir(base_dir))
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info("ClusterSessionStore ready dir=%s", self.base_dir)

    def _path(self, session_id: str) -> Path:
        if not session_id or "/" in session_id or "\\" in session_id or ".." in session_id or session_id in {".", ".."}:
            raise ValueError(f"非法会话 id: {session_id!r}")
        return self.base_dir / f"{session_id}.json"

    def list_sessions(self, limit: int = 50) -> list[dict]:
        rows: list[dict] = []
        for path in self.base_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as e:
                logger.warning("会话列表读取失败 path=%s: %s", path, e)
                continue
            data.pop("messages", None)
            rows.append(data)
        rows.sort(key=lambda s: float(s.get("updated_at", 0) or 0), reverse=True)
        return rows[: max(int(limit), 0)]

    def get_session(self, session_id: str) -> dict | None:
        try:
            path = self._path(session_id)
        except ValueError as e:
            logger.warning("会话读取拒绝: %s", e)
            return None
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            logger.warning("会话读取失败 id=%s: %s", session_id, e)
            return None

    def put_session(self, session: dict) -> dict:
        session_id = str((session or {}).get("id", "") or "")
        path = self._path(session_id)
        payload = dict(session or {})
        payload["id"] = session_id
        payload["updated_at"] = float(payload.get("updated_at") or 0.0) or time.time()
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
        logger.info(
            "会话回写 id=%s messages=%d bytes=%d",
            session_id,
            len(payload.get("messages") or []),
            path.stat().st_size,
        )
        return payload

    def delete_session(self, session_id: str) -> bool:
        try:
            path = self._path(session_id)
        except ValueError:
            return False
        if not path.exists():
            return False
        path.unlink()
        logger.info("会话删除 id=%s", session_id)
        return True

    def fork_session(self, session_id: str, title: str = "") -> dict | None:
        parent = self.get_session(session_id)
        if parent is None:
            return None
        forked = dict(parent)
        forked["id"] = f"chat-{int(time.time() * 1000):x}"
        forked["title"] = title or f"{parent.get('title', 'session')} (fork)"
        forked["metadata"] = {**(parent.get("metadata") or {}), "forked_from": session_id}
        return self.put_session(forked)

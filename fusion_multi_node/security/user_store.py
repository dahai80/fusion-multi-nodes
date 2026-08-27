"""用户令牌存储 — 多租户 per-user API 鉴权 (GAP-8, Phase F1)。

企业级多租户/远程接入阻塞项: 旧实现仅一个集群共享 Bearer token, 所有调用方共享同一身份,
无 per-user 鉴权、无 RBAC、task.user 客户自报可伪造。本模块补齐用户层:

- 用户记录持久化 `~/.fusion/multi-node/users.json` (100% 本地, 无云), FUSION_USERS_FILE 覆盖。
- 密钥只存 scrypt 哈希 (hashlib.scrypt, stdlib, 无新依赖); 明文 token 仅签发时返回一次。
- 令牌格式 fmu_<userid>_<secret> — BearerAuthMiddleware 按 fmu_ 前缀分流到本存储校验。
- 多活令牌 (rotation): 同一用户可持多个有效令牌, rotate 签新不废旧, revoke 废旧。

设计约束:
- 与 NodeRole (节点身份) 正交: UserRole 管用户层 RBAC (ADMIN/USER/VIEWER)。
- 本地文件原子写 (tmp + os.replace), 0600 权限, 加载失败不抛 (降级空库 + warning)。
- 集群内部 HTTP (master→agent 派发, agent 心跳, KV 跨节点) 仍用 cluster_token, 不走用户令牌。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import threading
from dataclasses import dataclass, field
from pathlib import Path

from fusion_multi_node.security.permission import UserRole

logger = logging.getLogger(__name__)

_USER_ID_PATTERN_LEN = 64
_SCRYPT_N = 16384
_SCRYPT_R = 8
_SCRYPT_P = 1
_SCRYPT_DKLEN = 32
_TOKEN_PREFIX = "fmu_"


def _default_users_path() -> Path:
    """惰性算默认路径 — 不在模块导入时冻结 (HOME 可被测试 monkeypatch, 与 audit_log 一致)。"""
    return Path.home() / ".fusion" / "multi-node" / "users.json"


@dataclass
class UserToken:
    tid: str
    token_hash: str  # scrypt hex
    salt: str  # hex
    created_at: float = 0.0
    label: str = ""


@dataclass
class UserRecord:
    user_id: str
    role: UserRole
    token_hash: str  # 留言级口令哈希 (create_user 设密码用; 令牌哈希见 tokens)
    salt: str
    tokens: list[UserToken] = field(default_factory=list)
    created_at: float = 0.0


def _hash_secret(secret: str, salt: bytes | None = None) -> tuple[str, str]:
    if salt is None:
        salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(
        secret.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_SCRYPT_DKLEN,
    )
    return dk.hex(), salt.hex()


def _hash_token_secret(secret: str, salt: bytes) -> str:
    dk = hashlib.scrypt(
        secret.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_SCRYPT_DKLEN,
    )
    return dk.hex()


class UserStore:
    """用户存储 — 文件持久化, 线程安全, 多活令牌。

    None-safe: UserStore is None (无 FUSION_USERS_FILE / users.json 且无 bootstrap)
    时 BearerAuthMiddleware 回退纯 cluster_token 路径, 单租户零配置向后兼容。
    """

    def __init__(self, path: str | None = None):
        env_path = os.environ.get("FUSION_USERS_FILE", "").strip()
        if path:
            self._path = Path(path)
        elif env_path:
            self._path = Path(env_path)
        else:
            self._path = _default_users_path()
        self._lock = threading.RLock()
        self._users: dict[str, UserRecord] = {}
        self._load()

    @property
    def path(self) -> Path:
        return self._path

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            with self._path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception as e:
            logger.warning(f"用户存储加载失败 (降级空库): {e}")
            self._users = {}
            return
        users: dict[str, UserRecord] = {}
        for uid, u in raw.items():
            try:
                role = UserRole(u["role"])
                rec = UserRecord(
                    user_id=uid,
                    role=role,
                    token_hash=u["token_hash"],
                    salt=u["salt"],
                    created_at=u.get("created_at", 0.0),
                    tokens=[
                        UserToken(
                            tid=t["tid"],
                            token_hash=t["token_hash"],
                            salt=t["salt"],
                            created_at=t.get("created_at", 0.0),
                            label=t.get("label", ""),
                        )
                        for t in u.get("tokens", [])
                    ],
                )
                users[uid] = rec
            except Exception as e:
                logger.warning(f"用户记录损坏跳过 {uid!r}: {e}")
        self._users = users
        logger.info(f"用户存储已加载: {len(self._users)} 用户 ({self._path})")

    def _save_locked(self) -> None:
        raw: dict[str, dict] = {}
        for uid, rec in self._users.items():
            raw[uid] = {
                "user_id": uid,
                "role": rec.role.value,
                "token_hash": rec.token_hash,
                "salt": rec.salt,
                "created_at": rec.created_at,
                "tokens": [
                    {
                        "tid": t.tid,
                        "token_hash": t.token_hash,
                        "salt": t.salt,
                        "created_at": t.created_at,
                        "label": t.label,
                    }
                    for t in rec.tokens
                ],
            }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        # P1-26 (审计 §6.4): flush+fsync 后再 replace, 防页缓存未刷盘崩溃丢令牌存储 (对齐 config.py save 范式)。
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._path)
        os.chmod(self._path, 0o600)

    def _save(self) -> None:
        with self._lock:
            self._save_locked()

    def list_users(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "user_id": rec.user_id,
                    "role": rec.role.value,
                    "created_at": rec.created_at,
                    "token_count": len(rec.tokens),
                }
                for rec in self._users.values()
            ]

    def get_user(self, user_id: str) -> UserRecord | None:
        with self._lock:
            return self._users.get(user_id)

    def create_user(self, user_id: str, role: UserRole, password: str = "") -> UserRecord:
        import time

        if not user_id or len(user_id) > _USER_ID_PATTERN_LEN:
            raise ValueError(f"非法 user_id: {user_id!r}")
        if not all(c.isalnum() or c in "-_" for c in user_id):
            raise ValueError(f"非法 user_id (仅字母数字 _ -): {user_id!r}")
        with self._lock:
            if user_id in self._users:
                raise ValueError(f"用户已存在: {user_id!r}")
            h, salt = _hash_secret(password or secrets.token_urlsafe(16))
            rec = UserRecord(
                user_id=user_id,
                role=role,
                token_hash=h,
                salt=salt,
                created_at=time.time(),
            )
            self._users[user_id] = rec
            self._save_locked()
            logger.info(f"用户已创建: {user_id} 角色={role.value}")
            return rec

    def delete_user(self, user_id: str) -> bool:
        with self._lock:
            if user_id not in self._users:
                return False
            del self._users[user_id]
            self._save_locked()
            logger.info(f"用户已删除: {user_id}")
            return True

    def set_role(self, user_id: str, role: UserRole) -> bool:
        with self._lock:
            rec = self._users.get(user_id)
            if rec is None:
                return False
            rec.role = role
            self._save_locked()
            logger.info(f"用户角色变更: {user_id} → {role.value}")
            return True

    def issue_token(self, user_id: str, label: str = "") -> str:
        """签发令牌 — 返回明文 fmu_<uid>_<secret>, 仅此一次。"""
        import time

        with self._lock:
            rec = self._users.get(user_id)
            if rec is None:
                raise KeyError(f"用户不存在: {user_id!r}")
            tid = secrets.token_urlsafe(8)
            secret = secrets.token_urlsafe(24)
            salt = secrets.token_bytes(16)
            token_hash = _hash_token_secret(secret, salt)
            rec.tokens.append(
                UserToken(
                    tid=tid,
                    token_hash=token_hash,
                    salt=salt.hex(),
                    created_at=time.time(),
                    label=label,
                )
            )
            self._save_locked()
            logger.info(f"令牌已签发: user={user_id} tid={tid} label={label!r}")
            return f"{_TOKEN_PREFIX}{user_id}_{secret}"

    def revoke_token(self, user_id: str, tid: str) -> bool:
        with self._lock:
            rec = self._users.get(user_id)
            if rec is None:
                return False
            before = len(rec.tokens)
            rec.tokens = [t for t in rec.tokens if t.tid != tid]
            if len(rec.tokens) == before:
                return False
            self._save_locked()
            logger.info(f"令牌已吊销: user={user_id} tid={tid}")
            return True

    def revoke_all_tokens(self, user_id: str) -> int:
        with self._lock:
            rec = self._users.get(user_id)
            if rec is None:
                return 0
            n = len(rec.tokens)
            rec.tokens = []
            self._save_locked()
            logger.info(f"全部令牌已吊销: user={user_id} count={n}")
            return n

    def rotate_user_token(self, user_id: str, label: str = "") -> str:
        """轮换 — 签新令牌, 旧令牌保留 (多活), 返回新明文。revoke 旧令牌另调。"""
        return self.issue_token(user_id, label=label or "rotated")

    def validate(self, token: str) -> tuple[str, UserRole] | None:
        """校验 fmu_<uid>_<secret> — 命中返回 (user_id, UserRole), 否则 None。

        常量时间比较 (secrets.compare_digest), 不泄露哪步失败。
        """
        if not token.startswith(_TOKEN_PREFIX):
            return None
        rest = token[len(_TOKEN_PREFIX) :]
        sep = rest.find("_")
        if sep <= 0:
            return None
        user_id = rest[:sep]
        secret = rest[sep + 1 :]
        if not user_id or not secret:
            return None
        with self._lock:
            rec = self._users.get(user_id)
            if rec is None:
                return None
            for t in rec.tokens:
                try:
                    salt = bytes.fromhex(t.salt)
                except ValueError:
                    continue
                candidate = _hash_token_secret(secret, salt)
                if secrets.compare_digest(candidate, t.token_hash):
                    return (user_id, rec.role)
            return None

    def is_empty(self) -> bool:
        with self._lock:
            return len(self._users) == 0

    def bootstrap_admin(self, user_id: str = "admin", password: str = "") -> str:
        """首启引导 — 无用户时创建 ADMIN, 返回首个令牌明文。已有用户则 no-op 返回空。"""
        with self._lock:
            if self._users:
                logger.debug("引导跳过: 用户库非空")
                return ""
        try:
            self.create_user(user_id, UserRole.ADMIN, password)
        except ValueError:
            # 已存在 (并发) — 直接签发
            pass
        return self.issue_token(user_id, label="bootstrap")


def load_user_store() -> UserStore | None:
    """加载用户存储 — FUSION_USERS_FILE 指定或默认路径存在则加载。

    无用户文件且无 bootstrap → 返回 None (中间件回退纯 cluster_token 路径)。
    有文件但空 → 返回 UserStore (is_empty True), 仍走用户令牌路径但全部 reject。
    """
    env_path = os.environ.get("FUSION_USERS_FILE", "").strip()
    default_exists = _default_users_path().exists()
    if not env_path and not default_exists:
        return None
    return UserStore()

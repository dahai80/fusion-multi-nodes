"""UserStore 单元测试 — 多租户用户令牌存储 (GAP-8 Phase F1)。

验: scrypt 哈希、令牌签发/校验/吊销/轮换、文件持久化 0600、原子写、
非法 user_id 拒绝、空库回退、bootstrap_admin。
"""

from __future__ import annotations

import json

import pytest

from fusion_multi_node.security.permission import UserRole
from fusion_multi_node.security.user_store import UserStore, load_user_store


class TestUserStoreCreate:
    def test_create_user_persists(self, tmp_path, monkeypatch):
        p = tmp_path / "users.json"
        monkeypatch.setenv("FUSION_USERS_FILE", str(p))
        store = UserStore()
        store.create_user("alice", UserRole.USER, "pw")
        assert p.exists()
        assert (p.stat().st_mode & 0o777) == 0o600
        raw = json.loads(p.read_text())
        assert "alice" in raw
        assert raw["alice"]["role"] == "user"
        # 密钥只存哈希, 不存明文
        assert "pw" not in p.read_text()

    def test_create_duplicate_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        with pytest.raises(ValueError, match="已存在"):
            store.create_user("alice", UserRole.ADMIN)

    def test_create_invalid_user_id(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        with pytest.raises(ValueError):
            store.create_user("bad/id", UserRole.USER)
        with pytest.raises(ValueError):
            store.create_user("a" * 65, UserRole.USER)
        with pytest.raises(ValueError):
            store.create_user("", UserRole.USER)

    def test_delete_user(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        assert store.delete_user("alice") is True
        assert store.delete_user("alice") is False
        assert store.get_user("alice") is None

    def test_set_role(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        assert store.set_role("alice", UserRole.ADMIN) is True
        assert store.get_user("alice").role == UserRole.ADMIN
        assert store.set_role("ghost", UserRole.ADMIN) is False


class TestUserStoreToken:
    def test_issue_and_validate(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        tok = store.issue_token("alice", label="dev")
        assert tok.startswith("fmu_alice_")
        result = store.validate(tok)
        assert result is not None
        uid, role = result
        assert uid == "alice"
        assert role == UserRole.USER

    def test_validate_wrong_secret(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        assert store.validate("fmu_alice_wrongsecret") is None

    def test_validate_unknown_user(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        assert store.validate("fmu_ghost_secret") is None

    def test_validate_non_fmu_token(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        assert store.validate("cluster-tok-xyz") is None
        assert store.validate("") is None

    def test_multiple_tokens_all_valid(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        t1 = store.issue_token("alice")
        t2 = store.issue_token("alice")
        assert t1 != t2
        assert store.validate(t1) is not None
        assert store.validate(t2) is not None

    def test_revoke_token(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        tok = store.issue_token("alice")
        tid = store.get_user("alice").tokens[0].tid
        assert store.revoke_token("alice", tid) is True
        assert store.validate(tok) is None
        assert store.revoke_token("alice", tid) is False

    def test_revoke_all_tokens(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        store.issue_token("alice")
        store.issue_token("alice")
        assert store.revoke_all_tokens("alice") == 2
        assert store.get_user("alice").tokens == []

    def test_rotate_keeps_old(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        old = store.issue_token("alice")
        new = store.rotate_user_token("alice")
        assert store.validate(old) is not None
        assert store.validate(new) is not None


class TestUserStorePersistence:
    def test_reload_after_restart(self, tmp_path, monkeypatch):
        p = tmp_path / "users.json"
        monkeypatch.setenv("FUSION_USERS_FILE", str(p))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        tok = store.issue_token("alice")
        # 模拟重启 — 新实例同文件
        store2 = UserStore()
        assert len(store2.list_users()) == 1
        assert store2.validate(tok) is not None
        assert store2.get_user("alice").role == UserRole.USER

    def test_atomic_write_no_partial(self, tmp_path, monkeypatch):
        p = tmp_path / "users.json"
        monkeypatch.setenv("FUSION_USERS_FILE", str(p))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        # 无 .tmp 残留
        assert not (tmp_path / "users.json.tmp").exists()
        # JSON 合法
        json.loads(p.read_text())

    def test_corrupt_file_degrades_to_empty(self, tmp_path, monkeypatch):
        p = tmp_path / "users.json"
        p.write_text("not json {{{")
        monkeypatch.setenv("FUSION_USERS_FILE", str(p))
        store = UserStore()
        assert store.is_empty() is True
        assert store.list_users() == []


class TestLoadUserStore:
    def test_returns_none_when_no_file(self, tmp_path, monkeypatch):
        # 无 env 无默认文件 → None (单租户零配置)
        monkeypatch.delenv("FUSION_USERS_FILE", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        assert load_user_store() is None

    def test_returns_store_when_env_set(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        loaded = load_user_store()
        assert loaded is not None
        # env 指向的文件不存在 → 空库
        assert loaded.is_empty() is True

    def test_returns_store_when_default_file_exists(self, tmp_path, monkeypatch):
        # 无 env 但默认路径存在 → 加载
        monkeypatch.delenv("FUSION_USERS_FILE", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        default = tmp_path / ".fusion" / "multi-node" / "users.json"
        default.parent.mkdir(parents=True, exist_ok=True)
        store = UserStore(path=str(default))
        store.create_user("alice", UserRole.USER)
        loaded = load_user_store()
        assert loaded is not None
        assert len(loaded.list_users()) == 1


class TestBootstrapAdmin:
    def test_bootstrap_creates_admin_and_token(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        tok = store.bootstrap_admin("admin")
        assert tok.startswith("fmu_admin_")
        assert store.validate(tok) == ("admin", UserRole.ADMIN)

    def test_bootstrap_noop_when_users_exist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUSION_USERS_FILE", str(tmp_path / "users.json"))
        store = UserStore()
        store.create_user("alice", UserRole.USER)
        assert store.bootstrap_admin("admin") == ""

"""v0.14.0 item 6 — CLI backup/restore 测试。

覆盖:
- create 生成 tar.gz + 含全部数据文件 + 0600 权限。
- restore 解包回写。
- --yes 跳确认。
- 空目录容错 (无数据文件仍生成空 tar 不抛)。
- 路径逃逸校验 (含 .. / 绝对路径的恶意 tar 拒解包)。
"""

from __future__ import annotations

import os
import tarfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from fusion_multi_node.cli import cli


@pytest.fixture
def isolated_home(tmp_path, monkeypatch):
    """隔离 HOME → tmp, backup 命令读 Path.home() / .fusion/multi-node。"""
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


def _seed_data(home: Path) -> dict[str, str]:
    """在 ~/.fusion/multi-node/ 写入测试数据文件, 返回 {name: content}。"""
    mn = home / ".fusion" / "multi-node"
    mn.mkdir(parents=True, exist_ok=True)
    (mn / "tls").mkdir(exist_ok=True)
    (mn / "kv").mkdir(exist_ok=True)
    files = {
        "config.json": '{"cluster": {}}',
        "tasks.json": "[]",
        "rule_epoch.json": '{"rule_epoch": 3}',
        "users.json": '{"users": {}}',
        "audit.log": "line1\nline2\n",
        ".cluster_token": "secret-token-xyz",
        "tls/ca.crt": "FAKE-CA-CERT",
        "kv/shard.bin": "FAKE-SHARD",
    }
    for name, content in files.items():
        p = mn / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    os.chmod(mn / ".cluster_token", 0o600)
    return files


class TestBackupCreate:
    def test_create_generates_targz_with_all_files(self, isolated_home):
        _seed_data(isolated_home)
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "create"])
        assert result.exit_code == 0, result.output
        assert "备份已创建" in result.output
        # 找生成的 tar.gz
        backups_dir = isolated_home / ".fusion" / "multi-node" / "backups"
        archives = list(backups_dir.glob("mn-*.tar.gz"))
        assert len(archives) == 1
        # 权限 0600
        assert oct(archives[0].stat().st_mode)[-3:] == "600"
        # 解 tar 验内容
        with tarfile.open(archives[0], "r:gz") as tar:
            names = tar.getnames()
        assert "config.json" in names
        assert "tasks.json" in names
        assert "rule_epoch.json" in names
        assert ".cluster_token" in names
        assert "tls/ca.crt" in names
        assert "kv/shard.bin" in names

    def test_create_custom_out_dir(self, isolated_home, tmp_path):
        _seed_data(isolated_home)
        out_dir = tmp_path / "custom-backups"
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "create", "--out", str(out_dir)])
        assert result.exit_code == 0, result.output
        assert len(list(out_dir.glob("mn-*.tar.gz"))) == 1

    def test_create_empty_dir_no_crash(self, isolated_home):
        # 无数据文件 → 仍生成 tar (空/仅子目录), 不抛
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "create"])
        assert result.exit_code == 0, result.output
        backups_dir = isolated_home / ".fusion" / "multi-node" / "backups"
        assert len(list(backups_dir.glob("mn-*.tar.gz"))) == 1

    def test_create_warns_about_token(self, isolated_home):
        _seed_data(isolated_home)
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "create"])
        assert "cluster_token 明文" in result.output


class TestBackupRestore:
    def test_restore_roundtrip(self, isolated_home, tmp_path):
        files = _seed_data(isolated_home)
        # create
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "create"])
        assert result.exit_code == 0
        archive = list((isolated_home / ".fusion" / "multi-node" / "backups").glob("mn-*.tar.gz"))[0]
        # 清空原数据 (模拟数据丢失)
        mn = isolated_home / ".fusion" / "multi-node"
        for name in files:
            (mn / name).unlink()
        assert not (mn / "config.json").exists()
        # restore --yes
        result = runner.invoke(cli, ["backup", "restore", "--in", str(archive), "--yes"])
        assert result.exit_code == 0, result.output
        assert "恢复完成" in result.output
        # 验回写
        assert (mn / "config.json").read_text() == files["config.json"]
        assert (mn / "rule_epoch.json").read_text() == files["rule_epoch.json"]
        assert (mn / ".cluster_token").read_text() == files[".cluster_token"]
        # token 权限复原 0600
        assert oct((mn / ".cluster_token").stat().st_mode)[-3:] == "600"

    def test_restore_confirm_prompt_aborted(self, isolated_home, tmp_path):
        _seed_data(isolated_home)
        runner = CliRunner()
        runner.invoke(cli, ["backup", "create"])
        archive = list((isolated_home / ".fusion" / "multi-node" / "backups").glob("mn-*.tar.gz"))[0]
        # 无 --yes 且输入 n → 中止
        result = runner.invoke(cli, ["backup", "restore", "--in", str(archive)], input="n\n")
        assert result.exit_code != 0

    def test_restore_corrupt_file_aborts(self, isolated_home, tmp_path):
        bad = tmp_path / "bad.tar.gz"
        bad.write_text("not a tar file")
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "restore", "--in", str(bad), "--yes"])
        assert result.exit_code != 0
        assert "损坏" in result.output or "tar.gz" in result.output

    def test_restore_rejects_path_traversal(self, isolated_home, tmp_path):
        # 构造恶意 tar 含 .. 路径 → restore 拒解包
        evil = tmp_path / "evil.tar.gz"
        with tarfile.open(evil, "w:gz") as tar:
            # 加一个合法条目让 tar 非空
            info = tarfile.TarInfo("config.json")
            info.size = 2
            import io

            tar.addfile(info, io.BytesIO(b"{}"))
            # 恶意逃逸条目
            info2 = tarfile.TarInfo("../escape.txt")
            info2.size = 4
            tar.addfile(info2, io.BytesIO(b"evil"))
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "restore", "--in", str(evil), "--yes"])
        assert result.exit_code != 0
        assert "不安全路径" in result.output
        # 验逃逸文件未被写出
        assert not (tmp_path.parent / "escape.txt").exists()

    def test_restore_rejects_symlink_escape(self, isolated_home, tmp_path):
        # TarSlip 变种: symlink linkname 越界 → restore 拒解包 (双层防护: 显式校验 + filter='data')
        evil = tmp_path / "evil-symlink.tar.gz"
        with tarfile.open(evil, "w:gz") as tar:
            # symlink 指向 dest 外绝对路径
            link = tarfile.TarInfo("evil-link")
            link.type = tarfile.SYMTYPE
            link.linkname = "/etc/passwd"
            tar.addfile(link)
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "restore", "--in", str(evil), "--yes"])
        assert result.exit_code != 0
        assert "不安全链接" in result.output
        # 验 symlink 未被创建于 dest
        assert not (isolated_home / ".fusion" / "multi-node" / "evil-link").exists()

    def test_restore_rejects_hardlink_escape(self, isolated_home, tmp_path):
        # hardlink linkname 越界 → restore 拒解包 (与 symlink 同校验路径)
        evil = tmp_path / "evil-hardlink.tar.gz"
        with tarfile.open(evil, "w:gz") as tar:
            link = tarfile.TarInfo("evil-hard")
            link.type = tarfile.LNKTYPE
            link.linkname = "../escape-target"
            tar.addfile(link)
        runner = CliRunner()
        result = runner.invoke(cli, ["backup", "restore", "--in", str(evil), "--yes"])
        assert result.exit_code != 0
        assert "不安全链接" in result.output

"""utils 模块测试。"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from fusion_multi_node.utils import get_data_dir, get_log_dir, setup_logger


class TestSetupLogger:
    def test_default_logger(self):
        logger = setup_logger("test_default")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_default"
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1

    def test_verbose_logger(self):
        logger = setup_logger("test_verbose", verbose=True)
        assert logger.level == logging.DEBUG
        handler = logger.handlers[0]
        fmt = handler.formatter._fmt
        assert "%(asctime)s" in fmt
        assert "%(lineno)d" in fmt

    def test_non_verbose_format(self):
        logger = setup_logger("test_non_verbose", verbose=False)
        handler = logger.handlers[0]
        fmt = handler.formatter._fmt
        assert "%(asctime)s" not in fmt
        assert "%(message)s" in fmt

    def test_custom_level(self):
        logger = setup_logger("test_custom_level", level=logging.WARNING)
        assert logger.level == logging.WARNING

    def test_verbose_overrides_level(self):
        logger = setup_logger("test_override", level=logging.WARNING, verbose=True)
        assert logger.level == logging.DEBUG

    def test_handlers_cleared(self):
        logger = setup_logger("test_clear")
        setup_logger("test_clear", level=logging.ERROR)
        assert len(logger.handlers) == 1


class TestRotatingFileHandler:
    # P1-16 (审计 §6.4): 设 FUSION_MULTINODE_LOG_FILE 时追加 RotatingFileHandler (有界落盘)。

    def test_env_adds_rotating_handler(self, tmp_path, monkeypatch):
        log_file = tmp_path / "app.log"
        monkeypatch.setenv("FUSION_MULTINODE_LOG_FILE", str(log_file))
        logger = setup_logger("test_rotate_env")
        # 1 StreamHandler + 1 RotatingFileHandler
        assert len(logger.handlers) == 2
        assert any(isinstance(h, RotatingFileHandler) for h in logger.handlers)
        assert log_file.parent.exists()

    def test_no_env_keeps_single_handler(self, monkeypatch):
        monkeypatch.delenv("FUSION_MULTINODE_LOG_FILE", raising=False)
        logger = setup_logger("test_rotate_no_env")
        assert len(logger.handlers) == 1
        assert not any(isinstance(h, RotatingFileHandler) for h in logger.handlers)

    def test_rotating_handler_writes_and_caps(self, tmp_path, monkeypatch):
        log_file = tmp_path / "app.log"
        monkeypatch.setenv("FUSION_MULTINODE_LOG_FILE", str(log_file))
        logger = setup_logger("test_rotate_write", level=logging.INFO)
        logger.info("hello rotation")
        for h in logger.handlers:
            h.flush()
        assert log_file.exists()
        assert "hello rotation" in log_file.read_text()
        # maxBytes=10MB, backupCount=5
        rh = next(h for h in logger.handlers if isinstance(h, RotatingFileHandler))
        assert rh.maxBytes == 10 * 1024 * 1024
        assert rh.backupCount == 5
        for h in logger.handlers:
            h.close()

    def test_bad_log_path_falls_back_console(self, tmp_path, monkeypatch):
        # 父目录是文件 → mkdir 失败 → 不阻断, 仍 1 StreamHandler (RotatingFileHandler 建失败跳过)。
        blocker = tmp_path / "blocker"
        blocker.write_text("x")
        monkeypatch.setenv("FUSION_MULTINODE_LOG_FILE", str(blocker / "app.log"))
        logger = setup_logger("test_rotate_bad")
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)


class TestGetDataDir:
    def test_returns_path(self):
        result = get_data_dir()
        assert isinstance(result, Path)
        assert ".fusion" in str(result)
        assert "multi-node" in str(result)

    def test_directory_exists(self):
        result = get_data_dir()
        assert result.exists()
        assert result.is_dir()


class TestGetLogDir:
    def test_returns_path(self):
        result = get_log_dir()
        assert isinstance(result, Path)
        assert "logs" in str(result)

    def test_directory_exists(self):
        result = get_log_dir()
        assert result.exists()
        assert result.is_dir()

    def test_under_data_dir(self):
        data_dir = get_data_dir()
        log_dir = get_log_dir()
        assert log_dir.parent == data_dir

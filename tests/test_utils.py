"""utils 模块测试。"""

import logging
from pathlib import Path

from fusion_multi_node.utils import setup_logger, get_data_dir, get_log_dir


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

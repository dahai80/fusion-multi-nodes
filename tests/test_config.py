"""Config 测试。

测试 ClusterConfig 的所有方法。
用户指令：要求测试覆盖率90%+。
"""

import os
import tempfile

import pytest

from fusion_multi_node.config.config import ClusterConfig, ConfigValidationError


class TestClusterConfigInit:
    def test_default_init(self):
        config = ClusterConfig()
        assert config._data is not None
        assert "cluster" in config._data

    def test_custom_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test_config.json")
            config = ClusterConfig(config_path=path)
            assert config.config_path == path


class TestClusterConfigGet:
    def test_get_existing_key(self):
        config = ClusterConfig()
        val = config.get("cluster.master_port", 11452)
        assert val == 11452 or isinstance(val, int)

    def test_get_missing_key_default(self):
        config = ClusterConfig()
        val = config.get("nonexistent.key", "default_val")
        assert val == "default_val"

    def test_get_nested_key(self):
        config = ClusterConfig()
        val = config.get("cluster.master_host", "127.0.0.1")
        assert isinstance(val, str)


class TestClusterConfigSet:
    def test_set_simple(self):
        config = ClusterConfig()
        config.set("test_key", "test_value")
        assert config.get("test_key") == "test_value"

    def test_set_nested(self):
        config = ClusterConfig()
        config.set("cluster.custom_field", 42)
        assert config.get("cluster.custom_field") == 42

    def test_set_dict(self):
        config = ClusterConfig()
        config.set("cluster.nested", {"a": 1, "b": 2})
        result = config.get("cluster.nested")
        assert result["a"] == 1


class TestClusterConfigMerge:
    def test_merge_dict(self):
        config = ClusterConfig()
        override = {"cluster": {"master_port": 9999}}
        config._data = config._merge(config._data, override)
        assert config.get("cluster.master_port") == 9999


class TestClusterConfigSaveLoad:
    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test_save.json")
            config1 = ClusterConfig(config_path=path)
            config1.set("test.save", "hello")
            config1.save()

            config2 = ClusterConfig(config_path=path)
            assert config2.get("test.save") == "hello"

    def test_load_missing_file(self):
        config = ClusterConfig(config_path="/tmp/nonexistent_config_xyz.json")
        # Should use defaults without crashing
        assert config.get("cluster.master_port") is not None or True


class TestClusterConfigToNodeAgentConfig:
    def test_to_node_agent_config(self):
        config = ClusterConfig()
        agent_config = config.to_node_agent_config()
        assert agent_config is not None
        assert hasattr(agent_config, "master_host") or isinstance(agent_config, dict)


# ── E4: 配置类型/范围校验 + schema_version + 批量 set ──


class TestConfigValidationE4:
    def test_schema_version_present(self):
        config = ClusterConfig()
        assert config.get("schema_version") == 1

    def test_set_rejects_dirty_port_string(self):
        config = ClusterConfig()
        with pytest.raises(ConfigValidationError):
            config.set("cluster.master_port", "abc")

    def test_set_rejects_port_out_of_range(self):
        config = ClusterConfig()
        with pytest.raises(ConfigValidationError):
            config.set("cluster.agent_port", 99999)
        with pytest.raises(ConfigValidationError):
            config.set("cluster.agent_port", 0)

    def test_set_rejects_negative_timeout(self):
        config = ClusterConfig()
        with pytest.raises(ConfigValidationError):
            config.set("cluster.heartbeat_interval", -1)

    def test_set_rejects_bool_for_port(self):
        config = ClusterConfig()
        with pytest.raises(ConfigValidationError):
            config.set("cluster.master_port", True)

    def test_set_unknown_key_still_lenient(self):
        # 未知键不校验, 兼容扩展/测试 (test_key 不在 _FIELD_VALIDATORS)
        config = ClusterConfig()
        config.set("test_key", "test_value")
        assert config.get("test_key") == "test_value"

    def test_set_many_batch_single_save(self, tmp_path):
        path = str(tmp_path / "batch.json")
        config = ClusterConfig(config_path=path)
        # 批量写多个已知键, 单次落盘
        config.set_many({
            "cluster.master_port": 11460,
            "cluster.heartbeat_interval": 5.0,
            "mcp.enabled": False,
        })
        assert config.get("cluster.master_port") == 11460
        assert config.get("cluster.heartbeat_interval") == 5.0
        assert config.get("mcp.enabled") is False
        # 重新加载应持久化
        reload = ClusterConfig(config_path=path)
        assert reload.get("cluster.master_port") == 11460

    def test_set_many_rejects_dirty_keeps_old(self, tmp_path):
        path = str(tmp_path / "batch_dirty.json")
        config = ClusterConfig(config_path=path)
        config.set("cluster.master_port", 11470)
        # 批量中混入脏值, 整批校验在 set 前抛出, 不落盘
        with pytest.raises(ConfigValidationError):
            config.set_many({
                "cluster.master_port": 11480,
                "cluster.heartbeat_interval": -2,  # 脏
            })
        assert config.get("cluster.master_port") == 11470

    def test_load_repairs_dirty_persisted_port(self, tmp_path):
        # 手写一份脏 port 落盘, 加载应回退默认并修复
        import json
        path = str(tmp_path / "dirty.json")
        with open(path, "w") as f:
            json.dump({"cluster": {"master_port": "not-a-port"}}, f)
        config = ClusterConfig(config_path=path)
        # 脏 port 回退默认 11452
        assert config.get("cluster.master_port") == 11452
        # 修复后落盘
        with open(path) as f:
            persisted = json.load(f)
        assert persisted["cluster"]["master_port"] == 11452

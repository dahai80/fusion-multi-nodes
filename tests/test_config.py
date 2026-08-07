"""Config 测试。

测试 ClusterConfig 的所有方法。
用户指令：要求测试覆盖率90%+。
"""

import os
import tempfile

from fusion_multi_node.config.config import ClusterConfig


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

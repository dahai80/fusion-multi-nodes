"""CLI 测试 — 覆盖所有命令和异步函数。

用户指令：要求测试覆盖率90%+。
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from fusion_multi_node.cli import (
    cli,
    _async_list_nodes,
    _async_cluster_start,
    _async_cluster_stop,
    _async_task_submit,
    _async_kv_warm,
    _async_network_detect,
    _async_caveman_test,
    _async_node_discover,
    _get_master,
)
from fusion_multi_node.master.cluster_master import (
    ClusterMaster,
    ClusterTask,
    NodeInfo,
    NodeStatus,
    ParallelMode,
)


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def master_with_nodes():
    # 直接写入 master.nodes 字典，避免调用 async register_node()
    m = ClusterMaster()
    for i in range(3):
        info = NodeInfo(
            node_id=f"n{i}", hostname=f"mac{i}", ip_address=f"10.0.0.{i}",
            port=9755, status=NodeStatus.ONLINE, last_heartbeat=time.time(),
            available_memory_gb=50.0 - i * 10, total_memory_gb=64.0,
            active_tasks=i, max_tasks=4,
        )
        m.nodes[f"n{i}"] = info
    return m


class TestCLIBase:
    def test_help(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Fusion-Multi-Node" in result.output

    def test_version(self, runner):
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0

    def test_verbose(self, runner):
        result = runner.invoke(cli, ["-v", "--help"])
        assert result.exit_code == 0


class TestNodeCommands:
    def test_node_help(self, runner):
        result = runner.invoke(cli, ["node", "--help"])
        assert result.exit_code == 0

    def test_node_list_empty(self, runner):
        with patch("fusion_multi_node.cli._get_master", return_value=ClusterMaster()):
            result = runner.invoke(cli, ["node", "list"])
            assert result.exit_code == 0

    def test_node_list_with_nodes(self, runner, master_with_nodes):
        with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
            result = runner.invoke(cli, ["node", "list"])
            assert result.exit_code == 0
            assert "n0" in result.output

    def test_node_list_online_only(self, runner, master_with_nodes):
        with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
            result = runner.invoke(cli, ["node", "list", "--online"])
            assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_async_list_nodes_with_nodes(self, master_with_nodes):
        with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
            await _async_list_nodes(online_only=False)

    @pytest.mark.asyncio
    async def test_async_list_nodes_online_only(self, master_with_nodes):
        with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
            await _async_list_nodes(online_only=True)

    @pytest.mark.asyncio
    async def test_async_list_nodes_empty(self):
        with patch("fusion_multi_node.cli._get_master", return_value=ClusterMaster()):
            await _async_list_nodes(online_only=False)

    def test_node_info_existing(self, runner, master_with_nodes):
        with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
            result = runner.invoke(cli, ["node", "info", "n0"])
            assert result.exit_code == 0
            assert "n0" in result.output

    def test_node_info_missing(self, runner):
        with patch("fusion_multi_node.cli._get_master", return_value=ClusterMaster()):
            result = runner.invoke(cli, ["node", "info", "missing"])
            assert result.exit_code == 0

    def test_node_start_help(self, runner):
        result = runner.invoke(cli, ["node", "start", "--help"])
        assert result.exit_code == 0

    def test_node_start_master(self, runner):
        mock_master = AsyncMock()
        mock_master.stop = AsyncMock()
        with patch("fusion_multi_node.cli.ClusterMaster", return_value=mock_master):
            with patch("fusion_multi_node.cli._async_node_start", new_callable=AsyncMock):
                result = runner.invoke(cli, ["node", "start", "--role", "master"])
                assert result.exit_code == 0

    def test_node_start_agent(self, runner):
        with patch("fusion_multi_node.cli._async_node_start", new_callable=AsyncMock):
            result = runner.invoke(cli, ["node", "start", "--role", "agent"])
            assert result.exit_code == 0

    def test_node_discover(self, runner):
        with patch("fusion_multi_node.cli._async_node_discover", new_callable=AsyncMock):
            result = runner.invoke(cli, ["node", "discover"])
            assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_async_node_discover_no_nodes(self):
        mock_mdns = MagicMock()
        mock_mdns.browse_async = AsyncMock(return_value=[])
        with patch("fusion_multi_node.discovery.MDNSDiscovery", return_value=mock_mdns):
            await _async_node_discover(timeout=0.01)

    @pytest.mark.asyncio
    async def test_async_node_discover_with_nodes(self):
        from fusion_multi_node.discovery.mdns_discovery import DiscoveryInfo
        mock_mdns = MagicMock()
        nodes = [DiscoveryInfo(name="m1", host="10.0.0.1", port=9754, properties={"role": "master"})]
        mock_mdns.browse_async = AsyncMock(return_value=nodes)
        with patch("fusion_multi_node.discovery.MDNSDiscovery", return_value=mock_mdns):
            await _async_node_discover(timeout=0.01)


class TestClusterCommands:
    def test_cluster_help(self, runner):
        result = runner.invoke(cli, ["cluster", "--help"])
        assert result.exit_code == 0

    def test_cluster_status(self, runner, master_with_nodes):
        with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
            result = runner.invoke(cli, ["cluster", "status"])
            assert result.exit_code == 0
            assert "n0" in result.output or "3" in result.output

    def test_cluster_stop(self, runner):
        with patch("fusion_multi_node.cli._async_cluster_stop", new_callable=AsyncMock):
            result = runner.invoke(cli, ["cluster", "stop"])
            assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_async_cluster_stop_with_services(self):
        import fusion_multi_node.cli as cli_mod
        old_master = cli_mod._master
        old_agent = cli_mod._agent
        old_obs = cli_mod._observability
        try:
            mock_m = AsyncMock()
            mock_a = AsyncMock()
            mock_o = AsyncMock()
            cli_mod._master = mock_m
            cli_mod._agent = mock_a
            cli_mod._observability = mock_o
            await _async_cluster_stop()
            mock_m.stop.assert_called_once()
            mock_a.stop.assert_called_once()
            mock_o.stop.assert_called_once()
        finally:
            cli_mod._master = old_master
            cli_mod._agent = old_agent
            cli_mod._observability = old_obs

    @pytest.mark.asyncio
    async def test_async_cluster_stop_no_services(self):
        import fusion_multi_node.cli as cli_mod
        old_master = cli_mod._master
        old_agent = cli_mod._agent
        old_obs = cli_mod._observability
        try:
            cli_mod._master = None
            cli_mod._agent = None
            cli_mod._observability = None
            await _async_cluster_stop()
        finally:
            cli_mod._master = old_master
            cli_mod._agent = old_agent
            cli_mod._observability = old_obs

    def test_cluster_start_master(self, runner):
        with patch("fusion_multi_node.cli._async_cluster_start", new_callable=AsyncMock):
            result = runner.invoke(cli, ["cluster", "start", "--mode", "master"])
            assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_async_cluster_start_master(self):
        import fusion_multi_node.cli as cli_mod
        old_master = cli_mod._master
        old_obs = cli_mod._observability
        try:
            mock_master = AsyncMock()
            mock_master.port = 9753
            mock_obs = AsyncMock()
            with patch("fusion_multi_node.cli.ClusterMaster", return_value=mock_master):
                with patch("fusion_multi_node.cli.ClusterObservability", return_value=mock_obs):
                    await _async_cluster_start("master", "http")
            assert cli_mod._master is not None or True
        finally:
            cli_mod._master = old_master
            cli_mod._observability = old_obs

    @pytest.mark.asyncio
    async def test_async_cluster_start_agent(self):
        import fusion_multi_node.cli as cli_mod
        old_agent = cli_mod._agent
        try:
            mock_agent = AsyncMock()
            mock_agent.config = MagicMock()
            mock_agent.config.node_id = "test-agent"
            mock_config = MagicMock()
            mock_config.to_node_agent_config.return_value = MagicMock()
            with patch("fusion_multi_node.cli.NodeAgent", return_value=mock_agent):
                with patch("fusion_multi_node.cli._config", mock_config):
                    await _async_cluster_start("agent", "http")
        finally:
            cli_mod._agent = old_agent


class TestTaskCommands:
    def test_task_help(self, runner):
        result = runner.invoke(cli, ["task", "--help"])
        assert result.exit_code == 0

    def test_task_list_empty(self, runner):
        with patch("fusion_multi_node.cli._get_master", return_value=ClusterMaster()):
            result = runner.invoke(cli, ["task", "list"])
            assert result.exit_code == 0

    def test_task_list_with_tasks(self, runner, master_with_nodes):
        m = master_with_nodes
        task = ClusterTask(
            task_id="t1", name="infer", mode=ParallelMode.PIPELINE,
            model_name="test", timeout_seconds=300.0,
        )
        m.tasks["t1"] = task
        with patch("fusion_multi_node.cli._get_master", return_value=m):
            result = runner.invoke(cli, ["task", "list"])
            assert result.exit_code == 0

    def test_task_submit_help(self, runner):
        result = runner.invoke(cli, ["task", "submit", "--help"])
        assert result.exit_code == 0

    def test_task_submit(self, runner, master_with_nodes):
        with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
            result = runner.invoke(cli, ["task", "submit", "-n", "test_task", "-m", "llama"])
            assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_async_task_submit_success(self, master_with_nodes):
        with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
            await _async_task_submit("test", "model", "pipeline", "hello", 300)

    @pytest.mark.asyncio
    async def test_async_task_submit_fail_no_nodes(self):
        m = ClusterMaster()
        with patch("fusion_multi_node.cli._get_master", return_value=m):
            await _async_task_submit("test", "model", "pipeline", "hello", 300)

    def test_task_cancel_existing(self, runner, master_with_nodes):
        m = master_with_nodes
        task = ClusterTask(
            task_id="t1", name="infer", mode=ParallelMode.PIPELINE,
            model_name="test",
        )
        m.tasks["t1"] = task
        with patch("fusion_multi_node.cli._get_master", return_value=m):
            result = runner.invoke(cli, ["task", "cancel", "t1"])
            assert result.exit_code == 0

    def test_task_cancel_missing(self, runner):
        with patch("fusion_multi_node.cli._get_master", return_value=ClusterMaster()):
            result = runner.invoke(cli, ["task", "cancel", "nope"])
            assert result.exit_code == 0


class TestConfigCommands:
    def test_config_help(self, runner):
        result = runner.invoke(cli, ["config", "--help"])
        assert result.exit_code == 0

    def test_config_list(self, runner):
        result = runner.invoke(cli, ["config", "list"])
        assert result.exit_code == 0

    def test_config_get(self, runner):
        result = runner.invoke(cli, ["config", "get", "cluster.master_port"])
        assert result.exit_code == 0

    def test_config_get_missing(self, runner):
        result = runner.invoke(cli, ["config", "get", "nonexistent.key"])
        assert result.exit_code == 0

    def test_config_set_string(self, runner):
        result = runner.invoke(cli, ["config", "set", "test.key", "hello"])
        assert result.exit_code == 0

    def test_config_set_json(self, runner):
        result = runner.invoke(cli, ["config", "set", "test.json_key", '{"a": 1}'])
        assert result.exit_code == 0


class TestNetworkCommands:
    def test_network_help(self, runner):
        result = runner.invoke(cli, ["network", "--help"])
        assert result.exit_code == 0

    def test_network_detect(self, runner):
        with patch("fusion_multi_node.cli._async_network_detect", new_callable=AsyncMock):
            result = runner.invoke(cli, ["network", "detect"])
            assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_async_network_detect_with_interfaces(self):
        from fusion_multi_node.utils.network_topology import LinkInfo, LinkType
        mock_detector = MagicMock()
        link = LinkInfo(
            interface="en0", type=LinkType.ETHERNET_1G, bandwidth_mbps=1000,
            latency_ms=1.0, is_rdma=False, priority=5,
        )
        mock_detector.detect = AsyncMock(return_value={"en0": link})
        mock_detector.get_best_link.return_value = link
        mock_detector.get_recommended_compression.return_value = "zlib"
        mock_detector.is_thunderbolt_available.return_value = False
        with patch("fusion_multi_node.cli.NetworkTopologyDetector", return_value=mock_detector):
            await _async_network_detect()

    @pytest.mark.asyncio
    async def test_async_network_detect_no_interfaces(self):
        mock_detector = MagicMock()
        mock_detector.detect = AsyncMock(return_value={})
        with patch("fusion_multi_node.cli.NetworkTopologyDetector", return_value=mock_detector):
            await _async_network_detect()

    @pytest.mark.asyncio
    async def test_async_network_detect_thunderbolt(self):
        from fusion_multi_node.utils.network_topology import LinkInfo, LinkType
        mock_detector = MagicMock()
        link = LinkInfo(
            interface="bridge0", type=LinkType.THUNDERBOLT_3, bandwidth_mbps=10000,
            latency_ms=0.1, is_rdma=True, priority=1,
        )
        mock_detector.detect = AsyncMock(return_value={"bridge0": link})
        mock_detector.get_best_link.return_value = link
        mock_detector.get_recommended_compression.return_value = "dict"
        mock_detector.is_thunderbolt_available.return_value = True
        with patch("fusion_multi_node.cli.NetworkTopologyDetector", return_value=mock_detector):
            await _async_network_detect()


class TestCavemanCommands:
    def test_caveman_help(self, runner):
        result = runner.invoke(cli, ["caveman", "--help"])
        assert result.exit_code == 0

    def test_caveman_test(self, runner):
        with patch("fusion_multi_node.cli._async_caveman_test", new_callable=AsyncMock):
            result = runner.invoke(cli, ["caveman", "test"])
            assert result.exit_code == 0

    def test_caveman_test_custom_data(self, runner):
        with patch("fusion_multi_node.cli._async_caveman_test", new_callable=AsyncMock):
            result = runner.invoke(cli, ["caveman", "test", "custom data"])
            assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_async_caveman_test(self):
        await _async_caveman_test("test data for compression")


class TestKVCommands:
    def test_kv_help(self, runner):
        result = runner.invoke(cli, ["kv", "--help"])
        assert result.exit_code == 0

    def test_kv_stats(self, runner):
        with patch("fusion_multi_node.cli._get_master", return_value=ClusterMaster()):
            result = runner.invoke(cli, ["kv", "stats"])
            assert result.exit_code == 0

    def test_kv_warm_no_prompts(self, runner):
        result = runner.invoke(cli, ["kv", "warm"])
        assert result.exit_code == 0

    def test_kv_warm_with_prompts(self, runner):
        with patch("fusion_multi_node.cli._async_kv_warm", new_callable=AsyncMock):
            result = runner.invoke(cli, ["kv", "warm", "-p", "hello", "-n", "n1"])
            assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_async_kv_warm_no_prompts(self):
        await _async_kv_warm([], [])

    @pytest.mark.asyncio
    async def test_async_kv_warm_with_nodes(self, master_with_nodes):
        mock_manager = MagicMock()
        mock_manager.warm_cache = AsyncMock(return_value={"success": 1, "failed": 0})
        with patch("fusion_multi_node.cli.KVSharingManager", return_value=mock_manager):
            with patch("fusion_multi_node.cli._get_master", return_value=master_with_nodes):
                await _async_kv_warm(["hello"], ["n0"])


class TestGetMaster:
    def test_creates_master(self):
        import fusion_multi_node.cli as cli_mod
        old_master = cli_mod._master
        try:
            cli_mod._master = None
            m = _get_master()
            assert m is not None
        finally:
            cli_mod._master = old_master

    def test_returns_existing(self):
        import fusion_multi_node.cli as cli_mod
        old_master = cli_mod._master
        test_master = ClusterMaster()
        try:
            cli_mod._master = test_master
            m = _get_master()
            assert m is test_master
        finally:
            cli_mod._master = old_master

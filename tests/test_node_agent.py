"""Node Agent coverage tests."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fusion_multi_node.agent.node_agent import AgentConfig, NodeAgent


def _make_mock_client(mock_resp=None, side_effect=None):
    mock_client = AsyncMock()
    if side_effect:
        mock_client.post = AsyncMock(side_effect=side_effect)
    else:
        mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=mock_client)


class TestAgentConfig:
    def test_defaults(self):
        cfg = AgentConfig()
        assert cfg.master_host == "localhost"
        assert cfg.master_port == 11452
        assert cfg.agent_port == 11445
        assert cfg.heartbeat_interval > 0

    def test_custom(self):
        cfg = AgentConfig(master_host="10.0.0.1", master_port=8888, node_id="custom_id")
        assert cfg.master_host == "10.0.0.1"
        assert cfg.master_port == 8888
        assert cfg.node_id == "custom_id"


class TestNodeAgentInit:
    def test_init_default(self):
        agent = NodeAgent()
        assert agent.config is not None
        assert agent.config.node_id != ""
        assert agent._running is False

    def test_init_with_config(self):
        cfg = AgentConfig(master_host="10.0.0.1", master_port=8888)
        agent = NodeAgent(config=cfg)
        assert agent.config.master_host == "10.0.0.1"

    def test_init_generates_node_id(self):
        cfg = AgentConfig()
        agent = NodeAgent(config=cfg)
        assert agent.config.node_id.startswith("node_")


class TestNodeAgentHardware:
    def test_collect_hardware_info(self):
        agent = NodeAgent()
        info = agent.collect_hardware_info()
        assert info["cpu_cores"] > 0
        assert info["total_memory_gb"] > 0
        assert info["available_memory_gb"] > 0
        assert "node_id" in info
        assert "hostname" in info
        assert "ip_address" in info
        assert "arch" in info

    def test_get_local_ip(self):
        agent = NodeAgent()
        ip = agent._get_local_ip()
        assert isinstance(ip, str)
        assert len(ip) > 0

    def test_get_mlx_version_not_running(self):
        agent = NodeAgent()
        version = agent._get_mlx_version()
        assert isinstance(version, str)

    def test_get_gpu_info(self):
        agent = NodeAgent()
        cores, model = agent._get_gpu_info()
        assert isinstance(cores, int)
        assert cores >= 0
        assert isinstance(model, str)

    def test_check_service_not_running(self):
        agent = NodeAgent()
        assert agent._check_service(19999) is False


class TestNodeAgentSendHeartbeat:
    @pytest.mark.asyncio
    async def test_send_heartbeat_success(self):
        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            ok = await agent.send_heartbeat()
        assert ok is True

    @pytest.mark.asyncio
    async def test_send_heartbeat_failure(self):
        agent = NodeAgent()
        mock_ac_class = _make_mock_client(side_effect=Exception("connection error"))
        with patch("httpx.AsyncClient", mock_ac_class):
            ok = await agent.send_heartbeat()
        assert ok is False

    @pytest.mark.asyncio
    async def test_send_heartbeat_with_current_task(self):
        agent = NodeAgent()
        agent._current_task = {"task_id": "t1"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            ok = await agent.send_heartbeat()
        assert ok is True


class TestNodeAgentReportHardware:
    @pytest.mark.asyncio
    async def test_report_hardware_success(self):
        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            ok = await agent.report_hardware()
        assert ok is True

    @pytest.mark.asyncio
    async def test_report_hardware_failure(self):
        agent = NodeAgent()
        mock_ac_class = _make_mock_client(side_effect=Exception("timeout"))
        with patch("httpx.AsyncClient", mock_ac_class):
            ok = await agent.report_hardware()
        assert ok is False


class TestNodeAgentExecuteTask:
    @pytest.mark.asyncio
    async def test_execute_task_inference(self):
        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "world"}}],
            "usage": {"total_tokens": 42},
        }
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await agent.execute_task({
                "task_id": "t1",
                "type": "inference",
                "model": "test",
                "params": {"prompt": "hello"},
            })
        assert result["task_id"] == "t1"
        assert result["content"] == "world"
        assert agent._current_task is None

    @pytest.mark.asyncio
    async def test_execute_task_inference_with_messages(self):
        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "response"}}],
            "usage": {},
        }
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await agent.execute_task({
                "task_id": "t2",
                "type": "inference",
                "model": "test",
                "params": {"messages": [{"role": "user", "content": "hi"}]},
            })
        assert result["content"] == "response"

    @pytest.mark.asyncio
    async def test_execute_task_embedding(self):
        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "data": [{"embedding": [0.1, 0.2, 0.3]}],
        }
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await agent.execute_task({
                "task_id": "t3",
                "type": "embedding",
                "model": "BGE-M3",
                "params": {"text": "hello"},
            })
        assert "embedding" in result
        assert result["dimensions"] == 3

    @pytest.mark.asyncio
    async def test_execute_task_plugin(self):
        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"result": "plugin_ok"}
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await agent.execute_task({
                "task_id": "t4",
                "type": "plugin",
                "plugin": "test_plugin",
                "action": "run",
                "params": {"key": "value"},
            })
        assert result["result"] == "plugin_ok"

    @pytest.mark.asyncio
    async def test_execute_task_unknown_type(self):
        agent = NodeAgent()
        result = await agent.execute_task({"task_id": "t5", "type": "unknown"})
        assert "error" in result
        assert agent._current_task is None

    @pytest.mark.asyncio
    async def test_execute_task_exception(self):
        agent = NodeAgent()
        mock_ac_class = _make_mock_client(side_effect=Exception("connection error"))
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await agent.execute_task({
                "task_id": "t6",
                "type": "inference",
                "model": "test",
                "params": {"prompt": "hello"},
            })
        assert "error" in result
        assert agent._current_task is None


class TestNodeAgentReportFault:
    @pytest.mark.asyncio
    async def test_report_fault_success(self):
        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            ok = await agent.report_fault("crash", "oom")
        assert ok is True

    @pytest.mark.asyncio
    async def test_report_fault_failure(self):
        agent = NodeAgent()
        mock_ac_class = _make_mock_client(side_effect=Exception("timeout"))
        with patch("httpx.AsyncClient", mock_ac_class):
            ok = await agent.report_fault("crash", "oom")
        assert ok is False


class TestNodeAgentLifecycle:
    @pytest.mark.asyncio
    async def test_start_without_server(self):
        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            await agent.start(with_server=False, auto_discover=False)
        assert agent._running is True
        agent._running = False

    @pytest.mark.asyncio
    async def test_stop(self):
        agent = NodeAgent()
        agent._running = True
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            await agent.stop()
        assert agent._running is False

    @pytest.mark.asyncio
    async def test_heartbeat_loop(self):
        agent = NodeAgent()
        agent._running = True
        agent.config.heartbeat_interval = 0.05
        call_count = 0

        async def mock_heartbeat():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                agent._running = False

        agent.send_heartbeat = mock_heartbeat
        await agent._heartbeat_loop()
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_hardware_report_loop(self):
        agent = NodeAgent()
        agent._running = True
        agent.config.report_interval = 0.05
        call_count = 0

        original_collect = agent.collect_hardware_info
        def mock_collect():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                agent._running = False
            return original_collect()
        agent.collect_hardware_info = mock_collect

        await agent._hardware_report_loop()
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_discover_master_success(self):
        agent = NodeAgent()
        mock_master_info = MagicMock()
        mock_master_info.host = "10.0.1.100"
        mock_master_info.port = 11452

        mock_mdns_class = MagicMock()
        mock_mdns_instance = MagicMock()
        mock_mdns_instance.find_master_async = AsyncMock(return_value=mock_master_info)
        mock_mdns_class.return_value = mock_mdns_instance

        with patch("fusion_multi_node.discovery.MDNSDiscovery", mock_mdns_class):
            result = await agent._discover_master()
        assert result is True
        assert agent.config.master_host == "10.0.1.100"

    @pytest.mark.asyncio
    async def test_discover_master_not_found(self):
        agent = NodeAgent()
        mock_mdns_class = MagicMock()
        mock_mdns_instance = MagicMock()
        mock_mdns_instance.find_master_async = AsyncMock(return_value=None)
        mock_mdns_class.return_value = mock_mdns_instance

        with patch("fusion_multi_node.discovery.MDNSDiscovery", mock_mdns_class):
            result = await agent._discover_master()
        assert result is False

    @pytest.mark.asyncio
    async def test_discover_master_exception(self):
        agent = NodeAgent()
        with patch("fusion_multi_node.discovery.MDNSDiscovery", side_effect=ImportError("no module")):
            result = await agent._discover_master()
        assert result is False

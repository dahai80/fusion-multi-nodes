"""Node Agent coverage tests."""

import asyncio
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
        assert cfg.agent_port == 11458
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

    @pytest.mark.asyncio
    async def test_report_hardware_collects_off_event_loop(self):
        # P1-10 (审计 §4.5): collect_hardware_info 同步阻塞须在 to_thread 里跑,
        # 不在事件循环线程 — 验证调用线程 ≠ 当前事件循环线程。
        import threading

        loop_thread = threading.get_ident()
        seen = {}

        def fake_collect():
            seen["tid"] = threading.get_ident()
            return MagicMock(node_id="n1")

        agent = NodeAgent()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_ac_class = _make_mock_client(mock_resp)
        with (
            patch.object(agent, "collect_hardware_info", side_effect=fake_collect),
            patch("httpx.AsyncClient", mock_ac_class),
        ):
            ok = await agent.report_hardware()
        assert ok is True
        assert seen["tid"] != loop_thread, "同步阻塞调用须移出事件循环 (to_thread)"


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
            result = await agent.execute_task(
                {
                    "task_id": "t1",
                    "type": "inference",
                    "model": "test",
                    "params": {"prompt": "hello"},
                }
            )
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
            result = await agent.execute_task(
                {
                    "task_id": "t2",
                    "type": "inference",
                    "model": "test",
                    "params": {"messages": [{"role": "user", "content": "hi"}]},
                }
            )
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
            result = await agent.execute_task(
                {
                    "task_id": "t3",
                    "type": "embedding",
                    "model": "BGE-M3",
                    "params": {"text": "hello"},
                }
            )
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
            result = await agent.execute_task(
                {
                    "task_id": "t4",
                    "type": "plugin",
                    "plugin": "test_plugin",
                    "action": "run",
                    "params": {"key": "value"},
                }
            )
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
            result = await agent.execute_task(
                {
                    "task_id": "t6",
                    "type": "inference",
                    "model": "test",
                    "params": {"prompt": "hello"},
                }
            )
        assert "error" in result
        assert agent._current_task is None

    @pytest.mark.asyncio
    async def test_execute_task_dedup_rejects_running_task_id(self):
        # P1-14 (审计 §5.3): 同 task_id 仍在运行 → 拒重复派发, 返回 dedup_blocked。
        agent = NodeAgent()
        agent._dispatch_token = "test-token"
        # 植入一个"运行中"占位句柄, 模拟上一执行未结束
        placeholder = asyncio.sleep(100)
        agent._running_task_handles["dup-tid"] = asyncio.create_task(placeholder)
        try:
            result = await agent.execute_task(
                {"task_id": "dup-tid", "type": "inference", "model": "test", "params": {"prompt": "x"}}
            )
        finally:
            agent._running_task_handles["dup-tid"].cancel()
            try:
                await agent._running_task_handles["dup-tid"]
            except (asyncio.CancelledError, Exception):
                pass
        assert result.get("dedup_blocked") is True
        assert "已运行" in result.get("error", "") or "重复" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_execute_task_overload_rejects_at_capacity(self):
        # P1-18 (审计 §6.6): _running_task_handles 达 max_tasks 上限 → 拒收 overload=True。
        cfg = AgentConfig()
        cfg.max_tasks = 1
        agent = NodeAgent(config=cfg)
        # 植入 1 个占位运行句柄填满容量 (max_tasks=1)
        placeholder = asyncio.sleep(100)
        agent._running_task_handles["running-1"] = asyncio.create_task(placeholder)
        try:
            result = await agent.execute_task(
                {"task_id": "overload-t", "type": "inference", "model": "test", "params": {"prompt": "x"}}
            )
        finally:
            agent._running_task_handles["running-1"].cancel()
            try:
                await agent._running_task_handles["running-1"]
            except (asyncio.CancelledError, Exception):
                pass
        assert result.get("overload") is True
        assert "已满" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_execute_task_anon_id_no_collision(self):
        # P1-14: 无 task_id 的直接调用分配匿名 id, 多次顺序调用序号递增不撞键。
        agent = NodeAgent()
        agent._dispatch_token = "test-token"
        assert agent._anon_task_seq == 0
        for _ in range(3):
            await agent.execute_task({"type": "unknown"})
        assert agent._anon_task_seq == 3


class TestNodeAgentSandboxGate:
    """M6-02 WorkerSandbox 接 NodeAgent 执行路径 (AR审计 #24 硬伤5)。"""

    @pytest.mark.asyncio
    async def test_no_sandbox_passthrough(self):
        # 无沙箱 = 原行为, 不 gate
        agent = NodeAgent()
        assert agent._sandbox is None
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "ok"}}],
            "usage": {},
        }
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await agent.execute_task(
                {"task_id": "s1", "type": "inference", "model": "m", "params": {"prompt": "hi"}}
            )
        assert result.get("sandbox_blocked") is None
        assert result["content"] == "ok"

    @pytest.mark.asyncio
    async def test_sandbox_allows_tmp_inference(self):
        from fusion_multi_node.security.sandbox import SandboxConfig, WorkerSandbox

        sandbox = WorkerSandbox(SandboxConfig(allowed_paths=["/tmp"]))
        agent = NodeAgent(sandbox=sandbox)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "ran"}}],
            "usage": {},
        }
        mock_ac_class = _make_mock_client(mock_resp)
        with patch("httpx.AsyncClient", mock_ac_class):
            result = await agent.execute_task(
                {"task_id": "s2", "type": "inference", "model": "m", "params": {"prompt": "hi"}}
            )
        assert result.get("sandbox_blocked") is None
        assert result["content"] == "ran"

    @pytest.mark.asyncio
    async def test_sandbox_blocks_forbidden_model_path(self):
        from fusion_multi_node.security.sandbox import SandboxConfig, WorkerSandbox

        sandbox = WorkerSandbox(SandboxConfig(allowed_paths=["/tmp"]))
        agent = NodeAgent(sandbox=sandbox)
        result = await agent.execute_task(
            {
                "task_id": "s3",
                "type": "inference",
                "model": "m",
                "params": {"prompt": "hi", "model_path": "/etc/shadow"},
            }
        )
        assert result.get("sandbox_blocked") is True
        assert "模型路径" in result["error"]
        assert agent._current_task is None

    @pytest.mark.asyncio
    async def test_sandbox_blocks_model_sync_network(self):
        from fusion_multi_node.security.sandbox import SandboxConfig, WorkerSandbox

        sandbox = WorkerSandbox(SandboxConfig(allowed_network_hosts=["192.168.1.10"]))
        agent = NodeAgent(sandbox=sandbox)
        result = await agent.execute_task(
            {
                "task_id": "s4",
                "type": "model_sync",
                "model_name": "qwen",
                "source_node": "evil.com",
                "source_port": 11452,
            }
        )
        assert result.get("sandbox_blocked") is True
        assert "模型同步对端" in result["error"]

    @pytest.mark.asyncio
    async def test_model_sync_rejects_unsafe_model_name(self):
        # 硬化 _execute_model_sync: 无沙箱也拒 ../ 与分隔符 (与 master_server 一致)
        agent = NodeAgent()
        result = await agent._execute_model_sync(
            {"model_name": "../etc", "model_id": "x", "source_node": "192.168.1.10"}
        )
        assert "error" in result
        assert "非法 model_name" in result["error"]

    @pytest.mark.asyncio
    async def test_model_sync_rejects_ssrf_host(self):
        agent = NodeAgent()
        result = await agent._execute_model_sync(
            {"model_name": "qwen", "model_id": "x", "source_node": "169.254.169.254"}
        )
        assert "error" in result
        assert "不安全对端主机" in result["error"]

    # ── E5: 插件路径穿越 / 恶意 model_name 段段校验 ──

    @pytest.mark.asyncio
    async def test_plugin_rejects_traversal_plugin(self):
        # E5: plugin 含 ../ 应被拒, 不转发到 fusion-desk
        agent = NodeAgent()
        result = await agent._execute_plugin({"task_id": "e5a", "plugin": "../../../admin", "action": "shutdown"})
        assert "error" in result
        assert "非法 plugin" in result["error"]

    @pytest.mark.asyncio
    async def test_plugin_rejects_traversal_action(self):
        agent = NodeAgent()
        result = await agent._execute_plugin({"task_id": "e5b", "plugin": "ok", "action": "../../etc/passwd"})
        assert "error" in result
        assert "非法 action" in result["error"]

    @pytest.mark.asyncio
    async def test_plugin_rejects_slash_in_segment(self):
        # E5: plugin 含分隔符 / 应被拒 (拼接 URL 会越段)
        agent = NodeAgent()
        result = await agent._execute_plugin({"task_id": "e5c", "plugin": "x/y", "action": "z"})
        assert "error" in result
        assert "非法 plugin" in result["error"]

    @pytest.mark.asyncio
    async def test_inference_rejects_unsafe_model_name(self):
        # E5: inference model 含特殊字符应被拒
        agent = NodeAgent()
        result = await agent._execute_inference(
            {"task_id": "e5d", "model": "model;rm -rf /", "params": {"prompt": "hi"}}
        )
        assert "error" in result
        assert "非法 model" in result["error"]

    @pytest.mark.asyncio
    async def test_sandbox_gate_blocks_plugin_traversal(self):
        # E5: _sandbox_gate 覆盖 plugin 类型, 无沙箱配置也强制段校验
        from fusion_multi_node.security.sandbox import SandboxConfig, WorkerSandbox

        sandbox = WorkerSandbox(SandboxConfig(allowed_paths=["/tmp"]))
        agent = NodeAgent(sandbox=sandbox)
        result = await agent.execute_task(
            {
                "task_id": "e5e",
                "type": "plugin",
                "plugin": "../../admin",
                "action": "run",
                "params": {},
            }
        )
        assert result.get("sandbox_blocked") is True
        assert "插件" in result["error"]

    @pytest.mark.asyncio
    async def test_sandbox_gate_blocks_traversal_without_sandbox(self):
        # AR #24: 默认部署 sandbox=None, E5 段校验仍须强制防穿越 (旧实现被 None 短路绕过)
        agent = NodeAgent()  # 默认无沙箱
        result = await agent.execute_task(
            {
                "task_id": "e5f",
                "type": "plugin",
                "plugin": "../../admin",
                "action": "run",
                "params": {},
            }
        )
        assert result.get("sandbox_blocked") is True
        assert "插件" in result["error"]


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

        # R1: _hardware_report_loop 调 _collect_dynamic_load (纯 psutil),
        # 非 collect_hardware_info。mock 后者 loop 不触发 → 永不置 _running=False → hang。
        original_collect = agent._collect_dynamic_load

        def mock_collect():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                agent._running = False
            return original_collect()

        agent._collect_dynamic_load = mock_collect

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
        with patch(
            "fusion_multi_node.discovery.MDNSDiscovery",
            side_effect=ImportError("no module"),
        ):
            result = await agent._discover_master()
        assert result is False

"""MCP Gateway 测试。"""

import asyncio
import time

import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch

from fusion_multi_node.mcp_gateway.mcp_gateway import (
    MCPClusterGateway,
    MCPRequest,
    MCPTool,
)


class TestMCPTool:
    def test_basic(self):
        tool = MCPTool(
            name="infer",
            description="Run inference",
            parameters={"type": "object", "properties": {"prompt": {"type": "string"}}},
        )
        assert tool.name == "infer"
        assert tool.call_count == 0
        assert tool.timeout == 60.0
        assert tool.required_gpu is False

    def test_with_requirements(self):
        tool = MCPTool(
            name="big_infer",
            description="Heavy inference",
            parameters={},
            required_memory_gb=32.0,
            required_gpu=True,
        )
        assert tool.required_memory_gb == 32.0
        assert tool.required_gpu is True


class TestMCPRequest:
    def test_basic(self):
        req = MCPRequest(
            request_id="r1",
            tool_name="infer",
            arguments={"prompt": "hi"},
            source="claude_code",
        )
        assert req.tool_name == "infer"
        assert req.status == "pending"
        assert req.token_count == 0
        assert req.error == ""


class TestMCPClusterGateway:
    def test_init(self):
        gw = MCPClusterGateway()
        assert gw.token_budget == 10_000_000
        assert gw.total_token_count == 0
        assert len(gw.tools) == 0

    def test_init_custom(self):
        gw = MCPClusterGateway(host="0.0.0.0", port=9756)
        assert gw.port == 9756

    def test_register_tool(self):
        gw = MCPClusterGateway()
        tool = MCPTool(name="t1", description="test tool", parameters={})
        gw.register_tool(tool)
        assert "t1" in gw.tools
        assert gw.tools["t1"].name == "t1"

    def test_unregister_tool(self):
        gw = MCPClusterGateway()
        gw.register_tool(MCPTool(name="t1", description="test", parameters={}))
        gw.unregister_tool("t1")
        assert "t1" not in gw.tools

    def test_get_tools_list(self):
        gw = MCPClusterGateway()
        gw.register_tool(MCPTool(name="t1", description="a", parameters={"type": "object"}))
        gw.register_tool(MCPTool(name="t2", description="b", parameters={"type": "object"}))
        tools = gw.get_tools_list()
        assert len(tools) == 2
        assert tools[0]["name"] == "t1"
        assert "input_schema" in tools[0]

    def test_set_node_selector(self):
        gw = MCPClusterGateway()
        selector = lambda tool: "n1"
        gw.set_node_selector(selector)
        assert gw._node_selector is selector

    @pytest.mark.asyncio
    async def test_handle_tool_call_unknown_tool(self):
        gw = MCPClusterGateway()
        result = await gw.handle_tool_call("missing_tool", {})
        assert "error" in result
        assert "Unknown tool" in result["error"]

    @pytest.mark.asyncio
    async def test_handle_tool_call_budget_exhausted(self):
        gw = MCPClusterGateway()
        gw.token_budget = 10
        gw.total_token_count = 10
        gw.register_tool(MCPTool(name="t1", description="test", parameters={}))
        result = await gw.handle_tool_call("t1", {})
        assert "error" in result
        assert "budget" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_handle_tool_call_success_mock(self):
        gw = MCPClusterGateway()
        gw.register_tool(MCPTool(name="infer", description="test", parameters={}))

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"result": "ok"}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await gw.handle_tool_call("infer", {"prompt": "hi"})
            assert result.get("result") == "ok"
            assert gw.total_token_count > 0

    @pytest.mark.asyncio
    async def test_handle_tool_call_with_node_selector(self):
        gw = MCPClusterGateway()
        gw.register_tool(MCPTool(name="infer", description="test", parameters={}))
        gw.set_node_selector(lambda tool: "remote_node")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"result": "remote_ok"}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await gw.handle_tool_call("infer", {})
            assert result["result"] == "remote_ok"

    @pytest.mark.asyncio
    async def test_forward_to_node_localhost_mock(self):
        gw = MCPClusterGateway()
        tool = MCPTool(name="t1", description="test", parameters={})
        request = MCPRequest(
            request_id="r1", tool_name="t1", arguments={},
            source="claude_code", assigned_node="localhost",
        )

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"local": True}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await gw._forward_to_node(request, tool)
            assert result["local"] is True

    @pytest.mark.asyncio
    async def test_forward_to_node_remote_mock(self):
        gw = MCPClusterGateway()
        tool = MCPTool(name="t1", description="test", parameters={})
        request = MCPRequest(
            request_id="r1", tool_name="t1", arguments={},
            source="claude_code", assigned_node="10.0.0.5",
        )

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"remote": True}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await gw._forward_to_node(request, tool)
            assert result["remote"] is True

    @pytest.mark.asyncio
    async def test_forward_to_node_http_error(self):
        gw = MCPClusterGateway()
        tool = MCPTool(name="t1", description="test", parameters={}, timeout=1.0)
        request = MCPRequest(
            request_id="r1", tool_name="t1", arguments={},
            source="claude_code", assigned_node="localhost",
        )
        try:
            result = await gw._forward_to_node(request, tool)
        except Exception:
            pass

    def test_get_stats(self):
        gw = MCPClusterGateway()
        gw.register_tool(MCPTool(name="t1", description="test", parameters={}))
        gw.total_token_count = 500
        stats = gw.get_stats()
        assert stats["registered_tools"] == 1
        assert stats["total_token_count"] == 500
        assert stats["token_remaining"] == 10_000_000 - 500

    @pytest.mark.asyncio
    async def test_start_stop(self):
        gw = MCPClusterGateway()
        await gw.start()
        assert gw._running is True
        await gw.stop()
        assert gw._running is False

    @pytest.mark.asyncio
    async def test_handle_tool_call_exception(self):
        gw = MCPClusterGateway()
        gw.register_tool(MCPTool(name="t1", description="test", parameters={}, timeout=0.001))

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=Exception("connection failed"))

        with patch.object(httpx, "AsyncClient", return_value=mock_client):
            result = await gw.handle_tool_call("t1", {"prompt": "hi"})
            assert "error" in result

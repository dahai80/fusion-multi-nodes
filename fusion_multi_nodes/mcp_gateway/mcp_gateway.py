"""MCP 集群网关 — Claude 兼容的全局统一 MCP 服务入口。

核心职责：
- 聚合所有节点插件能力，统一供给 Claude Desktop / Claude Code
- 自动路由工具调用到最优节点
- 子代理分布式分流
- Coding Plan 额度统一管理
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class MCPTool:
    """MCP 工具定义。"""
    name: str
    description: str
    parameters: Dict[str, Any]
    node_id: str = ""
    plugin: str = ""
    required_memory_gb: float = 0.0
    required_gpu: bool = False
    timeout: float = 60.0
    call_count: int = 0


@dataclass
class MCPRequest:
    """MCP 请求记录。"""
    request_id: str
    tool_name: str
    arguments: Dict[str, Any]
    source: str  # "claude_desktop" | "claude_code" | "api"
    assigned_node: str = ""
    status: str = "pending"
    created_at: float = 0.0
    completed_at: float = 0.0
    token_count: int = 0
    error: str = ""


class MCPClusterGateway:
    """MCP 集群网关 — 全局统一 MCP 服务入口。

    对外暴露标准 MCP 协议接口，对内聚合所有节点插件能力。
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 9756):
        self.host = host
        self.port = port
        self.tools: Dict[str, MCPTool] = {}
        self.requests: Dict[str, MCPRequest] = {}
        self._node_selector: Optional[Callable] = None
        self._running = False
        self.total_token_count: int = 0
        self.token_budget: int = 10_000_000  # 默认额度

    def register_tool(self, tool: MCPTool) -> None:
        """注册工具到集群 MCP 网关。"""
        self.tools[tool.name] = tool
        logger.info(f"MCP 工具注册: {tool.name} ({tool.plugin})")

    def unregister_tool(self, name: str) -> None:
        """注销工具。"""
        self.tools.pop(name, None)
        logger.info(f"MCP 工具注销: {name}")

    def get_tools_list(self) -> List[Dict[str, Any]]:
        """获取所有可用工具列表（MCP 协议格式）。"""
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.parameters,
            }
            for t in self.tools.values()
        ]

    def set_node_selector(self, selector: Callable) -> None:
        """设置节点选择器（由 Cluster Master 提供）。"""
        self._node_selector = selector

    async def handle_tool_call(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
        source: str = "claude_code",
    ) -> Dict[str, Any]:
        """处理工具调用请求。

        1. 查找工具定义
        2. 选择最优节点
        3. 转发到节点执行
        4. 返回结果
        """
        request_id = f"mcp_{uuid.uuid4().hex[:12]}"
        request = MCPRequest(
            request_id=request_id,
            tool_name=tool_name,
            arguments=arguments,
            source=source,
            created_at=time.time(),
        )
        self.requests[request_id] = request

        # 检查额度
        if self.total_token_count >= self.token_budget:
            request.status = "failed"
            request.error = "Token budget exhausted"
            return {"error": "Token budget exhausted"}

        # 查找工具
        tool = self.tools.get(tool_name)
        if not tool:
            request.status = "failed"
            request.error = f"Unknown tool: {tool_name}"
            return {"error": f"Unknown tool: {tool_name}"}

        # 选择节点
        if self._node_selector:
            node = self._node_selector(tool)
            request.assigned_node = node.node_id if hasattr(node, 'node_id') else str(node)
        else:
            request.assigned_node = "localhost"

        logger.info(f"MCP 调用: {tool_name} → {request.assigned_node}")

        # 执行（通过 Node Agent 或直接调用）
        try:
            result = await self._forward_to_node(request, tool)
            request.status = "completed"
            request.completed_at = time.time()
            tool.call_count += 1
            # 估算 token 消耗
            estimated_tokens = len(str(arguments)) // 4
            request.token_count = estimated_tokens
            self.total_token_count += estimated_tokens
            return result
        except Exception as e:
            request.status = "failed"
            request.error = str(e)
            return {"error": str(e)}

    async def _forward_to_node(self, request: MCPRequest, tool: MCPTool) -> Dict[str, Any]:
        """转发工具调用到目标节点执行。"""
        import httpx
        node_id = request.assigned_node

        payload = {
            "tool": tool.name,
            "plugin": tool.plugin,
            "arguments": request.arguments,
            "request_id": request.request_id,
        }

        # 如果节点是本机，直接通过 fusion-desk 本地 API
        if node_id == "localhost":
            async with httpx.AsyncClient(timeout=tool.timeout) as client:
                resp = await client.post(
                    f"http://localhost:9000/api/mcp/tools/{tool.name}",
                    json=payload,
                )
                return resp.json()
        else:
            # 转发到远程节点 agent
            node_port = 9755  # Node Agent 默认端口
            async with httpx.AsyncClient(timeout=tool.timeout) as client:
                resp = await client.post(
                    f"http://{node_id}:{node_port}/api/mcp/execute",
                    json=payload,
                )
                return resp.json()

    def get_stats(self) -> Dict[str, Any]:
        """获取 MCP 网关统计。"""
        return {
            "registered_tools": len(self.tools),
            "total_requests": len(self.requests),
            "completed": sum(1 for r in self.requests.values() if r.status == "completed"),
            "failed": sum(1 for r in self.requests.values() if r.status == "failed"),
            "total_token_count": self.total_token_count,
            "token_budget": self.token_budget,
            "token_remaining": self.token_budget - self.total_token_count,
        }

    async def start(self) -> None:
        """启动 MCP 网关。"""
        self._running = True
        logger.info(f"MCP 集群网关启动: {self.host}:{self.port}")

    async def stop(self) -> None:
        """停止 MCP 网关。"""
        self._running = False
        logger.info("MCP 集群网关已停止")
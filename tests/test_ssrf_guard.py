"""P0-5 SSRF 校验统一 — register/cancel/KV 出站路径 SSRF 守卫测试 (AR #24 H1/H2/H3)。

验:
- register_node 拒云元数据/链路本地主机 (H1), 放行 loopback + 私网。
- KV lookup_remote/transfer/warm 跳过云元数据 host, 不发请求 (H3)。
- cancel 通知跳过非安全对端 (H2, 经 master_server ASGI 路由验)。
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, NodeStatus, ParallelMode, TaskStatus
from fusion_multi_node.server.master_server import MasterServer
from fusion_multi_node.utils.auth import is_registerable_host, is_safe_outbound_host

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


def _node(node_id="n1", ip="10.0.0.1", port=11458) -> NodeInfo:
    return NodeInfo(
        node_id=node_id,
        hostname=f"mac-{node_id}",
        ip_address=ip,
        port=port,
        total_memory_gb=64.0,
        available_memory_gb=48.0,
        cpu_cores=12,
        gpu_cores=30,
    )


class TestRegisterNodeSSRF:
    """H1: register_node 注册期校验 ip。"""

    @pytest.mark.asyncio
    async def test_register_rejects_metadata_host(self):
        master = ClusterMaster()
        ok = await master.register_node(_node("bad1", ip="169.254.169.254"))
        assert ok is False, "云元数据 IP 不应注册成功"
        assert "bad1" not in master.nodes

    @pytest.mark.asyncio
    async def test_register_rejects_link_local(self):
        master = ClusterMaster()
        ok = await master.register_node(_node("bad2", ip="169.254.1.1"))
        assert ok is False, "链路本地 IP 不应注册成功"

    @pytest.mark.asyncio
    async def test_register_rejects_creds_in_host(self):
        master = ClusterMaster()
        ok = await master.register_node(_node("bad3", ip="evil@host/path"))
        assert ok is False, "携带凭据/路径的 host 不应注册成功"

    @pytest.mark.asyncio
    async def test_register_allows_loopback(self):
        master = ClusterMaster()
        ok = await master.register_node(_node("local", ip="127.0.0.1"))
        assert ok is True, "loopback 单机部署应允许注册"

    @pytest.mark.asyncio
    async def test_register_allows_private(self):
        master = ClusterMaster()
        ok = await master.register_node(_node("priv", ip="10.0.0.1"))
        assert ok is True
        assert master.nodes["priv"].status == NodeStatus.ONLINE


class TestIsRegisterableHost:
    @pytest.mark.parametrize(
        "host,expected",
        [
            ("127.0.0.1", True),
            ("10.0.0.1", True),
            ("192.168.1.5", True),
            ("localhost", True),
            ("169.254.169.254", False),
            ("169.254.1.1", False),
            ("0.0.0.0", False),
            ("224.0.0.1", False),
            ("evil@host", False),
            ("host/path", False),
            ("", False),
        ],
    )
    def test_is_registerable_host(self, host, expected):
        assert is_registerable_host(host) is expected


class TestIsSafeOutboundHost:
    @pytest.mark.parametrize(
        "host,expected",
        [
            ("127.0.0.1", True),
            ("10.0.0.1", True),
            ("node_1", True),  # 不可解析 node_id 仍合法 (不强制 DNS)
            ("localhost", True),
            ("169.254.169.254", False),
            ("metadata.google.internal", False),
            ("169.254.1.1", False),
            ("0.0.0.0", False),
            ("evil@host", False),
            ("host/path", False),
            ("", False),
        ],
    )
    def test_is_safe_outbound_host(self, host, expected):
        assert is_safe_outbound_host(host) is expected


class TestKVOutboundSSRF:
    """H3: KV 跨节点路径出站 SSRF 守卫 — 跳过恶意 host, 不发请求。"""

    @pytest.mark.asyncio
    async def test_lookup_remote_skips_metadata_host(self):
        m = KVSharingManager(enable_compression=False)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock()
        with patch("httpx.AsyncClient", MagicMock(return_value=mock_client)):
            result = await m.lookup_remote("test-model", "h1", ["169.254.169.254"])
        assert result is None, "云元数据 host 应被跳过"
        mock_client.post.assert_not_called(), "恶意 host 不应发请求"

    @pytest.mark.asyncio
    async def test_transfer_from_remote_skips_metadata_host(self):
        m = KVSharingManager(enable_compression=False)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock()
        with patch("httpx.AsyncClient", MagicMock(return_value=mock_client)):
            ok = await m.transfer_from_remote("c1", "169.254.169.254", "node_2")
        assert ok is False
        mock_client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_warm_cache_skips_metadata_host(self):
        m = KVSharingManager(enable_compression=False)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock()
        with patch("httpx.AsyncClient", MagicMock(return_value=mock_client)):
            results = await m.warm_cache("test-model", ["p1"], ["169.254.169.254"])
        assert results["failed"] == 1
        assert results["success"] == 0
        mock_client.post.assert_not_called()


class TestCancelRouteSSRF:
    """H2: master_server cancel 路由出站通知跳过非安全对端。"""

    @pytest.mark.asyncio
    async def test_cancel_skips_unsafe_node(self):
        master = ClusterMaster()
        # 直接注入 metadata-IP 节点 (绕过 register — 此处只验路由出站守卫, 不验 H1)
        bad_node = NodeInfo(
            node_id="bad",
            hostname="mac-bad",
            ip_address="169.254.169.254",
            port=11458,
            total_memory_gb=64.0,
            available_memory_gb=48.0,
            cpu_cores=12,
            gpu_cores=30,
        )
        master.nodes["bad"] = bad_node
        task = ClusterTask(
            task_id="t1",
            name="t1",
            mode=ParallelMode.DATA,
            model_name="m",
            assigned_nodes=["bad"],
            status=TaskStatus.RUNNING,
            created_at=0.0,
        )
        master.tasks["t1"] = task

        server = MasterServer(master=master, shared_token=TEST_TOKEN)
        server._approval_manager = None
        transport = ASGITransport(app=server.app)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock()
        with patch("httpx.AsyncClient", MagicMock(return_value=mock_client)):
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                resp = await c.post(
                    "/api/tasks/t1/cancel",
                    json={"reason": "test"},
                    headers=AUTH_HEADERS,
                )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["notified_nodes"] == [], "非安全对端不应被通知"
        mock_client.post.assert_not_called(), "取消通知不应向 metadata IP 发请求"

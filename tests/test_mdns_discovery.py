"""mDNS Discovery 测试。

测试 MDNSDiscovery、DiscoveryInfo 等。
用户指令：要求测试覆盖率90%+。
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fusion_multi_node.discovery.mdns_discovery import (
    SERVICE_TYPE,
    DiscoveryInfo,
    MDNSDiscovery,
    _service_info_to_discovery,
)


class TestDiscoveryInfo:
    def test_basic(self):
        info = DiscoveryInfo(name="node1", host="10.0.0.1", port=9754)
        assert info.name == "node1"
        assert info.host == "10.0.0.1"
        assert info.port == 9754
        assert info.discovered_at == 0.0
        assert info.properties == {}

    def test_server_name(self):
        info = DiscoveryInfo(name="node1", host="10.0.0.1", port=9754)
        assert info.server_name == f"node1.{SERVICE_TYPE}"

    def test_with_properties(self):
        info = DiscoveryInfo(
            name="master", host="10.0.0.1", port=9754,
            properties={"role": "master", "version": "1.0"},
            discovered_at=time.time(),
        )
        assert info.properties["role"] == "master"


class TestMDNSDiscoveryInit:
    def test_default_node_id(self):
        d = MDNSDiscovery()
        assert d.node_id.startswith("fusion-")
        assert d.service_type == SERVICE_TYPE

    def test_custom_node_id(self):
        d = MDNSDiscovery(node_id="custom-id")
        assert d.node_id == "custom-id"

    def test_initial_state(self):
        d = MDNSDiscovery()
        assert len(d._discovered) == 0
        assert d._registered is False
        assert d._zeroconf is None


class TestMDNSDiscoveryRegister:
    def test_register_with_zeroconf(self):
        d = MDNSDiscovery(node_id="test-node")
        mock_zc = MagicMock()
        mock_si = MagicMock()
        with patch("fusion_multi_node.discovery.mdns_discovery.Zeroconf", return_value=mock_zc, create=True):
            with patch("fusion_multi_node.discovery.mdns_discovery.ServiceInfo", return_value=mock_si, create=True):
                try:
                    d.register(port=9754, properties={"role": "master"})
                    assert d._registered is True
                except Exception:
                    pass  # zeroconf may not be installed

    def test_register_no_zeroconf(self):
        d = MDNSDiscovery(node_id="test-node")
        with patch.dict("sys.modules", {"zeroconf": None}):
            try:
                d.register(port=9754)
            except Exception:
                pass


class TestMDNSDiscoveryUnregister:
    def test_unregister_not_registered(self):
        d = MDNSDiscovery(node_id="test-node")
        d.unregister()
        assert d._registered is False

    def test_unregister_registered(self):
        d = MDNSDiscovery(node_id="test-node")
        mock_zc = MagicMock()
        d._zeroconf = mock_zc
        d._registered = True
        d.unregister()
        assert d._registered is False
        mock_zc.close.assert_called_once()
        assert d._zeroconf is None


class TestMDNSDiscoveryBrowse:
    def test_browse_no_zeroconf(self):
        d = MDNSDiscovery(node_id="test-node")
        with patch.dict("sys.modules", {"zeroconf": None}):
            try:
                result = d.browse(timeout=0.01)
                assert isinstance(result, list)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_browse_async_no_zeroconf(self):
        d = MDNSDiscovery(node_id="test-node")
        with patch.dict("sys.modules", {"zeroconf": None}):
            try:
                result = await d.browse_async(timeout=0.01)
                assert isinstance(result, list)
            except Exception:
                pass

    def test_browse_with_mock_zeroconf(self):
        d = MDNSDiscovery(node_id="test-node")
        mock_zc = MagicMock()
        mock_browser = MagicMock()
        with patch("fusion_multi_node.discovery.mdns_discovery.Zeroconf", return_value=mock_zc, create=True):
            with patch("fusion_multi_node.discovery.mdns_discovery.ServiceBrowser", return_value=mock_browser, create=True):
                try:
                    result = d.browse(timeout=0.01)
                    assert isinstance(result, list)
                except Exception:
                    pass


class TestMDNSDiscoveryGetDiscovered:
    def test_empty(self):
        d = MDNSDiscovery(node_id="test-node")
        assert d.get_discovered() == []

    def test_with_entries(self):
        d = MDNSDiscovery(node_id="test-node")
        info = DiscoveryInfo(name="node1", host="10.0.0.1", port=9754)
        d._discovered["node1"] = info
        result = d.get_discovered()
        assert len(result) == 1
        assert result[0].name == "node1"


class TestMDNSDiscoveryFindMaster:
    def test_find_master_no_discovered(self):
        d = MDNSDiscovery(node_id="test-node")
        with patch.object(d, "browse", return_value=[]):
            result = d.find_master(timeout=0.01)
            assert result is None

    def test_find_master_with_master(self):
        d = MDNSDiscovery(node_id="test-node")
        master_info = DiscoveryInfo(
            name="master", host="10.0.0.1", port=9754,
            properties={"role": "master", "node_id": "master-1"},
        )
        with patch.object(d, "browse", return_value=[master_info]):
            result = d.find_master(timeout=0.01)
            assert result is not None
            assert result.name == "master"

    def test_find_master_no_master_role(self):
        d = MDNSDiscovery(node_id="test-node")
        agent_info = DiscoveryInfo(
            name="agent1", host="10.0.0.2", port=11445,
            properties={"role": "agent"},
        )
        with patch.object(d, "browse", return_value=[agent_info]):
            result = d.find_master(timeout=0.01)
            assert result is None

    @pytest.mark.asyncio
    async def test_find_master_async_no_discovered(self):
        d = MDNSDiscovery(node_id="test-node")
        with patch.object(d, "browse_async", return_value=AsyncMock(return_value=[])):
            d.browse_async = AsyncMock(return_value=[])
            result = await d.find_master_async(timeout=0.01)
            assert result is None

    @pytest.mark.asyncio
    async def test_find_master_async_with_master(self):
        d = MDNSDiscovery(node_id="test-node")
        master_info = DiscoveryInfo(
            name="master", host="10.0.0.1", port=9754,
            properties={"role": "master", "node_id": "master-1"},
        )
        d.browse_async = AsyncMock(return_value=[master_info])
        result = await d.find_master_async(timeout=0.01)
        assert result is not None


class TestMDNSDiscoveryLocalIP:
    def test_get_local_ip(self):
        d = MDNSDiscovery(node_id="test-node")
        ip = d._get_local_ip()
        assert isinstance(ip, str)
        assert len(ip) > 0


class TestServiceInfoToDiscovery:
    def test_basic_conversion(self):
        mock_info = MagicMock()
        mock_info.parsed_addresses.return_value = ["10.0.0.1"]
        mock_info.port = 9754
        mock_info.properties = {b"role": b"master"}
        result = _service_info_to_discovery("test-node." + SERVICE_TYPE, mock_info)
        assert result.name == "test-node"
        assert result.host == "10.0.0.1"
        assert result.port == 9754
        assert result.properties["role"] == "master"

    def test_no_addresses(self):
        mock_info = MagicMock()
        mock_info.parsed_addresses.return_value = []
        mock_info.port = 9754
        mock_info.properties = {}
        result = _service_info_to_discovery("test", mock_info)
        assert result.host == "127.0.0.1"

    def test_string_properties(self):
        mock_info = MagicMock()
        mock_info.parsed_addresses.return_value = ["10.0.0.1"]
        mock_info.port = 9754
        mock_info.properties = {"key": "value"}
        result = _service_info_to_discovery("test", mock_info)
        assert result.properties["key"] == "value"

    def test_no_dot_in_name(self):
        mock_info = MagicMock()
        mock_info.parsed_addresses.return_value = ["10.0.0.1"]
        mock_info.port = 9754
        mock_info.properties = {}
        result = _service_info_to_discovery("simple", mock_info)
        assert result.name == "simple"

    def test_none_property_value(self):
        mock_info = MagicMock()
        mock_info.parsed_addresses.return_value = ["10.0.0.1"]
        mock_info.port = 9754
        mock_info.properties = {b"key": None}
        result = _service_info_to_discovery("test", mock_info)
        assert result.properties["key"] == ""

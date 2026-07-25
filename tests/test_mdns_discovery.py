"""mDNS 发现模块测试。"""

import pytest
from fusion_multi_node.discovery import MDNSDiscovery, DiscoveryInfo, SERVICE_TYPE


class TestDiscoveryInfo:
    def test_basic_fields(self):
        di = DiscoveryInfo(name="node1", host="10.0.1.5", port=9753)
        assert di.name == "node1"
        assert di.host == "10.0.1.5"
        assert di.port == 9753
        assert di.properties == {}
        assert di.discovered_at == 0.0

    def test_server_name(self):
        di = DiscoveryInfo(name="fusion-master", host="10.0.1.1", port=9753)
        assert di.server_name == f"fusion-master.{SERVICE_TYPE}"

    def test_with_properties(self):
        di = DiscoveryInfo(
            name="n1", host="10.0.1.2", port=9755,
            properties={"role": "worker", "arch": "arm64"},
        )
        assert di.properties["role"] == "worker"


class TestMDNSDiscovery:
    def test_init_defaults(self):
        mdns = MDNSDiscovery()
        assert mdns.service_type == SERVICE_TYPE
        assert mdns._registered is False

    def test_init_custom_node_id(self):
        mdns = MDNSDiscovery(node_id="my-node")
        assert mdns.node_id == "my-node"

    def test_browse_without_zeroconf(self):
        mdns = MDNSDiscovery()
        results = mdns.browse(timeout=0.1)
        assert results == []

    def test_register_unregister_without_zeroconf(self):
        mdns = MDNSDiscovery(node_id="test-master")
        ok = mdns.register(port=9753)
        # zeroconf 可能未安装，但不应崩溃
        if ok:
            assert mdns._registered
            mdns.unregister()
            assert not mdns._registered

    def test_get_local_ip(self):
        mdns = MDNSDiscovery()
        ip = mdns._get_local_ip()
        assert isinstance(ip, str)
        assert len(ip) > 0

    def test_find_master_no_nodes(self):
        mdns = MDNSDiscovery()
        result = mdns.find_master(timeout=0.1)
        assert result is None

    def test_get_discovered_empty(self):
        mdns = MDNSDiscovery()
        assert mdns.get_discovered() == []

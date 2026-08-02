from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fusion_multi_node.utils.network_topology import (
    LinkInfo,
    LinkType,
    NetworkPath,
    NetworkTopologyDetector,
)


class TestLinkType:
    def test_all_values(self):
        expected = [
            "thunderbolt_5",
            "thunderbolt_4",
            "thunderbolt_3",
            "ethernet_10g",
            "ethernet_1g",
            "ethernet_100m",
            "wifi_6e",
            "wifi_6",
            "unknown",
        ]
        for val, exp in zip(LinkType, expected):
            assert val.value == exp


class TestClassifyThunderbolt:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_thunderbolt_5(self):
        assert self.detector._classify_thunderbolt(40000) == LinkType.THUNDERBOLT_5
        assert self.detector._classify_thunderbolt(50000) == LinkType.THUNDERBOLT_5

    def test_thunderbolt_4(self):
        assert self.detector._classify_thunderbolt(20000) == LinkType.THUNDERBOLT_4
        assert self.detector._classify_thunderbolt(39999) == LinkType.THUNDERBOLT_4

    def test_thunderbolt_3_high(self):
        assert self.detector._classify_thunderbolt(10000) == LinkType.THUNDERBOLT_3
        assert self.detector._classify_thunderbolt(19999) == LinkType.THUNDERBOLT_3

    def test_thunderbolt_3_low(self):
        assert self.detector._classify_thunderbolt(5000) == LinkType.THUNDERBOLT_3
        assert self.detector._classify_thunderbolt(0) == LinkType.THUNDERBOLT_3


class TestClassifyEthernet:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_10g(self):
        assert self.detector._classify_ethernet(10000) == LinkType.ETHERNET_10G
        assert self.detector._classify_ethernet(25000) == LinkType.ETHERNET_10G

    def test_1g(self):
        assert self.detector._classify_ethernet(1000) == LinkType.ETHERNET_1G
        assert self.detector._classify_ethernet(9999) == LinkType.ETHERNET_1G

    def test_100m(self):
        assert self.detector._classify_ethernet(100) == LinkType.ETHERNET_100M
        assert self.detector._classify_ethernet(999) == LinkType.ETHERNET_100M
        assert self.detector._classify_ethernet(0) == LinkType.ETHERNET_100M


class TestClassifyWifi:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_wifi_6e(self):
        assert self.detector._classify_wifi(2400) == LinkType.WIFI_6E
        assert self.detector._classify_wifi(3000) == LinkType.WIFI_6E

    def test_wifi_6(self):
        assert self.detector._classify_wifi(1200) == LinkType.WIFI_6
        assert self.detector._classify_wifi(2399) == LinkType.WIFI_6

    def test_unknown(self):
        assert self.detector._classify_wifi(100) == LinkType.UNKNOWN
        assert self.detector._classify_wifi(0) == LinkType.UNKNOWN
        assert self.detector._classify_wifi(1199) == LinkType.UNKNOWN


class TestGetPriority:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_all_priorities(self):
        assert self.detector._get_priority(LinkType.THUNDERBOLT_5) == 0
        assert self.detector._get_priority(LinkType.THUNDERBOLT_4) == 1
        assert self.detector._get_priority(LinkType.THUNDERBOLT_3) == 2
        assert self.detector._get_priority(LinkType.ETHERNET_10G) == 3
        assert self.detector._get_priority(LinkType.ETHERNET_1G) == 4
        assert self.detector._get_priority(LinkType.ETHERNET_100M) == 5
        assert self.detector._get_priority(LinkType.WIFI_6E) == 6
        assert self.detector._get_priority(LinkType.WIFI_6) == 7
        assert self.detector._get_priority(LinkType.UNKNOWN) == 10


class TestDetectLoopback:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_loopback_added(self):
        self.detector._detect_loopback()
        assert "lo0" in self.detector._interfaces
        lo = self.detector._interfaces["lo0"]
        assert lo.type == LinkType.THUNDERBOLT_5
        assert lo.bandwidth_mbps == 40000
        assert lo.latency_ms == 0.01
        assert lo.is_rdma is False
        assert lo.is_active is True
        assert lo.priority == 0


class TestGetBestLink:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_no_interfaces(self):
        assert self.detector.get_best_link() is None

    def test_single_active(self):
        link = LinkInfo(
            type=LinkType.ETHERNET_1G,
            bandwidth_mbps=1000,
            latency_ms=0.1,
            interface="en0",
            is_active=True,
            priority=4,
        )
        self.detector._interfaces["en0"] = link
        best = self.detector.get_best_link()
        assert best is not None
        assert best.interface == "en0"

    def test_multiple_picks_highest_priority(self):
        self.detector._interfaces["en0"] = LinkInfo(
            type=LinkType.ETHERNET_1G,
            bandwidth_mbps=1000,
            latency_ms=0.1,
            interface="en0",
            is_active=True,
            priority=4,
        )
        self.detector._interfaces["bridge100"] = LinkInfo(
            type=LinkType.THUNDERBOLT_5,
            bandwidth_mbps=40000,
            latency_ms=0.05,
            interface="bridge100",
            is_active=True,
            priority=0,
        )
        best = self.detector.get_best_link()
        assert best.interface == "bridge100"

    def test_inactive_excluded(self):
        self.detector._interfaces["en0"] = LinkInfo(
            type=LinkType.ETHERNET_1G,
            bandwidth_mbps=1000,
            latency_ms=0.1,
            interface="en0",
            is_active=False,
            priority=4,
        )
        assert self.detector.get_best_link() is None


class TestPrimaryInterfaceHelpers:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_get_primary_interface_with_link(self):
        self.detector._interfaces["en0"] = LinkInfo(
            type=LinkType.ETHERNET_1G,
            bandwidth_mbps=1000,
            latency_ms=0.1,
            interface="en0",
            is_active=True,
            priority=4,
        )
        assert self.detector.get_primary_interface() == "en0"

    def test_get_primary_interface_no_link(self):
        assert self.detector.get_primary_interface() == "lo0"

    def test_get_link_type_with_link(self):
        self.detector._interfaces["bridge100"] = LinkInfo(
            type=LinkType.THUNDERBOLT_4,
            bandwidth_mbps=20000,
            latency_ms=0.05,
            interface="bridge100",
            is_active=True,
            priority=1,
        )
        assert self.detector.get_link_type() == LinkType.THUNDERBOLT_4

    def test_get_link_type_no_link(self):
        assert self.detector.get_link_type() == LinkType.UNKNOWN

    def test_get_link_speed_with_link(self):
        self.detector._interfaces["en0"] = LinkInfo(
            type=LinkType.ETHERNET_1G,
            bandwidth_mbps=1000,
            latency_ms=0.1,
            interface="en0",
            is_active=True,
            priority=4,
        )
        assert self.detector.get_link_speed() == 1000.0

    def test_get_link_speed_no_link(self):
        assert self.detector.get_link_speed() == 1000.0


class TestIsThunderboltAvailable:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_with_thunderbolt(self):
        self.detector._interfaces["bridge100"] = LinkInfo(
            type=LinkType.THUNDERBOLT_5,
            bandwidth_mbps=40000,
            latency_ms=0.05,
            interface="bridge100",
            is_rdma=True,
            is_active=True,
            priority=0,
        )
        assert self.detector.is_thunderbolt_available() is True

    def test_with_thunderbolt_3(self):
        self.detector._interfaces["bridge100"] = LinkInfo(
            type=LinkType.THUNDERBOLT_3,
            bandwidth_mbps=10000,
            latency_ms=0.1,
            interface="bridge100",
            is_rdma=True,
            is_active=True,
            priority=2,
        )
        assert self.detector.is_thunderbolt_available() is True

    def test_without_thunderbolt(self):
        self.detector._interfaces["en0"] = LinkInfo(
            type=LinkType.ETHERNET_1G,
            bandwidth_mbps=1000,
            latency_ms=0.1,
            interface="en0",
            is_active=True,
            priority=4,
        )
        assert self.detector.is_thunderbolt_available() is False

    def test_empty(self):
        assert self.detector.is_thunderbolt_available() is False


class TestRecommendedCompression:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def _set_link(self, link_type):
        self.detector._interfaces["iface0"] = LinkInfo(
            type=link_type,
            bandwidth_mbps=1000,
            latency_ms=0.1,
            interface="iface0",
            is_active=True,
            priority=self.detector._get_priority(link_type),
        )

    def test_thunderbolt_5_none(self):
        self._set_link(LinkType.THUNDERBOLT_5)
        assert self.detector.get_recommended_compression() == "none"

    def test_thunderbolt_4_none(self):
        self._set_link(LinkType.THUNDERBOLT_4)
        assert self.detector.get_recommended_compression() == "none"

    def test_thunderbolt_3_light(self):
        self._set_link(LinkType.THUNDERBOLT_3)
        assert self.detector.get_recommended_compression() == "light"

    def test_ethernet_10g_light(self):
        self._set_link(LinkType.ETHERNET_10G)
        assert self.detector.get_recommended_compression() == "light"

    def test_ethernet_1g_normal(self):
        self._set_link(LinkType.ETHERNET_1G)
        assert self.detector.get_recommended_compression() == "normal"

    def test_ethernet_100m_aggressive(self):
        self._set_link(LinkType.ETHERNET_100M)
        assert self.detector.get_recommended_compression() == "aggressive"

    def test_wifi_6e_aggressive(self):
        self._set_link(LinkType.WIFI_6E)
        assert self.detector.get_recommended_compression() == "aggressive"

    def test_wifi_6_aggressive(self):
        self._set_link(LinkType.WIFI_6)
        assert self.detector.get_recommended_compression() == "aggressive"

    def test_unknown_aggressive(self):
        self._set_link(LinkType.UNKNOWN)
        assert self.detector.get_recommended_compression() == "aggressive"


class TestMeasureInterfaceSpeed:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    async def test_baseT_speed(self):
        mock_result = MagicMock()
        mock_result.stdout = "en0: flags=8863<UP,BROADCAST,RUNNING>\n\tmedia: autoselect (1000baseT <full-duplex>)\n"
        with patch("subprocess.run", return_value=mock_result):
            speed = await self.detector._measure_interface_speed("en0")
            assert speed == 1000.0

    async def test_10g_baseT(self):
        mock_result = MagicMock()
        mock_result.stdout = "en0: flags=8863\n\tmedia: autoselect (10000baseT <full-duplex>)\n"
        with patch("subprocess.run", return_value=mock_result):
            speed = await self.detector._measure_interface_speed("en0")
            assert speed == 10000.0

    async def test_thunderbolt_media(self):
        mock_result = MagicMock()
        mock_result.stdout = "bridge100: flags=\n\tmedia: autoselect (thunderbolt)\n"
        with patch("subprocess.run", return_value=mock_result):
            speed = await self.detector._measure_interface_speed("bridge100")
            assert speed == 40000.0

    async def test_40_in_media(self):
        mock_result = MagicMock()
        mock_result.stdout = "bridge100: flags=\n\tmedia: 40Gbps\n"
        with patch("subprocess.run", return_value=mock_result):
            speed = await self.detector._measure_interface_speed("bridge100")
            assert speed == 40000.0

    async def test_no_media_line(self):
        mock_result = MagicMock()
        mock_result.stdout = "en0: flags=8863<UP>\n\tstatus: active\n"
        with patch("subprocess.run", return_value=mock_result):
            speed = await self.detector._measure_interface_speed("en0")
            assert speed == 1000.0

    async def test_subprocess_exception(self):
        with patch("subprocess.run", side_effect=Exception("fail")):
            speed = await self.detector._measure_interface_speed("en0")
            assert speed == 1000.0


class TestMeasureLatency:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    async def test_loopback(self):
        latency = await self.detector._measure_latency("lo0")
        assert latency == 0.01

    async def test_normal(self):
        latency = await self.detector._measure_latency("en0")
        assert latency == 0.1


class TestMeasurePeerLatency:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    async def test_successful_connection(self):
        mock_reader = AsyncMock()
        mock_writer = AsyncMock()
        with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
            with patch("asyncio.wait_for", return_value=(mock_reader, mock_writer)):
                latency = await self.detector.measure_peer_latency("192.168.1.100", count=3)
                assert latency >= 0

    async def test_all_fail_returns_default(self):
        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            latency = await self.detector.measure_peer_latency("192.168.1.100", count=2)
            assert latency == 10.0

    async def test_partial_fail(self):
        call_count = 0

        async def mock_wait_for(coro, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TimeoutError()
            mock_reader = AsyncMock()
            mock_writer = AsyncMock()
            mock_writer.close = MagicMock()
            mock_writer.wait_closed = AsyncMock()
            return (mock_reader, mock_writer)

        with patch("asyncio.wait_for", side_effect=mock_wait_for):
            latency = await self.detector.measure_peer_latency("192.168.1.100", count=2)
            assert latency > 0
            assert latency < 10.0


class TestDetectThunderbolt:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    async def test_no_thunderbolt(self):
        mock_result = MagicMock()
        mock_result.stdout = "No devices found"
        with patch("subprocess.run", return_value=mock_result):
            await self.detector._detect_thunderbolt()
            assert not any("bridge" in k for k in self.detector._interfaces)

    async def test_thunderbolt_with_bridge(self):
        sp_result = MagicMock()
        sp_result.stdout = "Thunderbolt 4 Bus"

        ifconfig_result = MagicMock()
        ifconfig_result.stdout = "lo0 bridge100 en0"

        ifconfig_detail = MagicMock()
        ifconfig_detail.stdout = "bridge100: flags=\n\tmedia: autoselect (thunderbolt)\n"

        def mock_run(cmd, **kwargs):
            if "system_profiler" in cmd[0] and "Thunderbolt" in cmd[1]:
                return sp_result
            if cmd == ["ifconfig", "-l"]:
                return ifconfig_result
            if cmd[0] == "ifconfig" and cmd[1] != "-l":
                return ifconfig_detail
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_thunderbolt()
            assert "bridge100" in self.detector._interfaces
            link = self.detector._interfaces["bridge100"]
            assert link.is_rdma is True
            assert link.is_active is True

    async def test_thunderbolt_with_fw_interface(self):
        sp_result = MagicMock()
        sp_result.stdout = "Thunderbolt 3 Bus"

        ifconfig_result = MagicMock()
        ifconfig_result.stdout = "lo0 fw0 en0"

        ifconfig_detail = MagicMock()
        ifconfig_detail.stdout = "fw0: flags=\n\tmedia: autoselect (1000baseT <full-duplex>)\n"

        def mock_run(cmd, **kwargs):
            if "system_profiler" in cmd[0]:
                return sp_result
            if cmd == ["ifconfig", "-l"]:
                return ifconfig_result
            if cmd[0] == "ifconfig" and cmd[1] != "-l":
                return ifconfig_detail
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_thunderbolt()
            assert "fw0" in self.detector._interfaces

    async def test_thunderbolt_subprocess_exception(self):
        with patch("subprocess.run", side_effect=Exception("no system_profiler")):
            await self.detector._detect_thunderbolt()
            assert len(self.detector._interfaces) == 0


class TestDetectEthernet:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    async def test_ethernet_interface(self):
        ifconfig_list = MagicMock()
        ifconfig_list.stdout = "lo0 en0 en1"

        ifconfig_detail = MagicMock()
        ifconfig_detail.stdout = "en0: flags=\n\tmedia: autoselect (1000baseT <full-duplex>)\n"

        sp_network = MagicMock()
        sp_network.stdout = "Ethernet Adapters:\nUSB 10/100/1000 LAN"

        def mock_run(cmd, **kwargs):
            if cmd == ["ifconfig", "-l"]:
                return ifconfig_list
            if cmd[0] == "ifconfig" and cmd[1] != "-l":
                return ifconfig_detail
            if "system_profiler" in cmd[0]:
                return sp_network
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_ethernet()
            assert "en0" in self.detector._interfaces
            link = self.detector._interfaces["en0"]
            assert link.type == LinkType.ETHERNET_1G
            assert link.is_rdma is False

    async def test_10g_ethernet(self):
        ifconfig_list = MagicMock()
        ifconfig_list.stdout = "lo0 en0"

        ifconfig_detail = MagicMock()
        ifconfig_detail.stdout = "en0: flags=\n\tmedia: autoselect (10000baseT <full-duplex>)\n"

        sp_network = MagicMock()
        sp_network.stdout = "Ethernet"

        def mock_run(cmd, **kwargs):
            if cmd == ["ifconfig", "-l"]:
                return ifconfig_list
            if cmd[0] == "ifconfig" and cmd[1] != "-l":
                return ifconfig_detail
            if "system_profiler" in cmd[0]:
                return sp_network
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_ethernet()
            assert "en0" in self.detector._interfaces
            assert self.detector._interfaces["en0"].type == LinkType.ETHERNET_10G

    async def test_skip_already_registered(self):
        self.detector._interfaces["en0"] = LinkInfo(
            type=LinkType.THUNDERBOLT_5,
            bandwidth_mbps=40000,
            latency_ms=0.01,
            interface="en0",
            is_active=True,
            priority=0,
        )
        ifconfig_list = MagicMock()
        ifconfig_list.stdout = "lo0 en0"

        sp_network = MagicMock()
        sp_network.stdout = "Ethernet"

        def mock_run(cmd, **kwargs):
            if cmd == ["ifconfig", "-l"]:
                return ifconfig_list
            if "system_profiler" in cmd[0]:
                return sp_network
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_ethernet()
            assert self.detector._interfaces["en0"].type == LinkType.THUNDERBOLT_5

    async def test_skip_non_ethernet_type(self):
        ifconfig_list = MagicMock()
        ifconfig_list.stdout = "lo0 en0"

        sp_network = MagicMock()
        sp_network.stdout = "Wi-Fi Adapters"

        def mock_run(cmd, **kwargs):
            if cmd == ["ifconfig", "-l"]:
                return ifconfig_list
            if "system_profiler" in cmd[0]:
                return sp_network
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_ethernet()
            assert "en0" not in self.detector._interfaces

    async def test_usb_ethernet(self):
        ifconfig_list = MagicMock()
        ifconfig_list.stdout = "lo0 en0"

        ifconfig_detail = MagicMock()
        ifconfig_detail.stdout = "en0: flags=\n\tmedia: autoselect (100baseT <full-duplex>)\n"

        sp_network = MagicMock()
        sp_network.stdout = "USB Ethernet"

        def mock_run(cmd, **kwargs):
            if cmd == ["ifconfig", "-l"]:
                return ifconfig_list
            if cmd[0] == "ifconfig" and cmd[1] != "-l":
                return ifconfig_detail
            if "system_profiler" in cmd[0]:
                return sp_network
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_ethernet()
            assert "en0" in self.detector._interfaces
            assert self.detector._interfaces["en0"].type == LinkType.ETHERNET_100M

    async def test_ethernet_subprocess_exception(self):
        with patch("subprocess.run", side_effect=Exception("fail")):
            await self.detector._detect_ethernet()
            assert len(self.detector._interfaces) == 0


class TestDetectWifi:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    async def test_wifi_detected(self):
        airport_result = MagicMock()
        airport_result.stdout = "en0"

        ifconfig_detail = MagicMock()
        ifconfig_detail.stdout = "en0: flags=\n\tmedia: autoselect\n"

        def mock_run(cmd, **kwargs):
            if "airport" in cmd[-1] or "Apple80211" in cmd[0]:
                return airport_result
            if cmd[0] == "ifconfig" and cmd[1] != "-l":
                return ifconfig_detail
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_wifi()
            assert "en0" in self.detector._interfaces

    async def test_wifi_en1(self):
        airport_result = MagicMock()
        airport_result.stdout = "en1"

        ifconfig_detail = MagicMock()
        ifconfig_detail.stdout = "en1: flags=\n\tmedia: autoselect (1200baseT)\n"

        def mock_run(cmd, **kwargs):
            if "airport" in cmd[-1] or "Apple80211" in cmd[0]:
                return airport_result
            if cmd[0] == "ifconfig" and cmd[1] != "-l":
                return ifconfig_detail
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_wifi()
            assert "en1" in self.detector._interfaces

    async def test_wifi_no_output(self):
        airport_result = MagicMock()
        airport_result.stdout = ""

        def mock_run(cmd, **kwargs):
            return airport_result

        with patch("subprocess.run", return_value=airport_result):
            await self.detector._detect_wifi()
            wifi_ifaces = [k for k in self.detector._interfaces if k in ("en0", "en1")]
            assert len(wifi_ifaces) == 0

    async def test_wifi_skip_already_registered(self):
        self.detector._interfaces["en0"] = LinkInfo(
            type=LinkType.ETHERNET_1G,
            bandwidth_mbps=1000,
            latency_ms=0.1,
            interface="en0",
            is_active=True,
            priority=4,
        )
        airport_result = MagicMock()
        airport_result.stdout = "en0"

        ifconfig_detail = MagicMock()
        ifconfig_detail.stdout = "en0: flags=\n\tmedia: autoselect\n"

        def mock_run(cmd, **kwargs):
            if "airport" in cmd[-1] or "Apple80211" in cmd[0]:
                return airport_result
            if cmd[0] == "ifconfig" and cmd[1] != "-l":
                return ifconfig_detail
            return MagicMock(stdout="")

        with patch("subprocess.run", side_effect=mock_run):
            await self.detector._detect_wifi()
            assert self.detector._interfaces["en0"].type == LinkType.ETHERNET_1G

    async def test_wifi_zero_speed_skipped(self):
        airport_result = MagicMock()
        airport_result.stdout = "en0"

        with (
            patch("subprocess.run", return_value=airport_result),
            patch.object(
                self.detector,
                "_measure_interface_speed",
                new_callable=AsyncMock,
                return_value=0.0,
            ),
            patch.object(
                self.detector,
                "_measure_latency",
                new_callable=AsyncMock,
                return_value=0.1,
            ),
        ):
            await self.detector._detect_wifi()
            assert "en0" not in self.detector._interfaces

    async def test_wifi_subprocess_exception(self):
        with patch("subprocess.run", side_effect=Exception("no airport")):
            await self.detector._detect_wifi()
            assert len(self.detector._interfaces) == 0


class TestGetInterfaceType:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    def test_thunderbolt_type(self):
        mock_result = MagicMock()
        mock_result.stdout = "Thunderbolt Bridge"
        with patch("subprocess.run", return_value=mock_result):
            assert self.detector._get_interface_type("bridge100") == "Thunderbolt"

    def test_ethernet_type(self):
        mock_result = MagicMock()
        mock_result.stdout = "USB 10/100/1000 LAN"
        with patch("subprocess.run", return_value=mock_result):
            assert self.detector._get_interface_type("en0") == "Ethernet"

    def test_ethernet_keyword(self):
        mock_result = MagicMock()
        mock_result.stdout = "Ethernet Adapter"
        with patch("subprocess.run", return_value=mock_result):
            assert self.detector._get_interface_type("en0") == "Ethernet"

    def test_unknown_type(self):
        mock_result = MagicMock()
        mock_result.stdout = "Wi-Fi"
        with patch("subprocess.run", return_value=mock_result):
            assert self.detector._get_interface_type("en0") == "Unknown"

    def test_subprocess_exception(self):
        with patch("subprocess.run", side_effect=Exception("fail")):
            assert self.detector._get_interface_type("en0") == "Unknown"


class TestFullDetect:
    def setup_method(self):
        self.detector = NetworkTopologyDetector()

    async def test_detect_clears_old(self):
        self.detector._interfaces["old0"] = LinkInfo(
            type=LinkType.UNKNOWN,
            bandwidth_mbps=0,
            latency_ms=0,
            interface="old0",
        )
        with (
            patch.object(self.detector, "_detect_thunderbolt", new_callable=AsyncMock),
            patch.object(self.detector, "_detect_ethernet", new_callable=AsyncMock),
            patch.object(self.detector, "_detect_wifi", new_callable=AsyncMock),
        ):
            await self.detector.detect()
            assert "old0" not in self.detector._interfaces
            assert "lo0" in self.detector._interfaces

    async def test_detect_sets_detected_flag(self):
        assert self.detector._detected is False
        with (
            patch.object(self.detector, "_detect_thunderbolt", new_callable=AsyncMock),
            patch.object(self.detector, "_detect_ethernet", new_callable=AsyncMock),
            patch.object(self.detector, "_detect_wifi", new_callable=AsyncMock),
        ):
            await self.detector.detect()
            assert self.detector._detected is True


class TestNetworkPath:
    def test_defaults(self):
        path = NetworkPath(source="a", target="b")
        assert path.links == []
        assert path.primary_link is None
        assert path.aggregated_bandwidth_mbps == 0.0
        assert path.avg_latency_ms == 0.0

    def test_with_values(self):
        link = LinkInfo(
            type=LinkType.THUNDERBOLT_5,
            bandwidth_mbps=40000,
            latency_ms=0.05,
            interface="bridge100",
        )
        path = NetworkPath(
            source="a",
            target="b",
            links=[link],
            primary_link=link,
            aggregated_bandwidth_mbps=40000,
            avg_latency_ms=0.05,
        )
        assert path.source == "a"
        assert len(path.links) == 1
        assert path.primary_link.type == LinkType.THUNDERBOLT_5

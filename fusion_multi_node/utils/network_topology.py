"""Network Topology — Thunderbolt RDMA 网络拓扑检测与链路优化。

自动检测 Mac 之间的网络连接类型：
- Thunderbolt 5 (40Gbps+) / Thunderbolt 4 (20Gbps) / Thunderbolt 3 (10Gbps)
- Ethernet 10GbE / 1GbE / 100Mbps
- 优先使用高速链路传输张量数据
"""

from __future__ import annotations

import asyncio
import logging
import platform
import socket
import struct
import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class LinkType(Enum):
    """网络链路类型。"""
    THUNDERBOLT_5 = "thunderbolt_5"     # 40Gbps+
    THUNDERBOLT_4 = "thunderbolt_4"     # 20Gbps
    THUNDERBOLT_3 = "thunderbolt_3"     # 10Gbps
    ETHERNET_10G = "ethernet_10g"       # 10Gbps
    ETHERNET_1G = "ethernet_1g"         # 1Gbps
    ETHERNET_100M = "ethernet_100m"     # 100Mbps
    WIFI_6E = "wifi_6e"                 # WiFi 6E
    WIFI_6 = "wifi_6"                   # WiFi 6
    UNKNOWN = "unknown"                 # 未知


@dataclass
class LinkInfo:
    """链路信息。"""
    type: LinkType
    bandwidth_mbps: float
    latency_ms: float
    interface: str
    is_rdma: bool = False
    is_active: bool = False
    priority: int = 0


@dataclass
class NetworkPath:
    """网络路径 — 两节点之间的最优链路。"""
    source: str
    target: str
    links: List[LinkInfo] = field(default_factory=list)
    primary_link: Optional[LinkInfo] = None
    aggregated_bandwidth_mbps: float = 0.0
    avg_latency_ms: float = 0.0


class NetworkTopologyDetector:
    """网络拓扑检测器 — 发现节点间链路并选择最优路径。"""

    def __init__(self):
        self._interfaces: Dict[str, LinkInfo] = {}
        self._paths: Dict[Tuple[str, str], NetworkPath] = {}
        self._detected = False

    async def detect(self) -> Dict[str, LinkInfo]:
        """检测本机所有网络接口。"""
        self._interfaces.clear()

        # 检测 Thunderbolt 桥接接口
        await self._detect_thunderbolt()
        # 检测以太网接口
        await self._detect_ethernet()
        # 检测 WiFi 接口
        await self._detect_wifi()
        # 回环
        self._detect_loopback()

        self._detected = True
        logger.info(f"网络拓扑检测完成: {len(self._interfaces)} 个接口")
        for name, link in self._interfaces.items():
            logger.debug(f"  {name}: {link.type.value} ({link.bandwidth_mbps}Mbps, {link.latency_ms}ms)")

        return self._interfaces

    async def _detect_thunderbolt(self) -> None:
        """检测 Thunderbolt 网桥接口。"""
        try:
            result = subprocess.run(
                ["system_profiler", "SPThunderboltDataType"],
                capture_output=True, text=True, timeout=10,
            )
            output = result.stdout

            # 解析 Thunderbolt 信息
            has_thunderbolt = "Thunderbolt" in output
            if not has_thunderbolt:
                return

            # 检测 Thunderbolt 网桥接口
            iface_result = subprocess.run(
                ["ifconfig", "-l"],
                capture_output=True, text=True, timeout=2,
            )
            interfaces = iface_result.stdout.strip().split()

            for iface in interfaces:
                if "bridge" in iface.lower() or "thunderbolt" in iface.lower() or "fw" in iface.lower():
                    speed = await self._measure_interface_speed(iface)
                    latency = await self._measure_latency(iface)
                    link_type = self._classify_thunderbolt(speed)
                    self._interfaces[iface] = LinkInfo(
                        type=link_type,
                        bandwidth_mbps=speed,
                        latency_ms=latency,
                        interface=iface,
                        is_rdma=True,
                        is_active=True,
                        priority=self._get_priority(link_type),
                    )

        except Exception as e:
            logger.debug(f"Thunderbolt 检测失败: {e}")

    async def _detect_ethernet(self) -> None:
        """检测以太网接口。"""
        try:
            result = subprocess.run(
                ["ifconfig", "-l"],
                capture_output=True, text=True, timeout=2,
            )
            interfaces = result.stdout.strip().split()

            for iface in interfaces:
                if iface.startswith("en") and iface not in self._interfaces:
                    # 检查是否为以太网（不是 WiFi）
                    iface_type = self._get_interface_type(iface)
                    if iface_type in ("Ethernet", "USB Ethernet"):
                        speed = await self._measure_interface_speed(iface)
                        latency = await self._measure_latency(iface)
                        link_type = self._classify_ethernet(speed)
                        self._interfaces[iface] = LinkInfo(
                            type=link_type,
                            bandwidth_mbps=speed,
                            latency_ms=latency,
                            interface=iface,
                            is_rdma=False,
                            is_active=True,
                            priority=self._get_priority(link_type),
                        )

        except Exception as e:
            logger.debug(f"以太网检测失败: {e}")

    async def _detect_wifi(self) -> None:
        """检测 WiFi 接口。"""
        try:
            result = subprocess.run(
                ["/System/Library/PrivateFrameworks/Apple80211.framework/Versions/Current/Resources/airport", "-I"],
                capture_output=True, text=True, timeout=3,
            )
            output = result.stdout
            if "en0" in output or "en1" in output:
                for iface in ["en0", "en1"]:
                    if iface not in self._interfaces:
                        speed = await self._measure_interface_speed(iface)
                        latency = await self._measure_latency(iface)
                        link_type = self._classify_wifi(speed)
                        if speed > 0:
                            self._interfaces[iface] = LinkInfo(
                                type=link_type,
                                bandwidth_mbps=speed,
                                latency_ms=latency,
                                interface=iface,
                                is_rdma=False,
                                is_active=True,
                                priority=self._get_priority(link_type),
                            )
        except Exception as e:
            logger.debug(f"WiFi 检测失败: {e}")

    def _detect_loopback(self) -> None:
        """回环接口。"""
        self._interfaces["lo0"] = LinkInfo(
            type=LinkType.THUNDERBOLT_5,  # 视为最高速
            bandwidth_mbps=40000,
            latency_ms=0.01,
            interface="lo0",
            is_rdma=False,
            is_active=True,
            priority=0,
        )

    def _get_interface_type(self, iface: str) -> str:
        """获取接口类型。"""
        try:
            result = subprocess.run(
                ["system_profiler", "SPNetworkDataType"],
                capture_output=True, text=True, timeout=5,
            )
            # 简单解析
            if "Thunderbolt" in result.stdout:
                return "Thunderbolt"
            if "USB 10/100/1000 LAN" in result.stdout or "Ethernet" in result.stdout:
                return "Ethernet"
        except Exception:
            pass
        return "Unknown"

    def _classify_thunderbolt(self, speed_mbps: float) -> LinkType:
        if speed_mbps >= 40000:
            return LinkType.THUNDERBOLT_5
        elif speed_mbps >= 20000:
            return LinkType.THUNDERBOLT_4
        elif speed_mbps >= 10000:
            return LinkType.THUNDERBOLT_3
        return LinkType.THUNDERBOLT_3

    def _classify_ethernet(self, speed_mbps: float) -> LinkType:
        if speed_mbps >= 10000:
            return LinkType.ETHERNET_10G
        elif speed_mbps >= 1000:
            return LinkType.ETHERNET_1G
        return LinkType.ETHERNET_100M

    def _classify_wifi(self, speed_mbps: float) -> LinkType:
        if speed_mbps >= 2400:
            return LinkType.WIFI_6E
        elif speed_mbps >= 1200:
            return LinkType.WIFI_6
        return LinkType.UNKNOWN

    def _get_priority(self, link_type: LinkType) -> int:
        """获取链路优先级（越低越优先）。"""
        priorities = {
            LinkType.THUNDERBOLT_5: 0,
            LinkType.THUNDERBOLT_4: 1,
            LinkType.THUNDERBOLT_3: 2,
            LinkType.ETHERNET_10G: 3,
            LinkType.ETHERNET_1G: 4,
            LinkType.ETHERNET_100M: 5,
            LinkType.WIFI_6E: 6,
            LinkType.WIFI_6: 7,
            LinkType.UNKNOWN: 10,
        }
        return priorities.get(link_type, 10)

    async def _measure_interface_speed(self, iface: str) -> float:
        """测量接口速度（Mbps）。"""
        try:
            result = subprocess.run(
                ["ifconfig", iface],
                capture_output=True, text=True, timeout=2,
            )
            for line in result.stdout.split("\n"):
                if "media:" in line:
                    if "baseT" in line:
                        # 提取速率，如 "media: autoselect (1000baseT <full-duplex>)"
                        import re
                        m = re.search(r'(\d+)baseT', line)
                        if m:
                            return float(m.group(1))
                    if "thunderbolt" in line.lower() or "40" in line:
                        return 40000.0
        except Exception:
            pass
        return 1000.0  # 默认 1Gbps

    async def _measure_latency(self, iface: str) -> float:
        """测量接口延迟（ms）。"""
        # 回环延迟极低
        if iface == "lo0":
            return 0.01
        return 0.1  # 局域网默认 0.1ms

    async def measure_peer_latency(self, peer_ip: str, count: int = 3) -> float:
        """测量到对端节点的延迟。"""
        latencies = []
        for _ in range(count):
            try:
                start = time.time()
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(peer_ip, 9755),
                    timeout=2.0,
                )
                writer.close()
                await writer.wait_closed()
                latencies.append((time.time() - start) * 1000)
            except Exception:
                pass

        if not latencies:
            return 10.0  # 默认 10ms
        return sum(latencies) / len(latencies)

    def get_best_link(self) -> Optional[LinkInfo]:
        """获取最优链路。"""
        active = [l for l in self._interfaces.values() if l.is_active]
        if not active:
            return None
        return min(active, key=lambda l: l.priority)

    def get_primary_interface(self) -> str:
        """获取主接口名称。"""
        best = self.get_best_link()
        return best.interface if best else "lo0"

    def get_link_type(self) -> LinkType:
        """获取主链路类型。"""
        best = self.get_best_link()
        return best.type if best else LinkType.UNKNOWN

    def get_link_speed(self) -> float:
        """获取主链路速度（Mbps）。"""
        best = self.get_best_link()
        return best.bandwidth_mbps if best else 1000.0

    def is_thunderbolt_available(self) -> bool:
        """检查是否有 Thunderbolt 高速链路。"""
        return any(
            l.type in (LinkType.THUNDERBOLT_5, LinkType.THUNDERBOLT_4, LinkType.THUNDERBOLT_3)
            for l in self._interfaces.values()
        )

    def get_recommended_compression(self) -> str:
        """根据链路类型推荐压缩策略。"""
        link_type = self.get_link_type()
        if link_type in (LinkType.THUNDERBOLT_5, LinkType.THUNDERBOLT_4):
            return "none"
        elif link_type in (LinkType.THUNDERBOLT_3, LinkType.ETHERNET_10G):
            return "light"
        elif link_type == LinkType.ETHERNET_1G:
            return "normal"
        else:
            return "aggressive"
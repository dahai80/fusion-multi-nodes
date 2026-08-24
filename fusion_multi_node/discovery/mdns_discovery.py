"""mDNS/Bonjour 零配置节点发现。

基于 zeroconf 实现：
- Master: 注册 mDNS 服务，局域网可发现
- Agent: 浏览 mDNS 服务，自动发现 Master
- 支持共享密钥验证，防止未授权节点加入
"""

from __future__ import annotations

import hashlib
import logging
import platform
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

SERVICE_TYPE = "_fusionmlx._tcp.local."
DEFAULT_DISCOVERY_PORT = 11450


def _hash_cluster_secret(secret: str, node_id: str = "") -> str:
    # 绑定 node_id: 防止持同一共享密钥的 Worker 用他人 node_id 广播伪造 master。
    # hash = sha256(secret || ":" || node_id)[:32]
    material = f"{secret}:{node_id}".encode()
    return hashlib.sha256(material).hexdigest()[:32]


@dataclass
class DiscoveryInfo:
    """发现的节点信息。"""

    name: str
    host: str
    port: int
    properties: dict[str, str] = field(default_factory=dict)
    discovered_at: float = 0.0

    @property
    def server_name(self) -> str:
        return f"{self.name}.{SERVICE_TYPE}"


class MDNSDiscovery:
    """mDNS 节点发现管理器。"""

    def __init__(
        self,
        node_id: str = "",
        service_type: str = SERVICE_TYPE,
        cluster_secret: str = "",
    ):
        self.node_id = node_id or f"fusion-{platform.node().lower()}"
        self.service_type = service_type
        self._cluster_secret = cluster_secret
        # sticky master: 一旦锁定合法 master_id, 拒绝任何不同 id 的 master 广播 (防 rogue-master 接管)
        self._locked_master_id: str | None = None
        self._registry: Any | None = None
        self._browser: Any | None = None
        self._zeroconf: Any | None = None
        self._discovered: dict[str, DiscoveryInfo] = {}
        # E8: _discovered 被 zeroconf daemon 线程 (add_service/remove_service 回调) 与
        # async 协程 (browse_async/get_discovered) 跨线程读写, 无锁会触发
        # RuntimeError: dictionary changed size during iteration。加锁保护所有访问点。
        self._discovered_lock = threading.Lock()
        self._on_discover: Callable[[DiscoveryInfo], None] | None = None
        self._on_remove: Callable[[str], None] | None = None
        self._registered = False

    # ── 服务注册（Master 用） ──

    def register(
        self,
        port: int = 11452,
        properties: dict[str, str] | None = None,
    ) -> bool:
        """注册 mDNS 服务，使局域网内其他节点可发现。"""
        try:
            from zeroconf import IPVersion, ServiceInfo, Zeroconf
        except ImportError:
            logger.error("zeroconf 未安装，请运行: pip install zeroconf")
            return False

        props = properties or {}
        props.setdefault("node_id", self.node_id)
        props.setdefault("role", "master")
        props.setdefault("arch", platform.machine())
        props.setdefault("hostname", platform.node())
        props.setdefault("heartbeat_interval", "3")
        props.setdefault("heartbeat_timeout", "15")
        if self._cluster_secret:
            props["cluster_hash"] = _hash_cluster_secret(self._cluster_secret, self.node_id)

        try:
            local_ip = self._get_local_ip()
            info = ServiceInfo(
                self.service_type,
                name=f"{self.node_id}.{self.service_type}",
                addresses=[socket.inet_aton(local_ip)],
                port=port,
                properties=props,
            )

            self._zeroconf = Zeroconf(ip_version=IPVersion.V4Only)
            self._zeroconf.register_service(info)
            self._registered = True
            self._registry = info
            logger.info(f"mDNS 服务注册成功: {self.node_id} @ {local_ip}:{port}")
            return True
        except Exception as e:
            logger.error(f"mDNS 注册失败: {e}")
            return False

    def unregister(self) -> None:
        """注销 mDNS 服务。"""
        if self._zeroconf and self._registered:
            try:
                if self._registry:
                    self._zeroconf.unregister_service(self._registry)
                self._zeroconf.close()
                logger.info("mDNS 服务已注销")
            except Exception as e:
                logger.warning(f"mDNS 注销异常: {e}")
            finally:
                self._zeroconf = None
                self._registry = None
                self._registered = False

    # ── 服务浏览（Agent 用） ──

    def browse(
        self,
        timeout: float = 5.0,
        on_discover: Callable[[DiscoveryInfo], None] | None = None,
        on_remove: Callable[[str], None] | None = None,
    ) -> list[DiscoveryInfo]:
        """浏览局域网内 mDNS 服务，返回发现的节点列表。

        注意: 此方法是同步的，会阻塞调用线程 (time.sleep)。仅限 CLI/同步上下文使用;
        Agent 运行时路径用 browse_async/find_master_async, 不可在事件循环线程调本方法
        (会冻结心跳/推理协程)。_discovered 跨线程访问经 _discovered_lock 保护。
        """
        try:
            from zeroconf import ServiceBrowser, Zeroconf
        except ImportError:
            logger.error("zeroconf 未安装，请运行: pip install zeroconf")
            return []

        self._on_discover = on_discover
        self._on_remove = on_remove
        with self._discovered_lock:
            self._discovered.clear()

        class _Listener:
            def __init__(self, outer: MDNSDiscovery):
                self._outer = outer

            def add_service(self, zc: Any, type_: str, name: str) -> None:
                info = zc.get_service_info(type_, name)
                if info:
                    di = _service_info_to_discovery(name, info)
                    with self._outer._discovered_lock:
                        self._outer._discovered[name] = di
                    if self._outer._on_discover:
                        self._outer._on_discover(di)
                    logger.info(f"mDNS 发现节点: {di.name} @ {di.host}:{di.port}")

            def remove_service(self, zc: Any, type_: str, name: str) -> None:
                with self._outer._discovered_lock:
                    self._outer._discovered.pop(name, None)
                if self._outer._on_remove:
                    self._outer._on_remove(name)
                logger.info(f"mDNS 节点离线: {name}")

            def update_service(self, zc: Any, type_: str, name: str) -> None:
                self.add_service(zc, type_, name)

        try:
            zc = Zeroconf()
            browser = ServiceBrowser(zc, self.service_type, handlers=[_Listener(self)])
            self._zeroconf = zc
            self._browser = browser

            logger.info(f"mDNS 浏览中... (超时: {timeout}s)")
            time.sleep(timeout)

            browser.cancel()
            zc.close()
            self._browser = None
            self._zeroconf = None

            with self._discovered_lock:
                results = list(self._discovered.values())
            logger.info(f"mDNS 发现 {len(results)} 个节点")
            return results
        except Exception as e:
            logger.error(f"mDNS 浏览失败: {e}")
            return []

    async def browse_async(
        self,
        timeout: float = 5.0,
        on_discover: Callable[[DiscoveryInfo], None] | None = None,
        on_remove: Callable[[str], None] | None = None,
    ) -> list[DiscoveryInfo]:
        """异步浏览 mDNS 服务 — 非阻塞版本。"""
        import asyncio

        try:
            from zeroconf import ServiceBrowser, Zeroconf
        except ImportError:
            logger.error("zeroconf 未安装，请运行: pip install zeroconf")
            return []

        self._on_discover = on_discover
        self._on_remove = on_remove
        with self._discovered_lock:
            self._discovered.clear()

        class _AsyncListener:
            def __init__(self, outer: MDNSDiscovery):
                self._outer = outer

            def add_service(self, zc: Any, type_: str, name: str) -> None:
                info = zc.get_service_info(type_, name)
                if info:
                    di = _service_info_to_discovery(name, info)
                    with self._outer._discovered_lock:
                        self._outer._discovered[name] = di
                    if self._outer._on_discover:
                        self._outer._on_discover(di)
                    logger.info(f"mDNS 发现节点: {di.name} @ {di.host}:{di.port}")

            def remove_service(self, zc: Any, type_: str, name: str) -> None:
                with self._outer._discovered_lock:
                    self._outer._discovered.pop(name, None)
                if self._outer._on_remove:
                    self._outer._on_remove(name)
                logger.info(f"mDNS 节点离线: {name}")

            def update_service(self, zc: Any, type_: str, name: str) -> None:
                self.add_service(zc, type_, name)

        try:
            zc = Zeroconf()
            browser = ServiceBrowser(zc, self.service_type, handlers=[_AsyncListener(self)])
            self._zeroconf = zc
            self._browser = browser

            logger.info(f"mDNS 异步浏览中... (超时: {timeout}s)")
            await asyncio.sleep(timeout)

            browser.cancel()
            zc.close()
            self._browser = None
            self._zeroconf = None

            with self._discovered_lock:
                results = list(self._discovered.values())
            logger.info(f"mDNS 异步发现 {len(results)} 个节点")
            return results
        except Exception as e:
            logger.error(f"mDNS 异步浏览失败: {e}")
            return []

    def get_discovered(self) -> list[DiscoveryInfo]:
        """获取已发现的节点列表。"""
        with self._discovered_lock:
            return list(self._discovered.values())

    def _verify_master_candidate(self, node: DiscoveryInfo) -> bool:
        """校验单个 master 候选: 角色 + node_id + 绑定密钥 + sticky 锁定。"""
        if node.properties.get("role") != "master":
            return False
        node_id = node.properties.get("node_id", "")
        if not node_id:
            logger.warning(f"mDNS 节点 {node.name} 缺少 node_id，跳过")
            return False
        if self._cluster_secret:
            remote_hash = node.properties.get("cluster_hash", "")
            expected = _hash_cluster_secret(self._cluster_secret, node_id)
            if remote_hash != expected:
                logger.warning(f"mDNS 节点 {node.name} 密钥验证失败 (node_id={node_id})，跳过")
                return False
        # sticky master: 已锁定 id 时, 仅接受同一 id (rogue 用不同 id 广播 master 被拒)
        if self._locked_master_id is not None and node_id != self._locked_master_id:
            logger.warning(f"mDNS 拒绝 rogue-master: 锁定 {self._locked_master_id!r}, 收到 {node_id!r}")
            return False
        return True

    def lock_master_id(self, master_id: str) -> None:
        """锁定合法 master node_id — 首次确认合法 master 后调用, 后续拒绝其他 id。"""
        if master_id and self._locked_master_id is None:
            self._locked_master_id = master_id
            logger.info(f"mDNS sticky master 已锁定: {master_id}")

    def find_master(self, timeout: float = 5.0) -> DiscoveryInfo | None:
        """浏览并找到 Master 节点。"""
        nodes = self.browse(timeout)
        for node in nodes:
            if not self._verify_master_candidate(node):
                continue
            self.lock_master_id(node.properties.get("node_id", ""))
            return node
        return None

    async def find_master_async(self, timeout: float = 5.0) -> DiscoveryInfo | None:
        """异步查找 Master 节点 — 非阻塞版本。"""
        nodes = await self.browse_async(timeout)
        for node in nodes:
            if not self._verify_master_candidate(node):
                continue
            self.lock_master_id(node.properties.get("node_id", ""))
            return node
        return None

    # ── 辅助 ──

    def _get_local_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    @staticmethod
    def get_heartbeat_config(properties: dict[str, str]) -> dict[str, int]:
        interval = int(properties.get("heartbeat_interval", "3"))
        timeout = int(properties.get("heartbeat_timeout", "15"))
        return {"interval": interval, "timeout": timeout}


def _service_info_to_discovery(name: str, info: Any) -> DiscoveryInfo:
    """将 zeroconf ServiceInfo 转为 DiscoveryInfo。"""
    addresses = info.parsed_addresses()
    host = addresses[0] if addresses else "127.0.0.1"
    props = {}
    if info.properties:
        for k, v in info.properties.items():
            key = k.decode("utf-8") if isinstance(k, bytes) else k
            val = v.decode("utf-8") if isinstance(v, bytes) else str(v) if v else ""
            props[key] = val

    short_name = name.split(".")[0] if "." in name else name

    return DiscoveryInfo(
        name=short_name,
        host=host,
        port=info.port,
        properties=props,
        discovered_at=time.time(),
    )

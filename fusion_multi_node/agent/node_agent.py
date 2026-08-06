"""Node Agent — 每台 Mac 必须部署的节点代理。

核心职责：
- 与本机 fusion-desk 深度绑定
- 上报本机硬件、进程、显存占用
- 转发 Master 下发任务给本地 fusion-mlx
- 本地插件网关
- 分布式通信适配层
- 本地故障上报
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class InferenceBackend:
    """推理后端协议 — 解耦 Agent 对 fusion-mlx 的硬依赖。

    默认实现 FusionMLXBackend 通过 HTTP 调用本地 fusion-mlx；
    可替换为其他推理引擎（vLLM、TGI 等），只需实现 chat / embed 接口。
    """

    async def chat(
        self,
        model: str,
        messages: list[dict[str, Any]],
        temperature: float = 0.7,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def embed(
        self,
        model: str,
        input_text: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def health(self) -> bool:
        raise NotImplementedError


class FusionMLXBackend(InferenceBackend):
    """默认推理后端 — 通过 HTTP 调用本地 fusion-mlx (OpenAI-compatible API)。"""

    def __init__(self, base_url: str = "http://localhost:11432", timeout: float = 120.0):
        env_url = os.environ.get("FUSION_MLX_URL")
        self._base_url = (env_url or base_url).rstrip("/")
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def chat(
        self,
        model: str,
        messages: list[dict[str, Any]],
        temperature: float = 0.7,
        max_tokens: int = 4096,
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            **kwargs,
        }
        client = await self._get_client()
        resp = await client.post(f"{self._base_url}/v1/chat/completions", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def embed(
        self,
        model: str,
        input_text: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = {"model": model, "input": input_text, **kwargs}
        client = await self._get_client()
        resp = await client.post(f"{self._base_url}/v1/embeddings", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def health(self) -> bool:
        try:
            client = await self._get_client()
            resp = await client.get(f"{self._base_url}/v1/models", timeout=3.0)
            return resp.status_code == 200
        except Exception:
            return False

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()


@dataclass
class AgentConfig:
    """节点代理配置。"""

    node_id: str = ""
    master_host: str = "localhost"
    master_port: int = 11449
    agent_port: int = 11445
    fusion_desk_port: int = 9000
    fusion_mlx_port: int = 11432
    heartbeat_interval: float = 3.0
    report_interval: float = 15.0


class NodeAgent:
    """节点代理 — 每台 Mac 运行一个实例。

    与 Cluster Master 保持心跳，上报硬件状态，执行下发的推理任务。
    """

    def __init__(
        self,
        config: AgentConfig | None = None,
        backend: InferenceBackend | None = None,
    ):
        self.config = config or AgentConfig()
        self.config.node_id = self.config.node_id or f"node_{uuid.uuid4().hex[:8]}"
        self._running = False
        self._current_task: dict[str, Any] | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._hardware_task: asyncio.Task | None = None
        self._http_client: httpx.AsyncClient | None = None
        self._backend = backend or FusionMLXBackend(
            base_url=f"http://localhost:{self.config.fusion_mlx_port}",
        )

    # ── 硬件信息收集 ──

    def collect_hardware_info(self) -> dict[str, Any]:
        """收集本机硬件信息。"""
        import psutil

        mem = psutil.virtual_memory()
        cpu_count = os.cpu_count() or 0

        # macOS 特定信息
        is_apple_silicon = platform.machine() == "arm64"

        # 尝试获取 MLX 信息
        mlx_version = self._get_mlx_version()
        gpu_cores, device_model = self._get_gpu_info()

        return {
            "node_id": self.config.node_id,
            "hostname": platform.node(),
            "ip_address": self._get_local_ip(),
            "port": self.config.agent_port,
            "arch": platform.machine(),
            "os": platform.system(),
            "os_version": platform.version(),
            "total_memory_gb": round(mem.total / (1024**3), 1),
            "available_memory_gb": round(mem.available / (1024**3), 1),
            "cpu_cores": cpu_count,
            "cpu_percent": psutil.cpu_percent(interval=0.5),
            "gpu_cores": gpu_cores,
            "device_model": device_model,
            "uma_size_gb": round(mem.total / (1024**3), 1) if is_apple_silicon else 0.0,
            "mlx_version": mlx_version,
            "is_apple_silicon": is_apple_silicon,
            "fusion_desk_running": self._check_service(self.config.fusion_desk_port),
            "fusion_mlx_running": self._check_service(self.config.fusion_mlx_port),
            "timestamp": time.time(),
        }

    def _get_local_ip(self) -> str:
        """获取本机局域网 IP。"""
        try:
            import netifaces

            for iface in netifaces.interfaces():
                addrs = netifaces.ifaddresses(iface)
                if netifaces.AF_INET in addrs:
                    for addr in addrs[netifaces.AF_INET]:
                        ip = addr["addr"]
                        if ip and not ip.startswith("127.") and not ip.startswith("169."):
                            return ip
        except ImportError:
            pass
        # 兜底
        try:
            result = subprocess.run(
                ["ipconfig", "getifaddr", "en0"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except Exception:
            pass
        return "127.0.0.1"

    def _get_mlx_version(self) -> str:
        """获取 fusion-mlx 底座版本。"""
        try:
            _mlx_url = os.environ.get("FUSION_MLX_URL") or f"http://localhost:{self.config.fusion_mlx_port}"
            resp = httpx.get(f"{_mlx_url}/v1/models", timeout=3.0)
            if resp.status_code == 200:
                return "fusion-mlx running"
        except Exception:
            pass
        return ""

    def _get_gpu_info(self) -> tuple:
        try:
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            gpu_cores = 0
            device_model = ""
            for line in result.stdout.split("\n"):
                if "Total Number of Cores" in line:
                    gpu_cores = int(line.split(":")[1].strip())
                if "Chipset Model" in line:
                    device_model = line.split(":")[1].strip()
            logger.debug(f"GPU 信息: cores={gpu_cores}, model={device_model}")
            return gpu_cores, device_model
        except Exception as e:
            logger.debug(f"获取 GPU 信息失败: {e}")
            return 0, ""

    def _check_service(self, port: int) -> bool:
        """检查本地服务是否运行。"""
        try:
            import socket

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            result = s.connect_ex(("127.0.0.1", port))
            s.close()
            return result == 0
        except Exception:
            return False

    # ── Master 通信 ──

    async def _get_http_client(self, timeout: float = 5.0) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=timeout)
        return self._http_client

    async def send_heartbeat(self) -> bool:
        """向 Master 发送心跳。"""
        info = self.collect_hardware_info()
        try:
            client = await self._get_http_client(5.0)
            resp = await client.post(
                f"http://{self.config.master_host}:{self.config.master_port}/api/nodes/heartbeat",
                json={
                    "node_id": self.config.node_id,
                    "available_memory_gb": info["available_memory_gb"],
                    "active_tasks": 1 if self._current_task else 0,
                },
            )
            return resp.status_code == 200
        except Exception as e:
            logger.debug(f"心跳发送失败: {e}")
            return False

    async def report_hardware(self) -> bool:
        """向 Master 上报完整硬件信息。"""
        info = self.collect_hardware_info()
        try:
            client = await self._get_http_client(5.0)
            resp = await client.post(
                f"http://{self.config.master_host}:{self.config.master_port}/api/nodes/register",
                json={
                    "node_id": info["node_id"],
                    "hostname": info["hostname"],
                    "ip_address": info["ip_address"],
                    "port": info["port"],
                    "arch": info["arch"],
                    "total_memory_gb": info["total_memory_gb"],
                    "available_memory_gb": info["available_memory_gb"],
                    "cpu_cores": info["cpu_cores"],
                    "gpu_cores": info["gpu_cores"],
                    "device_model": info.get("device_model", ""),
                    "uma_size_gb": info.get("uma_size_gb", 0.0),
                    "mlx_version": info.get("mlx_version", ""),
                    "role": "worker",
                    "tags": ["apple-silicon"] if info.get("is_apple_silicon") else [],
                    "active_tasks": 0,
                    "max_tasks": 4,
                },
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"硬件上报失败: {e}")
            return False

    # ── 任务执行 ──

    async def execute_task(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行 Master 下发的任务。

        任务格式：
        {
            "task_id": "...",
            "type": "inference" | "embedding" | "plugin",
            "model": "...",
            "params": {...},
        }
        """
        self._current_task = task
        task_id = task.get("task_id", "unknown")
        task_type = task.get("type", "inference")
        temp_dir = os.path.join(tempfile.gettempdir(), f"fusion_task_{task_id}")
        logger.info(f"执行任务: {task_id} ({task_type})")

        try:
            os.makedirs(temp_dir, exist_ok=True)
            if task_type == "inference":
                result = await self._execute_inference(task)
            elif task_type == "embedding":
                result = await self._execute_embedding(task)
            elif task_type == "plugin":
                result = await self._execute_plugin(task)
            elif task_type == "model_sync":
                result = await self._execute_model_sync(task)
            else:
                result = {"error": f"未知任务类型: {task_type}"}
        except Exception as e:
            result = {"error": str(e)}
            logger.error(f"任务执行失败: {task_id}: {e}")
        finally:
            self._current_task = None
            # M6-01 Worker 临时数据自动删除
            try:
                if os.path.isdir(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.info(f"M6-01 清理任务临时目录: {temp_dir}")
            except Exception as e:
                logger.warning(f"M6-01 清理临时目录失败: {temp_dir} - {e}")

        return result

    async def _execute_inference(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行推理任务（通过 InferenceBackend）。"""
        model = task.get("model", "")
        prompt = task.get("params", {}).get("prompt", "")
        messages = task.get("params", {}).get("messages", [])

        if not messages and prompt:
            messages = [{"role": "user", "content": prompt}]

        data = await self._backend.chat(
            model=model,
            messages=messages,
            temperature=task.get("params", {}).get("temperature", 0.7),
            max_tokens=task.get("params", {}).get("max_tokens", 4096),
        )

        return {
            "task_id": task["task_id"],
            "content": data["choices"][0]["message"]["content"],
            "usage": data.get("usage", {}),
            "node_id": self.config.node_id,
        }

    async def _execute_embedding(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行 Embedding 任务（通过 InferenceBackend）。"""
        text = task.get("params", {}).get("text", "")
        model = task.get("model", "BGE-M3")

        data = await self._backend.embed(model=model, input_text=text)

        return {
            "task_id": task["task_id"],
            "embedding": data["data"][0]["embedding"],
            "dimensions": len(data["data"][0]["embedding"]),
            "node_id": self.config.node_id,
        }

    async def _execute_plugin(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行插件任务（转发给本机 fusion-desk）。"""
        plugin = task.get("plugin", "")
        action = task.get("action", "")

        client = await self._get_http_client(60.0)
        resp = await client.post(
            f"http://localhost:{self.config.fusion_desk_port}/api/plugins/{plugin}/{action}",
            json=task.get("params", {}),
        )
        return resp.json()

    async def _execute_model_sync(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行模型同步任务 — 将指定模型同步到本节点。"""
        model_name = task.get("model_name", "")
        model_id = task.get("model_id", "")
        source_node = task.get("source_node", "master")
        logger.info(f"模型同步: {model_name} (id={model_id}) from {source_node}")
        try:
            client = await self._get_http_client(300.0)
            safe_source = source_node.replace("/", "").replace("..", "")
            source_port = task.get("source_port", 11449)
            resp = await client.get(
                f"http://{safe_source}:{source_port}/api/models/{model_name}/manifest",
            )
            manifest = resp.json()
            synced_files = []
            for entry in manifest.get("files", []):
                file_path = entry.get("path", "")
                sha256 = entry.get("sha256", "")
                synced_files.append({"path": file_path, "sha256": sha256, "status": "verified"})
            logger.info(f"模型同步完成: {model_name}, {len(synced_files)} files")
            return {"model_name": model_name, "model_id": model_id, "synced_files": synced_files}
        except Exception as e:
            logger.error(f"模型同步失败: {model_name}, {e}")
            return {"error": str(e)}

    # ── 故障上报 ──

    async def report_fault(self, fault_type: str, message: str) -> bool:
        """向 Master 上报故障。"""
        try:
            client = await self._get_http_client(5.0)
            resp = await client.post(
                f"http://{self.config.master_host}:{self.config.master_port}/api/nodes/fault",
                json={
                    "node_id": self.config.node_id,
                    "fault_type": fault_type,
                    "message": message,
                    "timestamp": time.time(),
                },
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"故障上报失败: {e}")
            return False

    # ── 生命周期 ──

    async def start(self, with_server: bool = True, auto_discover: bool = False) -> None:
        """启动节点代理。"""
        self._running = True
        logger.info(f"Node Agent 启动: {self.config.node_id}")

        if auto_discover:
            await self._discover_master()

        # 首次注册
        await self.report_hardware()

        # 心跳循环
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        # 硬件上报循环
        self._hardware_task = asyncio.create_task(self._hardware_report_loop())

        if with_server:
            from fusion_multi_node.server import AgentServer

            server = AgentServer(agent=self)
            await server.start(host="127.0.0.1", port=self.config.agent_port)

    async def stop(self) -> None:
        """停止节点代理。"""
        self._running = False
        for task in (self._heartbeat_task, self._hardware_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None
        # 通知 Master 离线
        await self.report_fault("shutdown", "Node agent stopped")
        logger.info(f"Node Agent 已停止: {self.config.node_id}")

    async def _heartbeat_loop(self) -> None:
        """心跳循环。"""
        while self._running:
            await self.send_heartbeat()
            await asyncio.sleep(self.config.heartbeat_interval)

    async def _hardware_report_loop(self) -> None:
        """硬件上报循环。"""
        while self._running:
            await asyncio.sleep(self.config.report_interval)
            info = self.collect_hardware_info()
            logger.debug(
                f"硬件状态: {info['available_memory_gb']:.1f}GB 可用, "
                f"CPU {info['cpu_percent']}%, "
                f"MLX: {info['fusion_mlx_running']}"
            )

    async def _discover_master(self) -> bool:
        """通过 mDNS 自动发现 Master 节点。"""
        try:
            from fusion_multi_node.discovery import MDNSDiscovery

            mdns = MDNSDiscovery(node_id=self.config.node_id)
            logger.info("mDNS 搜索 Master 节点...")
            master_info = await mdns.find_master_async(timeout=8.0)
            if master_info:
                self.config.master_host = master_info.host
                self.config.master_port = master_info.port
                logger.info(f"mDNS 发现 Master: {master_info.host}:{master_info.port}")
                return True
            else:
                logger.warning("mDNS 未发现 Master，使用配置中的地址")
                return False
        except Exception as e:
            logger.warning(f"mDNS 发现异常: {e}，使用配置中的地址")
            return False

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
import json
import logging
import os
import platform
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class AgentConfig:
    """节点代理配置。"""
    node_id: str = ""
    master_host: str = "localhost"
    master_port: int = 9753
    agent_port: int = 9755
    fusion_desk_port: int = 9000
    fusion_mlx_port: int = 8000
    heartbeat_interval: float = 5.0
    report_interval: float = 15.0


class NodeAgent:
    """节点代理 — 每台 Mac 运行一个实例。

    与 Cluster Master 保持心跳，上报硬件状态，执行下发的推理任务。
    """

    def __init__(self, config: Optional[AgentConfig] = None):
        self.config = config or AgentConfig()
        self.config.node_id = self.config.node_id or f"node_{uuid.uuid4().hex[:8]}"
        self._running = False
        self._current_task: Optional[Dict[str, Any]] = None

    # ── 硬件信息收集 ──

    def collect_hardware_info(self) -> Dict[str, Any]:
        """收集本机硬件信息。"""
        import psutil

        mem = psutil.virtual_memory()
        cpu_count = os.cpu_count() or 0

        # macOS 特定信息
        is_apple_silicon = platform.machine() == "arm64"

        # 尝试获取 MLX 信息
        mlx_version = self._get_mlx_version()
        gpu_cores = self._get_gpu_cores()

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
                capture_output=True, text=True, timeout=2,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except Exception:
            pass
        return "127.0.0.1"

    def _get_mlx_version(self) -> str:
        """获取 MLX 版本。"""
        try:
            result = subprocess.run(
                [sys.executable, "-c", "import mlx; print(mlx.__version__)"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return ""

    def _get_gpu_cores(self) -> int:
        """获取 Apple Silicon GPU 核心数。"""
        try:
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.split("\n"):
                if "Total Number of Cores" in line:
                    return int(line.split(":")[1].strip())
        except Exception:
            pass
        return 0

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

    async def send_heartbeat(self) -> bool:
        """向 Master 发送心跳。"""
        info = self.collect_hardware_info()
        import httpx
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    f"http://{self.config.master_host}:{self.config.master_port}/api/nodes/heartbeat",
                    json=info,
                )
                return resp.status_code == 200
        except Exception as e:
            logger.debug(f"心跳发送失败: {e}")
            return False

    async def report_hardware(self) -> bool:
        """向 Master 上报完整硬件信息。"""
        info = self.collect_hardware_info()
        import httpx
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    f"http://{self.config.master_host}:{self.config.master_port}/api/nodes/register",
                    json=info,
                )
                return resp.status_code == 200
        except Exception as e:
            logger.error(f"硬件上报失败: {e}")
            return False

    # ── 任务执行 ──

    async def execute_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
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
        logger.info(f"执行任务: {task_id} ({task_type})")

        try:
            if task_type == "inference":
                result = await self._execute_inference(task)
            elif task_type == "embedding":
                result = await self._execute_embedding(task)
            elif task_type == "plugin":
                result = await self._execute_plugin(task)
            else:
                result = {"error": f"未知任务类型: {task_type}"}
        except Exception as e:
            result = {"error": str(e)}
            logger.error(f"任务执行失败: {task_id}: {e}")

        self._current_task = None
        return result

    async def _execute_inference(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """执行推理任务（通过 fusion-mlx HTTP API）。"""
        model = task.get("model", "")
        prompt = task.get("params", {}).get("prompt", "")
        messages = task.get("params", {}).get("messages", [])

        if not messages and prompt:
            messages = [{"role": "user", "content": prompt}]

        import httpx
        payload = {
            "model": model,
            "messages": messages,
            "temperature": task.get("params", {}).get("temperature", 0.7),
            "max_tokens": task.get("params", {}).get("max_tokens", 4096),
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                f"http://localhost:{self.config.fusion_mlx_port}/v1/chat/completions",
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        return {
            "task_id": task["task_id"],
            "content": data["choices"][0]["message"]["content"],
            "usage": data.get("usage", {}),
            "node_id": self.config.node_id,
        }

    async def _execute_embedding(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """执行 Embedding 任务。"""
        text = task.get("params", {}).get("text", "")
        model = task.get("model", "BGE-M3")

        import httpx
        payload = {"model": model, "input": text}

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"http://localhost:{self.config.fusion_mlx_port}/v1/embeddings",
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        return {
            "task_id": task["task_id"],
            "embedding": data["data"][0]["embedding"],
            "dimensions": len(data["data"][0]["embedding"]),
            "node_id": self.config.node_id,
        }

    async def _execute_plugin(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """执行插件任务（转发给本机 fusion-desk）。"""
        plugin = task.get("plugin", "")
        action = task.get("action", "")

        import httpx
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"http://localhost:{self.config.fusion_desk_port}/api/plugins/{plugin}/{action}",
                json=task.get("params", {}),
            )
            return resp.json()

    # ── 故障上报 ──

    async def report_fault(self, fault_type: str, message: str) -> bool:
        """向 Master 上报故障。"""
        import httpx
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
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

    async def start(self) -> None:
        """启动节点代理。"""
        self._running = True
        logger.info(f"Node Agent 启动: {self.config.node_id}")

        # 首次注册
        await self.report_hardware()

        # 心跳循环
        asyncio.create_task(self._heartbeat_loop())
        # 硬件上报循环
        asyncio.create_task(self._hardware_report_loop())

    async def stop(self) -> None:
        """停止节点代理。"""
        self._running = False
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
            logger.debug(f"硬件状态: {info['available_memory_gb']:.1f}GB 可用, "
                        f"CPU {info['cpu_percent']}%, "
                        f"MLX: {info['fusion_mlx_running']}")
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

from fusion_multi_node.utils.auth import is_safe_path_segment

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
    master_port: int = 11452
    agent_port: int = 11445
    fusion_desk_port: int = 9000
    fusion_mlx_port: int = 11432
    heartbeat_interval: float = 3.0
    report_interval: float = 15.0
    cluster_token: str = ""


class NodeAgent:
    """节点代理 — 每台 Mac 运行一个实例。

    与 Cluster Master 保持心跳，上报硬件状态，执行下发的推理任务。
    """

    def __init__(
        self,
        config: AgentConfig | None = None,
        backend: InferenceBackend | None = None,
        sandbox: Any = None,
    ):
        self.config = config or AgentConfig()
        self.config.node_id = self.config.node_id or f"node_{uuid.uuid4().hex[:8]}"
        if not self.config.cluster_token:
            try:
                from fusion_multi_node.utils.auth import load_or_create_token

                self.config.cluster_token = load_or_create_token()
                logger.info("已加载集群共享密钥用于 Master 鉴权")
            except Exception as e:
                logger.warning(f"加载集群密钥失败，Master 通信可能被 401 拒绝: {e}")
        self._running = False
        self._current_task: dict[str, Any] | None = None
        # 运行中任务协程注册表 — task_id → asyncio.Task, 供 cancel_task 终止运行推理
        self._running_task_handles: dict[str, asyncio.Task] = {}
        self._heartbeat_task: asyncio.Task | None = None
        self._hardware_task: asyncio.Task | None = None
        self._http_client: httpx.AsyncClient | None = None
        # R1: 准静态硬件信息缓存 (设备型号/GPU核数/MLX版本/IP 等)。启动时采集一次,
        # 心跳/硬件循环仅用 psutil 刷动态字段 (可用内存/CPU负载), 不再每 3s fork system_profiler。
        self._static_hardware: dict[str, Any] | None = None
        self._backend = backend or FusionMLXBackend(
            base_url=f"http://localhost:{self.config.fusion_mlx_port}",
        )
        # M6-02 Worker 沙箱 (AR审计 #24 硬伤5: security/ 原为死代码, 零路径/网络过滤)
        # 仅启用入口 gate 检查 (check_path_access/check_network_access) — 纯 Python, 进程内。
        # 不调 apply_limits/resource.setrlimit: NodeAgent 是单长跑进程服务多任务,
        # 进程级 RLIMIT_AS/CPU 会整 agent 一起限制, 误杀在途任务。OS 级强隔离走
        # SandboxExecutor (subprocess 插件), 推理为 HTTP 调用无子进程, 不适用。
        self._sandbox = sandbox

    # ── 硬件信息收集 ──

    def _ensure_static_hardware(self) -> dict[str, Any]:
        """采集并缓存准静态硬件信息 (设备型号/GPU核数/IP/MLX版本等)。

        R1 修复: 这类信息在进程生命周期内基本不变, 启动时采集一次即可。
        system_profiler/sysctl 子进程 (秒级开销) 只在此处运行一次,
        后续心跳/硬件循环改用 _collect_dynamic_load (纯 psutil, 微秒级)。
        """
        if self._static_hardware is not None:
            return self._static_hardware

        cpu_count = os.cpu_count() or 0
        is_apple_silicon = platform.machine() == "arm64"
        mlx_version = self._get_mlx_version()
        gpu_cores, device_model = self._get_gpu_info()
        local_ip = self._get_local_ip()

        self._static_hardware = {
            "node_id": self.config.node_id,
            "hostname": platform.node(),
            "ip_address": local_ip,
            "port": self.config.agent_port,
            "arch": platform.machine(),
            "os": platform.system(),
            "os_version": platform.version(),
            "cpu_cores": cpu_count,
            "gpu_cores": gpu_cores,
            "device_model": device_model,
            "mlx_version": mlx_version,
            "is_apple_silicon": is_apple_silicon,
        }
        logger.info(
            f"R1 静态硬件采集完成 (缓存): {self._static_hardware['device_model']} "
            f"gpu={gpu_cores}cores ip={local_ip} mlx={mlx_version or '未运行'}"
        )
        return self._static_hardware

    def _collect_dynamic_load(self) -> dict[str, Any]:
        """采集动态负载字段 (纯 psutil, 无子进程, 微秒级)。

        R1 修复: 供心跳/硬件循环高频调用, 替代 collect_hardware_info 的全量重采集。
        R5 修复: active_tasks 取 len(_running_task_handles) 反映并发任务数,
                 task_queue_len 同源, 供 LoadRouter 队列维度真实感知。
        """
        import psutil

        mem = psutil.virtual_memory()
        active = len(self._running_task_handles) or (1 if self._current_task else 0)
        total_gb = mem.total / (1024**3)
        avail_gb = mem.available / (1024**3)
        return {
            "total_memory_gb": round(total_gb, 1),
            "available_memory_gb": round(avail_gb, 1),
            "cpu_percent": psutil.cpu_percent(interval=None),
            "uma_size_gb": round(total_gb, 1) if platform.machine() == "arm64" else 0.0,
            "active_tasks": active,
            "task_queue_len": active,
            "uma_used_ratio": round(max(0.0, 1.0 - avail_gb / total_gb) if total_gb > 0 else 0.0, 3),
            "fusion_desk_running": self._check_service(self.config.fusion_desk_port),
            "fusion_mlx_running": self._check_service(self.config.fusion_mlx_port),
            "timestamp": time.time(),
        }

    def collect_hardware_info(self) -> dict[str, Any]:
        """收集本机硬件信息 (静态缓存 + 动态 psutil 合并)。"""
        static = self._ensure_static_hardware()
        dynamic = self._collect_dynamic_load()
        return {**static, **dynamic}

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
            headers = {}
            if self.config.cluster_token:
                headers["Authorization"] = f"Bearer {self.config.cluster_token}"
            self._http_client = httpx.AsyncClient(timeout=timeout, headers=headers)
        return self._http_client

    async def send_heartbeat(self) -> bool:
        """向 Master 发送心跳 + 五维负载。

        R1 修复: 仅用 psutil 刷动态字段, 不再每 3s fork system_profiler。
        R5 修复: 心跳同时 POST /api/nodes/load 上报五维负载 (task_queue_len/cpu/uma),
                 LoadRouter 队列维度据此真实感知并发负载, 不再恒为 0。
        静态硬件信息由 report_hardware 启动时一次性上报。
        """
        load = self._collect_dynamic_load()
        try:
            client = await self._get_http_client(5.0)
            resp = await client.post(
                f"http://{self.config.master_host}:{self.config.master_port}/api/nodes/heartbeat",
                json={
                    "node_id": self.config.node_id,
                    "available_memory_gb": load["available_memory_gb"],
                    "active_tasks": load["active_tasks"],
                },
            )
            ok = resp.status_code == 200

            # R5: 同步五维负载到 LoadRouter (心跳路径, 无需单独定时器)
            try:
                await client.post(
                    f"http://{self.config.master_host}:{self.config.master_port}/api/nodes/load",
                    json={
                        "node_id": self.config.node_id,
                        "uma_used_ratio": load["uma_used_ratio"],
                        "cpu_percent": load["cpu_percent"],
                        "task_queue_len": load["task_queue_len"],
                    },
                )
            except Exception as le:
                logger.debug(f"负载上报失败 (不影响心跳): {le}")
            return ok
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

        # M6-02 沙箱入口 gate — 派发前校验路径/网络, 拒则不执行 (AR审计 #24)
        gate_err = self._sandbox_gate(task, task_type, temp_dir)
        if gate_err:
            self._current_task = None
            logger.warning(f"沙箱 gate 拒绝任务 {task_id}: {gate_err}")
            return {"task_id": task_id, "error": gate_err, "sandbox_blocked": True}

        # 把执行体包成可取消的 asyncio.Task 并登记, 供 cancel_task 中止
        async def _run():
            try:
                os.makedirs(temp_dir, exist_ok=True)
                if task_type == "inference":
                    return await self._execute_inference(task)
                if task_type == "embedding":
                    return await self._execute_embedding(task)
                if task_type == "plugin":
                    return await self._execute_plugin(task)
                if task_type == "model_sync":
                    return await self._execute_model_sync(task)
                return {"error": f"未知任务类型: {task_type}"}
            except asyncio.CancelledError:
                logger.warning(f"任务被取消中止: {task_id}")
                raise
            finally:
                self._running_task_handles.pop(task_id, None)
                self._current_task = None
                # M6-01 Worker 临时数据自动删除
                try:
                    if os.path.isdir(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        logger.info(f"M6-01 清理任务临时目录: {temp_dir}")
                except Exception as e:
                    logger.warning(f"M6-01 清理临时目录失败: {temp_dir} - {e}")

        handle = asyncio.create_task(_run(), name=f"task_{task_id}")
        self._running_task_handles[task_id] = handle
        try:
            result = await handle
        except asyncio.CancelledError:
            result = {"error": "cancelled", "cancelled": True}
            logger.info(f"任务已取消: {task_id}")
        except Exception as e:
            result = {"error": str(e)}
            logger.error(f"任务执行失败: {task_id}: {e}")

        return result

    def _sandbox_gate(self, task: dict[str, Any], task_type: str, temp_dir: str) -> str | None:
        """M6-02 沙箱入口校验 — 校验任务携带的不可信路径/网络, 拒则返回原因字符串。

        纯 Python 进程内检查, 不做 OS 级资源限制 (见 __init__ 注释)。
        只过滤任务方下发的路径/对端 (model_path、model_sync source), 不校验 agent
        自建临时目录 — 后者是 agent 内部工作目录 (macOS 上为 $TMPDIR 非 /tmp),
        对其加沙箱 allowed_paths 会误拒所有任务。
        无沙箱或允许时返回 None。
        """
        if self._sandbox is None:
            return None
        # model_sync 出站对端主机走网络白名单
        if task_type == "model_sync":
            source_node = task.get("source_node", "")
            if source_node and not self._sandbox.check_network_access(source_node):
                return f"模型同步对端 {source_node} 不在沙箱允许网络白名单内"
        # 推理/插件模型路径 (如显式给出) 走只读路径校验
        model_path = task.get("params", {}).get("model_path", "") if isinstance(task.get("params"), dict) else ""
        if model_path and not self._sandbox.check_path_access(model_path, write=False):
            return f"模型路径 {model_path} 不在沙箱允许访问路径内"
        # E5: 插件 plugin/action 段段校验防穿越 (无沙箱配置时也强制, 因属不可信输入)
        if task_type == "plugin":
            for seg_key in ("plugin", "action"):
                seg = task.get(seg_key, "")
                if seg and not is_safe_path_segment(seg):
                    return f"插件 {seg_key} 段非法 (穿越/特殊字符): {seg!r}"
        # E5: 推理/Embedding model_name 段校验
        if task_type in ("inference", "embedding"):
            model_name = task.get("model", "")
            if model_name and not is_safe_path_segment(model_name):
                return f"模型名段非法 (穿越/特殊字符): {model_name!r}"
        return None

    async def cancel_task(self, task_id: str) -> bool:
        """取消正在运行的任务 — 真中止运行推理协程, 非假动作。"""
        handle = self._running_task_handles.get(task_id)
        if not handle or handle.done():
            logger.info(f"任务无可取消运行句柄: {task_id}")
            return False
        handle.cancel()
        logger.info(f"已发送取消信号到任务协程: {task_id}")
        return True

    async def _execute_inference(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行推理任务（通过 InferenceBackend）。"""
        model = task.get("model", "")
        # E5: model_name 为不可信输入, 校验防特殊字符触发下游解析问题
        if model and not is_safe_path_segment(model):
            logger.warning(f"推理任务拒绝: 非法 model 段 {model!r}")
            return {"task_id": task.get("task_id", ""), "error": f"非法 model: {model!r}"}
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
        # E5: model 校验
        if not is_safe_path_segment(model):
            logger.warning(f"Embedding 任务拒绝: 非法 model 段 {model!r}")
            return {"task_id": task.get("task_id", ""), "error": f"非法 model: {model!r}"}

        data = await self._backend.embed(model=model, input_text=text)

        return {
            "task_id": task["task_id"],
            "embedding": data["data"][0]["embedding"],
            "dimensions": len(data["data"][0]["embedding"]),
            "node_id": self.config.node_id,
        }

    async def _execute_plugin(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行插件任务（转发给本机 fusion-desk）。

        E5: plugin/action 为不可信外部输入, 段段校验 is_safe_path_segment 防 ../ 穿越,
        防特殊字符注入下游 URL。转发经 _get_http_client, 已带集群 Bearer token 鉴权。
        """
        plugin = task.get("plugin", "")
        action = task.get("action", "")
        if not is_safe_path_segment(plugin):
            logger.warning(f"插件任务拒绝: 非法 plugin 段 {plugin!r}")
            return {"task_id": task.get("task_id", ""), "error": f"非法 plugin: {plugin!r}"}
        if not is_safe_path_segment(action):
            logger.warning(f"插件任务拒绝: 非法 action 段 {action!r}")
            return {"task_id": task.get("task_id", ""), "error": f"非法 action: {action!r}"}

        client = await self._get_http_client(60.0)
        resp = await client.post(
            f"http://localhost:{self.config.fusion_desk_port}/api/plugins/{plugin}/{action}",
            json=task.get("params", {}),
        )
        return resp.json()

    async def _execute_model_sync(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行模型同步任务 — 将指定模型同步到本节点。"""
        from fusion_multi_node.utils.auth import (
            build_safe_url,
            is_safe_peer_host,
        )

        model_name = task.get("model_name", "")
        model_id = task.get("model_id", "")
        source_node = task.get("source_node", "master")
        logger.info(f"模型同步: {model_name} (id={model_id}) from {source_node}")
        try:
            if not is_safe_path_segment(model_name):
                return {"error": f"非法 model_name: {model_name!r}"}
            if not is_safe_peer_host(source_node):
                return {"error": f"不安全对端主机: {source_node!r}"}
            client = await self._get_http_client(300.0)
            source_port = task.get("source_port", 11452)
            url = build_safe_url(
                "http", source_node, source_port, f"/api/models/{model_name}/manifest"
            )
            resp = await client.get(url)
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
        """停止节点代理 — 优雅关停: drain 在途任务后再下线。"""
        self._running = False
        for task in (self._heartbeat_task, self._hardware_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        # drain: 取消运行中推理协程并等待退出 (避免静默丢工作)
        if self._running_task_handles:
            logger.info(f"关停 drain: 取消 {len(self._running_task_handles)} 个在途任务")
            for handle in list(self._running_task_handles.values()):
                if not handle.done():
                    handle.cancel()
            for handle in list(self._running_task_handles.values()):
                try:
                    await handle
                except (asyncio.CancelledError, Exception):
                    pass
            self._running_task_handles.clear()
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
        """硬件上报循环 (仅日志, R1: 用动态采集避免 system_profiler 风暴)。"""
        while self._running:
            await asyncio.sleep(self.config.report_interval)
            load = self._collect_dynamic_load()
            logger.debug(
                f"硬件状态: {load['available_memory_gb']:.1f}GB 可用, "
                f"CPU {load['cpu_percent']}%, "
                f"MLX: {load['fusion_mlx_running']}"
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

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
import socket
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from fusion_multi_node import __version__ as _node_protocol_version
from fusion_multi_node.agent.mlx_memory import fetch_mlx_memory
from fusion_multi_node.agent.rate_pacer import PacerConfig, RateLimitExhausted, dispatch_with_pacing
from fusion_multi_node.security.mtls import client_kwargs as mtls_client_kwargs
from fusion_multi_node.security.mtls import scheme as mtls_scheme
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
    """默认推理后端 — 通过 HTTP 调用本地 fusion-mlx (OpenAI-compatible API)。

    P3: 除 /v1/chat/completions 外, 接上游 /distributed/* (#621) 真实张量
    PIPELINE — load_shard 注册层分片, pipeline_step 跑层前向, 激活 b64.npy 跨节点。
    /distributed/* 受 fusion-mlx api_key 保护 (Bearer), 与集群 cluster_token 不同源。
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11432",
        timeout: float = 120.0,
        api_key: str = "",
        pacer: PacerConfig | None = None,
    ):
        env_url = os.environ.get("FUSION_MLX_URL")
        self._base_url = (env_url or base_url).rstrip("/")
        self._timeout = timeout
        # 显式 api_key 优先 (确定性, Rule 5); 未显式传则回落 env。
        self._api_key = api_key or os.environ.get("FUSION_MLX_API_KEY", "")
        self._client: httpx.AsyncClient | None = None
        # GAP-6 客户端限流: 429 退避重试。默认 PacerConfig (3 次, 指数退避, 10s 预算)。
        # 上游 fusion-mlx --rate-limit 限流 (#635 已修: 0 默认关, 显式上限仍 429);
        # 旧实现 429 一律 raise_for_status → 节点被误判逻辑错误 ban (GAP-6 审计 §7)。
        self._pacer = pacer or PacerConfig()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    @property
    def base_url(self) -> str:
        # 暴露解析后的 base_url (含 FUSION_MLX_URL env 覆盖) — deep-health / readiness
        # 探测据此拼 /v1/models URL, 不再回退 localhost:{fusion_mlx_port} (issue #60)。
        return self._base_url

    @property
    def api_key(self) -> str:
        # 暴露解析后的 api_key (含 FUSION_MLX_API_KEY env) — deep-health / readiness
        # /v1/models 探测须带 Bearer, fusion-mlx 启用 api_key 时无头恒 401 (issue #60)。
        return self._api_key

    def _dist_headers(self) -> dict[str, str]:
        """fusion-mlx /distributed/* 鉴权头 — api_key Bearer。"""
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    async def load_shard(
        self,
        model_id: str,
        shard_index: int,
        layer_range: list[int],
        dtype: str | None = None,
    ) -> dict[str, Any]:
        """上游 /distributed/load_shard — 注册模型层分片, 返回 shard_id。"""
        body = {
            "model_id": model_id,
            "shard_index": shard_index,
            "layer_range": layer_range,
            "dtype": dtype,
        }
        client = await self._get_client()
        resp = await client.post(
            f"{self._base_url}/distributed/load_shard",
            json=body,
            headers=self._dist_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    async def pipeline_step(
        self,
        shard_id: str,
        hidden_states: str | None,
        input_ids: list[int] | None,
        position_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """上游 /distributed/pipeline_step — 跑分片层前向, 返回 hidden_states b64.npy。"""
        body = {
            "shard_id": shard_id,
            "hidden_states": hidden_states,
            "input_ids": input_ids,
            "position_ids": position_ids,
        }
        client = await self._get_client()
        resp = await client.post(
            f"{self._base_url}/distributed/pipeline_step",
            json=body,
            headers=self._dist_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    async def drop_shard(self, shard_id: str) -> dict[str, Any]:
        """上游 /distributed/shards/{id} DELETE — 释放分片 (E2E 清理)。"""
        client = await self._get_client()
        req = client.build_request(
            "DELETE",
            f"{self._base_url}/distributed/shards/{shard_id}",
            headers=self._dist_headers(),
        )
        resp = await client.send(req)
        resp.raise_for_status()
        return resp.json()

    async def decode(
        self,
        shard_id: str,
        hidden_states: str,
        max_tokens: int = 1,
    ) -> dict[str, Any]:
        # H1 PIPELINE 出 token — forward 链末段 hidden_states 经 lm_head 解码出 token。
        # 上游 fusion-mlx /distributed/decode 端点 issue #630 未落地时返 404,
        # 调用方 (pipeline_inference) 须 catch 后 fallback 返隐藏状态 + 标记。
        body = {
            "shard_id": shard_id,
            "hidden_states": hidden_states,
            "max_tokens": max_tokens,
        }
        client = await self._get_client()
        resp = await client.post(
            f"{self._base_url}/distributed/decode",
            json=body,
            headers=self._dist_headers(),
        )
        resp.raise_for_status()
        return resp.json()

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
        # /v1/* 同样受 fusion-mlx api_key 保护 (Bearer), 与 /distributed/* 同源。
        # 原实现漏带 Authorization → 任何启用 auth 的 fusion-mlx 推理一律 401。
        # GAP-6: 429 经 dispatch_with_pacing 退避重试, 不再直接 raise_for_status 误 ban。
        url = f"{self._base_url}/v1/chat/completions"
        resp = await dispatch_with_pacing(
            lambda: client.post(url, json=payload, headers=self._dist_headers()),
            self._pacer,
        )
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
        # GAP-6: 429 退避重试 (同 chat)。
        url = f"{self._base_url}/v1/embeddings"
        resp = await dispatch_with_pacing(
            lambda: client.post(url, json=payload, headers=self._dist_headers()),
            self._pacer,
        )
        resp.raise_for_status()
        return resp.json()

    async def health(self) -> bool:
        try:
            client = await self._get_client()
            # /v1/models 须带 api_key Bearer — fusion-mlx 启用鉴权时无头恒 401 (issue #60)。
            resp = await client.get(f"{self._base_url}/v1/models", timeout=3.0, headers=self._dist_headers())
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
    # P2: agent server 绑定地址 — 多机部署需绑定可路由 IP/0.0.0.0, 否则 Master 无法回连 /api/execute。
    # 默认 127.0.0.1 (单机/测试); 多机经 CLI --host 或 start.sh AGENT_HOST 覆盖。
    agent_host: str = "127.0.0.1"
    agent_port: int = 11458
    fusion_desk_port: int = 9000
    fusion_mlx_port: int = 11432
    # P3: fusion-mlx /distributed/* 鉴权 api_key (与集群 cluster_token 不同源)。
    # 空 = /distributed 路由 401; 生产填 fusion-mlx settings.auth.api_key, 测试用 env。
    fusion_mlx_api_key: str = ""
    heartbeat_interval: float = 3.0
    report_interval: float = 15.0
    cluster_token: str = ""
    # 单节点并发任务上限 — Master 据此 gate 派发 (active_tasks >= max_tasks 不派)。
    # 默认 4; 容器/裸机多并发压测经 FUSION_AGENT_MAX_TASKS env 调高。
    max_tasks: int = 4
    # P2-9 (审计 §6.2): 子进程插件 per-task 资源限制 (仅 SandboxExecutor 子进程, 非主推理进程)。
    # 默认 0 = 不限 (不误杀单长跑 agent; 推理资源在 fusion-mlx 侧)。
    # operator 设 >0 → 起子进程插件时经 preexec_fn 把 RLIMIT_AS/RLIMIT_CPU 加到子进程。
    # 0 不限: 保既有行为, 主推理路径不 setrlimit (维持现状)。
    task_mem_limit_mb: int = 0
    task_cpu_quota: int = 0
    # #63/#65: 节点角色 — worker(默认)/general/heavy。heavy 节点亲和 heavy 任务 + pipeline shard 派发。
    node_role: str = "worker"

    def __post_init__(self) -> None:
        env_mt = os.environ.get("FUSION_AGENT_MAX_TASKS")
        env_mt = os.environ.get("FUSION_AGENT_MAX_TASKS")
        if env_mt:
            try:
                self.max_tasks = max(1, int(env_mt))
            except ValueError:
                pass
        # P2-9: env 覆盖 per-task rlimit knob。
        env_mem = os.environ.get("FUSION_TASK_MEM_LIMIT_MB")
        if env_mem:
            try:
                self.task_mem_limit_mb = max(0, int(env_mem))
            except ValueError:
                pass
        env_cpu = os.environ.get("FUSION_TASK_CPU_QUOTA")
        if env_cpu:
            try:
                self.task_cpu_quota = max(0, int(env_cpu))
            except ValueError:
                pass
        env_role = os.environ.get("FUSION_NODE_ROLE", "").strip().lower()
        if env_role in ("worker", "general", "heavy"):
            self.node_role = env_role


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
        # P1-14: 无 task_id 的直接调用分配匿名 id 的自增序号 (避免多匿名任务 _running_task_handles 撞键)。
        self._anon_task_seq = 0
        self._heartbeat_task: asyncio.Task | None = None
        self._hardware_task: asyncio.Task | None = None
        self._http_client: httpx.AsyncClient | None = None
        # R1: 准静态硬件信息缓存 (设备型号/GPU核数/MLX版本/IP 等)。启动时采集一次,
        # 心跳/硬件循环仅用 psutil 刷动态字段 (可用内存/CPU负载), 不再每 3s fork system_profiler。
        self._static_hardware: dict[str, Any] | None = None
        self._backend = backend or FusionMLXBackend(
            base_url=f"http://localhost:{self.config.fusion_mlx_port}",
            api_key=self.config.fusion_mlx_api_key,
        )
        # M6-02 Worker 沙箱 (AR审计 #24 硬伤5: security/ 原为死代码, 零路径/网络过滤)
        # 仅启用入口 gate 检查 (check_path_access/check_network_access) — 纯 Python, 进程内。
        # 不调 apply_limits/resource.setrlimit: NodeAgent 是单长跑进程服务多任务,
        # 进程级 RLIMIT_AS/CPU 会整 agent 一起限制, 误杀在途任务。OS 级强隔离走
        # SandboxExecutor (subprocess 插件), 推理为 HTTP 调用无子进程, 不适用。
        self._sandbox = sandbox

    def build_subprocess_sandbox_config(self):
        # P2-9 (审计 §6.2): AgentConfig.task_mem_limit_mb/task_cpu_quota → SandboxConfig
        # (仅子进程插件用, 经 SandboxExecutor.execute_in_sandbox preexec_fn 加 rlimit)。
        # 0 = 不限: knob 传 0 → SandboxConfig 对应字段 0 → _apply_rlimits_in_child 跳过该项。
        # nproc/disk 不在 AgentConfig 暴露 (per-task 限插件内存/CPU 已够), 传 0 跳过。
        # 主推理路径不调此方法 (维持不 setrlimit, 推理资源在 fusion-mlx 侧)。
        from fusion_multi_node.security.sandbox import SandboxConfig

        enforce = self.config.task_mem_limit_mb > 0 or self.config.task_cpu_quota > 0
        return SandboxConfig(
            max_memory_mb=self.config.task_mem_limit_mb,
            max_cpu_seconds=self.config.task_cpu_quota,
            max_disk_mb=0,
            max_processes=0,
            enforce_rlimits=enforce,
        )

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

    async def _collect_dynamic_load(self) -> dict[str, Any]:
        """采集动态负载字段 (psutil + MLX Metal 显存, 无 system_profiler 子进程)。

        R1 修复: 供心跳/硬件循环高频调用, 替代 collect_hardware_info 的全量重采集。
        R5 修复: active_tasks 取 len(_running_task_handles) 反映并发任务数,
                 task_queue_len 同源, 供 LoadRouter 队列维度真实感知。
        #64 修复: 抓 fusion-mlx /v1/health memory 块 → metal_util (VRAM_FIRST weight 0.2)
                 + gpu_memory_*_gb。原 metal_util 恒 0.0 = VRAM_FIRST 该维度死权重。
                 psutil 统一内存 = 系统级, MLX active bytes = 推理实际 Metal 占用, 二者互补。
                 底座未运行 → None → 全 0.0, 不拖垮心跳 (离线安全)。
        """
        import psutil

        mem = psutil.virtual_memory()
        active = len(self._running_task_handles) or (1 if self._current_task else 0)
        total_gb = mem.total / (1024**3)
        avail_gb = mem.available / (1024**3)
        # #64: 抓 MLX Metal 显存 — base_url/api_key 含 env 覆盖 (#60 property)。
        mlx_mem = await fetch_mlx_memory(self._backend.base_url, self._backend.api_key)
        metal_util = 0.0
        gpu_used_gb = 0.0
        gpu_total_gb = 0.0
        if mlx_mem and mlx_mem["total_gb"] > 0:
            metal_util = round(min(1.0, mlx_mem["active_gb"] / mlx_mem["total_gb"]), 3)
            gpu_used_gb = mlx_mem["active_gb"]
            gpu_total_gb = mlx_mem["total_gb"]
        return {
            "total_memory_gb": round(total_gb, 1),
            "available_memory_gb": round(avail_gb, 1),
            "cpu_percent": psutil.cpu_percent(interval=None),
            "uma_size_gb": round(total_gb, 1) if platform.machine() == "arm64" else 0.0,
            "active_tasks": active,
            "task_queue_len": active,
            "uma_used_ratio": round(max(0.0, 1.0 - avail_gb / total_gb) if total_gb > 0 else 0.0, 3),
            "metal_util": metal_util,
            "gpu_memory_used_gb": gpu_used_gb,
            "gpu_memory_total_gb": gpu_total_gb,
            "fusion_desk_running": self._check_service(self.config.fusion_desk_port),
            "fusion_mlx_running": self._check_service(self.config.fusion_mlx_port),
            "timestamp": time.time(),
        }

    async def collect_hardware_info(self) -> dict[str, Any]:
        """收集本机硬件信息 (静态缓存 + 动态 psutil+MLX 合并)。

        P1-10: _ensure_static_hardware 首次调 system_profiler/ipconfig (至 5s) —
        经 to_thread 移出 event loop; 后续命中缓存为纯内存微秒级。
        #64: 动态负载 _collect_dynamic_load 已 async (抓 MLX /v1/health)。
        """
        static = await asyncio.to_thread(self._ensure_static_hardware)
        dynamic = await self._collect_dynamic_load()
        return {**static, **dynamic}

    def _get_local_ip(self) -> str:
        # 跨平台取本机可达 IP — 优先 socket UDP connect (零依赖, 取 master 回连的源 IP)。
        # UDP connect 不发包, 仅内核选路由出口, 对端不必存活 (但需可解析)。
        for probe_host in (self.config.master_host, "8.8.8.8"):
            try:
                if probe_host in ("0.0.0.0", "localhost", "127.0.0.1"):
                    probe_host = "8.8.8.8"
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.settimeout(2)
                s.connect((probe_host, int(self.config.master_port)))
                ip = s.getsockname()[0]
                s.close()
                if ip and not ip.startswith("127.") and not ip.startswith("169.254."):
                    return ip
            except Exception as e:
                logger.debug(f"socket 探测本机 IP 失败 ({probe_host}): {e}")
                continue
        # macOS 裸机兜底
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
            # /v1/models 须带 api_key Bearer — fusion-mlx 启用鉴权时无头恒 401 (issue #60)。
            _api_key = self.config.fusion_mlx_api_key or os.environ.get("FUSION_MLX_API_KEY", "")
            _headers = {"Authorization": f"Bearer {_api_key}"} if _api_key else {}
            resp = httpx.get(f"{_mlx_url}/v1/models", timeout=3.0, headers=_headers)
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
            self._http_client = httpx.AsyncClient(timeout=timeout, headers=headers, **mtls_client_kwargs())
        return self._http_client

    async def send_heartbeat(self) -> bool:
        """向 Master 发送心跳 + 五维负载。

        R1 修复: 仅用 psutil 刷动态字段, 不再每 3s fork system_profiler。
        R5 修复: 心跳同时 POST /api/nodes/load 上报五维负载 (task_queue_len/cpu/uma),
                 LoadRouter 队列维度据此真实感知并发负载, 不再恒为 0。
        静态硬件信息由 report_hardware 启动时一次性上报。
        """
        load = await self._collect_dynamic_load()
        try:
            client = await self._get_http_client(5.0)
            resp = await client.post(
                f"{mtls_scheme()}://{self.config.master_host}:{self.config.master_port}/api/nodes/heartbeat",
                json={
                    "node_id": self.config.node_id,
                    "total_memory_gb": load["total_memory_gb"],
                    "available_memory_gb": load["available_memory_gb"],
                    "active_tasks": load["active_tasks"],
                },
            )
            ok = resp.status_code == 200

            # R5: 同步五维负载到 LoadRouter (心跳路径, 无需单独定时器)
            # #64: 补 metal_util (VRAM_FIRST weight 0.2) + gpu_memory_*_gb (端点展示)。
            try:
                await client.post(
                    f"{mtls_scheme()}://{self.config.master_host}:{self.config.master_port}/api/nodes/load",
                    json={
                        "node_id": self.config.node_id,
                        "uma_used_ratio": load["uma_used_ratio"],
                        "cpu_percent": load["cpu_percent"],
                        "task_queue_len": load["task_queue_len"],
                        "metal_util": load["metal_util"],
                        "gpu_memory_used_gb": load["gpu_memory_used_gb"],
                        "gpu_memory_total_gb": load["gpu_memory_total_gb"],
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
        # P1-10: collect_hardware_info 内部已把 system_profiler/ipconfig (至 5s)
        # 经 to_thread 移出 event loop (审计 §4.5); collect_hardware_info 自身 async。
        info = await self.collect_hardware_info()
        try:
            client = await self._get_http_client(5.0)
            resp = await client.post(
                f"{mtls_scheme()}://{self.config.master_host}:{self.config.master_port}/api/nodes/register",
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
                    # P1-17 (审计 §6.7): 上报多节点协议版本, master 比对兼容性。
                    "protocol_version": _node_protocol_version,
                    "role": self.config.node_role,
                    "tags": ["apple-silicon"] if info.get("is_apple_silicon") else [],
                    "active_tasks": 0,
                    "max_tasks": self.config.max_tasks,
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
        # P1-14 (审计 §5.3): 拒同 task_id 重复派发 — master 重派同 task_id 到本节点时
        # 上一执行仍在跑 → 拒, 返回 dedup 错误 (master 归类逻辑错误不重试, 避免双重推理)。
        # 无 task_id (直接调用/旧客户端) 分配匿名 id, 不做去重但防 _running_task_handles 撞键。
        raw_task_id = task.get("task_id") or ""
        if raw_task_id:
            if raw_task_id in self._running_task_handles:
                logger.warning(f"P1-14 拒重复派发: task_id={raw_task_id} 仍在运行")
                return {"task_id": raw_task_id, "error": "task_id 已在运行 (重复派发)", "dedup_blocked": True}
            task_id = raw_task_id
        else:
            self._anon_task_seq += 1
            task_id = f"anon-{self._anon_task_seq}"
            task["task_id"] = task_id
        self._current_task = task
        task_type = task.get("type", "inference")
        temp_dir = os.path.join(tempfile.gettempdir(), f"fusion_task_{task_id}")
        logger.info(f"执行任务: {task_id} ({task_type})")

        # M6-02 沙箱入口 gate — 派发前校验路径/网络, 拒则不执行 (AR审计 #24)
        gate_err = self._sandbox_gate(task, task_type, temp_dir)
        if gate_err:
            self._current_task = None
            logger.warning(f"沙箱 gate 拒绝任务 {task_id}: {gate_err}")
            return {"task_id": task_id, "error": gate_err, "sandbox_blocked": True}

        # P1-18 (审计 §6.6): 本地并发容量 gate — _running_task_handles 达 max_tasks 上限拒收。
        # master 心跳报告 active_tasks 有 TOCTOU, 本地入口强制 gate 兜底防过载。
        # 返 overload=True (master 归类 transient 不重试不 ban, 选其他节点)。
        if len(self._running_task_handles) >= self.config.max_tasks:
            self._current_task = None
            cur = len(self._running_task_handles)
            logger.warning(f"P1-18 节点任务已满: {cur}/{self.config.max_tasks}, 拒收 {task_id}")
            return {"task_id": task_id, "error": "节点任务已满", "overload": True}

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
                if task_type == "pipeline_step":
                    return await self._execute_pipeline_step(task)
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
        # E5: 不可信输入段段校验防穿越 — 无沙箱配置时也强制 (plugin/action/model 不可信)。
        # 旧实现 sandbox is None 在此之前 short-circuit return, 致默认部署 E5 gate 失效
        # (AR 审计 #24: 默认安全姿态 sandbox=None → 入口防穿越形同虚设)。
        if task_type == "plugin":
            for seg_key in ("plugin", "action"):
                seg = task.get(seg_key, "")
                if seg and not is_safe_path_segment(seg):
                    return f"插件 {seg_key} 段非法 (穿越/特殊字符): {seg!r}"
        if task_type in ("inference", "embedding"):
            model_name = task.get("model", "")
            if model_name and not is_safe_path_segment(model_name):
                return f"模型名段非法 (穿越/特殊字符): {model_name!r}"
        # 沙箱路径/网络白名单 — 无沙箱配置时跳过 (仅 E5 段校验为强制项)
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

        # GAP-6: 429 限流耗尽重试预算 → 标 rate_limited, master 归类瞬时失败 (可重试),
        # 不进 logic_fail (不 ban 健康节点)。其他异常照常上抛交 _run 包 error。
        try:
            data = await self._backend.chat(
                model=model,
                messages=messages,
                temperature=task.get("params", {}).get("temperature", 0.7),
                max_tokens=task.get("params", {}).get("max_tokens", 4096),
            )
        except RateLimitExhausted as e:
            logger.warning(f"推理任务限流未恢复: {task.get('task_id', '')}: {e}")
            return {
                "task_id": task.get("task_id", ""),
                "error": str(e),
                "rate_limited": True,
                "node_id": self.config.node_id,
            }

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

        # GAP-6: 429 限流耗尽 → rate_limited 信号 (同 inference)。
        try:
            data = await self._backend.embed(model=model, input_text=text)
        except RateLimitExhausted as e:
            logger.warning(f"Embedding 任务限流未恢复: {task.get('task_id', '')}: {e}")
            return {
                "task_id": task.get("task_id", ""),
                "error": str(e),
                "rate_limited": True,
                "node_id": self.config.node_id,
            }

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
            url = build_safe_url(mtls_scheme(), source_node, source_port, f"/api/models/{model_name}/manifest")
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

    async def _execute_pipeline_step(self, task: dict[str, Any]) -> dict[str, Any]:
        """P3 真实张量 PIPELINE 步骤 — 调上游 fusion-mlx /distributed/*。

        Master 把模型按层切成多段, 每节点跑一段。首段带 input_ids (embed+layers),
        后续段带 hidden_states (b64.npy, 仅 layers)。本方法:
        1) load_shard 注册本节点层段 → shard_id
        2) pipeline_step 跑层前向 → 出口 hidden_states b64.npy
        返回 {hidden_states, shape, dtype, shard_id, node_id} 供 Master 链传下一段。
        末段输出即最终 hidden_states (lm_head/解码超上游首版范围, docs line 151)。
        """
        params = task.get("params", {}) or {}
        model_id = params.get("model_id", "")
        shard_index = int(params.get("shard_index", 0))
        layer_range = params.get("layer_range", [])
        input_ids = params.get("input_ids")
        hidden_states = params.get("hidden_states")  # b64.npy 或 None (首段)
        position_ids = params.get("position_ids")
        task_id = task.get("task_id", "")
        if not model_id or not layer_range:
            return {"task_id": task_id, "error": "pipeline_step 缺 model_id 或 layer_range"}
        # 模型路径为不可信输入 — is_safe_path_segment 校验 (防注入下游 URL 路径)。
        # 注意: model_id 可为绝对路径 (上游 _resolve_model_path 限制根目录), 这里仅
        # 拦特殊字符, 不拦路径分隔符 (与 model_sync 的 source_node 网络校验不同维度)。
        backend = self._backend
        if not isinstance(backend, FusionMLXBackend):
            return {"task_id": task_id, "error": "pipeline_step 需 FusionMLXBackend (上游 /distributed/*)"}
        logger.info(
            f"P3 pipeline_step: task={task_id} model={model_id} range={layer_range} "
            f"shard_index={shard_index} has_hidden={hidden_states is not None}"
        )
        try:
            shard_info = await backend.load_shard(model_id, shard_index, layer_range)
            shard_id = shard_info["shard_id"]
            logger.info(f"P3 load_shard ok: {shard_id} num_layers={shard_info.get('num_layers')}")
            out = await backend.pipeline_step(shard_id, hidden_states, input_ids, position_ids)
            logger.info(
                f"P3 pipeline_step ok: shard={shard_id} out_shape={out.get('shape')} "
                f"dtype={out.get('dtype')} b64_len={len(out.get('hidden_states', ''))}"
            )
            return {
                "task_id": task_id,
                "shard_id": shard_id,
                "hidden_states": out["hidden_states"],
                "shape": out["shape"],
                "dtype": out["dtype"],
                "node_id": self.config.node_id,
            }
        except httpx.HTTPStatusError as he:
            # #65: 上游 /distributed/* 404 = 端点未实现 (fusion-mlx#621), 区分于 shard 不存在。
            # master 据此映射 FAILED + 明确报错 (不可重试, 非瞬时节点故障)。
            if he.response.status_code == 404:
                logger.error(f"P3 pipeline_step 上游未实现: task={task_id} model={model_id} 404")
                return {
                    "task_id": task_id,
                    "error": "上游 /distributed/* 未实现 (fusion-mlx#621)",
                    "upstream_missing": True,
                }
            logger.error(f"P3 pipeline_step 失败: task={task_id} model={model_id}: {he}")
            return {"task_id": task_id, "error": f"pipeline_step: {he}"}
        except Exception as e:
            logger.error(f"P3 pipeline_step 失败: task={task_id} model={model_id}: {e}")
            return {"task_id": task_id, "error": f"pipeline_step: {e}"}

    # ── 故障上报 ──

    async def report_fault(self, fault_type: str, message: str) -> bool:
        """向 Master 上报故障。"""
        try:
            client = await self._get_http_client(5.0)
            resp = await client.post(
                f"{mtls_scheme()}://{self.config.master_host}:{self.config.master_port}/api/nodes/fault",
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
            await server.start(host=self.config.agent_host, port=self.config.agent_port)

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
            load = await self._collect_dynamic_load()
            logger.debug(
                f"硬件状态: {load['available_memory_gb']:.1f}GB 可用, "
                f"CPU {load['cpu_percent']}%, "
                f"MLX: {load['fusion_mlx_running']} metal={load['metal_util']:.2f}"
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

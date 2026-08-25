"""P0-C 跨机真网络 E2E — 真 bind 端口 + 真 HTTP 跨进程 (非 ASGITransport)。

真链: MasterServer 真端口 uvicorn.serve ←→ AgentServer 真端口 uvicorn.serve
两进程级 HTTP (真实 TCP socket), 经 httpx 真网络回连。验:
- agent 真注册到 master (经 HTTP /api/nodes/register)
- 真任务派发跨 HTTP (master → agent /api/execute)
- agent 掉线 → master 标 OFFLINE → 重启重连 → 恢复 ONLINE + 可派

非容器: 进程内起真 uvicorn (真端口真 socket), 跨 HTTP 边界通信。
容器 E2E (docker-compose) 见 TestContainerE2E (同文件), skip-gate docker 可用。
免真模型 — FakeBackend (测网络/调度, 非推理正确性)。
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from unittest.mock import patch

import httpx
import pytest

from fusion_multi_node.agent import AgentConfig, NodeAgent
from fusion_multi_node.agent.node_agent import InferenceBackend
from fusion_multi_node.master import ClusterMaster
from fusion_multi_node.server.agent_server import AgentServer
from fusion_multi_node.server.master_server import MasterServer

logger = logging.getLogger(__name__)

TEST_TOKEN = "realnet-token"


class FakeBackend(InferenceBackend):
    async def chat(self, model, messages, temperature=0.7, max_tokens=4096, **kwargs):
        return {"choices": [{"message": {"content": "ok"}}], "usage": {"total_tokens": 1}}

    async def embed(self, model, input_text, **kwargs):
        return {"data": [{"embedding": [0.1]}]}

    async def health(self):
        return True


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _wait_health(url: str, headers: dict, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=2.0) as c:
        while time.monotonic() < deadline:
            try:
                r = await c.get(url, headers=headers)
                if r.status_code == 200:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.3)
    return False


class _Cluster:
    """真端口 master + N agent, 各自 uvicorn.serve 真监听。"""

    def __init__(self):
        self.master_port = _free_port()
        self.master: ClusterMaster = ClusterMaster(heartbeat_timeout=6.0)
        self.master._dispatch_token = TEST_TOKEN
        self.mserver = MasterServer(master=self.master, shared_token=TEST_TOKEN)
        self.mserver._approval_manager = None
        self.agents: list[NodeAgent] = []
        self.aservers: list[AgentServer] = []
        self._serve_tasks: list[asyncio.Task] = []

    def _add_agent(self, node_id: str) -> NodeAgent:
        aport = _free_port()
        cfg = AgentConfig(
            node_id=node_id,
            master_host="127.0.0.1",
            master_port=self.master_port,
            agent_host="127.0.0.1",
            agent_port=aport,
            cluster_token=TEST_TOKEN,
            heartbeat_interval=1.0,
            max_tasks=4,
        )
        agent = NodeAgent(cfg, backend=FakeBackend())
        agent._dispatch_token = TEST_TOKEN
        # 测试仅绑 127.0.0.1 → 覆盖注册 IP 为 127.0.0.1 (否则上报真 LAN IP, master 回连失败)。
        agent._get_local_ip = lambda: "127.0.0.1"
        aserver = AgentServer(agent=agent, shared_token=TEST_TOKEN)
        aserver._approval_manager = None
        aserver._rate_limiter = None
        self.agents.append(agent)
        self.aservers.append(aserver)
        return agent

    async def _serve(self) -> None:
        # 起 master 后台循环 (健康检查/超时标 OFFLINE/重试) — MasterServer.start 只起 uvicorn。
        await self.master.start(with_server=False, with_mdns=False)
        mtask = asyncio.create_task(self.mserver.start(host="127.0.0.1", port=self.master_port))
        self._serve_tasks.append(mtask)
        ok = await _wait_health(
            f"http://127.0.0.1:{self.master_port}/api/health", {"Authorization": f"Bearer {TEST_TOKEN}"}
        )
        assert ok, "master 真端口未就绪"

        for aserver, agent in zip(self.aservers, self.agents):
            atask = asyncio.create_task(aserver.start(host="127.0.0.1", port=agent.config.agent_port))
            self._serve_tasks.append(atask)
            ok = await _wait_health(
                f"http://127.0.0.1:{agent.config.agent_port}/api/health",
                {"Authorization": f"Bearer {TEST_TOKEN}"},
            )
            assert ok, f"agent {agent.config.node_id} 真端口未就绪"
            # 起 agent 注册 + 心跳循环 (server 已手动起, with_server=False 避免重复绑端口)。
            agent._running = True
            await agent.report_hardware()
            agent._heartbeat_task = asyncio.create_task(agent._heartbeat_loop())

    async def start(self, n_agents: int = 2) -> None:
        for i in range(n_agents):
            self._add_agent(f"agent-{i}")
        await self._serve()
        # agent 经 HTTP 自注册到 master; 等注册 + 心跳生效。
        await self._wait_online(n_agents)

    async def _wait_online(self, expect: int, timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            online = [n for n in self.master.nodes.values() if n.status.value == "online"]
            if len(online) >= expect:
                return
            await asyncio.sleep(0.3)
        raise AssertionError(f"master 注册节点不足: 期望 {expect}, 实际 {len(self.master.nodes)}")

    async def stop(self) -> None:
        for aserver in self.aservers:
            await aserver.stop()
        await self.mserver.stop()
        for t in self._serve_tasks:
            t.cancel()
        for t in self._serve_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


@pytest.fixture
async def cluster():
    # 测试仅绑 127.0.0.1; 产品 SSRF 防护拒环回 — 测试放行环回 (测网络/调度, 非 SSRF)。
    with patch("fusion_multi_node.utils.auth.is_safe_peer_host", return_value=True), patch(
        "fusion_multi_node.master.cluster_master.is_safe_peer_host", return_value=True
    ):
        c = _Cluster()
        await c.start(n_agents=2)
        yield c
        await c.stop()


class TestRealNetworkE2E:
    """真端口 + 真 HTTP 跨进程 E2E。"""

    @pytest.mark.asyncio
    async def test_real_register_cross_http(self, cluster):
        # agent 经真 HTTP 注册到 master (非 ASGITransport, 真 socket)。
        assert len(cluster.master.nodes) == 2, "两 agent 须经真 HTTP 注册"
        for nid in ("agent-0", "agent-1"):
            assert nid in cluster.master.nodes
            assert cluster.master.nodes[nid].status.value == "online"

    @pytest.mark.asyncio
    async def test_real_task_dispatch_cross_http(self, cluster):
        # 提交任务经 master:11452 真 HTTP → 派发到 agent 真 HTTP /api/execute。
        async with httpx.AsyncClient(timeout=15.0) as c:
            resp = await c.post(
                f"http://127.0.0.1:{cluster.master_port}/api/tasks/submit",
                json={
                    "name": "realnet-task",
                    "mode": "data",
                    "model_name": "qwen-1b",
                    "task_type": "inference",
                    "prompt": "hi",
                    "max_tokens": 8,
                },
                headers={"Authorization": f"Bearer {TEST_TOKEN}"},
            )
        assert resp.status_code in (200, 202), f"提交失败: {resp.status_code} {resp.text}"
        tid = resp.json()["task_id"]

        # 等派发 + FakeBackend 完成。
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            t = cluster.master.tasks.get(tid)
            if t and t.status.value in ("completed", "failed"):
                break
            await asyncio.sleep(0.3)
        t = cluster.master.tasks.get(tid)
        assert t is not None, "任务未注册到 master"
        assert t.status.value == "completed", f"真 HTTP 派发未完成: {t.status.value} {t.error}"

    @pytest.mark.asyncio
    async def test_node_drop_then_reconnect(self, cluster):
        # 停 agent-0 → master 心跳超时标 OFFLINE → 重启 agent-0 → 重连恢复 ONLINE + 可派。
        dropped = cluster.agents[0]
        dropped_id = dropped.config.node_id
        dropped_port = dropped.config.agent_port

        # 停掉 agent-0 的 server + 心跳。
        await cluster.aservers[0].stop()
        dropped._running = False
        if dropped._heartbeat_task and not dropped._heartbeat_task.done():
            dropped._heartbeat_task.cancel()

        # 等心跳超时 → OFFLINE (heartbeat_timeout=6.0)。
        deadline = time.monotonic() + 15.0
        went_offline = False
        while time.monotonic() < deadline:
            node = cluster.master.nodes.get(dropped_id)
            if node and node.status.value == "offline":
                went_offline = True
                break
            await asyncio.sleep(0.5)
        assert went_offline, f"掉线节点 {dropped_id} 未被标 OFFLINE"

        # 重启 agent-0 — 新 agent 实例同端口自注册回 master。
        cfg = AgentConfig(
            node_id=dropped_id,
            master_host="127.0.0.1",
            master_port=cluster.master_port,
            agent_host="127.0.0.1",
            agent_port=dropped_port,
            cluster_token=TEST_TOKEN,
            heartbeat_interval=1.0,
            max_tasks=4,
        )
        new_agent = NodeAgent(cfg, backend=FakeBackend())
        new_agent._dispatch_token = TEST_TOKEN
        new_agent._get_local_ip = lambda: "127.0.0.1"
        new_aserver = AgentServer(agent=new_agent, shared_token=TEST_TOKEN)
        new_aserver._approval_manager = None
        new_aserver._rate_limiter = None
        serve_task = asyncio.create_task(new_aserver.start(host="127.0.0.1", port=dropped_port))
        try:
            ok = await _wait_health(
                f"http://127.0.0.1:{dropped_port}/api/health",
                {"Authorization": f"Bearer {TEST_TOKEN}"},
            )
            assert ok, "重连 agent 真端口未就绪"
            # 注册 + 起心跳循环 (否则 health check 再次超时标 OFFLINE)。
            new_agent._running = True
            await new_agent.report_hardware()
            hb_task = asyncio.create_task(new_agent._heartbeat_loop())

            # 等重连 → ONLINE。
            deadline = time.monotonic() + 15.0
            reconnected = False
            while time.monotonic() < deadline:
                node = cluster.master.nodes.get(dropped_id)
                if node and node.status.value == "online":
                    reconnected = True
                    break
                await asyncio.sleep(0.5)
            assert reconnected, f"重连节点 {dropped_id} 未恢复 ONLINE"

            # 恢复可派 — 提交任务, agent-0 应能接到。
            async with httpx.AsyncClient(timeout=15.0) as c:
                resp = await c.post(
                    f"http://127.0.0.1:{cluster.master_port}/api/tasks/submit",
                    json={
                        "name": "reconnect-task",
                        "mode": "data",
                        "model_name": "qwen-1b",
                        "preferred_node_id": dropped_id,
                        "task_type": "inference",
                        "prompt": "hi",
                        "max_tokens": 8,
                    },
                    headers={"Authorization": f"Bearer {TEST_TOKEN}"},
                )
            assert resp.status_code in (200, 202), f"重连后提交失败: {resp.status_code}"
        finally:
            new_agent._running = False
            if hb_task and not hb_task.done():
                hb_task.cancel()
            await new_aserver.stop()
            serve_task.cancel()
            try:
                await serve_task
            except (asyncio.CancelledError, Exception):
                pass


def _docker_available() -> bool:
    import shutil
    import subprocess

    if not shutil.which("docker"):
        return False
    try:
        r = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        return r.returncode == 0
    except Exception:
        return False


_skip_no_docker = pytest.mark.skipif(
    not _docker_available(),
    reason="docker 不可用 (需 Docker Desktop 运行 + fusion-multi-node:latest 镜像)",
)


class TestContainerE2E:
    """docker-compose 真 1 Master + N Agent 跨容器 HTTP E2E。skip-gate docker。

    起 compose (master:11452 + 2 agent), 验跨容器注册 + 派发, 完成后 down。
    依赖镜像 fusion-multi-node:latest 已 build (docker compose build)。
    """

    @pytest.mark.asyncio
    @_skip_no_docker
    async def test_container_cross_register_and_dispatch(self):
        import os
        import subprocess

        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        token = "container-e2e-token"
        env = {**os.environ, "FUSION_CLUSTER_TOKEN": token}

        # up: master + 2 agent (--scale agent=2)。
        up = subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", "agent=2"],
            cwd=repo,
            env=env,
            capture_output=True,
            timeout=180,
        )
        assert up.returncode == 0, f"compose up 失败: {up.stderr.decode()[:500]}"
        try:
            # 等 master 健康 + 2 agent 注册 (跨容器真 HTTP)。
            ok = await _wait_health(
                "http://127.0.0.1:11452/api/health", {"Authorization": f"Bearer {token}"}, timeout=60.0
            )
            assert ok, "容器 master 未就绪"

            deadline = time.monotonic() + 60.0
            registered = 0
            while time.monotonic() < deadline:
                async with httpx.AsyncClient(timeout=5.0) as c:
                    try:
                        r = await c.get(
                            "http://127.0.0.1:11452/api/nodes",
                            headers={"Authorization": f"Bearer {token}"},
                        )
                        if r.status_code == 200:
                            registered = r.json().get("total", 0)
                    except Exception:
                        pass
                if registered >= 2:
                    break
                await asyncio.sleep(1.0)
            assert registered >= 2, f"跨容器注册节点不足: 期望 2, 实际 {registered}"

            # 跨容器派发 (经 host:11452 → 容器内 agent)。
            async with httpx.AsyncClient(timeout=30.0) as c:
                resp = await c.post(
                    "http://127.0.0.1:11452/api/tasks/submit",
                    json={
                        "name": "container-task",
                        "mode": "data",
                        "model_name": "qwen-1b",
                        "task_type": "inference",
                        "prompt": "hi",
                        "max_tokens": 8,
                    },
                    headers={"Authorization": f"Bearer {token}"},
                )
            assert resp.status_code in (200, 202), f"跨容器提交失败: {resp.status_code} {resp.text}"
        finally:
            subprocess.run(
                ["docker", "compose", "down", "-v"],
                cwd=repo,
                env=env,
                capture_output=True,
                timeout=60,
            )

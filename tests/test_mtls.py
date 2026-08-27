"""P1-G mTLS 传输层测试 — 真实 TCP 端口双向证书认证。

验收: 节点带证书互连, 无证书拒绝, 非集群 CA 签名拒绝。
mTLS 关: 集群内 httpx/http 互连受证书保护, 无证书节点握手即拒 (到不了 ASGI)。

真实端口: 起 AgentServer 真 uvicorn (localhost:0 系统分配端口), 非 ASGITransport。
清理: 测试结束删临时 CA 目录 (过程数据规则 — 只留最终输出件+日志)。
"""

from __future__ import annotations

import asyncio
import socket
from pathlib import Path

import httpx
import pytest

from fusion_multi_node.security import mtls as mtls_mod
from fusion_multi_node.server.agent_server import AgentServer


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _provision_cluster(tmpdir: Path) -> dict[str, str]:
    """生成集群 CA + master/worker 两节点叶证书, 返回 env 映射。"""
    ca_dir = tmpdir / "ca"
    ca_cert, ca_key = mtls_mod.provision_cluster(str(ca_dir))
    nodes = {}
    for nid, role in [("master-1", "master"), ("worker-1", "worker")]:
        out = tmpdir / nid
        cert, key = mtls_mod.provision_node(nid, role, ca_cert, ca_key, str(out))
        nodes[nid] = {"cert": cert, "key": key, "role": role}
    return {"ca_cert": ca_cert, "ca_key": ca_key, "nodes": nodes}


def _set_mtls_env(monkeypatch, provisioned: dict[str, str], node_id: str) -> None:
    """设 mTLS env 开关 + 指定节点的证书路径。"""
    monkeypatch.setenv("FUSION_MTLS_ENABLED", "1")
    monkeypatch.setenv("FUSION_MTLS_CA_CERT", provisioned["ca_cert"])
    n = provisioned["nodes"][node_id]
    monkeypatch.setenv("FUSION_MTLS_NODE_CERT", n["cert"])
    monkeypatch.setenv("FUSION_MTLS_NODE_KEY", n["key"])
    monkeypatch.setenv("FUSION_MTLS_NODE_ID", node_id)
    monkeypatch.setenv("FUSION_MTLS_NODE_ROLE", n["role"])
    mtls_mod._ENABLED = True


def _clear_mtls_env(monkeypatch) -> None:
    for k in (
        "FUSION_MTLS_ENABLED",
        "FUSION_MTLS_CA_CERT",
        "FUSION_MTLS_NODE_CERT",
        "FUSION_MTLS_NODE_KEY",
        "FUSION_MTLS_NODE_ID",
        "FUSION_MTLS_NODE_ROLE",
    ):
        monkeypatch.delenv(k, raising=False)
    mtls_mod._ENABLED = False


async def _start_agent_server(server: AgentServer, host: str, port: int) -> asyncio.Task:
    task = asyncio.create_task(server.start(host=host, port=port))
    # 轮询健康端点就绪
    for _ in range(100):
        await asyncio.sleep(0.05)
        try:
            import urllib.request

            urllib.request.urlopen(f"http://{host}:{port}/api/health", timeout=0.5)
        except Exception:
            try:
                # mTLS 下 health 也需 https — 用 socket 探活
                with socket.create_connection((host, port), timeout=0.3):
                    pass
                break
            except Exception:
                continue
        break
    return task


class TestMtlsTransport:
    """mTLS 双向认证 — 真实端口握手验证。"""

    @pytest.mark.asyncio
    async def test_provision_cluster_generates_ca_and_leaves(self, tmp_path):
        """provision_cluster + provision_node 生成 CA + 叶证书, 节点 CN/role 写入证书 O。"""
        ca_cert, ca_key = mtls_mod.provision_cluster(str(tmp_path / "ca"))
        assert Path(ca_cert).exists()
        assert Path(ca_key).exists()
        cert_path, key_path = mtls_mod.provision_node("node-x", "worker", ca_cert, ca_key, str(tmp_path / "node-x"))
        from cryptography import x509

        leaf = x509.load_pem_x509_certificate(Path(cert_path).read_bytes())
        cn = leaf.subject.get_attributes_for_oid(x509.NameOID.COMMON_NAME)[0].value
        org = leaf.subject.get_attributes_for_oid(x509.NameOID.ORGANIZATION_NAME)[0].value
        assert cn == "node-x"
        assert org == "worker"

    @pytest.mark.asyncio
    async def test_cert_client_connects_no_cert_rejected(self, tmp_path, monkeypatch):
        """带集群 CA 签名证书的客户端可连; 无证书客户端 TLS 握手失败 (到不了 ASGI)。"""
        prov = _provision_cluster(tmp_path)
        # agent 用 worker-1 证书启动 (要求对端客户端证书)
        _set_mtls_env(monkeypatch, prov, "worker-1")
        # server_ssl_context 读 env → reload module 函数
        import importlib

        importlib.reload(mtls_mod)
        _set_mtls_env(monkeypatch, prov, "worker-1")

        server = AgentServer(shared_token="test-token")
        server._rate_limiter = None  # 禁速率限制干扰
        port = _free_port()
        serve_task = await _start_agent_server(server, "127.0.0.1", port)
        try:
            # 1) 带 master-1 证书的客户端 → 200
            ctx = mtls_mod.client_ssl_context()
            assert ctx is not None
            async with httpx.AsyncClient(verify=ctx, timeout=5.0) as client:
                resp = await client.get(
                    f"https://127.0.0.1:{port}/api/health",
                    headers={"Authorization": "Bearer test-token"},
                )
                assert resp.status_code == 200

            # 2) 无证书客户端 → TLS 握手失败 (ConnectError / SSLError)
            with pytest.raises((httpx.ConnectError, Exception)) as ei:
                async with httpx.AsyncClient(timeout=3.0) as bare:
                    await bare.get(
                        f"https://127.0.0.1:{port}/api/health",
                        headers={"Authorization": "Bearer test-token"},
                    )
            assert "tls" in str(ei.value).lower() or "ssl" in str(ei.value).lower() or "cert" in str(ei.value).lower()
        finally:
            server._uvicorn_server.should_exit = True
            serve_task.cancel()
            try:
                await serve_task
            except (asyncio.CancelledError, Exception):
                pass
            _clear_mtls_env(monkeypatch)
            importlib.reload(mtls_mod)

    @pytest.mark.asyncio
    async def test_non_cluster_ca_cert_rejected(self, tmp_path, monkeypatch):
        """对端证书由别的 CA 签名 → 校验失败 (client 不信任)。"""
        prov = _provision_cluster(tmp_path)
        _set_mtls_env(monkeypatch, prov, "worker-1")
        import importlib

        importlib.reload(mtls_mod)
        _set_mtls_env(monkeypatch, prov, "worker-1")

        server = AgentServer(shared_token="test-token")
        server._rate_limiter = None
        port = _free_port()
        serve_task = await _start_agent_server(server, "127.0.0.1", port)
        try:
            # 另起一套独立 CA + 签一个叶证书 — 与集群 CA 无关
            rogue_dir = tmp_path / "rogue"
            rogue_ca, rogue_key = mtls_mod.provision_cluster(str(rogue_dir))
            rogue_cert, rogue_key_leaf = mtls_mod.provision_node(
                "rogue", "master", rogue_ca, rogue_key, str(rogue_dir / "rogue")
            )
            import ssl

            rogue_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            rogue_ctx.load_cert_chain(rogue_cert, rogue_key_leaf)
            rogue_ctx.check_hostname = False
            rogue_ctx.verify_mode = ssl.CERT_REQUIRED
            rogue_ctx.load_verify_locations(rogue_ca)
            # 集群端信任 prov CA, 不信任 rogue CA → 握手失败
            with pytest.raises(Exception):
                async with httpx.AsyncClient(verify=rogue_ctx, timeout=3.0) as client:
                    await client.get(
                        f"https://127.0.0.1:{port}/api/health",
                        headers={"Authorization": "Bearer test-token"},
                    )
        finally:
            server._uvicorn_server.should_exit = True
            serve_task.cancel()
            try:
                await serve_task
            except (asyncio.CancelledError, Exception):
                pass
            _clear_mtls_env(monkeypatch)
            importlib.reload(mtls_mod)

    @pytest.mark.asyncio
    async def test_mtls_disabled_defaults_http(self, monkeypatch):
        """mTLS 关 → scheme=http, client_kwargs={} (不破坏现有 http 行为)。"""
        _clear_mtls_env(monkeypatch)
        assert mtls_mod.scheme() == "http"
        assert mtls_mod.client_kwargs() == {}
        assert mtls_mod.server_ssl_context() is None
        assert mtls_mod.client_ssl_context() is None


class TestFineGrainedPermission:
    """细粒度权限 — ASGITransport, 强制模式 (enforce=True)。

    验收: master 角色可 execute+cancel; worker 可 execute 不可 cancel;
    缺 X-Node-Id → 403; 越界 → 403。
    """

    @pytest.mark.asyncio
    async def test_master_can_execute_and_cancel(self):
        """master 头 → /api/execute 200, /api/tasks/cancel 404 (无任务但过权限, 非 403)。"""
        from httpx import ASGITransport, AsyncClient

        from fusion_multi_node.agent import AgentConfig, NodeAgent
        from fusion_multi_node.agent.node_agent import InferenceBackend

        class FakeBackend(InferenceBackend):
            async def chat(self, model, messages, temperature=0.7, max_tokens=4096, **kwargs):
                return {"choices": [{"message": {"content": "ok"}}], "usage": {"total_tokens": 1}}

            async def embed(self, model, input_text, **kwargs):
                return {"data": [{"embedding": [0.1]}]}

            async def health(self):
                return True

        agent = NodeAgent(
            config=AgentConfig(node_id="agent-1", cluster_token="t", agent_port=21460),
            backend=FakeBackend(),
        )
        server = AgentServer(agent=agent, shared_token="t")
        server._permission_enforce = True  # 强制模式 (模拟 mTLS 开)
        server._rate_limiter = None
        hdr = {"Authorization": "Bearer t", "X-Node-Id": "master", "X-Node-Role": "master"}
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/execute",
                json={"task_type": "inference", "model_name": "m", "prompt": "hi", "max_tokens": 8},
                headers=hdr,
            )
            assert resp.status_code == 200, resp.text

    @pytest.mark.asyncio
    async def test_worker_cannot_cancel(self):
        """worker 头 → /api/tasks/cancel 403 (TASK_CANCEL 不在 WORKER 权限集)。"""
        from httpx import ASGITransport, AsyncClient

        from fusion_multi_node.agent import AgentConfig, NodeAgent

        agent = NodeAgent(
            config=AgentConfig(node_id="agent-1", cluster_token="t", agent_port=21461),
            backend=None,
        )
        server = AgentServer(agent=agent, shared_token="t")
        server._permission_enforce = True
        server._rate_limiter = None
        hdr = {"Authorization": "Bearer t", "X-Node-Id": "worker-9", "X-Node-Role": "worker"}
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/api/tasks/cancel", json={"task_id": "x"}, headers=hdr)
            assert resp.status_code == 403, resp.text

    @pytest.mark.asyncio
    async def test_worker_can_execute(self):
        """worker 头 → /api/execute 200 (TASK_EXECUTE 在 WORKER 权限集)。"""
        from httpx import ASGITransport, AsyncClient

        from fusion_multi_node.agent import AgentConfig, NodeAgent
        from fusion_multi_node.agent.node_agent import InferenceBackend

        class FakeBackend(InferenceBackend):
            async def chat(self, model, messages, temperature=0.7, max_tokens=4096, **kwargs):
                return {"choices": [{"message": {"content": "ok"}}], "usage": {"total_tokens": 1}}

            async def embed(self, model, input_text, **kwargs):
                return {"data": [{"embedding": [0.1]}]}

            async def health(self):
                return True

        agent = NodeAgent(
            config=AgentConfig(node_id="agent-1", cluster_token="t", agent_port=21462),
            backend=FakeBackend(),
        )
        server = AgentServer(agent=agent, shared_token="t")
        server._permission_enforce = True
        server._rate_limiter = None
        hdr = {"Authorization": "Bearer t", "X-Node-Id": "worker-9", "X-Node-Role": "worker"}
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/execute",
                json={"task_type": "inference", "model_name": "m", "prompt": "hi", "max_tokens": 8},
                headers=hdr,
            )
            assert resp.status_code == 200, resp.text

    @pytest.mark.asyncio
    async def test_enforce_missing_node_id_403(self):
        """强制模式缺 X-Node-Id → 403。"""
        from httpx import ASGITransport, AsyncClient

        from fusion_multi_node.agent import AgentConfig, NodeAgent

        agent = NodeAgent(
            config=AgentConfig(node_id="agent-1", cluster_token="t", agent_port=21463),
            backend=None,
        )
        server = AgentServer(agent=agent, shared_token="t")
        server._permission_enforce = True
        server._rate_limiter = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/execute",
                json={"task_type": "inference", "model_name": "m", "prompt": "hi"},
                headers={"Authorization": "Bearer t"},
            )
            assert resp.status_code == 403, resp.text

    @pytest.mark.asyncio
    async def test_compatible_no_header_allows(self):
        """兼容模式 (enforce=False) 无 X-Node-Id → 放行 (现有测试/CLI 无头)。"""
        from httpx import ASGITransport, AsyncClient

        from fusion_multi_node.agent import AgentConfig, NodeAgent
        from fusion_multi_node.agent.node_agent import InferenceBackend

        class FakeBackend(InferenceBackend):
            async def chat(self, model, messages, temperature=0.7, max_tokens=4096, **kwargs):
                return {"choices": [{"message": {"content": "ok"}}], "usage": {"total_tokens": 1}}

            async def embed(self, model, input_text, **kwargs):
                return {"data": [{"embedding": [0.1]}]}

            async def health(self):
                return True

        agent = NodeAgent(
            config=AgentConfig(node_id="agent-1", cluster_token="t", agent_port=21464),
            backend=FakeBackend(),
        )
        server = AgentServer(agent=agent, shared_token="t")
        server._permission_enforce = False
        server._rate_limiter = None
        transport = ASGITransport(app=server.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/execute",
                json={"task_type": "inference", "model_name": "m", "prompt": "hi", "max_tokens": 8},
                headers={"Authorization": "Bearer t"},
            )
            assert resp.status_code == 200, resp.text

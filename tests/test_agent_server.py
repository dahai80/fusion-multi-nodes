"""Agent Server FastAPI 测试。"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from fusion_multi_node.agent import AgentConfig
from fusion_multi_node.distributed_mlx import KVCacheEntry, KVShard
from fusion_multi_node.server.agent_server import AgentServer

TEST_TOKEN = "test-cluster-token"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


def _make_kv_entry(
    cache_id: str = "kv1",
    model_name: str = "llama-3b",
    prompt_hash: str = "abc123",
) -> KVCacheEntry:
    shard = KVShard(
        shard_id="shard_0",
        model_name=model_name,
        layer_index=0,
        node_id="node_1",
        token_count=100,
        size_bytes=4096,
        created_at=time.time(),
    )
    return KVCacheEntry(
        cache_id=cache_id,
        model_name=model_name,
        prompt_hash=prompt_hash,
        prompt_prefix="hello",
        shards=[shard],
        total_tokens=100,
        total_size_bytes=4096,
        created_at=time.time(),
    )


@pytest.fixture
def mock_agent():
    agent = MagicMock()
    agent.config = AgentConfig(node_id="test_node")
    agent.collect_hardware_info.return_value = {
        "node_id": "test_node",
        "hostname": "mac-test",
        "cpu_cores": 12,
    }
    agent.execute_task = AsyncMock(return_value={"result": "done"})
    return agent


@pytest.fixture
def mock_kv_manager():
    mgr = MagicMock()
    mgr.get_stats.return_value = {
        "local_entries": 0,
        "local_size_mb": 0.0,
    }
    mgr.lookup_local.return_value = None
    mgr.transfer_from_remote = AsyncMock(return_value=True)
    mgr.store_local.return_value = True
    return mgr


@pytest.fixture
def agent_server(mock_agent, mock_kv_manager):
    return AgentServer(agent=mock_agent, kv_manager=mock_kv_manager, shared_token=TEST_TOKEN)


@pytest.fixture
def app(agent_server):
    return agent_server.app


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestAgentServerInit:
    def test_default_init(self):
        server = AgentServer()
        assert server.agent is not None
        assert server.kv_manager is not None
        assert server.app is not None
        assert server._started_at == 0.0

    def test_custom_init(self, mock_agent, mock_kv_manager):
        server = AgentServer(agent=mock_agent, kv_manager=mock_kv_manager)
        assert server.agent is mock_agent
        assert server.kv_manager is mock_kv_manager


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health_before_start(self, client, agent_server):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    @pytest.mark.asyncio
    async def test_health_after_start(self, client, agent_server):
        resp = await client.get("/api/health")
        data = resp.json()
        assert data["status"] == "ok"

    @pytest.mark.asyncio
    async def test_health_liveness_checks_present(self, client):
        # C11: liveness 带本地依赖检查 (disk/mem/fusion_mlx_port)
        resp = await client.get("/api/health")
        data = resp.json()
        assert data["role"] == "agent"
        assert "checks" in data
        assert {"disk_ok", "mem_ok", "fusion_mlx_port"}.issubset(data["checks"].keys())

    @pytest.mark.asyncio
    async def test_health_deep_degraded_fusion_mlx_down(self, client):
        # C11: readiness — fusion-mlx HTTP 探测失败 → degraded
        resp = await client.get("/api/health/deep")
        data = resp.json()
        assert data["role"] == "agent"
        assert data["checks"]["fusion_mlx_ready"] is False
        assert data["status"] == "degraded"

    @pytest.mark.asyncio
    async def test_health_deep_ok_fusion_mlx_up(self, mock_agent, mock_kv_manager):
        # C11: readiness — mock fusion-mlx /v1/models 200 → ok
        mock_agent._check_service.return_value = True
        mock_agent._backend.base_url = "http://localhost:9999"
        server = AgentServer(agent=mock_agent, kv_manager=mock_kv_manager, shared_token=TEST_TOKEN)
        transport = ASGITransport(app=server.app)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        with patch("httpx.AsyncClient", MagicMock(return_value=mock_client)):
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                resp = await c.get("/api/health/deep")
        data = resp.json()
        assert data["checks"]["fusion_mlx_ready"] is True
        assert data["status"] == "ok"

    @pytest.mark.asyncio
    async def test_health_deep_ok_remote_mlx_via_url(self, mock_agent, mock_kv_manager):
        # issue #60: 容器化部署 — MLX 在宿主机 (host.docker.internal:11434), 非本机 localhost。
        # backend.base_url 经 FUSION_MLX_URL env 指向非本机 → readiness 须以 HTTP 探测为准,
        # 不因本地 socket 探测 localhost:11434 失败而恒 degraded (旧 bug)。
        # 且 /v1/models 探测须带 api_key Bearer — fusion-mlx 启用鉴权时无头恒 401 (同 issue)。
        mock_agent._check_service.return_value = False  # 容器内本地 socket 探测必失败
        mock_agent._backend.base_url = "http://host.docker.internal:11434"
        mock_agent._backend.api_key = "fg-admin-key"
        server = AgentServer(agent=mock_agent, kv_manager=mock_kv_manager, shared_token=TEST_TOKEN)
        transport = ASGITransport(app=server.app)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        with patch("httpx.AsyncClient", MagicMock(return_value=mock_client)):
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                resp = await c.get("/api/health/deep")
        data = resp.json()
        # 探测 URL 取 backend.base_url (非本机), 非 localhost:{fusion_mlx_port}。
        call_args, call_kwargs = mock_client.get.call_args
        assert call_args[0] == "http://host.docker.internal:11434/v1/models"
        # 探测须带 Bearer api_key — 漏头则 fusion-mlx 鉴权 401 (issue #60 第二半)。
        assert call_kwargs["headers"]["Authorization"] == "Bearer fg-admin-key"
        # 非本机 MLX → fusion_mlx_port 取 HTTP 探测结果 (True), 不取本地 socket (False)。
        assert data["checks"]["fusion_mlx_port"] is True
        assert data["checks"]["fusion_mlx_ready"] is True
        assert data["status"] == "ok"


class TestExecuteEndpoint:
    @pytest.mark.asyncio
    async def test_execute_task_success(self, client, mock_agent):
        mock_agent.execute_task.return_value = {"content": "hello world"}
        resp = await client.post(
            "/api/execute",
            json={
                "task_type": "inference",
                "model_name": "llama-3b",
                "prompt": "say hello",
                "max_tokens": 2048,
                "temperature": 0.7,
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["result"] == {"content": "hello world"}
        call_args = mock_agent.execute_task.call_args[0][0]
        assert call_args["type"] == "inference"
        assert call_args["model"] == "llama-3b"
        assert call_args["model_name"] == "llama-3b"
        assert call_args["params"]["prompt"] == "say hello"
        assert call_args["params"]["max_tokens"] == 2048
        assert call_args["params"]["temperature"] == 0.7

    @pytest.mark.asyncio
    async def test_execute_task_with_extra(self, client, mock_agent):
        resp = await client.post(
            "/api/execute",
            json={
                "task_type": "inference",
                "model_name": "llama-3b",
                "extra": {"top_p": 0.9},
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        call_args = mock_agent.execute_task.call_args[0][0]
        assert call_args["params"]["top_p"] == 0.9

    @pytest.mark.asyncio
    async def test_execute_task_failure(self, client, mock_agent):
        mock_agent.execute_task.side_effect = RuntimeError("model not found")
        resp = await client.post(
            "/api/execute",
            json={
                "task_type": "inference",
                "model_name": "missing-model",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 500
        assert "内部错误" in resp.json()["detail"]


class TestKVLookupEndpoint:
    @pytest.mark.asyncio
    async def test_kv_lookup_found(self, client, mock_kv_manager):
        entry = _make_kv_entry()
        mock_kv_manager.lookup_local.return_value = entry
        # mock_kv_manager 为 MagicMock — _serialize_entry 缺返回值。
        # 用真 manager 序列化同款 entry, 验 route 回传 {"found":True,"entry":{...}} 形状。
        from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager

        mock_kv_manager._serialize_entry.return_value = KVSharingManager()._serialize_entry(entry)
        resp = await client.post(
            "/api/kv/lookup",
            json={
                "model_name": "llama-3b",
                "prompt_hash": "abc123",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        # 契约对齐 lookup_remote: {"found": True, "entry": {...}} (非旧扁平 dict)。
        assert data["found"] is True
        entry = data["entry"]
        assert entry["cache_id"] == "kv1"
        assert entry["model_name"] == "llama-3b"
        assert entry["prompt_hash"] == "abc123"
        assert entry["total_tokens"] == 100
        assert entry["total_size_bytes"] == 4096
        assert len(entry["shards"]) == 1
        shard = entry["shards"][0]
        assert shard["shard_id"] == "shard_0"
        assert shard["layer_index"] == 0
        assert shard["node_id"] == "node_1"
        assert shard["token_count"] == 100
        assert shard["size_bytes"] == 4096

    @pytest.mark.asyncio
    async def test_kv_lookup_not_found(self, client, mock_kv_manager):
        mock_kv_manager.lookup_local.return_value = None
        resp = await client.post(
            "/api/kv/lookup",
            json={
                "model_name": "llama-3b",
                "prompt_hash": "missing",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 404


class TestKVTransferEndpoint:
    @pytest.mark.asyncio
    async def test_kv_transfer_success(self, client, mock_kv_manager):
        # 推模型: 路由查本地 lookup_local_by_id → _serialize_entry 回传。
        mock_kv_manager.lookup_local_by_id.return_value = MagicMock(cache_id="kv1")
        mock_kv_manager._serialize_entry.return_value = {"cache_id": "kv1"}
        resp = await client.post(
            "/api/kv/transfer",
            json={
                "cache_id": "kv1",
                "target_node": "node_2",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["entry"]["cache_id"] == "kv1"

    @pytest.mark.asyncio
    async def test_kv_transfer_not_found(self, client, mock_kv_manager):
        # 本地无此 cache_id → 404 (非静默 200)。
        mock_kv_manager.lookup_local_by_id.return_value = None
        resp = await client.post(
            "/api/kv/transfer",
            json={
                "cache_id": "kv-missing",
                "target_node": "node_2",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_kv_transfer_with_custom_port(self, client, mock_kv_manager):
        mock_kv_manager.lookup_local_by_id.return_value = MagicMock(cache_id="kv1")
        mock_kv_manager._serialize_entry.return_value = {"cache_id": "kv1"}
        resp = await client.post(
            "/api/kv/transfer",
            json={
                "cache_id": "kv1",
                "target_node": "node_2",
                "target_port": 8765,
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200


class TestKVWarmEndpoint:
    @pytest.mark.asyncio
    async def test_kv_warm(self, client, mock_kv_manager):
        # S4: kv_warm 改本地 store_local 契约 (不再跨节点二次远推, 避递归)。
        # manager.warm_cache 负责跨节点分发, 各 Worker 收此请求只本地预存。
        mock_kv_manager.store_local.return_value = True
        resp = await client.post(
            "/api/kv/warm",
            json={
                "model_name": "llama-3b",
                "prompt": "hello world",
                "prompt_hash": "abc123",
                "total_tokens": 16,
                "total_size_bytes": 1024,
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["warmed"] == 1
        mock_kv_manager.store_local.assert_called_once()
        entry = mock_kv_manager.store_local.call_args[0][0]
        assert entry.model_name == "llama-3b"
        assert entry.prompt_hash == "abc123"
        assert entry.total_tokens == 16

    @pytest.mark.asyncio
    async def test_kv_warm_no_results(self, client, mock_kv_manager):
        # store_local 拒存 (如超容量) → warmed==0, status==skip。
        mock_kv_manager.store_local.return_value = False
        resp = await client.post(
            "/api/kv/warm",
            json={
                "model_name": "llama-3b",
                "prompt": "hello",
                "prompt_hash": "xyz",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "skip"
        assert data["warmed"] == 0

    @pytest.mark.asyncio
    async def test_kv_warm_builds_entry_from_request(self, client, mock_kv_manager):
        # E7: 请求字段须正确映射到 KVCacheEntry (model_name/prompt_hash/node_id/size)。
        # 旧契约断 warm_cache.nodes 透传 — 新契约无 nodes, 改断 store_local 入参正确性。
        mock_kv_manager.store_local.return_value = True
        await client.post(
            "/api/kv/warm",
            json={
                "model_name": "llama-3b",
                "prompt": "warm me",
                "prompt_hash": "deadbeef",
                "total_tokens": 32,
                "total_size_bytes": 2048,
            },
            headers=AUTH_HEADERS,
        )
        entry = mock_kv_manager.store_local.call_args[0][0]
        assert entry.cache_id == "warm-deadbeef"
        assert entry.prompt_prefix == "warm me"
        assert entry.total_size_bytes == 2048
        assert len(entry.shards) == 1
        assert entry.shards[0].node_id == "test_node"


class TestKVStatsEndpoint:
    @pytest.mark.asyncio
    async def test_kv_stats(self, client, mock_kv_manager):
        mock_kv_manager.get_stats.return_value = {
            "local_entries": 5,
            "local_size_mb": 128.0,
        }
        resp = await client.get("/api/kv/stats", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["local_entries"] == 5
        assert data["local_size_mb"] == 128.0


class TestHardwareEndpoint:
    @pytest.mark.asyncio
    async def test_hardware_info(self, client, mock_agent):
        resp = await client.get("/api/hardware", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "test_node"
        assert data["hostname"] == "mac-test"
        mock_agent.collect_hardware_info.assert_called_once()

    @pytest.mark.asyncio
    async def test_hardware_info_runs_off_event_loop(self, client, mock_agent):
        # P1-2 (审计 §4.5): /api/hardware 经 asyncio.to_thread 调 collect_hardware_info,
        # 不阻塞事件循环。验证: 调用期间并发 sleep 计时器能推进 (事件循环未被独占)。
        def slow_collect():
            time.sleep(0.05)
            return {"node_id": "test_node", "hostname": "mac-test"}

        mock_agent.collect_hardware_info.side_effect = slow_collect
        loop_marker = asyncio.create_task(self._background_counter())
        resp = await client.get("/api/hardware", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        # 后台计时器与请求并发跑; to_thread 不阻塞事件循环 → 后台任务能推进
        await loop_marker

    @staticmethod
    async def _background_counter():
        # 事件循环健康标志: 连续 5 次 10ms sleep 累计应 ~50ms, 不被同步调用卡死。
        for _ in range(5):
            await asyncio.sleep(0.01)


class TestAgentServerStart:
    @pytest.mark.asyncio
    async def test_start(self, agent_server):
        mock_uvicorn = MagicMock()
        mock_config = MagicMock()
        mock_server = MagicMock()
        mock_server.serve = AsyncMock()
        mock_uvicorn.Config.return_value = mock_config
        mock_uvicorn.Server.return_value = mock_server
        with patch.dict("sys.modules", {"uvicorn": mock_uvicorn}):
            await agent_server.start(host="127.0.0.1", port=9999)
        mock_uvicorn.Config.assert_called_once_with(
            agent_server.app,
            host="127.0.0.1",
            port=9999,
            log_level="warning",
        )
        mock_uvicorn.Server.assert_called_once_with(mock_config)
        assert agent_server._started_at > 0
        assert agent_server._uvicorn_server is mock_server
        mock_server.serve.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_port_conflict_raises_with_hint(self, agent_server):
        """issue #25: 端口被占用 → OSError 带冲突端口提示, 非通用 bind 错误。"""
        mock_uvicorn = MagicMock()
        mock_config = MagicMock()
        mock_server = MagicMock()
        # bind 失败 — Address already in use
        mock_server.serve = AsyncMock(side_effect=OSError(48, "Address already in use"))
        mock_uvicorn.Config.return_value = mock_config
        mock_uvicorn.Server.return_value = mock_server
        with patch.dict("sys.modules", {"uvicorn": mock_uvicorn}):
            with pytest.raises(OSError, match="11445.*fusion-comfyui"):
                await agent_server.start(host="127.0.0.1", port=11445)

    @pytest.mark.asyncio
    async def test_start_port_conflict_no_known_conflict(self, agent_server):
        """未知冲突端口 → OSError 仍抛 (无 hint 但含端口号)。"""
        mock_uvicorn = MagicMock()
        mock_config = MagicMock()
        mock_server = MagicMock()
        mock_server.serve = AsyncMock(side_effect=OSError(48, "Address already in use"))
        mock_uvicorn.Config.return_value = mock_config
        mock_uvicorn.Server.return_value = mock_server
        with patch.dict("sys.modules", {"uvicorn": mock_uvicorn}):
            with pytest.raises(OSError, match="12345"):
                await agent_server.start(host="127.0.0.1", port=12345)


class TestAgentServerStop:
    @pytest.mark.asyncio
    async def test_stop_without_server(self, agent_server):
        await agent_server.stop()
        assert agent_server._uvicorn_server is None

    @pytest.mark.asyncio
    async def test_stop_with_server(self, agent_server):
        mock_server = MagicMock()
        agent_server._uvicorn_server = mock_server
        await agent_server.stop()
        assert mock_server.should_exit is True


class TestAgentServerKVPersistenceLifecycle:
    # P1-9: AgentServer.start 恢复 KV 缓存, stop 落盘 — 真 KVSharingManager + tmp_path。
    @pytest.mark.asyncio
    async def test_start_loads_and_stop_saves_kv_cache(self, mock_agent, tmp_path):
        import json

        from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager

        persist = tmp_path / "kv_cache.json"
        # 先写一份已落盘的缓存 (模拟上一次运行 stop 时落盘)
        mgr1 = KVSharingManager(enable_compression=False, persist_path=str(persist))
        mgr1.store_local(
            KVCacheEntry(
                cache_id="c1",
                model_name="m",
                prompt_hash="h1",
                prompt_prefix="Hello",
                total_tokens=10,
                total_size_bytes=512,
                created_at=time.time(),
                ttl_seconds=3600.0,
                shards=[
                    KVShard(
                        shard_id="s1",
                        model_name="m",
                        layer_index=0,
                        node_id="n1",
                        token_count=10,
                        size_bytes=512,
                        created_at=time.time(),
                    )
                ],
            )
        )
        assert mgr1.save() is True

        # 新 server 用同一 persist_path — start() 应恢复 c1
        mgr2 = KVSharingManager(enable_compression=False, persist_path=str(persist))
        server = AgentServer(agent=mock_agent, kv_manager=mgr2, shared_token=TEST_TOKEN)
        mock_uvicorn = MagicMock()
        mock_uvicorn.Config.return_value = MagicMock()
        mock_server = MagicMock()
        mock_server.serve = AsyncMock()
        mock_uvicorn.Server.return_value = mock_server
        with patch.dict("sys.modules", {"uvicorn": mock_uvicorn}):
            await server.start(host="127.0.0.1", port=9999)
        # 恢复后本地缓存应有 c1
        assert mgr2.lookup_local("m", "h1") is not None

        # 再存一条新缓存 → stop() 落盘应包含 c1 + c2
        mgr2.store_local(
            KVCacheEntry(
                cache_id="c2",
                model_name="m",
                prompt_hash="h2",
                prompt_prefix="World",
                total_tokens=5,
                total_size_bytes=256,
                created_at=time.time(),
                ttl_seconds=3600.0,
                shards=[],
            )
        )
        await server.stop()
        data = json.loads(persist.read_text(encoding="utf-8"))
        assert {e["cache_id"] for e in data["entries"]} == {"c1", "c2"}

    @pytest.mark.asyncio
    async def test_stop_saves_kv_cache(self, mock_agent, tmp_path):
        import json

        from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager

        persist = tmp_path / "kv_cache.json"
        mgr = KVSharingManager(enable_compression=False, persist_path=str(persist))
        server = AgentServer(agent=mock_agent, kv_manager=mgr, shared_token=TEST_TOKEN)
        await server.stop()
        # 无缓存条目 → 落盘 0 条但文件应存在
        assert persist.exists()
        assert json.loads(persist.read_text(encoding="utf-8"))["entry_count"] == 0

    @pytest.mark.asyncio
    async def test_p2_3_stop_calls_kv_manager_close(self, mock_agent, tmp_path):
        """P2-3 (审计 §6.3): AgentServer.stop 调 kv_manager.close() 关 httpx + 张量后端,
        修资源泄漏 (旧 stop 仅 save 不 close)。用真 KVSharingManager + 注入未关 httpx 客户端验 close 生效。"""
        from fusion_multi_node.distributed_mlx.kv_cache_sharing import KVSharingManager

        mgr = KVSharingManager(enable_compression=False, persist_path=str(tmp_path / "kv.json"))
        # 触发 _get_http_client 创建 httpx 客户端 (模拟用过跨节点路径)。
        client = await mgr._get_http_client(5.0)
        assert client is not None and not client.is_closed
        server = AgentServer(agent=mock_agent, kv_manager=mgr, shared_token=TEST_TOKEN)
        await server.stop()
        # close() 后 httpx 客户端已关 (句柄释放, 非泄漏)。
        assert client.is_closed

    @pytest.mark.asyncio
    async def test_p2_3_stop_close_failure_does_not_raise(self, mock_agent, tmp_path):
        """P2-3: kv_manager.close() 抛异常 → stop 不传播 (catch + warning, 不拖垮停服)。"""
        mgr = MagicMock()
        mgr.save.side_effect = RuntimeError("盘满")
        mgr.close = AsyncMock(side_effect=RuntimeError("关连接失败"))
        server = AgentServer(agent=mock_agent, kv_manager=mgr, shared_token=TEST_TOKEN)
        # save + close 均抛, stop 仍正常返回 (两步各自 try/except 容错)。
        await server.stop()
        mgr.save.assert_called_once()
        mgr.close.assert_awaited_once()


class TestP3_2KVSaveAlert:
    """P3-2 (审计 §6.11): AgentServer.stop KV 落盘失败升 critical + 上报 master 故障。"""

    @pytest.mark.asyncio
    async def test_save_failure_reports_fault(self, tmp_path):
        # save() 抛异常 → critical 日志 + report_fault 上报 master (best-effort)。
        mgr = MagicMock()
        mgr.save.side_effect = RuntimeError("盘满")
        mgr.close = AsyncMock()
        agent = MagicMock()
        agent.config = AgentConfig(node_id="n1")
        agent.report_fault = AsyncMock(return_value=True)
        server = AgentServer(agent=agent, kv_manager=mgr, shared_token=TEST_TOKEN)
        await server.stop()
        mgr.save.assert_called_once()
        agent.report_fault.assert_awaited_once()
        kwargs = agent.report_fault.await_args.kwargs
        assert kwargs.get("fault_type") == "kv_persist_failed"

    @pytest.mark.asyncio
    async def test_save_returns_false_reports_fault(self, tmp_path):
        # save() 返 False (落盘内部失败不抛) → 同样上报故障。
        mgr = MagicMock()
        mgr.save.return_value = False
        mgr.close = AsyncMock()
        agent = MagicMock()
        agent.config = AgentConfig(node_id="n1")
        agent.report_fault = AsyncMock(return_value=True)
        server = AgentServer(agent=agent, kv_manager=mgr, shared_token=TEST_TOKEN)
        await server.stop()
        agent.report_fault.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_save_success_no_fault(self, tmp_path):
        # save() 成功 → 不上报故障 (避免噪声)。
        mgr = MagicMock()
        mgr.save.return_value = True
        mgr.close = AsyncMock()
        agent = MagicMock()
        agent.config = AgentConfig(node_id="n1")
        agent.report_fault = AsyncMock(return_value=True)
        server = AgentServer(agent=agent, kv_manager=mgr, shared_token=TEST_TOKEN)
        await server.stop()
        agent.report_fault.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_report_fault_failure_does_not_break_stop(self, tmp_path):
        # report_fault 自身失败 (停服期 master 不可达) → 不传播, stop 正常返回。
        mgr = MagicMock()
        mgr.save.return_value = False
        mgr.close = AsyncMock()
        agent = MagicMock()
        agent.config = AgentConfig(node_id="n1")
        agent.report_fault = AsyncMock(side_effect=RuntimeError("master 不可达"))
        server = AgentServer(agent=agent, kv_manager=mgr, shared_token=TEST_TOKEN)
        await server.stop()
        mgr.close.assert_awaited_once()

"""Agent Server FastAPI 测试。"""

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
    mgr.transfer_from_remote.return_value = True
    mgr.warm_cache.return_value = {"warmed": 0}
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
        assert call_args["model_name"] == "llama-3b"
        assert call_args["prompt"] == "say hello"
        assert call_args["max_tokens"] == 2048
        assert call_args["temperature"] == 0.7

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
        assert call_args["top_p"] == 0.9

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
        assert data["cache_id"] == "kv1"
        assert data["model_name"] == "llama-3b"
        assert data["prompt_hash"] == "abc123"
        assert data["total_tokens"] == 100
        assert data["total_size_bytes"] == 4096
        assert len(data["shards"]) == 1
        shard = data["shards"][0]
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
        mock_kv_manager.transfer_from_remote.return_value = True
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

    @pytest.mark.asyncio
    async def test_kv_transfer_failure(self, client, mock_kv_manager):
        mock_kv_manager.transfer_from_remote.return_value = False
        resp = await client.post(
            "/api/kv/transfer",
            json={
                "cache_id": "kv1",
                "target_node": "node_2",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 500

    @pytest.mark.asyncio
    async def test_kv_transfer_with_custom_port(self, client, mock_kv_manager):
        mock_kv_manager.transfer_from_remote.return_value = True
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
        mock_kv_manager.warm_cache.return_value = {"warmed": 3}
        resp = await client.post(
            "/api/kv/warm",
            json={
                "model_name": "llama-3b",
                "prompts": ["hello", "world", "test"],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["warmed"] == 3

    @pytest.mark.asyncio
    async def test_kv_warm_no_results(self, client, mock_kv_manager):
        mock_kv_manager.warm_cache.return_value = {}
        resp = await client.post(
            "/api/kv/warm",
            json={
                "model_name": "llama-3b",
                "prompts": ["hello"],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["warmed"] == 0


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

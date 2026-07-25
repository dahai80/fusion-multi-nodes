# API Reference — fusion-multi-node v0.1.0
# User instruction: "生成doc文档和READMD，README_CN，提交配置库，发布0.1.0版本"
# Public API surface for all 7 modules + config + utils

## Table of Contents

- [Cluster Master](#cluster-master)
- [Node Agent](#node-agent)
- [mDNS Discovery](#mdns-discovery)
- [FMP Protocol](#fmp-protocol)
- [Distributed MLX Bridge](#distributed-mlx-bridge)
- [MCP Cluster Gateway](#mcp-cluster-gateway)
- [Cluster Observability](#cluster-observability)
- [Configuration](#configuration)
- [Utilities](#utilities)

---

## Cluster Master

`from fusion_multi_node.master import ClusterMaster, NodeInfo, ClusterTask, KVCacheEntry, NodeStatus, ParallelMode, TaskStatus`

### ClusterMaster

```python
master = ClusterMaster(host="0.0.0.0", port=9753)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `register_node` | `(node: NodeInfo) -> NodeInfo` | Register node, sets status=ONLINE, updates heartbeat |
| `assign_task` | `(task: ClusterTask) -> ClusterTask` | Assign task to best-scoring node |
| `complete_task` | `(task_id: str, error: str = None) -> ClusterTask` | Complete or fail a task |
| `cancel_task` | `(task_id: str) -> bool` | Cancel a pending/running task |
| `migrate_task` | `(task_id: str) -> ClusterTask` | Migrate task to another node |
| `check_heartbeat` | `() -> list[str]` | Check all nodes, return stale node IDs |
| `find_kv_cache` | `(model: str, prompt_hash: str) -> KVCacheEntry or None` | Look up KV cache by model+hash |
| `add_kv_cache` | `(entry: KVCacheEntry)` | Add entry to global KV cache pool |

**Public attributes**: `nodes: dict[str, NodeInfo]`, `tasks: dict[str, ClusterTask]`, `kv_cache: dict[str, KVCacheEntry]`

### NodeInfo

```python
node = NodeInfo(
    node_id="node_1",
    hostname="mac-studio-1",
    ip_address="10.0.0.1",
    port=9755,
    total_memory_gb=64.0,
    available_memory_gb=48.0,
)
print(node.score)  # 0.0-1.0, mem_weight=0.4 + task_weight=0.4 + net_penalty=0.2
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `node_id` | `str` | required | Unique node identifier |
| `hostname` | `str` | `""` | Machine hostname |
| `ip_address` | `str` | `""` | IP address |
| `port` | `int` | `9755` | Agent port |
| `total_memory_gb` | `float` | `0.0` | Total unified memory |
| `available_memory_gb` | `float` | `0.0` | Available memory |
| `status` | `NodeStatus` | `OFFLINE` | Node status |
| `last_heartbeat` | `float` | `0.0` | Last heartbeat timestamp |
| `task_count` | `int` | `0` | Current assigned tasks |
| `link_type` | `str` | `""` | Network link type |
| `latency_ms` | `float` | `0.0` | Network latency |

### ClusterTask

```python
task = ClusterTask(
    task_id="task_1",
    name="batch-inference",
    mode=ParallelMode.DATA,
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `task_id` | `str` | required | Unique task identifier |
| `name` | `str` | `""` | Task name |
| `mode` | `ParallelMode` | `PIPELINE` | Pipeline or data parallel |
| `status` | `TaskStatus` | `PENDING` | Current status |
| `assigned_node` | `str` | `None` | Assigned node ID |
| `model` | `str` | `None` | Model name |
| `timeout` | `float` | `300.0` | Timeout in seconds |
| `created_at` | `float` | `time.time()` | Creation timestamp |
| `started_at` | `float` | `None` | Start timestamp |
| `completed_at` | `float` | `None` | Completion timestamp |
| `error` | `str` | `None` | Error message if failed |

### Enums

| Enum | Values |
|------|--------|
| `NodeStatus` | `OFFLINE`, `ONLINE`, `BUSY`, `ERROR` |
| `ParallelMode` | `PIPELINE`, `DATA` |
| `TaskStatus` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `MIGRATED`, `TIMEOUT` |

---

## Node Agent

`from fusion_multi_node.agent import NodeAgent, AgentConfig`

### NodeAgent

```python
config = AgentConfig(node_id="my_mac", master_host="10.0.0.1")
agent = NodeAgent(config)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `start` | `() -> None` | Start heartbeat loop and task polling |
| `stop` | `() -> None` | Stop agent |
| `collect_hardware_info` | `() -> dict` | Return CPU/memory/GPU info via psutil |
| `execute_task` | `(task: dict) -> dict` | Execute task via fusion-mlx httpx API |
| `report_fault` | `(error: str) -> None` | Report fault to master |

### AgentConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `node_id` | `str` | `""` | Node identifier |
| `master_host` | `str` | `"localhost"` | Master hostname |
| `master_port` | `int` | `9753` | Master port |
| `discovery_port` | `int` | `9754` | mDNS discovery port |
| `heartbeat_interval` | `float` | `5.0` | Heartbeat interval (seconds) |
| `task_poll_interval` | `float` | `2.0` | Task poll interval (seconds) |

---

## mDNS Discovery

`from fusion_multi_node.discovery import MDNSDiscovery, DiscoveryInfo`

### MDNSDiscovery

```python
mdns = MDNSDiscovery(node_id="fusion-master")
mdns.register(port=9753, properties={"role": "master"})
master_info = await mdns.find_master_async(timeout=5.0)
mdns.unregister()
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `register` | `(port: int, properties: dict = None) -> None` | Register mDNS service |
| `unregister` | `() -> None` | Unregister service |
| `browse` | `(timeout: float = 5.0) -> list[DiscoveryInfo]` | Browse for services |
| `find_master` | `(timeout: float = 5.0) -> DiscoveryInfo or None` | Synchronous master lookup |
| `find_master_async` | `(timeout: float = 5.0) -> DiscoveryInfo or None` | Async master lookup |

### DiscoveryInfo

| Field | Type | Description |
|-------|------|-------------|
| `node_id` | `str` | Service node ID |
| `host` | `str` | Host address |
| `port` | `int` | Service port |
| `properties` | `dict` | Service properties |

---

## FMP Protocol

`from fusion_multi_node.protocol import FMPMessage, PayloadType, MessageType, FMPCrypto, CircuitBreaker, CircuitBreakerState, FMPConnectionManager, FMPRouter`

### FMPMessage

```python
msg = FMPMessage.create("master", "node1", PayloadType.HEARTBEAT, {"status": "ok"})
data = msg.serialize()     # bytes
msg2 = FMPMessage.deserialize(data)  # FMPMessage
```

Three layers:
- **LinkLayer**: `source`, `destination`, `hop_count`, `timestamp`
- **BusinessLayer**: `payload_type`, `payload` (dict), `rounds`, `priority`
- **ControlLayer**: `message_type`, `ack_required`, `flow_control`

| Method | Signature | Description |
|--------|-----------|-------------|
| `create` | `(source, dest, ptype, payload, **) -> FMPMessage` | Create new message |
| `serialize` | `() -> bytes` | Serialize to binary |
| `deserialize` | `(data: bytes) -> FMPMessage` | Deserialize from binary |

### FMPCrypto

```python
key = FMPCrypto.generate_key()        # 32 bytes
crypto = FMPCrypto(key=key)
encrypted = crypto.encrypt_message(msg)
crypto.decrypt_message(msg)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `generate_key` | `() -> bytes` | Generate 256-bit AES key |
| `encrypt_message` | `(msg: FMPMessage) -> None` | Encrypt message payload in-place |
| `decrypt_message` | `(msg: FMPMessage) -> None` | Decrypt message payload in-place |

### CircuitBreaker

```python
cb = CircuitBreaker(name="node1", failure_threshold=5, recovery_timeout=30.0)
if cb.allow_request():
    try:
        result = await do_work()
        cb.record_success()
    except Exception:
        cb.record_failure()
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `allow_request` | `() -> bool` | Check if request is allowed |
| `record_success` | `() -> None` | Record successful call |
| `record_failure` | `() -> None` | Record failed call |

States: `CLOSED` -> `OPEN` (after threshold failures) -> `HALF_OPEN` (after recovery_timeout)

---

## Distributed MLX Bridge

`from fusion_multi_node.distributed_mlx import DistributedMLXBridge, KVSharingManager, CavemanManager, CavemanCompressor`

### DistributedMLXBridge

```python
bridge = DistributedMLXBridge()
shards = await bridge.shard_model("llama-70b", num_shards=4)
result = await bridge.pipeline_inference("llama-70b", "Hello", ["n1","n2","n3","n4"])
result = await bridge.data_parallel_inference("qwen-7b", ["prompt1","prompt2"], ["n1","n2"])
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `shard_model` | `(model: str, num_shards: int) -> list[ModelShard]` | Split model into shards |
| `pipeline_inference` | `(model, prompt, nodes) -> dict` | Pipeline parallel inference |
| `data_parallel_inference` | `(model, prompts, nodes) -> dict` | Data parallel inference |
| `sync_weights` | `(model: str, nodes: list) -> bool` | Sync model weights across nodes |

### KVSharingManager

```python
kv = KVSharingManager(max_local_cache_mb=4096.0)
kv.store_local(entry)
found = kv.lookup_local("qwen", "abc123")
prefixed = kv.lookup_by_prefix("qwen", "abc")
await kv.warm_cache("qwen", "Hello world", ["n1"])
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `store_local` | `(entry: KVCacheEntry) -> None` | Store in local cache |
| `lookup_local` | `(model, prompt_hash) -> KVCacheEntry or None` | Look up by exact hash |
| `lookup_by_prefix` | `(model, prefix_hash) -> list[KVCacheEntry]` | Look up by prefix |
| `lookup_remote` | `(model, prompt_hash, source_node, source_ip) -> KVCacheEntry or None` | Look up on remote node |
| `warm_cache` | `(model, prompt, nodes) -> None` | Pre-warm cache on nodes |
| `evict_lru` | `() -> int` | Evict least recently used entries |

### CavemanManager

```python
manager = CavemanManager()
compressed, method, stats = await manager.compress_tensor(data, link_type="ethernet_1g")
decompressed = await manager.decompress_tensor(compressed, method, original_shape, original_dtype)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `compress_tensor` | `(data, link_type) -> (bytes, str, dict)` | Auto-select and compress |
| `decompress_tensor` | `(data, method, shape, dtype) -> ndarray` | Decompress tensor |

**Link-type selection**: Thunderbolt -> dict, Ethernet -> zlib, WiFi -> diff

---

## MCP Cluster Gateway

`from fusion_multi_node.mcp_gateway import MCPClusterGateway, MCPTool, MCPRequest`

### MCPClusterGateway

```python
gateway = MCPClusterGateway(host="0.0.0.0", port=9756)
gateway.register_tool(MCPTool(name="code_review", description="Review code",
                               parameters={"type":"object","properties":{"code":{"type":"string"}}}))
result = await gateway.handle_tool_call("code_review", {"code": "..."}, source="claude_code")
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `register_tool` | `(tool: MCPTool) -> None` | Register a tool |
| `unregister_tool` | `(name: str) -> None` | Unregister a tool |
| `list_tools` | `() -> list[MCPTool]` | List all tools |
| `handle_tool_call` | `(name, args, source) -> dict` | Route and execute tool call |
| `get_token_usage` | `() -> dict` | Get token budget usage |

### MCPTool

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Tool name |
| `description` | `str` | Tool description |
| `parameters` | `dict` | JSON Schema parameters |
| `node_id` | `str` | Source node ID |

---

## Cluster Observability

`from fusion_multi_node.observability import ClusterObservability, MetricPoint, Alert, LogEntry`

### ClusterObservability

```python
obs = ClusterObservability(retention_hours=24.0)
obs.record_metric("node_1", "memory_used_gb", 16.0, tags={"gpu": "m4_ultra"})
obs.add_log(LogEntry(time.time(), "node_1", "INFO", "scheduler", "Task completed"))
alert = obs.create_alert("warning", "High memory", "node_1 at 90%")
await obs.check_alert_rules(nodes)
report = obs.get_cluster_report()
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `record_metric` | `(node_id, name, value, tags=None) -> None` | Record a metric |
| `add_log` | `(entry: LogEntry) -> None` | Add a log entry |
| `create_alert` | `(level, title, message) -> Alert` | Create an alert |
| `check_alert_rules` | `(nodes: dict) -> list[Alert]` | Evaluate alert rules |
| `get_cluster_report` | `() -> dict` | Generate cluster summary |
| `cleanup_expired` | `() -> int` | Remove expired data |

---

## Configuration

`from fusion_multi_node.config import ClusterConfig`

### ClusterConfig

```python
config = ClusterConfig()
config.set("cluster.master_host", "10.0.0.1")
host = config.get("cluster.master_host")
agent_config = config.to_node_agent_config()
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `(key: str, default=None) -> Any` | Get value by dot-notation key |
| `set` | `(key: str, value) -> None` | Set value by dot-notation key |
| `to_node_agent_config` | `() -> AgentConfig` | Convert to agent config |

---

## Utilities

`from fusion_multi_node.utils import NetworkTopologyDetector, setup_logger, get_data_dir, get_log_dir`

### NetworkTopologyDetector

```python
detector = NetworkTopologyDetector()
topology = detector.detect()
link = detector.detect_link_type("10.0.0.1")
latency = detector.measure_latency("10.0.0.1")
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `detect` | `() -> dict` | Detect all network interfaces |
| `detect_link_type` | `(ip: str) -> str` | Detect link type for IP |
| `measure_latency` | `(ip: str) -> float` | Measure latency in ms |

### Logger

```python
logger = setup_logger("my_module")
logger.info("Hello")
```

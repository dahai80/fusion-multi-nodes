# Python API Reference — fusion-multi-node

Class-level API for programmatic use. For HTTP routes see [API.md](API.md).

## Contents

- [Cluster Master](#cluster-master)
- [Node Agent](#node-agent)
- [mDNS Discovery](#mdns-discovery)
- [FMP Protocol](#fmp-protocol)
- [Distributed MLX Bridge](#distributed-mlx-bridge)
- [MCP Cluster Gateway](#mcp-cluster-gateway)
- [Cluster Observability](#cluster-observability)
- [Security & Multi-Tenant (GAP-8)](#security--multi-tenant-gap-8)
- [Configuration](#configuration)
- [Utilities](#utilities)

---

## Cluster Master

`from fusion_multi_node.master import ClusterMaster, NodeInfo, ClusterTask, KVCacheEntry, NodeStatus, ParallelMode, TaskStatus`

### ClusterMaster

```python
master = ClusterMaster(host="127.0.0.1", port=11452)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `register_node` | `(node: NodeInfo) -> bool` | Register/update node (F-A12 PATCH 幂等). Returns `True` ok, `False` if banned |
| `unregister_node` | `(node_id: str, reason: str = "") -> None` | Remove node; `reason="banned"` writes blacklist |
| `report_fault` | `(node_id: str, fault_type: str = "", message: str = "") -> bool` | Mark node FAULT + count; threshold in window → auto-ban (F-A13) |
| `is_node_banned` | `(node_id: str) -> bool` | Whether node is in ban window |
| `unban_node` | `(node_id: str) -> bool` | Manually lift ban |
| `assign_task` | `(task: ClusterTask) -> bool` | Assign task to best node; queues PENDING if no nodes |
| `complete_task` | `(task_id: str, error: str = None) -> ClusterTask` | Complete or fail a task |
| `cancel_task` | `(task_id: str) -> bool` | Cancel a pending/running task (recursive sub_tasks) |
| `migrate_task` | `(task_id: str) -> bool` | Manually migrate task to another node (转 MIGRATED → 重派). **注**: MIGRATED 语义亦由 P1-15 自动路径满足 — 节点 OFFLINE 时其 RUNNING 任务自动转 PENDING + 源节点并入 `exclude_nodes` 重派 (等价自动迁移), 手动 API 为显式运维操作 |
| `degrade_task` | `(task_id: str) -> bool` | Degrade model (chain 70b→...→1b, max 2) |
| `check_heartbeat` | `() -> list[str]` | Check all nodes, return stale node IDs |
| `find_kv_cache` | `(model: str, prompt_hash: str) -> KVCacheEntry or None` | Look up KV cache by model+hash |
| `add_kv_cache` | `(entry: KVCacheEntry)` | Add entry to global KV cache pool |
| `acquire_chat_slot` / `release_chat_slot` | `(user_id: str) -> bool` / `(user_id: str) -> None` | F3 tenant-quota inflight counter vs `tenant_max_concurrent` |

**Public attributes**: `nodes`, `tasks`, `kv_cache`, `load_router: LoadRouter`, `_observability`.

### NodeInfo

| Field | Type | Description |
|-------|------|-------------|
| `node_id` | `str` | Unique node identifier |
| `hostname` | `str` | Machine hostname |
| `ip_address` | `str` | IP address |
| `port` | `int` | Agent port (11458) |
| `total_memory_gb` / `available_memory_gb` | `float` | Unified memory |
| `status` | `NodeStatus` | Node status |
| `role` | `str` | MASTER/worker |
| `cpu_cores` / `gpu_cores` | `int` | Hardware |
| `device_model` / `uma_size_gb` | `str` / `float` | Device model / UMA |
| `active_tasks` / `max_tasks` | `int` | Task slots |
| `score` | `float` | 0.0–1.0 routing score |
| `last_heartbeat` | `float` | Last heartbeat timestamp |

### ClusterTask

| Field | Type | Description |
|-------|------|-------------|
| `task_id` / `name` | `str` | Task id / name |
| `mode` | `ParallelMode` | PIPELINE/DATA |
| `status` | `TaskStatus` | PENDING/RUNNING/COMPLETED/FAILED/TIMEOUT/MIGRATED/PARTIAL/CANCELLED |
| `assigned_nodes` | `list[str]` | Assigned node IDs |
| `model_name` | `str` | Model name |
| `priority` | `int` | Priority (higher = sooner) |
| `user` | `str` | Authenticated owner (F2) |
| `sub_tasks` | `list[str]` | Child task IDs |
| `result` | `dict` | Result payload |

### Enums

| Enum | Values |
|------|--------|
| `NodeStatus` | `OFFLINE`, `ONLINE`, `BUSY`, `FAULT` |
| `ParallelMode` | `PIPELINE`, `DATA` |
| `TaskStatus` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `MIGRATED`, `TIMEOUT`, `PARTIAL`, `CANCELLED` |

> **MIGRATED 语义** (P3-3 校准): 由手动 `migrate_task` API 与 P1-15 自动路径共同设置 — 节点 OFFLINE 时其 RUNNING 任务经 `_refresh_node_statuses` 自动转 PENDING + 源节点并入 `exclude_nodes` 重派 (等价自动迁移), 非"仅手动"。手动 API 保留显式运维迁移。

---

## Node Agent

`from fusion_multi_node.agent import NodeAgent, AgentConfig, FusionMLXBackend`

### NodeAgent

```python
config = AgentConfig(node_id="my_mac", master_host="127.0.0.1", master_port=11452)
agent = NodeAgent(config)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `start` / `stop` | `() -> None` | Lifecycle (heartbeat + task poll) |
| `collect_hardware_info` | `() -> dict` | CPU/memory/GPU via psutil |
| `execute_task` | `(task: dict) -> dict` | Execute via fusion-mlx; dedup by `task_id` (P1-14) |
| `report_fault` | `(fault_type, message) -> bool` | Report fault to master |

### AgentConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `node_id` | `str` | `""` | Node identifier |
| `master_host` / `master_port` | `str`/`int` | `127.0.0.1`/`11452` | Master address |
| `fusion_mlx_port` | `int` | `11432` | Upstream fusion-mlx |
| `heartbeat_interval` / `task_poll_interval` | `float` | `5.0`/`2.0` | Loops |

### FusionMLXBackend

HTTP client to fusion-mlx. `chat` / `embed` carry `Authorization: Bearer {api_key}`; GAP-6 pacing (429 `Retry-After`, exp backoff, `RateLimitExhausted`).

---

## mDNS Discovery

`from fusion_multi_node.discovery import MDNSDiscovery, DiscoveryInfo`

### MDNSDiscovery

| Method | Signature | Description |
|--------|-----------|-------------|
| `register` | `(port, properties=None) -> None` | Register mDNS service |
| `unregister` | `() -> None` | Unregister |
| `find_master_async` | `(timeout=5.0) -> DiscoveryInfo or None` | Async master lookup |

`DiscoveryInfo`: `node_id`, `host`, `port`, `properties`.

---

## FMP Protocol

`from fusion_multi_node.protocol import FMPMessage, PayloadType, FMPCrypto, CircuitBreaker, FMPConnectionManager, FMPRouter`

### FMPMessage — three layers (Link/Business/Control)

| Method | Description |
|--------|-------------|
| `create(source, dest, ptype, payload, **) -> FMPMessage` | Create |
| `serialize() -> bytes` / `deserialize(data) -> FMPMessage` | Binary codec |

### FMPCrypto — AES-256-GCM (+ MetalCryptoBackend CommonCrypto)

| Method | Description |
|--------|-------------|
| `generate_key() -> bytes` | 256-bit key |
| `encrypt_message(msg)` / `decrypt_message(msg)` | In-place |

### CircuitBreaker — `CLOSED → OPEN (threshold) → HALF_OPEN (recovery_timeout)`

| Method | Description |
|--------|-------------|
| `allow_request() -> bool` / `record_success()` / `record_failure()` | State machine |

---

## Distributed MLX Bridge

`from fusion_multi_node.distributed_mlx import DistributedMLXBridge, KVSharingManager, CavemanManager`

### DistributedMLXBridge

| Method | Description |
|--------|-------------|
| `shard_model(model, num_shards) -> list[ModelShard]` | Split model |
| `pipeline_inference(model, prompt, nodes) -> dict` | Pipeline parallel |
| `data_parallel_inference(model, prompts, nodes) -> dict` | Data parallel |
| `sync_weights(model, nodes) -> bool` | Sync weights |

### KVSharingManager — cross-node KV cache (disk-persisted, P1-9)

| Method | Description |
|--------|-------------|
| `store_local(entry)` / `lookup_local(model, hash)` | Local cache |
| `lookup_remote(model, hash, node, ip)` / `transfer_from_remote(...)` | Cross-node HTTP |
| `warm_cache(model, prompt, nodes)` | Pre-warm |
| `save()` / `load()` | Disk persistence (`~/.fusion/multi-node/kv_cache.json`) |

### CavemanManager — tensor compression (Thunderbolt→dict, Ethernet→zlib, WiFi→diff)

| Method | Description |
|--------|-------------|
| `compress_tensor(data, link_type) -> (bytes, str, dict)` | Auto-select + compress |
| `decompress_tensor(data, method, shape, dtype) -> ndarray` | Decompress |

---

## MCP Cluster Gateway

`from fusion_multi_node.mcp_gateway import MCPClusterGateway, MCPTool`

> **Migration-debt** — not wired (dead code), pending migration to fusion-gateway #106.

| Method | Description |
|--------|-------------|
| `register_tool(tool)` / `unregister_tool(name)` / `list_tools()` | Tool registry |
| `handle_tool_call(name, args, source) -> dict` | Route + execute |

---

## Cluster Observability

`from fusion_multi_node.observability import ClusterObservability, LogStore`

### ClusterObservability

| Method | Description |
|--------|-------------|
| `record_metric(node_id, name, value, tags=None)` | Record metric |
| `create_alert(severity, title, message) -> Alert` | Create alert |
| `check_alert_rules(nodes) -> list[Alert]` | Evaluate rules |
| `generate_optimization_suggestions() -> list[dict]` | Suggestions (op #9) |
| `get_prometheus_metrics() -> str` | exposition 0.0.4 |

### LogStore — `export_logs(fmt)`, `FaultDiagnoser`.

---

## Security & Multi-Tenant (GAP-8)

### UserStore — `from fusion_multi_node.security.user_store import UserStore, UserRole`

File-based (`~/.fusion/multi-node/users.json`, scrypt hashes, 0600). Token format `fmu_<userid>_<secret>`, plaintext returned ONCE.

| Method | Description |
|--------|-------------|
| `create_user(user_id, role=USER) -> str` | Create + return token once |
| `issue_token(user_id) -> str` / `revoke_token(user_id, tid)` / `rotate_user_token(user_id) -> dict` | Token lifecycle (F5 rotation) |
| `validate(token) -> (user_id, UserRole) or None` | Constant-time |
| `bootstrap_admin(user_id) -> str` | First-run ADMIN (env `FUSION_BOOTSTRAP_ADMIN`) |

### UserRole — `ADMIN`, `USER`, `VIEWER` (orthogonal to `NodeRole`)

`check_user_path_access(role, path, method)` — user-RBAC path gate.

### BearerAuthMiddleware — `from fusion_multi_node.utils.auth import BearerAuthMiddleware`

Dual-token: `fmu_` prefix → UserStore lookup (resolves role, injects scope); else cluster-token (O(1)). `FUSION_CLUSTER_TOKEN_PREVIOUS` accepts rolling-restart overlap (F5).

### AuditLogger — `from fusion_multi_node.security.audit_log import get_audit_logger`

Append JSONL `~/.fusion/multi-node/audit.log` (ts/actor/action/path/method/node_id/result). Wired at auth-fail, permission-deny, register/approve/task submit+cancel.

### mTLS — `fusion_multi_node.security.mtls` — env `FUSION_MTLS_ENABLED` (fail-closed: missing certs → RuntimeError, no plaintext fallback).

---

## Configuration

`from fusion_multi_node.config import ClusterConfig`

### ClusterConfig — `~/.fusion/multi-node/config.json` (deep-merged over `DEFAULT_CONFIG`)

| Method | Description |
|--------|-------------|
| `get(key, default=None)` / `set(key, value)` | Dot-notation access |
| `to_node_agent_config() -> AgentConfig` | Convert |
| `save()` | Persist |

Auto-migrates stale ports on load (`_migrate_stale_ports`).

---

## Utilities

`from fusion_multi_node.utils import NetworkTopologyDetector, setup_logger`

### NetworkTopologyDetector

| Method | Description |
|--------|-------------|
| `detect() -> dict` | Detect all network interfaces (async via `to_thread`) |
| `detect_link_type(ip) -> str` / `measure_latency(ip) -> float` | Per-IP |

### setup_logger — `StreamHandler`(stdout); env `FUSION_MULTINODE_LOG_FILE` → `RotatingFileHandler` 10MB×5.

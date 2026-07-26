# Changelog — fusion-multi-node

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-07-26

### Added

- **LoadMetrics + LoadRouter** (`master/load_metrics.py`)
  - Five-dimensional load metrics: uma_used_ratio, cpu_percent, metal_util, task_queue_len, net_rtt_ms
  - Four routing strategies: BALANCED, VRAM_FIRST, LOCALITY_FIRST, LOW_LATENCY
  - LocalForcedGate: ≤0.5B models forced local execution
  - VRAM-first scheduling: ≥13B models routed to highest-VRAM node

- **Task Sharding** (`master/task_sharding.py`)
  - ShardingType: INFERENCE, AST, VECTORIZE
  - ShardingStrategy: BY_FILE, BY_DOCUMENT, BY_BATCH
  - ShardResult merge with ordering and dedup

- **AST Diff** (`master/ast_diff.py`)
  - compute_ast_diff: added/removed/modified node detection
  - apply_ast_diff: incremental AST reconstruction

- **FMP KV Cache Sync** (`protocol/fmp_message.py`, `distributed_mlx/kv_cache_sharing.py`)
  - KVCacheSyncMessage with FMP protocol
  - sync_to_cluster() in KVSharingManager

- **Storage Enhancements** (`storage/storage_volume.py`, `storage/shard_replication.py`)
  - Capacity monitoring with configurable thresholds
  - LRU auto-eviction for cache volumes
  - ShardReplicator with SHA-256 checksum verification
  - distribute_shard() for model shard distribution to Worker volumes

- **NodeInfo Extensions**
  - device_model + uma_size_gb in NodeInfo, mDNS properties, and registration API
  - role field added to NodeInfo (master/worker/standby)
  - NodeStatus.FAULT state added

- **Node Approval Integration** (`server/master_server.py`)
  - /api/nodes/register now routes through NodeApprovalManager
  - Unapproved nodes blocked from joining cluster

- **Monitoring API** (`server/master_server.py`)
  - GET /api/v1/nodes/{node_id}/metrics — node load metrics
  - GET /api/v1/tasks/{task_id}/progress — task execution progress

- **Log Level Standardization** (`observability/log_store.py`)
  - LogLevel enum: INFO, WARN, ERROR, FATAL
  - Master log aggregation via collect_node_logs()

- **Autoscaler Built-in Actions** (`autoscaler/autoscaler.py`)
  - scale_up: activate standby nodes
  - scale_down: migrate tasks via ClusterMaster.migrate_task() then deactivate

- **Sandbox Resource Limits** (`security/sandbox.py`)
  - CPU/memory/disk limit enforcement via resource module
  - macOS sandbox-exec integration notes

- **Timeout Auto-retry** (`master/cluster_master.py`)
  - Timed-out tasks auto-enter retry queue (1 retry per architecture spec)

### Changed

- Heartbeat interval: 5.0s → 3.0s (DEFAULT_CONFIG)
- Retry attempts: 3 → 1 (architecture spec alignment)
- pyproject.toml: added protobuf>=5.0.0 dependency
- pyproject.toml: version bumped 0.3.0 → 0.4.0
- protocol/__init__.py: exports FMPProtoMessage, FMPEnvelope, FMPControl, FMPPayload

### Fixed

- AST diff _find_node/_remove_nodes root-path traversal bug
- ClusterConfig.save() PermissionError on system dirs (chmod try/except)
- test_get_gpu_cores → test_get_gpu_info method rename

## [0.3.0] - 2026-07-25

### Added

- Full audit remediation (P0-P3, 53 findings)
- asyncio.Lock for all shared mutable state
- Unbounded dict cleanup (tasks/nodes/requests/shards/pipelines/rounds/hot_prompts)
- TLS certificate fingerprint pinning
- Task retry queue for assign_task failures
- O(1) KV lookup index
- httpx connection reuse
- InferenceBackend protocol decoupling
- StandbyMaster HA stub
- msgpack serialization option
- Load-aware task assignment
- MDNSDiscovery async browse rewrite

### Fixed

- 53 audit findings across P0-P3 severity levels
- All ruff lint errors resolved

## [0.2.0] - 2026-07-25

### Added

- BearerAuth middleware for all API endpoints
- SSRF validation on user-supplied URLs
- AES-GCM AAD (Additional Authenticated Data) binding
- InMemoryRateLimiter (time-driven, sliding window)
- ECDH key exchange + TLS protocol extension
- mDNS shared-secret verification
- encrypt_message immutability guarantee
- ASGI auth + rate-limit middleware

### Fixed

- 16 security findings (4 CRITICAL) from initial security audit

## [0.1.0] - 2026-07-25

### Added

- **Cluster Master** — node registration, score-based selection, task lifecycle, KV cache pool, heartbeat monitoring
- **Node Agent** — hardware info collection, heartbeat, task execution, mDNS auto-discovery
- **mDNS Discovery** — Bonjour zero-config service registration and browsing
- **FMP Protocol** — three-layer binary protocol, AES-GCM encryption, circuit breaker, hop_count
- **Distributed MLX Bridge** — model sharding, pipeline/data parallelism, Caveman compression, KV cache sharing
- **MCP Cluster Gateway** — tool registration, node selection, request forwarding
- **Cluster Observability** — metrics, logs, alerts, cluster reports
- **Configuration** — JSON config with dot-notation, recursive merge
- **CLI** — 15+ commands across 7 groups
- 585 tests, 96.1% code coverage

[0.4.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dahai80/fusion-multi-node/releases/tag/v0.1.0

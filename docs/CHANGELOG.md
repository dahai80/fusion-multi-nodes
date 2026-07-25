# Changelog — fusion-multi-node
# User instruction: "生成doc文档和READMD，README_CN，提交配置库，发布0.1.0版本"
# Records all changes for v0.1.0 release

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-25

### Added

- **Cluster Master** (`fusion_multi_node.master`)
  - `ClusterMaster` — node registration, score-based selection, task lifecycle, KV cache pool, heartbeat monitoring
  - `NodeInfo` — node dataclass with `score` property (mem 0.4 + task 0.4, net penalty 0.2)
  - `ClusterTask` — task lifecycle: PENDING → RUNNING → COMPLETED/FAILED/MIGRATED/TIMEOUT
  - `KVCacheEntry` — global KV cache entry with TTL and access tracking
  - Enums: `NodeStatus`, `ParallelMode`, `TaskStatus`

- **Node Agent** (`fusion_multi_node.agent`)
  - `NodeAgent` — hardware info collection (psutil), heartbeat, task execution (httpx → fusion-mlx), fault reporting
  - `AgentConfig` — agent configuration (master_host, ports, intervals)

- **mDNS Discovery** (`fusion_multi_node.discovery`)
  - `MDNSDiscovery` — Bonjour/mDNS zero-config service registration and browsing
  - `DiscoveryInfo` — discovered service dataclass
  - Async master discovery with `find_master_async()`

- **FMP Protocol** (`fusion_multi_node.protocol`)
  - `FMPMessage` — three-layer binary message (LinkLayer, BusinessLayer, ControlLayer)
  - `FMPCrypto` — AES-GCM encryption for inter-node communication
  - `CircuitBreaker` — CLOSED → OPEN → HALF_OPEN state machine fault tolerance
  - `FMPConnectionManager` — TCP long connection management with retry
  - `FMPRouter` — message routing and dispatch
  - Enums: `PayloadType`, `MessageType`, `CircuitBreakerState`

- **Distributed MLX Bridge** (`fusion_multi_node.distributed_mlx`)
  - `DistributedMLXBridge` — model sharding, pipeline/data parallel inference, weight sync
  - `KVSharingManager` — LRU eviction, local store/lookup, prefix match, remote lookup, cache warm
  - `CavemanCompressor` — zlib/diff/dict compression methods, auto-select
  - `CavemanManager` — link-type aware compression (Thunderbolt→dict, Ethernet→zlib, WiFi→diff)
  - Dataclasses: `ModelShard`, `DistConfig`, `KVCacheEntry`, `KVShard`
  - Enums: `DistMode`, `CompressionMethod`, `LinkType`

- **MCP Cluster Gateway** (`fusion_multi_node.mcp_gateway`)
  - `MCPClusterGateway` — tool registration, node selection, request forwarding, token budget tracking
  - `MCPTool`, `MCPRequest` dataclasses

- **Cluster Observability** (`fusion_multi_node.observability`)
  - `ClusterObservability` — metrics, logs, alerts, alert rules, cluster reports, retention cleanup
  - `MetricPoint`, `Alert`, `LogEntry` dataclasses

- **Configuration** (`fusion_multi_node.config`)
  - `ClusterConfig` — JSON config with dot-notation get/set, recursive merge, `to_node_agent_config()`
  - Default path: `~/.fusion/multi-node/config.json`

- **Utilities** (`fusion_multi_node.utils`)
  - `NetworkTopologyDetector` — Thunderbolt/Ethernet/WiFi/loopback detection, latency measurement
  - `setup_logger`, `get_data_dir`, `get_log_dir`

- **CLI** (`fusion_multi_node.cli`)
  - 15+ commands across 7 groups: cluster, node, task, config, network, caveman, kv
  - `fusion-multi-node --version` shows v0.1.0

### Testing

- 585 tests passing
- 96.1% code coverage
- Full module coverage: master, agent, discovery, protocol, distributed_mlx, mcp_gateway, observability, config, utils, cli

[0.1.0]: https://github.com/dahai80/fusion-multi-node/releases/tag/v0.1.0

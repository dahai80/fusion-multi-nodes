# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fusion-multi-node is a distributed Apple Silicon MLX cluster orchestration library. It provides Bonjour/mDNS zero-config node discovery, FMP (Fusion Message Protocol) binary communication with circuit breaker fault tolerance, pipeline/data parallel inference, KV cache sharing, Caveman token compression, MCP gateway for Claude integration, and cluster observability. Serves as shared cluster infrastructure for fusion-mlx, fusion-agent-studio, and fusion-code.

## Commands

```bash
source .venv/bin/activate   # Enter project environment (required first)
pip install -e .             # Install editable
pip install -e ".[test]"     # Install with test dependencies
pip install -e ".[all]"      # Install all optional dependencies
pytest tests/ -q             # Run all tests
pytest tests/test_core.py::TestClass::test_name -q   # Run single test
fusion-multi-node --help     # CLI help
```

## Architecture

Modular package structure under `fusion_multi_node/`:

### Core Modules

- **`master/`** — Cluster master orchestration
  - `ClusterMaster` — Global scheduler: node registration, task assignment, migration, timeout, KV cache pool, health check loop
  - `NodeInfo` — Node dataclass with `score` property (mem 0.4 + task 0.4, net penalty 0.2)
  - `ClusterTask` — Task lifecycle: PENDING → RUNNING → COMPLETED/FAILED/MIGRATED/TIMEOUT
  - `KVCacheEntry` — Global KV cache entry with TTL and access tracking
  - Enums: `NodeStatus`, `ParallelMode`, `TaskStatus`

- **`agent/`** — Node agent
  - `NodeAgent` — Hardware info collection (psutil), heartbeat, task execution (httpx → fusion-mlx), fault reporting
  - `AgentConfig` — Agent configuration (master_host, ports, intervals)

- **`distributed_mlx/`** — Distributed MLX inference
  - `DistributedMLXBridge` — Model sharding, pipeline/data parallel inference, weight sync
  - `KVSharingManager` — LRU eviction, local store/lookup, prefix match, remote lookup, cache warm, compression
  - `CavemanCompressor` — zlib/diff/dict compression methods, auto-select
  - `CavemanManager` — Link-type aware compression (Thunderbolt→dict, Ethernet→zlib, WiFi→diff)
  - Dataclasses: `ModelShard`, `DistConfig`, `DistMode`, `KVCacheEntry`, `KVShard`

- **`mcp_gateway/`** — MCP gateway for Claude integration
  - `MCPClusterGateway` — Tool registration, node selection, request forwarding, token budget tracking
  - `MCPTool`, `MCPRequest` dataclasses

- **`observability/`** — Cluster observability
  - `ClusterObservability` — Metrics, logs, alerts, alert rules, cluster reports, retention cleanup
  - `MetricPoint`, `Alert`, `LogEntry` dataclasses

- **`config/`** — Configuration
  - `ClusterConfig` — JSON config with dot-notation get/set, recursive merge, `to_node_agent_config()`
  - Default path: `~/.fusion/multi-node/config.json`

- **`utils/`** — Utilities
  - `NetworkTopologyDetector` — Thunderbolt/Ethernet/WiFi/loopback detection, latency measurement
  - `setup_logger`, `get_data_dir`, `get_log_dir`

- **`cli.py`** — Click CLI: node, cluster, task, config, network, caveman, kv command groups

### Key Constants

| Constant | Default | Purpose |
|---|---|---|
| Master port | 9753 | Cluster master service port |
| Discovery port | 9754 | mDNS discovery port |
| Agent port | 9755 | Node agent port |
| Heartbeat timeout | 15.0s | Stale node threshold |
| Task timeout | 300.0s | Default task timeout |
| KV cache TTL | 3600.0s | Default KV cache expiry |
| Token budget | 10,000,000 | MCP gateway token limit |

## Conventions

- Python >= 3.11, 4-space indentation (multiples of 4, no 5/9/11 spaces)
- All classes use `logging.getLogger(__name__)` — no print statements
- No docstrings — clean code, self-documenting names
- Dataclasses for data models, no manual to_dict/from_dict unless needed
- External deps: httpx, psutil, click (core); zeroconf, fastapi, uvicorn (optional)
- Tests in `tests/`, pytest with pytest-asyncio for async tests

<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>Cluster scheduling core for distributed Apple Silicon MLX inference</strong></p>
  <p><em>Pool multiple Macs into a unified AI cluster — pipeline parallelism, data parallelism, 100% local-first.</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.14.2-blue" alt="Version">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License">
  <img src="https://img.shields.io/badge/tests-1347%20passed-brightgreen" alt="Tests">
</p>

---

> **🐛 v0.14.2 (2026-09-02) — Containerized agent deep-health readiness fix (issue #60)**
>
> The agent `/api/health/deep` readiness probe — used by the Docker healthcheck — never went healthy for containerized agents,
> leaving containers perpetually `(unhealthy)` despite registering online. Three fixes: (1) probe now honors `FUSION_MLX_URL`
> (was falling back to `localhost:11432`, a gateway port); (2) the `/v1/models` probe now sends the `Authorization: Bearer`
> api_key header (was getting `401` with auth enabled); (3) remote/host MLX no longer misclassified as down by the local socket
> check. Containerized agents now report `status: ok` and go `(healthy)`. 1347 tests, ruff clean. See [CHANGELOG](docs/CHANGELOG.md).

> **📦 v0.14.2-rc.1 (Release Candidate) — 2026-08-28**
>
> RC — v0.14.1 final baseline packaged as a candidate. Content = HEAD (enterprise 7 blockers v0.14.0 + TarSlip security patch v0.14.1),
> no new code changes. **Not GA.** 1343 tests, ruff clean, randomized-order bidirectional green. See [CHANGELOG](docs/CHANGELOG.md).

> **🔒 v0.14.1 (2026-08-28) — Security patch: backup restore path-escape hardening**
>
> `backup restore` TarSlip variant fix — symlink/hardlink `linkname` out-of-bounds validation +
> `extractall(filter="data")` (PEP 706) backstop. Does not assume backups are trusted (Rule 12). 1343 tests, ruff clean.

> **📦 v0.14.0 (2026-08-28) — Enterprise production-readiness blockers fixed (7 items)**
>
> All 7 enterprise production blockers landed: (1) HA `cluster start` wiring-gap fix (that path never started HA before);
> (2) observability persistence on by default (`observability.persist=True` + `_cleanup_loop` 300s periodic save, no longer lost on restart);
> (3) alert outbound webhook config section (env-first, non-zero config means no alerts); (4) mTLS config section + lazy `is_enabled()` +
> `configure_from_config` config→env bridge (fail-closed unchanged, still off by default for test compatibility); (5) synthetic KV cross-node transport
> declared **production-ready** (#33 closed, real tensor env-gated bonus); (6) CLI `backup create/restore` one-shot backup/restore
> (full 9 files + tls/ + kv/, atomic tar.gz 0600, path-escape validation); (7) rule-epoch/confirm persistence (no longer reset to zero on restart /
> HA failover from 0, `_build_state_sync_payload` included in sync). Strategy = config sections + deploy-layer env passthrough + doc guidance
> (no default flips except `observability.persist`). Baseline 1309 → 1343 tests green (randomized-order bidirectional), ruff clean. See
> [CHANGELOG](docs/CHANGELOG.md). Production mTLS/HA must be explicitly enabled: see `docs/DEPLOYMENT.md`.

---

> **📦 v0.12.1 (2026-08-28) — Audit 0826 P2+P3 remediation (15 items)**
>
> Audit `fusion-multi-node-audit-result-product-0826.md` ruled all 12 P2 + 3 P3 items fixed in code
> (incl. design-tradeoff items broken open via env-gate, not docs-only). Security/resource (3): mTLS cert SAN + `check_hostname=True` /
> MLXKVTransport SSRF guard / docker-compose resource limits. KV capacity (2): export-size sync / proactive probe at ban expiry.
> Events/election (3): election I/O outside lock / event-drop alert / F2 dynamic subpath all ops. Container/isolation design-tradeoff break-open (4):
> sandbox rlimit / PARTIAL crash completion / PIPELINE segment-level checkpoint / observability deque persistence (all env-gated).
> Deploy/config (3): autoscaler wording 503 / AgentServer KV persist critical alert / MIGRATED auto semantic calibration.
> Resource leak (1): AgentServer.stop calls kv_manager.close. Baseline 1262 → 1317 tests green. See
> [CHANGELOG](docs/CHANGELOG.md). This completes all 47 audit-0826 items (5 P0 + 27 P1 + 12 P2 + 3 P3).

---

> **📦 v0.12.0 (2026-08-27) — Audit 0826 P1 remediation (27 items)**
>
> Audit `fusion-multi-node-audit-result-product-0826.md` ruled all 27 P1 items fixed in code.
> Fault-tolerant scheduling (8): H3 RUNNING→PENDING re-dispatch with `exclude_nodes` / node OFFLINE auto-migrate in-flight /
> `_pending_queue` cap 503 / retry exponential backoff / agent_server 429 not counted toward circuit breaker / incremental persistence /
> httpx connection pool explicit config / `sync_kv_cache` exception classification. KV tensor (2): `import_tensor` distinguishes degradation
> vs real failure / cross-node exception classification + consecutive-failure alert. Security (7): optional PII scrubbing on HTTP dispatch / cloud_fallback
> import-time disable guard / RBAC fail-closed + full-route registry / audit-write-failure alert / `compare_digest` /
> manual_join mTLS scheme. API contract (1): 9 raw dict → pydantic (422 not 400). Agent (3):
> `/api/hardware` to_thread / election gap 503 / local `max_tasks` overload gate. Perf/ops (6): real-inference
> throughput benchmark / Prometheus node-level metrics / HA doc calibration / kv+user fsync / log-file stderr hint.
> Baseline 1213 → 1262 tests green. See [CHANGELOG](docs/CHANGELOG.md). P2/P3 remediation continues (v0.12.1).

---

> **📦 v0.11.0 (2026-08-27) — GAP-7 KV tensor cross-node transport (close #33)**
>
> `ClusterMaster.sync_kv_cache` orchestrates via a pluggable tensor backend (synthetic default / MLX real-tensor env-gated
> `FUSION_KV_TENSOR_BACKEND=mlx`) source agent `/api/kv/export` → target `/api/kv/import`,
> returns `True`. `KVShard` adds a `tensor` field (base64-compressed over JSON, `store_local` budget gate).
> Synthetic backend satisfies #33 acceptance (tensor round-trip across 2 agents); real tensor awaits upstream fusion-mlx issue #650
> to activate (env-gated bonus, 404→degrade to synthetic + warn). New `kv_tensor_transport.py` +
> three test groups (`test_kv_tensor_serialize.py` 11, `test_kv_export_import_routes.py` 6,
> `test_kv_tensor_e2e.py` 4+1 skip) + rewrote `test_new_features.py` sync cases.
> See [CHANGELOG](docs/CHANGELOG.md).
>
> Completed (Phase A-F + GAP-7): issues/PR → RC → GAP-1 always-on → GAP-6 rate-limit pacing → GAP-5 dead-code → F1-F5 multi-tenant → GAP-7 KV tensor transport.

---

## 📋 Overview

**Fusion-Multi-Node** is the cluster scheduling core for the [Fusion-MLX](https://github.com/dahai80) ecosystem. It enables pooling multiple Apple Silicon Macs (M4/M5 Studio/Max) into a distributed inference cluster.

### Two Distributed Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Pipeline Parallelism** | Split large models (70B+) across multiple Macs, each handling a subset of layers | Run oversized local models |
| **Data Parallelism** | Load the same model on multiple Macs, distribute batch requests for higher throughput | High-throughput batch inference |

### Core Modules

| Module | Responsibility |
|--------|---------------|
| **Cluster Master** | Node discovery, resource scheduler, task lifecycle, KV cache pool, fault tolerance, task auto-degradation, load-aware routing, task sharding, AST diff, FMP KV sync, real-tensor PIPELINE layer-split chain (to fusion-mlx `/distributed/*`, ✅upstream endpoints delivered issue #621/#630 closed; multi-node client stubs `load_shard`/`pipeline_step` wired, ⚠️real-model end-to-end verification pending long-term), master→agent dispatch loop, **H3 task persistence + crash recovery** (RUNNING/PENDING atomic disk write, auto-re-dispatch on crash restart). HA election wired to `start(ha_config=)` (off by default single Master; StandbyMaster class is a dead-code prototype). cloud_fallback scheduling path cut in v0.8.2 (100% local) |
| **Node Agent** | Per-machine daemon, hardware reporting, task execution, mDNS auto-discovery, pipeline_step (upstream `/distributed/load_shard`+`pipeline_step` delivered issue #621 closed, b64.npy activates cross-node, ⚠️real-model end-to-end pending long-term) |
| **mDNS Discovery** | Bonjour/mDNS zero-config node discovery, manual IP join fallback |
| **FMP Protocol** | Three-layer binary protocol, AES-GCM encryption, TCP long connection, circuit breaker, hop_count, FMP inbound server. ⚠️Starts but never used as dispatch transport (HTTP dispatch only) |
| **Distributed MLX Bridge** | Pipeline/data parallelism, model sharding, Caveman compression, KV cache sharing. ✅Cross-node KV transport production-ready (GAP-7/#33, v0.11.0): `SyntheticKVTransport` default backend routes synthetic KVCacheEntry cross-node; real-tensor `MLXKVTransport` env-gated experimental bonus (`FUSION_KV_TENSOR_BACKEND=mlx`, awaits upstream #650) |
| **Security** | Node approval, Master/Worker permission isolation, Worker sandbox, OS-level sandbox-exec, data scrubbing, FMPCrypto (AES-256-GCM + ECDH), Metal AES-GCM acceleration |
| **Observability** ✅Wired | Metrics, logs, alerts, log store & export, intelligent fault diagnosis, optimization suggestions. **P0-8 wired to `ClusterMaster.start/stop` lifecycle + `_health_check_loop` periodic metric/alert collection (deduped); `/api/v1/observability/{logs/export,suggestions,alerts}` now returns 200; `/api/v1/metrics` (Prometheus) also wired. v0.14.0 persistence on by default (`observability.persist=True`, `observability.jsonl` to disk, `_cleanup_loop` 300s periodic save)** |
| **Storage Volumes** | Volume abstraction, checkpoint persistence, capacity monitoring, LRU eviction. **ShardReplicator / DistributedKVStore / quorum read-write not wired into the production path, library-level only** |

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Claude Code / API / fusion-desk UI         │
│                           ↓                                  │
│              fusion-multi-node Cluster Master                 │
│  (Discovery, Scheduler, KV Pool, [Election·HA optional],      │
│   Degradation, Security, Observability)                      │
│                           ↓                                  │
│     ┌──────────────┬──────────────┬──────────────┐           │
│     │  Node Agent   │  Node Agent  │  Node Agent  │           │
│     │  (Mac M4)     │  (Mac M4)    │  (Mac M4)    │           │
│     │  fusion-desk  │  fusion-desk │  fusion-desk │           │
│     │  fusion-mlx   │  fusion-mlx  │  fusion-mlx  │           │
│     └──────────────┴──────────────┴──────────────┘           │
│                           ↓                                  │
│              Distributed MLX (mlx.distributed)                │
│         Thunderbolt RDMA / Ethernet / P2P Bridge              │
└──────────────────────────────────────────────────────────────┘
```

### Ecosystem Position

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                          │
│   fusion-desk  │  fusion-code  │  fusion-ui  │  Claude App   │
└──────────────────────────┬──────────────────────────────────┘
                           │ MCP / HTTP
┌──────────────────────────▼──────────────────────────────────┐
│                    Control Layer                               │
│         fusion-multi-node (Cluster Master + Node Agent)        │
└──────────────────────────┬──────────────────────────────────┘
                           │ distributed API
┌──────────────────────────▼──────────────────────────────────┐
│                    Inference Layer                             │
│         fusion-mlx (MLX distributed, quantization, Metal)     │
│         Fusion-KB (vector search, RAG)                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Installation

```bash
git clone https://github.com/dahai80/fusion-multi-node.git
cd fusion-multi-node

pip install -e .            # Core install
pip install -e ".[all]"     # All optional deps
pip install -e ".[test]"    # Test deps
```

### Start Cluster

```bash
# Start Cluster Master
fusion-multi-node cluster start --mode master

# Start Node Agent (on each Mac)
fusion-multi-node cluster start --mode agent

# Check status
fusion-multi-node cluster status
fusion-multi-node node list
```

### CLI Quick Reference

```bash
fusion-multi-node cluster start/stop/status    # Cluster management
fusion-multi-node cluster pending/approve/reject # Node approval
fusion-multi-node node list/info/discover      # Node management
fusion-multi-node task submit/list/cancel      # Task management
fusion-multi-node config list/get/set          # Configuration
fusion-multi-node network detect               # Network topology
fusion-multi-node caveman test                 # Caveman compression
fusion-multi-node kv stats/warm                # KV cache management
```

---

## 🏗️ Module Architecture

### 1. Cluster Master (`fusion_multi_node.master`)

The single source of truth for the cluster — node registration, health checks, task scheduling, KV cache, master election, cloud fallback, task auto-degradation, real-tensor PIPELINE layer-split chain.

#### Health endpoints (C11 — readiness vs liveness)

- `GET /api/health` — **liveness**: local dependencies (disk free >512MB / memory >256MB / task-store writable), does not check upstream/node quorum. Always HTTP 200, body `status: "ok"|"degraded"`. For `start.sh` / docker livenessProbe — process alive is enough, does not block startup.
- `GET /api/health/deep` — **readiness**: liveness + node quorum (≥1 ONLINE node). body `status: "ok"|"degraded"`, includes `online_nodes` count. For LB / orchestrator to drain a half-broken master (host healthy but no usable nodes → not ready). **Not for inter-service depends_on** (would deadlock with agent startup).
- Both endpoints are exempt from Bearer auth (k8s probes carry no token).

#### Pipeline Parallelism — real-tensor layer split (to fusion-mlx `/distributed/*`, #621)

PIPELINE mode splits the model into segments per `model_shards`; each node runs one layer-forward segment. The first segment carries `input_ids`
(embed + layers); subsequent segments carry the previous segment's output `hidden_states` (b64.npy, layers only). Activation tensors
are chained in order by the scheduler to the last node; the last node's output = final hidden_states.

```python
from fusion_multi_node.master import ClusterMaster, ClusterTask, ParallelMode

task = ClusterTask(
    task_id="task-pipeline",
    name="layer-split",
    mode=ParallelMode.PIPELINE,
    model_name="Llama-3.2-1B-Instruct-4bit",
    model_shards=[
        {"shard_index": 0, "layer_range": [0, 8]},
        {"shard_index": 1, "layer_range": [8, 16]},
    ],
    task_type="pipeline_step",
    params={
        "model_id": "~/.fusion-mlx/models/mlx-community-Llama-3.2-1B-Instruct-4bit",
        "input_ids": [10, 20, 30, 40],
    },
)
await master.assign_task(task)
# → last node returns hidden_states (shape [1,4,2048] float16, b64.npy)
# lm_head / decode is beyond upstream /distributed/* first-version scope — scheduler only does the layer-forward chain, not token generation
```

> Real-model E2E verified (Llama-3.2-1B-Instruct-4bit, 16 layers split [0,8]/[8,16],
> see `tests/test_pipeline_e2e.py`). Requires fusion-mlx running + `mlx.fusion_mlx_api_key` configured.

```python
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode

master = ClusterMaster(host="127.0.0.1", port=11452)

node = NodeInfo(
    node_id="node_1",
    hostname="mac-studio-1",
    ip_address="10.0.0.1",
    port=11458,
    total_memory_gb=64.0,
    available_memory_gb=48.0,
)
await master.register_node(node)  # re-register = PATCH (preserves runtime state), returns bool (False during ban)

task = ClusterTask(
    task_id="task_1",
    name="batch-inference",
    mode=ParallelMode.DATA,
    required_capability="inference",
    preferred_node_id="node_1",
    priority=5,
)
master.assign_task(task)
await master.cancel_task("task_1", reason="user request", cancel_sub_tasks=True)
await master.degrade_task("task_1")  # 70b→32b→13b→8b→3b→1b
master.complete_task("task_1")
```

**Key capabilities**: Load-aware routing (BALANCED/VRAM_FIRST/LOCALITY_FIRST/LOW_LATENCY, thread-safe strategy switching), local-force gate (≤0.5B models), VRAM-first scheduling (≥13B), score-based node selection with capability filtering, task lifecycle (PENDING→RUNNING→COMPLETED/FAILED/TIMEOUT/MIGRATED), recursive cancel, model auto-degradation chain, migration, KV cache pool with FMP sync, AST diff-only transmission, task sharding (inference/AST/vectorize, shard timeout), heartbeat timeout, task-level circuit breaker (S1 dispatch-fault auto-ban).

#### Idempotent node registration + fault blacklist (F-A12 / F-A13, #20)

- **F-A12 idempotent registration**: re-registering the same `node_id` = PATCH semantics — preserves Master-authoritative runtime-state fields
  (`active_tasks`/`max_tasks`/`network_rtt_ms`/`status`), only updates hardware-declared fields
  (memory/CPU/GPU/hostname/port). Node restart does not lose runtime state and does not wipe in-flight dispatch counts.
- **F-A13 fault blacklist**: `report_fault` accumulates within the `_FAULT_WINDOW_S` (60s) window to
  `_FAULT_THRESHOLD` (3) → auto-ban for `_BAN_DURATION_S` (300s). During ban `register_node`
  returns `False` (HTTP 403 rejected). `unregister_node(reason="banned")` proactively blacklists.
  Lazy auto-unban on expiry; `is_node_banned()` / `unban_node()` for manual query/unban.

```python
# Fault circuit breaker: 3 reports in a row → ban 5 min, re-registration rejected during ban
await master.report_fault("node_1", "oom", "out of memory")
assert not master.is_node_banned("node_1")
await master.report_fault("node_1", "oom", "again")  # 3rd report triggers ban
assert master.is_node_banned("node_1")
assert await master.register_node(node) is False       # rejected during ban
master.unban_node("node_1")                            # manual unban
```

#### Task-level circuit breaker (S1, #70) — auto-ban on dispatch failure

- **Dispatch-failure fault report**: `_dispatch_to_node` failure (SSRF rejection / agent HTTP non-200 / agent returns non-ok)
  → auto-calls `report_fault(node_id, "dispatch_failed")`, counted into the F-A13 fault window.
- **Scheduling skips banned nodes**: `select_nodes` candidate filter skips nodes within ban — originally only `register_node`
  intercepted, the scheduling path missed it, so faulty nodes were dispatched repeatedly; S1 closes the scheduling-side gap.
- Consecutive dispatch failures reaching `_FAULT_THRESHOLD` (3) auto-ban; not selected during ban; selectable again after expiry/unban.

```python
# 3 dispatch failures → node auto-banned, select_nodes no longer picks it
for i in range(master._FAULT_THRESHOLD):
    await master._dispatch_task(task_failing_on_node_1)
assert master.is_node_banned("node_1")
assert await master.select_nodes(ParallelMode.DATA, count=1) == []  # all banned → empty
```

#### Production metrics endpoint (S2, #71) — Prometheus exposition

- **`GET /api/v1/metrics`**: plain-text Prometheus 0.0.4 exposition, no external deps, scrapable directly by Prometheus / Grafana agent.
- Cluster-level aggregate metrics:
  - Nodes: `fusion_cluster_nodes_total` / `fusion_cluster_nodes_online`
  - Tasks: `fusion_cluster_tasks_total` / `_running` / `_pending` / `_completed` / `_failed`
  - Retries: `fusion_cluster_task_retries_total` (counter)
  - KV: `fusion_cluster_kv_cache_entries`
  - Memory: `fusion_cluster_memory_total_gb` / `_available_gb`
  - Dispatch latency: `fusion_cluster_dispatch_latency_seconds` (summary, p50/p90/p99 + sum/count)
- Reuses `get_stats` + dispatch latency (`completed_at - started_at`) + `_retry_count`. Bearer auth not exempt — internal scrape carries the cluster token.

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:11452/api/v1/metrics
```

### Master Election (`fusion_multi_node.master.election`) — P4 wired to start(), off by default

> **Current status: P4 + P0-1 wired into `ClusterMaster.start(ha_config=...)`.** When `ha.enabled=True` it calls
> `setup_election` to start the election loop; default `enabled=False` single-Master backward-compatible. Raft-simplified
> priority voting, `on_elected`/`on_deposed` callbacks. Leader heartbeat broadcast + term/voted_for persistence
> (`~/.fusion/multi-node/election_state.json`) wired (P0-1, fixes term-churn re-election).
> **Note:** the `StandbyMaster` class (independent of MasterElection) is still a dead-code prototype, not production-ready.
>
> **H3 task persistence (v0.8.2, wired):** even single-Master without full HA, RUNNING/PENDING tasks are atomically written to disk
> (`~/.fusion/multi-node/tasks.json`); after a Master crash and `start()` restart, `_restore_tasks` auto-recovers
> (RUNNING→PENDING re-dispatch), no task loss.
>
> **H2 crash self-healing (v0.8.2, wired):** launchd process supervisor — `./start.sh install-launchd` renders
> `deploy/com.dahai80.fusion-multi-node.plist` (KeepAlive crash 10s-throttled auto-restart) → launchctl load.
> Crash → launchd restart → H3 task recovery = self-healing closed loop, no task loss. See `docs/HA-CRASH-RECOVERY.md`.
>
> **Deployment options**: single-machine nohup / single-machine launchd supervisor / docker-compose multi-machine small cluster / multi-Master HA (technical preview).
> This project targets local-first Apple Silicon small clusters, **not K8s orchestration** — see `docs/DEPLOYMENT.md`.
> **Operations runbook**: fault handling (node/Master offline, split-brain, disk full, fusion-mlx unreachable, task backlog) + version upgrade/backup restore/token rotation — see `docs/OPERATIONS.md`.

```python
from fusion_multi_node.master import ClusterMaster

master = ClusterMaster(host="127.0.0.1", port=11452)
await master.start(ha_config={
    "enabled": True,
    "node_id": "master-1",
    "priority": 5,
    "peers": ["master-2", "master-3"],
})
```

### 2. Node Agent (`fusion_multi_node.agent`)

Runs on every Mac — hardware metrics, heartbeat, task execution via fusion-mlx API.

**Health endpoints (C11)**: `GET /api/health` (liveness — disk/memory + fusion-mlx port probe, no outbound HTTP) / `GET /api/health/deep` (readiness — liveness + real HTTP probe to fusion-mlx `/v1/models`, determines whether the agent can actually infer). Both endpoints exempt from Bearer auth.

```python
from fusion_multi_node.agent import NodeAgent, AgentConfig

config = AgentConfig(node_id="my_mac", master_host="10.0.0.1")
agent = NodeAgent(config)
await agent.start()

info = agent.collect_hardware_info()
result = await agent.execute_task({"task_id": "t1", "type": "inference", "model": "qwen3.5-9b"})
```

### 3. mDNS Discovery (`fusion_multi_node.discovery`)

Zero-config Bonjour/mDNS node discovery with manual IP join fallback.

```python
from fusion_multi_node.discovery import MDNSDiscovery
from fusion_multi_node.discovery.manual_join import ManualJoinClient, ManualJoinManager

# mDNS auto-discovery
mdns = MDNSDiscovery(node_id="fusion-master")
mdns.register(port=11452, properties={"role": "master"})
master = await mdns.find_master_async(timeout=5.0)

# Manual IP join (mDNS fallback)
client = ManualJoinClient()
resp = await client.join(master_host="10.0.0.1", master_port=11452, node_id="node-1")

mgr = ManualJoinManager(cluster_secret="my-secret", auto_approve=True)
result = mgr.handle_join_request({"node_id": "node-1", "cluster_secret": "my-secret"})
```

### 4. FMP Protocol (`fusion_multi_node.protocol`)

Three-layer binary protocol with AES-GCM encryption, circuit breaker, and hop_count broadcast limit.

```python
from fusion_multi_node.protocol import (
    FMPMessage,
    PayloadType,
    FMPCrypto,
    FMPConnectionManager,
    FMPRouter,
    CircuitBreaker,
    FMPServer,
)

msg = FMPMessage.create("master", "node1", PayloadType.HEARTBEAT, {"status": "ok"})
data = msg.serialize()
msg2 = FMPMessage.deserialize(data)

key = FMPCrypto.generate_key()
crypto = FMPCrypto(key=key)
crypto.encrypt_message(msg)
crypto.decrypt_message(msg)

cb = CircuitBreaker(name="node1", failure_threshold=5)
if cb.allow_request():
    cb.record_success()
```

**Three layers**: LinkLayer (routing, hop_count), BusinessLayer (payload, rounds), ControlLayer (heartbeat, ACK, flow control). **Unified interface**: FMPInterface wraps connection management, message construction, encryption, heartbeat. **Protobuf v2**: Structured .proto with Envelope/Control/Payload messages, auto-fallback to JSON/msgpack.

### 5. Security (`fusion_multi_node.security`)

Node approval, Master/Worker permission isolation, Worker sandbox, data scrubbing.

```python
from fusion_multi_node.security.permission import (
    PermissionManager,
    NodeRole,
    Permission,
)
from fusion_multi_node.security.node_approval import NodeApprovalManager
from fusion_multi_node.security.sandbox import (
    WorkerSandbox,
    SandboxConfig,
    SandboxExecutor,
)
from fusion_multi_node.security.data_scrubber import DataScrubber
from fusion_multi_node.security.crypto import FMPCrypto, MetalCryptoBackend

# Permission isolation
pm = PermissionManager()
pm.assign_role("master-1", NodeRole.MASTER)
pm.assign_role("worker-1", NodeRole.WORKER)
pm.has_permission("worker-1", Permission.TASK_EXECUTE)  # True
pm.has_permission("worker-1", Permission.TASK_SUBMIT)  # False
pm.check_path_access("worker-1", "/api/execute", "POST")  # True

# Node approval
mgr = NodeApprovalManager(auto_approve_patterns=["192.168."])
req = mgr.request_join(node_id="n1", hostname="mac-1", ip_address="192.168.1.10", port=11445)
mgr.approve("n1", approved_by="admin")

# Worker sandbox
sandbox = WorkerSandbox(
    config=SandboxConfig(
        allowed_paths=["/tmp", "/data"],
        allowed_network_hosts=["api.openai.com"],
    )
)
sandbox.check_path_access("/tmp/out", write=True)  # True
sandbox.check_network_access("api.openai.com")  # True
sandbox.filter_environment({"HOME": "/u", "SECRET": "x"})  # SECRET removed

# Data scrubbing (phone, email, API key, ID card, etc.)
scrubber = DataScrubber()
text, hits = scrubber.scrub_text("Call 13912345678, key=sk-abc123...")

# OS-level sandbox execution (macOS sandbox-exec / Linux unshare)
executor = SandboxExecutor()
result = await executor.execute_in_sandbox("task-1", ["python", "script.py"])

# Metal AES-GCM acceleration (Apple Silicon hardware)
metal = MetalCryptoBackend()
encrypted = metal.encrypt(key, plaintext)
decrypted = metal.decrypt(key, encrypted)

# Secure transfer pipeline (AST diff + PII scrubbing)
from fusion_multi_node.security.secure_transfer import SecureTransferPipeline

pipeline = SecureTransferPipeline()
transfer = pipeline.prepare_transfer(old_ast, new_ast)  # diff + scrub
restored = pipeline.apply_transfer(base_ast, transfer)  # rebuild
```

#### mTLS node mutual trust (#80)

Intra-cluster node connections optionally use mutual TLS (mTLS), private CA + per-node leaf certs. Env switch `FUSION_MTLS_ENABLED=1` enables it; off = all-HTTP no-op (does not break existing tests/CLI).

```python
from fusion_multi_node.security import mtls

# Generate cluster CA (3650 days) + per-node leaf cert (CN=node_id, O=role, 365 days)
ca_cert, ca_key = mtls.provision_cluster("/path/to/ca")
node_cert, node_key = mtls.provision_node("worker-1", "worker", ca_cert, ca_key, "/path/to/worker-1")

# Server: uvicorn.Config(**server_ssl_kwargs()) — requires peer client cert (CERT_REQUIRED)
# Client: httpx.AsyncClient(**client_kwargs()) — verify=ctx validates server cert + presents client cert
# URL scheme: mtls.scheme() → "https" / "http"
```

Fine-grained permissions (enforced when mTLS is on): AgentServer reads caller identity from `X-Node-Id`/`X-Node-Role` headers → `PermissionManager` validates path permissions.
- MASTER: all APIs (incl. execute + cancel)
- WORKER: execute / heartbeat / KV lookup-transfer / hardware; **no** cancel
- Enforce mode missing `X-Node-Id` → 403; role lacks permission → 403
- Compat mode (mTLS off) missing header → allow (existing http tests/CLI carry no header)

#### Multi-tenant quota + priority queue (#81)

P1-H multi-tenant scheduling: global default max concurrent running tasks per tenant; over-quota tasks enter a priority queue (not rejected); high-priority queued tasks get free nodes first (non-preemptive, does not kill running tasks).

```python
from fusion_multi_node.master import ClusterMaster

master = ClusterMaster()
master.configure_scheduling(tenant_max_concurrent=4)  # 0 = unlimited quota (node capacity still limits)

# Over-quota tasks auto-enqueue, assign_task returns True (not rejected)
await master.assign_task(task)
# Queue ordered by priority desc; node online / task complete / cancel a slot task → drain queue head
```

- Quota global default: config key `scheduling.tenant_max_concurrent` (default 4, 0=unlimited), auto-loaded on CLI startup
- Over-quota enqueue: tenant running tasks reach quota → new task `TaskStatus.PENDING` enqueued, `assign_task` returns True
- No-node enqueue: `select_nodes` finds no available node → enqueue (no longer returns 503), drains on node online
- Priority: `ClusterTask.priority` (TaskPriority: LOW=0/NORMAL=1/HIGH=2/CRITICAL=3), queue sorted desc
- Drain triggers: `complete_task` / `register_node` / `cancel_task` (canceling a slot task frees the concurrency slot)
- HTTP: `POST /api/tasks/submit` enqueue returns `202 {"queued": true}` (successful dispatch still returns 200)
- Cancel: `cancel_task` recursively removes main/sub tasks from the queue; queued tasks are registered in `master.tasks` and are queryable/cancelable

### 6. Observability (`fusion_multi_node.observability`)

Metrics, logs, alerts, log store with export, intelligent fault diagnosis.

```python
from fusion_multi_node.observability import ClusterObservability, LogEntry
from fusion_multi_node.observability.log_store import (
    LogStore,
    StoredLog,
    FaultDiagnoser,
)

# Metrics & alerts
obs = ClusterObservability(retention_hours=168.0)
obs.record_metric("node_1", "memory_used_gb", 16.0, tags={"gpu": "m4_ultra"})
obs.add_log(LogEntry(time.time(), "node_1", "INFO", "scheduler", "Task completed"))
logs = obs.export_logs(fmt="json")  # M8-02 log export
suggestions = obs.generate_optimization_suggestions()  # M8-03 smart suggestions

# Log store & export
store = LogStore()
store.store(
    StoredLog(
        timestamp=time.time(),
        level="error",
        source="master",
        message="heartbeat timeout",
    )
)
results = store.query(level="error")
json_data = store.export_json()
csv_data = store.export_csv()

# Fault diagnosis (pattern matching + root cause analysis)
diagnoser = FaultDiagnoser()
results = diagnoser.diagnose(logs)
freq = diagnoser.analyze_frequency(logs, group_by="source")
```

### 7. Storage Volumes (`fusion_multi_node.storage`)

> **Status**: `StorageVolume`/`CheckpointManager`/`DistributedKVStore` library-level available.
> `ShardReplicator` FMP cross-node transport and quorum read-write are **not wired** into the production path
> (`set_fmp_interface` has no caller). Quorum read-write has an E9 guard: without a `storage_volume` it
> always rejects (`error=no_storage_volume`), no longer falls back to in-memory self-consistency, avoiding false reports of majority-persistence success.
> This section is a library-level API reference.

Volume abstraction, shard replication, checkpoint persistence.

```python
from fusion_multi_node.storage import StorageVolume, VolumeSpec, VolumeType
from fusion_multi_node.storage import ShardReplicator, ReplicationConfig
from fusion_multi_node.storage import CheckpointManager, CheckpointEntry
from fusion_multi_node.storage import DistributedKVStore, KVEntry

# Volume management
sv = StorageVolume(base_dir="/data/volumes")
sv.create_volume(VolumeSpec(name="models", volume_type=VolumeType.LOCAL))
sv.write_file("models", "config.json", b'{"model": "llama-70b"}')
data = sv.read_file("models", "config.json")

# Shard replication
replicator = ShardReplicator(config=ReplicationConfig(replication_factor=2))
replicas = replicator.assign_replicas("shard-1", "/models/llama.bin", 1024, nodes)
healthy = replicator.get_healthy_replica("shard-1")

# Checkpoint persistence
cp = CheckpointManager(checkpoint_dir="/data/checkpoints")
cp.save(CheckpointEntry(checkpoint_id="cp-1", task_id="t1", node_id="n1", step=5, state_data={...}))
latest = cp.load_latest("t1")

# Distributed KV Store with TTL, partitions, snapshot/restore
kv = DistributedKVStore(data_dir="/data/kv")
kv.put("config:model", {"name": "llama-70b"}, partition="config", ttl_seconds=3600)
val = kv.get("config:model")
kv.snapshot()  # M9-03: persist to disk
kv.restore("snapshot.json", merge=True)

# Quorum read/write for shard replication
qr = replicator.quorum_write("shard-1", data, storage_volume=sv)
qread = replicator.quorum_read("shard-1", storage_volume=sv)
```

---

## 🔧 Configuration

Default config at `~/.fusion/multi-node/config.json`:

```json
{
  "cluster": {
    "name": "fusion-cluster",
    "master_host": "127.0.0.1",
    "master_port": 11452,
    "discovery_port": 11450,
    "agent_port": 11445,
    "mcp_port": 11446,
    "heartbeat_timeout": 15.0,
    "heartbeat_interval": 3.0,
    "report_interval": 15.0
  },
  "parallel": {
    "default_mode": "pipeline",
    "pipeline_timeout": 300.0,
    "data_parallel_timeout": 120.0,
    "caveman_compress": true,
    "communication": "auto"
  },
  "mlx": {
    "fusion_mlx_port": 11432,
    "fusion_kb_port": 11434,
    "fusion_desk_port": 9000,
    "model_hub_port": 11435
  },
  "mcp": {
    "enabled": true,
    "token_budget": 10000000,
    "tool_timeout": 60.0
  },
  "observability": {
    "retention_hours": 24.0,
    "alert_enabled": true,
    "log_level": "info"
  }
}
```

**Port migration**: v0.6.5 legacy ports (master 9753 / discovery 9754 / agent 9755 / mcp 9756 / fusion_mlx 8000) are auto-migrated to current defaults on config load, and a mis-set `master_host=0.0.0.0` is reverted to `127.0.0.1`; the migration is written back to `config.json`. `ClusterConfig` load uses a deep copy, so `set()` does not pollute the class-level `DEFAULT_CONFIG`.


---

## 🧪 Testing

```bash
pip install -e ".[test]"

# Run all tests (1343 tests)
pytest tests/ -v

# With coverage
pytest tests/ --cov=fusion_multi_node --cov-report=html

# Run specific module
pytest tests/test_cluster_master.py -v
pytest tests/test_protocol.py -v
pytest tests/test_new_features.py -v
```

### Real-model E2E (requires fusion-mlx running)

```bash
~/claude-home/fusion-mlx/start.sh start        # start inference engine (port 11434)

# DATA parallel 2-node real inference (skip-gate: fusion-mlx alive + model in /v1/models list)
pytest tests/test_data_parallelism_e2e.py -v

# Cross-node KV cache sharing (synthetic data, no model needed, no skip-gate)
pytest tests/test_kv_sharing_e2e.py -v

# Pipeline parallel layer-split real inference
pytest tests/test_pipeline_e2e.py -v

~/claude-home/fusion-mlx/start.sh stop         # shut down when done
```

> Default model `mlx-community-Llama-3.2-1B-Instruct-4bit`, api_key via config `mlx.fusion_mlx_api_key`.
> When fusion-mlx is stopped, E2E auto-skips (skip-gate), does not block CI green.

### Cross-machine real-network E2E (#76)

Real port bind + real HTTP cross-process (not ASGITransport) — starts a real uvicorn real-port server in-process, communicates over real TCP sockets.

```bash
# Real-port cross-process: register / dispatch / offline-reconnect (no real model, FakeBackend)
pytest tests/test_real_network_e2e.py -v

# Container cross-machine: docker-compose 1 Master + 2 Agent (skip-gate docker available)
pytest tests/test_real_network_e2e.py::TestContainerE2E -v
```

- Real register: agent over real HTTP `/api/nodes/register` to master (real socket)
- Real dispatch: master → agent `/api/execute` over HTTP (FakeBackend completes non-real inference)
- Offline-reconnect: stop agent → master heartbeat timeout marks OFFLINE → restart same node → reconnect restores ONLINE + dispatchable
- Container E2E: `docker compose up --scale agent=2` cross-container register + dispatch; skips when docker unavailable

### Cross-machine KV sharing scale stress (#79)

N real-port agents over HTTP validate large-scale KV cache migration — warm_cache scale + transfer migration + latency + 0 loss (synthetic KVCacheEntry, no real model).

```bash
# 4 stress cases: warm scale / warm latency / warm→transfer migration / VRAM accumulation
pytest tests/test_kv_stress.py -v
```

- warm scale: M prompt × N node all succeed (0 loss)
- warm latency: single warm p99 < 1.0s
- transfer migration: warm to node-0 → transfer pulls to node-1, cross-node 0 loss (push model: source node returns serialized entry → target deserializes + store_local)
- VRAM accumulation: local_entries / total_size_bytes proxy for VRAM usage

> KV transfer push-model fix (v0.8.4): the original `/api/kv/transfer` route callback `transfer_from_remote` caused recursion + source_node containing a colon failed sanitize — switched to push model (source node looks up local and returns entry, target deserializes + store_local), added `_serialize_entry` + `lookup_local_by_id`.

### Container node auto-approval (v0.8.4)

`docker-compose` master is configured by default with `FUSION_AUTO_APPROVE_PATTERNS` (trusted-subnet substring match) — container/LAN nodes auto-join without manual `cluster approve`.

```bash
# compose default: 192.168. / 10. / 172.16.0.0/12 subnets auto-approved
docker compose up -d --scale agent=2

# Bare-metal custom trusted subnet (comma-separated; CIDR takes precedence for exact match, non-CIDR falls back to substring/wildcard)
FUSION_AUTO_APPROVE_PATTERNS="10.0.1." ./start.sh start
```

> Production should open auto-approval only for trusted subnets; without the env, it falls back to the manual approval gate (`fusion-multi-node cluster approve <node_id>`).

### Port-conflict explicit error (v0.8.7)

Follow-up to issue #25: the NodeAgent default port was moved off 11445 → 11458 in v0.8.0 (resolved conflict with fusion-comfyui, `_STALE_PORT_MAP` auto-migrates old configs). This adds an explicit error on bind failure — `AgentServer.start` / `MasterServer.start` catch `OSError` and, for known conflict ports (comfyui 11445 / fusion-mlx 11432/11434 / master 11452 / mDNS 11450 / MCP 11446), append a hint "(conflicts with {service} default port)", rather than a generic bind error. Test: `test_start_port_conflict_raises_with_hint` (agent + master, mock uvicorn serve raises EADDRINUSE). Full suite 946 passed 1 skipped.

### Phase 4 fault-injection E2E (v0.8.6)

End-to-end self-healing validation of the scheduler against real faults (real ASGI routes, not unit mocks; inference uses synthetic FakeBackend, does not touch fusion-mlx):

1. **agent crash → timeout → retry → re-dispatch to a live node** — agent-a removed from routing (simulated crash, dispatch 404), task timeout `check_timeouts` → `_enqueue_retry` (TIMEOUT→PENDING) → drain retry queue `assign_task` (select_nodes skips banned agent-a) → lands on agent-b → COMPLETED. Full-chain locking: timeout-enqueue + re-dispatch-to-live + task completion.

2. **Repeated dispatch failure → ban → new task routed to a live node** — agent-a crash, consecutive dispatches `_FAULT_THRESHOLD` times all 404 → `report_fault` reaches threshold within the window and auto-bans → new task `select_nodes` skips the banned node → routes to agent-b → COMPLETED. Integration-level validation (existing `test_task_circuit_breaker` is unit-level).

3. **HA leader failure → standby promoted to leader → dispatch resumes + synced tasks readable** — m1 (leader) holds a task via `_persist_tasks` → HTTP push to m2 (standby) `receive_synced_tasks` to disk; m1 demoted + m2 promoted to leader (`_on_demoted_from_leader`/`_on_elected_leader` flip `_is_leader`) → m2 `assign_task` no longer returns False due to the standby guard → synced tasks are taken over without loss.

Tests: `tests/test_fault_injection.py` (3 scenarios, PortRoutingTransport + real AgentServer `/api/execute` + FakeBackend). Full suite 943 passed 1 skipped.

### KV cross-node lookup contract fix + approval CIDR exact match (v0.8.5)

Two defect fixes exposed by strict review:

1. **`lookup_remote` always returned None** — the `/api/kv/lookup` route returned a flat dict (no `found`/`entry` keys), so `lookup_remote` decoding `data.get("found")` was always falsy → cross-node KV reuse lookup silently failed. A unit mock fabricating `{"found":True,"entry":{...}}` shape hid this bug (false-confidence test). Fix: route aligned to contract returns `{"found":True,"entry":_serialize_entry}`, plus a real-chain E2E contract lock (`test_kv_lookup_remote_cross_node_contract` — store on node-a, node-b queries back over HTTP, not mock).

2. **Auto-approval `"172."` substring over-matched public networks** — compose default `172.` substring matched public `172.0–15`/`172.32–255` (private is only `172.16.0.0/12`). Fix: CIDR takes precedence for exact match (`ipaddress.ip_network` containment test), non-CIDR falls back to substring/wildcard for old-config compatibility; compose default changed to `172.16.0.0/12`. Added regression test (`test_auto_approve_cidr_precision` — `172.16.1.5` allowed / `172.1.2.3` rejected).

---

## 📊 Key Constants

| Constant | Default | Purpose |
|----------|---------|---------|
| Master port | 11452 | Cluster Master service port |
| Discovery port | 11450 | mDNS discovery port |
| Agent port | 11445 | Node Agent port |
| MCP port | 11446 | MCP Gateway port |
| Heartbeat timeout | 15.0s | Stale node threshold |
| Task timeout | 300.0s | Default task timeout |
| KV cache TTL | 3600.0s | Default KV cache expiry |
| Token budget | 10,000,000 | MCP gateway token limit |
| Degradation chain | 70b→32b→13b→8b→3b→1b | Model auto-degradation |

---

## 📋 Changelog

### v0.7.0 ✅ (Current) — Adversarial review fixes (AR 2026-08-24)

**P0 Security foundation refactor**
- [x] F1-F2 path traversal guard: cluster_sync path-traversal interception (NUL/absolute/drive/normpath + is_safe_path_segment)
- [x] F3-F4 SSRF guard: is_safe_peer_host rejects loopback/link-local/metadata/multicast, build_safe_url enforces scheme
- [x] F5 TLS key persistence: private key NoEncryption + file mode 0600
- [x] F6 TLS pinning: no pin → fail-closed (raise), pin fingerprint CERT_REQUIRED+VERIFY_PEER+DER callback
- [x] F7 FMP protobuf binary payload base64, forbid utf-8 replace corruption
- [x] F8 fmp_server shard_id/file_path path validation
- [x] F9 mDNS sticky-master + node_id bound to cluster_hash, prevents Worker forging master
- [x] F10 validate_node_id split into is_safe_path_segment + is_safe_peer_host, all sinks hardened

**P1 Production-path correctness + lifecycle**
- [x] #8 assign_task TOCTOU elimination: re-check-inside-lock
- [x] #9 heartbeat/fault routing via locked methods, unknown node 404 (fail-visible)
- [x] #10 real task cancellation: CANCELLED state, Master→Agent /api/tasks/cancel aborts running inference
- [x] #11 SIGTERM + graceful shutdown drain: asyncio.Event + signal handling + in-flight task coroutine gather
- [x] #12 config.save() atomic write: temp + os.fsync + os.replace
- [x] #13 task_id uuid4 replaces int(time.time())

**P1 HA wiring-or-cut + compliance boundary**
- [x] #14 Cut false HA claims: StandbyMaster/MasterElection/setup_election marked as un-wired dead code, production single-Master has no HA
- [x] #15 Compliance boundary: cloud_fallback **v0.8.2 scheduling path severed** (ClusterMaster no longer reaches cloud API); mcp_gateway/ast_diff/cluster_sync functional-debt pending migration to fusion-gateway (#106) / fusion-cowork (#61); cluster_sync LAN-only is_safe_peer_host hardening

**P2 Un-wired-prototype gating + security wiring + unbounded growth**
- [x] #17 DataScrubber adds openai_key/github_pat/slack_token/jwt_token + numeric-boundary fix for CJK adjacency; DataIsolation realpath+commonpath prevents symlink bypass; PermissionManager block-by-default (verified fail-closed)
- [x] #18 _metric_times list→deque(maxlen=10000) aligned with metrics, fixes unbounded growth + index misalignment
- [x] #24 WorkerSandbox wired to NodeAgent execution path: `execute_task` entry `_sandbox_gate` validates task-carried paths/network (`check_path_access`/`check_network_access`), rejects dispatch on failure (defect 5: security/ was dead code with zero filtering → in-process gate is a real defense); `_execute_model_sync` uses `is_safe_peer_host`+`build_safe_url`+`is_safe_path_segment` (consistent with master_server, fixes weak `.replace()`); does not call `apply_limits`/`setrlimit` (process-level resource limits would mis-kill a single long-running agent), `SandboxExecutor` is for subprocess plugins only
- [x] #23 M9/M10 integration-test gating — defect 4, four contract-bug fixes + regression gate (audit allows: wire OR pragma/remove; chose fix, real correctness, unit-testable):
  - caveman dictionary compression silent corruption: variable-length 2/4-byte codes with no delimiter → decompression only reads 2 bytes, never matches. Changed to fixed-length 2-byte codes (`>H`, `dictionary_size` truncated to 65536) + length-prefixed records (control byte 0x01=dictionary hit/0x02=raw passthrough)
  - autoscaler cooldown gate bypass: `update_config` zeroed `_last_action_time` → hot-reload bypassed cooldown, continuous scale-up/down storms. Changed to preserve last-action time, cooldown stays continuous across hot reloads
  - kv_store/fmp_server signature mismatch: `_on_kv_get` calls `get_entry(key, partition)`, original signature 1 param → inbound KV_GET always TypeError. `get_entry` adds optional `partition`, validates partition match when given; `ttl` None→`or 0.0` prevents `is_expired` TypeError
  - shard_replication quorum false claim: `_sync_via_fmp` fire-and-forget (`ensure_future` not awaited) yet returns `success=True`/`checksum_verified=True` → quorum write guarantee fabricated. Honest: only `await`-ed sends are `success`, fire-and-forget marked `success=False`+"unconfirmed" log, `checksum_verified` always False (no application-layer ACK)
- [x] M9/M10/shard_replication un-wired prototypes marked non-production (audit allows: wire OR pragma/remove); WorkerSandbox wired (#24), M9/M10 contract bugs fixed (#23)

Regression: 826 tests passed, 0 ruff errors.

### v0.7.1 ✅ — Second-round architecture audit fixes (2026-08-24, 22 items)

> Audit source: `audit/fusion-multi-node-audit-report-0824.md` (363 lines, H1-H5 / R1-R8 / E1-E9).
> Process: upstream issues filed first → code landed (PR #18, branch `release/v0.7.0-ar-audit-fixes`).

**P0 (H1/H4/E2/E7) — fake-impl/dead-code/honesty**
- [x] H1 Verified fusion-mlx has no `/distributed/*` endpoint → upstream issue #621; distributed_bridge Pipeline marked unimplemented + honest error (in-repo). Note: upstream #621/#630 later delivered, real E2E `tests/test_pipeline_e2e.py` verified passing
- [x] H4 Four dead subsystems (HA/autoscaler/cluster_sync/shard_replication) marked un-wired + externally-exposed routes removed
- [x] E2 kv_transfer `source_node` uses real node address (not `localhost`)
- [x] E7 kv_warm target node taken from the online-node table (not empty-set default)

**P1 (H2/H5/R1/R2/R8/E3/R6) — concurrency/performance/correctness**
- [x] H2 Split ClusterMaster single lock by resource domain (nodes / tasks / kv)
- [x] H5 LoadRouter/KVSharing threading.Lock → asyncio (removes cross-thread blocking)
- [x] R1 Hardware info cached at startup, heartbeat takes only dynamic fields
- [x] R2 task_id uuid4 replaces `int(time.time())`
- [x] R8+E3 distributed_bridge `raise_for_status` + response schema validation + error logging
- [x] R6 `get_online_nodes` pure snapshot (no side effects)

**P2 (H3/R3/R4/R5/R7/E1/E4/E5/E6/E8/E9) — prototype gating/security/robustness**
- [x] H3 HA dead code (StandbyMaster/MasterElection) documentation downgraded
- [x] R3 `sync_kv_cache` orchestrates cross-node transport via tensor backend, returns True (P3-28 / GAP-7 / #33 delivered in v0.11.0; synthetic default + MLX real-tensor env-gated pending upstream #650)
- [x] R4 `cancel_task` changed to `asyncio.gather` + reuses single AsyncClient (removes sequential notification)
- [x] R5 agent `_running_tasks` set + five-dimensional load reporting
- [x] R7 Model-size regex boundary matching (prevents `1b` mis-matching `10b/100b`)
- [x] E1 `ClusterSyncManager` moved to `__init__` + `start()`/`stop()` lifecycle (4 routes folded)
- [x] E4 Config field-level validation table + `schema_version` + `set_many` batch single-persist + load self-repair of dirty values
- [x] E5 plugin/action/model_name `is_safe_path_segment` sanitization + `_sandbox_gate` covers all task types
- [x] E6 `model_config` failure raises, not silently swallowed
- [x] E8 mDNS `_discovered` cross-thread `threading.Lock` (fixes dict-changed-size race)
- [x] E9 Quorum read/write without `storage_volume` always rejected (no longer self-consistently falsely reporting majority persistence)

Regression: 849 tests passed, 0 ruff errors.

---

## 🛣️ Roadmap

### v0.10.3 ✅ — GAP-8 Phase F1: multi-tenant token foundation (2026-08-27)
- [x] **per-user token store** (`security/user_store.py`) — `UserStore` file-persisted `users.json` (scrypt hash, 0600, atomic write), token format `fmu_<uid>_<secret>`, multiple-active issue/revoke/rotate
- [x] **UserRole** (`security/permission.py`) — ADMIN/USER/VIEWER orthogonal to NodeRole + `check_user_path_access` path authorization
- [x] **Dual-token middleware** (`utils/auth.py`) — `BearerAuthMiddleware` routes by `fmu_` prefix to UserStore, cluster_token hot path O(1) unchanged; falls back to pure cluster_token without user_store (single-tenant zero-config backward compatibility)
- [x] **First-boot bootstrap** — `FUSION_BOOTSTRAP_ADMIN` env auto-creates ADMIN + issues first token
- [x] 28 new tests (test_user_store 22 + TestUserTokenAuth 6); 1112 tests, 0 ruff errors

### v0.10.2 ✅ — GAP-5 dead-code cleanup/annotation (2026-08-26)
- [x] **autoscaler route explicit not-wired** (GAP-5) — `GET/PUT /api/v1/autoscaler/config` changed from ambiguous `{"enabled":False}` to 503 + detail stating un-wired; module kept pending migration
- [x] **StandbyMaster dead code deleted** (GAP-5) — zero instantiation/zero import/zero test/zero reference, independent of the wired MasterElection; HA path unified to MasterElection
- [x] 2 autoscaler not-wired tests; 1085 tests, 0 ruff errors

### v0.10.1 ✅ — GAP-6 rate-limit adaptation (2026-08-26)
- [x] **Client-side rate-limit adaptation** (GAP-6) — `agent/rate_pacer.py` intercepts fusion-mlx 429: reads `Retry-After`, exponential backoff retry (3 attempts, 10s budget, deterministic no jitter), raises `RateLimitExhausted` when exhausted
- [x] `FusionMLXBackend.chat`/`embed` wrapped via `dispatch_with_pacing` (no longer directly `raise_for_status` mis-classifying 429 as a logic error)
- [x] Master rate-limit classification fix — `rate_limited` → transient failure (`transient_fail`, retryable), not `logic_fail`, **does not call `report_fault`, does not ban healthy nodes**
- [x] Upstream fusion-mlx #635 CLOSED (PR #637, `--rate-limit 0` truly disables rate limiting, default off); explicit-ceiling 429 absorbed by backoff
- [x] 16 rate-limit tests (14 unit + 2 integration); 1083 tests, 0 ruff errors

### v0.10.0 ✅ — GAP-1 always-on SLA (2026-08-26)
- [x] **HA full-state sync** (GAP-1) — leader periodically pushes nodes/kv_cache/banned_nodes to standby; standby holds the complete topology, failover dispatches immediately (always-on gap ≤ election timeout ~10s). HA remains opt-in, 2+ Masters with explicit config get always-on, single-Master deployment unchanged
- [x] `/api/ha/sync-state` endpoint + `receive_synced_state` idempotent merge (lock order nodes→kv non-nested); `_state_sync_loop` (5s) wired to `start()`/`stop()` lifecycle
- [x] 6 HA state-sync tests (topology sync / idempotent / failover immediate dispatch / endpoint round-trip / single-Master no target / illegal status fallback)
- [x] 1067 tests, 0 ruff errors

### v0.10.0-rc.1 🔶 — Release Candidate (2026-08-26)
- [x] #31 retry node avoidance — `exclude_nodes` hard blocklist (select_nodes filter + assign_task passthrough + backup selection honors it, breaks the retry-back-to-bad-node loop)
- [x] GAP-4 CI fix — `pytest-randomly` declared + 3 Linux x86_64-incompatible tests skip-gated
- [x] Re-audit §8 release conditions 2/4/5 disclosure completed — GAP-1 HA SPOF / GAP-6 throughput ceiling / GAP-5 dead code + GAP-7 KV no-op
- [x] Single-tenant LAN conditionally commercial-ready; multi-tenant/remote SaaS + always-on SLA blockers declared
- [x] 1061 tests, 0 ruff errors, CI green

### v0.1.0 ✅
- [x] Cluster Master — node discovery, scheduler, task lifecycle, fault tolerance
- [x] Node Agent — hardware reporting, heartbeat, task execution, mDNS auto-discovery
- [x] mDNS Discovery — Bonjour zero-config service registration and browsing
- [x] FMP Protocol — three-layer binary protocol, AES-GCM encryption, circuit breaker
- [x] Distributed MLX — model sharding, pipeline/data parallelism, Caveman compression, KV cache sharing
- [x] MCP Gateway — unified MCP endpoint for Claude integration
- [x] Observability — metrics, logs, alerts, cluster reports
- [x] CLI — 15+ commands for cluster/node/task/config/network/caveman/kv management

### v0.3.0 ✅
- [x] Full audit remediation (P0-P3), 585 tests, 0 ruff errors

### v0.5.0 ✅
- [x] M1-02 device_model + UMA size in mDNS discovery & NodeInfo
- [x] M1-03 Heartbeat interval 5s→3s
- [x] M1-02/03 mDNS heartbeat_interval/timeout in broadcast properties, real device_model + uma_size_gb
- [x] M1-05 Manual IP join fallback (mDNS failure scenario)
- [x] M2-04 hop_count broadcast storm prevention
- [x] M2-01 Structured .proto with Envelope/Control/Payload messages
- [x] M2-03 FMP heartbeat sending (start_heartbeat/stop_heartbeat on connection)
- [x] M2-05 FMPInterface unified API (connect, send_heartbeat, send_task_assign, broadcast)
- [x] M3-01 Master/Worker permission isolation
- [x] M3-02 Node approval mechanism (integrated into /api/nodes/register)
- [x] M3-02 NodeInfo.role field (master/worker/standby)
- [x] M3-05 TaskSpec separation (task definition vs runtime state)
- [x] M3-02 NodeStatus.FAULT enum value
- [x] M3-03 Master election (Raft-simplified with priority)
- [x] M4-01 LoadMetrics + LoadRouter structured load-aware routing
- [x] M4-02 Local-force gate (≤0.5B models forced local)
- [x] M4-03 VRAM-first scheduling (≥13B models, thread-safe strategy switching)
- [x] M4-04 Task auto-degradation (70b→32b→13b→8b→3b→1b)
- [x] M4-05 Cloud API fallback (OpenAI/Anthropic, daily cost limits)
- [x] M5-01/02/05 Task sharding (inference/AST/vectorize, by_file/by_document/by_batch, result merge)
- [x] M5-03 Timeout task auto-retry queue (_enqueue_retry, max 1 attempt)
- [x] M5-03 TaskShard timeout field + is_timed_out property
- [x] M5-04 Task full-lifecycle cancel (recursive sub-task)
- [x] M6-01 Master data isolation enforcement
- [x] M6-01 Worker temp dir cleanup (auto mkdir/rmtree on task execute)
- [x] M6-02 Worker sandbox (resource limits, path/network filtering, usage monitoring, subprocess env)
- [x] M6-03 Node approval integrated into register endpoint
- [x] M6-04 AST diff-only transmission
- [x] M6-04 Data scrubbing (phone, email, API key, ID card, etc.)
- [x] M6-04 FMPCrypto (AES-256-GCM with ECDH-negotiated session keys)
- [x] M7-06 Monitoring API v1 (/api/v1/nodes/{id}/metrics, /api/v1/tasks/{id}/progress)
- [x] M7-06 /api/v1/cluster/stats + /api/v1/tasks/{id}/timeline endpoints
- [x] M8-01 LogLevel standard enum (INFO/WARN/ERROR/FATAL) + Master all-node log aggregation (collect_node_logs)
- [x] M8 Log store & export (JSON/CSV/text)
- [x] M8 Intelligent fault diagnosis (pattern matching + root cause)
- [x] M9-02/03 Storage data transfer + capacity monitoring + LRU eviction
- [x] M9-01 Distributed KV Store (TTL, partitions, snapshot/restore, persistence)
- [x] M9-02 Quorum read/write for shard replication
- [x] M9-03 KV Store snapshot/restore
- [x] M9-04 FMP protocol KV cache sync
- [x] M9 Storage volumes (local/shared/distributed)
- [x] M9 Shard replication with health tracking
- [x] M9 Checkpoint persistence
- [x] M9 Model shard distribution
- [x] M10-02/03 Autoscaler builtin scale actions (standby activation + migrate-then-deactivate)
- [x] M10 Autoscaler (conservative/balanced/aggressive policies)
- [x] M10 Task migration on scale-down
- [x] protobuf>=5.0.0 dependency
- [x] P0: FMPServer inbound TCP server (cross-node shard/KV transport)
- [x] P0: Protobuf structured encoding (envelope/control/payload fields)
- [x] P0: Autoscaler hot-reload (update_config/update_policy)
- [x] P0: Cross-node FMP transport (ShardReplicator + DistributedKVStore remote ops)
- [x] P1: Log retention 7 days (168h default) + log export API
- [x] P1: Smart optimization suggestions (alert-driven + error pattern analysis)
- [x] P1: SandboxExecutor (macOS sandbox-exec / Linux unshare / python-resource fallback)
- [x] P2: Metal AES-GCM acceleration (Apple Silicon CommonCrypto bridge + auto-fallback)
- [x] P2: CLI --transport fmp wiring (FMPServer + FMPConnectionManager)
- [x] 805 tests, 0 ruff errors

### v0.8.2 ✅ — Production-readiness hard blockers + soft debt (2026-08-25)
- [x] H3 Master task persistence + crash-startup recovery (atomic persist, RUNNING→PENDING re-dispatch)
- [x] H2 launchd process supervisor — crash self-healing loop (KeepAlive restart + H3 recovery)
- [x] H4 cloud_fallback scheduling path severed (100% local compliance); functional-debt pending migration to fusion-gateway/fusion-cowork
- [x] H1 PIPELINE token output — upstream fusion-mlx #630 decode endpoint delivered (closed); real E2E `tests/test_pipeline_e2e.py` verified passing
- [x] S1 task-level circuit breaker — dispatch failure reports fault + select_nodes skips banned nodes
- [x] S2 production metrics endpoint /api/v1/metrics (Prometheus exposition)
- [x] S3 load/stress baseline tests (scheduling-layer throughput / tail latency / zero loss)
- [x] S4 real-model integration-test coverage (DATA parallel E2E real inference + KV sharing E2E real ASGI route chain; plus 3 production bug fixes: FusionMLXBackend `/v1/*` missing auth / KVSharingManager cross-node HTTP missing auth / KVWarmRequest contract mismatch)
- [x] 888 tests, 0 ruff errors

### v0.8.3 ✅ — Container scale stress + scheduling TOCTOU fix (2026-08-25)
- [x] P0-A HA dual-Master election wired to `start(ha_config=)` default-on (election HTTP vote layer reused, no external deps)
- [x] P0-B Containerization — `Dockerfile` + `docker-compose.yml` (1 Master + N Agent, `--scale agent=N` unlimited scale-out); agent reconnects via container bridge IP, does not occupy host ports; inference engine bare-metal `host.docker.internal:11434` reconnect
- [x] BUG#3 Agent in-container local-IP detection — cross-platform socket UDP connect (zero deps, takes master reconnect source IP), replaces macOS-only `ipconfig`
- [x] BUG#4 NodeApprovalManager approval path dropped hardware metadata — register passes through metadata, approve rebuilds NodeInfo from metadata (mem/max_tasks/cpu no longer falls back to default 0/4)
- [x] **Scheduling TOCTOU race fix** — `select_nodes` ran outside the lock → concurrent preemption of the preferred node when full → in-lock backup selection of free nodes (`_select_free_nodes_locked`), no longer returns 503 outright. c8 concurrent 40 tasks 0× 503 verified
- [x] `FUSION_AGENT_MAX_TASKS` env — per-agent concurrency cap tunable (16 during stress)
- [x] Container stress client `scripts/stress_live.py` — concurrent submission via master:11452, measures throughput/tail latency/success rate; `--rps` client rate gate aligned to upstream rate-limit bucket
- [x] Cluster ops tool `scripts/cluster_ops.py` — approve-all / status / unban-all
- [x] P1-E Observability stack template — Grafana dashboard / Prometheus / Alertmanager (deploy/observability/)
- [x] Phase-3 scheduling stress passed — 4 nodes 50 tasks success 1.0, c8 contention 40 tasks success 1.0, 0× 503
- [x] Upstream blocker fusion-mlx #635 — `--rate-limit 0` does not disable the module-level 60rpm limiter, multiple agents sharing one api_key hit one bucket, issue filed (not fixable in this repo)
- [x] 911 tests, 0 ruff errors

### v0.8.8 ✅ — Enterprise audit P0 remediation (2026-08-26, AR #24)

> Audit source: `audit/fusion-multi-node-audit-result-0826.md` (29 items, P0×8). This batch landed P0-1~P0-8 (P0 fully cleared).
- [x] **P0-1 HA leader heartbeat + term/voted_for persistence** — fixes multi-Master term churn causing continuous re-election; `election_state.json` atomic persist, restart recovers voting state
- [x] **P0-2 dispatch-failure retry** — `_dispatch_to_node` HTTP non-200/status!=ok raises → `report_fault("dispatch_failed")` + retry; retry-exhausted → FAILED (not cloud fallback)
- [x] **P0-3 agent internal error into circuit breaker** — 200+ok but result.error (OOM/bad model) → `report_fault("agent_internal_error")` + node FAULT + task FAILED (not retry)
- [x] **P0-4 default security posture** — E5 path-traversal gate enforced even without sandbox (plugin/action/model_name segment validation); README discloses default security boundary + minimal hardening steps + Preview positioning
- [x] **P0-5 SSRF validation unified** — H1 register rejects cloud-metadata/link-local IP; H2 cancel notification goes through build_safe_url; H3 KV cross-node outbound guard (3 sites); added `is_registerable_host`/`is_safe_outbound_host` two-semantics separation
- [x] **P0-6 deep health check** — `/api/health` liveness (disk/mem/task-store, HTTP 200 body status) + `/api/health/deep` readiness (master +node quorum / agent +fusion-mlx `/v1/models`); compose healthcheck validates body status; both endpoints Bearer-exempt
- [x] **P0-7 declaration alignment** — README fixed MCP/Observability/FMP/KV-tensor/PIPELINE dead-code annotations; `__init__.py` separates MasterElection(wired) vs StandbyMaster(dead); cluster_sync docstring fixed out-of-date; CLAUDE.md single-lock → three-locks
- [x] **P0-8 Observability wiring** — `ClusterObservability` wired to `ClusterMaster.start/stop` lifecycle + `_health_check_loop` periodically collects node metrics/alert rules (dedup by node_id+title, prevents deque flooding); cli injects an instance with configured retention; `/api/v1/observability/{logs/export,suggestions,alerts}` no longer 503
- [x] 994 tests, 0 ruff errors

### v0.8.8 ✅ — Enterprise audit P1 remediation (2026-08-26, AR #24)

> Audit source: `audit/fusion-multi-node-audit-result-0826.md` (29 items, P1×9). This batch landed P1-9~P1-18 item by item.
- [x] **P1-9 KV cache persistence** (C12) — `KVSharingManager` adds disk `save()`/`load()` (atomic tmp+replace, skips expired entries); `AgentServer.start` restores local KV cache, `stop` persists → agent restart can recover/pre-warm (audit §6.3, original pure-memory OrderedDict lost on restart)
- [x] **P1-10 async blocking elimination** (C13/§4.1/§4.5) — sync blocking inside async handler/path (psutil 100ms, system_profiler up to 10s, sysctl, airport, ifconfig) all moved off the event loop via `asyncio.to_thread`: master_server `get_node_load`, node_agent `report_hardware`, cluster_master `_start_mdns`, network_topology `detect()` full chain (5 subprocess sites + `_get_interface_type` converted to async); added 3 cross-thread assertion tests (calling thread ≠ event-loop thread)
- [x] **P1-11 fsync moved out of lock** (C14/§4.2) — `_persist_tasks_locked` split into in-lock snapshot + out-of-lock `_write_task_store` (includes `os.fsync` blocking I/O); 7 state-write sites (assign/complete_dispatch/cancel/receive_synced_tasks/_persist_tasks) changed to in-lock snapshot→release lock→persist; added assertion test (`_tasks_lock.locked()` is False during persist)
- [x] **P1-12 find_kv_cache lock-order fix** (C15/§2.4/§4.4) — `find_kv_cache` originally held `_kv_lock` while `await _is_node_online` (cross-domain `_nodes_lock`) = kv→nodes nested lock-holding, violating the nodes→kv convention, deadlock risk; changed to snapshot the online-node set under `_nodes_lock`→release→match under `_kv_lock`, the two lock domains are not nested; added assertion test (`_kv_lock` holding region must not acquire `_nodes_lock`)
- [x] **P1-13 per-task HTTP timeout** (C16/§5.4) — `_dispatch_to_node` HTTP timeout was fixed client default 300s, >300s tasks pre-empted → FAILED without retry; changed to per-request `timeout=task.timeout_seconds+30` buffer (floor 30s), letting the task-level timeout (`_check_task_timeouts`→TIMEOUT+retry) trigger before the HTTP dead-agent backstop; added 2 tests (600s→630s, 1s→floor 31s)
- [x] **P1-14 dispatch dedup token** (C17/§5.3) — `/api/execute` payload originally hardcoded `task_id=""` → agent cannot detect duplicate dispatch (master re-dispatches same task_id to same node = double inference); master `_dispatch_to_node` passes real task_id (pipeline segments `{task_id}-step{N}`), `ExecuteRequest` adds task_id field passthrough, `NodeAgent.execute_task` rejects same task_id already running (returns dedup_blocked → master classifies as logic error, no retry); direct calls without task_id get an anonymous id to prevent `_running_task_handles` key collision; added 2 tests (reject duplicate / anonymous sequence increment)
- [x] **P1-15 H3 persist-failure visible** (C18/§5.6) — `_write_task_store` persist failure originally only `logger.error` silently swallowed (task persistence is the foundation of crash recovery; failure means Master crash loses all RUNNING tasks); changed to wire P0-8 Observability emitting a `critical` alert (with disk/permission guidance) + `task_persist_failed` metric; added test (failure→critical alert + metric 1.0)
- [x] **P1-16 log rotation** (§6.4) — `setup_logger` appends a `RotatingFileHandler` (10MB×5 bounded) when env `FUSION_MULTINODE_LOG_FILE` is set; `start.sh` nohup stdout → `/dev/null` (app logs go through the file handler for bounded rotation, avoids duplicate unbounded growth with nohup stdout.log), stderr still captured for crash stacks; launchd plist `StandardOutPath`→`/dev/null` + passes `FUSION_MULTINODE_LOG_FILE` env; `docker-compose.yml` both services add `logging: json-file max-size 10m max-file 3`; added 4 tests (env trigger / no-env single handler / write+cap / bad-path fallback to console)
- [x] **P1-17 protocol-version compat check** (§6.7) — `NodeRegisterRequest` adds `protocol_version` field (multi-node protocol version, not mlx_version); `NodeAgent` reports `__version__` on register; `master_server` `_check_protocol_compat` compares agent version ≥ `MIN_COMPAT_PROTOCOL_VERSION` (0.8.0), below rejects 400 + downgrade guidance (upgrade to ≥ min); empty/non-standard format passes through + warn (gray-period backward compat, no false reject); added 4 tests (reject incompatible / pass compatible / pass old-client empty / pass non-standard format)
- [x] **P1-18 failure push channel** (§5.5) — `ClusterMaster` adds a task-status event bus (`_event_subscribers` asyncio.Queue list, `subscribe_task_events`/`unsubscribe_task_events`/`_emit_task_event` non-blocking broadcast, full queue drops oldest); `_finalize_task`(completed/failed)/`_enqueue_retry`(retry/failed)/`assign_task`(running)/`cancel_task`(cancelled) state-transition points all emit (in-lock pure-memory, does not block scheduling); added `GET /api/tasks/events` SSE endpoint (text/event-stream, ready first-frame + 15s keepalive, BearerAuthMiddleware auth, route registered before `/api/tasks/{task_id}` to avoid path-param capture); added 8 tests (FAILED/COMPLETED/retry-exhausted/cancel emit / full-queue drop-oldest / unsubscribe stops push / SSE route contract / 401 auth)
- [x] 1029 tests, 0 ruff errors

### v0.8.8 ✅ — Enterprise audit P2 remediation (2026-08-26, AR #24)

> Audit source: `audit/fusion-multi-node-audit-result-0826.md` (29 items, P2×8). This batch landed P2-19~P2-26 item by item.

- [x] **P2-22 Master rate limiting** (§3.8) — `MasterServer` adds `RateLimitMiddleware` (reuses agent_server `InMemoryRateLimiter`, 120 req/60s/IP, threshold higher than agent because intra-cluster heartbeat 10s×N + dispatch traffic); health-check/docs exempt; prevents DoS + approval queue (`max_pending=100`) exhaustion. Added 2 tests (429 burst / health exempt)
- [x] **P2-26 retry-count persistence** (§5.7) — `_retry_count` is a dynamic attribute, `asdict` does not serialize it → Master crash-restart zeroes it → allows extra retries beyond `_max_retry_attempts`. `_task_to_dict` explicitly serializes `_retry_count`, `_task_from_dict` restores it; persistence loop test (persist includes field + new Master restores preserving budget, not zeroed)
- [x] **P2-25 stale-doc cleanup** (§1.8/§2.4) — three stale declarations corrected: `cluster_sync.py:5` docstring (self-described "not wired" → actually wired to master_server lifecycle), CLAUDE.md single-lock description (→ "split into three locks nodes→tasks→kv"), `__init__.py` HA description (MasterElection wired / StandbyMaster dead-code boundary clarified); verified autoscaler "un-wired dead code (always 404)" declaration still holds
- [x] **P2-23 compose default-credential removal** (§6.10) — `docker-compose.yml` removed `FUSION_CLUSTER_TOKEN:-dev-cluster-token-change-me` and `FUSION_MLX_API_KEY:-dahai168` weak defaults, switched to `${VAR:?hint}` so compose fails to start with a hint when unset; added `.env.example` template (with strong-random-value generation guidance); `.gitignore` adds `.env` to prevent real credentials entering the repo
- [x] **P2-24 PII redaction scope documented** (§3.7) — verified `data_scrubber`/`FMPCrypto`/`SecureTransferPipeline` only instantiated on the FMP path (`fmp_server.py:230` DATA_SYNC), default HTTP dispatch path is plaintext with no redaction no encryption; README security-boundary table + Capabilities already mark "FMP path only"; CLAUDE.md security module adds scope note (audit allows "or explicitly FMP-path-only protection"); also corrected the Master rate-limit line (no longer "unlimited" after P2-22)
- [x] **P2-19 deployment-plan docs** (§6.5) — added `docs/DEPLOYMENT.md` clarifying the local-first Apple Silicon small-cluster positioning: four modes (single-machine nohup / single-machine launchd / docker-compose multi-machine / multi-Master HA tech-preview) + scale-out resources + "non-goal — why no K8s" (platform-bound MLX/Metal / offline constraint / scale mismatch / ops cost, enterprise orchestration is fusion-gateway's job); README links; also corrected `docs/HA-CRASH-RECOVERY.md` stale declaration (MasterElection is wired, not a prototype)
- [x] **P2-20 config hot-reload** (§6.8) — added `POST /api/v1/config/reload` endpoint (Bearer auth): re-reads `config.json` + re-applies runtime-tunable fields (`scheduling.tenant_max_concurrent` → `configure_scheduling`); fields requiring restart (port/ha_config/mdns) are listed in the response's `restart_required` hint; `MasterServer(config=)` + `ClusterMaster.start(config=)` inject `ClusterConfig`, CLI passes `_config`; without injection returns 503; added 3 tests (hot-reload re-applies quota / edit-disk-then-reload takes effect / no-injection 503 / no-auth 401)
- [x] **P2-21 ops runbook** (§6.9) — added `docs/OPERATIONS.md` covering 10 handling flows (diagnosis entry / node offline / Master offline / split-brain / disk-full / fusion-mlx unreachable / task backlog / version upgrade / backup-restore / token rotation), each section has symptom/diagnosis/handling/recovery-verify; commands include health/metrics/alerts endpoints + `~/.fusion/multi-node/` persistence paths + ports (11452/11458) + H3 recovery/circuit-breaker/priority-queue cross-refs; README links

### P3 — Long-term (audit §5.9 / feature completeness)

- [x] **P3-27 PIPELINE end-to-end** — upstream fusion-mlx `/distributed/*` delivered (issue #621/#630 closed: load_shard/pipeline_step/decode/sync_weights); multi-node client stubs `node_agent.load_shard`/`pipeline_step` + `_execute_pipeline_step` wired; real E2E `tests/test_pipeline_e2e.py` (Llama-3.2-1B 16 layers split [0,8]/[8,16] b64.npy tensor round-trip) verified passing
- [x] **P3-28 tensor-level KV cross-node transport** (GAP-7, #33) — delivered in v0.11.0: `sync_kv_cache` via a pluggable tensor backend (synthetic default / MLX real-tensor env-gated `FUSION_KV_TENSOR_BACKEND=mlx`) orchestrates source `/api/kv/export` → target `/api/kv/import`, returns True; `KVShard.tensor` base64-compressed travels with JSON cross-node; synthetic backend satisfies #33 acceptance (tensor round-trip across 2 agents); real tensor awaits upstream fusion-mlx issue #650 to activate (404→degrade to synthetic + warn)
- [x] 1203 tests, 0 ruff errors
- [x] **P3-29 partial-success semantics** (§5.9) — DATA parallel with some nodes succeeding and some failing no longer marks the whole task FAILED: added `TaskStatus.PARTIAL` terminal state (no retry, preserves `result.outputs` for the client to take partial results); `_dispatch_data` aggregates three states (all-success COMPLETED / partial-success PARTIAL / all-failed FAILED); `_finalize_task(partial=)` branch + event bus emits `partial`; stats `partial_tasks` count + Prometheus gauge `fusion_cluster_tasks_partial`; CLI 🟡 icon; `/api/tasks` progress event `partial`; crash-recovery PARTIAL terminal state preserved (not re-dispatched); integration test `test_data_parallel_partial_success` (agent-a success + agent-b fail → PARTIAL preserves output)
- [x] 1036 tests, 0 ruff errors

### Future
- [ ] Distributed MLX operator bridge (mlx.distributed API)
- [ ] Distributed MLX operator bridge (mlx.distributed API)
- [ ] Plugin ecosystem cluster registration
- [ ] Cluster monitoring dashboard (fusion-studio)
- [ ] Thunderbolt RDMA acceleration
- [ ] Cross-node KV cache with Caveman compression

---

## 🔒 Security

### ⚠️ Default Deployment Security Boundary

Current version is positioned as a **technical preview (Preview)**, suitable for single-machine
development and trusted-LAN experimentation. It is **not** a production-grade commercial cluster
release. The default deployment posture has these known limits — hardened alternatives are listed:

| Area | Default posture | Hardening step |
|------|-----------------|----------------|
| **Node identity** | Single shared Bearer token (`~/.fusion/multi-node/.cluster_token`) is the only node identity; one leak = whole cluster compromised | Provision per-node certs + enable mTLS (below) |
| **mTLS** | **Off by default** — intra-cluster HTTP is plaintext, zero node-identity verification at transport | `FUSION_MTLS_ENABLED=1` + `provision_cluster`/`provision_node` (see `security/mtls.py`) |
| **Worker sandbox** | **`None` by default** — no OS-level resource isolation for inference/plugin tasks. Untrusted-input path traversal **is** still enforced at the task gate (E5, always-on); model_sync network & model_path whitelisting are only active when a sandbox is configured | Construct `WorkerSandbox(SandboxConfig(...))` and pass to `NodeAgent(sandbox=...)` |
| **Master rate limit** | 120 req/60s/IP global throttle (v0.8.8 P2-22) — guards register/join/vote/submit against burst DoS + approval-queue exhaustion; health/metrics/SSE exempt. For hostile-LAN exposure add a reverse proxy with finer policy | Deploy behind a rate-limiting reverse proxy for finer per-route policy on untrusted-LAN exposure |
| **PII scrubbing / AES-GCM** | Wired only on the FMP protocol path (not the default HTTP dispatch path) | Use `--transport fmp` to get encrypted, scrubbed transport |
| **Availability** | Single Master — Master crash = whole-cluster stall. Multi-Master HA election exists (P4) but is **technical preview**, not production-validated | Run on supervised host (launchd KeepAlive) for crash restart; do not rely on HA for production SLA |

**Minimum hardening for any multi-machine deployment** (trusted LAN):
1. Enable mTLS: `provision_cluster` once, `provision_node` per node, set `FUSION_MTLS_ENABLED=1` + cert paths on every node.
2. Restrict `FUSION_AUTO_APPROVE_PATTERNS` to your exact subnet CIDR (not broad patterns).
3. Run Master under `./start.sh install-launchd` (KeepAlive crash restart).
4. Do not expose Master/Agent ports to public networks.

### Capabilities

- **100% local offline** — Zero external network dependencies
- **Node approval** — New nodes require approval or pattern-based auto-approval
- **Master/Worker isolation** — Role-based permission, API path access control
- **mTLS node auth** — Private CA + per-node leaf cert, env-gated mutual TLS — **off by default, opt-in** (#80)
- **Multi-tenant quota + priority queue** — Per-tenant concurrent cap, over-quota enqueue, priority-ordered dispatch (#81)
- **Real-network E2E** — True port bind + real HTTP cross-process; node drop/reconnect; docker-compose cross-container (#76)
- **KV cache stress** — N-node cross-HTTP KV warm/transfer at scale, 0-loss migration, p99 latency baseline (#79)
- **Cross-node KV lookup** — `lookup_remote` contract-aligned (route→found/entry→decode); real-chain E2E lock (v0.8.5)
- **Auto node approval** — Trusted-subnet auto-join via `FUSION_AUTO_APPROVE_PATTERNS` env; CIDR-precise (`172.16.0.0/12`), substring/wildcard fallback (v0.8.4→v0.8.5)
- **Worker sandbox** — CPU/memory/disk limits, path & network whitelisting — **opt-in (`NodeAgent(sandbox=...)`), not default**; E5 untrusted-input traversal guard is always-on regardless of sandbox
- **Data scrubbing** — Auto-detect and redact PII (phone, email, API keys, ID cards) — **FMP path only**
- **AES-GCM encryption** — FMP protocol encrypted communication — **FMP path only**
- **Circuit breaker** — Automatic fault isolation for failing nodes (dispatch failure + agent-internal error both visible)
- **Cross-node guard transport** — Issue #52: 3 TRANSPORT primitives for fusion-guard consumption — audit-chain HMAC (tamper-evident `seq`/`prev_hash`/`mac`, `GET /api/v1/audit/chain` segment fetch) + cluster-wide rule-epoch broadcast (`GET /api/v1/rules/epoch`) + cross-node confirm relay (`POST /api/confirm`). HKDF-SHA256 derives 3 domain-separated MAC keys from cluster_token (no new secret). 100% local/LAN, no cloud. Multi-node defines TRANSPORT+IDENTITY+KEY SCHEME only; guard implements consumer.
- **No telemetry** — No analytics, no phoning home

---

## 📄 License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

Contributions welcome! Please ensure:

1. Tests pass: `pytest tests/ -v`
2. Lint passes: `ruff check fusion_multi_node/`
3. 4-space indentation, no docstrings (self-documenting names)
4. All classes use `logging.getLogger(__name__)`

---

<p align="center">
  <strong>Fusion-Multi-Node — Pool Macs, Unify Inference, Scale Locally.</strong>
</p>
<p align="center">
  <sub>Built with ❤️ by Fusion-MLX Team</sub>
</p>

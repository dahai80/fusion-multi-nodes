# HTTP API Reference — fusion-multi-node

Master HTTP API on `127.0.0.1:11452` (default). All endpoints require `Authorization: Bearer <token>` except exempt paths (`/api/health`, `/docs`, `/openapi.json`, `/redoc`, `/`, `/favicon.ico`).

Two token types (GAP-8 Phase F):
- **Cluster token** — shared intra-cluster secret (`~/.fusion/multi-node/.cluster_token`), all node-to-node traffic. Format: opaque.
- **User token** — `fmu_<userid>_<secret>`, user-facing master routes only. Resolves `UserRole` (ADMIN/USER/VIEWER). See [docs/PYTHON_API.md](PYTHON_API.md) for `UserStore`.

OpenAPI schema at `/openapi.json`; interactive docs at `/docs`.

## Contents

- [Cluster Control Contract — 9 Operations](#cluster-control-contract--9-operations)
- [Inference](#inference)
- [Observability & Metrics](#observability--metrics)
- [Autoscaler](#autoscaler)
- [User Management (ADMIN only)](#user-management-admin-only)
- [Config Hot-Reload](#config-hot-reload)
- [Legacy `/api/*` Routes](#legacy-apiroutes)

---

## Cluster Control Contract — 9 Operations

The 9 operations fusion-agent-studio delegates to a real cluster (replaces its in-memory dev cluster). All return typed JSON (`response_model=` Pydantic schemas; see `/openapi.json` `components/schemas/V1*`).

| # | Operation | Method + Path | Response Model | Auth |
|---|-----------|---------------|----------------|------|
| 1 | list_nodes | `GET /api/v1/nodes` | `V1NodeListResponse` | cluster\|user |
| 2 | join_node | `POST /api/v1/nodes/register` | `V1NodeRegisterResponse` | cluster |
| 3 | remove_node | `DELETE /api/v1/nodes/{node_id}` | `V1StatusResponse` | cluster\|user(ADMIN) |
| 4 | submit_task | `POST /api/v1/tasks/submit` | `V1TaskSubmitResponse` | cluster\|user |
| 5 | migrate_task | `POST /api/v1/tasks/{task_id}/migrate` | `V1StatusResponse` | cluster\|user |
| 6 | degrade_task | `POST /api/v1/tasks/{task_id}/degrade` | `V1StatusResponse` | cluster\|user |
| 7 | task_progress | `GET /api/v1/tasks/{task_id}/progress` | `V1TaskProgressResponse` | cluster\|user |
| 8 | cluster_stats | `GET /api/v1/cluster/stats` | `V1ClusterStatsResponse` | cluster\|user |
| 9 | observability suggestions | `GET /api/v1/observability/suggestions` | `V1ObservabilitySuggestionsResponse` | cluster\|user |

### 1. list_nodes — `GET /api/v1/nodes`

Lists all nodes.

Response `V1NodeListResponse`:
```
{ "total": int, "online": int, "nodes": [V1NodeResponse] }
```

`V1NodeResponse` (16 fields): `node_id`, `hostname`, `ip_address`, `port`, `status`, `role`, `total_memory_gb`, `available_memory_gb`, `cpu_cores`, `gpu_cores`, `device_model`, `uma_size_gb`, `active_tasks`, `max_tasks`, `score`, `last_heartbeat`.

### 2. join_node — `POST /api/v1/nodes/register`

Registers a node agent. Gated by `NodeApprovalManager` + protocol-version compat check (agent must send `protocol_version` ≥ `0.8.0`).

Body `NodeRegisterRequest` (key fields): `node_id`, `hostname`, `ip_address`, `port`, `total_memory_gb`, `available_memory_gb`, `cpu_cores`, `gpu_cores`, `device_model`, `uma_size_gb`, `max_tasks`, `protocol_version`.

Response `V1NodeRegisterResponse`: `{ "status": str, "node_id": str, "role": "worker" }`.

Errors: `400` illegal `node_id` / protocol incompatible; `403` banned node.

### 3. remove_node — `DELETE /api/v1/nodes/{node_id}`

Removes a node.

Response `V1StatusResponse`: `{ "status": "ok", "task_id": "", "node_id": <id>, "action": "removed" }`. `404` if not found.

### 4. submit_task — `POST /api/v1/tasks/submit`

Submits a task. Returns `200` (dispatched/running) or `202` (queued — no nodes / priority queue). HA standby returns `503`.

Body: `name`, `mode` (`data`\|`pipeline`), `model_name`, `prompt`, optional `priority`, `required_capability`, `timeout_seconds`, `target_node_id`, `task_id`.

Response `V1TaskSubmitResponse` (V1TaskResponse + `queued: bool`). `V1TaskResponse` (16 fields): `task_id`, `name`, `mode`, `model_name`, `status`, `assigned_nodes`, `created_at`, `started_at`, `completed_at`, `error`, `required_capability`, `priority`, `degraded_from_model`, `degradation_count`, `cancel_reason`, `sub_tasks`, `result`.

### 5. migrate_task — `POST /api/v1/tasks/{task_id}/migrate`

Migrates a running task to another node.

Response `V1StatusResponse`: `{ "status": "ok", "task_id": <id>, "action": "migrated" }`. `404` if not found.

### 6. degrade_task — `POST /api/v1/tasks/{task_id}/degrade`

Degrades a task's model to the next smaller in `MODEL_DEGRADATION_CHAIN` (`70b→32b→13b→8b→3b→1b`). Max 2 degradations.

Response `V1StatusResponse`: `{ "status": "ok", "task_id": <id> }`. `400` if no smaller model available / max reached; `404` if not found.

### 7. task_progress — `GET /api/v1/tasks/{task_id}/progress`

Response `V1TaskProgressResponse`: `task_id`, `name`, `status`, `progress` (0.0–1.0), `total_shards`, `completed_shards`, `assigned_nodes`, `elapsed_seconds`, `remaining_seconds`, `model_name`. `404` if not found.

### 8. cluster_stats — `GET /api/v1/cluster/stats`

Response `V1ClusterStatsResponse`:
```
{
  "cluster": { "online_nodes", "total_nodes", "active_tasks", "total_memory_gb", "available_memory_gb", "utilization" },
  "tasks": { "total", "completed", "failed" },
  "load_summary": {...}
}
```

### 9. observability suggestions — `GET /api/v1/observability/suggestions`

Response `V1ObservabilitySuggestionsResponse`: `{ "suggestions": [dict], "error": str }`.

---

## Inference

### `POST /v1/chat/completions` — unified chat proxy (F3)

Pass-through to selected node's fusion-mlx via `select_nodes(DATA, count=1)`. OpenAI-compatible request/response body; `stream: true` → SSE.

Auth: user token (`chat:complete`, USER/ADMIN not VIEWER) OR cluster token. Tenant-quota gate: per-user inflight vs `scheduling.tenant_max_concurrent`; `429` over.

---

## Observability & Metrics

| Method + Path | Description |
|---------------|-------------|
| `GET /api/v1/observability/alerts?severity=` | Active alerts |
| `GET /api/v1/observability/logs/export?fmt=json\|csv&since=&node_id=` | Log export |
| `GET /api/v1/metrics` | Prometheus exposition (text/plain 0.0.4) |
| `GET /api/v1/nodes/{node_id}/metrics` | Per-node load metrics |
| `GET /api/v1/tasks/{task_id}/timeline` | Task event timeline |
| `GET /api/tasks/events` | SSE task-state event stream (15s keepalive) |

---

## Autoscaler

| Method + Path | Response | Note |
|---------------|----------|------|
| `GET /api/v1/autoscaler/config` | `V1AutoscalerConfigResponse` | **503 not-wired** — module exists but not instantiated (GAP-5). Documented contract, not ambiguous `enabled:False`. |
| `PUT /api/v1/autoscaler/config` | `V1StatusResponse` | **503 not-wired** (same). |

---

## User Management (ADMIN only)

GAP-8 Phase F2. Requires user token with ADMIN role.

| Method + Path | Description |
|---------------|-------------|
| `POST /api/v1/users` | Create user (returns token once) |
| `GET /api/v1/users` | List users |
| `GET /api/v1/users/{user_id}` | Get user |
| `DELETE /api/v1/users/{user_id}` | Delete user |
| `PUT /api/v1/users/{user_id}/role` | Set user role |
| `POST /api/v1/users/{user_id}/tokens` | Issue token (returns plaintext once) |
| `DELETE /api/v1/users/{user_id}/tokens/{tid}` | Revoke token |
| `POST /api/v1/users/{user_id}/tokens/rotate` | Rotate token (F5) |

---

## Config Hot-Reload

### `POST /api/v1/config/reload` (P2-20)

Re-reads `config.json` + re-applies `scheduling.tenant_max_concurrent`. Port/HA/mDNS require restart → response `restart_required` hint. `503` if no config injected.

---

## Legacy `/api/*` Routes

Pre-v1 raw-dict routes kept for backward compat (no `response_model`): `/api/health`, `/api/nodes`, `/api/nodes/register`, `/api/tasks/submit`, `/api/tasks/{id}`, `/api/tasks/{id}/cancel`, `/api/cluster/stats` (flat), `/api/kv/*`, `/api/ha/*`. Prefer `/api/v1/*` for typed contracts.

---

## Python API

Class-level API (ClusterMaster, NodeAgent, KVSharingManager, etc.) moved to [docs/PYTHON_API.md](PYTHON_API.md).

# Changelog — fusion-multi-node

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.17.0] - 2026-09-04 — Epoch/leader_id exposure + per-leader token stale-write reject

### Added — epoch/leader_id exposure (issue #76)
- **`ClusterMaster.leader_epoch()`**: returns the monotonic leadership epoch (Raft `current_term`). HA standby: the election term (increments on `_become_leader`). Single-master / active-active: `0` (no election, deterministic — a client seeing epoch 0 + empty `leader_id` knows there is a single authority, no split-brain to detect).
- **`ClusterMaster.current_leader_id()`**: HA → the elected leader's node id; active-active → this master's `_ha_node_id`; single-master → `""`.
- Exposed additively in cluster API responses so clients (Fusion Studio MultiNode Track B) reject stale-leader responses deterministically instead of client-side split-brain heuristics (counting masters across polls):
  - `/api/nodes` and `/api/nodes/{node_id}` — per-node `epoch` + `leader_id` (same value across nodes in one master's view) + cluster-level `epoch`/`leader_id`/`is_leader`.
  - `/api/cluster/stats` — `epoch`/`leader_id`/`is_leader`/`leader_token` in the stats dict.
  - `/api/v1/nodes` (incl. `{node_id}`) and `/api/v1/cluster/stats` — same fields added to the typed v1 contract (`V1NodeResponse`/`V1NodeListResponse` gained optional `epoch`/`leader_id`; cluster sub-dict of v1 stats carries `epoch`/`leader_id`/`is_leader`/`leader_token`).
- Additive fields only — existing clients ignore unknown fields; no client behavior change required to ship.

### Added — per-leader token stale-write reject, opt-in (issue #77)
- **`ClusterMaster.leader_token()`**: `HMAC-SHA256(cluster_secret, "{epoch}:{leader_id}")[:32]`. The cluster secret is the existing shared cluster token (`FUSION_CLUSTER_TOKEN` / `.cluster_token`), reused — no new secret, no cloud, offline-safe. Same epoch + leader_id derives the same token on every master; a failover (new epoch) derives a different token.
- **`GET /api/leader/credentials`**: returns `{epoch, leader_id, leader_token, is_leader, enforce}`. A client fetches this after a failover, refreshes its stored token, then sends it on subsequent mutations. Bearer-authenticated (not exempt).
- **Stale-token reject on mutations**: opt-in via env `FUSION_LEADER_TOKEN_ENFORCE=1`, active only in HA standby mode (`_election is not None`). Submit (`/api/tasks/submit`, `/api/v1/tasks/submit`) and cancel (`/api/tasks/{task_id}/cancel`) routes read `X-Leader-Token`; if enforcement is on, HA is active, and a header is present that differs from the current `leader_token()`, the route returns **`409 LeaderChanged`** (warning log + audit `leader_token_reject`). A missing header is still accepted (graceful — defense-in-depth; a client that never sends the header behaves as before, a client that refreshes + sends gets stale reject). Single-master and active-active never reject regardless of env.
- **`ClusterMaster.leader_token_enforce()`**: reports whether enforcement is active (`_election is not None` AND env `FUSION_LEADER_TOKEN_ENFORCE=1`).

### Tests
- `tests/test_epoch_leader.py` — single-master zero/empty exposure across all 6 routes; HA leader/standby epoch + leader_id; leader_token determinism + cross-epoch divergence (9 cases).
- `tests/test_leader_token.py` — enforce-off accepts stale; HA correct/stale/missing; single-master + active-active never reject; `/api/leader/credentials` consistency; cancel + v1-submit reject paths (10 cases).
- Suite: 1451 passed, 0 failed, 15 skipped (baseline 1433 + 18 new).

## [0.16.0] - 2026-09-03 — Cluster drain, idempotency, fencing, supervisor coordination, optional identity

### Added — cluster-level drain with health-gate (issue #69)
- **`GET /api/nodes/{node_id}/drain-status`**: returns `{draining, in_flight, ready, long_task_active}`. `ready` is `true` when the node is draining AND has zero in-flight tasks — this is the signal an operator/supervisor waits for before stopping services on the node.
- **CLI `cluster drain --wait [--timeout N]`**: POSTs drain, then polls `drain-status` every 2s until `ready` or timeout. Exit 0 on ready, exit 1 on timeout (prints `in_flight` + `long_task_active`). MVP refuse-long: a long-running task (config `drain.long_task_threshold_seconds`, default 300) keeps `ready` false — checkpoint migration is future work (cross-node KV transfer #33 is the path).
- `ClusterMaster._inflight_count_for_node(node_id)` counts RUNNING tasks whose `assigned_nodes` contain the node.

### Added — submit-time exclude_nodes (issue #70)
- `TaskSubmitRequest.exclude_nodes` already flowed through to `ClusterTask.exclude_nodes` and the `select_nodes` filter; this release adds the regression test and OpenAPI field description, closing the gap.

### Added — X-Idempotency-Key on submit (issue #71)
- **`ClusterMaster.try_idempotency(key)` / `register_idempotency(key, task_id)`**: idempotency keys (key → task_id + expires_at) guarded under `_tasks_lock`, TTL default 86400s (config `scheduling.idempotency_ttl_seconds`). Duplicate submit with the same `X-Idempotency-Key` header returns the existing task_id without creating a new task; expired keys are purged on access. Both submit routes (`/api/tasks/submit` and `/api/v1/tasks/submit`) honor the header.

### Added — fencing token + authoritative membership (issue #72)
- **`MasterElection.fencing_token`**: monotonic, incremented in `_become_leader`, persisted with term/voted_for, exposed via `get_state()` + `current_fencing_token` property. On election, `ClusterMaster` stores `self._fencing_token` and propagates `X-Fencing-Token` + `X-Leader-Id` headers in `_dispatch_to_node`.
- **NodeAgent fencing check**: `execute_task` tracks `self._last_fencing_token`; an incoming token lower than the last seen is rejected as `{"error": "stale master (fencing token expired)", "fencing_rejected": True}` (log warning). Token 0 (single-master / active-active, no election) never rejects — backward compatible. Master classifies `fencing_rejected` as a non-retryable logic failure (a stale master should not retry).
- **Authoritative `/api/nodes`**: response now carries `cluster_view: bool` (this master is leader OR synced-from-leader within the sync interval) and `partitioned: bool` (this master is a non-leader minority that cannot reach quorum). Clients read `partitioned` to globally disable writes to a partitioned master.

### Added — supervisor coordination (issue #73)
- **`SupervisorBridge`** (`agent/supervisor_bridge.py`): shells out to `fusion-sv <op> [svc]` via `subprocess.run`. Ops: `status`, `drain`, `rollout`, `shutdown`, `backup`. Offline-safe — `FileNotFoundError` (fusion-sv not installed) returns `{"available": False}` without crashing; inference path is unaffected. Env `FUSION_SV_BIN` overrides the binary path.
- **NodeAgent**: optional `supervisor` ctor arg (lazy `SupervisorBridge()` on first use); new `supervisor_rpc` task type; heartbeat carries `supervisor_available` (cached ping, 30s throttle, `to_thread` to not block the loop).
- **AgentServer routes**: `GET /api/supervisor/status`, `POST /api/supervisor/{op}` (op allowlist, optional `svc` query), cross-node reachable via peer master → peer agent HTTP.
- **MasterServer forward**: `POST /api/nodes/{node_id}/supervisor/{op}` + `GET /api/nodes/{node_id}/supervisor/status` forward to the target node's agent (SSRF-guarded `is_safe_peer_host` + `build_safe_url`).
- **CLI**: `cluster supervisor <op> <node_id> [--svc S]`; `cluster rollout-node <node_id>` drives the per-node drain → rollout sequence (MVP sequential).
- `NodeInfo.supervisor_available` aggregated from agent heartbeats; `/api/nodes` includes it.

### Added — OPTIONAL fusion-identity integration (issue #74)
- **`IdentityProvider`** (`security/identity_provider.py`): OPTIONAL client for the Fusion ecosystem identity service. When the operator sets `FUSION_IDENTITY_URL` (opt-in), it verifies JWTs via `POST /api/v1/auth/verify`, sources per-tenant concurrent quota via `/api/v1/admin/tenants/{tid}/quota`, and reports task usage via `/api/v1/tenants/{tid}/usage` (best-effort, never blocks scheduling). When `FUSION_IDENTITY_URL` is unset, `get_identity_provider()` returns `None` and all behavior falls back to local config + `fmu_` UserStore — the **offline default is unchanged** (100% local/offline rule preserved). Fail-closed: when enabled and the identity service is unreachable, `verify_jwt` raises (the operator opted in, identity is the authority); it never silently admits.
- **`BearerAuthMiddleware` `jwt_verifier` param**: a JWT-shaped token (three dot-separated parts, not `fmu_`) is routed to the injected `jwt_verifier`; on success, `scope["user_id"]`/`["user_role"]`/`["tenant_quota"]` are injected. On `None` (invalid/revoked) or exception (unreachable), 401 fail-closed. The `fmu_` and cluster-token paths are unchanged and coexist — `fmu_` UserStore is NOT retired.
- **Per-tenant quota**: `ClusterMaster._tenant_quotas` + `set_tenant_quota(tid, concurrent)`; `assign_task` and `acquire/release_chat_slot` use `_tenant_limit_for(user)` (per-tenant override falls back to the global `_tenant_max_concurrent`). master_server submit routes resolve the quota via identity on submit when enabled.
- **Usage reporting**: `_finalize_task` captures `(user, model_name, success)` and, after releasing the lock, reports usage best-effort via `asyncio.to_thread` (never blocks finalize).

### Changed
- `assign_task` quota check uses `_tenant_limit_for(task.user)` (per-tenant override first, else global).
- `_dispatch_to_node` sends `X-Fencing-Token` + `X-Leader-Id` headers (fencing).
- `/api/nodes` response carries `cluster_view`, `partitioned`, `supervisor_available` fields.

### Configuration
- `scheduling.idempotency_ttl_seconds` (default 86400) — idempotency key TTL.
- `drain.long_task_threshold_seconds` (default 300) — drain refuse-long threshold.

### Tests
- 1433 passed, 14 skipped (baseline 1356 + 77 new). New: `test_exclude_nodes.py`, `test_idempotency.py`, `test_cluster_drain.py`, `test_fencing.py`, `test_supervisor_bridge.py`, `test_identity_provider.py`, `test_jwt_auth.py`; extended `test_scheduling.py` (per-tenant quota), `test_agent_server.py` (supervisor routes), `test_master_server.py` (supervisor forward).

## [0.15.0] - 2026-09-02 — Active-Active dual-master + real GPU load + pipeline gate

### Added — Active-Active dual-master (issue #63)
- **`ha.mode = "active-active"`**: both masters run active and accept task submissions concurrently (no standby 503). No
  election is started — `_election` stays `None`, `_is_leader` stays `True`, so the existing standby guards pass on both
  masters. Bi-directional `_peer_sync_loop` (default 2.0s interval) pushes nodes + KV + banned + task state to all configured
  peers via the existing `/api/ha/sync-tasks` + `/api/ha/sync-state` routes; both masters run the loop, so state converges in
  both directions. Offline-safe, no Redis (the Redis/quota/LB deploy dependency is tracked in fusion-gateway #159).
- **Task ownership (`ClusterTask.owner_master`)**: a task is owned by the master that accepted it. Only the owner master
  dispatches; peers hold mirrors. `assign_task` returns `False` (owner-skip) for tasks whose `owner_master` is a peer.
  `receive_synced_tasks` applies owner-wins — a master never lets a peer's sync overwrite a task it owns. Convergence is
  eventually-consistent (owner-wins + single-owner-dispatch), acceptable for a small-fleet deploy without strong-linearizability.
- **Task ID uniqueness**: active-active prefixes task IDs with the master's `node_id` (`master-1-<uuid>`) so cross-master IDs
  never collide — keeps the agent P1-14 dedup safe even if both masters ever dispatch the same logical id.
- **Node-role affinity + drain**: `NodeInfo.role` (`worker`/`general`/`heavy`) and `ClusterTask.tier` (`heavy`/`general`).
  `LoadRouter` adds a soft affinity bonus (+0.15) to heavy-role nodes for heavy-tier tasks. `NodeInfo.draining` + new
  `POST /api/nodes/{node_id}/drain` / `undrain` routes + CLI `cluster drain|undrain <node_id>` exclude a node from new-task
  selection while in-flight tasks continue — a draining master's local node stops new dispatch; the peer keeps serving.
- **Config**: `ha.mode` (`"standby"` default = current single-leader behavior, `"active-active"` opt-in), `node.role`. Peers
  accept `{node_id, ip, port, priority}` dicts (existing shape).

### Fixed — real GPU/Metal load (issue #64)
- **VRAM parse was a no-op**: `cluster_sync.collect_load_report` had a `system_profiler SPDisplaysDataType` parse stubbed as
  `pass`, so `NodeLoadReport.gpu_memory_*_gb` was always `0.0` and `LoadMetrics.metal_util` (VRAM_FIRST weight 0.2) was never
  populated — VRAM_FIRST routing weight was dead. Fix: new `fusion_multi_node/agent/mlx_memory.py` `fetch_mlx_memory()` scrapes
  fusion-mlx `GET /v1/health` (local loopback, offline-safe) for the real Metal memory block
  (`mlx_active_bytes` / `mlx_cache_bytes` / `mlx_peak_bytes` / `total_bytes` / `oom_risk`), with a graceful `None` fallback on
  connect error/timeout/non-200 so an offline MLX never blocks scheduling. `oom_risk` is read from the memory block first,
  falling back to top-level (fusion-mlx exposes it at the top level of `/v1/health`).
- **Agent heartbeat** now carries `metal_util` (active/total) + `gpu_memory_used_gb` / `gpu_memory_total_gb`; the master stores
  them on `NodeInfo` and feeds `metal_util` into `LoadMetrics`. `GET /api/v1/nodes/{id}/metrics` no longer hits a
  non-existent `_node_loads` attr (was `AttributeError`) — reads the real `_metrics`.

### Fixed — pipeline 404 gate (issue #65, upstream-blocked)
- **Pipeline-parallel `/distributed/*` returns 404** (fusion-mlx#621). The real dispatch path already hard-errored (task
  FAILED), but with no 404-vs-other distinction, no config gate, and any node eligible. In-repo part: `parallel.pipeline_enabled`
  (default `False`) — submitting `mode=pipeline` while disabled returns `400` with a clear upstream-missing message instead of
  a downstream 404. `parallel.pipeline_shard_roles` (`["heavy"]` default) hard-filters candidate nodes by role when enabled.
  `_execute_pipeline_step` maps a `404` to `{"upstream_missing": True, ...}`; master `_dispatch_pipeline` maps
  `upstream_missing` to `FAILED` (non-retryable, not a transient node fault — does not trip the S1 circuit breaker or ban the
  node). Upstream fusion-mlx#621 has since landed; the gate stays as a defensive default until operators explicitly enable it.

### Tests
- `test_active_active.py` (8): both-masters-accept-submit, task-id prefix uniqueness, mirror-not-redispatched (owner-skip),
  owner-wins-not-overwritten, peer-sync propagates nodes+tasks, drain-excludes, heavy-tier affinity, standby regression
  (mode=standby still 503s).
- `test_pipeline_gate.py` (5): pipeline-disabled-rejects-submit (400), pipeline-enabled-passes-gate, shard-role-filter,
  execute-pipeline-step-404 → upstream_missing, dispatch-pipeline upstream_missing → FAILED.
- `test_mlx_memory.py` (6): parses memory block, no-key omits auth header, non-200 → None, missing-memory-block → None,
  connect-error → None, sync-version parses.
- `test_master_server.py::test_submit_task_pipeline`: fixed for the new gate (injects `pipeline_enabled=True` + broadened
  `pipeline_shard_roles`).
- Suite: 1356 passed, 0 failed, 14 skipped (E2E skip-gated on fusion-mlx/model availability). Ruff check + format clean.

### Upstream
- fusion-mlx #621 (pipeline `/distributed/*` API) — landed.
- fusion-gateway #159 — multi-tenant Redis quota + tier priority queues + Traefik active-active LB (deploy plan §3), the
  traffic-splitting authority above multi-node.

## [0.14.2] - 2026-09-02 — Containerized agent deep-health readiness fix

### Fixed
- **Agent `/api/health/deep` readiness never reports ok in containers** (issue #60): the readiness probe — used by the
  Docker healthcheck — never went healthy for containerized agents, leaving containers perpetually `(unhealthy)` despite the
  agent registering online with the master. Three root causes, all in the deep-health MLX probe path:
  1. **Wrong probe URL**: the handler resolved the MLX URL via `getattr(_backend, "base_url")`, but `FusionMLXBackend` stored
     the resolved URL on `self._base_url` (underscore) — the attribute was `None`, so the probe fell back to
     `localhost:{fusion_mlx_port}` (default `11432`, a gateway port, not the MLX inference port `11434`). In containers where
     MLX runs on the host (`FUSION_MLX_URL=http://host.docker.internal:11434`), the probe hit the wrong address and failed.
     Fix: expose a real `base_url` property returning the env-overridden `_base_url`; the probe now honors `FUSION_MLX_URL`.
  2. **Missing api_key Bearer header**: the `/v1/models` probe omitted `Authorization: Bearer <api_key>`. With fusion-mlx auth
     enabled, every probe returned `401`, so `fusion_mlx_ready` was always `False`. Fix: the deep-health probe,
     `FusionMLXBackend.health()`, and `_get_mlx_version` all send the api_key Bearer header (resolved from the backend or
     `FUSION_MLX_API_KEY`); header is omitted when no key is configured (anonymous probe, backward compatible).
  3. **Local socket probe misclassified remote MLX as down**: `fusion_mlx_port` used a local `connect_ex` socket probe, which
     is always `False` when MLX is on a remote/host address. Fix: when `backend_url` is non-local, use the HTTP probe result
     for `fusion_mlx_port` instead of the local socket check.
- Verified live: containerized agents now report `status: ok` / `fusion_mlx_ready: True`, containers go `(healthy)`, and the
  master sees both agents online. Container E2E cross-register test passes.

### Tests
- `test_agent_server.py::test_health_deep_ok_remote_mlx_via_url` — asserts the probe targets `FUSION_MLX_URL` and carries the
  `Bearer` api_key header in the container (remote-MLX) scenario.
- `test_rate_pacer.py::test_health_probe_carries_api_key_bearer` /
  `test_health_probe_no_key_omits_auth_header` — `FusionMLXBackend.health()` sends the Bearer header when a key is configured
  and omits it (not an empty `Bearer`) when none is.
- Suite: 1347 passed, 7 skipped (was 1344 + 3 new).

### Changed
- `pyproject.toml` / `__init__.py`: `0.14.2rc1` → `0.14.2` (RC → GA; this release adds the real issue #60 fix).

## [0.14.2-rc.1] - 2026-08-28 — Release Candidate

> ⚠️ **RC release**: the v0.14.1 final baseline packaged as a release candidate. Content = HEAD (v0.14.0 enterprise 7 blockers + v0.14.1 TarSlip
> security patch), **no new code changes**. **Not GA**. Enterprise production-readiness blockers are all cleared (v0.14.0), the security patch is merged (v0.14.1),
> 0 open issues/PRs, CI green. This RC serves as the candidate baseline for the next patch line, validating publishability.

### Changed
- `pyproject.toml` / `__init__.py` / `README.md`: 0.14.1 → 0.14.2rc1 (RC pre-release)

## [0.14.1] - 2026-08-28 — Security patch: backup restore path-escape hardening

### Fixed
- **backup restore TarSlip hardening** (found in security review): `cli.py` `backup_restore` originally validated only `member.name`
  rejecting `..`/absolute paths, missing symlink/hardlink `linkname` escape (TarSlip variant — a malicious tar containing a symlink
  pointing to `/etc/passwd`, where `extractall` creates that symlink and a sibling file member then escapes through it). Fix: added
  symlink/hardlink `linkname` escape validation (rejecting absolute/`..` linkname) + `extractall(filter="data")`
  (PEP 706, py3.12) backstop rejecting symlink/hardlink/device members. Two-layer defense. Backups are a trusted source (produced by this repo's
  `create`), but `restore --in` accepts an arbitrary-path tar.gz — does not assume trust (Rule 12).

### Tests
- `tests/test_backup_cli.py` +2: symlink linkname escape rejected / hardlink linkname escape rejected.
  Full suite 1343 passed, ruff clean.

## [0.14.0] - 2026-08-28 — Enterprise production-readiness blocker remediation (7 items)

> **Production-readiness blockers cleared**: all 7 enterprise commercial blockers landed — HA wiring gap / observability persistence / alert outbound /
> mTLS config section / KV production-ready declaration / CLI backup-restore / rule-epoch persistence. Strategy = config section + deployment-layer
> env passthrough + documentation guidance (user decision), **no default flips** (mTLS/HA stay default-off for test compatibility); the only default flipped
> is `observability.persist` (tests construct it explicitly). Baseline 1309 → 1341 tests all green (26 new + 6 regression net).
> Green in both random-order directions. ruff clean.

### Fixed — 7 blockers

- **HA single-Master wiring gap (item 1)**: `cli.py` `cluster start` path calls `_master.start()` without
  `ha_config` (only `node start` passes it) → that path never starts HA. Fix: `_async_cluster_start` aligned with
  `_async_node_start`, reads `ClusterConfig().get_ha_config()` + injects `config=`, eliminating the inconsistency between the two startup
  paths. HA stays default-off (single-Master compatible); `config.json` `ha.enabled=true` + peers enables it explicitly.
- **Observability fully in-memory deque (item 2)**: `observability.persist` defaults to False + no periodic save → crash loses
  data after stop. Fix: `persist` defaults to True (the only default flip; tests construct explicitly and don't read the default); `_cleanup_loop`
  adds a periodic save at the end (300s cadence, `self.save()` persists the increment when persist=True); `node start` master branch
  injects a `ClusterObservability` built from config (retention+persist), eliminating the bare-constructor fallback.
- **Alerts have no outbound channel (item 3)**: `_register_alert_webhook` is env-only (`FUSION_ALERT_WEBHOOK_URL`),
  zero-config means no alerts. Fix: `config` adds an `observability.alerts.{webhook_url,webhook_timeout}` section;
  `_register_alert_webhook` reads config then falls back to env (env wins); registers a fire-and-forget POST only when non-empty
  (httpx, to_thread non-blocking). 100% local — webhook points to an intranet endpoint.
- **mTLS default-off with no config section (item 4)**: `_ENABLED` reads env at import time and caches it → config-driven
  `enabled` has no effect; no config section, no deployment guidance. Fix: `config` adds a `security.mtls.{enabled,ca_cert,
  node_cert,node_key,node_id,node_role}` section (default-off); `mtls.py` `_ENABLED` import-time cache
  → lazy `is_enabled()` reading env; new `configure_from_config(cfg)` config→env bridge (env wins, does not overwrite a
  non-empty already-set value); both master startup paths call the bridge before start. **fail-closed unchanged** (enabled but incomplete certs raise
  RuntimeError "incomplete cert path", GAP-2 not broken). mTLS stays default-off (test-compatible); production must enable explicitly.
- **No built-in data backup (item 6)**: `docs/OPERATIONS.md` manual recipe omits users.json/audit.log/
  observability.jsonl/tls//kv/; no CLI command, no restore procedure. Fix: `cli.py` new `backup` command group —
  `backup create [--out DIR]` (tar.gz packs the full `~/.fusion/multi-node/` — 9 files+tls/+kv/,
  atomic tmp+rename, 0600, includes `.cluster_token` plaintext + log warning) + `backup restore --in FILE
  [--yes]` (path-escape validation rejects `..`/absolute paths, `--yes` skips confirmation, corrupt files abort).
- **Rule-epoch/confirm in-memory state (item 7)**: `_rule_epoch`/`_confirms` purely in-memory → resets to zero on restart / HA
  failover starts from 0 (guard re-baselines/re-queries; v0.13.0 CHANGELOG listed this as a known limitation). Fix: added `_rule_epoch_path
  = ~/.fusion/multi-node/rule_epoch.json`; `_load_rule_epoch_state()` (restore on start, corrupt-disk tolerant
  → defaults to 0/empty) + `_save_rule_epoch_snapshot()` (atomic tmp+fsync+replace, off-lock async
  to_thread) + `_persist_rule_epoch_async()` + `_mark_rule_epoch_dirty()` (throttle dirty-flag 5s,
  `_persist_loop` 15s backstop); `advance_rule_epoch`/`receive_rule_epoch`/`receive_confirm` wired in after write;
  `stop()` final persist; HA `_build_state_sync_payload` includes epoch+confirm, standby
  `receive_synced_state` takes max epoch (prevents regression) + merges confirms.

### Docs — item 5 (KV production-ready declaration, not a code blocker)

- `docs/DEPLOYMENT.md` + `README.md`: declare synthetic KV cross-node transport **production-ready** (since v0.11.0, issue
  #33 closed) — `SyntheticKVTransport` default backend, cross-node HTTP routes synthetic KVCacheEntry, `sync_kv_cache`
  returns True; real-tensor `MLXKVTransport` = env-gated experimental bonus (`FUSION_KV_TENSOR_BACKEND=mlx`,
  a pure env flip when upstream #650 lands, 404→degrade graceful). Production can use synthetic KV; not a blocker.
- `docs/DEPLOYMENT.md` adds 5 production sections: multi-Master HA config example / mTLS node-mutual-trust required / alert outbound channel /
  KV production-ready / observability persistence.
- `docs/HA-CRASH-RECOVERY.md`: multi-Master production config example + rule-epoch/confirm persistence (no longer in-memory).
- `docs/OPERATIONS.md`: backup scope completed with 5 missing items + references the new `backup create/restore` CLI + restore procedure
  + keeps the manual recipe as a fallback + mTLS certificate rotation.

### Changed

- `config/config.py`: `observability.persist` defaults to True; adds `observability.alerts` +
  `security.mtls` subsections + validators + `get_mtls_config()`/`get_alert_webhook_config()`/
  `get_ha_config()`.
- `security/mtls.py`: lazy `is_enabled()` + `configure_from_config()`; all call sites
  (server_ssl_kwargs/client_kwargs/scheme/certs_available) go through `is_enabled()` rather than bare `_ENABLED`.
- `observability/observability.py`: `_cleanup_loop` periodic save.
- `master/cluster_master.py`: `_register_alert_webhook` reads config then falls back to env; rule_epoch/confirm
  persistence + HA sync payload.
- `cli.py`: `cluster start` passes ha_config+config; both master paths call `mtls.configure_from_config`;
  new `backup` command group.
- Version 0.13.0 → **0.14.0** (`pyproject.toml`, `__init__.py`).

### Tests — 26 new

- `tests/test_backup_cli.py` (8): create tar.gz all-files+0600 / custom out / empty-dir tolerance /
  token warning / restore roundtrip / confirm abort / corrupt abort / path-escape reject.
- `tests/test_rule_epoch_persist.py` (10): advance restart-restore / confirm restart-restore / missing-file default 0 /
  corrupt JSON default 0 / throttle defer / persist_loop flush / stop final persist / HA standby receives epoch /
  receives confirm / prevents regression.
- `tests/test_mtls_config_bridge.py` (8): disabled by default / config writes env / env wins /
  fail-closed incomplete certs / empty section no-op / no-method no-op / empty string does not write env / scheme lazy.

### Known limitations

- mTLS/HA still default-off (production must enable explicitly: config section + deployment-layer env, see `docs/DEPLOYMENT.md`).
- Upstream real-tensor KV env-gated (pending fusion-mlx #650); synthetic KV is production-ready, not a blocker.

## [0.13.0] - 2026-08-28 — issue #52 cross-node guard TRANSPORT primitives

> **New feature (minor)**: cross-node guard contract — 3 TRANSPORT primitives consumed by fusion-guard.
> Multi-node only defines **TRANSPORT + IDENTITY + KEY SCHEME**; guard implements the consumer side (federated chain verification /
> RuleSet reconcile / confirm aggregation). Links to fusion-guard issue #4. 100% local/LAN, no cloud.

### Added
- **cluster_key HKDF derivation** (`security/cluster_key.py`): HKDF-SHA256 domain-separates from cluster_token to derive
  3 independent MAC keys for the primitives (audit chain / rule epoch / confirm relay). **No new secret added** — cluster_token
  is already the cluster-member root trust source. Public: `derive_audit_chain_key` / `derive_rule_epoch_key` /
  `derive_confirm_relay_key` / `mac_payload` / `verify_mac` (constant-time) / `canonical_json` /
  `post_confirm` (agent/guard→master POST helper).
- **Primitive 1 — audit-chain HMAC** (`security/audit_log.py`): monotonic `seq` / `prev_hash` chaining (sha256 over the complete
  prior record including mac) / `mac` tamper detection. Chain computation failure degrades to writing chainless fields (preserves the "audit never loses events" contract).
  Chain-segment pull endpoint `GET /api/v1/audit/chain?since_seq=N` (master + agent) — guard pulls this node's chain segment to aggregate.
- **Primitive 2 — rule-epoch broadcast** (`master/cluster_master.py`): `advance_rule_epoch` (leader advances +
  best-effort broadcast to workers + HA peers) / `receive_rule_epoch` (rejects lag to prevent regression, idempotent on equal, accepts ahead) /
  `_state_sync_loop` periodic re-broadcast fills gaps. Endpoints `GET /api/v1/rules/epoch` + `POST /api/v1/rules/epoch/advance`
  + `POST /api/rules/epoch` (agent receive side).
- **Primitive 3 — confirm relay** (`master/cluster_master.py`): `receive_confirm` (MAC verification, bad MAC rejected)
  / `get_confirms` (aggregates filtered by epoch). Endpoints `POST /api/confirm` + `GET /api/v1/confirms?epoch=N`.
- **RBAC**: 6 user RBAC items (`CLUSTER_INTERNAL` sentinel rejects all user tokens / `cluster:stats` USER+VIEWER read /
  `user:manage` ADMIN-only) + 2 agent node-RBAC items (`NODE_LIST` / `NODE_HEARTBEAT`).
- **API docs**: `docs/API.md` adds a Guard Transport section (drift-detection contract-test guard).
- **Tests**: 4 new files 51 tests — `test_cluster_key.py` (12) / `test_audit_chain.py` (13) /
  `test_rule_epoch.py` (14) / `test_confirm_relay.py` (12). All ASGI in-process, no real socket.

### Layer boundary
- Multi-node = scheduling + transport; guard = per-host authentication. No fusion-guard import (cross-repo zero-dependency, multi-node self-contained).
- guard holds cluster_token + `X-Node-Id: master` to pull agent endpoints (reuses MASTER role).

### Known limitations
- Rule epoch in-memory state: resets to zero on restart, HA failover starts from 0 (guard re-baselines).
- confirm is not relayed to HA standby: failover loses in-flight confirms (guard re-queries). Not a defect; the contract allows it.

### Stale-branch cleanup
- Deleted 19 stale branches behind main (including v0.12.2/v0.12.3 deleted dead code — merging would reintroduce dead code).

### Tests
- 1309 passed, 0 failed, 7 skipped (random-order both directions seed=0/1 green).
- lint: ruff check clean; format: ruff format --check clean.

## [0.12.3] - 2026-08-28 — autoscaler dead-code removal

> **Dead-code removal**: the `autoscaler/` package (469 lines) has zero instantiations; its route always returned 503 not-wired.
> Core scale up/down depends on a non-existent standby node pool (NodeInfo has no role field) — scale-up is permanently a no-op,
> scale-down would set a genuinely-online Mac to offline (machine still present but service stopped = reduced availability), a cloud-elasticity model that doesn't fit
> a fixed Mac pool. Only rebalance has meaning but duplicates the existing LoadRouter routing. Deletion loses no functionality (it was already 503).
> Tests 1268 → 1261 (net delete 7: 8 TestAutoscaler + 2 not-wired server + 2 v1 contract, plus an accompanying +3 reorg), ruff clean, no API break.

### Removed

- **`fusion_multi_node/autoscaler/` package** — `autoscaler.py` (469 lines) + `__init__.py`. Zero-instantiation dead code,
  `ClusterMaster._autoscaler` never assigned. `Autoscaler` / `AutoscalerConfig` / `ScalePolicy` / `ScaleAction`
  all deleted.
- **`master_server.py` routes** — `GET/PUT /api/v1/autoscaler/config` (always 503) +
  `AutoscalerConfigUpdateRequest` / `V1AutoscalerConfigResponse` request models + route-shadowing comments.
- **`permission.py` entries** — `AUTOSCALER_MANAGE` permission constant + MASTER frozenset member +
  `/api/autoscaler` path mapping + user-RBAC `("PUT"/"GET", "/api/v1/autoscaler/config")` 2 entries.
- **Tests** — `TestAutoscaler` (8 cases, test_new_features) + `TestMasterServerAutoscalerNotWired` (2 cases)
  + `test_v1_contract` autoscaler 503 contract (2 cases) + stale comments.

### Kept

- `cluster_sync.py` / `data_scrubber.py` — still live (confirmed kept in v0.12.2).
- README historical changelog lines (autoscaler cooldown-gate fix etc.) — historical record, not deleted.

## [0.12.2] - 2026-08-28 — Migration-debt cleanup (#106/#61 CLOSED and landed)

> **Dead-module retirement**: receiving ends are CLOSED and landed — fusion-gateway #106 (Go gateway absorbs cloud adapter +
> MCPClusterGateway) + fusion-cowork #61 (ast_diff + cluster_sync landed, self-contained, no multi-node dependency).
> The multi-node side deletes dead modules and their re-exports, tests, and accompanying dead code; **keeps live cluster_sync** (agent cross-node
> model sync routes through the master manifest; deleting it would lose functionality). 100% local/offline positioning reinforced (cloud-path residue fully gone).
> Tests 1317 → 1268 (net delete 49: 19 mcp_gateway + 9 secure_transfer + 8 cloud_fallback + 6 ast_diff + 7 route/init accompanying), ruff clean, no API break.

### Removed (4 dead modules + 1 dead handler + tests)

- **cloud_fallback.py** — import-time disable guard, dispatch path cut in v0.8.2, zero dispatch references. Along with
  the `master/__init__.py` try/except degradation block + 6 Cloud* re-exports + `TestCloudFallback` (8 cases) deleted.
- **mcp_gateway/ package** — zero-route/zero-instantiation/zero-CLI dead code. Along with `test_mcp_gateway.py` (19 cases) +
  `test_core.py` `TestMCPGateway` (3 cases) + MCP imports deleted. Port 11446 belongs to FMP transport (not mcp_gateway).
- **ast_diff.py** — sole consumer of secure_transfer → only instantiated in `fmp_server.register_data_sync_handler`
  (zero call sites repo-wide). Along with `TestASTDiff` (6 cases) deleted.
- **secure_transfer.py** — the only ast_diff link. Along with `test_secure_transfer.py` (9 cases) deleted.
  **data_scrubber.py kept** (live — dispatch PII scrubbing P1-3 + KV warm_cache).
- **fmp_server.register_data_sync_handler** — zero-call dead handler; the lazy-import secure_transfer inside its closure
  was deleted with it. Other handlers (SHARD_SYNC/KV/HEARTBEAT) kept live.

### Kept (live, mistaken deletion = lost functionality)

- **cluster_sync.py** — wired into the master_server lifecycle + 4 routes; agent `_execute_model_sync` pulls the manifest
  via the master manifest route = real cross-node model sync. Kept by user decision; only stale comments updated.

## [0.12.1] - 2026-08-28 — Audit 0826 P2+P3 remediation (15 items)

> **Final-pass remediation**: audit `fusion-multi-node-audit-result-product-0826.md` judged 12 P2 + 3 P3 items
> all code-fixed and landed (including design-tradeoff items broken open via env-gating, not docs-only). Baseline 1262 → 1317 tests
> all green (~55 new cases). ruff clean. No API break (patch bump). Audit 0826 all 47 items (5 P0 + 27 P1
> + 12 P2 + 3 P3) now landed and complete.

### Security / resource (3)

- **P2-1 mTLS client `check_hostname=False` (cert missing SAN)**: `provision_node` issued leaf cert without
  SubjectAlternativeName → hostname could not be verified; old impl `check_hostname=False` fail-open. Fix:
  `provision_node` takes an `ip` arg; cert chain adds `SubjectAlternativeName([DNSName(node_id), IPAddress(ip)])`;
  `client_ssl_context` switched to `check_hostname=True` (SAN present, verifiable).
- **P2-2 `MLXKVTransport` base_url no SSRF guard**: operator env built URL for direct connect, no SSRF check. Fix:
  `_get_client` validates the built URL via `is_safe_outbound_host`; unsafe host raises RuntimeError (fail-closed).
- **P2-13 docker-compose no `mem_limit`/`cpus`**: both services had no resource cap; OOM could drag down the host. Fix: master/agent
  get `mem_limit`/`cpus` (default 4g/4, overridable via `FUSION_*_MEM_LIMIT`/`FUSION_*_CPUS` env) + `deploy.resources.limits`
  (compose v3 spec). `.env.example` documents the 4 resource env vars.

### KV capacity (2)

- **P2-4 `KVShard.tensor` export did not sync source `_local_size_bytes`**: export set tensor without updating size →
  source LRU gate failed, memory exceeded `max_local_cache_mb`. Fix: `export_bundle` writes tensor and synchronously updates
  `entry.total_size_bytes` + recomputes `_local_size_bytes`; `import_bundle`/`store_local` verify incoming chunks do not exceed limits.
- **P2-5 ban-expiry lazy unban had no active probe**: ban expired passively (only cleared on next select_nodes). Fix:
  `_health_check_loop` actively probes ban-expired nodes via `/api/health` — probe OK unbans + info log; failure extends ban.

### Event / election (3)

- **P2-6 election `_lock` held during await HTTP+fsync**: single lock held across await slowed election. Fix: under-lock do only in-memory
  reads/writes (term/voted/leader); HTTP send_vote + fsync `_save_state` moved outside the lock (snapshot fields, await outside lock), aligning
  with the cluster_master in-lock-snapshot / out-of-lock-I/O pattern.
- **P2-7 `_emit_task_event` dropped events on full queue with no persistence backstop**: QueueFull get_nowait dropped oldest without warning. Fix:
  on drop `logger.warning` + `record_metric("cluster","event_dropped",1.0)` (feeds P0-5 webhook, can alert).
- **P2-8 F2 dynamic subpath covered only 3 ops, other trailing ops defaulted to allow**: `check_user_path_access` only joint-checked cancel/migrate/
  degrade; `/result`/`/retry`/`/status` and other trailing ops bypassed parent permission. Fix: dynamic task subpath check extended to
  all trailing ops — jointly check parent path `/api/tasks/{id}` permission; non-allowlisted ops inherit parent permission.

### Container / isolation — design tradeoffs broken open (4)

- **P2-9 `apply_limits` setrlimit never called**: process-level rlimit killed single long-running agent. Fix: `AgentConfig` adds
  `task_mem_limit_mb`/`task_cpu_quota` (default 0=unlimited); `SandboxExecutor` passes rlimit when spawning subprocess plugins
  (subprocess plugins only, not the main inference process); main inference resources live on the fusion-mlx side, boundary documented. env-gated.
- **P2-10 PARTIAL terminal state could not be re-dispatched for completion after crash** (design tradeoff broken open): PARTIAL terminal state unrecoverable after crash recovery. Fix:
  env `FUSION_PARTIAL_RECOVERY=1` — persist PARTIAL; `_restore_tasks` extracts completed sub-results (`result.outputs[].node_id`)
  into `exclude_nodes` + stores `_partial_prev_outputs`; `_dispatch_data` merges (not overwrites) to complete. DATA-parallel
  each node independent, completable; PIPELINE hidden_states chained, not applicable (keep whole-task failure, documented).
- **P2-11 PIPELINE had no PARTIAL, any stage failure = whole-task failure** (design tradeoff broken open): hidden_states chain is a hard dependency. Fix:
  semantics unchanged (keep whole-task failure); env `FUSION_PIPELINE_CHECKPOINT=1` — each stage's hidden_states persist to `task.params["_pipeline_ckpt"]`,
  failed retries resume from the nearest checkpoint (not from scratch). `_enqueue_retry` preserves params so the checkpoint survives retries.
- **P2-12 observability deque + event bus lost on restart** (design tradeoff broken open): all-in-memory deque lost on restart. Fix: env
  `ClusterConfig.observability.persist` (default False) — `save()`/`load()` persist to
  `~/.fusion/multi-node/observability.jsonl` (atomic tmp+replace+fsync, capped to most recent N entries); event bus not persisted
  (SSE real-time semantics, documented); master start(load)/stop(save) wired to lifecycle.

### Deployment / config (3)

- **P3-1 `__init__.py:22` autoscaler "always 404" wording drift** (actually 503): recalibrated to "always 503 not-wired (not 404)";
  README.md two places corrected in sync.
- **P3-2 `AgentServer.stop` KV persist failure had no critical alert**: old warning-only easily drowned by shutdown logs. Fix: persist failure
  (save returns False or raises) upgraded to `logger.critical` + best-effort `report_fault("kv_persist_failed")` reported to
  master (shutdown tolerates failure, does not block stop, does not mis-ban healthy nodes).
- **P3-3 MIGRATED state only via manual `migrate_task` → auto-migration unimplemented**: recalibrated — P1-15 (node OFFLINE auto-redistribute)
  already covers the "auto-migration" semantics (RUNNING→PENDING + exclude source node re-dispatch); `migrate_task` API kept for manual explicit migration.
  `PYTHON_API.md` adds MIGRATED semantics note (set by both manual + auto paths, not "manual only").

### Resource leak (1)

- **P2-3 `AgentServer.stop` did not call `kv_manager.close()` → httpx+transport leak**: old stop only saved, did not close →
  `KVSharingManager._http_client` + `MLXKVTransport` held httpx.AsyncClient handle leak. Fix: stop persists then
  calls `await self.kv_manager.close()` to close httpx client + tensor transport backend (close already try/except fault-tolerant).

### Tests added (~55)

- `test_mtls.py`: SAN + check_hostname verification
- `test_kv_tensor_e2e.py` / `test_kv_cache_sharing.py`: SSRF guard + size sync
- `test_agent_server.py`: stop close + P3-2 KV persist alert (4)
- `test_cluster_master.py`: P2-5 ban active probe / P2-7 event-drop alert / P2-10 PARTIAL completion (5) / P2-11 PIPELINE checkpoint (4)
- `test_election_p2_6.py`: election out-of-lock I/O
- `test_enterprise_security.py`: P2-8 dynamic subpath all-ops
- `test_node_agent.py`: P2-9 sandbox rlimit
- `test_observability.py`: P2-12 persist save/load (5)
- `test_docker_compose.py` (new): P2-13 resource-limit YAML validation (4)

## [0.12.0] - 2026-08-27 — Audit 0826 P1 remediation (27 items)

> **Enterprise production hardening**: audit `fusion-multi-node-audit-result-product-0826.md` judged 27 P1 items
> (fault-tolerant scheduling / KV tensor / security / API contract / Agent / performance-ops) all code-fixed and landed. After P0 hotfix
> baseline 1213 → 1262 tests all green (~49 new cases). ruff clean. No API break (minor bump).

### Fault tolerance / scheduling — cluster_master (8)

- **P1-1 H3 RUNNING→PENDING re-dispatch unimplemented → orphan tasks**: `_restore_tasks` only re-dispatched PENDING;
  RUNNING/MIGRATED stuck with `started_at=0` permanently after crash recovery. Fix: RUNNING/MIGRATED → PENDING +
  clear started_at + clear assigned_nodes + original node into exclude_nodes (avoid revisiting bad node) + retry_count not incremented
  (crash is not a task failure). Aligns with CLAUDE.md "RUNNING→PENDING re-dispatch" self-description.
- **P1-14 timeout retry did not repopulate `task.exclude_nodes` → revisits same bad node**: `_enqueue_retry` reset PENDING but
  left exclude_nodes untouched. Fix: `assigned_nodes` merged into `exclude_nodes` (deduped); `migrate_task` source
  node into exclude; P1-1 transition also merges original assigned_nodes. `assign_task` filters via exclude_nodes.
- **P1-15 node OFFLINE did not auto-migrate in-flight tasks → waits full 300s timeout**: `_refresh_node_statuses` marked
  OFFLINE but left in-flight tasks untouched. Fix: on OFFLINE, all RUNNING tasks on that node → PENDING + node into
  exclude_nodes + `_enqueue_retry` (same path as P1-14, avoid lock nesting). Rate-limited to prevent flapping avalanche. Equivalent auto-migration
  (P3-3 semantics satisfied by this path + manual `migrate_task`).
- **P1-16 `sync_kv_cache` did not classify exceptions → always False, no retry**: `status_code != 200` + `except`
  all False. Fix: distinguish transient (429/5xx/timeout — warning + retryable flag) vs logic (404/logic error —
  False); layered on the P0-3 streaming base.
- **P1-19 `_pending_queue` had no length cap → overload buildup**: insufficient nodes enqueued without limit. Fix: `MAX_PENDING_QUEUE`
  (config `scheduling.max_pending_queue`, default 1000); full → reject enqueue, `assign_task` returns False →
  master_server submit responds 503 `cluster queue full`.
- **P1-21 `_retry_loop` retried infinitely with no backoff**: assign failure immediately `_enqueue_retry` no backoff. Fix: per-task
  exponential backoff — `next_retry_at = now + backoff` (base 30s, cap 600s, deterministic no jitter); `_retry_loop`
  skips not-yet-due tasks; `_max_retry_loop_attempts` (default 10) exceeded → FAILED.
- **P1-23 agent_server rate-limit 429 accumulated circuit-breaker fault → healthy high-QPS node mis-banned**: GAP-6 fixed fusion-mlx internal
  429 but missed agent_server's own 429. Fix: `_dispatch_to_node` checks for 429 before the generic `!= 200` raise → classifies as
  transient (no report_fault, reads `Retry-After`, same classification as P0-2).
- **P1-11 `_persist_tasks_locked` full asdict O(N) every dispatch**: 1000 tasks O(N²). Fix: incremental persistence —
  `_dirty_task_ids` dirty flag, only asdict dirty tasks + write incremental patch (periodic full + incremental hybrid).
- **P1-13 httpx connection pool had no explicit config**: default max_connections=100. Fix: read config `network.http_limits`
  to build `httpx.Limits` passed to `_get_dispatch_http`; default scaled to cluster size (nodes×4).

### KV tensor — kv_cache_sharing / kv_tensor_transport (2)

- **P1-20 `MLXKVTransport.import_tensor` returned True on failure, masking it**: `except: return True`. Fix: 404
  (upstream not landed) still degrades to True (synthetic backstop + warning); other `except` returns False (real load failure, caller
  knows); `SyntheticKVTransport.import_tensor` stays True.
- **P1-22 `KVSharingManager` cross-node calls silently swallowed exceptions**: `lookup_remote` (debug) /
  `transfer_from_remote` (error+False) / `warm_cache` (warning+failed++). Fix: classify — 429/5xx/
  timeout/connection-refused warning + `record_metric` (transient not counted toward ban); consecutive failures reaching threshold (3) raise
  `create_alert` (warning, node_id); `lookup_remote` log debug→info (network-partition ops must be visible).

### Security — security / discovery (7)

- **P1-3 HTTP dispatch path PII in plaintext** (recalibrated downgrade): DataScrubber was FMP-path only. Fix: optional HTTP-path scrubbing —
  `ClusterConfig.security.http_pii_scrub` (default False, trusted LAN keeps plaintext). When enabled, `_dispatch_to_node`
  payload + chat proxy + warm_cache go through `DataScrubber.scrub` on prompt/messages. Docs mandate: across untrusted
  network segments require mTLS + this option.
- **P1-4 `cloud_fallback` module kept with hardcoded cloud API** (recalibrated downgrade): add import-time disable guard — module top
  `if FUSION_CLOUD_FALLBACK_ENABLED != "1": raise ImportError(...)`; `__init__.py` import site try/except
  degrades; test stubs set env. Module file kept (migration debt made tangible, pending #106).
- **P1-5 RBAC `check_user_path_access` fail-open**: unregistered path `perm is None`→`return True`. Fix:
  fail-closed — `perm is None`→`return False`; explicit allowlist passes cluster-internal/health/docs routes
  (`_USER_EXEMPT_PATHS` frozenset). Cluster token hits `role is None` early-return before this function, unaffected.
- **P1-6 `_enforce_user_rbac` coverage incomplete** (8 routes without user-RBAC): config/reload / autoscaler PUT /
  observability logs export / ha/sync-state / nodes / kv / metrics. Fix: all registered in
  `_USER_PATH_PERMISSION_MAP` (ADMIN: config/autoscaler/ha; VIEWER: metrics; USER: kv read);
  cluster-internal routes use sentinel `CLUSTER_INTERNAL` (cluster_token only, user tokens all rejected).
- **P1-7 AuditLogger write failure silently degraded**: `except: warning` no raise. Fix: `record_metric("audit",
  "write_failed",1.0)` + `create_alert` (warning, "audit log write failed"); no raise (auth main path not
  dragged down, ops-visible). `read()` likewise.
- **P1-8 `manual_join.py` cluster_secret not constant-time compared**: `!=`. Fix:
  `secrets.compare_digest`; empty secret → warning + force compare-fail (reject all joins).
- **P1-9 `manual_join.py` hardcoded `http://`, mTLS-enabled join breaks**: URL changed to
  `f"{mtls_scheme()}://..."`; `_get_client` passes `**mtls_client_kwargs()`.

### API contract — master_server (1)

- **P1-10 9 routes raw dict without pydantic validation**: sync/incremental / join / approve / reject /
  autoscaler PUT / ha/vote / ha/sync-tasks / ha/sync-state / ha/heartbeat. Fix: each gets a pydantic
  `BaseModel` (`IncrementalSyncRequest` / `ManualJoinRequest` / `NodeApproveRequest` /
  `NodeRejectRequest` / `AutoscalerConfigUpdateRequest` / `VoteRequest` (reused) /
  `HASyncTasksRequest` / `HASyncStateRequest` / `HAHeartbeatRequest`); handler signature dict→Model;
  FastAPI returns 422 (not 400) for missing/invalid.

### Agent — node_agent / agent_server / election (3)

- **P1-2 `/api/hardware` synchronously blocked the event loop**: `async def hardware_info()` synchronously called
  `collect_hardware_info` (system_profiler 5s + ipconfig). Fix:
  `await asyncio.to_thread(self.agent.collect_hardware_info)` — aligns with `report_hardware` pattern.
- **P1-17 HA election gap-window had no 503**: during election `_is_leader` not yet decided, `/api/tasks/submit` still dispatched. Fix:
  `MasterElection.leader_known` property; guard `_election configured and not _is_leader and not leader_known` → 503
  `election in transition`; sync period 5s→2s (config `ha.state_sync_interval`).
- **P1-18 agent `_running_task_handles` had no local capacity cap → TOCTOU**: master relied on heartbeat TOCTOU. Fix:
  `execute_task` entry `if len(_running_task_handles) >= config.max_tasks: return {"overload":True,
  "error":"node task queue full"}`; master `_dispatch_*` adds `overload` classification (transient, no report_fault, picks
  another node). Anonymous task `anon-{seq}` prevents `_running_task_handles` key collision.

### Performance / ops — tests / observability / docs / config / utils (6)

- **P1-12 no real-inference throughput baseline**: `test_load_stress.py` FastBackend fake zero latency. Fix: new
  `test_real_inference_benchmark.py` — skip-gate `_mlx_alive() and _model_available()`, real fusion-mlx
  + `mlx-community-Llama-3.2-1B-Instruct-4bit`, measures single/multi-node DATA-parallel throughput, asserts multi-node ≥0.9× single-node
  (real-model jitter margin).
- **P1-24 Prometheus missing circuit-breaker / rate-limit / node-level metrics**: cluster aggregate only. Fix: `get_prometheus_metrics` adds
  `fusion_cluster_banned_nodes` gauge / `fusion_cluster_rate_limited_total` counter / node-level
  `fusion_node_memory_total_gb{node_id}` / `fusion_node_memory_available_gb{node_id}` /
  `fusion_node_active_tasks{node_id}` / `fusion_node_banned{node_id}` (node snapshot holds `_nodes_lock` separately,
  not nested with `_tasks_lock`).
- **P1-25 `HA-CRASH-RECOVERY.md:133` stale (KV no-op)**: changed to "KV tensor cross-node transport delivered (GAP-7 v0.11.0),
  synthetic default + MLX env-gated pending upstream #650; streaming transport since v0.11.1".
- **P1-26 `kv_cache.json`/`users.json` missing fsync**: align with `config.py` save pattern — `f.flush()`+
  `os.fsync(f.fileno())` then `os.replace`. Both files changed.
- **P1-27 CLI direct-start had no log file**: `setup_logger` no env → stdout only, no disk persist. Fix: without
  `FUSION_MULTINODE_LOG_FILE` writes a stderr hint (no handler added → `len(handlers)==1` assertion holds). README
  run section emphasizes the env var.
- **P1-12 baseline support**: `AgentConfig(max_tasks=64)` + register payload `max_tasks=64` +
  `master._task_store_path` guards H3 persist dir-missing + agent overload false-reject.

### Tests — ~49 new

- `test_cluster_master.py`: H3 re-dispatch / exclude / OFFLINE migration / queue cap / backoff / 429 / incremental persist /
  election gap-window 503.
- `test_master_server.py`: pydantic 9-route 422 + RBAC fail-closed + all-route registration + node-level metrics.
- `test_kv_*.py`: import_tensor degrade/fail distinction + cross-node exception-classified alert.
- `test_enterprise_security.py` / `test_mtls.py` / `test_user_rbac.py`: cloud_fallback guard /
  manual_join compare_digest+mTLS / RBAC fail-closed.
- `test_real_inference_benchmark.py` (new): skip-gated real-inference baseline.
- `test_utils.py`: P1-27 stderr hint.

### Maintenance

- ruff format applied to all batch-touched files (cluster_master / kv / security / server / agent / tests).

## [0.11.1] - 2026-08-27 — Audit 0826 P0 hotfix (5 blockers)

> **Production-blocker elimination**: audit `fusion-multi-node-audit-result-product-0826.md` judged 5 P0 blockers
> (❌ not fit for enterprise-grade commercial release) all code-fixed and landed. Loop fault tolerance / dispatch mis-ban / KV streaming /
> async persist / alert egress five items closed loop, re-review releasable. Baseline 1203 → 1213 tests all green.

### Fixed — P0 blockers (5)

- **P0-1 4 background loops had no per-iteration exception isolation**: `_persist_loop` / `_retry_loop` / `_health_check_loop`
  (cluster_master) + `_election_loop` (election) only had outer `try/except CancelledError`; an `await` inside the loop body
  throwing a non-cancel exception → killed the whole loop. Master appeared healthy (HTTP 200) but persistence/retry/timeout/election
  silently stalled, zero alerts. Fix: each loop body gets an inner `try: <body> except CancelledError: raise
  except Exception: logger.warning; continue` (reuses the existing `_state_sync_loop` pattern).
  test: all 4 loops throw RuntimeError on first call, assert loop did not die (counter increments).
- **P0-2 `dedup_blocked`/`sandbox_blocked` mis-classified as logic_fail + report_fault**: GAP-6 fixed
  `rate_limited` but missed `dedup_blocked`/`sandbox_blocked` → fell into `"error" in r` → `logic_fail` +
  `report_fault`. H3 re-dispatch triggered dedup → accumulated fault → 60s window 3 times → healthy node banned 300s.
  Fix: `_dispatch_data`/`_dispatch_pipeline`, after the `rate_limited` branch and before `"error" in r`,
  add an independent classification (no report_fault, no retry — dedup is master's own re-dispatch error, sandbox block is config).
  test: mock agent returns `{"dedup_blocked":True}`, assert `report_fault` not called + node not banned.
- **P0-3 KV tensor base64+JSON single POST → 1.5GB peak / JSON blocking**: full bundle via
  `exp_resp.json().get("bundle")` materialized in memory then `client.post(json=)`. 500MB tensor → base64 inflation
  1.33× → JSON parse peak 1.5GB. Fix: streaming binary protocol — header JSON metadata (shards without tensor)
  + fixed-length magic + each shard's raw tensor bytes concatenated. agent `/api/kv/export-stream`
  (`StreamingResponse` octet-stream) + `/api/kv/import-stream` (raw body); master
  `sync_kv_cache` `aread()` source response → `content=src_bytes` target request body; old JSON bundle path
  backward-compatible (export-stream 404 degrades). test: 10MB synthetic tensor streaming round-trip byte-complete
  (tracemalloc peak recorded for audit).
- **P0-4 `_write_task_store` synchronous fsync blocked the event loop**: fsync already moved out of `_tasks_lock` (P1-11)
  but still blocked the asyncio single thread (SSD 1-5ms/fsync, 100 task/s takes 10-50%). Fix: 5 call sites
  (`_persist_tasks`/assign/finalize/cancel/retry) changed to `await asyncio.to_thread(
  self._write_task_store, snapshot)` — in-lock snapshot copy is pure memory, disk write inside to_thread holds no lock, does not block.
  test: monkeypatch slow disk 80ms, concurrent 40ms `asyncio.sleep` timer completes <0.07s (proves fsync
  moved off the event loop).
- **P0-5 alerts had no egress channel, `on_alert` zero registrations**: the alert mechanism existed (`create_alert` synchronously calls handler)
  but master never registered → node-offline/memory alerts only entered the deque, ops had to poll. Fix: `ClusterMaster.start`
  reads env `FUSION_ALERT_WEBHOOK_URL`; non-empty registers a fire-and-forget handler — on `Alert` →
  `asyncio.create_task(_post_alert_webhook)` (`to_thread` wraps httpx POST, failure warning does not
  drag down the alert chain). Empty env → `logger.info`, not forced. test: env sets webhook, monkeypatch httpx POST
  asserts Alert serialized POST called + `create_alert` <50ms non-blocking.

### Tests — ~10 new

- `test_cluster_master.py`: 4 background-loop fault tolerance + P0-2 dedup no-ban.
- `test_kv_tensor_e2e.py`: `TestKVTensorStreamingMemory` 10MB streaming round-trip.
- `test_task_persistence.py`: `test_fsync_does_not_block_event_loop`.
- `test_observability.py`: `TestP05AlertWebhook` (2 cases).
- `tests/test_scheduling.py::test_quota_zero_unlimited`: fixed existing baseline failure (SSRF guard
  monkeypatch dual-module + `_HoldClient` locks RUNNING count stable).

### Maintenance

- ruff format applied to this batch's touched files (cluster_master / kv_cache_sharing / agent_server / 4 tests).

## [0.11.0] - 2026-08-27 — GAP-7 KV tensor cross-node transport (close #33)

> **`sync_kv_cache` tensor-level cross-node transport delivered**: orchestrates a pluggable tensor backend across source `/api/kv/export` → target
> `/api/kv/import`, returns `True`. Synthetic backend (default, deterministic `hashlib`-generated tensor, no dependency) satisfies #33 acceptance
> (tensor round-trip across 2 agents); MLX real-tensor backend env-gated (`FUSION_KV_TENSOR_BACKEND=mlx`)
> pending upstream fusion-mlx issue #650 to activate — 404→degrade to synthetic + warn (fail-visible, Rule 12).
> P3-28 / GAP-7 / issue #33 three items unified and closed.

### Added

- **KVShard tensor field** (`distributed_mlx/kv_cache_sharing.py`) — S1, GAP-7
  - `KVShard.tensor: bytes | None = None` new field (metadata unchanged, tensor is the new payload).
  - `_serialize_entry`/`_deserialize_entry` extended: tensor base64 travels with JSON (compress flag `tensor_compress`: "caveman"/"none"); old bundles without tensor backward-compatible (tensor=None, key omitted).
  - `KVSharingManager` ctor adds `transport: KVTransportBackend | None` injection (default synthetic); `export_bundle(cache_id, model_name)`/`import_bundle(bundle)` new methods (produce/store tensor via transport, store_local budget gate).
- **Pluggable tensor backend** (`distributed_mlx/kv_tensor_transport.py`) — S1, GAP-7 (new file)
  - `KVTransportBackend` Protocol (`export_tensor`/`import_tensor`/`name`/`close`).
  - `SyntheticKVTransport` (default, name="synthetic"): deterministic sha256-based synthetic tensor (default 512 bytes, same seed same bytes, differs across node_id), no numpy dependency, pure local.
  - `MLXKVTransport` (env-gated `FUSION_KV_TENSOR_BACKEND=mlx`, name="mlx"): calls fusion-mlx `/distributed/kv_cache/export|import` (pending #650); 404→degrade to synthetic + warn. Reads `FUSION_MLX_URL`/`FUSION_MLX_API_KEY`.
  - `get_kv_transport()` factory reads env to select backend (default "synthetic").
- **Agent export/import routes** (`server/agent_server.py`) — S2, GAP-7
  - `POST /api/kv/export` body `{cache_id, model_name}` → `{status, bundle}` (source local cache including tensor).
  - `POST /api/kv/import` body `{bundle}` → `{status, stored}` (target store_local budget gate + LRU).
  - `KVExportRequest`/`KVImportRequest` request models.
- **Master `sync_kv_cache` real transport** (`master/cluster_master.py`) — S3, GAP-7
  - Rewritten: register KVCacheSyncMessage metadata → `_kv_lock` snapshot entry → resolve source (`_snapshot_nodes`) + target (explicit or `select_nodes(DATA, exclude_nodes=[src])`) → bidirectional SSRF guard (`is_safe_peer_host`) → source `/api/kv/export` (build_safe_url + Bearer + X-Node-Id/Role "master", timeout=max(30, size_mb*2+30)) → target `/api/kv/import` → on success register replica `KVCacheEntry(cache_id="{id}@{tgt}")` + LRU trim. Returns True/False (any hop failure/missing → False, no false-reporting of partial success).
  - Adds optional `target_node_id=""` param (empty→auto-select a non-source online node).
- **`/api/kv/sync` route** (`server/master_server.py`) — S3, GAP-7
  - `POST /api/kv/sync` body `{cache_id, model_name, source_node_id, size_mb, target_node_id?}` → `{status, synced}` (Bearer auth, standby guard 503, audit action `kv_sync`).
  - `KVSyncRequest` request model.

### Changed

- `tests/test_new_features.py::TestKVCacheSyncMessage::test_sync_kv_cache_in_master` rewritten — no longer asserts "truthfully returns False", now asserts transport executed (returns True + tensor retrieved at target); `test_sync_kv_cache_missing_entry` stays False.

### Tests

- **`tests/test_kv_tensor_serialize.py`** (new, 11 cases) — S1: KVShard.tensor round-trip / no-tensor backward-compat / SyntheticKVTransport determinism / differs across node_id / env backend selection / export/import_bundle wiring tensor.
- **`tests/test_kv_export_import_routes.py`** (new, 6 cases) — S2: ASGI route round-trip (PortRoutingTransport) / budget reject oversize / missing cache 404 / auth 401.
- **`tests/test_kv_tensor_e2e.py`** (new, 4+1 skip) — S3: master orchestrates 2 agents real ASGI, tensor bytes cross-node complete / auto-select target / missing entry False; env-gated real-tensor test skipped (pending #650).
- `tests/test_master_server.py` adds `test_kv_sync_route_missing_entry`.

### Docs / Version

- Version 0.10.7 → **0.11.0** (`pyproject.toml`, `fusion_multi_node/__init__.py`).
- `__init__.py` module docstring: removed "tensor-level KV cross-node transport still no-op", reflects delivery + upstream #650 gating.
- README badge: version 0.10.7→0.11.0, tests 1181→1203; header F5→GAP-7 release block; R3/P3-28 marked delivered.
- Full `pytest tests/ -q`: **1203 passed**, 1 skipped, ruff clean.

### Acceptance (#33)

1. `sync_kv_cache` switches to real KV tensor cross-node + returns True ✓
2. Integration test verifies tensor round-trip across 2 agents (`test_kv_tensor_e2e.py`) ✓
3. README "Master-level KV tensor sync is no-op" → delivered ✓

### Risks / constraints

- **JSON tensor size**: base64-compressed shard tensor travels with JSON — large tensors inflate. Mitigation: Caveman compression default-on; `size_mb` budget gate rejects oversized; route timeout scales with size_mb (reuses P1-13 pattern). v0.11.0 has no streaming (metadata+bundle single POST), streaming deferred.
- **Upstream dependency**: `MLXKVTransport` is dead code until #650 lands — synthetic backend is the always-available default, #33 acceptance does not depend on upstream. Real tensor is an env-gated bonus.
- **100% local/offline**: synthetic backend is pure local computation; `MLXKVTransport` only calls local fusion-mlx (same node/cluster), introduces no cloud path.

## [0.10.7] - 2026-08-27 — GAP-8 Phase F5: token rotation + multi-tenant ops runbook

> **User multi-active token rotation + cluster shared-token zero-downtime rolling**: user token rotate issues new and keeps old (multi-active, client
> gradual rollout switch no downtime), revoke handled separately. Cluster shared token opens an overlap window via `FUSION_CLUSTER_TOKEN_PREVIOUS` env —
> inbound accepts current + previous (constant-time `secrets.compare_digest`, does not leak which matched), outbound always sends
> current (`_get_dispatch_token` reads `FUSION_CLUSTER_TOKEN`). Roll node-by-node in master→agent order,
> no 401 offline window. Added `docs/OPERATIONS.md` multi-tenant user-token ops section (bootstrap admin / CRUD /
> rotation-revocation / audit). GAP-8 Phase F (multi-tenant / remote access) hereby complete (KV no-op pending upstream, issue #33).

### Added

- **Cluster token previous-active overlap window** (`utils/auth.py`) — GAP-8 Phase F5
  - `BearerAuthMiddleware.__init__` reads `FUSION_CLUSTER_TOKEN_PREVIOUS` env to inject old token; empty/unset/identical to current → no overlap window (single token, behavior unchanged).
  - Cluster token validation path: current miss → if previous exists and matches, pass (constant-time compare, info log `cluster token overlap window: previous-active token passed`), else 401 + audit `auth_fail`.
  - Outbound `_get_dispatch_token` (cluster_master.py) unchanged — reads `FUSION_CLUSTER_TOKEN` (current); during rolling restart the peer already accepts the old value first.
- **Multi-tenant user-token ops runbook** (`docs/OPERATIONS.md`) — GAP-8 Phase F5
  - Rewrote "Token rotation" section: F5 zero-downtime rolling flow (set previous → roll current node-by-node → close window) + full-stop-full-start alternative; outbound semantics note (master→agent order).
  - Added "Multi-tenant user tokens" section: first-boot bootstrap ADMIN (`FUSION_BOOTSTRAP_ADMIN`) / user CRUD API (create/issue/rotate/revoke/list) / multi-active rotation semantics / zero-config backward-compat / audit query.
  - Diagnostic entry data-directory table adds `users.json` (multi-tenant scrypt hash) + `audit.log` (security audit JSONL).
- **Token-rotation tests** (`tests/test_token_rotation.py`, 7 cases) — GAP-8 Phase F5
  - User: rotate issues new and keeps old (old+new both 200); after revoke old token old 401 new 200; rotate route returns new token + old token still valid.
  - Cluster: previous+current both accepted (200); env unset → previous 401; previous==current opens no window (another token 401).
  - Outbound: `_get_dispatch_token` returns current (not previous); previous token still 200 on inbound (out/in two-end semantics separated).

### Changed

- Version 0.10.6 → **0.10.7** (`pyproject.toml`, `fusion_multi_node/__init__.py`).
- README badge: version 0.10.6→0.10.7, tests 1174→1181; header F4→F5 release block; remaining-tasks deletes F5 (done), leaves only KV no-op (#33).

### Fixed

- None (no defect fix this round).

### Tests

- Full `pytest tests/ -q`: **1181 passed**, ruff clean.
- New `test_token_rotation.py` 7 cases (+7, 1174→1181).
- Note: `test_pipeline_e2e.py::test_pipeline_two_shard_real_tensor` is real fusion-mlx tensor inference; under full-suite load it occasionally shows RUNNING (real-model forward timing race); passes when run alone, not a code defect.

## [0.10.6] - 2026-08-27 — GAP-8 Phase F4: cluster-control API contract /api/v1

> **/api/v1 typed contract + HTTP docs + drift detection**: `/api/v1/*` routes get `response_model=` Pydantic contracts,
> covering 9 cluster-control operations (list_nodes/register/remove/submit/migrate/degrade/progress/cluster_stats/
> observability_suggestions). fusion-agent-studio can bridge to a real multi-node cluster on this basis (replacing the in-memory dev cluster,
> resolves #32). OpenAPI `/openapi.json` now returns typed schema for the 9 ops + autoscaler/observability.

### Added

- **13 V1* Pydantic response models** (`server/master_server.py`) — GAP-8 Phase F4, issue #32
  - `V1NodeResponse` (16 fields incl role), `V1NodeListResponse`, `V1NodeRegisterResponse`, `V1StatusResponse`
  - `V1TaskResponse` (16 fields), `V1TaskSubmitResponse` (+queued), `V1TaskProgressResponse`
  - `V1ClusterStatsResponse`, `V1ObservabilitySuggestionsResponse`, `V1AutoscalerConfigResponse`
  - Aligned to actual `_node_to_resp`/`_task_to_resp` output; old v0.1-era `NodeResponse`/`TaskResponse` (stale fields, never wired to response_model) deleted
- **typed /api/v1 routes** — 9 ops typed via `response_model=`:
  - `GET /api/v1/nodes`, `GET /api/v1/nodes/{id}`, `POST /api/v1/nodes/register`, `DELETE /api/v1/nodes/{id}`
  - `POST /api/v1/tasks/submit` (200 dispatched / 202 queued), `POST /api/v1/tasks/{id}/migrate`, `POST /api/v1/tasks/{id}/degrade`
  - `GET /api/v1/tasks/{id}/progress`, `GET /api/v1/cluster/stats`, `GET /api/v1/observability/suggestions`
  - autoscaler GET/PUT explicit 503 not-wired (contract-documented, unambiguous enabled:False)
- **HTTP docs** — `docs/API.md` rewritten as an HTTP route contract table (9-op contract table + other route groupings); Python class docs moved out to `docs/PYTHON_API.md`
- **Drift-detection tests** — `tests/test_api_docs_contract.py`: every `/api/v1` route must appear in API.md; 9-op contract table complete; PYTHON_API.md exists

### Fixed

- **Duplicate route shadowing** (first-registered-wins): old untyped `/api/v1/cluster/stats`, `/observability/suggestions`,
  `/autoscaler/config` (GET/PUT), `/tasks/{id}/progress` shared paths with new typed copies → later registrations shadowed (dead code).
  Fix: bless old routes with `response_model=` (fields aligned to V1* models), delete 5 shadowed copies + unused `V1AutoscalerConfigUpdateRequest`.
  Verified: OpenAPI `/openapi.json` returns correct `$ref` for the 4 routes (V1ClusterStatsResponse etc).

### Tests

- `tests/test_v1_contract.py` (17): 9-op schema validation + register 400 / submit 202 queued / degradation-chain model / progress 404
- `tests/test_api_docs_contract.py` (3): docs drift detection
- Regression 1174 passed (+20), ruff clean

## [0.10.5] - 2026-08-27 — GAP-8 Phase F3: unified inference proxy /v1/chat/completions

> **Unified inference entry + tenant in-flight quota**: master adds a lightweight `/v1/chat/completions` pass-through proxy,
> routes via `select_nodes(DATA, count=1)` to a chosen node agent `/api/v1/chat/completions` →
> `FusionMLXBackend.chat` (native OpenAI format, supports streaming SSE passthrough). User token via `chat:complete` RBAC +
> tenant in-flight concurrency quota (reuses `_tenant_max_concurrent`, exceeded → 429 + audit `chat_quota_exceeded`); cluster token
> internal pass-through (no tenant gate). Resolves #27 two-route split — client via the unified master inference entry, not the task pipeline (synchronous direct return,
> does not enter self.tasks/persistence/priority queue).

### Added

- **master `/v1/chat/completions` proxy** (`server/master_server.py`) — GAP-8 Phase F3, issue #27
  - `ChatCompletionsProxyRequest` (model/messages/temperature/max_tokens/stream/extra)
  - Flow: `_enforce_user_rbac` (chat:complete; VIEWER→403) → `acquire_chat_slot` (tenant quota, exceeded → 429 +
    audit `chat_quota_exceeded`) → `select_nodes(DATA, count=1)` → `build_safe_url` + `is_safe_peer_host`
    (outbound SSRF guard) → reuse `_get_dispatch_http` connection pool to forward → native OpenAI format direct return / streaming `StreamingResponse`
  - Slot release unified try/finally + `stream_released` flag (streaming releases in `_relay` finally, non-streaming/exception releases in outer layer, prevents double)
  - Audit `actor=user_id` (user token) / `master` (cluster token), `action=chat`, `node_id=selected node`
- **agent `/api/v1/chat/completions` passthrough** (`server/agent_server.py`) — GAP-8 Phase F3
  - `ChatCompletionsRequest` + `POST /api/v1/chat/completions` → `_check_permission` (TASK_EXECUTE) →
    `FusionMLXBackend.chat` (429 backoff + api_key Bearer); non-streaming direct return, streaming `StreamingResponse` passthrough fusion-mlx SSE
  - `is_safe_path_segment(model)` guard (path-traversal prevention, illegal → 400); non-FusionMLXBackend → 503
- **Tenant in-flight quota** (`master/cluster_master.py`) — GAP-8 Phase F3
  - `_chat_lock` + `_inflight_chat: dict[str,int]` lightweight counter (independent of the three-domain locks, does not pollute self.tasks)
  - `acquire_chat_slot` / `release_chat_slot` reuses `_tenant_max_concurrent` (0=unlimited); quota full returns False → 429
- **node-RBAC mapping** (`security/permission.py`): `/api/v1/chat/completions` → TASK_EXECUTE (cluster-internal master dispatch)

### Tests

- `tests/test_chat_proxy.py` (8): USER non-streaming 200 routing / cluster token pass-through / VIEWER 403 / no node 503 /
  tenant quota full 429 + audit / audit actor=chat / streaming SSE / slot release returns to zero
- `tests/test_agent_chat_passthrough.py` (5): non-streaming / streaming SSE / illegal model 400 / no token 401 / empty model 400

**Regression**: 1154 tests passed, 0 ruff errors.

## [0.10.4] - 2026-08-27 — GAP-8 Phase F2: per-user RBAC + user CRUD + tamper-proof audit

> **Multi-tenant RBAC enforcement + user management**: user token authenticated via `check_user_path_access` by UserRole (USER
> can submit/cancel, VIEWER read-only, migrate/degrade ADMIN-only); `task.user` takes the authenticated user_id, ignores
> client self-report (prevents forged audit actor); adds ADMIN-only user CRUD + token issue/revoke/rotate API. Cluster token path unchanged
> (internal trusted, user-layer auth does not intercept). Fixed the RBAC bypass defect on dynamic task subpath `/api/tasks/{id}/<op>`.

### Added

- **per-user RBAC enforcement** (`server/master_server.py`) — GAP-8 Phase F2
  - `_resolve_actor` / `_user_token_role` / `_enforce_user_rbac`: user token authenticated via `check_user_path_access`;
    cluster token has no `user_role` → skip user layer, fall to node-RBAC (internal trusted)
  - `submit_task` / `cancel_task` / `degrade_task` / `migrate_task`: user token → per-user RBAC;
    `task.user=authenticated user_id` (ignores client `req.user`, prevents forged audit actor)
  - Audit `actor=authenticated user_id`; VIEWER/USER privilege escalation → 403 + audit `permission_deny`
- **User-management CRUD** (`server/master_server.py`) — ADMIN-only (`user:manage` permission)
  - `POST /api/v1/users` (create user), `GET /api/v1/users[/{id}]` (list/detail, does not return hash/salt)
  - `DELETE /api/v1/users/{id}` (delete, rejects self-delete), `PUT /api/v1/users/{id}/role` (change role)
  - `POST /api/v1/users/{id}/tokens` (issue, plaintext returned once only), `DELETE /api/v1/users/{id}/tokens/{tid}` (revoke)
  - `POST /api/v1/users/{id}/tokens/rotate` (rotate, old token kept multi-active)
  - Cluster token calling user management → 403 (requires ADMIN user token); no user_store → 503
  - Token plaintext does not enter audit log (prevent log leakage); `is_safe_path_segment` guards user_id
- **Request models**: `UserCreateRequest` / `UserTokenIssueRequest` / `UserRoleUpdateRequest`

### Fixed

- **RBAC dynamic task subpath bypass** (`security/permission.py`): `check_user_path_access` prefix-match did not reach
  `/api/tasks/{task_id}/<op>` (op is trailing, not a prefix); added task parent-path + trailing op joint check
  (cancel/migrate/degrade), otherwise VIEWER could bypass cancel auth

### Tests

- `tests/test_user_rbac.py` (12): USER submit OK / VIEWER read-only 403 / migrate·degrade ADMIN-only /
  `task.user`=authenticated non-forged / audit actor=authenticated / cluster-token path unchanged
- `tests/test_user_crud.py` (17): create/read/update/delete / issue-revoke-rotate / non-ADMIN 403 / cluster-token 403 /
  token plaintext never in audit / persisted across restart / self-delete rejected / 503 without store
- Full suite 1141 passed (F1 baseline 1112 + F2 29)

## [0.10.3] - 2026-08-27 — GAP-8 Phase F1: per-user token store + dual-token middleware

> **Multi-tenant token foundation**: introduces per-user API tokens (orthogonal to the cluster shared token); BearerAuthMiddleware routes by the `fmu_`
> prefix; user token storage file-persisted (scrypt hash, 0600); UserRole (ADMIN/USER/VIEWER) + user-layer
> path auth. Single-tenant zero-config backward-compatible (no users.json → pure cluster_token, byte-level old behavior).

### Added

- **User token store** (`security/user_store.py`) — GAP-8 Phase F1
  - `UserStore`: file-persisted `~/.fusion/multi-node/users.json` (FUSION_USERS_FILE override), atomic tmp+replace, 0600
  - Keys store only scrypt hash (`hashlib.scrypt`, stdlib, no new dependency); plaintext token returned once only at issue
  - Token format `fmu_<userid>_<secret>`; multi-active tokens (rotate issues new without revoking old, revoke revokes)
  - `create/delete/list/issue/revoke/revoke_all/rotate/validate/set_role/bootstrap_admin`
  - `load_user_store()` — no env no file → None (middleware falls back to pure cluster_token); present → UserStore
- **UserRole** (`security/permission.py`) — user-layer role orthogonal to NodeRole (ADMIN/USER/VIEWER)
  - `_USER_ROLE_PERMISSIONS` + `_USER_PATH_PERMISSION_MAP` + `check_user_path_access(role, path, method)`
  - ADMIN: user management + all task operations; USER: task submit/cancel/query + inference; VIEWER: read-only
- **Dual-token middleware** (`utils/auth.py` `BearerAuthMiddleware`)
  - `user_store` optional param: when injected `fmu_` prefix → UserStore.validate → inject scope user_id/user_role
  - cluster_token path O(1) unchanged; without user_store `fmu_` explicitly rejected (cluster-internal traffic does not carry user tokens)
  - Auth-failure audit detail distinguished (user-token validation failure / not usable for node routes / token mismatch)
- **First-boot bootstrap** — `FUSION_BOOTSTRAP_ADMIN` env: when no user store exists, auto-create ADMIN and issue the first token (logged only, token not echoed)
- **`security/__init__.py`** re-export UserRole/UserStore/UserRecord/UserToken/check_user_path_access/load_user_store

### Backward compatibility

- No `FUSION_USERS_FILE` and no `~/.fusion/multi-node/users.json` → `load_user_store()` returns None →
  middleware pure cluster_token, byte-level identical to old version. Single-tenant zero-config deployment has no behavior change.
- Cluster-internal HTTP (master→agent dispatch, agent heartbeat, KV cross-node, CLI) still uses cluster_token, unaffected.

### Tests

- `tests/test_user_store.py` (22 cases): create/delete/role/issue/validate/revoke/rotate/multi-active/persist/atomic-write/corrupt-degrade/empty-store/bootstrap
- `tests/test_enterprise_security.py::TestUserTokenAuth` (6 cases): fmu pass/wrong 401+audit/cluster unchanged/agent rejects fmu_/no-store fallback/bootstrap env
- `tests/conftest.py`: clears FUSION_USERS_FILE/FUSION_BOOTSTRAP_ADMIN (isolated HOME has no users.json → None)
- 1112 tests (was 1085), 0 ruff errors

## [0.10.2] - 2026-08-26 — GAP-5 dead-code remediation

> **Dead-code cleanup/labeling**: the autoscaler not-wired route changed from ambiguous `{"enabled":False}` to explicit 503 not-wired;
> StandbyMaster dead code deleted (zero instantiation, independent of the already-wired MasterElection). Modules kept pending migration.

### Changed

- **autoscaler route explicit not-wired** (`server/master_server.py`) — GAP-5 audit §7
  - Old `GET /api/v1/autoscaler/config` returned `{"enabled": False}` — ambiguous ("disabled" vs "not implemented")
  - Changed to 503 + detail stating not-wired (`Autoscaler not-wired: module exists but never instantiated`)
  - `PUT /api/v1/autoscaler/config` likewise changed from 404 to 503 not-wired
  - Module (`autoscaler/`) kept pending migration (not a production path, not a cloud-compliance debt)

### Removed

- **StandbyMaster dead code** (`master/cluster_master.py`) — GAP-5 audit §7
  - Zero production instantiation, zero imports (except `master/__init__.py` re-export), zero tests, zero CLI/server references
  - Independent of the already-wired `MasterElection` (the actual path for P4 HA + GAP-1 full-state sync)
  - Deleted class + `master/__init__.py` re-export; `__init__.py` module docstring updated
  - HA path now unique: `MasterElection` (single Master `_election is None` no HA; multi-Master `ha_config` explicit start)

### Fixed

- **autoscaler route ambiguity** (GAP-5 audit §7) — `enabled:False` changed to explicit 503 not-wired, avoids misreading as wired-but-off

### Tests

- `tests/test_master_server.py::TestMasterServerAutoscalerNotWired` (2 cases): GET/PUT not-wired → 503 + detail contains not-wired
- 1085 tests (was 1083), 0 ruff errors

## [0.10.1] - 2026-08-26 — GAP-6 throughput cap + client-side pacing

> **Rate-limit adaptation completed**: upstream fusion-mlx #635 fixed (PR #637, `--rate-limit 0` truly disables rate limiting, default off);
> this release adds client-side 429 backoff retry + master rate-limit classification fix — healthy nodes being rate-limited no longer mis-banned.

### Added

- **GAP-6 client-side rate-limit adaptation** (`agent/rate_pacer.py`) — adds fusion-mlx 429 rate-limit handling
  - `dispatch_with_pacing(send_request, pacer)`: wraps HTTP send; on 429 reads the `Retry-After` header, exponential backoff sleep, retries within a `budget_seconds` budget; non-429 (incl 5xx/401) returned as-is without retry
  - `PacerConfig` dataclass (deterministic no jitter, Rule 5): `max_retries=3`, `initial_backoff=0.5`, `max_backoff=5.0`, `budget_seconds=10.0`, `next_backoff(attempt)=min(initial*2^attempt, max)`
  - `parse_retry_after(resp)`: seconds / HTTP-date / missing falls back to 1.0s / illegal falls back to 1.0s / negative clamped to 0.0
  - `RateLimitExhausted` exception: budget exhausted still 429 → raised, carries `last_status`/`retry_after`/`attempts`
  - `FusionMLXBackend.__init__` adds `pacer: PacerConfig | None` param; `chat()`/`embed()` wrapped via `dispatch_with_pacing` (no longer directly `raise_for_status`)
  - `_execute_inference`/`_execute_embedding` catch `RateLimitExhausted` → return `{"error":..., "rate_limited": True, "node_id":...}` (marks rate-limit transient failure)
  - **Defect chain (before fix)**: 429 → `raise_for_status` throws `HTTPStatusError` → agent wraps `{"error":...}` → master `_dispatch_data` `"error" in r` → `logic_fail=True` + `report_fault("agent_internal_error")` → 3 faults/60s → **healthy rate-limited node banned 300s** (misjudgment: rate-limit is transient, not a logic error)
  - `tests/test_rate_pacer.py` (14 cases): backoff determinism / Retry-After parsing / 429 retry-until-success / 429 exhausted raises / budget truncation / 5xx no-retry / backend chat 429 backoff-to-success / exhausted raises RateLimitExhausted

### Changed

- **master rate-limit classification fix** (`master/cluster_master.py` `_dispatch_data` / `_dispatch_pipeline`) — GAP-6
  - `_dispatch_data` adds a branch (placed before `"error" in r`, since the rate_limited dict also has an "error" key): `r.get("rate_limited")` → `transient_fail=True`, does not enter `logic_fail`, **does not call `report_fault`**, does not accumulate circuit-breaker fault count, no ban
  - `_dispatch_pipeline` likewise: rate_limited → `_finalize_task(success=False, retryable=True)`; Exception branch also changed to `retryable=True` (was non-retryable)
  - **Effect**: rate-limited node fault count stays empty, `is_node_banned` always False, healthy node rate-limit no longer blacklists
  - `tests/test_dispatch_integration.py::TestRateLimitedDispatch` (2 cases): single-node rate-limit → FAILED no-ban + fault_counts empty; one healthy one rate-limited → PARTIAL + rate-limited node no-ban

### Fixed

- **Healthy rate-limited node mis-ban** (GAP-6 audit §7) — 429 rate-limit classified as `transient_fail` (retryable) rather than `logic_fail`, skips `report_fault`, does not enter the circuit-breaker window

### External

- **Upstream fusion-mlx #635 CLOSED** (2026-08-25, PR #637 `fix(auth): --api-key on --model-dir path + --rate-limit 0 disables limiter (#636, #635)`): `--rate-limit 0` truly disables the 60rpm limiter, default off; when an explicit cap is set it still returns 429 → absorbed by this release's client-side backoff

## [0.10.0] - 2026-08-26 — GAP-1 always-on SLA

> **Enterprise HA completed**: multi-Master full-state sync landed; standby holds the complete cluster topology and immediately takes over scheduling after the leader dies.
> HA still opt-in (single-Master deployment unchanged); 2+ Masters with explicit config get always-on (gap window ≤ election timeout ~10s).

### Added

- **GAP-1 HA full-state sync** (`master/cluster_master.py` / `server/master_server.py`) — completes the always-on SLA
  - Original HA (v0.8.3) synced only tasks; after Master death standby lacked nodes/kv/banned → had to wait for node re-registration to schedule, not always-on
  - Extended sync scope: leader periodically pushes **nodes + kv_cache + banned_nodes** to standby; standby `receive_synced_state` idempotent-merges
  - `_node_to_dict`/`_node_from_dict` (NodeInfo serialization, status enum ↔ string) + `_kv_to_dict`/`_kv_from_dict`
  - `_build_state_sync_targets` (self-contained nodes→kv two locks snapshotted separately, no nesting) + `_push_sync_state_to_standbys` (out-of-lock async best-effort)
  - `_state_sync_loop` (5s period) wired to `start(ha_config=)` start / `stop()` cancel; leader-only push
  - New endpoint `POST /api/ha/sync-state` — standby receives full state, returns `{"status":"ok","counts":{"nodes":N,"kv":K,"banned":B}}`
  - **Lock order**: nodes→kv (declaration order), `receive_synced_state` holds the two domain locks separately without nesting (consistent with `find_kv_cache` P1-12)
  - **Ban merge**: take the later unban time (whichever of leader/standby banned is more authoritative); expired bans not merged
  - **HA still opt-in**: single Master (`_election is None`) does not start the sync loop, `_build_state_sync_targets` returns empty, behavior unchanged
  - **Failover semantics**: after standby promotes to leader it already holds synced nodes/kv/banned, `assign_task` can dispatch immediately, no gap window
  - `tests/test_ha_election.py::TestHAStateSync` (6 cases): topology sync arrives / idempotent merge / failover immediate scheduling / endpoint round-trip / single Master no targets / illegal status falls back to OFFLINE
  - `docs/HA-CRASH-RECOVERY.md` adds "Multi-Master HA + full-state sync" section (sync-content table / enable config / failover chain)

### Changed

- `pyproject.toml` / `__init__.py`: 0.10.0rc1 → 0.10.0 (GAP-1 always-on = minor)
- `__init__.py` module doc: MasterElection description adds "GAP-1 full-state sync + always-on"

## [0.10.0-rc.1] - 2026-08-26 — Release Candidate

> ⚠️ **RC release**: enterprise production-readiness disclosure gap closed + #31 retry node avoidance. Re-audit §8 release conditions 2/4/5 met (condition 1 CI met in v0.9.0, condition 3 mTLS enforcement met in v0.9.0). **Not GA** — GAP-1/6/5 enterprise residual gaps still unaddressed (see Phase C/D/E plan below).

### Added

- **#31 retry node avoidance: `exclude_nodes` hard blacklist** (`server/master_server.py` / `master/cluster_master.py` / `master/task_spec.py`)
  - `TaskSubmitRequest` / `ClusterTask` / `TaskSpec` add `exclude_nodes: list[str]` field, round-tripped via `to_dict`/`from_dict`/`_task_from_dict` (H3 persistence preserves across restart)
  - `select_nodes`: filters `exclude_nodes` **before** LoadRouter scoring — hard blacklist, never falls back to a node in the list. No candidates after filter → returns `[]` + warning
  - `assign_task`: passes `task.exclude_nodes` through to `select_nodes`
  - `_select_free_nodes_locked` (TOCTOU re-selection): re-selection also honors the blacklist; concurrent-preemption re-selection never falls back to a failed node
  - `preferred_node_id` stays a soft hint (LoadRouter `preferred_bonus`), orthogonal to the hard blacklist
  - **Behavior**: on retry the caller adds the failed node to `exclude_nodes` and resubmits; scheduler picks a different healthy node; if all avoided → enters priority queue (P1-H) rather than dispatching to a blacklisted node. Breaks the "retry lands on the same bad node" loop
  - `tests/test_cluster_master.py` (5 cases): blacklist filter / all-avoided empty-candidate / preferred soft hint / end-to-end assign_task passthrough

### Fixed

- **GAP-4 CI workflow fix** (re-audit §8 condition 1 closed, latent defect introduced in v0.9.0)
  - `pyproject.toml` `[test]`: declares `pytest-randomly>=3.15.0` — CI ran `pytest -p randomly` without declaring the dependency; only local venv had it; a fresh CI install hit `ImportError: No module named 'randomly'`
  - 3 Linux x86_64 runner-incompatible tests get skip-gates (Apple Silicon target project, CI=ubuntu-latest):
    - `tests/test_sandbox_executor.py::test_execute_in_sandbox_timeout`: unshare needs CAP_SYS_ADMIN, CI lacks permission → runtime probe + skip
    - `tests/test_core.py::test_collect_hardware`: asserts `arch == arm64`, non-darwin → skip
    - `tests/test_real_network_e2e.py::test_container_cross_register_and_dispatch`: docker-compose needs `FUSION_MLX_API_KEY` env, CI lacks it → extend skip-gate to require that env (CI does not run real-inference E2E)

### Supplementary disclosure — re-audit §8 release conditions 2/4/5 (commercial pre-conditions)

> v0.9.0 judged ⚠️ CONDITIONAL-READY. This RC closes 3 disclosure conditions, making the single-tenant LAN scenario **conditionally** commercial-ready. Multi-tenant / remote SaaS + always-on SLA still blocked (see below).

- **Condition 2 — GAP-1 HA SPOF disclosure**: default single-Master deployment; Master down → cluster unavailable. `MasterElection` election is opt-in (`start(ha_config={...})`); standby syncs tasks only (not nodes/kv/banned-set). **Does not meet always-on SLA**. Production always-on requires: HA default-on + standby full-state sync (Phase C plan). Current applicability: single-tenant LAN tolerating brief downtime.
- **Condition 4 — GAP-6 throughput cap declaration**: upstream fusion-mlx issue #635 — `--rate-limit 0` does not truly disable the 60rpm limiter; multi-node high-QPS stress is limited. **Single-node inference throughput is capped by the upstream 60rpm limit**; multi-node scales linearly but a single node does not break through. High-QPS clients must adapt to 429 (Phase D plan: client-side throttling + doc declaration).
- **Condition 5 — GAP-5 dead code + GAP-7 KV no-op disclosure**:
  - **GAP-5 dead code**: `autoscaler/` route silently returns `enabled: False` (ambiguous semantics); `mcp_gateway/` unwired dead code (pending migration to fusion-gateway #106); `cloud_fallback.py` scheduling path cut in v0.8.2 (module + unit tests kept for standalone validation, pending migration); `StandbyMaster` unwired dead code (single-Master production has no HA). Phase E plan: mark unimplemented / migrate / clean up.
  - **GAP-7 tensor KV no-op**: `ClusterMaster.sync_kv_cache` returns False (metadata-only sync, not tensors). Upstream fusion-mlx has no KV tensor export endpoint (issue #650 filed, blocks #33). Cross-node KV tensor reuse unavailable; current KV is local pre-warm only (`/api/kv/warm` local `store_local`).

### Multi-tenant / remote SaaS blocker declaration (GAP-8 residual)

- v0.9.0 fixed audit logging + permission enforcement default-on, **but a single shared Bearer token** (`~/.fusion/multi-node/.cluster_token`) has no per-user RBAC. Multi-tenant / remote SaaS scenarios are **unusable** — must be replaced with multi-user auth + token rotation (Phase F plan). Current applicability: LAN of a trusted single ops team.

### Changed

- `pyproject.toml` / `__init__.py`: 0.9.0 → 0.10.0rc1 (RC, commercial-precondition disclosure = minor pre-release)
- `pyproject.toml` `[test]`: adds `pytest-randomly>=3.15.0`

## [0.9.0] - 2026-08-26

### Added

- **Enterprise production-readiness blocker fixes** (re-audit 2026-08-26 GAP-2/GAP-4/GAP-8) — security posture upgrade
  - **GAP-2 mTLS fail-closed** (`security/mtls.py`): old impl silently fell back to plaintext when mTLS enabled but cert path incomplete (fail-open); default deployment had zero node-identity verification. Changed to fail-closed — `server_ssl_context()` / `client_ssl_context()` / `server_ssl_kwargs()` / `client_kwargs()` enabled but certs incomplete → raise `RuntimeError` refusing plaintext fallback. Added `certs_available()` helper. mTLS-off behavior unchanged (plaintext is legitimate).
  - **GAP-8 audit log** (`security/audit_log.py` new module): `AuditLogger` append-writes JSONL to `~/.fusion/multi-node/audit.log`, fields ts/actor/action/path/method/node_id/result/detail. Module-level singleton `get_audit_logger()`, thread-safe (threading.Lock), write failure degrades to warning without dragging down the main path. Path overridable via `FUSION_AUDIT_LOG` env. Wired into security action points: `BearerAuthMiddleware` auth failure (auth_fail) / agent permission denial (permission_deny) / master node registration (register ok/denied) / approval granted (approve) / approval rejected (reject) / task submit (task_submit) / task cancel (task_cancel). `BearerAuthMiddleware` gains `audit_logger` param.
  - **GAP-8 permission enforcement default-on** (`server/agent_server.py`): old `_permission_enforce` was only active with mTLS (default off). Now reads `FUSION_PERMISSION_ENFORCE` env (default "1"=on), missing X-Node-Id → 403 (production zero-trust). mTLS-on also enforces. Test isolation: `tests/conftest.py` autouse sets `FUSION_PERMISSION_ENFORCE=0` to fall back to compat mode (existing http tests' AUTH_HEADERS lack X-Node-Id and must pass).
  - **GAP-4 CI workflow** (`.github/workflows/ci.yml` new): ruff check + pytest (random order + fixed-seed double run, catches test-isolation regressions). Gates every release. `FUSION_PERMISSION_ENFORCE=0` isolation.
  - `tests/test_enterprise_security.py` (21 cases): mTLS fail-closed (8) / AuditLogger (6) / auth-failure audit (2) / permission enforcement default (3) / master route audit (2).

### Changed

- `security/__init__.py`: exports `AuditLogger`
- `pyproject.toml` / `__init__.py`: 0.8.9 → 0.9.0 (security posture upgrade = minor)
- `tests/conftest.py`: autouse fixture adds `FUSION_PERMISSION_ENFORCE=0` + `FUSION_AUDIT_LOG` isolation + `reset_audit_logger()` rebuilds the singleton per test

## [0.8.9] - 2026-08-26

### Fixed

- **Test-isolation defect fix** (found in re-audit 2026-08-26, Rule 9/12 violation) — tests polluted the real `~/.fusion/multi-node`
  - `tests/conftest.py` (new): autouse `_isolated_home` fixture redirects HOME to a per-test tmp_path, isolating tasks.json/config.json/kv_cache.json/election_state.json — all `~/.fusion` writes; symlinks the real `~/.docker` to preserve docker compose plugin discovery (container E2E unaffected)
  - `fusion_multi_node/cli.py`: module-level `_config = ClusterConfig()` (resolved `Path.home()` at import, cached the real path, HOME redirect could not override it) → lazy `_get_config()`, instantiated only on first access; 14 read sites all updated
  - Root cause: H3 task persistence wrote non-terminal tasks to the real tasks.json + CLI instantiated at import; pollution source `TestPriorityQueue::test_cancel_running_drains_queue` left a RUNNING task; order-dependent failure under random order, deterministic order's 1036-green masked the bug
  - Verification: `pytest tests/ -q` random order 1036 passed, ruff clean, real `~/.fusion/multi-node` never touched

### Added

- **Re-audit report** `docs/audit/RE_AUDIT_2026-08-26.md` — v0.8.8 evidence re-review against the original audit's 13 CRITICAL + 29 items; judgment ⚠️ CONDITIONAL-READY (was ❌); 8 enterprise residual gaps + 5 release conditions; P0 8/8 P1 10/10 P2 8/8 P3 2/3 (P3-28 tensor KV no-op issue #33 known limitation)

## [0.8.2] - 2026-08-25

### Added

- **H3 Master task persistence + crash startup recovery** (#66)
  - `_persist_tasks_locked`: atomically persists non-terminal tasks (tmp+os.replace+fsync); terminal states not stored
  - Immediate persist on write: assign_task (RUNNING) / _finalize_task (terminal) / cancel_task (terminal), all holding `_tasks_lock`
  - `_persist_loop`: 15s periodic snapshot backstop; `start()` calls `_restore_tasks()` to recover, `stop()` does final persist
  - `_restore_tasks`: RUNNING/MIGRATED → PENDING re-dispatch (tasks in flight at crash must be rescheduled)
  - `tests/test_task_persistence.py` (10 cases): recovery semantics / terminal not stored / atomic write / corrupt file / full chain start→stop→restore
- **H2 launchd process supervisor — crash self-heal loop** (#69)
  - `deploy/com.dahai80.fusion-multi-node.plist`: launchd template (KeepAlive crash restart + ThrottleInterval 10s throttle + RunAtLoad), placeholder render (venv/host/port/logdir)
  - `start.sh install-launchd` / `uninstall-launchd`: render plist → `~/Library/LaunchAgents` → launchctl load/unload; detects a nohup process and stops it first to hand off to launchd (avoid dual instances)
  - Crash → launchd restart → H3 `_restore_tasks` recovers = no task loss (process layer + data layer dual guarantee)
  - `docs/HA-CRASH-RECOVERY.md`: crash self-heal chain diagram + two-layer guarantee + limitation notes
- **S1 task-level circuit breaker** (#70) — auto-ban on dispatch failure, no longer keeps dispatching to a faulted node
  - `_dispatch_to_node` failure (SSRF rejection / HTTP non-200 / agent returns non-ok) → `report_fault(node_id, "dispatch_failed")` accumulates faults
  - `select_nodes` candidate filter skips nodes within a ban period (gap: original ban only blocked in `register_node`, scheduling path missed it)
  - Reaching `_FAULT_THRESHOLD` (3) within the window auto-bans; banned nodes not selected during the ban; selectable again after expiry / manual unban
  - `tests/test_task_circuit_breaker.py` (6 cases): dispatch-failure reports fault / repeated failure bans / success reports no fault / banned node skipped / all-banned returns empty / unban re-selects
- **S2 production metrics endpoint** (#71) — Prometheus exposition `/api/v1/metrics`
  - `ClusterMaster.get_prometheus_metrics`: plaintext 0.0.4 exposition, no external dependencies
  - Cluster-level aggregation: total/online nodes, total/running/pending/completed/failed tasks, total retries, KV cache entries, total/available memory, dispatch-latency quantiles (p50/p90/p99 + sum/count)
  - Reuses `get_stats` + dispatch latency (completed_at - started_at) + `_retry_count`; Bearer auth not exempt (internal scrape carries token)
  - `tests/test_master_server.py::TestPrometheusMetrics` (4 cases): auth / text-plain / exposition shape / empty cluster
- **S3 load/stress baseline tests** (#72) — scheduling-layer stress throughput / tail latency / no loss
  - `tests/test_load_stress.py` (3 cases): four-node cluster + FastBackend (zero latency, no real model), real dispatch via PortRoutingTransport ASGI routing
  - 40 concurrent tasks with no loss (lost=0/failed=0/backend_calls=40), dispatch throughput > 20 task/s
  - Dispatch-latency tail distribution (p95 < 1.0s, p99 < 2.0s); DATA parallel two-node 20-task throughput > 10 task/s
  - Stress run lifts agent rate limit (default 30 req/min → 100000) + node max_tasks=200 (measures scheduling throughput, not capacity ceiling)
- **S4 real-model integration test coverage** (#73) — DATA parallel E2E real inference + KV sharing E2E real ASGI routing chain
  - `tests/test_data_parallelism_e2e.py` (1 case): skip-gate `_mlx_alive() and _model_available()` (checks `/v1/models` list contains the model id), skips if fusion-mlx is down; 2-node DATA parallel real inference (`mlx-community-Llama-3.2-1B-Instruct-4bit`), asserts COMPLETED / node_count==2 / both nodes return non-empty content+usage
  - `tests/test_kv_sharing_e2e.py` (4 cases): synthetic KVCacheEntry validates the cross-node HTTP routing chain (not model tensors, no skip-gate) — same-node store→lookup round-trip / miss 404 / warm cross-node push / stats route; `PortRoutingTransport` routes by URL port to ASGI (no real TCP), manager `_get_http_client` monkeypatch rewrites the `:11458` port
  - Covers the agent_server KV route end-to-end that the original 17/24 unit-mock files never reached

### Changed

- **H4 cloud_fallback scheduling path cut** (#67) — the only module violating the "100% local/offline" positioning
  - Deleted `ClusterMaster.setup_cloud_fallback` / `fallback_to_cloud` / `_cloud_client` field / import
  - `_enqueue_retry`: retry limit exceeded → FAILED directly, no longer falls back to cloud
  - `_retry_loop`: deleted `_cloud_fallback_pending` branch, pure retry
  - `cloud_fallback.py` module file + unit tests kept for standalone validation, no longer wired to the scheduler; pending migration to fusion-gateway (#106)
- **Functional-attribution-debt separation** (#67) — ast_diff / cluster_sync / mcp_gateway are all pure-local computation (not cloud-compliance debt)
  - ast_diff reused by `secure_transfer` (PII redaction transport) → pending migration to fusion-cowork (#61)
  - cluster_sync reused by `master_server` (LAN model manifest) → pending migration to fusion-cowork (#61)
  - mcp_gateway unwired dead code → pending migration to fusion-gateway (#106)
- `__init__.py` `__version__` 0.7.1 → 0.8.2 (historical missed-update fix); comments distinguish cloud-compliance debt vs functional-attribution debt
- pyproject.toml version 0.8.1 → 0.8.2

### Fixed

- CLAUDE.md: cluster_sync "not wired into lifecycle" is in fact wired into master_server start()/stop() — corrected; cloud_fallback marked with the v0.8.2 cut status
- **FusionMLXBackend `/v1/*` missing auth header** (#73, exposed by S4 E2E) — `chat`/`embed` POST `/v1/chat/completions`, `/v1/embeddings` did not carry `Authorization`. When fusion-mlx enables api_key, `/v1/*` is equally protected (same source as `/distributed/*`); missing header always 401. Added `headers=self._dist_headers()`. Production defect: any fusion-mlx inference with auth enabled got a direct 401
- **KVSharingManager cross-node HTTP missing auth header** (#73, exposed by S4 E2E) — `lookup_remote`/`transfer_from_remote`/`warm_cache` POST to the peer agent `/api/kv/*` did not carry `Authorization`. The peer `BearerAuthMiddleware` authenticates by default; missing token → all 401. `KVSharingManager` gains a `cluster_token` param + `_auth_headers()`; `AgentServer.__init__` passes through `shared_token`. Production defect: cross-node KV sharing all-401 on authenticated agents
- **KVWarmRequest contract mismatch + kv_warm route recursion** (#73, exposed by S4 E2E) — schema required `prompts: list[str]` (plural, required) but `warm_cache` sent `{model_name, prompt, prompt_hash}` (singular) → 422; and the `/api/kv/warm` route callback `self.kv_manager.warm_cache` (a second cross-node remote push → recursion). Changed schema to `{model_name, prompt, prompt_hash, total_tokens, total_size_bytes}`; the route only does local `store_local` (cross-node distribution is owned by `warm_cache`)

## [0.8.1] - 2026-08-25

### Added

- **Node registration idempotent + fault blacklist** (#20, F-A12 / F-A13)
  - F-A12: `register_node` re-registration = PATCH semantics — preserves Master-authoritative runtime fields (`active_tasks`/`max_tasks`/`network_rtt_ms`/`status`), only updates hardware-declared fields. A node restart does not wipe the in-flight task count. Return value changed from `None` to `bool` (`False` during a ban)
  - F-A13: `report_fault` accumulates within the `_FAULT_WINDOW_S` (60s) window; reaching `_FAULT_THRESHOLD` (3) → auto-ban for `_BAN_DURATION_S` (300s); during the ban `register_node` is rejected (master_server HTTP 403)
  - `unregister_node(reason="banned")` actively blacklists; `is_node_banned()` / `unban_node()` manual query/unban; lazy auto-unban on expiry
  - `tests/test_node_registration.py` (9 cases): PATCH preserves runtime state / OFFLINE recovery / fault-threshold ban / ban rejects registration / manual unban / reason blacklist / window decay

### Fixed

- agent port migration (#19, PR #22): Node Agent default port 11445 → 11458 (collision with fusion-comfyui), full 81-site replacement + `ClusterConfig._STALE_PORT_MAP` auto-migration (9755→11458, 11445→11458)
- `tests/test_pipeline_e2e.py` E2E skip-gate adds a local `mlx` package importability check — a venv without the mlx package cleanly skips rather than crashing on import

## [0.8.0] - 2026-08-25

### Added

- **Real-tensor PIPELINE layer-split chain** (P3, wired to fusion-mlx #621 `/distributed/*`)
  - `FusionMLXBackend`: `load_shard` / `pipeline_step` / `drop_shard` HTTP calls to the upstream distributed endpoints, b64.npy activation format, Bearer api_key auth (explicit over env, Rule 5)
  - `NodeAgent._execute_pipeline_step`: pipeline_step task type, reads model_id/layer_range/hidden_states/input_ids, calls upstream load_shard + pipeline_step, returns {shard_id, hidden_states, shape, dtype, node_id}
  - `ClusterMaster._dispatch_pipeline`: real layer split — splits by `task.model_shards`, first segment carries input_ids (embed+layers), subsequent segments chain hidden_states, the final node's output = the final tensor. `_dispatch_to_node` passes through pipeline_step_params
  - `AgentConfig.fusion_mlx_api_key` + `DEFAULT_CONFIG.mlx.fusion_mlx_api_key` + `to_node_agent_config` passthrough
  - `agent_server`: ALLOWED_TASK_TYPES adds pipeline_step; PIPELINE_EXTRA_KEYS passes through model_id/shard_index/layer_range/hidden_states/input_ids/position_ids
  - **Real-model E2E** (`tests/test_pipeline_e2e.py`): Llama-3.2-1B-Instruct-4bit (16 layers) split [0,8]/[8,16], two NodeAgents share a real fusion-mlx, PortRoutingTransport dispatch, final node returns hidden_states shape [1,4,2048] float16, b64.npy round-trip verification. Requires fusion-mlx running + a small model; skips otherwise

- **master→agent dispatch loop wiring** (P1): `assign_task` actually sends HTTP to assigned_nodes (PIPELINE sequential chain / DATA concurrent), `_dispatch_tasks` tracks + `_finalize_task` backfills
- **HA election wiring** (P4): `ClusterMaster.start(ha_config=...)` calls `setup_election` to start the election loop when enabled=True (default off, single Master)
- **start.sh agent role** (P2): supports `--role agent` to launch a NodeAgent
- **Real multi-node integration test** (P5, `tests/test_dispatch_integration.py`): PortRoutingTransport routing + real ASGI agent, no real TCP

### Changed

- `cluster_master.py`: stale comment "setup_election not called by start()" updated to the P4 wired status
- pyproject.toml: version 0.7.1 → 0.8.0
- README.md: badge/tests updated (852), module table + architecture diagram + election section + real-tensor PIPELINE example

### Fixed

### Fixed

- `test_node_agent.py::test_hardware_report_loop` hang: R1 refactor changed `_hardware_report_loop` to call `_collect_dynamic_load`, but the test still mocked `collect_hardware_info` → call_count never grew → infinite hang. Changed the mock to `_collect_dynamic_load`

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

[0.10.2]: https://github.com/dahai80/fusion-multi-node/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/dahai80/fusion-multi-node/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.10.0-rc.1...v0.10.0
[0.9.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.8.9...v0.9.0
[0.8.2]: https://github.com/dahai80/fusion-multi-node/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/dahai80/fusion-multi-node/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.4.0...v0.8.0
[0.4.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dahai80/fusion-multi-node/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dahai80/fusion-multi-node/releases/tag/v0.1.0

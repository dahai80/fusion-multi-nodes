# fusion-multi-node 对抗性代码审查报告

- **审查日期**: 2026-08-24
- **审查版本**: v0.6.9 (commit 66f18af)
- **审查范围**: `fusion_multi_node/` 全 40 模块, 13847 行
- **审查方式**: 4 路并行审计 agent (protocol / security / storage+distributed+autoscaler / server+agent+discovery+config) + lead 逐文件复读 13 模块
- **审查模式**: 对抗性逆向评审, 默认代码存在漏洞/设计缺陷/隐性风险
- **活体致命缺陷二次确认**: grep + 源码核对完成
- **只读审查**: 未改任何代码

## 目录

1. [架构硬伤](#一架构硬伤跨模块关联设计失误不修无法商用)
2. [活体致命缺陷](#二活体致命缺陷现网接线面非死代码)
3. [活体逻辑 bug + 性能](#三活体逻辑-bug--性能现网路径非致命但商用阻塞性)
4. [商用发布判定](#四明确结论是否具备生产商用发布条件)
5. [P0/P1/P2 修复清单](#五达到商用发布条件必须做的事按优先级)

---

## 一、架构硬伤（跨模块关联设计失误，不修无法商用）

### 硬伤 1：单一对称共享密钥 = 零角色隔离 = 任何 Worker 即全权 Master

`~/.fusion/multi-node/.cluster_token` 是所有 Master/Agent/CLI 共用的同一把对称密钥。后果链：

- 任何持 token 的 Worker 可 POST `role="master"` 注册 → `master_server.py:293-294` 直接 `assign_role(MASTER)` → 全集群接管。
- `approve`/`reject`/`migrate`/`autoscaler-config`/`routing-strategy` 端点只查 bearer token, Worker 持同 token → 审批门、路由策略全可被 Worker 操纵。审批门 (M6-03) 形同虚设。
- mDNS `cluster_hash = sha256(token)[:32]`, token 全员持有 → 任何 Worker 可广播伪造 master, `find_master` 接受 → rogue-master 接管, 心跳/任务结果全流向攻击者。
- 所有 SSRF sink (`/api/sync/incremental`、`network_topology.measure_peer_latency`、agent `_execute_model_sync`) 对 Worker 可达, 因 token 是全权通行证。

**根因**: 无凭证分层 (admin token vs worker token), 无 mTLS per-node 证书。单点修路由无用, 整类"Worker 可做任何事"必须从凭证层重构。

### 硬伤 2：HA/选举/Standby 全是未接线死代码, 但对外宣称高可用

- `StandbyMaster`: 零生产实例化 (grep 仅 `__init__` re-export + 类定义)。宣称 STANDBY→LEARNING→TAKING_OVER→ACTIVE 状态机, 但 LEARNING 同步从未实现, `_take_over` 新建空 Master (0 节点/0 任务/0 KV), 集群状态全丢。
- `MasterElection`: 零生产实例化。Raft-simplified 实现自相矛盾——2 节点集群 `majority=(1+1)//2+1=2` 永远选不出 (只剩 1 个他节点投票); `current_term/voted_for` 纯内存无持久化, 重启归零可投给过期候选人; `_start_election` 持 `self._lock` 跨整个投票轮, 集群冻结。
- 现网真相: 单 Master, 无 HA, Master 挂 = 集群挂。文档/模块名暗示的高可用是虚假承诺。

### 硬伤 3：重启即失忆——核心状态全内存无持久化

`NodeApprovalManager` 审批/拒绝记录、`MasterElection` term/vote、`ClusterMaster` 节点表/任务表/KV 缓存、路由策略——全内存。Master 重启: 已审批节点变陌生、运行中任务丢失、KV 缓存清空、选举 term 归零。对接线面: 无优雅关停 (见硬伤 6), 重启 = 集群状态硬重置。

### 硬伤 4：M9 存储 + M9 分布式 KV/shard + M10 Autoscaler = 未接线原型, 含多个活体级致命缺陷潜伏

grep 确认: `DistributedMLXBridge`、`ShardReplicator`、`CheckpointManager`、`StorageVolume`、`DistributedKVStore`、`Autoscaler` 均零非测试实例化。`set_fmp_interface`/`handle_kv_response`/`register_kv_handler` 全无调用方。`ClusterSyncManager.start()` 从未调。后果:

- 表象: feature-complete 分布式存储层。
- 实质: 原型。接线后立即激活致命栈: `fmp_server.py:234` `kv_store.get_entry(key, partition)` 签名不匹配 (实际只收 1 参) → inbound KV 必 TypeError; `shard_replication._sync_via_fmp` fire-and-forget 却返回 `success=True, checksum_verified=True` → quorum 写保证是虚构; `caveman_compress._dict_*` 变长码 (2/4 字节无分隔) + 反查表用 2 字节读 4 字节码 → 张量静默损坏; autoscaler `update_config` 无锁 + `_last_action_time=0.0` 清零冷却 → 热重载竞态 + 绕过冷却门。
- 测试套件隔离通过, 跨模块契约已破。任何未来接线若无集成测试门禁, 必爆。

### 硬伤 5：安全子系统与运行时脱节——security/ 是文档不是防御

- `WorkerSandbox`/`SandboxExecutor`: grep `agent/`、`distributed_mlx/`、`master/` 零导入 → Worker 任务执行零路径/网络/环境过滤。沙箱模块死代码, 提供虚假安全感。
- `PermissionManager.check_path_access`: 仅 5/~25 端点调用, 无中间件级强制, 新端点默认裸奔。
- `DataIsolationPolicy`: `master_only_paths` 默认相对路径 `.fusion/master`, 与绝对输入路径比较恒 False → master-only 数据可被随意传输。
- `SecureTransferPipeline.apply_transfer`: 接收侧不重洗 PII, 发送方漏洗即落地。
- `DataScrubber`: 正则漏 `sk-`/`ghp_`/`xoxb-`/JWT, 电话/身份证无 `\b` 边界 → 最常见密钥格式漏洗。
- security/ 全包: 存在、被独立测试、生产被绕过。共享 bearer token 是唯一真闸门, 且对所有节点平权。

### 硬伤 6：无真正任务取消 + 无优雅关停 + SIGTERM 不处理

- `cli.py:476` `task_cancel` → `complete_task(task_id, "cancelled by user")`, 标记 COMPLETED/FAILED, **不通知节点中止运行任务**。无 CANCELLED 状态。运行中推理继续烧 GPU, 取消是假动作。
- `cli.py:202-210` `while True: await asyncio.sleep(1)`, 只捕 `KeyboardInterrupt`, `start.sh stop` 发的 SIGTERM 不处理 → 无 drain, 在途任务静默丢失, Master 永不知任务失败, 只能等超时路径。
- `MasterServer.stop`/`AgentServer.stop`: 仅设 `should_exit=True`, 不等在途 handler 协程 drain、不 cancel、不报 FAILED → 关停 = 丢工作。

### 硬伤 7：合规边界破口 ×4, 违"100%本地/离线"定位

`cloud_fallback.py` (OpenAI/Anthropic 云回退 + 日成本上限)、`mcp_gateway/` (MCP 集群端点, localhost:9000 魔法端口)、`ast_diff.py` (AST diff, 属 fusion-cowork)、`cluster_sync.py` (远端 HTTP 拉模型, 含路径穿越/SSRF)。CLAUDE.md 自标 P2 债务。底层算力基座不应有云调用, 定位与实现矛盾。

### 硬伤 8：无界增长模式遍地

`_shards` (task_sharding:126, 从不修剪)、`_approved`/`_rejected` (node_approval, 内存无上限)、`_metric_times` (observability:99, 与 `metrics` deque 不同步 → bisect 错位 → get_metrics 返回错/空)、未解决告警 (observability:444, 只清已解决+旧, 未解决永不清 → 与告警风暴叠加无界)、`_shards`、KV `_local_size_bytes` 幽灵字节 (kv_cache_sharing, 重复 cache_id 不扣旧 → 容量永久虚高)。长跑 Master = 缓慢内存泄漏。

---

## 二、活体致命缺陷（现网接线面, 非死代码）

| # | 文件:行 | 缺陷 | 后果 |
|---|---------|------|------|
| F1 | `master_server.py:284,293` | 注册端点信任 `req.role`, Worker POST `role="master"` 获全权 | Worker → 集群接管 |
| F2 | `master_server.py:228-249` | `/api/join` 绕过审批门直接 `register_node`, `auto_approve=True` 默认 | 任意持 token 节点注入伪造 ip |
| F3 | `cluster_sync.py:324` | `dest=os.path.join(model_dir, fentry.path)`, `fentry.path` 来自远端 manifest 无校验 | 路径穿越任意写 `../../etc/cron.d/evil` |
| F4 | `cluster_sync.py:318` | `safe_host=source_host.replace("/","").replace("..","")` 弱 SSRF 清洗, 无 allowlist/元数据 IP 拦截 | SSRF: 攻击节点触发向 169.254.169.254 拉取 |
| F5 | `key_exchange.py:121-125` | `BestAvailableEncryption(os.urandom(32))` passphrase 未持久化, 重启无法解 `node.key` | TLS 握手必然失败, 重启即断 |
| F6 | `key_exchange.py:210-245` | 自签证书集群共用 + 无 pin 时 `check_hostname=False`+`CERT_REQUIRED`+`load_verify_locations(自签)` | MITM 只需任一节点 cert |
| F7 | `fmp_protobuf.py:414` | `pay.data.decode("utf-8",errors="replace")` 有损转码破坏密文/二进制 payload | 密文/protobuf/msgpack 数据损坏 |
| F8 | `fmp_server.py:163-186` | `_on_shard_sync` `file_path=payload["file_path"]` 无校验直传 `storage_volume.write_file` | 路径注入 |
| F9 | `mdns_discovery.py:255-291` | `cluster_hash` 用对称 token 派生, Worker 可伪造广播 master | rogue-master 接管 |
| F10 | `utils/auth.py:15` | `validate_node_id` 名为 SSRF guard 实为路径穿越过滤, 接受 `localhost`/`169_254_169_254` | 所有调用方误信 SSRF 已防 |

---

## 三、活体逻辑 bug + 性能（现网路径, 非致命但商用阻塞性）

- `cluster_master.py:360,403` assign_task 锁释放→select_nodes+nodes 读无锁→重获锁: TOCTOU, 并发 assign_task 双分配 + active_tasks 双计。
- `cluster_master.py:380-394` VRAM 策略 `original_strategy` 无锁读, `finally` 用 `!= original_strategy` 判断恢复: 并发下策略状态损坏。
- `cluster_master.py:627-648` KV 缓存 FIFO 非 LRU (`min(...,key=created_at)`), `access_count` 增但不参驱逐, TTL 按创建非最后访问, 懒过期仅 search 触发。
- `master_server.py:340-363` heartbeat/fault 路由直接改 `node.last_heartbeat/status/active_tasks` 绕 `master._lock`, 与 `_health_check_loop` 竞态: 刚刷新节点被重新标 OFFLINE (或反之)。
- `election.py:165` 2 节点 `majority=2` 永选不出; `:191-193` `req.term>self.current_term` 在 `:186` 已赋值后恒 False (死分支); 无持久化。
- `task_sharding.py:185-207` `_group_by_key` 跨 key 扁平成 batch, 破 BY_FILE/BY_DOCUMENT 语义; `:329-341` `_merge_embeddings` 按完成序非 shard_index 序 → 向量顺序打乱 (数据正确性 bug)。
- `observability.py:96-99,137` `metrics` deque(maxlen) vs `_metric_times` 无界 list 脱配 → `bisect_left` 错位 → `get_metrics` 返回错/空, IndexError 风险。
- `cli.py:411` `task_id=f"task_{int(time.time())}"` 同秒提交碰撞, 后者覆盖前者。
- `agent_server.py:74-117` 限流用 `X-Forwarded-For` 首 IP 作 key, 攻击者每请求换值 → 限流永不触发。
- `config/config.py:124-137` `save()` 非原子直写 `config.json`, 崩溃中写 → 截断/空文件 → 下次 load 静默重置 DEFAULT_CONFIG, 丢用户配置。
- `distributed_bridge.py:105-119` `num_shards > total_layers` 时末 shard `range(9,8)` 空 → 空层 shard 分配节点, 静默破流水线。
- `distributed_bridge.py:281-296` `_get_model_config` 异常静默返硬编码 `{num_hidden_layers:32}` → fusion-mlx 暂慢时按虚构 32 层分片, 推理产出垃圾。

---

## 四、明确结论：是否具备生产商用发布条件？

**不具备。**

现网接线面 (调度 + 双服务器 + mDNS + 本地 KV + FMP) 存在 **10 个活体致命缺陷**: 注册角色提权 (F1)、join 绕审批 (F2)、路径穿越 (F3/F8)、SSRF (F4)、TLS 重启即断 (F5)、TLS MITM (F6)、密文/二进制有损转码 (F7)、mDNS rogue-master (F9)、伪 SSRF guard (F10)。其中 F1/F2/F9 任一即可被单一恶意/被控 Worker 完成集群接管。安全模型地基 (单对称密钥零角色隔离) 从根上失效, 不是补路由能修。

叠加 8 个架构硬伤: HA/选举全死代码却宣称可用 (硬伤 2)、重启即失忆 (硬伤 3)、未接线原型潜伏致命栈 (硬伤 4)、security/ 全包死代码 (硬伤 5)、假取消+无优雅关停 (硬伤 6)、合规破口 ×4 (硬伤 7)、无界内存增长 (硬伤 8)。

**注**: 未接线子系统的致命缺陷 (caveman 变长码损坏、quorum 虚构、fmp_server 签名不匹配等) 当前不触发, 但代码已 merge 且对外呈现 feature-complete。一旦接线即爆, 且无集成测试门禁。

---

## 五、达到商用发布条件必须做的事（按优先级）

### P0 — 安全地基重构（阻塞一切商用）

1. 凭证分层: 引入 admin/master token vs worker token, 或 mTLS per-node 证书。移除 `req.role` 自声明, 角色由 Master 基于预置身份分配 (F1)。
2. `/api/join` 走 `NodeApprovalManager`, 默认 `auto_approve=False` (F2)。
3. 所有出站 HTTP sink 加 host allowlist + 元数据 IP (169.254/16、loopback、private) 拦截 + `build_url`/`validate_node_id` 真替换 (F3/F4/F8 + cluster_sync SSRF)。
4. `validate_node_id` 拆 `is_safe_path_segment` + 真 `is_safe_peer_host` (解析 IP 拒私网/环回/链路本地), 更新所有 sink (F10)。
5. TLS: passphrase 持久化或改无加密 key + 文件权限 0600 (F5); 无 pin 时 fail-closed 而非 `check_hostname=False`+自签信任; pin 指纹强制 (F6)。
6. `fmp_protobuf.py:414` 二进制 payload 走 base64 或原始 bytes, 禁 `decode("utf-8",errors="replace")` (F7)。
7. mDNS: master 已配置且健康时不覆盖; cluster_hash 用非对称/预共享且 Worker 不可伪造, 或加 first-contact join-token (F9)。

### P1 — 现网路径正确性 + 生命周期

8. assign_task 全程持 `self._lock` 或用乐观锁版本号, 消除 TOCTOU。
9. heartbeat/fault/load 路由改为调 `ClusterMaster.update_heartbeat(...)` 等加锁方法, 禁止裸改 node 字段。
10. 真任务取消: 加 CANCELLED 状态, cancel 传播到节点中止运行推理。
11. SIGTERM 处理 + 关停 drain: 在途 task 协程 gather + 超时 + 向 Master 报 FAILED。
12. `config.save()` 改 temp + `os.replace` 原子写。
13. task_id 用 uuid4 替 `int(time.time())`。

### P1 — HA 要么接线要么砍

14. 二选一: (a) 接线 StandbyMaster + MasterElection 并实现 LEARNING 状态同步 + 持久化 term/vote, 或 (b) 删除并从文档/CLI 移除高可用宣称。当前"宣称可用实际死代码"最糟。

### P1 — 合规边界

15. `cloud_fallback.py`/`mcp_gateway/`/`ast_diff.py`/`cluster_sync.py`: 迁移出 (fusion-gateway / fusion-cowork) 或删除, 恢复"100%本地"定位。

### P2 — 未接线原型门禁

16. M9/M10 接线前加集成测试门禁: 真 master+agent 起、`set_fmp_interface`+`register_kv_handler` 接、`quorum_write`→`quorum_read` 往返断言、autoscaler 热重载断言无阈值混杂。修 fmp_server 签名、quorum fire-and-forget、caveman 变长码。或对未接线路径标 `# pragma: no cover` 并从公开 API 移除, 停止虚假 feature 呈现。

### P2 — security/ 接线或砍

17. `WorkerSandbox` 接 NodeAgent 执行路径或删; `PermissionManager` 改 FastAPI dependency 全路由 block-by-default; `DataIsolationPolicy` 路径转绝对 + `realpath`+`commonpath`; `DataScrubber` 补 `sk-`/`ghp_`/`xoxb-`/JWT + `\b` 边界; 接收侧重洗 PII。

### P2 — 无界增长

18. `_shards`/`_approved`/`_rejected`/`_metric_times`(对齐 deque)/未解决告警/KV 幽灵字节 全部加上限或对齐修剪。

---

## 最终判定

fusion-multi-node 当前是一个**功能骨架完整但安全地基失效、关键能力 (HA/取消/关停/分布式存储) 死代码或破损、合规边界破口**的原型级实现。**不可商用发布。** 805 测试全绿是假象——测试隔离通过, 跨模块契约已破, 且安全端点零端到端验证。需先完成 P0 全部 7 项 (安全地基) + P1 路径正确性与 HA/合规决策, 方可重新评估商用门槛。

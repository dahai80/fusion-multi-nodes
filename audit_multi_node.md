# fusion-multi-node 内存泄漏与并发可靠性专项审计

**审计日期**: 2026-07-25
**审计对象**: ~/fusion/fusion-multi-node
**重点领域**: 高位内存泄漏、并发可靠性
**代码规模**: 31 源文件, ~4500 行核心代码

---

## 综合评级

| 维度 | 评分 (满分10) | 等级 |
|------|:---:|:---:|
| 内存泄漏风险 | 4.5 | C |
| 并发可靠性 | 4.0 | C- |
| 资源生命周期管理 | 5.0 | C+ |

**结论**: 项目存在多处高位内存泄漏风险和并发竞态缺陷。最严重的问题集中在 (1) 无界字典永不清理、(2) asyncio.Task 无引用导致任务丢失、(3) 共享可变状态无锁保护。以下按严重程度分级列出所有发现。

---

## P0 — 必须立即修复

### P0-1: ClusterMaster.tasks / nodes / kv_cache 无界增长

**文件**: `master/cluster_master.py:128-130`

```python
self.nodes: Dict[str, NodeInfo] = {}
self.tasks: Dict[str, ClusterTask] = {}
self.kv_cache: Dict[str, KVCacheEntry] = {}
```

**问题**: 三个核心字典只增不减（除 kv_cache 在 `find_kv_cache` 中附带清理外）。长期运行场景下：

- `tasks`: 已完成(FAILED/COMPLETED/TIMEOUT)的任务永远不会被移除，每个任务含 `model_shards: List[Dict]`，大模型场景下单条任务可占数 KB。按每分钟 10 个任务计，一天积累 14,400 条，一周 ~100K 条。
- `nodes`: 已离线节点只标记 `OFFLINE` 但不移除，节点 IP 动态变化时旧条目持续累积。
- `kv_cache`: `find_kv_cache` 中有附带清理，但 `register_kv_cache` 无容量限制，若注册速度超过查询速度仍会膨胀。

**影响**: Master 进程 RSS 持续增长，最终 OOM。

**修复建议**:
1. `tasks`: `_health_check_loop` 中清理已完成/超时任务（保留最近 N 条或 max_age 内的）。
2. `nodes`: `get_online_nodes()` 已标记 OFFLINE，增加定期清理 `OFFLINE` 超过阈值时间的节点。
3. `kv_cache`: 增加最大条目数限制，淘汰最久未访问。

---

### P0-2: MCPClusterGateway.requests 无界增长

**文件**: `mcp_gateway/mcp_gateway.py:64`

```python
self.requests: Dict[str, MCPRequest] = {}
```

**问题**: 每次工具调用都往 `requests` 插入一条记录（行 116），永不移除。MCPRequest 含 `arguments: Dict[str, Any]`，大量参数的场景下单条可达数 KB。作为面向 Claude 的网关，调用频率可能很高。

**影响**: 网关进程内存线性增长。

**修复建议**: 使用 `collections.deque(maxlen=...)` 或定期清理已完成/失败的请求。参考 `observability.py` 的 deque 模式。

---

### P0-3: FMPRouter._rounds 无界增长

**文件**: `protocol/fmp_router.py:61`

```python
self._rounds: Dict[str, RoundInfo] = {}
```

**问题**: `register_round()` 只增不减，`cleanup_stale_rounds()` 存在但从未被任何代码调用。每个多轮对话创建一个 RoundInfo，对话结束后条目永驻。

**影响**: 路由器进程内存持续增长。

**修复建议**: 在 `_health_check_loop` 或独立的定时任务中调用 `cleanup_stale_rounds()`。

---

### P0-4: asyncio.create_task 无引用 — 任务可能被 GC 丢弃

**文件**: 多处

| 文件 | 行号 | 代码 |
|------|------|------|
| `fmp_connection.py` | 93 | `asyncio.create_task(self._read_loop())` |
| `fmp_connection.py` | 185 | `asyncio.create_task(self._auto_reconnect())` |
| `cluster_master.py` | 316 | `asyncio.create_task(self._health_check_loop())` |
| `node_agent.py` | 339 | `asyncio.create_task(self._heartbeat_loop())` |
| `node_agent.py` | 340 | `asyncio.create_task(self._hardware_report_loop())` |
| `observability.py` | 265 | `asyncio.create_task(self._cleanup_loop())` |

**问题**: `asyncio.create_task()` 返回的 Task 对象未被保存到实例变量。Python 文档明确警告：如果对 Task 的引用未保留，它可能在执行期间被垃圾回收，导致任务静默消失。在 CPython 中由于引用计数通常不会发生，但在 PyPy 或特定 GC 策略下会触发。更实际的风险是：无法在 `stop()` 中取消这些后台任务。

**影响**: (1) 后台任务静默丢失（连接读取中断、心跳停止）；(2) `stop()` 无法优雅关闭后台协程。

**修复建议**:
```python
self._tasks: List[asyncio.Task] = []
# ...
task = asyncio.create_task(self._read_loop())
self._tasks.append(task)

async def stop(self):
    self._running = False
    for t in self._tasks:
        t.cancel()
    await asyncio.gather(*self._tasks, return_exceptions=True)
    self._tasks.clear()
```

---

### P0-5: FMPConnection._read_loop 断连后 fire-and-forget 重连

**文件**: `fmp_connection.py:183-185`

```python
self.info.is_alive = False
if self._running:
    asyncio.create_task(self._auto_reconnect())
```

**问题**: (1) `_auto_reconnect` 的 Task 无引用（同 P0-4）；(2) 若 `_running` 在 `_auto_reconnect` 循环期间变为 `False`，循环退出，但之前在 `connect()` 中又启动了新的 `_read_loop` Task，形成新旧 Task 并存。多次断连/重连后可能同时存在多个 `_read_loop` 和 `_auto_reconnect` Task，造成重复读、重复重连。

**影响**: 连接泄漏、消息重复处理、CPU 空转。

**修复建议**:
1. 在 `connect()` 中先取消旧的 `_read_loop` Task 再启动新的。
2. 用 `self._read_task` / `self._reconnect_task` 引用保存 Task，`disconnect()` 中取消。
3. `_auto_reconnect` 成功后只通过 `connect()` 中的 `_read_loop` 启动读取，不再额外启动。

---

## P1 — 应尽快修复

### P1-1: FMPConnectionManager._connections 并发读写无保护

**文件**: `protocol/fmp_connection.py:208`

```python
self._connections: Dict[str, FMPConnection] = {}
```

**问题**: `add_connection` / `remove_connection` / `broadcast` / `close_all` 均在异步上下文中操作 `_connections`，但无锁保护。虽然 Python GIL 保证单条 dict 操作原子性，但多协程交叉执行时存在逻辑竞态：

- `broadcast()` 遍历 `list(self._connections.items())` 时，另一协程可能执行 `remove_connection` → `disconnect()`，导致向已断开连接发送消息。
- `add_connection` 先检查 `node_id in self._connections`，再创建连接，存在 TOCTOU 竞态。

**影响**: 广播消息丢失或发送到已关闭连接。

**修复建议**: 使用 `asyncio.Lock` 保护 `_connections` 的增删操作，broadcast 中对每个连接的 send 已有连接状态检查，保持现状即可。

---

### P1-2: ClusterMaster 共享状态在 HTTP handler 中无锁读写

**文件**: `server/master_server.py` + `master/cluster_master.py`

**问题**: FastAPI 通过 uvicorn 在单线程事件循环中运行，但以下场景仍有风险：

- `heartbeat` handler (行 154-166) 读取并修改 `node.last_heartbeat`、`node.available_memory_gb`、`node.status`，与 `_health_check_loop` 中的 `check_timeouts()` / `get_online_nodes()` 存在竞态。例如 `_health_check_loop` 刚将节点标记 OFFLINE，heartbeat 随后又将其标记 ONLINE，导致状态翻转。
- `assign_task` (行 206-224) 修改 `task.status` 和 `node.active_tasks`，与 `complete_task` / `migrate_task` 交叉执行可能导致 `active_tasks` 计数不准。

**影响**: 节点状态抖动、任务计数偏差。

**修复建议**: 对核心状态变更路径加 `asyncio.Lock`，或使用原子操作（如 `asyncio.Event` 标记状态转换中）。

---

### P1-3: KVSharingManager._local_cache 非线程安全的 LRU 淘汰

**文件**: `distributed_mlx/kv_cache_sharing.py:81`

```python
self._local_cache: OrderedDict[str, KVCacheEntry] = OrderedDict()
```

**问题**: `store_local` 和 `lookup_local` 均修改 `_local_cache` 和 `_local_size_bytes`。若从多个协程并发调用（例如同时处理多个推理请求），存在：
- `store_local` 中 `_evict` 可能淘汰刚被 `lookup_local` 命中的条目。
- `_local_size_bytes` 的增量/减量非原子，可能累计偏差。

**影响**: 缓存容量统计偏差，极端情况缓存无限增长或误淘汰活跃条目。

**修复建议**: 加 `asyncio.Lock` 或改为单线程消费模式。

---

### P1-4: InMemoryRateLimiter 清理不及时

**文件**: `server/agent_server.py:29-62`

```python
_MAX_IP_ENTRIES = 10000
_CLEANUP_INTERVAL = 100
```

**问题**: 清理仅在每 100 次调用时触发。若短时间内大量不同 IP 请求（DDoS 或公网暴露），在达到 100 次调用前 `_counts` 字典可膨胀至远超 `_MAX_IP_ENTRIES`。清理函数本身是 O(n) 全量扫描，在 10000 条目时可能造成延迟尖峰。

**影响**: 内存尖峰 + 请求处理延迟抖动。

**修复建议**: 改为时间驱动清理（每 N 秒执行一次），而非调用次数驱动。

---

### P1-5: NodeAgent.execute_task 未 await — 协程不执行

**文件**: `server/agent_server.py:188`

```python
result = self.agent.execute_task(task)
```

**问题**: `NodeAgent.execute_task` 是 `async` 方法，但这里没有 `await`。`result` 将是一个 coroutine 对象而非实际结果，所有任务请求都会返回 `{"status": "ok", "result": <coroutine object>}`。

**影响**: 所有通过 HTTP API 提交的任务实际不执行，返回值错误。这是功能性 BUG。

**修复建议**: `result = await self.agent.execute_task(task)`

---

### P1-6: DistributedMLXBridge._active_pipelines 无界增长

**文件**: `distributed_mlx/distributed_bridge.py:63`

```python
self._active_pipelines: Dict[str, Dict[str, Any]] = {}
```

**问题**: 每次流水线推理都往 `_active_pipelines` 插入记录，pipeline_inference 完成后标记 `status=completed` 但不移除。长期运行后字典持续膨胀。

**修复建议**: 推理完成后延迟清理，或使用 deque + maxlen。

---

### P1-7: DistributedMLXBridge._shards 无界增长

**文件**: `distributed_mlx/distributed_bridge.py:62`

```python
self._shards: Dict[str, List[ModelShard]] = {}
```

**问题**: `shard_model` 只增不减，不同模型名或相同模型多次分片都会累积。

**修复建议**: 分片加载完成后清理，或限制缓存数量。

---

### P1-8: KVCacheWarmScheduler._hot_prompts 无界增长

**文件**: `distributed_mlx/kv_cache_sharing.py:290`

```python
self._hot_prompts: Dict[str, int] = {}
```

**问题**: `record_prompt` 对每个 prompt 前 100 字符做 key 计数，永不清理。若输入 prompt 高度分散（常见于推理服务），字典将无限膨胀。

**修复建议**: 增加最大条目数限制或定期重置计数器（如每小时清零）。

---

## P2 — 建议修复

### P2-1: CavemanCompressor._dictionary / _reverse_dict 永不清理

**文件**: `distributed_mlx/caveman_compress.py:43-44`

```python
self._dictionary: Dict[int, bytes] = {}
self._reverse_dict: Dict[bytes, int] = {}
```

**问题**: `build_dictionary` 根据输入 token 构建，但多次调用会覆盖而非清理旧条目。若输入 token 集合完全不同，旧条目变成无效数据占用内存。

**修复建议**: `build_dictionary` 开头先清空旧字典。

---

### P2-2: CavemanManager.total_original / total_compressed 无限累加

**文件**: `distributed_mlx/caveman_compress.py:226-227`

```python
self.total_original = 0
self.total_compressed = 0
```

**问题**: 两个计数器只增不减，长期运行后数值溢出风险（Python int 无溢出，但统计意义丧失）。`get_stats` 中 `savings_bytes` 可能为负数（若解压后比压缩前大）。

**修复建议**: 增加 `reset_stats()` 方法，或改为滑动窗口统计。

---

### P2-3: FMPConnection.send 中加密修改传入消息

**文件**: `protocol/fmp_connection.py:135-136`

```python
if self._crypto and not msg.encrypted:
    msg = self._crypto.encrypt_message(msg)
```

**问题**: `FMPCrypto.encrypt_message` 是原地修改（`msg.business.payload = encrypted_payload; msg.encrypted = True`），修改了调用者的消息对象。若同一消息需要广播到多个连接，第一个连接加密后，后续连接看到的是加密后的 payload，且 `msg.encrypted=True` 跳过加密步骤，导致发送密文。

**影响**: `FMPConnectionManager.broadcast()` 中，第一个连接成功后，后续连接发送的是加密后的密文（未再次加密，但接收方解密后得到的是密文而非明文）。

**修复建议**: `send()` 中深拷贝消息后再加密，或 `encrypt_message` 返回新对象而非原地修改。

---

### P2-4: FMPCrypto 每次 encrypt/decrypt 重新 import

**文件**: `protocol/fmp_message.py:287-289, 300-301`

```python
def encrypt(self, plaintext, aad=None):
    ...
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    ...
    aesgcm = AESGCM(self._key)
```

**问题**: 每次加密/解密都 (1) import 模块 (2) 创建新的 AESGCM 实例。import 有缓存开销小，但 AESGCM 实例每次创建是浪费。在高频消息场景下（心跳 10s + 业务消息），每秒可能创建数百个 AESGCM 对象。

**修复建议**: 在 `__init__` 中 import 并缓存 AESGCM 实例。

---

### P2-5: FMPConnection._read_loop 中 on_message 回调可能阻塞事件循环

**文件**: `protocol/fmp_connection.py:171-172`

```python
if self._on_message:
    self._on_message(msg)
```

**问题**: `_on_message` 是同步回调，若回调中执行耗时操作（如 JSON 解析大消息、同步 IO），会阻塞整个事件循环。对于消息密集场景，这会造成所有连接的消息处理延迟。

**修复建议**: 改为 `asyncio.create_task(self._on_message(msg))` 或使用 `asyncio.get_event_loop().call_soon`。

---

### P2-6: mDNS browse 使用 time.sleep 阻塞事件循环

**文件**: `discovery/mdns_discovery.py:164`

```python
time.sleep(timeout)
```

**问题**: `browse()` 方法中使用同步 `time.sleep`，会阻塞整个事件循环。`browse_async` 通过 `run_in_executor` 缓解了这个问题，但 `find_master` 直接调用 `browse()` 也是同步的。

**影响**: 若在 async 上下文中误用 `browse()` 或 `find_master()`，整个服务卡住。

**修复建议**: 所有公共 API 只暴露 async 版本，或使用 `asyncio.sleep` 重写。

---

### P2-7: TLSCertManager 私钥未加密存储

**文件**: `protocol/key_exchange.py:109-113`

```python
key_pem = key.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.TraditionalOpenSSL,
    serialization.NoEncryption(),
)
```

**问题**: RSA 私钥以明文 PEM 写入磁盘，权限 0o600 虽然限制了其他用户读取，但在多用户系统或容器环境中仍有风险。

**修复建议**: 使用密码加密私钥，或使用 Keychain 存储。

---

### P2-8: ClusterObservability._cleanup_loop 重建 deque 的开销

**文件**: `observability/observability.py:280-296`

```python
new_metrics = collections.deque(
    (m for m in self.metrics if m.timestamp > cutoff),
    maxlen=self.metrics.maxlen,
)
self.metrics = new_metrics
```

**问题**: 每 5 分钟重建整个 deque，在 10000 条目时需要遍历全部数据。虽然不是内存泄漏，但在高负载下可能造成 GC 压力（旧 deque 被整体丢弃）。

**修复建议**: 改为逐条 `popleft` 清理，避免一次性重建。

---

### P2-9: NodeAgent 每次请求 import httpx

**文件**: `agent/node_agent.py` 多处

```python
async def send_heartbeat(self):
    import httpx
    ...
async def report_hardware(self):
    import httpx
    ...
```

**问题**: 函数级 import 虽然有模块缓存，但每次调用都有 dict lookup 开销。更严重的是每次创建新的 `AsyncClient`，不复用连接池。在高频心跳（5s 间隔）场景下，每分钟创建 12+ 个 TCP 连接。

**修复建议**: 在 `__init__` 中创建 `httpx.AsyncClient` 实例并复用，`stop()` 中关闭。

---

### P2-10: FMPMessage.deserialize 不验证 payload_len 与实际数据长度一致性

**文件**: `protocol/fmp_message.py:250-266`

```python
json_bytes = data[FMP_HEADER_SIZE:FMP_HEADER_SIZE + payload_len]
d = json.loads(json_bytes.decode("utf-8"))
```

**问题**: 若 `data` 实际长度 < `FMP_HEADER_SIZE + payload_len`，切片不会报错（Python 切片越界不抛异常），而是返回短数据，后续 `json.loads` 会失败并抛出难以理解的 JSON 解析错误。

**修复建议**: 在切片前检查 `len(data) >= FMP_HEADER_SIZE + payload_len`。

---

## 并发模型总结

### 当前并发架构

```
Master:  uvicorn (单线程 event loop)
         ├── HTTP handlers (FastAPI)
         ├── _health_check_loop (asyncio.Task, 无引用)
         └── mDNS register (同步, 启动时一次)

Agent:   uvicorn (单线程 event loop)
         ├── HTTP handlers (FastAPI)
         ├── _heartbeat_loop (asyncio.Task, 无引用)
         └── _hardware_report_loop (asyncio.Task, 无引用)

FMP:     单线程 event loop
         ├── _read_loop per connection (asyncio.Task, 无引用)
         ├── _auto_reconnect per connection (asyncio.Task, 无引用)
         └── send with _send_lock per connection
```

### 并发安全矩阵

| 共享状态 | 写入方 | 读取方 | 保护机制 | 安全? |
|----------|--------|--------|----------|:---:|
| ClusterMaster.nodes | register_node, heartbeat, unregister | get_online_nodes, select_nodes | 无 | 不安全 |
| ClusterMaster.tasks | assign_task, complete_task, migrate_task | check_timeouts, get_stats | 无 | 不安全 |
| ClusterMaster.kv_cache | register_kv_cache, find_kv_cache | find_kv_cache | 无 | 部分安全(find_kv_cache内清理) |
| FMPRouter._rounds | register_round, route | cleanup_stale_rounds | 无 | 不安全 |
| FMPRouter._blocked_nodes | block_node, unblock_node | route | 无 | 安全(单线程GIL) |
| FMPConnectionManager._connections | add_connection, remove_connection | broadcast, send_to | 无 | 不安全 |
| KVSharingManager._local_cache | store_local, lookup_local | get_stats | 无 | 不安全 |
| InMemoryRateLimiter._counts | is_allowed | _cleanup_stale | 无 | 部分安全(GIL+单条原子) |
| MCPClusterGateway.requests | handle_tool_call | get_stats | 无 | 安全(单线程追加) |

### 关键风险路径

1. **心跳超时误判**: `_health_check_loop` 标记节点 OFFLINE → 同时 heartbeat handler 标记 ONLINE → 状态翻转 → 任务调度错误
2. **广播消息丢失**: `broadcast` 遍历中 `remove_connection` 被调用 → 向已关闭连接发送 → 日志报错但消息丢失
3. **KV 缓存计数偏差**: 并发 `store_local` → `_local_size_bytes` 累加不原子 → 淘汰过早或过晚
4. **FMP 消息加密破坏**: `broadcast` 调用 `send` → 第一个连接原地加密消息 → 后续连接发送密文

---

## 修复优先级路线图

| 阶段 | 问题 | 预计工作量 |
|------|------|-----------|
| **阶段1** (1-2天) | P0-4 (Task引用), P0-5 (重连竞态), P1-5 (await缺失) | 4h |
| **阶段2** (2-3天) | P0-1 (Master无界字典), P0-2 (MCP requests), P0-3 (Router rounds) | 6h |
| **阶段3** (3-5天) | P1-1 (连接池锁), P1-2 (Master状态锁), P1-3 (KV锁), P2-3 (广播加密) | 8h |
| **阶段4** (5-7天) | P1-4~P1-8, P2-1~P2-10 | 12h |

**总预估工作量**: ~30 小时

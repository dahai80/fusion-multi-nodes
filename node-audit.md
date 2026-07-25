# fusion-multi-node 最严格审计报告

**审计日期**: 2026-07-25
**审计对象**: fusion-multi-node（分布式 Apple Silicon MLX 集群编排库）
**审计版本**: v0.2.0 (commit 540f4d1)
**代码规模**: 49 个 Python 文件, 11,699 行代码, 31 个源文件, 18 个测试文件, 585 个测试用例
**审计范围**: 系统架构、可靠性、性能、代码质量、安全、内存泄漏、并发竞态、测试覆盖

---

## 综合评分

| 维度 | 评分 (满分10) | 等级 | 权重 |
|------|:---:|:---:|:---:|
| 系统架构 | 8.5 | A- | 15% |
| 可靠性 | 6.5 | B | 20% |
| 性能 | 7.0 | B+ | 15% |
| 代码质量 | 7.5 | B+ | 10% |
| 安全 | 7.0 | B+ | 15% |
| 内存泄漏 | 6.0 | C+ | 10% |
| 并发竞态 | 5.5 | C | 10% |
| 测试覆盖 | 8.0 | A- | 5% |
| **综合加权** | **7.06** | **B+** | 100% |

**结论**: 这是一个设计良好、模块化清晰的分布式系统库。架构层面表现出色，测试覆盖率优秀。但在并发安全、内存管理和生产级可靠性方面存在显著短板，不建议直接用于生产环境，需要针对 P0/P1 问题进行修复。

---

## 1. 系统架构审计 (8.5/10, A-)

### 1.1 优点

**模块化清晰**: 9 个顶级模块各司其职，依赖方向单一
- `master/` → 调度核心，无下游依赖
- `agent/` → 节点代理，依赖 master/config
- `distributed_mlx/` → 分布式推理桥
- `mcp_gateway/` → Claude 集成入口
- `protocol/` → FMP 通信协议层（独立）
- `server/` → FastAPI HTTP 服务层
- `discovery/` → mDNS 零配置发现
- `observability/` → 监控/告警
- `config/` → 全局配置

**分层设计合理**:
- 协议层 (`protocol/`) 与业务层 (`master/agent/`) 解耦
- FMP 协议采用 Link/Business/Control 三层封装，符合 OSI 思想
- HTTP 服务层独立，可替换为其他 RPC 框架

**设计模式运用得当**:
- Circuit Breaker 模式（熔断器）
- Strategy 模式（Caveman 自动选择压缩算法）
- Observer 模式（告警处理器）
- LRU 缓存模式（KV 共享）

### 1.2 问题

**[P2] 缺少领域边界与依赖注入**
- `NodeAgent.execute_task()` 直接通过 `httpx` 调用 fusion-mlx，与外部服务强耦合
- `DistributedMLXBridge` 硬编码 `localhost:8000` 作为 fusion-mlx 端点
- 建议引入 `Backend` 抽象接口，便于 Mock 测试与多后端支持

**[P2] Master 单点故障无 HA 方案**
- `ClusterMaster` 为全局唯一实例，进程崩溃即全集群失联
- 缺少 Master 选举（Raft/Paxos）或主备切换机制
- 文档中未提及 HA 策略

**[P3] 模块间循环依赖风险**
- `master/cluster_master.py` 在运行时延迟导入 `server.MasterServer` 和 `discovery.MDNSDiscovery`，规避了循环依赖，但设计上是倒置的：核心调度不应感知服务层
- 建议采用依赖反转，由 `server` 层持有 `master` 引用并主动注册

### 1.3 架构图（基于代码还原）

```
┌─────────────────────────────────────────────────────────┐
│                    Claude Desktop/Code                   │
└────────────────────────┬────────────────────────────────┘
                         │ MCP Protocol
┌────────────────────────▼────────────────────────────────┐
│                  MCPClusterGateway (9756)                │
│         工具聚合 · 节点路由 · Token 预算管理              │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP
┌────────────────────────▼────────────────────────────────┐
│              MasterServer (9753) ← ClusterMaster         │
│     节点注册 · 任务调度 · 故障迁移 · KV 池 · 健康检查    │
└─────────┬──────────────────────────────┬────────────────┘
          │ FMP Protocol                 │ mDNS Discovery (9754)
          │ (TCP 长连接 + AES-GCM)       │
┌─────────▼──────────────────────────────▼────────────────┐
│                   NodeAgent (9755)                       │
│  硬件采集 · 心跳上报 · 任务执行 → fusion-mlx (8000)      │
│  KV 缓存共享 (LRU + Caveman 压缩)                        │
└──────────────────────────────────────────────────────────┘
```

---

## 2. 可靠性审计 (6.5/10, B)

### 2.1 优点

- **Circuit Breaker 实现完整**: CLOSED → OPEN → HALF_OPEN 三态机正确，含失败计数、恢复超时、探测放行
- **心跳机制健全**: 15s 超时阈值，10s 健康检查循环，自动标记 OFFLINE
- **任务超时处理**: `check_timeouts()` 周期性扫描，TIMEOUT 状态独立
- **FMP 连接自动重连**: 3s 重连间隔，5 次重试上限

### 2.2 问题

**[P0] `assign_task` 内存泄漏 + 任务永不清理**
`cluster_master.py:218` `self.tasks[task.task_id] = task` 永远不会被删除。`complete_task()` 仅改状态，不清理。长期运行下 `self.tasks` 无界增长。

```python
# cluster_master.py:226 — 只改状态，不删除
def complete_task(self, task_id: str, error: str = "") -> None:
    task = self.tasks.get(task_id)
    if not task:
        return
    task.status = TaskStatus.COMPLETED if not error else TaskStatus.FAILED
    # ❌ 缺少: del self.tasks[task_id] 或定时清理
```

**[P0] Master 心跳超时后节点不被剔除**
`get_online_nodes()` 仅将超时节点状态改为 OFFLINE，但 `self.nodes` 字典永不删除。僵尸节点长期占用调度候选位。

**[P1] 任务迁移后状态不一致**
`migrate_task()` 先设 `MIGRATED` 再重置为 `PENDING` 并调用 `assign_task()`。若 `assign_task` 失败（节点不足），任务停留在 `PENDING` 但 `assigned_nodes` 为空，无后续重试机制。

**[P1] `_estimate_memory` 过于粗糙**
仅按模型名包含 `"70b"/"13b"` 等字符串估算，未读取实际模型配置。实际内存需求可能被严重低估或高估。

**[P2] HTTP 客户端未复用连接池**
全代码库中 `httpx.AsyncClient` 每次请求都新建实例：
```python
# node_agent.py:253, distributed_bridge.py:139 等多处
async with httpx.AsyncClient(timeout=120.0) as client:
    resp = await client.post(...)
```
每次请求都建立新 TCP 连接，无连接复用，高并发下性能损耗显著。

**[P2] `_active_pipelines` 字典无清理**
`distributed_bridge.py:63` `self._active_pipelines` 在流水线完成后仅改状态，永不删除，长期运行内存泄漏。

**[P3] 缺少幂等性保证**
`register_node` 重复注册会覆盖原节点信息，无版本号/时间戳冲突检测。网络抖动下可能丢失最新心跳。

---

## 3. 性能审计 (7.0/10, B+)

### 3.1 优点

- **异步 IO 全面采用**: `asyncio` 贯穿 master/agent/server/bridge，非阻塞设计
- **数据并行正确使用 `asyncio.gather`**: `distributed_bridge.py:231` 批量推理并行化
- **Caveman 压缩按链路自适应**: Thunderbolt 用 dict（轻量），Ethernet 用 zlib/diff，节省 40-60% 带宽
- **KV 缓存 LRU 淘汰**: `OrderedDict` + `_evict()` 实现正确

### 3.2 问题

**[P1] `lookup_local` 线性扫描全表**
`kv_cache_sharing.py:110` 对 `_local_cache` 做 O(n) 遍历查找 `model_name + prompt_hash`。缓存条目多时严重拖慢查询。应建立二级索引 `(model_name, prompt_hash) → cache_id`。

**[P2] `MDNSDiscovery.browse()` 同步阻塞**
`mdns_discovery.py:164` `time.sleep(timeout)` 阻塞事件循环。虽然提供了 `browse_async` 用 `run_in_executor` 包装，但 `find_master` 同步版本仍可能被误用。

**[P2] Observability 指标查询全表扫描**
`observability.py:98` `get_metrics` 遍历全部 `self.metrics`（上限 10000 条）做过滤。无时间索引，高频查询下 CPU 开销大。

**[P3] `data_parallel_inference` 节点分配不均衡**
`distributed_bridge.py:227` `nodes[i % len(nodes)]` 在 `len(prompts) < len(nodes)` 时部分节点空闲，未做负载感知分配。

**[P3] 序列化使用 JSON**
FMP 消息 `serialize()` 使用 `json.dumps`，对于二进制张量数据效率低下。应考虑 MessagePack/Protobuf。

---

## 4. 代码质量审计 (7.5/10, B+)

### 4.1 优点

- **零 TODO/FIXME/HACK**: 代码库无遗留标记，说明开发纪律良好
- **零 bare except**: 所有异常捕获都有具体处理
- **命名规范一致**: 类 PascalCase，函数 snake_case，常量 UPPER_CASE
- **Dataclass 广泛使用**: 数据模型清晰，符合现代 Python 风格
- **日志规范**: 全程 `logging.getLogger(__name__)`，无 print（仅 1 处违规）

### 4.2 问题

**[P1] ruff 检出 65 个错误**
其中 58 个可自动修复，主要是：
- `E741` 模糊变量名 `l`（network_topology.py 多处）
- 未使用导入
- 行长度超限

**[P1] mypy 未配置/未运行**
项目无 `mypy.ini` 或 `[tool.mypy]` 配置，CI 未强制类型检查。多个模块返回 `Dict[str, Any]` 失去类型保护。

**[P2] 类型标注不完整**
关键方法缺少返回类型标注：
- `ClusterMaster.register_node()` → `None` 未标注
- `KVSharingManager.store_local()` → `bool` 已标注但参数类型宽松

**[P2] 1 处 print 违反约定**
`config/config.py:71` 使用 `print` 而非 `logger`：
```python
except Exception as e:
    print(f"加载配置失败: {e}")  # ❌ 应使用 logger
    self._data = dict(self.DEFAULT_CONFIG)
```

**[P3] 魔法数字过多**
```python
# cluster_master.py:272 — 基础内存 2.0，模型加 4.0，70b 加 32.0 等
base = 2.0
if task.model_name:
    base += 4.0
    if "70b" in task.model_name.lower():
        base += 32.0
```
应提取为 `MODEL_MEMORY_PROFILE` 常量表。

---

## 5. 安全审计 (7.0/10, B+)

### 5.1 优点

- **Bearer Token 认证**: `BearerAuthMiddleware` 使用 `secrets.compare_digest` 防时序攻击
- **Token 文件权限 0o600**: `auth.py:47` 正确设置
- **SSRF 防护**: `sanitize_node_url_part()` + `SAFE_NODE_ID_PATTERN` 正则校验
- **TLS 自签名证书**: RSA 2048 + SHA256，ECDHE+AESGCM 密码套件
- **AES-GCM 加密**: FMP 消息加密使用 AEAD，防篡改
- **输入白名单**: `ALLOWED_TASK_TYPES`, `ALLOWED_EXTRA_KEYS` 限制用户输入

### 5.2 问题

**[P0] TLS 证书校验形同虚设**
`key_exchange.py:151` `ctx.check_hostname = False`，且 `load_verify_locations(cert_path)` 加载的是**自己的**证书而非 CA。任何持有任意证书的节点都能加入集群，中间人攻击无防护。

```python
def get_client_ssl_context(self):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_cert_chain(cert_path, key_path)
    ctx.load_verify_locations(cert_path)  # ❌ 加载自己证书，非 CA
    ctx.check_hostname = False             # ❌ 关闭主机名校验
    ctx.verify_mode = ssl.CERT_REQUIRED    # ✅ 但只校验签名链，自签名=无校验
```

**[P1] `EXEMPT_PATHS` 健康端点泄露信息**
`auth.py:70` `/api/health` 免认证，`HealthResponse` 暴露 `node_id` 和 `uptime_seconds`，攻击者可探测集群规模与运行时长。

**[P1] Cluster Secret 哈希仅取前 16 字符**
`mdns_discovery.py:26` `_hash_cluster_secret` 用 SHA256 但只取 hexdigest[:16]（64 位熵）。虽够用，但 `cluster_hash` 明文放入 mDNS properties 广播，局域网内可被抓包。

**[P2] `AgentServer` 速率限制仅按 IP**
`agent_server.py:117` `InMemoryRateLimiter` 单机内存计数，多 Agent 节点无法共享限流状态，且 `defaultdict(list)` 对每 IP 维护时间戳列表，DDoS 攻击下 IP 数量爆炸导致内存耗尽。

**[P2] `InMemoryRateLimiter` 内存泄漏**
`agent_server.py:29` `self._counts: Dict[str, List[float]] = defaultdict(list)` 永远不清理已过期 IP 的条目，长期运行内存增长。

**[P3] HTTP 路径拼接未统一校验**
多处 `f"http://{node_id}:{port}/..."` 中 `node_id` 来自网络，虽有 `sanitize_node_url_part` 但调用点不统一（`node_agent.py` 内部调用未 sanitize `master_host`）。

---

## 6. 内存泄漏审计 (6.0/10, C+)

### 6.1 确认的内存泄漏

| # | 位置 | 严重度 | 描述 |
|---|------|:---:|------|
| 1 | `cluster_master.py:218` `self.tasks` | **P0** | 任务完成后永不删除，长期运行无界增长 |
| 2 | `cluster_master.py:128` `self.nodes` | **P0** | OFFLINE 节点永不剔除，僵尸节点堆积 |
| 3 | `cluster_master.py:130` `self.kv_cache` | **P1** | 仅在 `find_kv_cache` 被调用时懒清理，无主动 GC |
| 4 | `mcp_gateway.py:116` `self.requests` | **P0** | MCP 请求记录永不清理，高频调用下爆炸 |
| 5 | `observability.py:61` `self.metrics` | **P2** | 虽有 `_max_metrics=10000` 上限和 `_cleanup_loop`，但 `metrics = metrics[-self._max_metrics:]` 切片会短暂双倍内存 |
| 6 | `observability.py:62` `self.logs` | **P2** | 同上，`_max_logs=50000` |
| 7 | `observability.py:62` `self.alerts` | **P1** | **无上限、无清理**，告警历史永久累积 |
| 8 | `distributed_bridge.py:63` `self._active_pipelines` | **P1** | 流水线完成后仅改状态，永不删除 |
| 9 | `agent_server.py:29` `InMemoryRateLimiter._counts` | **P2** | 过期 IP 条目不清理 |
| 10 | `fmp_router.py:61` `self._rounds` | **P3** | 有 `cleanup_stale_rounds` 但**从未被调用** |

### 6.2 资源未释放

**[P1] `asyncio.create_task` 未持有引用**
多处 `asyncio.create_task(...)` 未保存返回的 Task 对象：
```python
# master/cluster_master.py:316
asyncio.create_task(self._health_check_loop())
# agent/node_agent.py:339-341
asyncio.create_task(self._heartbeat_loop())
asyncio.create_task(self._hardware_report_loop())
# observability/observability.py:268
asyncio.create_task(self._cleanup_loop())
# protocol/fmp_connection.py:93,185
asyncio.create_task(self._read_loop())
asyncio.create_task(self._auto_reconnect())
```
Python 文档明确警告：未保存引用的 Task 可能被垃圾回收中途取消，导致静默失败。应改为：
```python
self._health_task = asyncio.create_task(self._health_check_loop())
```

**[P2] `NodeAgent.stop()` 不取消后台任务**
`stop()` 仅设 `self._running = False`，但 `_heartbeat_loop`/`_hardware_report_loop` 正在 `await asyncio.sleep` 中，需等待下一个循环才退出，期间可能触发已关闭资源的访问。

### 6.3 无界资源清单

| 资源 | 上限 | 清理机制 | 风险 |
|------|:---:|:---:|------|
| `ClusterMaster.tasks` | ❌ 无 | ❌ 无 | 内存爆炸 |
| `ClusterMaster.nodes` | ❌ 无 | ❌ 无 | 僵尸节点堆积 |
| `ClusterMaster.kv_cache` | ❌ 无 | ⚠️ 懒清理 | 依赖调用频率 |
| `MCPClusterGateway.requests` | ❌ 无 | ❌ 无 | 内存爆炸 |
| `ClusterObservability.alerts` | ❌ 无 | ❌ 无 | 内存爆炸 |
| `ClusterObservability.metrics` | 10000 | ✅ 周期清理 | 低 |
| `ClusterObservability.logs` | 50000 | ✅ 周期清理 | 低 |
| `KVSharingManager._local_cache` | 4096MB | ✅ LRU 淘汰 | 低 |
| `InMemoryRateLimiter._counts` | ❌ 无 | ❌ 无 | DDoS 下爆炸 |
| `FMPRouter._rounds` | ❌ 无 | ⚠️ 有清理但未调用 | 长期泄漏 |

---

## 7. 并发与竞态审计 (5.5/10, C)

### 7.1 严重问题

**[P0] 共享可变状态无锁保护**
`ClusterMaster` 的 `nodes`/`tasks`/`kv_cache` 字典被多个协程并发读写，**全程无 `asyncio.Lock`**：

```python
# master/cluster_master.py
class ClusterMaster:
    def __init__(self, ...):
        self.nodes: Dict[str, NodeInfo] = {}      # ❌ 无锁
        self.tasks: Dict[str, ClusterTask] = {}   # ❌ 无锁
        self.kv_cache: Dict[str, KVCacheEntry] = {} # ❌ 无锁

    async def _health_check_loop(self):           # 后台协程 A
        while self._running:
            self.check_timeouts()                 # 写 self.tasks
            online = len(self.get_online_nodes()) # 写 self.nodes (改 status)

    # 同时 MasterServer 的 HTTP handler 在另一个协程 B 调用:
    # - register_node()   写 self.nodes
    # - assign_task()     写 self.tasks, self.nodes
    # - complete_task()   写 self.tasks, self.nodes
```

虽然 CPython 的 GIL 保证单条字典操作原子性，但 **多步复合操作不原子**：
- `get_online_nodes()` 遍历 `self.nodes` 同时修改 `node.status`，若另一协程在遍历中 `register_node()` 插入新节点，`RuntimeError: dictionary changed size during iteration`
- `assign_task()` 先 `select_nodes`（读 nodes）再写 `task.assigned_nodes`，期间节点可能已被 `check_heartbeat` 标记 OFFLINE，导致任务分配到已死节点

**[P0] `MCPClusterGateway.handle_tool_call` 竞态**
`total_token_count` 的"检查-更新"非原子：
```python
if self.total_token_count >= self.token_budget:  # 协程 A 读
    return {"error": "Token budget exhausted"}
# ... 协程 A 执行中, 协程 B 也通过了检查
self.total_token_count += estimated_tokens       # 两协程都写, 超额
```

### 7.2 中等问题

**[P1] `FMPConnection.send` 锁粒度问题**
`fmp_connection.py:133` `async with self._send_lock` 保护写入，但 `is_connected` 检查在锁外，两个协程可能同时通过检查并竞争 `writer`。

**[P1] `FMPConnection._read_loop` 与 `_auto_reconnect` 竞态**
`_read_loop` 退出时 `asyncio.create_task(self._auto_reconnect())`，若 `_running` 在此期间被 `disconnect()` 设为 False，重连循环会立即退出，但若时序相反，会启动一个无人看管的重连任务。

**[P2] `Observability._alert_handlers` 列表在迭代时可能被修改**
`observability.py:174` `for handler in self._alert_handlers:` 遍历期间若另一协程 `on_alert()` 注册新处理器，可能引发并发修改异常。

### 7.3 改进建议

1. 为 `ClusterMaster` 引入 `asyncio.Lock` 保护 `nodes`/`tasks`/`kv_cache` 的复合操作
2. MCP token 计数改用 `asyncio.Semaphore` 或原子操作
3. 所有 `asyncio.create_task` 保存引用并在 `stop()` 中 `task.cancel()` + `await task`
4. 警警处理器列表改为不可变快照：`for handler in list(self._alert_handlers)`

---

## 8. 测试覆盖审计 (8.0/10, A-)

### 8.1 数据概览

| 指标 | 数值 |
|------|:---:|
| 测试用例总数 | 585 |
| 测试文件数 | 18 |
| 测试通过率 | 100% (585/585) |
| 测试运行时间 | 24.57s |
| 警告数 | 4 (mock 未 await) |
| 测试/源文件比 | 0.58 (18/31) |
| 平均每测试文件用例数 | 32.5 |

### 8.2 各模块测试分布

| 模块 | 测试数 | 评价 |
|------|:---:|------|
| network_topology | 72 | ✅ 优秀 |
| cli | 58 | ✅ 优秀 |
| observability | 44 | ✅ 优秀 |
| cluster_master | 42 | ✅ 优秀 |
| fmp_connection | 39 | ✅ 良好 |
| caveman_compress | 38 | ✅ 良好 |
| core | 35 | ✅ 良好 |
| kv_cache_sharing | 34 | ✅ 良好 |
| node_agent | 30 | ⚠️ 一般 |
| master_server | 30 | ⚠️ 一般 |
| distributed_bridge | 28 | ⚠️ 一般 |
| mdns_discovery | 26 | ⚠️ 一般 |
| fmp_protocol | 25 | ⚠️ 一般 |
| fmp_router | 23 | ⚠️ 一般 |
| mcp_gateway | 19 | ⚠️ 一般 |
| agent_server | 19 | ⚠️ 一般 |
| config | 12 | ⚠️ 一般 |
| utils | 11 | ⚠️ 一般 |

### 8.3 问题

**[P1] 缺少并发竞态测试**
585 个测试中无 `asyncio.Lock` 竞态测试、无高并发压测。`ClusterMaster` 的共享状态无任何并发安全测试覆盖。

**[P1] 缺少内存泄漏测试**
无 `tracemalloc` 或 `objgraph` 集成，长期运行的内存增长未被验证。

**[P2] mock 未 await 警告 (4 处)**
`test_network_topology.py` 中 `AsyncMock` 返回的协程未被 await，反映测试对异步语义的覆盖不严谨。

**[P2] 测试覆盖不均**
`utils` (11) 和 `config` (12) 测试偏少，`mcp_gateway`/`agent_server` (各19) 对关键路径覆盖不足。

---

## 9. 综合问题优先级清单

### P0 — 必须立即修复 (生产阻断)

1. **`ClusterMaster.tasks` 无界增长** → 添加完成任务的 TTL 清理或上限淘汰
2. **`ClusterMaster.nodes` 僵尸节点堆积** → OFFLINE 节点超时后 `del`
3. **`MCPClusterGateway.requests` 无界增长** → 添加请求历史的环形缓冲或定期清理
4. **`ClusterMaster` 共享状态无锁** → 引入 `asyncio.Lock` 保护复合操作
5. **MCP token 计数竞态** → 改用原子操作或信号量
6. **TLS 证书校验形同虚设** → 实现 CA 签名或指纹 pinning，移除 `check_hostname=False`
7. **`asyncio.create_task` 未持有引用** → 全部改为保存引用

### P1 — 高优先级 (1 周内修复)

8. `Observability.alerts` 无上限无清理
9. `distributed_bridge._active_pipelines` 无清理
10. `assign_task` 失败后任务停留在 PENDING 无重试
11. `FMPRouter._rounds` 清理函数从未被调用
12. HTTP 客户端未复用连接池
13. `lookup_local` 线性扫描全表
14. ruff 65 个错误需修复
15. mypy 类型检查未配置
16. 缺少并发竞态测试
17. 缺少内存泄漏测试

### P2 — 中优先级 (迭代内修复)

18. `_estimate_memory` 过于粗糙
19. `MDNSDiscovery.browse()` 同步阻塞
20. Observability 指标查询全表扫描
21. `EXEMPT_PATHS` 健康端点泄露信息
22. `InMemoryRateLimiter` 内存泄漏
23. Master 单点故障无 HA 方案
24. 缺少领域边界与依赖注入
25. 类型标注不完整
26. 1 处 print 违反约定
27. 测试覆盖不均

### P3 — 低优先级 (技术债务)

28. `data_parallel_inference` 节点分配不均衡
29. 序列化使用 JSON
30. 魔法数字过多
31. HTTP 路径拼接未统一校验
32. 模块间循环依赖风险
33. 缺少幂等性保证
34. mock 未 await 警告

---

## 10. 修复路线图建议

### 阶段一: 紧急修复 (1-2 天)
- 修复全部 P0 内存泄漏（tasks/nodes/requests 上限 + 清理）
- 为 `ClusterMaster` 添加 `asyncio.Lock`
- 保存所有 `asyncio.create_task` 引用并在 `stop()` 中取消

### 阶段二: 安全加固 (3-5 天)
- 重写 TLS 证书校验：实现集群 CA 或指纹 pinning
- 健康端点脱敏：移除 node_id/uptime 暴露
- 速率限制器添加过期清理

### 阶段三: 可靠性提升 (1 周)
- 任务迁移失败后引入重试队列
- Master 心跳超时节点主动剔除
- `_estimate_memory` 接入真实模型配置

### 阶段四: 性能优化 (1 周)
- HTTP 连接池复用（`httpx.AsyncClient` 单例化）
- KV 缓存建立二级索引
- Observability 指标添加时间索引

### 阶段五: 工程化 (持续)
- 配置 mypy 并纳入 CI
- 修复全部 ruff 错误
- 添加并发竞态测试与内存泄漏测试
- 文档补充 HA 策略与运维指南

---

## 附录 A: 审计方法论

本次审计采用**分层穿透 + 多维度交叉验证**方法：

1. **静态分析**: ruff (65 errors), grep 模式扫描 (TODO/bare except/print/dangerous calls)
2. **动态验证**: pytest 全量运行 (585 passed in 24.57s)
3. **架构重建**: 通过 `list_symbols`/`read_file` 还原模块依赖图
4. **竞态推演**: 逐行分析 async 协程的共享状态访问路径
5. **内存追踪**: 枚举所有 `Dict`/`List` 集合的上限与清理机制
6. **安全审计**: 认证/授权/注入/密钥/证书五维扫描

**审计局限性**:
- 未进行模糊测试 (fuzzing)
- 未进行实际多节点集群压测
- 未审查依赖供应链 (SBOM)
- 未评估 macOS 特定权限模型交互

---

## 附录 B: 评分标准

| 分数 | 等级 | 含义 |
|:---:|:---:|------|
| 9-10 | A+ | 业界领先，可作为参考实现 |
| 8-8.9 | A/A- | 优秀，生产就绪 |
| 7-7.9 | B+ | 良好，修复 P1 后可用 |
| 6-6.9 | B/C+ | 合格，存在显著风险 |
| 5-5.9 | C | 不合格，需重大改进 |
| <5 | D/F | 严重缺陷，不可用 |

---

**报告生成**: AtomCode (GLM-5.2)
**审计耗时**: 23 轮交互
**文件输出**: `/Users/dahai/fusion/fusion-multi-node/node-audit.md`

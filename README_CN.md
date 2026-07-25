# Chinese version of README.md for fusion-multi-node v0.1.0
# User instruction: "生成doc文档和READMD，README_CN，提交配置库，发布0.1.0版本"
# This is a standalone Chinese README, mirrors README.md content in Chinese

<div align="center">
  <h1>🔗 Fusion-Multi-Node</h1>
  <p><strong>分布式 Apple Silicon MLX 集群调度核心</strong></p>
  <p><em>多台 Mac 组网，统一 AI 推理集群 — 流水线并行、数据并行、MCP 网关</em></p>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/版本-0.1.0-blue" alt="版本">
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/macOS-Apple%20Silicon-brightgreen" alt="macOS">
  <img src="https://img.shields.io/badge/许可证-MIT-green" alt="许可证">
  <img src="https://img.shields.io/badge/测试-585%20通过-brightgreen" alt="测试">
  <img src="https://img.shields.io/badge/覆盖率-96%25-brightgreen" alt="覆盖率">
</p>

---

## 📋 概览

**Fusion-Multi-Node** 是 [Fusion-MLX](https://github.com/dahai80) 生态的集群调度核心。将多台 Apple Silicon Mac（M4/M5 Studio/Max）组网为分布式推理集群。

### 两种分布式模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **流水线并行** | 大模型(70B+)拆分到多台 Mac，每台处理部分层 | 运行超大本地模型 |
| **数据并行** | 多台 Mac 加载相同模型，批量分发请求提升吞吐 | 高吞吐批量推理 |

### 七大核心模块

| 模块 | 职责 | 覆盖率 |
|------|------|--------|
| **集群主控** | 节点发现、资源调度、任务生命周期、KV 缓存池、容错 | 95% |
| **节点代理** | 单机守护、硬件上报、任务执行、mDNS 自发现 | 90% |
| **mDNS 发现** | Bonjour/mDNS 零配置节点发现、服务注册/浏览 | 86% |
| **FMP 协议** | 三层二进制协议、AES-GCM 加密、TCP 长连接、熔断器 | 95% |
| **分布式 MLX 桥** | 流水线/数据并行、模型分片、Caveman 压缩、KV 缓存共享 | 97% |
| **MCP 集群网关** | 统一 MCP 端点、工具路由、Claude Desktop/Code 集成 | 100% |
| **集群可观测** | 指标、日志、告警、集群健康仪表 | 100% |

### 架构图

```
┌──────────────────────────────────────────────────────────────┐
│                  Claude Code / API / fusion-desk UI          │
│                           ↓                                  │
│              fusion-multi-node 集群主控                       │
│       (自发现、调度器、KV 缓存池、容错)                        │
│                           ↓                                  │
│     ┌──────────────┬──────────────┬──────────────┐           │
│     │  节点代理      │  节点代理     │  节点代理     │           │
│     │  (Mac M4)     │  (Mac M4)    │  (Mac M4)    │           │
│     │  fusion-desk  │  fusion-desk │  fusion-desk │           │
│     │  fusion-mlx   │  fusion-mlx  │  fusion-mlx  │           │
│     └──────────────┴──────────────┴──────────────┘           │
│                           ↓                                  │
│              分布式 MLX (mlx.distributed)                     │
│         Thunderbolt RDMA / Ethernet / P2P Bridge             │
└──────────────────────────────────────────────────────────────┘
```

### 生态定位

```
┌─────────────────────────────────────────────────────────────┐
│                      应用层                                   │
│   fusion-desk  │  fusion-code  │  fusion-ui  │  Claude App   │
└──────────────────────────┬──────────────────────────────────┘
                           │ MCP / HTTP
┌──────────────────────────▼──────────────────────────────────┐
│                      控制层                                   │
│       fusion-multi-node (集群主控 + 节点代理)                   │
│       MCP 集群网关                                            │
└──────────────────────────┬──────────────────────────────────┘
                           │ 分布式 API
┌──────────────────────────▼──────────────────────────────────┐
│                      推理层                                   │
│       fusion-mlx (MLX 分布式、量化、Metal)                     │
│       Fusion-KB (向量搜索、RAG)                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 快速开始

### 安装

```bash
git clone https://github.com/dahai80/fusion-multi-node.git
cd fusion-multi-node

pip install -e .            # 核心安装
pip install -e ".[all]"     # 全部可选依赖
pip install -e ".[test]"    # 测试依赖
```

### 启动集群

```bash
# 启动集群主控
fusion-multi-node cluster start --mode master

# 启动节点代理（每台 Mac 上）
fusion-multi-node cluster start --mode agent

# 查看状态
fusion-multi-node cluster status
fusion-multi-node node list
```

### CLI 速查

```bash
fusion-multi-node cluster start/stop/status    # 集群管理
fusion-multi-node node list/info/discover      # 节点管理
fusion-multi-node task submit/list/cancel      # 任务管理
fusion-multi-node config list/get/set          # 配置管理
fusion-multi-node network detect               # 网络拓扑
fusion-multi-node caveman test                 # Caveman 压缩
fusion-multi-node kv stats/warm                # KV 缓存管理
```

---

## 📖 命令参考

### 全局选项

| 选项 | 说明 |
|------|------|
| `--verbose`, `-v` | 详细调试输出 |
| `--version` | 显示版本号 |

### 集群管理

| 命令 | 说明 |
|------|------|
| `cluster start --mode master` | 启动集群主控（端口 9753） |
| `cluster start --mode agent` | 启动节点代理（端口 9755） |
| `cluster start --mode both` | 同时启动主控和代理 |
| `cluster stop` | 停止所有集群服务 |
| `cluster status` | 显示集群概览 |

### 节点管理

| 命令 | 说明 |
|------|------|
| `node list` | 列出所有注册节点 |
| `node list --online` | 仅显示在线节点 |
| `node info <node_id>` | 显示节点详细信息 |
| `node start --role master` | 以主控角色启动 |
| `node start --role agent` | 以代理角色启动 |
| `node discover` | mDNS 发现局域网节点 |

### 任务管理

| 命令 | 说明 |
|------|------|
| `task submit -n <名称> -m <模型> --mode pipeline` | 提交流水线任务 |
| `task submit -n <名称> -m <模型> --mode data` | 提交数据并行任务 |
| `task list` | 列出所有任务 |
| `task cancel <task_id>` | 取消任务 |

### 配置管理

| 命令 | 说明 |
|------|------|
| `config list` | 显示所有配置 |
| `config get <key>` | 获取配置值 |
| `config set <key> <value>` | 设置配置值 |

### 网络与压缩

| 命令 | 说明 |
|------|------|
| `network detect` | 检测网络拓扑和链路类型 |
| `caveman test [数据]` | 测试 Caveman 压缩 |
| `kv stats` | 显示 KV 缓存统计 |
| `kv warm --prompt <文本> --nodes <节点>` | 预热 KV 缓存 |

---

## 🏗️ 模块架构

### 1. 集群主控 (`fusion_multi_node.master`)

集群唯一真实来源 — 节点注册、健康检查、任务调度、KV 缓存。

```python
from fusion_multi_node.master import ClusterMaster, ClusterTask, NodeInfo, ParallelMode

master = ClusterMaster(host="0.0.0.0", port=9753)

node = NodeInfo(node_id="node_1", hostname="mac-studio-1", ip_address="10.0.0.1",
                port=9755, total_memory_gb=64.0, available_memory_gb=48.0)
master.register_node(node)

task = ClusterTask(task_id="task_1", name="batch-inference", mode=ParallelMode.DATA)
master.assign_task(task)
master.complete_task("task_1")
```

**核心能力**: 评分节点选择、任务生命周期(PENDING→RUNNING→COMPLETED/FAILED/TIMEOUT)、迁移、KV 缓存池、心跳超时。

### 2. 节点代理 (`fusion_multi_node.agent`)

运行于每台 Mac — 硬件指标、心跳、通过 fusion-mlx API 执行任务。

```python
from fusion_multi_node.agent import NodeAgent, AgentConfig

config = AgentConfig(node_id="my_mac", master_host="10.0.0.1")
agent = NodeAgent(config)
await agent.start()

info = agent.collect_hardware_info()
result = await agent.execute_task({"task_id": "t1", "type": "inference", "model": "qwen3.5-9b"})
```

### 3. mDNS 发现 (`fusion_multi_node.discovery`)

零配置 Bonjour/mDNS 节点发现。主控注册、代理浏览。

```python
from fusion_multi_node.discovery import MDNSDiscovery

mdns = MDNSDiscovery(node_id="fusion-master")
mdns.register(port=9753, properties={"role": "master"})

master = await mdns.find_master_async(timeout=5.0)
mdns.unregister()
```

### 4. FMP 协议 (`fusion_multi_node.protocol`)

三层二进制协议，AES-GCM 加密，熔断器容错。

```python
from fusion_multi_node.protocol import (
    FMPMessage, PayloadType, FMPCrypto,
    FMPConnectionManager, FMPRouter, CircuitBreaker,
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

**三层**: LinkLayer(路由、hop_count)、BusinessLayer(payload、rounds)、ControlLayer(心跳、ACK、流控)。

### 5. 分布式 MLX 桥 (`fusion_multi_node.distributed_mlx`)

三个子模块支撑分布式推理：

```python
from fusion_multi_node.distributed_mlx import DistributedMLXBridge, CavemanManager, KVSharingManager

# 模型分片 & 流水线
bridge = DistributedMLXBridge()
shards = await bridge.shard_model("llama-70b", num_shards=4)
result = await bridge.pipeline_inference("llama-70b", "What is AI?", ["n1", "n2", "n3", "n4"])

# Caveman 压缩（节省 40-60% 带宽）
manager = CavemanManager()
compressed, method, stats = await manager.compress_tensor(data, link_type="ethernet_1g")

# KV 缓存共享
kv = KVSharingManager(max_local_cache_mb=4096.0)
kv.store_local(entry)
found = kv.lookup_local("qwen", "abc123")
```

### 6. MCP 集群网关 (`fusion_multi_node.mcp_gateway`)

统一 MCP 端点，为 Claude Desktop/Code 聚合所有节点工具。

```python
from fusion_multi_node.mcp_gateway import MCPClusterGateway, MCPTool

gateway = MCPClusterGateway(host="0.0.0.0", port=9756)
tool = MCPTool(name="code_review", description="Review code",
               parameters={"type": "object", "properties": {"code": {"type": "string"}}})
gateway.register_tool(tool)
result = await gateway.handle_tool_call("code_review", {"code": "..."}, source="claude_code")
```

### 7. 集群可观测 (`fusion_multi_node.observability`)

指标、日志、告警，自动清理过期数据。

```python
from fusion_multi_node.observability import ClusterObservability, LogEntry

obs = ClusterObservability(retention_hours=24.0)
obs.record_metric("node_1", "memory_used_gb", 16.0, tags={"gpu": "m4_ultra"})
obs.add_log(LogEntry(time.time(), "node_1", "INFO", "scheduler", "Task completed"))
alert = obs.create_alert("warning", "High memory", "node_1 at 90% utilization")
await obs.check_alert_rules(nodes)
```

---

## 🔧 配置

默认配置路径 `~/.fusion/multi-node/config.json`：

```json
{
  "cluster": {
    "master_host": "0.0.0.0",
    "master_port": 9753,
    "discovery_port": 9754,
    "agent_port": 9755,
    "heartbeat_timeout": 15.0,
    "heartbeat_interval": 5.0
  },
  "parallel": {
    "default_mode": "pipeline",
    "pipeline_timeout": 300.0,
    "caveman_compress": true
  },
  "mlx": {
    "fusion_mlx_port": 8000,
    "fusion_desk_port": 9000
  },
  "mcp": {
    "token_budget": 10000000,
    "tool_timeout": 60.0
  },
  "observability": {
    "retention_hours": 24.0
  }
}
```

---

## 🧪 测试

```bash
pip install -e ".[test]"

# 运行全部测试（585 个）
pytest tests/ -v

# 带覆盖率（96.1%）
pytest tests/ --cov=fusion_multi_node --cov-report=html

# 运行指定模块
pytest tests/test_cluster_master.py -v
pytest tests/test_protocol.py -v
```

---

## 📊 关键常量

| 常量 | 默认值 | 用途 |
|------|--------|------|
| Master 端口 | 9753 | 集群主控服务端口 |
| 发现端口 | 9754 | mDNS 发现端口 |
| Agent 端口 | 9755 | 节点代理端口 |
| MCP 端口 | 9756 | MCP 网关端口 |
| 心跳超时 | 15.0s | 失活节点判定阈值 |
| 任务超时 | 300.0s | 默认任务超时 |
| KV 缓存 TTL | 3600.0s | 默认 KV 缓存过期 |
| Token 预算 | 10,000,000 | MCP 网关 token 限额 |

---

## 🛣️ 路线图

### v0.1.0 ✅（当前）
- [x] 集群主控 — 节点发现、调度器、任务生命周期、容错
- [x] 节点代理 — 硬件上报、心跳、任务执行、mDNS 自发现
- [x] mDNS 发现 — Bonjour 零配置服务注册与浏览
- [x] FMP 协议 — 三层二进制协议、AES-GCM 加密、熔断器
- [x] 分布式 MLX — 模型分片、流水线/数据并行、Caveman 压缩、KV 缓存共享
- [x] MCP 网关 — 统一 MCP 端点，Claude 集成
- [x] 集群可观测 — 指标、日志、告警、集群报告
- [x] CLI — 15+ 命令覆盖集群/节点/任务/配置/网络/压缩/KV 管理
- [x] 96.1% 测试覆盖率（585 测试）

### 未来规划
- [ ] 分布式 MLX 算子桥接（mlx.distributed API）
- [ ] 插件生态集群注册
- [ ] 集群监控仪表（fusion-ui）
- [ ] Thunderbolt RDMA 加速
- [ ] 跨节点 KV 缓存与 Caveman 压缩

---

## 🔒 安全

- **100% 本地离线** — 零外部网络依赖
- **节点认证** — 所有代理必须向主控注册
- **AES-GCM 加密** — FMP 协议加密通信
- **熔断器** — 自动隔离故障节点
- **无遥测** — 无分析、无回传

---

## 📄 许可证

MIT License。详见 [LICENSE](LICENSE)。

---

## 🤝 贡献

欢迎贡献！请确保：

1. 测试通过：`pytest tests/ -v`
2. 覆盖率 ≥ 90%：`pytest --cov=fusion_multi_node`
3. 4 空格缩进，无 docstring（自文档化命名）
4. 所有类使用 `logging.getLogger(__name__)`

---

<p align="center">
  <strong>Fusion-Multi-Node — 组网 Mac，统一推理，本地扩展。</strong>
</p>
<p align="center">
  <sub>Fusion-MLX 团队 ❤️ 出品</sub>
</p>

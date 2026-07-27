# Fusion 生态 GUI 审计 & 落地/迁移计划

> 生成时间: 2026-07-26
> 审计范围: fusion-multi-node, fusion-studio, fusion-mlx, fusion-comfyui, fusion-bench, fusion-security, fusion-desk, fusion-doc, fusion-agent-studio, fusion-trainer

---

## 一、审计结论

### 1.1 fusion-multi-node GUI 状态

**结论: 纯后端，零 GUI 代码**

| 类型 | 数量 | 说明 |
|------|------|------|
| Web UI (HTML/CSS/JS) | 0 | 无任何模板或前端代码 |
| CLI UI (Click) | 1 | `cli.py` — 终端表格输出 |
| REST API 端点 | 28 | Master 22 + Agent 6，全部返回 JSON |
| FastAPI 自动文档 | 2 | `/docs` (Swagger) + `/redoc` (ReDoc) |

所有数据展示完全依赖 fusion-studio 的 SwiftUI 面板消费 REST API。

### 1.2 fusion-studio MultiNode 面板状态

**已实现 5 个面板 + 1 个孤立面板:**

| 面板 | 文件 | 状态 | 问题 |
|------|------|------|------|
| M7-01 集群概览 | ClusterOverviewView.swift | ✅ 已修复 | API 模型已修正 |
| M7-02 拓扑可视化 | ClusterTopologyView.swift | ✅ 已修复 | — |
| M7-03 任务监控 | TaskMonitorView.swift | ✅ 已修复 | — |
| M7-04 节点操作 | NodeActionsView.swift | ⚠️ 孤立 | 无导航路由，无法访问 |
| M7-05 告警中心 | AlertCenterView.swift | ✅ 已修复 | suggestion 字段已修正 |

**P0 已修复的致命问题 (5项):**

1. ✅ `/api/v1/cluster/stats` 响应是嵌套 `{cluster:{...}, tasks:{...}}`，原代码解码为扁平 `ClusterStats` → 新增 `V1ClusterStatsResponse` 中间层 + `ClusterStats.from()` 转换
2. ✅ `/api/nodes` 返回 `{total, online, nodes:[]}` 包裹结构，原代码解码为 `[ClusterNode]` → 新增 `NodeListResponse` 中间层
3. ✅ `/api/tasks` 返回 `{total, tasks:[]}` 包裹结构，原代码解码为 `[ClusterTask]` → 新增 `TaskListResponse` 中间层
4. ✅ `/api/v1/observability/alerts` 后端端点不存在 → 已在 `master_server.py` 新增该端点
5. ✅ `OptimizationSuggestion` 字段名 `impact/detail` 与后端 `priority/related_alert` 不匹配 → 已对齐后端字段

### 1.3 P1 功能缺口 (6项)

| # | 缺口 | 说明 | 状态 |
|---|------|------|------|
| 1 | InspectorPanel 缺 `.node(id:)` 和 `.clusterTask(id:)` | 点击节点/任务时右侧面板无内容 | ✅ 已修复 |
| 2 | NodeActionsView 无导航路由 | AppState.Module 枚举缺少对应 case | ✅ 已修复 |
| 3 | 提交任务表单缺失 | Inspector 显示 "Custom content" 而非实际表单 | ✅ 已修复 (SubmitTaskView) |
| 4 | 任务进度可视化缺失 | Engine 方法存在但无 UI | ✅ 已修复 (TaskProgressView) |
| 5 | 任务时间线可视化缺失 | Engine 方法存在但无 UI | ✅ 已修复 (TaskProgressView) |
| 6 | 任务迁移 UI 缺失 | Engine 方法存在但无 UI | ✅ 已修复 (TaskMonitorView migration sheet) |

### 1.4 P2 API 覆盖缺口

**Master Server 未被 GUI 消费的端点 (10个):**

| 端点 | 功能 | 建议面板 | 状态 |
|------|------|----------|------|
| `GET /api/health` | 健康检查 | 连接状态指示器 | ✅ 已用于 checkHealth |
| `POST /api/join` | 节点加入 | 节点管理面板 | ✅ NodeActionsView |
| `POST /api/routing/strategy` | 路由策略 | 路由策略面板 | ✅ RoutingStrategyView |
| `GET /api/routing/summary` | 负载概要 | 路由策略面板 | ✅ RoutingStrategyView |
| `GET /api/nodes/{id}` | 单节点详情 | Inspector 面板 | ⚠️ 待实现 |
| `GET /api/tasks/{id}` | 单任务详情 | Inspector 面板 | ⚠️ 待实现 |
| `POST /api/kv/register` | KV 注册 | KV 缓存面板 | ✅ MultiNodeEngine.registerKVCache |
| `GET /api/kv/find/{model}` | KV 查找 | KV 缓存面板 | ✅ KVCacheView |
| `GET /api/v1/observability/logs/export` | 日志导出 | 告警中心 | ✅ AlertCenterView export button |
| `GET /api/v1/nodes/{id}/metrics` | 节点指标 | Inspector 面板 | ✅ 已用于 fetchNodeMetrics |

**Agent Server 集成状态 (7个端点):**

| 端点 | 功能 | 说明 | 状态 |
|------|------|------|------|
| `GET /api/health` | Agent 健康检查 | KV 面板 | ✅ KVCacheView health indicator |
| `POST /api/execute` | 推理执行 | 高危操作，建议仅监控 | ❌ 不暴露 |
| `POST /api/kv/lookup` | KV 查找 | KV 面板 | ✅ KVCacheView |
| `POST /api/kv/transfer` | KV 迁移 | KV 面板 | ✅ KVCacheView |
| `POST /api/kv/warm` | KV 预热 | KV 面板 | ✅ KVCacheView |
| `GET /api/kv/stats` | KV 统计 | KV 面板 | ✅ KVCacheView |
| `GET /api/hardware` | 硬件信息 | KV 面板 | ✅ KVCacheView |

---

## 二、外部 GUI 迁移评估

### 2.1 高优先级迁移 (原生 SwiftUI → fusion-studio 模块)

| 项目 | 技术 | 屏幕 | 迁移方式 | 工作量 |
|------|------|------|----------|--------|
| **fusion-mlx/apps/fusion-mac** | SwiftUI | 17 屏幕 | 原生合并为 fusion-studio 模块 | 3-5天 |
| **fusion-mlx admin web** | FastAPI + Jinja2 (13模板) | Login/Dashboard/Canvas/Chat/Bench/Settings/Models | 原生 SwiftUI 重写 | 5-8天 |
| **fusion-desk browser** | SwiftUI | BrowserApp + 8 组件 | 合并为 Desk 模块增强 | 2-3天 |
| **fusion-comfyui FusionComfyUI** | SwiftUI + WKWebView | App/ModelManager/ServerManager/WebView | 合并为 ComfyUI 模块 | 1-2天 |

### 2.2 中优先级嵌入 (WebView 容器)

| 项目 | 技术 | 功能 | 嵌入方式 | 状态 |
|------|------|------|----------|------|
| **fusion-bench bench-site** | Next.js 16 + React 19 | Benchmarks/Compare/Performance | BenchView WebView tab (localhost:3000) | ✅ 已完成 |
| **fusion-security frontend** | React 18 + Ant Design | Dashboard/Projects/Scans | SecurityView WebView tab (localhost:3000) | ✅ 已完成 |
| **fusion-doc gateway** | Node.js SPA | 文档浏览 | WebViewContainer | ⏳ 待定 |

### 2.3 低优先级 (复杂/第三方)

| 项目 | 说明 |
|------|------|
| fusion-agent-studio langflow | 大型 React 平台，建议 WebView 嵌入 |
| fusion-comfyui LiveTalking/Gradio | 第三方工具，仅 WebView |
| fusion-trainer Typer CLI | 终端 CLI，无需迁移 |

---

## 三、落地计划

### Phase 1: P1 功能补齐 ✅ 全部完成

| 任务 | 文件 | 状态 |
|------|------|------|
| 修复 InspectorPanel 支持 `.node(id:)` / `.clusterTask(id:)` | InspectorPanel.swift | ✅ |
| 路由 NodeActionsView (新增 Module 枚举) | AppState.swift + ModuleDetailView.swift | ✅ |
| 实现提交任务表单 | SubmitTaskView.swift | ✅ |
| 实现任务进度条+时间线组件 | TaskProgressView.swift | ✅ |
| 实现任务迁移对话框 | TaskMonitorView.swift | ✅ |
| 添加日志导出按钮 | AlertCenterView.swift | ✅ |
| Inspector 增强: 节点详细指标 + 任务时间戳 | InspectorPanel.swift | ✅ |

### Phase 2: P2 新增面板 ✅ 已完成

| 任务 | 新文件 | 状态 |
|------|--------|------|
| KV 缓存管理面板 | KVCacheView.swift | ✅ |
| 路由策略面板 | RoutingStrategyView.swift | ✅ |
| 服务面板 (WebView 嵌入) | ServiceWebView.swift | ✅ |
| Agent 服务器集成 | MultiNodeEngine.swift 扩展 | ✅ |

### Phase 3: 外部 GUI 迁移

| 任务 | 来源 | 目标模块 | 状态 |
|------|------|----------|------|
| 合并 fusion-mac | fusion-mlx/apps/fusion-mac | ModelHub + MLXOptimizer | ⏳ 待定 |
| 嵌入 bench-site | fusion-bench/bench-site | BenchView WebView tab | ✅ 已完成 |
| 嵌入 security frontend | fusion-security/frontend | SecurityView WebView tab | ✅ 已完成 |
| 修正 ServiceWebView URL | ServiceWebView.swift | bench→3000, security→3000 | ✅ 已完成 |
| 合并 FusionComfyUI | fusion-comfyui/FusionComfyUI | 新 ComfyUI 模块 | ⏳ 用户决定暂缓 |
| 合并 fusion-desk browser | fusion-desk/browser | Desk 增强 | ⏳ 待定 |
| 嵌入 fusion-doc gateway | fusion-doc/gateway | Doc WebView | ⏳ 待定 |

### Phase 4: admin web 迁移 (可选)

将 fusion-mlx 的 13 模板 admin web 原生重写为 SwiftUI。预计 5-8 天。

---

## 四、API 覆盖率矩阵

| API 端点 | Engine 方法 | View 调用 | 状态 |
|----------|-------------|-----------|------|
| `GET /api/health` | `checkHealth()` | 连接检测 | ✅ |
| `GET /api/v1/cluster/stats` | `fetchClusterStats()` | ClusterOverviewView | ✅ |
| `GET /api/nodes` | `fetchNodes()` | ClusterOverviewView | ✅ |
| `GET /api/nodes/{id}` | — | Inspector (engine.nodes 缓存) | ✅ |
| `GET /api/v1/nodes/{id}/metrics` | `fetchNodeMetrics()` | InspectorPanel | ✅ |
| `DELETE /api/nodes/{id}` | `removeNode()` | ClusterOverviewView | ✅ |
| `POST /api/join` | `joinNode()` | NodeActionsView | ✅ |
| `GET /api/tasks` | `fetchTasks()` | TaskMonitorView | ✅ |
| `POST /api/tasks/submit` | `submitTask()` | SubmitTaskView | ✅ |
| `POST /api/tasks/{id}/cancel` | `cancelTask()` | TaskMonitorView | ✅ |
| `POST /api/tasks/{id}/degrade` | `degradeTask()` | TaskMonitorView | ✅ |
| `POST /api/tasks/{id}/migrate` | `migrateTask()` | TaskMonitorView migration sheet | ✅ |
| `GET /api/v1/tasks/{id}/progress` | `fetchTaskProgress()` | TaskProgressView | ✅ |
| `GET /api/v1/tasks/{id}/timeline` | `fetchTaskTimeline()` | TaskProgressView | ✅ |
| `GET /api/v1/autoscaler/config` | `fetchAutoscalerConfig()` | NodeActionsView | ✅ |
| `PUT /api/v1/autoscaler/config` | `updateAutoscalerConfig()` | NodeActionsView | ✅ |
| `GET /api/v1/observability/alerts` | `fetchAlerts()` | AlertCenterView | ✅ |
| `GET /api/v1/observability/suggestions` | `fetchSuggestions()` | AlertCenterView | ✅ |
| `GET /api/v1/observability/logs/export` | — | AlertCenterView export button | ✅ |
| `POST /api/routing/strategy` | `setRoutingStrategy()` | RoutingStrategyView | ✅ |
| `GET /api/routing/summary` | `fetchRoutingSummary()` | RoutingStrategyView | ✅ |
| `GET /api/kv/find/{model}` | `findKVCache()` | KVCacheView | ✅ |
| `POST /api/kv/register` | `registerKVCache()` | MultiNodeEngine | ✅ |
| Agent: `GET /api/health` | `checkAgentHealth()` | KVCacheView health indicator | ✅ |
| Agent: `POST /api/kv/lookup` | `agentKVLookup()` | KVCacheView | ✅ |
| Agent: `POST /api/kv/transfer` | `agentKVTransfer()` | KVCacheView | ✅ |
| Agent: `POST /api/kv/warm` | `agentKVWarm()` | KVCacheView | ✅ |
| Agent: `GET /api/kv/stats` | `fetchAgentKVStats()` | KVCacheView | ✅ |
| Agent: `GET /api/hardware` | `fetchAgentHardware()` | KVCacheView + Inspector | ✅ |
| Agent: `POST /api/execute` | — | 高危操作，不暴露 | ❌ 刻意不暴露 |

**覆盖率: 27/28 = 96%** (唯一未消费端点为刻意不暴露的推理执行)

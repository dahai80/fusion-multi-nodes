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

| # | 缺口 | 说明 |
|---|------|------|
| 1 | InspectorPanel 缺 `.node(id:)` 和 `.clusterTask(id:)` | 点击节点/任务时右侧面板无内容 |
| 2 | NodeActionsView 无导航路由 | AppState.Module 枚举缺少对应 case |
| 3 | 提交任务表单缺失 | Inspector 显示 "Custom content" 而非实际表单 |
| 4 | 任务进度可视化缺失 | Engine 方法存在但无 UI |
| 5 | 任务时间线可视化缺失 | Engine 方法存在但无 UI |
| 6 | 任务迁移 UI 缺失 | Engine 方法存在但无 UI |

### 1.4 P2 API 覆盖缺口

**Master Server 未被 GUI 消费的端点 (10个):**

| 端点 | 功能 | 建议面板 |
|------|------|----------|
| `GET /api/health` | 健康检查 | 连接状态指示器 (已用于 checkHealth) |
| `POST /api/join` | 节点加入 | 节点管理面板 |
| `POST /api/routing/strategy` | 路由策略 | 路由策略面板 |
| `GET /api/routing/summary` | 负载概要 | 集群概览 |
| `GET /api/nodes/{id}` | 单节点详情 | Inspector 面板 |
| `GET /api/tasks/{id}` | 单任务详情 | Inspector 面板 |
| `POST /api/kv/register` | KV 注册 | KV 缓存面板 |
| `GET /api/kv/find/{model}` | KV 查找 | KV 缓存面板 |
| `GET /api/v1/observability/logs/export` | 日志导出 | 告警中心 |
| `GET /api/v1/nodes/{id}/metrics` | 节点指标 | Inspector 面板 |

**Agent Server 完全未集成 (7个端点):**

| 端点 | 功能 | 说明 |
|------|------|------|
| `GET /api/health` | Agent 健康检查 | 需连接 Agent 9755 端口 |
| `POST /api/execute` | 推理执行 | 高危操作，建议仅监控 |
| `POST /api/kv/lookup` | KV 查找 | KV 面板 |
| `POST /api/kv/transfer` | KV 迁移 | KV 面板 |
| `POST /api/kv/warm` | KV 预热 | KV 面板 |
| `GET /api/kv/stats` | KV 统计 | KV 面板 |
| `GET /api/hardware` | 硬件信息 | Inspector 面板 |

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

| 项目 | 技术 | 功能 | 嵌入方式 | 工作量 |
|------|------|------|----------|--------|
| **fusion-bench bench-site** | Next.js 16 + React 19 | Benchmarks/Compare/Performance | WebViewContainer | 0.5天 |
| **fusion-security frontend** | React 18 + Ant Design | Dashboard/Projects/Scans | WebViewContainer | 0.5天 |
| **fusion-doc gateway** | Node.js SPA | 文档浏览 | WebViewContainer | 0.5天 |

### 2.3 低优先级 (复杂/第三方)

| 项目 | 说明 |
|------|------|
| fusion-agent-studio langflow | 大型 React 平台，建议 WebView 嵌入 |
| fusion-comfyui LiveTalking/Gradio | 第三方工具，仅 WebView |
| fusion-trainer Typer CLI | 终端 CLI，无需迁移 |

---

## 三、落地计划

### Phase 1: P1 功能补齐 (当前)

| 任务 | 文件 | 预计工时 |
|------|------|----------|
| 修复 InspectorPanel 支持 `.node(id:)` / `.clusterTask(id:)` | InspectorPanel.swift | 2h |
| 路由 NodeActionsView (新增 Module 枚举) | AppState.swift + ModuleDetailView.swift | 0.5h |
| 实现提交任务表单 (Inspector 内) | 新建 SubmitTaskInspectorView.swift | 2h |
| 实现任务进度条组件 | 新建 TaskProgressView.swift | 1.5h |
| 实现任务时间线组件 | 新建 TaskTimelineView.swift | 2h |
| 实现任务迁移对话框 | TaskMonitorView.swift 修改 | 1h |
| 添加日志导出按钮 | AlertCenterView.swift 修改 | 0.5h |

### Phase 2: P2 新增面板

| 任务 | 新文件 | 预计工时 |
|------|--------|----------|
| KV 缓存管理面板 | KVCachesView.swift | 4h |
| 路由策略面板 | RoutingStrategyView.swift | 2h |
| 节点加入对话框 | NodeJoinView.swift | 1.5h |
| Agent 服务器集成 | MultiNodeEngine.swift 扩展 | 3h |

### Phase 3: 外部 GUI 迁移

| 任务 | 来源 | 目标模块 | 预计工时 |
|------|------|----------|----------|
| 合并 fusion-mac | fusion-mlx/apps/fusion-mac | ModelHub + MLXOptimizer | 3-5天 |
| 嵌入 bench-site | fusion-bench/bench-site | Bench WebView | 0.5天 |
| 嵌入 security frontend | fusion-security/frontend | SafetyView WebView | 0.5天 |
| 合并 FusionComfyUI | fusion-comfyui/FusionComfyUI | 新 ComfyUI 模块 | 1-2天 |
| 合并 fusion-desk browser | fusion-desk/browser | Desk 增强 | 2-3天 |
| 嵌入 fusion-doc gateway | fusion-doc/gateway | Doc WebView | 0.5天 |

### Phase 4: admin web 迁移 (可选)

将 fusion-mlx 的 13 模板 admin web 原生重写为 SwiftUI。预计 5-8 天。

---

## 四、API 覆盖率矩阵

| API 端点 | Engine 方法 | View 调用 | 状态 |
|----------|-------------|-----------|------|
| `GET /api/health` | `checkHealth()` | 连接检测 | ✅ |
| `GET /api/v1/cluster/stats` | `fetchClusterStats()` | ClusterOverviewView | ✅ |
| `GET /api/nodes` | `fetchNodes()` | ClusterOverviewView | ✅ |
| `GET /api/nodes/{id}` | — | — | ❌ 待 Inspector |
| `GET /api/v1/nodes/{id}/metrics` | `fetchNodeMetrics()` | Context menu | ✅ |
| `DELETE /api/nodes/{id}` | `removeNode()` | ClusterOverviewView | ✅ |
| `POST /api/join` | `joinNode()` | — | ❌ 待 UI |
| `GET /api/tasks` | `fetchTasks()` | TaskMonitorView | ✅ |
| `POST /api/tasks/submit` | `submitTask()` | — | ❌ 待表单 |
| `POST /api/tasks/{id}/cancel` | `cancelTask()` | TaskMonitorView | ✅ |
| `POST /api/tasks/{id}/degrade` | `degradeTask()` | TaskMonitorView | ✅ |
| `POST /api/tasks/{id}/migrate` | `migrateTask()` | — | ❌ 待 UI |
| `GET /api/v1/tasks/{id}/progress` | `fetchTaskProgress()` | — | ❌ 待 UI |
| `GET /api/v1/tasks/{id}/timeline` | `fetchTaskTimeline()` | — | ❌ 待 UI |
| `GET /api/v1/autoscaler/config` | `fetchAutoscalerConfig()` | NodeActionsView | ✅ |
| `PUT /api/v1/autoscaler/config` | `updateAutoscalerConfig()` | NodeActionsView | ✅ |
| `GET /api/v1/observability/alerts` | `fetchAlerts()` | AlertCenterView | ✅ 新增 |
| `GET /api/v1/observability/suggestions` | `fetchSuggestions()` | AlertCenterView | ✅ |
| `GET /api/v1/observability/logs/export` | `exportLogs()` | — | ❌ 待按钮 |
| `POST /api/routing/strategy` | `setRoutingStrategy()` | — | ❌ 待 UI |
| `GET /api/routing/summary` | — | — | ❌ 待集成 |
| `GET /api/kv/find/{model}` | — | — | ❌ 待 KV 面板 |
| `POST /api/kv/register` | — | — | ❌ 待 KV 面板 |
| Agent: `GET /api/health` | — | — | ❌ 待集成 |
| Agent: `GET /api/kv/stats` | — | — | ❌ 待 KV 面板 |
| Agent: `GET /api/hardware` | — | — | ❌ 待 Inspector |

**覆盖率: 12/28 = 43%** → Phase 1+2 完成后目标 85%+

# fusion-multi-node web

多节点集群状态 UI — Union Web Shell 模块（PRD
`architecture/fusion-union-web-prd-1015.md` N-8）。展示节点拓扑、节点服务
状态、跨节点资源用量与任务概览，数据来自集群 Master API。

纯静态 SPA（React 18 + Vite + TypeScript + Tailwind + Bun）。自身不含任何
鉴权逻辑：内嵌模式走 gateway shell 会话，独立开发模式仅接受 `VITE_API_KEY`
开发便利令牌。

## 页面

| 路由 | 标题 | 内容 |
|------|------|------|
| `#/topology` | 拓扑 | Leader 卡（epoch / leader_id / cluster_view），按角色分组的节点卡（heavy/general/worker），集群统计瓦片 |
| `#/nodes` | 节点 | 可排序节点表（状态、角色、内存、活跃任务、score、drivers），点行进入节点详情 |
| `#/nodes/:id` | 节点详情 | 硬件 KV 面板、资源瓦片、派发到该节点的近期任务 |
| `#/metrics` | 指标 | Master Prometheus 暴露的实时 gauge + 本地采样时序（在线节点、运行/待派任务、内存） |

## 本地开发

```bash
bun install
bun run dev        # vite dev server 端口 11472，/api 代理到 http://127.0.0.1:11452
```

环境变量：

- `FUSION_MULTINODE_WEB_PORT` — dev/preview 端口（默认 11472，已登记
  `architecture/port-registry.yaml`）
- `FUSION_DEV_BACKEND` — dev 代理指向的 Master API（默认
  `http://127.0.0.1:11452`，可指向远端 master）
- `VITE_API_KEY` — 独立开发模式的集群令牌（仅开发便利；内嵌模式不需要）

先启动 master（仓库根目录 `./start.sh start`），`/api/*` 才有响应。
`bun test` 覆盖纯逻辑（Prometheus 解析、状态映射、格式化）；`bun run build`
先跑 typecheck 再打包。

## 内嵌（Shell）模式

gateway 将 `dist/` 托管在 `/shell/fusion-multi-nodes/`，模块由
`manifest.json` 描述（id `fusion-multi-nodes`，category `platform`，
devices `desktop` + `mobile-readonly`）。内嵌模式（`?embedded=1` 或 iframe）
下模块隐藏自带底部导航、显示顶部页签；窄视口显示 mobile-readonly 横幅。
鉴权走 gateway 会话，产物内不携带任何密钥。

## 构建

```bash
bun run build      # typecheck + vite build → dist/
bun run lint       # biome check src
bun test           # bun test runner
```

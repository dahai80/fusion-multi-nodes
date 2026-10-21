# fusion-multi-node web

Cluster status UI for fusion-multi-node — a Union Web shell module (PRD
`architecture/fusion-union-web-prd-1015.md` N-8). Shows node topology,
per-node service status, cross-node resource usage and task overview,
backed by the cluster master API.

Pure static SPA (React 18 + Vite + TypeScript + Tailwind + Bun). No auth
logic of its own: embedded mode rides the gateway shell session, standalone
dev mode accepts a `VITE_API_KEY` dev convenience token.

## Pages

| Route | Title | Content |
|-------|-------|---------|
| `#/topology` | Topology | Leader card (epoch / leader_id / cluster_view), node cards grouped by role (heavy/general/worker), cluster stat tiles |
| `#/nodes` | Nodes | Sortable node table (status, role, memory, active tasks, score, drivers), row click → node detail |
| `#/nodes/:id` | Node detail | Hardware KV panel, resource tiles, recent tasks dispatched to the node |
| `#/metrics` | Metrics | Live gauges from the master Prometheus exposition + locally sampled time series (nodes online, tasks running/pending, memory) |

## Development

```bash
bun install
bun run dev        # vite dev server on 11472, /api proxied to http://127.0.0.1:11452
```

Environment overrides:

- `FUSION_MULTINODE_WEB_PORT` — dev/preview port (default 11472, registered in
  `architecture/port-registry.yaml`)
- `FUSION_DEV_BACKEND` — master API base for the dev proxy (default
  `http://127.0.0.1:11452`; point at a remote master if needed)
- `VITE_API_KEY` — cluster token for standalone dev auth (dev convenience only;
  never required in embedded mode)

Start a master first (`./start.sh start` at the repo root) so `/api/*`
responds. `bun test` covers the pure logic (Prometheus parsing, status
mapping, formatting); `bun run build` runs typecheck then bundles.

## Embedded (shell) mode

The gateway serves `dist/` under `/shell/fusion-multi-nodes/` and the module
is described by `manifest.json` (id `fusion-multi-nodes`, category
`platform`, devices `desktop` + `mobile-readonly`). In embedded mode
(`?embedded=1` or iframe) the module hides its own bottom navigation and
shows a minimal top tab bar; on narrow viewports it surfaces the
mobile-readonly banner. Auth is the gateway session — the bundle carries no
secret.

## Build

```bash
bun run build      # typecheck + vite build → dist/
bun run lint       # biome check src
bun test           # bun test runner
```

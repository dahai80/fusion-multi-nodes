---
doc_id: fusion-multi-node/preview-stack
product: fusion-multi-node
class: core
entry: service
doc_type: guide
title: {zh: "自动化客户预览栈", en: "Automated Customer Preview Stack"}
audience: engineer
status: draft
version: 0.20.8
updated: 2026-11-09
owner: @dahai
source_of_truth:
  - deliver/preview/preview-stack.sh
  - deliver/preview/docker-compose.preview.yml
  - deliver/preview/synthetic_seed.py
---

# Automated Customer Preview Stack (issue #108)

Companion to fusion-cowork#422 (match-node). Owner mandate: every pipeline
node automated, no manual demo fallback.

One command brings up an isolated demo stack for "try existing project":

```
./deliver/preview/preview-stack.sh up [project-dir]
```

Pipeline (fully automated, no human steps):

1. compose preview stack — base `docker-compose.yml` + preview overlay,
   isolated project (`fusion-preview`), host ports 21452 / 21472 so it
   coexists with a delivery stack; runs **without** fusion-mlx (agent
   healthcheck downgraded to liveness; demo data needs no inference).
2. synthetic demo data injection — deterministic fixtures seeded into the
   cluster session store (`preview-demo-*`, titled `[SYNTHETIC DEMO]`).
   Never real customer data (business/AGENTS.md rule 6 red line).
3. health-check gate — master `/api/health` must report `ok` before the
   URL is issued; failure auto-tears the stack down.
4. preview URL + notification — prints `http://127.0.0.1:21472/`; if
   `FUSION_PREVIEW_NOTIFY_CMD` is set it receives the URL as `$1`
   (fusion-cowork cow#404 email channel plugs in here).
5. TTL auto-teardown — `FUSION_PREVIEW_TTL_MINUTES` (default 120; 0
   disables); detached watcher runs `compose down -v` and removes state.
   Immediate: `./deliver/preview/preview-stack.sh teardown`.

Configuration (env):

| Variable | Default | Meaning |
|----------|---------|---------|
| `FUSION_PREVIEW_TTL_MINUTES` | 120 | auto-teardown delay |
| `FUSION_PREVIEW_NOTIFY_CMD` | — | command receiving preview URL |
| `FUSION_PREVIEW_MASTER_PORT` | 21452 | host port, master API |
| `FUSION_PREVIEW_WEB_PORT` | 21472 | host port, web console |
| `FUSION_PREVIEW_PROJECT` | fusion-preview | compose project name |

terminal-mac path is out of scope for this feature (per #108).

Requires a one-time `deliver/ubuntu/bring-up.sh` run in the project dir to
create `.env` (cluster token + fusion-core checkout) and the prebuilt image.

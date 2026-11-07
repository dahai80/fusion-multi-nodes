---
doc_id: fusion-multi-node/changelog
product: fusion-multi-node
class: core
entry: service
doc_type: changelog
title: {zh: "fusion-multi-node 更新日志", en: "fusion-multi-node Changelog"}
audience: engineer
status: draft
version: 0.20.5
updated: 2026-11-07
owner: "@dahai"
source_of_truth:
  - docs/CHANGELOG.md
  - pyproject.toml
---

# Changelog — fusion-multi-node

The full, per-version history lives in [`docs/CHANGELOG.md`](docs/CHANGELOG.md) (Keep a Changelog 1.1.0 format, SemVer). This root file is the contract-facing index; update `docs/CHANGELOG.md` first, then mirror the latest entry here.

## [0.20.5] - 2026-11-03 — Resident web console server on :11472 (#95)

### Fixed
- Resident `web/dist` static server on `:11472` with `/api` reverse proxy to the master; fixes backend_unavailable for the union web menu module (`start.sh start-web` / `stop-web`).

See [`docs/CHANGELOG.md`](docs/CHANGELOG.md) for earlier releases (0.x → 0.20.5).

---
doc_id: fusion-multi-node/delivery
product: fusion-multi-node
class: core
entry: service
doc_type: guide
title: {zh: "fusion-multi-node 交付包", en: "fusion-multi-node Delivery Packages"}
audience: engineer
status: draft
version: 0.20.7
updated: 2026-11-09
owner: @dahai
source_of_truth:
  - deliver/ubuntu/bring-up.sh
  - deliver/macos/bootstrap-guest.sh
  - docker-compose.yml
  - Dockerfile
---

# Customer-Experience Delivery Packages (issue #103)

Two supported paths for customer evaluation deployments.

## Path 1 — macOS (terminal-mac-style VM)

Run `deliver/macos/bootstrap-guest.sh` **inside** a fresh macOS guest VM
(Apple Virtualization). It provisions:

- shared venv + `fusion-multi-node` (master `:11452`, agent `:11458`)
- optional `--with-mlx` to also provision the fusion-mlx inference engine
  (`:11434`, models pulled via `hf-mirror.com`)
- web console on `:11472` (requires bun in the guest; skipped otherwise)

```bash
bash deliver/macos/bootstrap-guest.sh --with-mlx
```

Add a second Mac as a worker agent:

```bash
FUSION_MASTER_HOST=<master-ip> bash deliver/macos/bootstrap-guest.sh <dir>
```

**Upstream gaps (terminal-mac RC)**: guest reset / clone / file-import are
not yet available upstream. Until then, treat the guest as disposable —
re-run the bootstrap script to rebuild the environment from scratch; it is
idempotent (existing clones are pulled, not re-created).

**Stack scope**: this package provisions the cluster layer (multi-node +
optional inference engine). Gateway / cowork / web-shell union-stack
packages are provisioned by their own delivery owners; point the guest at
their delivery scripts when available.

## Path 2 — Ubuntu (docker compose)

One command on a clean Ubuntu VM with Docker Engine + compose v2:

```bash
./deliver/ubuntu/bring-up.sh
```

What it does:

1. Creates `.env` from `.env.example`; replaces placeholder/weak
   `FUSION_CLUSTER_TOKEN` with a generated strong token.
2. `docker compose up -d --build --wait` — master + agent + web console
   (same image, three roles). The web dist is built inside the image
   (multi-stage bun build), so the guest needs no bun/node.
3. Verifies container health and prints access URLs.

| Endpoint | URL |
|----------|-----|
| Cluster API | `http://127.0.0.1:11452/api/health` |
| Web console | `http://127.0.0.1:11472/` |

Scale agents: `docker compose up -d --scale agent=3`.
Tear down: `docker compose down`.

**Build prerequisites**: `fusion-core` (the shared in-tree base library,
not on PyPI) enters the image as an extra build context. `bring-up.sh`
resolves it automatically: uses `$FUSION_CORE_DIR` (default
`~/fusion/fusion-core`), clones it on first run (HTTPS, SSH fallback).
Shell-exported `FUSION_*` variables are unset by the script so `.env`
is the single source of truth.

**Inference on Ubuntu**: fusion-mlx is Apple-Silicon-only and stays
bare-metal on a Mac. Containers reach it via `host.docker.internal`
(see `FUSION_MLX_URL` in `docker-compose.yml`). A clean Ubuntu VM runs
the scheduling layer E2E (task submit, routing, retries); inference
results require a Mac node joined to the cluster. Offline-friendly:
images are prebuilt (`docker compose build` once, then `docker save` /
`docker load` on the air-gapped guest), models via hf-mirror.

## Automated preview stack (issue #108)

See [preview/README.md](preview/README.md) — one-command isolated demo stack
(synthetic data only, health gate, TTL auto-teardown, notification hook for
fusion-cowork cow#404).

## Acceptance mapping

| Issue #103 acceptance | Covered by |
|-----------------------|-----------|
| macOS: fresh guest boots, stack healthy, E2E pipeline | `bootstrap-guest.sh` + `start.sh status` + cluster CLI |
| Ubuntu: `docker compose up` on clean VM, E2E passes | `bring-up.sh` (wraps `docker compose up --wait` + health verify) |
| Bilingual docs | this file + `README_CN.md` (same directory) |

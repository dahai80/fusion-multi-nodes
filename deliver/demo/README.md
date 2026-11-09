---
doc_id: fusion-multi-node/demo-fleet
product: fusion-multi-node
class: core
entry: service
doc_type: guide
title: {zh: "演示机编排（客户面 Mac A + 隐身算力 Mac B）", en: "Demo Fleet Orchestration (Customer-Facing Mac A, Hidden Compute Mac B)"}
audience: engineer
status: draft
version: 0.20.9
updated: 2026-11-09
owner: @dahai
source_of_truth:
  - deliver/demo/orchestrate-demo.sh
  - deliver/macos/bootstrap-guest.sh
---

# Demo Fleet Orchestration (issue #107)

The customer enters through **Mac A's IP only**. Mac B provides compute for
the demo but is never exposed to the customer — no URLs, no ports, no
mentions. fusion-multi-node is the layer that makes B invisible-but-useful.

## Topology

```
customer ──▶ Mac A (customer-facing)          Mac B (compute only, hidden)
             master :11452  ◀── joins ───────  NodeAgent :11458
             web console :11472                fusion-mlx :11434
             fusion-gateway :11432 (optional)
             ai-* demo app UI/API
```

Inference flow: app on A → master F3 proxy `POST /v1/chat/completions`
(`:11452`, OpenAI-compatible) → routed to B's agent → B's local fusion-mlx.
The customer only ever talks to A.

## Setup

**Mac B (compute, one command)** — reuse the existing guest bootstrap in
agent role:

```bash
FUSION_MASTER_HOST=<mac-a-ip> bash deliver/macos/bootstrap-guest.sh --with-mlx
```

Note: make sure the agent's inference endpoint points at B's local mlx
(`FUSION_MLX_URL=http://127.0.0.1:11434`; this project's default config port
differs — see the port table in the root README).

**Mac A (customer entry)** — run master + web console (`start.sh`), the
ai-\* demo app, and optionally fusion-gateway (cloud LLM upstream lives
there, not here). Point the app's LLM calls at
`http://<mac-a-ip>:11452/v1/chat/completions`.

## Orchestration (run on Mac A)

```bash
bash deliver/demo/orchestrate-demo.sh status   # master/gateway health + joined compute nodes
bash deliver/demo/orchestrate-demo.sh verify   # E2E chat completion routed to a worker node
```

`verify` picks the model from `FUSION_VERIFY_MODEL` (default
`mlx-community-Llama-3.2-1B-Instruct-4bit`) and fails loudly if no compute
node is reachable — a demo must never start on a silent broken cluster.

Publish to the customer: only Mac A's app URL (and optionally A's web
console). B stays LAN-internal.

## Sandbox note

Compute-side test execution (batch commands on B, e.g. demo health probes)
requires the sandbox, which is opt-in on the agent:

```bash
FUSION_NODE_TEST_EXEC=1 FUSION_NODE_SANDBOX=1 \
FUSION_NODE_SANDBOX_PATHS="$HOME/fusion" \
    # + start the agent — assembles AgentSandbox (gate + subprocess exec)
```

Default remains off — without it, test-type dispatch returns
`sandbox_unavailable` (fail visible).

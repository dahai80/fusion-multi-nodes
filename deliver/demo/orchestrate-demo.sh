#!/usr/bin/env bash
# Demo-fleet orchestration (#107) — customer-facing Mac A, compute-only Mac B.
#
# Topology (customer NEVER sees Mac B):
#   Mac A (customer entry):  master :11452 + web console :11472 + fusion-gateway
#                            :11432 + the ai-* demo app (its UI binds A's IP)
#   Mac B (compute only):    NodeAgent :11458 + fusion-mlx :11434, joined to A's
#                            cluster. Inference flows: app on A -> master F3 proxy
#                            POST /v1/chat/completions (:11452) -> B agent -> B mlx.
#   B's IP/ports are never published to the customer; only A's are.
#
# Mac B provisioning reuses deliver/macos/bootstrap-guest.sh (agent role,
# --with-mlx for the inference engine) — this script does NOT reimplement it.
#
# Usage (run on Mac A):
#   orchestrate-demo.sh status    # master + gateway + which nodes joined
#   orchestrate-demo.sh verify    # E2E: chat completion routed to a worker node
#
# Env:
#   FUSION_MASTER_URL    default http://127.0.0.1:11452
#   FUSION_GATEWAY_URL   default http://127.0.0.1:11432
#   FUSION_VERIFY_MODEL  model id for the verify call, default from /v1/models
set -euo pipefail

ACTION="${1:-status}"
MASTER_URL="${FUSION_MASTER_URL:-http://127.0.0.1:11452}"
GATEWAY_URL="${FUSION_GATEWAY_URL:-http://127.0.0.1:11432}"

log_info()  { printf "\033[0;32m[INFO]\033[0m  %s\n" "$*"; }
log_warn()  { printf "\033[0;33m[WARN]\033[0m  %s\n" "$*"; }
log_error() { printf "\033[0;31m[ERROR]\033[0m %s\n" "$*" >&2; }

auth_header() {
    # Cluster token lives next to the master's config; missing token = clear error.
    local token_file="$HOME/.fusion/multi-node/.cluster_token"
    if [[ -f "$token_file" ]]; then
        printf "Authorization: Bearer %s" "$(cat "$token_file")"
    else
        log_error "cluster token not found at $token_file"
        return 1
    fi
}

cmd_status() {
    local fail=0 auth
    auth="$(auth_header)" || exit 1

    if curl -sf --max-time 4 "$MASTER_URL/api/health" >/dev/null 2>&1; then
        log_info "master: healthy ($MASTER_URL)"
    else
        log_error "master: DOWN ($MASTER_URL)"
        fail=1
    fi
    if curl -sf --max-time 4 "$GATEWAY_URL/healthz" >/dev/null 2>&1; then
        log_info "gateway: healthy ($GATEWAY_URL)"
    else
        log_warn "gateway: DOWN ($GATEWAY_URL) — demo app can fall back to master F3 proxy"
    fi

    log_info "cluster nodes (compute providers — internal, not customer-facing):"
    local nodes_json
    nodes_json="$(curl -sf --max-time 4 -H "$auth" "$MASTER_URL/api/nodes" 2>/dev/null || echo '{"nodes":[]}')"
    python3 - "$nodes_json" <<'PY'
import json
import sys

data = json.loads(sys.argv[1])
nodes = data.get("nodes", [])
if not nodes:
    print("  (none joined — run bootstrap-guest.sh on the compute Mac)")
for n in nodes:
    status = n.get("status", "?")
    addr = f"{n.get('ip_address', '?')}:{n.get('agent_port', '?')}"
    print(f"  {n.get('node_id', '?')}  status={status}  addr={addr}  role={n.get('role', '?')}")
PY
    return "$fail"
}

cmd_verify() {
    local auth models model
    auth="$(auth_header)" || exit 1

    log_info "verifying E2E inference: customer entry (Mac A) -> master -> worker node (Mac B)"
    if [[ -z "${FUSION_VERIFY_MODEL:-}" ]]; then
        models="$(curl -sf --max-time 4 -H "$auth" "$MASTER_URL/v1/models" 2>/dev/null || echo "")"
        if [[ -n "$models" ]]; then
            model="$(printf '%s' "$models" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["data"][0]["id"])' 2>/dev/null || echo "")"
        fi
        model="${model:-mlx-community-Llama-3.2-1B-Instruct-4bit}"
    else
        model="$FUSION_VERIFY_MODEL"
    fi
    log_info "model: $model"

    local resp
    resp="$(curl -sf --max-time 120 -H "$auth" -H "Content-Type: application/json" \
        -d "{\"model\":\"$model\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with the single word: ok\"}],\"max_tokens\":16}" \
        "$MASTER_URL/v1/chat/completions" 2>&1)" || {
        log_error "chat completion via master failed — check that a compute node with fusion-mlx is joined"
        exit 1
    }
    local content
    content="$(printf '%s' "$resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["choices"][0]["message"]["content"])' 2>/dev/null || echo "<unparseable>")"
    log_info "completion from cluster compute: $content"

    log_info "customer-facing surface (publish ONLY these, all on Mac A):"
    log_info "  demo app UI:   its own port on Mac A (per the app's docs)"
    log_info "  cluster API:   $MASTER_URL/api/health"
    log_info "  cluster web:   http://<mac-a-ip>:11472/"
    log_info "  LLM endpoint:  $MASTER_URL/v1/chat/completions (OpenAI-compatible; gateway $GATEWAY_URL optional)"
    log_info "compute stays hidden: Mac B is reachable only from the LAN, never from the customer."
}

case "$ACTION" in
    status) cmd_status ;;
    verify) cmd_verify ;;
    *) echo "Usage: $0 {status|verify}"; exit 2 ;;
esac

#!/usr/bin/env bash
# Automated customer preview stack (#108) — companion to fusion-cowork#422.
# Fully automated: clone/pull project -> compose preview stack -> inject
# synthetic demo data -> health gate -> print preview URL + optional
# notification hook. Zero human intervention; TTL auto-teardown.
#
# Usage:
#   preview-stack.sh up    [project-dir]   # bring up + seed + notify (default)
#   preview-stack.sh teardown [project-dir]# immediate teardown
#   preview-stack.sh status [project-dir]  # stack status
#
# Env:
#   FUSION_PREVIEW_TTL_MINUTES   auto-teardown delay (default 120; 0 = no TTL)
#   FUSION_PREVIEW_NOTIFY_CMD    command receiving the preview URL as $1
#                                (cowork cow#404 email channel plugs in here)
#   FUSION_PREVIEW_MASTER_PORT   host port for master (default 21452)
#   FUSION_PREVIEW_WEB_PORT      host port for web console (default 21472)
#   FUSION_PREVIEW_PROJECT       compose project name (default fusion-preview)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_DIR="${2:-$REPO_ROOT}"
COMPOSE_PROJECT="${FUSION_PREVIEW_PROJECT:-fusion-preview}"
TTL_MIN="${FUSION_PREVIEW_TTL_MINUTES:-120}"
MASTER_PORT="${FUSION_PREVIEW_MASTER_PORT:-21452}"
WEB_PORT="${FUSION_PREVIEW_WEB_PORT:-21472}"
STATE_DIR="${PROJECT_DIR}/.preview-stack"
PID_FILE="${STATE_DIR}/ttl-watcher.pid"

log_info()  { printf "\033[0;32m[INFO]\033[0m  %s\n" "$*"; }
log_warn()  { printf "\033[0;33m[WARN]\033[0m  %s\n" "$*"; }
log_error() { printf "\033[0;31m[ERROR]\033[0m %s\n" "$*"; }

compose() {
    docker compose -p "$COMPOSE_PROJECT" \
        -f "$REPO_ROOT/docker-compose.yml" \
        -f "$SCRIPT_DIR/docker-compose.preview.yml" "$@"
}

# Shell-exported FUSION_* vars silently override .env (compose precedence).
unset FUSION_CLIENT_KEY FUSION_MLX_API_KEY MLX_API_KEY FUSION_CLUSTER_TOKEN \
      FUSION_MULTINODE_PORT FUSION_AGENT_PORT FUSION_MASTER_HOST FUSION_MASTER_PORT \
      FUSION_MLX_URL FUSION_MULTINODE_WEB_PORT

teardown_watcher() {
    # Detached TTL watcher: sleeps then tears the stack down. Idempotent.
    [[ "$TTL_MIN" == "0" ]] && return 0
    mkdir -p "$STATE_DIR"
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        return 0
    fi
    nohup bash -c "
        sleep $((TTL_MIN * 60))
        docker compose -p '$COMPOSE_PROJECT' \
            -f '$REPO_ROOT/docker-compose.yml' \
            -f '$SCRIPT_DIR/docker-compose.preview.yml' down -v --remove-orphans
        rm -rf '$STATE_DIR'
    " </dev/null >/dev/null 2>&1 &
    echo $! > "$PID_FILE"
    log_info "TTL watcher armed: auto-teardown in ${TTL_MIN} min (pid $(cat "$PID_FILE"))"
}

cmd_up() {
    cd "$PROJECT_DIR"
    if [[ ! -f .env ]]; then
        log_error ".env missing in $PROJECT_DIR — run deliver/ubuntu/bring-up.sh once first"
        exit 1
    fi
    log_info "building + starting preview stack (project $COMPOSE_PROJECT)..."
    compose --profile preview up -d --build --wait

    token="$(grep '^FUSION_CLUSTER_TOKEN=' .env | cut -d= -f2-)"
    log_info "health gate: master /api/health..."
    health_ok=0
    for _attempt in $(seq 1 30); do
        if compose exec -T master python -c "import urllib.request,json,sys; r=urllib.request.urlopen('http://127.0.0.1:11452/api/health',timeout=3); sys.exit(0 if json.loads(r.read()).get('status')=='ok' else 1)" 2>/dev/null; then
            health_ok=1
            break
        fi
        sleep 2
    done
    if [[ "$health_ok" != "1" ]]; then
        log_error "master health gate failed"
        cmd_teardown
        exit 1
    fi

    log_info "injecting synthetic demo data (deterministic fixtures, no real data)..."
    if ! python3 "$SCRIPT_DIR/synthetic_seed.py" --master-url "http://127.0.0.1:${MASTER_PORT}" --token "$token"; then
        log_error "synthetic seed failed — tearing down"
        cmd_teardown
        exit 1
    fi

    teardown_watcher

    preview_url="http://127.0.0.1:${WEB_PORT}/"
    log_info "preview stack ready: ${preview_url}"
    log_info "master API: http://127.0.0.1:${MASTER_PORT}/api/health"
    if [[ -n "${FUSION_PREVIEW_NOTIFY_CMD:-}" ]]; then
        log_info "firing notification hook: $FUSION_PREVIEW_NOTIFY_CMD"
        bash -c "$FUSION_PREVIEW_NOTIFY_CMD" _ "$preview_url" || log_warn "notification hook failed (stack stays up)"
    fi
    log_info "teardown: $0 teardown | auto in ${TTL_MIN} min"
}

cmd_teardown() {
    compose down -v --remove-orphans 2>/dev/null || true
    if [[ -f "$PID_FILE" ]]; then
        kill "$(cat "$PID_FILE")" 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
    log_info "preview stack torn down"
}

cmd_status() {
    compose ps
}

case "${1:-up}" in
    up)       cmd_up ;;
    teardown) cmd_teardown ;;
    status)   cmd_status ;;
    *) echo "Usage: $0 {up|teardown|status} [project-dir]"; exit 2 ;;
esac

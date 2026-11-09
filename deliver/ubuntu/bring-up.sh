#!/usr/bin/env bash
# Customer-experience bring-up for Ubuntu (issue #103 Path 2).
# Single command: ./deliver/ubuntu/bring-up.sh [project-dir]
# - prepares .env (strong FUSION_CLUSTER_TOKEN generated if absent)
# - docker compose up -d --build --wait (master + agent + web console)
# - verifies health and prints access URLs
# Requires: docker + docker compose v2. No other host deps.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${1:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
cd "$PROJECT_DIR"

log_info()  { printf "\033[0;32m[INFO]\033[0m  %s\n" "$*"; }
log_error() { printf "\033[0;31m[ERROR]\033[0m %s\n" "$*"; }

if ! command -v docker >/dev/null 2>&1; then
    log_error "docker not found — install Docker Engine + compose v2 first"
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    log_error "docker compose v2 not available"
    exit 1
fi

if [[ ! -f .env ]]; then
    if [[ -f .env.example ]]; then
        cp .env.example .env
    else
        : > .env
    fi
fi

current_token="$(grep '^FUSION_CLUSTER_TOKEN=' .env | cut -d= -f2- || true)"
# Placeholder or empty token gets replaced by a strong generated one (never ship weak defaults).
if [[ -z "$current_token" || "$current_token" == change-me* || ${#current_token} -lt 24 ]]; then
    if command -v openssl >/dev/null 2>&1; then
        token="fusion-$(openssl rand -hex 24)"
    else
        token="fusion-$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
    fi
    # Replace or append a strong generated token (idempotent across reruns).
    if grep -q '^FUSION_CLUSTER_TOKEN=' .env; then
        sed -i.bak "s|^FUSION_CLUSTER_TOKEN=.*|FUSION_CLUSTER_TOKEN=${token}|" .env && rm -f .env.bak
    else
        printf '\nFUSION_CLUSTER_TOKEN=%s\n' "$token" >> .env
    fi
    log_info "generated strong FUSION_CLUSTER_TOKEN into .env"
fi

# fusion-core (shared base lib, not on PyPI) must be available as a local
# git checkout — it enters the image build as an extra build context.
# GitHub HTTPS is unreachable on many target networks; clone via SSH fallback.
FUSION_CORE_DIR="${FUSION_CORE_DIR:-$HOME/fusion/fusion-core}"
if [[ ! -f "$FUSION_CORE_DIR/pyproject.toml" ]]; then
    log_info "fusion-core not found at $FUSION_CORE_DIR — cloning..."
    mkdir -p "$(dirname "$FUSION_CORE_DIR")"
    git clone "https://github.com/dahai80/fusion-core.git" "$FUSION_CORE_DIR" 2>/dev/null         || git clone "git@github.com:dahai80/fusion-core.git" "$FUSION_CORE_DIR"
fi
export FUSION_CORE_DIR
log_info "using fusion-core checkout: $FUSION_CORE_DIR"

# Shell-exported FUSION_* vars silently override .env values (compose
# precedence: environment > env_file). Unset the conflict-prone ones so the
# customer's .env is the single source of truth (#103 acceptance re-run).
unset FUSION_CLIENT_KEY FUSION_MLX_API_KEY MLX_API_KEY FUSION_CLUSTER_TOKEN       FUSION_MULTINODE_PORT FUSION_AGENT_PORT FUSION_MASTER_HOST FUSION_MASTER_PORT       FUSION_MLX_URL FUSION_MULTINODE_WEB_PORT

log_info "building + starting stack (master + agent + web)..."
docker compose up -d --build --wait

log_info "verifying health endpoints..."
master_status="$(docker inspect -f '{{.State.Health.Status}}' fusion-multinode-master 2>/dev/null || echo missing)"
web_status="$(docker inspect -f '{{.State.Health.Status}}' fusion-multinode-web 2>/dev/null || echo missing)"
if [[ "$master_status" != "healthy" || "$web_status" != "healthy" ]]; then
    log_error "unhealthy — master=${master_status} web=${web_status}; inspect with: docker compose ps && docker compose logs"
    exit 1
fi

web_port="$(grep -E '^FUSION_WEB_PORT=' .env | cut -d= -f2- || true)"
web_port="${web_port:-11472}"
log_info "stack healthy."
log_info "cluster API:  http://127.0.0.1:11452/api/health"
log_info "web console:  http://127.0.0.1:${web_port}/"
log_info "submit task:  docker compose exec master fusion-multi-node cluster status"
log_info "scale agents: docker compose up -d --scale agent=3"
log_info "tear down:    docker compose down"

#!/usr/bin/env bash
# Customer-experience bootstrap for a fresh macOS guest (issue #103 Path 1).
# Run INSIDE the guest VM (terminal-mac-style Apple Virtualization guest):
#   bash deliver/macos/bootstrap-guest.sh [--with-mlx] [install-dir]
# Provisions the fusion-multi-node cluster layer (master + agent + web console)
# and, with --with-mlx, the fusion-mlx inference engine on the same machine.
# Network: git clones + pip from the configured mirror; models via HF_MIRROR
# (hf-mirror.com). Gateway/cowork/web-shell stack packages are provisioned by
# their own delivery owners when available (see deliver/README.md).
set -euo pipefail

WITH_MLX=0
if [[ "${1:-}" == "--with-mlx" ]]; then
    WITH_MLX=1
    shift
fi
INSTALL_DIR="${1:-$HOME/fusion}"

log_info()  { printf "\033[0;32m[INFO]\033[0m  %s\n" "$*"; }
log_warn()  { printf "\033[0;33m[WARN]\033[0m  %s\n" "$*"; }
log_error() { printf "\033[0;31m[ERROR]\033[0m %s\n" "$*"; }

# python3.12 required (repo .python-version). Xcode CLT provides python3.
if ! command -v python3 >/dev/null 2>&1; then
    log_info "installing Xcode Command Line Tools (provides python3)..."
    xcode-select --install 2>/dev/null || true
    log_error "re-run this script after Xcode CLT install completes"
    exit 1
fi

# Git is required to clone repos.
if ! command -v git >/dev/null 2>&1; then
    log_error "git not found — install Xcode Command Line Tools first: xcode-select --install"
    exit 1
fi

mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

clone_repo() {
    local repo="$1"
    if [[ -d "$repo/.git" ]]; then
        log_info "$repo already cloned — pulling latest main"
        git -C "$repo" pull --ff-only || log_warn "$repo pull failed, keeping local state"
    else
        log_info "cloning $repo..."
        git clone "https://github.com/dahai80/${repo}.git"
    fi
}

clone_repo fusion-multi-nodes
if [[ "$WITH_MLX" == "1" ]]; then
    clone_repo fusion-mlx
fi

# Shared monorepo venv (all fusion python projects share it).
if [[ ! -f "$INSTALL_DIR/.venv/bin/activate" ]]; then
    log_info "creating shared venv (python3.12)..."
    python3 -m venv "$INSTALL_DIR/.venv"
fi
# shellcheck disable=SC1091
source "$INSTALL_DIR/.venv/bin/activate"
python -m pip install --upgrade pip
pip install -e "$INSTALL_DIR/fusion-multi-nodes[web]"

if [[ "$WITH_MLX" == "1" ]]; then
    export HF_MIRROR="https://hf-mirror.com"
    log_info "starting fusion-mlx (inference engine, port 11434)..."
    bash "$INSTALL_DIR/fusion-mlx/start.sh" start || log_warn "fusion-mlx start failed — cluster runs schedule-only without it"
fi

log_info "starting cluster master (127.0.0.1:11452)..."
bash "$INSTALL_DIR/fusion-multi-nodes/start.sh" start

log_info "starting node agent (127.0.0.1:11458)..."
FUSION_MULTINODE_ROLE=agent bash "$INSTALL_DIR/fusion-multi-nodes/start.sh" start

log_info "building + starting web console (:11472)..."
if command -v bun >/dev/null 2>&1; then
    (cd "$INSTALL_DIR/fusion-multi-nodes/web" && bun install && bun run build)
    bash "$INSTALL_DIR/fusion-multi-nodes/start.sh" start-web
else
    log_warn "bun not installed — web console skipped (install bun: https://bun.sh)"
fi

log_info "verifying..."
bash "$INSTALL_DIR/fusion-multi-nodes/start.sh" status
if [[ "$WITH_MLX" == "1" ]]; then
    bash "$INSTALL_DIR/fusion-mlx/start.sh" status || true
fi

log_info "bootstrap complete."
log_info "web console: http://127.0.0.1:11472/ (if built)"
log_info "cluster API: http://127.0.0.1:11452/api/health"
log_info "add a second Mac as agent: FUSION_MASTER_HOST=<this-ip> bash deliver/macos/bootstrap-guest.sh <dir>"

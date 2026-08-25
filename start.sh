#!/bin/bash
# fusion-multi-node lifecycle manager (start|stop|restart|status)
# 默认启动集群 Master (127.0.0.1:11452)；ROLE=agent 启动 Worker Agent (11445)。
# Port 11452 对齐 fusion-studio multiNodePort；11445 为 Node Agent 端口。
# Callers: fusion-studio UpstreamServiceManager (manual start; optional service).
# Affected API: start.sh start|stop|restart|status; status exits 0 if running, 1 if not.
# Data schemas: PID file .fusion-multi-node.{master,agent}.pid; logs/{stdout,stderr}_{master,agent}.log。
# Env:
#   ROLE (master|agent, default master)
#   FUSION_MULTINODE_HOST / FUSION_MULTINODE_PORT (master)
#   FUSION_AGENT_HOST / FUSION_AGENT_PORT (agent, default 127.0.0.1/11445)
#   FUSION_MASTER_HOST / FUSION_MASTER_PORT (agent 回连 master, default 127.0.0.1/11452)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV="${SCRIPT_DIR}/.venv"
ROLE="${FUSION_MULTINODE_ROLE:-master}"
HOST="${FUSION_MULTINODE_HOST:-127.0.0.1}"
PORT="${FUSION_MULTINODE_PORT:-11452}"
# Agent 角色 env
AGENT_HOST="${FUSION_AGENT_HOST:-127.0.0.1}"
AGENT_PORT="${FUSION_AGENT_PORT:-11445}"
MASTER_HOST="${FUSION_MASTER_HOST:-127.0.0.1}"
MASTER_PORT="${FUSION_MASTER_PORT:-11452}"
PID_FILE="${SCRIPT_DIR}/.fusion-multi-node.${ROLE}.pid"
LOG_DIR="${SCRIPT_DIR}/logs"
STDOUT_LOG="${LOG_DIR}/stdout_${ROLE}.log"
STDERR_LOG="${LOG_DIR}/stderr_${ROLE}.log"
HEALTH_WAIT=60

log_info()  { printf "\033[0;32m[INFO]\033[0m  %s\n" "$*"; }
log_warn()  { printf "\033[0;33m[WARN]\033[0m  %s\n" "$*"; }
log_error() { printf "\033[0;31m[ERROR]\033[0m %s\n" "$*"; }

ensure_venv() {
    if [[ -f "${VENV}/bin/activate" ]]; then
        # shellcheck disable=SC1091
        source "${VENV}/bin/activate"
    else
        log_warn "no .venv found at ${VENV}, using system python3"
    fi
}

get_pid() {
    [[ -f "$PID_FILE" ]] && cat "$PID_FILE" || echo ""
}

is_running() {
    local pid
    pid=$(get_pid)
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

# Health = process alive AND GET /api/health returns 200 (stdlib urllib, no deps).
# master 用 $PORT，agent 用 $AGENT_PORT。
is_healthy() {
    is_running || return 1
    local health_host="$HOST" health_port="$PORT"
    if [[ "$ROLE" == "agent" ]]; then
        health_host="$AGENT_HOST"
        health_port="$AGENT_PORT"
    fi
    MN_HOST="$health_host" MN_PORT="$health_port" python3 - <<'PY' 2>/dev/null
import os, sys, urllib.request
host = os.environ.get("MN_HOST", "127.0.0.1")
port = os.environ.get("MN_PORT", "11452")
try:
    with urllib.request.urlopen(f"http://{host}:{port}/api/health", timeout=2.0) as r:
        sys.exit(0 if r.status == 200 else 1)
except Exception:
    sys.exit(1)
PY
}

start() {
    if is_running; then
        log_info "multi-node ${ROLE} already running (PID $(get_pid))"
        exit 0
    fi
    mkdir -p "$LOG_DIR"
    ensure_venv

    if [[ "$ROLE" == "agent" ]]; then
        log_info "starting multi-node agent on ${AGENT_HOST}:${AGENT_PORT} (master ${MASTER_HOST}:${MASTER_PORT})..."
        nohup fusion-multi-node node start --role agent \
            --host "$AGENT_HOST" --port "$AGENT_PORT" \
            --master-host "$MASTER_HOST" --master-port "$MASTER_PORT" \
            --no-mdns \
            >> "$STDOUT_LOG" 2>> "$STDERR_LOG" &
        local pid=$!
        echo "$pid" > "$PID_FILE"
        log_info "launched agent (PID ${pid}), waiting for health..."
    else
        log_info "starting multi-node master on ${HOST}:${PORT}..."
        nohup fusion-multi-node node start --role master --host "$HOST" --port "$PORT" --no-mdns \
            >> "$STDOUT_LOG" 2>> "$STDERR_LOG" &
        local pid=$!
        echo "$pid" > "$PID_FILE"
        log_info "launched (PID ${pid}), waiting for health..."
    fi

    local i
    for i in $(seq 1 "$HEALTH_WAIT"); do
        if is_healthy; then
            if [[ "$ROLE" == "agent" ]]; then
                log_info "multi-node agent running (PID ${pid}) at ${AGENT_HOST}:${AGENT_PORT}"
            else
                log_info "multi-node master running (PID ${pid}) at ${HOST}:${PORT}"
            fi
            exit 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            log_error "process exited prematurely. recent stderr:"
            tail -n 20 "$STDERR_LOG" 2>/dev/null || true
            rm -f "$PID_FILE"
            exit 1
        fi
        sleep 1
    done

    log_error "timeout after ${HEALTH_WAIT}s. recent stderr:"
    tail -n 20 "$STDERR_LOG" 2>/dev/null || true
    exit 1
}

stop() {
    local pid
    pid=$(get_pid)
    if [[ -z "$pid" ]]; then
        log_info "multi-node not running"
        return 0
    fi
    log_info "stopping multi-node (PID ${pid})..."
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    log_info "stopped"
}

status() {
    if is_healthy; then
        if [[ "$ROLE" == "agent" ]]; then
            echo "running ${ROLE} (PID $(get_pid)) at ${AGENT_HOST}:${AGENT_PORT}"
        else
            echo "running ${ROLE} (PID $(get_pid)) at ${HOST}:${PORT}"
        fi
        exit 0
    fi
    echo "not running (${ROLE})"
    exit 1
}

restart() {
    stop || true
    start
}

case "${1:-status}" in
    start)   start ;;
    stop)    stop ;;
    restart) restart ;;
    status)  status ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 2
        ;;
esac

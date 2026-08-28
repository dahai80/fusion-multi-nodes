#!/bin/bash
# fusion-multi-node lifecycle manager (start|stop|restart|status)
# 默认启动集群 Master (127.0.0.1:11452)；ROLE=agent 启动 Worker Agent (11458)。
# Port 11452 对齐 fusion-studio multiNodePort；11458 为 Node Agent 端口。
# Callers: fusion-studio UpstreamServiceManager (manual start; optional service).
# Affected API: start.sh start|stop|restart|status; status exits 0 if running, 1 if not.
# Data schemas: PID file .fusion-multi-node.{master,agent}.pid; logs/{stdout,stderr}_{master,agent}.log。
# Env:
#   ROLE (master|agent, default master)
#   FUSION_MULTINODE_HOST / FUSION_MULTINODE_PORT (master)
#   FUSION_AGENT_HOST / FUSION_AGENT_PORT (agent, default 127.0.0.1/11458)
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
AGENT_PORT="${FUSION_AGENT_PORT:-11458}"
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
        # P1-16 (审计 §6.4): 应用日志走 RotatingFileHandler (10MB×5 有界) 落 ${LOG_DIR}/app_${ROLE}.log,
        # stdout 重定向 /dev/null (避免 nohup stdout.log 与文件 handler 重复无界增长); stderr 仍落盘捕获崩溃栈。
        FUSION_MULTINODE_LOG_FILE="${LOG_DIR}/app_${ROLE}.log" \
        nohup fusion-multi-node node start --role agent \
            --host "$AGENT_HOST" --port "$AGENT_PORT" \
            --master-host "$MASTER_HOST" --master-port "$MASTER_PORT" \
            --no-mdns \
            >> /dev/null 2>> "$STDERR_LOG" &
        local pid=$!
        echo "$pid" > "$PID_FILE"
        log_info "launched agent (PID ${pid}), waiting for health..."
    else
        log_info "starting multi-node master on ${HOST}:${PORT}..."
        # P1-16 (审计 §6.4): 应用日志走 RotatingFileHandler (10MB×5 有界) 落 ${LOG_DIR}/app_${ROLE}.log,
        # stdout 重定向 /dev/null (避免 nohup stdout.log 与文件 handler 重复无界增长); stderr 仍落盘捕获崩溃栈。
        FUSION_MULTINODE_LOG_FILE="${LOG_DIR}/app_${ROLE}.log" \
        nohup fusion-multi-node node start --role master --host "$HOST" --port "$PORT" --no-mdns \
            >> /dev/null 2>> "$STDERR_LOG" &
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

# ── H2 launchd 进程守护: 崩溃自动重启 + H3 任务持久化恢复 = 不丢任务 ──
# install-launchd: 渲染 deploy plist 占位符 → ~/Library/LaunchAgents → launchctl load
# uninstall-launchd: launchctl unload + 删 plist
# 现网单 Master: KeepAlive 崩溃重启 (10s 节流), start() _restore_tasks 恢复 RUNNING→PENDING 重派

LAUNCHD_LABEL="com.dahai80.fusion-multi-node"
PLIST_TEMPLATE="${SCRIPT_DIR}/deploy/com.dahai80.fusion-multi-node.plist"
LAUNCHD_PLIST="${HOME}/Library/LaunchAgents/${LAUNCHD_LABEL}.plist"

install_launchd() {
    [[ "$ROLE" != "master" ]] && { log_error "install-launchd 仅支持 master 角色 (HA 守护 Master)"; exit 2; }
    [[ ! -f "$PLIST_TEMPLATE" ]] && { log_error "plist 模板缺失: $PLIST_TEMPLATE"; exit 2; }
    mkdir -p "${HOME}/Library/LaunchAgents" "$LOG_DIR"

    local venv_bin="${VENV}/bin"
    local fusion_mn="${venv_bin}/fusion-multi-node"
    if [[ ! -x "$fusion_mn" ]]; then
        log_warn "venv 无 fusion-multi-node, 用 PATH 中的 fusion-multi-node"
        venv_bin="/usr/local/bin:/usr/bin"
        fusion_mn="fusion-multi-node"
    fi

    sed \
        -e "s|@@VENV_BIN@@|${venv_bin}|g" \
        -e "s|@@FUSION_MN@@|${fusion_mn}|g" \
        -e "s|@@LOGDIR@@|${LOG_DIR}|g" \
        -e "s|@@HOST@@|${HOST}|g" \
        -e "s|@@PORT@@|${PORT}|g" \
        -e "s|@@REPO@@|${SCRIPT_DIR}|g" \
        -e "s|@@MTLS_ENABLED@@|${FUSION_MTLS_ENABLED:-}|g" \
        -e "s|@@MTLS_CA_CERT@@|${FUSION_MTLS_CA_CERT:-}|g" \
        -e "s|@@MTLS_NODE_CERT@@|${FUSION_MTLS_NODE_CERT:-}|g" \
        -e "s|@@MTLS_NODE_KEY@@|${FUSION_MTLS_NODE_KEY:-}|g" \
        -e "s|@@MTLS_NODE_ID@@|${FUSION_MTLS_NODE_ID:-}|g" \
        -e "s|@@MTLS_NODE_ROLE@@|${FUSION_MTLS_NODE_ROLE:-}|g" \
        -e "s|@@ALERT_WEBHOOK_URL@@|${FUSION_ALERT_WEBHOOK_URL:-}|g" \
        -e "s|@@CLUSTER_TOKEN@@|${FUSION_CLUSTER_TOKEN:-}|g" \
        -e "s|@@BOOTSTRAP_ADMIN@@|${FUSION_BOOTSTRAP_ADMIN:-}|g" \
        -e "s|@@USERS_FILE@@|${FUSION_USERS_FILE:-}|g" \
        "$PLIST_TEMPLATE" > "$LAUNCHD_PLIST"
    log_info "渲染 launchd plist → $LAUNCHD_PLIST"

    # 若 nohup 进程在跑, 先停 (交给 launchd 托管, 避免双实例)
    if is_running; then
        log_warn "检测到 nohup 进程 (PID $(get_pid)), 先停止转交 launchd 托管"
        stop || true
    fi

    launchctl unload "$LAUNCHD_PLIST" 2>/dev/null || true
    launchctl load "$LAUNCHD_PLIST"
    log_info "launchd 已加载 ${LAUNCHD_LABEL} (RunAtLoad + KeepAlive 崩溃自愈)"
    log_info "崩溃 → launchd 10s 节流重启 → start() _restore_tasks 恢复任务 (H3), 不丢任务"
    log_info "查看: launchctl list | grep fusion-multi-node ; 日志: ${LOG_DIR}/stdout_master.launchd.log"
}

uninstall_launchd() {
    if [[ -f "$LAUNCHD_PLIST" ]]; then
        launchctl unload "$LAUNCHD_PLIST" 2>/dev/null || true
        rm -f "$LAUNCHD_PLIST"
        log_info "launchd 已卸载 ${LAUNCHD_LABEL} (plist 已删)"
    else
        log_info "无 launchd plist (${LAUNCHD_PLIST}), 未安装"
    fi
}

case "${1:-status}" in
    start)   start ;;
    stop)    stop ;;
    restart) restart ;;
    status)  status ;;
    install-launchd)   install_launchd ;;
    uninstall-launchd) uninstall_launchd ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|install-launchd|uninstall-launchd}"
        exit 2
        ;;
esac

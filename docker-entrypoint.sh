#!/bin/sh
# Fusion-Multi-Node 容器入口 — 按 ROLE env 启动 master 或 agent。
# ROLE=master (默认): 绑定 0.0.0.0:11452, 禁 mDNS (容器内无 Bonjour)。
# ROLE=agent: 绑定 0.0.0.0:AGENT_PORT, 回连 master:MASTER_PORT。
set -e

ROLE="${FUSION_MULTINODE_ROLE:-master}"

if [ "$ROLE" = "agent" ]; then
    AGENT_HOST="${FUSION_AGENT_HOST:-0.0.0.0}"
    AGENT_PORT="${FUSION_AGENT_PORT:-11458}"
    MASTER_HOST="${FUSION_MASTER_HOST:-master}"
    MASTER_PORT="${FUSION_MASTER_PORT:-11452}"
    echo "启动 Agent: ${AGENT_HOST}:${AGENT_PORT} → master ${MASTER_HOST}:${MASTER_PORT}"
    exec fusion-multi-node node start \
        --role agent \
        --host "$AGENT_HOST" \
        --port "$AGENT_PORT" \
        --master-host "$MASTER_HOST" \
        --master-port "$MASTER_PORT" \
        --no-mdns
else
    HOST="${FUSION_MULTINODE_HOST:-0.0.0.0}"
    PORT="${FUSION_MULTINODE_PORT:-11452}"
    echo "启动 Master: ${HOST}:${PORT}"
    exec fusion-multi-node node start \
        --role master \
        --host "$HOST" \
        --port "$PORT" \
        --no-mdns
fi

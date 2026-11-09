#!/bin/sh
# Fusion-Multi-Node container entrypoint — start by ROLE env (master|agent|web).
# ROLE=master (default): bind 0.0.0.0:11452, mDNS off (no Bonjour in container).
# ROLE=agent: bind 0.0.0.0:AGENT_PORT, join master:MASTER_PORT.
# ROLE=web (#103 customer delivery): serve web/dist console on 11472 via serve_dist.py.
set -e

ROLE="${FUSION_MULTINODE_ROLE:-master}"

if [ "$ROLE" = "agent" ]; then
    AGENT_HOST="${FUSION_AGENT_HOST:-0.0.0.0}"
    AGENT_PORT="${FUSION_AGENT_PORT:-11458}"
    MASTER_HOST="${FUSION_MASTER_HOST:-master}"
    MASTER_PORT="${FUSION_MASTER_PORT:-11452}"
    echo "Starting Agent: ${AGENT_HOST}:${AGENT_PORT} -> master ${MASTER_HOST}:${MASTER_PORT}"
    exec fusion-multi-node node start \
        --role agent \
        --host "$AGENT_HOST" \
        --port "$AGENT_PORT" \
        --master-host "$MASTER_HOST" \
        --master-port "$MASTER_PORT" \
        --no-mdns
elif [ "$ROLE" = "web" ]; then
    echo "Starting Web console: ${FUSION_MULTINODE_WEB_HOST:-0.0.0.0}:${FUSION_MULTINODE_WEB_PORT:-11472} -> master ${FUSION_MULTINODE_HOST:-master}:${FUSION_MULTINODE_PORT:-11452}"
    exec python web/serve_dist.py
else
    HOST="${FUSION_MULTINODE_HOST:-0.0.0.0}"
    PORT="${FUSION_MULTINODE_PORT:-11452}"
    echo "Starting Master: ${HOST}:${PORT}"
    exec fusion-multi-node node start \
        --role master \
        --host "$HOST" \
        --port "$PORT" \
        --no-mdns
fi

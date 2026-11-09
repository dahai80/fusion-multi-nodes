# Fusion-Multi-Node container image — cluster scheduling layer (Master + Agent + Web console).
# Inference engine fusion-mlx stays bare-metal; containers reach it via host.docker.internal.
# python:3.12-slim (arm64) matches pyproject target; package is pure Python + deps.
# Stage 1 builds the web console (web/dist) with bun so the customer image (#103)
# ships the cluster console without requiring bun on the guest.

FROM oven/bun:1 AS web-builder

WORKDIR /web
COPY web/package.json web/bun.lock ./
RUN bun install --frozen-lockfile
COPY web/ ./
RUN bun run build

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
    PIP_TRUSTED_HOST=mirrors.aliyun.com

WORKDIR /app

# Copy dependency manifest first (layer cache); no separate requirements file.
COPY pyproject.toml ./
COPY fusion_multi_node/ ./fusion_multi_node/

# [web] extra (fastapi/uvicorn) — required by Master/Agent servers.
RUN pip install --no-cache-dir -e ".[web]"

# Expose Master (11452), Agent (11458), Web console (11472). Actual listener depends on ROLE.
EXPOSE 11452 11458 11472

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Web console assets + resident static server (ROLE=web).
COPY --from=web-builder /web/dist ./web/dist
COPY web/serve_dist.py ./web/serve_dist.py

ENTRYPOINT ["docker-entrypoint.sh"]

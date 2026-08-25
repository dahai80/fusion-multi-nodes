# Fusion-Multi-Node 容器镜像 — 集群调度层 (Master + Agent)
# 推理引擎 fusion-mlx 保持裸机部署, 容器经 host.docker.internal 回连。
# python:3.12-slim (arm64) 对齐 pyproject 声明目标; 包为纯 Python + 依赖。

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
    PIP_TRUSTED_HOST=mirrors.aliyun.com

WORKDIR /app

# 先拷依赖清单 (若变更少, 利用 layer 缓存); 本项目无独立 requirements, 直接装包。
COPY pyproject.toml ./
COPY fusion_multi_node/ ./fusion_multi_node/

# 安装 [web] extra (fastapi/uvicorn) — Master/Agent 服务必需。
RUN pip install --no-cache-dir -e ".[web]"

# 暴露 Master (11452) 与 Agent (11458) 端口。实际监听由 ROLE 决定。
EXPOSE 11452 11458

# 入口脚本: 按 ROLE env 切换 master / agent 启动参数。
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]

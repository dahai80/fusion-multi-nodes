# fusion-multi-node 可观测性栈部署指南

本目录提供 Prometheus + Grafana + Alertmanager 三件套模板, 对接 Master 已有的 `GET /api/v1/metrics` 端点 (S2, Prometheus 文本 0.0.4 格式)。

> ⚠️ 本目录仅含模板与配置, **不修改** 主仓 `docker-compose.yml` (容器化由专门 agent 维护)。下方提供可直接粘贴的 compose 服务片段。

## 文件清单

| 文件 | 用途 |
|------|------|
| `prometheus.yml` | Prometheus 抓取配置, 5s 抓取 Master `/api/v1/metrics` |
| `alerts.yml` | Prometheus 告警规则 (5 条) |
| `alertmanager.yml` | Alertmanager 接收端占位配置 |
| `grafana-dashboard.json` | Grafana 面板导入 JSON (9 个 panel) |

## 指标来源

所有指标由 `ClusterMaster.get_prometheus_metrics` (`fusion_multi_node/master/cluster_master.py`) 生成, 指标名与类型如下:

| 指标 | 类型 | 含义 |
|------|------|------|
| `fusion_cluster_nodes_total` | gauge | 集群注册节点总数 |
| `fusion_cluster_nodes_online` | gauge | 在线节点数 |
| `fusion_cluster_tasks_total` | gauge | 任务总数 |
| `fusion_cluster_tasks_running` | gauge | 运行中任务数 |
| `fusion_cluster_tasks_pending` | gauge | 待派发任务数 |
| `fusion_cluster_tasks_completed` | gauge | 已完成任务数 |
| `fusion_cluster_tasks_failed` | gauge | 失败/超时任务数 |
| `fusion_cluster_task_retries_total` | counter | 任务重试总次数 |
| `fusion_cluster_kv_cache_entries` | gauge | KV 缓存条目数 |
| `fusion_cluster_memory_total_gb` | gauge | 集群总内存 GB |
| `fusion_cluster_memory_available_gb` | gauge | 集群可用内存 GB |
| `fusion_cluster_dispatch_latency_seconds{quantile="0.5"|"0.9"|"0.99"}` | summary | 派发延迟秒, 含 `_sum` / `_count` |

## 前置条件

1. Master 服务已运行 (`./start.sh start`), 监听 `127.0.0.1:11452`。
2. 集群共享 token 已生成, 位于 `~/.fusion/multi-node/.cluster_token` (mode 0600, Master/Agent 共享)。`/api/v1/metrics` **未豁免 Bearer 鉴权**, 抓取必须携带该 token。
3. Docker / Docker Compose 可用 (容器化部署场景)。

## 部署步骤

### 1. 准备 token

```bash
# 读取集群共享 token (Master 启动后自动生成)
cat ~/.fusion/multi-node/.cluster_token
```

将 `prometheus.yml` 中的 `<cluster-token>` 替换为该 token 值。

### 2. 填写 Alertmanager 接收端

编辑 `alertmanager.yml`, 将 `webhook-placeholder` receiver 改为真实接收端:

```yaml
receivers:
    - name: webhook
      webhook_configs:
          - url: https://your-webhook.example.com/alert  # 企业微信/飞书/自定义网关
            send_resolved: true
    # 或 Slack:
    # - name: slack
    #   slack_configs:
    #       - api_url: https://hooks.slack.com/services/xxx
    #         channel: "#ops"
```

### 3. 添加 compose 服务 (粘贴到 `docker-compose.yml`)

```yaml
services:
    prometheus:
        image: prom/prometheus:latest
        container_name: fusion-multinode-prometheus
        volumes:
            - ./deploy/observability/prometheus.yml:/etc/prometheus/prometheus.yml:ro
            - ./deploy/observability/alerts.yml:/etc/prometheus/alerts.yml:ro
            - prometheus-data:/prometheus
        command:
            - --config.file=/etc/prometheus/prometheus.yml
            - --storage.tsdb.retention.time=15d
        ports:
            - "9090:9090"
        networks:
            - fusion-net
        depends_on:
            - master  # 若 master 也容器化; 否则用 extra_hosts 指向宿主机

    alertmanager:
        image: prom/alertmanager:latest
        container_name: fusion-multinode-alertmanager
        volumes:
            - ./deploy/observability/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
            - alertmanager-data:/alertmanager
        command:
            - --config.file=/etc/alertmanager/alertmanager.yml
        ports:
            - "9093:9093"
        networks:
            - fusion-net

    grafana:
        image: grafana/grafana:latest
        container_name: fusion-multinode-grafana
        volumes:
            - grafana-data:/var/lib/grafana
            - ./deploy/observability/grafana-dashboard.json:/var/lib/grafana/dashboards/fusion-multi-node.json:ro
        environment:
            - GF_SECURITY_ADMIN_PASSWORD=admin  # 首次登录后修改
            - GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/fusion-multi-node.json
        ports:
            - "3000:3000"
        networks:
            - fusion-net
        depends_on:
            - prometheus

volumes:
    prometheus-data:
    alertmanager-data:
    grafana-data:

networks:
    fusion-net:
        driver: bridge
```

> 若 Master 不在 compose 网络内 (宿主机直跑 `./start.sh start`), 在 `prometheus` 服务下加 `extra_hosts: ["master:host-gateway"]`, 并把 `prometheus.yml` 的 target 改为 `master:11452` → `host.docker.internal:11452` (Mac) 或宿主机 IP。

### 4. 导入 Grafana 面板

容器启动后, Grafana 已通过 volume 自动加载 dashboard JSON。若需手动导入:

1. 打开 `http://localhost:3000` (默认 admin/admin)。
2. **Configuration → Data Sources → Add data source → Prometheus**, URL 填 `http://prometheus:9090`。
3. **Dashboards → Import → Upload JSON file**, 选择 `grafana-dashboard.json`。
4. 在数据源变量 `DS_PROMETHEUS` 选择上一步建的 Prometheus。
5. 保存, 面板顶部出现 `${DS_PROMETHEUS}` 变量切换。

### 5. 启动并验证

```bash
docker compose up -d prometheus alertmanager grafana

# 验证 Prometheus 抓取成功
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[].health'
# 期望: "up"

# 验证指标可查
curl -s "http://localhost:9090/api/v1/query?query=fusion_cluster_nodes_online" | jq '.data.result'

# 验证告警规则已加载
curl -s http://localhost:9090/api/v1/rules | jq '.data.groups[].rules[].name'
# 期望: ["MultinodeNodeOffline","MultinodeTaskBacklog","MultinodeHighFailureRate","MultinodeHighLatencyP99","MultinodeMasterDown"]
```

## 告警 → 事件映射

| 告警 | 严重度 | 触发条件 | 含义 / 响应 |
|------|--------|----------|-------------|
| `MultinodeMasterDown` | critical | `up{job="fusion-multinode"} == 0` 持续 1m | Master 不可达: 进程崩溃或网络中断。立即介入, 检查 `./start.sh status` / `logs/stderr.log`, 必要时 `./start.sh install-launchd` 启用崩溃自愈 |
| `MultinodeNodeOffline` | warning | `(total - online) / total > 0` 持续 1m | 节点离线: Agent 进程退出或网络分区。检查该节点 `NodeAgent` 日志与 mDNS 发现 |
| `MultinodeTaskBacklog` | warning | `tasks_pending > 50` 持续 2m | 任务积压: 调度器过载或可用节点不足。检查 `LoadRouter` 策略与节点健康, 考虑扩容 |
| `MultinodeHighFailureRate` | warning | `increase(tasks_failed[5m]) > 5` | 失败率飙升: 节点异常或模型不可用。检查 fusion-mlx `/v1/models` 与节点资源 |
| `MultinodeHighLatencyP99` | warning | `dispatch_latency_seconds{quantile="0.99"} > 2` 持续 5m | 派发延迟飙升: 节点负载不均或网络拥塞。检查 `LoadMetrics` 与网络拓扑 |

### 抑制规则

`alertmanager.yml` 配置: 当 `MultinodeMasterDown` 触发时, 抑制同 service 的 warning 级告警 (Master 都挂了, 节点离线/积压等次生告警无意义)。

## 常见问题

**Q: Prometheus target 显示 `down`?**
A: 1) token 不匹配 → 检查 `prometheus.yml` 的 `bearer_token` 与 `~/.fusion/multi-node/.cluster_token` 一致; 2) Master 未起 → `./start.sh status`; 3) 容器无法访问宿主机端口 → 用 `host.docker.internal` 或 `extra_hosts`。

**Q: 指标值全为 0?**
A: 集群无注册节点或无任务。先 `fusion-multi-node node list` 确认有节点, `fusion-multi-node task list` 确认有任务。

**Q: `increase(fusion_cluster_tasks_failed[5m])` 无数据?**
A: `tasks_failed` 是 gauge, 5m 内无失败任务则增量为 0 (正常)。若持续为空, 检查 Master 是否有完成任务生命周期。

**Q: Grafana 面板 `${DS_PROMETHEUS}` 变量无选项?**
A: 先建 Prometheus 数据源 (步骤 4.2), 变量会自动列举。

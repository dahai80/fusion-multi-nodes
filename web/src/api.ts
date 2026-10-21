import { DEV_API_KEY, isEmbedded } from "./sdk";

// Typed client over the cluster master API (direct :11452 behind the Vite
// dev proxy, gateway /shell/fusion-multi-nodes/ proxy when embedded — both
// same-origin). Master endpoints are cluster-token gated:
// - Embedded: the gateway /shell proxy injects the module key upstream, so a
//   plain same-origin fetch carries no secret in this bundle (spec: no auth
//   logic in UI).
// - Standalone dev: VITE_API_KEY (dev convenience only) attaches
//   Authorization: Bearer — the master BearerAuthMiddleware accepts Bearer.
// Raw fetch (not the sdk bridge) so the Prometheus text body (/api/v1/metrics)
// works in both modes.

export interface NodeEntry {
    node_id: string;
    hostname: string;
    ip_address: string;
    port: number;
    status: string;
    role: string;
    total_memory_gb: number;
    available_memory_gb: number;
    cpu_cores?: number;
    gpu_cores?: number;
    device_model?: string;
    uma_size_gb?: number | null;
    active_tasks: number;
    max_tasks: number;
    score: number;
    last_heartbeat: number;
    draining?: boolean;
    supervisor_available?: boolean;
    drivers?: string[];
    epoch?: number;
    leader_id?: string;
    resident?: boolean;
}

export interface NodesResponse {
    total: number;
    online: number;
    nodes: NodeEntry[];
    cluster_view: string;
    partitioned: boolean;
    epoch: number;
    leader_id: string;
    is_leader: boolean;
}

export interface ClusterStats {
    total_nodes: number;
    online_nodes: number;
    total_tasks: number;
    active_tasks: number;
    completed_tasks: number;
    partial_tasks: number;
    failed_tasks: number;
    kv_cache_entries: number;
    total_memory_gb: number;
    available_memory_gb: number;
    epoch: number;
    leader_id: string;
    is_leader: boolean;
    leader_token?: string;
    load_summary?: Record<string, unknown>;
}

export interface TaskEntry {
    task_id: string;
    name: string;
    mode: string;
    model_name: string;
    status: string;
    assigned_nodes: string[];
    created_at: number;
    started_at: number;
    completed_at: number;
    error: string | null;
    required_capability?: string | null;
    priority: number;
    degraded_from_model?: string | null;
    degradation_count: number;
    cancel_reason?: string | null;
    sub_tasks: string[];
    result?: Record<string, unknown> | null;
    batch_id?: string | null;
}

export interface TasksResponse {
    tasks: TaskEntry[];
    total: number;
}

// Served under the gateway shell path (/shell/fusion-multi-nodes/) requests
// must carry the module prefix so they land on the gateway /shell proxy;
// served standalone the prefix is empty. Derived from the document pathname —
// one build serves both topologies.
export function apiBase(): string {
    if (typeof window === "undefined") return "";
    const m = window.location.pathname.match(/^(\/shell\/[^/]+)/);
    return m ? m[1] : "";
}

function authHeaders(): Record<string, string> {
    if (!isEmbedded() && DEV_API_KEY)
        return { Authorization: `Bearer ${DEV_API_KEY}` };
    return {};
}

async function request(path: string, init?: RequestInit): Promise<Response> {
    const res = await fetch(`${apiBase()}${path}`, {
        ...init,
        headers: { ...authHeaders(), ...(init?.headers ?? {}) },
    });
    if (!res.ok) throw new Error(`request failed: ${res.status} ${path}`);
    return res;
}

async function getJson<T>(path: string): Promise<T> {
    const res = await request(path);
    return (await res.json()) as T;
}

export const api = {
    nodes: () => getJson<NodesResponse>("/api/nodes"),
    node: (nodeId: string) =>
        getJson<NodeEntry>(`/api/nodes/${encodeURIComponent(nodeId)}`),
    clusterStats: () => getJson<ClusterStats>("/api/cluster/stats"),
    tasks: (limit = 100) => getJson<TasksResponse>(`/api/tasks?limit=${limit}`),
    metricsText: async () => {
        const res = await request("/api/v1/metrics");
        return res.text();
    },
};

// --- Prometheus text parsing (/api/v1/metrics is text/plain 0.0.4) ---
// Master exposition (cluster_master.get_prometheus_metrics): cluster-level
// gauges + dispatch latency summary. Parse by metric name, last sample wins.

export interface ClusterMetrics {
    nodes_total: number;
    nodes_online: number;
    tasks_total: number;
    tasks_running: number;
    tasks_pending: number;
    tasks_completed: number;
    tasks_failed: number;
    tasks_partial: number;
    task_retries_total: number;
    kv_cache_entries: number;
    memory_total_gb: number;
    memory_available_gb: number;
    dispatch_latency_p50: number;
    dispatch_latency_p90: number;
    dispatch_latency_p99: number;
    banned_nodes: number;
}

const GAUGE_NAMES: Array<{ name: string; key: keyof ClusterMetrics }> = [
    { name: "fusion_cluster_nodes_total", key: "nodes_total" },
    { name: "fusion_cluster_nodes_online", key: "nodes_online" },
    { name: "fusion_cluster_tasks_total", key: "tasks_total" },
    { name: "fusion_cluster_tasks_running", key: "tasks_running" },
    { name: "fusion_cluster_tasks_pending", key: "tasks_pending" },
    { name: "fusion_cluster_tasks_completed", key: "tasks_completed" },
    { name: "fusion_cluster_tasks_failed", key: "tasks_failed" },
    { name: "fusion_cluster_tasks_partial", key: "tasks_partial" },
    { name: "fusion_cluster_task_retries_total", key: "task_retries_total" },
    { name: "fusion_cluster_kv_cache_entries", key: "kv_cache_entries" },
    { name: "fusion_cluster_memory_total_gb", key: "memory_total_gb" },
    { name: "fusion_cluster_memory_available_gb", key: "memory_available_gb" },
    { name: "fusion_cluster_banned_nodes", key: "banned_nodes" },
];

const LATENCY_QUANTILES: Array<{
    quantile: string;
    key: keyof ClusterMetrics;
}> = [
    { quantile: "0.5", key: "dispatch_latency_p50" },
    { quantile: "0.9", key: "dispatch_latency_p90" },
    { quantile: "0.99", key: "dispatch_latency_p99" },
];

export function parseClusterMetrics(text: string): ClusterMetrics {
    const out: ClusterMetrics = {
        nodes_total: 0,
        nodes_online: 0,
        tasks_total: 0,
        tasks_running: 0,
        tasks_pending: 0,
        tasks_completed: 0,
        tasks_failed: 0,
        tasks_partial: 0,
        task_retries_total: 0,
        kv_cache_entries: 0,
        memory_total_gb: 0,
        memory_available_gb: 0,
        dispatch_latency_p50: 0,
        dispatch_latency_p90: 0,
        dispatch_latency_p99: 0,
        banned_nodes: 0,
    };
    for (const line of text.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) continue;
        const sp = trimmed.lastIndexOf(" ");
        if (sp <= 0) continue;
        const metric = trimmed.slice(0, sp);
        const value = Number(trimmed.slice(sp + 1));
        if (!Number.isFinite(value)) continue;
        // dispatch latency is a summary with {quantile=".."} labels
        const latMatch = metric.match(
            /^fusion_cluster_dispatch_latency_seconds\{quantile="([^"]+)"\}$/,
        );
        if (latMatch) {
            const hit = LATENCY_QUANTILES.find(
                (q) => q.quantile === latMatch[1],
            );
            if (hit) out[hit.key] = value;
            continue;
        }
        const bare = "fusion_cluster_dispatch_latency_seconds";
        if (metric === bare || metric.startsWith(`${bare}_`)) continue;
        const gauge = GAUGE_NAMES.find((g) => g.name === metric);
        if (gauge) out[gauge.key] = value;
    }
    return out;
}

// --- View munging helpers (pure, unit-tested) ---

export type NodeStatusKind =
    | "success"
    | "running"
    | "warning"
    | "error"
    | "offline";

export function nodeStatusKind(
    n: Pick<NodeEntry, "status" | "draining">,
): NodeStatusKind {
    if (n.draining) return "warning";
    switch (n.status) {
        case "online":
            return "success";
        case "busy":
            return "running";
        case "draining":
            return "warning";
        case "offline":
            return "offline";
        default:
            return "offline";
    }
}

export function taskStatusKind(status: string): NodeStatusKind {
    switch (status) {
        case "COMPLETED":
            return "success";
        case "RUNNING":
            return "running";
        case "PENDING":
            return "warning";
        case "PARTIAL":
            return "warning";
        case "FAILED":
        case "TIMEOUT":
        case "CANCELLED":
            return "error";
        default:
            return "offline";
    }
}

export function formatGb(v: number | undefined): string {
    if (v === undefined || !Number.isFinite(v)) return "-";
    return `${v.toFixed(1)} GB`;
}

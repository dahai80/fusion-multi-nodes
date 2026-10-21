import {
    EmptyState,
    ErrorState,
    MetricTile,
    SkeletonLines,
    StatusBadge,
    useAutoRetry,
} from "@fusion/web-ui";
import { useMemo } from "react";
import { type ClusterStats, type NodeEntry, api, nodeStatusKind } from "../api";
import { PageHeader } from "../components/PageHeader";

// Topology view: leader card on top, agent nodes grouped by node role below
// (grid implies the hierarchy — no graph lib). Stat tiles read real
// ClusterMaster.get_stats() fields.
const ROLE_ORDER: string[] = ["heavy", "general", "worker"];

const ROLE_LABELS: Record<string, string> = {
    heavy: "Heavy tier",
    general: "General tier",
    worker: "Worker tier",
};

function LeaderCard({ stats }: { stats: ClusterStats | null }) {
    const leader = stats?.is_leader ?? false;
    return (
        <div className="rounded-card border border-border bg-bg-elevated p-4">
            <div className="flex items-center justify-between gap-2">
                <div className="text-sm text-text-primary">Cluster master</div>
                <StatusBadge
                    status={leader ? "success" : "warning"}
                    label={leader ? "leader" : "standby"}
                />
            </div>
            <div className="mt-2 grid grid-cols-3 gap-2 font-mono text-xs tabular-nums text-text-secondary">
                <div>
                    <div className="text-text-muted">leader_id</div>
                    <div className="truncate text-text-primary">
                        {stats?.leader_id || "-"}
                    </div>
                </div>
                <div>
                    <div className="text-text-muted">epoch</div>
                    <div className="text-text-primary">
                        {stats?.epoch ?? "-"}
                    </div>
                </div>
                <div>
                    <div className="text-text-muted">cluster_view</div>
                    <div className="text-text-primary">
                        {nodesClusterView(stats)}
                    </div>
                </div>
            </div>
        </div>
    );
}

function nodesClusterView(stats: ClusterStats | null): string {
    if (!stats) return "-";
    // /api/nodes carries cluster_view; stats response doesn't — derive a
    // display hint from epoch/is_leader instead of a fake value.
    return stats.is_leader ? "authoritative" : "synced";
}

export function TopologyPage() {
    const nodesQ = useAutoRetry(() => api.nodes(), []);
    const statsQ = useAutoRetry(() => api.clusterStats(), []);

    const nodes = nodesQ.data?.nodes ?? [];
    const stats = statsQ.data;

    const byRole = useMemo(() => {
        const map = new Map<string, NodeEntry[]>();
        for (const n of nodes) {
            const k = n.role ?? "worker";
            const arr = map.get(k);
            if (arr) arr.push(n);
            else map.set(k, [n]);
        }
        return ROLE_ORDER.flatMap((r) => {
            const entries = map.get(r);
            return entries ? [[r, entries] as const] : [];
        });
    }, [nodes]);

    if (nodesQ.loading && !nodesQ.data)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader title="Topology" />
                <SkeletonLines rows={6} />
            </div>
        );
    if (nodesQ.error)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader title="Topology" />
                <ErrorState
                    message={nodesQ.error.message}
                    onRetry={nodesQ.manualRetry}
                />
            </div>
        );
    if (nodes.length === 0)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader title="Topology" />
                <EmptyState
                    title="No nodes registered"
                    description="Start node agents (fusion-multi-node node start --role agent) and they will register with the master."
                />
            </div>
        );

    return (
        <div className="mx-auto max-w-5xl px-4">
            <PageHeader
                title="Topology"
                subtitle={
                    stats
                        ? `${stats.online_nodes}/${stats.total_nodes} nodes online · epoch ${stats.epoch} · leader ${stats.leader_id || "-"}`
                        : undefined
                }
            />
            {nodesQ.data?.partitioned ? (
                <div className="mb-3 rounded-card border border-warning bg-warning/10 px-3 py-2 text-xs text-warning">
                    脑裂视图 — 本 master 非权威 (partitioned), 全局写操作应禁用
                </div>
            ) : null}
            <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
                <MetricTile
                    label="Nodes online"
                    value={`${stats?.online_nodes ?? 0}/${stats?.total_nodes ?? 0}`}
                />
                <MetricTile
                    label="Tasks running"
                    value={stats?.active_tasks ?? 0}
                />
                <MetricTile
                    label="Tasks completed"
                    value={stats?.completed_tasks ?? 0}
                />
                <MetricTile
                    label="Tasks failed"
                    value={stats?.failed_tasks ?? 0}
                />
            </div>

            <div className="mb-3">
                <LeaderCard stats={stats ?? null} />
            </div>

            {byRole.map(([role, entries]) => (
                <section key={role} className="mb-4">
                    <div className="mb-2 text-sm text-text-secondary">
                        {ROLE_LABELS[role] ?? role}
                        <span className="ml-2 text-xs text-text-muted">
                            {
                                entries.filter((n) => n.status === "online")
                                    .length
                            }
                            /{entries.length} online
                        </span>
                    </div>
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                        {entries.map((n) => (
                            <a
                                key={n.node_id}
                                href={`#/nodes/${encodeURIComponent(n.node_id)}`}
                                className="block rounded-card border border-border bg-bg-elevated p-3 hover:bg-bg-muted/50"
                            >
                                <div className="flex items-start justify-between gap-2">
                                    <div className="min-w-0">
                                        <div className="truncate font-mono text-sm text-text-primary">
                                            {n.node_id}
                                        </div>
                                        <div className="mt-1 truncate text-xs text-text-muted">
                                            {n.hostname} ·{" "}
                                            {n.device_model ?? "-"}
                                        </div>
                                    </div>
                                    <StatusBadge
                                        status={nodeStatusKind(n)}
                                        label={n.status}
                                    />
                                </div>
                                <div className="mt-2 flex justify-between font-mono text-xs tabular-nums text-text-secondary">
                                    <span>
                                        {n.active_tasks}/{n.max_tasks} tasks
                                    </span>
                                    <span>{n.score.toFixed(2)} score</span>
                                </div>
                            </a>
                        ))}
                    </div>
                </section>
            ))}
        </div>
    );
}

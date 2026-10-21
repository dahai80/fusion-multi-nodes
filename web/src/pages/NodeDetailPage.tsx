import {
    EmptyState,
    ErrorState,
    KVPanel,
    MetricTile,
    SkeletonLines,
    StatusBadge,
    useAutoRetry,
} from "@fusion/web-ui";
import { useMemo } from "react";
import {
    type TaskEntry,
    api,
    formatGb,
    nodeStatusKind,
    taskStatusKind,
} from "../api";
import { PageHeader } from "../components/PageHeader";

// Node drilldown: hardware KV panel + resource tiles + recent tasks
// dispatched to this node (filtered client-side from /api/tasks).
export function NodeDetailPage({ nodeId }: { nodeId: string }) {
    const nodeQ = useAutoRetry(() => api.node(nodeId), [nodeId]);
    const tasksQ = useAutoRetry(() => api.tasks(200), []);

    const node = nodeQ.data;
    const nodeTasks = useMemo(
        () =>
            (tasksQ.data?.tasks ?? [])
                .filter((t) => t.assigned_nodes.includes(nodeId))
                .slice(0, 20),
        [tasksQ.data, nodeId],
    );

    if (nodeQ.loading && !nodeQ.data)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader title={nodeId} subtitle="Node detail" />
                <SkeletonLines rows={6} />
            </div>
        );
    if (nodeQ.error)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader title={nodeId} subtitle="Node detail" />
                <ErrorState
                    message={nodeQ.error.message}
                    onRetry={nodeQ.manualRetry}
                />
            </div>
        );

    return (
        <div className="mx-auto max-w-5xl px-4">
            <PageHeader
                title={node?.hostname || nodeId}
                subtitle={`${nodeId} · ${node?.ip_address ?? "-"}:${node?.port ?? "-"} · ${node?.device_model ?? "-"}`}
                actions={
                    node ? (
                        <StatusBadge
                            status={nodeStatusKind(node)}
                            label={node.status}
                        />
                    ) : undefined
                }
            />
            <a
                href="#/nodes"
                className="mb-3 inline-block text-xs text-brand hover:underline"
            >
                ← All nodes
            </a>

            <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
                <MetricTile label="CPU cores" value={node?.cpu_cores ?? "-"} />
                <MetricTile
                    label="Memory free"
                    value={formatGb(node?.available_memory_gb)}
                />
                <MetricTile
                    label="Active tasks"
                    value={node?.active_tasks ?? 0}
                />
                <MetricTile
                    label="Score"
                    value={(node?.score ?? 0).toFixed(2)}
                />
            </div>

            <div className="mb-4">
                <KVPanel
                    title="Hardware & cluster"
                    pairs={[
                        ["node_id", node?.node_id ?? "-"],
                        ["role", node?.role ?? "-"],
                        ["draining", String(node?.draining ?? false)],
                        ["resident", String(node?.resident ?? false)],
                        ["total_memory_gb", formatGb(node?.total_memory_gb)],
                        [
                            "available_memory_gb",
                            formatGb(node?.available_memory_gb),
                        ],
                        ["cpu_cores", String(node?.cpu_cores ?? "-")],
                        ["gpu_cores", String(node?.gpu_cores ?? "-")],
                        ["device_model", node?.device_model ?? "-"],
                        ["uma_size_gb", String(node?.uma_size_gb ?? "-")],
                        ["max_tasks", String(node?.max_tasks ?? "-")],
                        ["score", (node?.score ?? 0).toFixed(2)],
                        [
                            "last_heartbeat",
                            node?.last_heartbeat
                                ? new Date(
                                      node.last_heartbeat * 1000,
                                  ).toLocaleString()
                                : "-",
                        ],
                        ["drivers", (node?.drivers ?? []).join(", ") || "-"],
                        [
                            "supervisor",
                            String(node?.supervisor_available ?? false),
                        ],
                        ["epoch", String(node?.epoch ?? "-")],
                        ["leader_id", node?.leader_id ?? "-"],
                    ]}
                />
            </div>

            <div className="mb-2 text-sm text-text-secondary">Recent tasks</div>
            {nodeTasks.length === 0 ? (
                <EmptyState
                    title="No tasks on this node"
                    description="Tasks dispatched to this node will appear here."
                />
            ) : (
                <div className="flex flex-col gap-2">
                    {nodeTasks.map((t) => (
                        <TaskRow key={t.task_id} t={t} />
                    ))}
                </div>
            )}
        </div>
    );
}

function TaskRow({ t }: { t: TaskEntry }) {
    return (
        <div className="rounded-card border border-border bg-bg-elevated p-3">
            <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                    <div className="truncate font-mono text-xs text-text-primary">
                        {t.task_id}
                    </div>
                    <div className="mt-1 text-xs text-text-muted">
                        {t.name} · {t.mode} · {t.model_name || "-"}
                    </div>
                </div>
                <StatusBadge
                    status={taskStatusKind(t.status)}
                    label={t.status}
                />
            </div>
        </div>
    );
}

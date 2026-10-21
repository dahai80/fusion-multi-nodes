import {
    DataTable,
    EmptyState,
    ErrorState,
    SkeletonLines,
    StatusBadge,
    useAutoRetry,
} from "@fusion/web-ui";
import { useEffect, useMemo } from "react";
import { type NodeEntry, api, formatGb, nodeStatusKind } from "../api";
import { PageHeader } from "../components/PageHeader";

// Node inventory table (poll 10s): status/role/resources per node, row
// click drills into #/nodes/{id}. Four states (spec §6.1) via
// Skeleton/EmptyState/ErrorState + useAutoRetry backoff.
export function NodesPage() {
    const { data, error, loading, manualRetry } = useAutoRetry(
        () => api.nodes(),
        [],
    );

    // Poll refresh while mounted — heartbeats/status transitions land without reload.
    useEffect(() => {
        const timer = window.setInterval(() => manualRetry(), 10000);
        return () => window.clearInterval(timer);
    }, [manualRetry]);

    const nodes = data?.nodes ?? [];

    const columns = useMemo(
        () => [
            {
                key: "node_id",
                header: "Node",
                render: (n: NodeEntry) => (
                    <span className="font-mono text-xs">{n.node_id}</span>
                ),
            },
            {
                key: "status",
                header: "Status",
                render: (n: NodeEntry) => (
                    <StatusBadge status={nodeStatusKind(n)} label={n.status} />
                ),
            },
            {
                key: "role",
                header: "Role",
                render: (n: NodeEntry) => (
                    <span className="text-xs text-text-secondary">
                        {n.role}
                        {n.draining ? " · draining" : ""}
                    </span>
                ),
            },
            {
                key: "mem",
                header: "Mem (used/total)",
                align: "right" as const,
                sortable: true,
                render: (n: NodeEntry) => (
                    <span className="font-mono text-xs tabular-nums">
                        {formatGb(n.total_memory_gb - n.available_memory_gb)} /{" "}
                        {formatGb(n.total_memory_gb)}
                    </span>
                ),
            },
            {
                key: "tasks",
                header: "Active tasks",
                align: "right" as const,
                sortable: true,
                render: (n: NodeEntry) => (
                    <span className="font-mono text-xs tabular-nums">
                        {n.active_tasks}/{n.max_tasks}
                    </span>
                ),
            },
            {
                key: "score",
                header: "Score",
                align: "right" as const,
                sortable: true,
                render: (n: NodeEntry) => (
                    <span className="font-mono text-xs tabular-nums">
                        {n.score.toFixed(2)}
                    </span>
                ),
            },
            {
                key: "drivers",
                header: "Drivers",
                render: (n: NodeEntry) => (
                    <span className="text-xs text-text-muted">
                        {(n.drivers ?? []).length > 0
                            ? (n.drivers ?? []).join(", ")
                            : "-"}
                    </span>
                ),
            },
        ],
        [],
    );

    if (loading && !data)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader title="Nodes" />
                <SkeletonLines rows={6} />
            </div>
        );
    if (error)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader title="Nodes" />
                <ErrorState message={error.message} onRetry={manualRetry} />
            </div>
        );

    return (
        <div className="mx-auto max-w-5xl px-4">
            <PageHeader
                title="Nodes"
                subtitle={`${data?.online ?? 0}/${data?.total ?? 0} online · epoch ${data?.epoch ?? "-"} · leader ${data?.leader_id || "-"}`}
            />
            <DataTable
                columns={columns}
                rows={nodes}
                rowKey={(n) => n.node_id}
                renderCard={(n) => (
                    <a
                        href={`#/nodes/${encodeURIComponent(n.node_id)}`}
                        className="block"
                    >
                        <div className="flex items-start justify-between gap-2">
                            <span className="truncate font-mono text-sm text-text-primary">
                                {n.node_id}
                            </span>
                            <StatusBadge
                                status={nodeStatusKind(n)}
                                label={n.status}
                            />
                        </div>
                        <div className="mt-1 flex justify-between font-mono text-xs tabular-nums text-text-secondary">
                            <span>
                                {formatGb(
                                    n.total_memory_gb - n.available_memory_gb,
                                )}{" "}
                                / {formatGb(n.total_memory_gb)}
                            </span>
                            <span>
                                {n.active_tasks}/{n.max_tasks} tasks
                            </span>
                        </div>
                    </a>
                )}
                emptyState={
                    <EmptyState
                        title="No nodes registered"
                        description="Start node agents and they will register with the master over mDNS or manual join."
                    />
                }
            />
        </div>
    );
}

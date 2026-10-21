import {
    EmptyState,
    ErrorState,
    MetricChart,
    MetricTile,
    SkeletonLines,
    useAutoRetry,
} from "@fusion/web-ui";
import { useEffect, useRef, useState } from "react";
import { type ClusterMetrics, api, parseClusterMetrics } from "../api";
import { PageHeader } from "../components/PageHeader";

// Cluster metrics view: tiles from the parsed Prometheus exposition
// (/api/v1/metrics) + rolling time series. The master exposes only current
// gauges (no TSDB) — the client samples the endpoint on an interval and
// keeps a bounded in-memory window so MetricChart gets real {t, v} points.
const SAMPLE_MS = 15000;
const WINDOW_MS = 30 * 60 * 1000;
const MAX_POINTS = 120;

type SeriesKey =
    | "nodes_online"
    | "tasks_running"
    | "tasks_pending"
    | "memory_available_gb";

const SERIES: Array<{ key: SeriesKey; label: string; unit: string }> = [
    { key: "nodes_online", label: "Nodes online", unit: "" },
    { key: "tasks_running", label: "Tasks running", unit: "" },
    { key: "tasks_pending", label: "Tasks pending", unit: "" },
    { key: "memory_available_gb", label: "Cluster mem available", unit: "GB" },
];

interface Sample {
    t: number;
    v: Record<SeriesKey, number>;
}

export function MetricsPage() {
    const { data, error, loading, manualRetry } = useAutoRetry(
        () => api.metricsText(),
        [],
    );
    const [samples, setSamples] = useState<Sample[]>([]);
    const latest = useRef<ClusterMetrics | null>(null);

    // Feed the rolling window: each poll result appends one sample.
    useEffect(() => {
        if (!data) return;
        const m = parseClusterMetrics(data);
        latest.current = m;
        setSamples((prev) => {
            const t = Date.now();
            const next = [
                ...prev,
                {
                    t,
                    v: {
                        nodes_online: m.nodes_online,
                        tasks_running: m.tasks_running,
                        tasks_pending: m.tasks_pending,
                        memory_available_gb: m.memory_available_gb,
                    },
                },
            ];
            const cutoff = t - WINDOW_MS;
            const kept = next.filter((s) => s.t >= cutoff);
            return kept.slice(-MAX_POINTS);
        });
    }, [data]);

    if (loading && !data)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader
                    title="Metrics"
                    subtitle="cluster-level gauges from /api/v1/metrics"
                />
                <SkeletonLines rows={6} />
            </div>
        );
    if (error)
        return (
            <div className="mx-auto max-w-5xl px-4">
                <PageHeader title="Metrics" />
                <ErrorState message={error.message} onRetry={manualRetry} />
            </div>
        );

    const m = latest.current;
    const hasSamples = samples.length > 0;

    return (
        <div className="mx-auto max-w-5xl px-4">
            <PageHeader
                title="Metrics"
                subtitle={`Prometheus exposition · sampled every ${SAMPLE_MS / 1000}s · window ${WINDOW_MS / 60000}m`}
            />

            <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
                <MetricTile
                    label="Nodes online"
                    value={`${m?.nodes_online ?? 0}/${m?.nodes_total ?? 0}`}
                />
                <MetricTile
                    label="Tasks running"
                    value={m?.tasks_running ?? 0}
                />
                <MetricTile
                    label="Dispatch p90"
                    value={(m?.dispatch_latency_p90 ?? 0).toFixed(2)}
                    unit="s"
                />
                <MetricTile
                    label="Mem available"
                    value={(m?.memory_available_gb ?? 0).toFixed(1)}
                    unit="GB"
                />
            </div>

            {!hasSamples ? (
                <EmptyState
                    title="Collecting samples…"
                    description={`Time series build up locally as /api/v1/metrics is polled every ${SAMPLE_MS / 1000}s. Tiles above show live gauges now.`}
                />
            ) : (
                <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                    {SERIES.map(({ key, label, unit }) => (
                        <MetricChart
                            key={key}
                            series={[
                                {
                                    label,
                                    points: samples.map((s) => ({
                                        t: s.t,
                                        v: s.v[key],
                                    })),
                                },
                            ]}
                            unit={unit}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

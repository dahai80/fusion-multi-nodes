import { describe, expect, it } from "bun:test";
import {
    formatGb,
    nodeStatusKind,
    parseClusterMetrics,
    taskStatusKind,
} from "../src/api";

// parseClusterMetrics must read the master's real exposition
// (cluster_master.get_prometheus_metrics): gauges + dispatch latency
// summary with quantile labels; HELP/TYPE lines ignored; unknown metrics
// skipped (forward compat).
const SAMPLE_EXPO = `
# HELP fusion_cluster_nodes_total 集群注册节点总数
# TYPE fusion_cluster_nodes_total gauge
fusion_cluster_nodes_total 3
# TYPE fusion_cluster_nodes_online gauge
fusion_cluster_nodes_online 2
fusion_cluster_tasks_total 42
fusion_cluster_tasks_running 2
fusion_cluster_tasks_pending 1
fusion_cluster_tasks_completed 30
fusion_cluster_tasks_failed 5
fusion_cluster_tasks_partial 4
fusion_cluster_task_retries_total 7
fusion_cluster_kv_cache_entries 12
fusion_cluster_memory_total_gb 96.00
fusion_cluster_memory_available_gb 40.50
# TYPE fusion_cluster_dispatch_latency_seconds summary
fusion_cluster_dispatch_latency_seconds{quantile="0.5"} 0.1234
fusion_cluster_dispatch_latency_seconds{quantile="0.9"} 0.5678
fusion_cluster_dispatch_latency_seconds{quantile="0.99"} 1.2345
fusion_cluster_dispatch_latency_seconds_sum 9.9
fusion_cluster_dispatch_latency_seconds_count 30
fusion_cluster_banned_nodes 1
fusion_cluster_unknown_future_metric 5
`.trim();

describe("parseClusterMetrics", () => {
    it("parses all gauges from master exposition", () => {
        const m = parseClusterMetrics(SAMPLE_EXPO);
        expect(m.nodes_total).toBe(3);
        expect(m.nodes_online).toBe(2);
        expect(m.tasks_total).toBe(42);
        expect(m.tasks_running).toBe(2);
        expect(m.tasks_pending).toBe(1);
        expect(m.tasks_completed).toBe(30);
        expect(m.tasks_failed).toBe(5);
        expect(m.tasks_partial).toBe(4);
        expect(m.task_retries_total).toBe(7);
        expect(m.kv_cache_entries).toBe(12);
        expect(m.memory_total_gb).toBeCloseTo(96.0);
        expect(m.memory_available_gb).toBeCloseTo(40.5);
        expect(m.banned_nodes).toBe(1);
    });

    it("parses dispatch latency quantiles and skips sum/count", () => {
        const m = parseClusterMetrics(SAMPLE_EXPO);
        expect(m.dispatch_latency_p50).toBeCloseTo(0.1234);
        expect(m.dispatch_latency_p90).toBeCloseTo(0.5678);
        expect(m.dispatch_latency_p99).toBeCloseTo(1.2345);
    });

    it("returns zeros on empty/garbage input", () => {
        for (const text of ["", "# only comments", "garbage line", "a b c"]) {
            const m = parseClusterMetrics(text);
            expect(m.nodes_total).toBe(0);
            expect(m.tasks_total).toBe(0);
        }
    });

    it("last sample wins on duplicate metrics", () => {
        const m = parseClusterMetrics(
            "fusion_cluster_nodes_online 1\nfusion_cluster_nodes_online 5"
        );
        expect(m.nodes_online).toBe(5);
    });
});

describe("status kind mapping", () => {
    it("maps node status + draining to badge kinds", () => {
        expect(nodeStatusKind({ status: "online", draining: false })).toBe("success");
        expect(nodeStatusKind({ status: "busy", draining: false })).toBe("running");
        expect(nodeStatusKind({ status: "online", draining: true })).toBe("warning");
        expect(nodeStatusKind({ status: "draining", draining: true })).toBe("warning");
        expect(nodeStatusKind({ status: "offline", draining: false })).toBe("offline");
        expect(nodeStatusKind({ status: "future_status", draining: false })).toBe(
            "offline"
        );
    });

    it("maps task status to badge kinds", () => {
        expect(taskStatusKind("COMPLETED")).toBe("success");
        expect(taskStatusKind("RUNNING")).toBe("running");
        expect(taskStatusKind("PENDING")).toBe("warning");
        expect(taskStatusKind("PARTIAL")).toBe("warning");
        expect(taskStatusKind("FAILED")).toBe("error");
        expect(taskStatusKind("TIMEOUT")).toBe("error");
        expect(taskStatusKind("CANCELLED")).toBe("error");
        expect(taskStatusKind("MIGRATED")).toBe("offline");
    });
});

describe("formatGb", () => {
    it("formats finite values and dashes undefined", () => {
        expect(formatGb(12.34)).toBe("12.3 GB");
        expect(formatGb(undefined)).toBe("-");
        expect(formatGb(Number.NaN)).toBe("-");
    });
});

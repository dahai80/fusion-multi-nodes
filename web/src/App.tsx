import { ReadOnlyBanner } from "@fusion/web-ui";
import { useEffect, useState } from "react";
import { MetricsPage } from "./pages/MetricsPage";
import { NodeDetailPage } from "./pages/NodeDetailPage";
import { NodesPage } from "./pages/NodesPage";
import { TopologyPage } from "./pages/TopologyPage";
import { embeddedParam, isEmbedded } from "./sdk";

// Hash router (no router dep). Standalone shows a bottom tab bar; embedded
// (?embedded=1 or iframe) the shell owns the chrome so the module hides its
// bottom nav and shows a minimal top tab bar instead (mlx #1277 pattern —
// the shell menu has only a single module-root entry, so page-level nav
// must stay reachable inside the iframe).
type Route = "topology" | "nodes" | "metrics";

type DetailRoute =
    | { page: "detail"; nodeId: string }
    | { page: Exclude<Route, never> };

function parseHash(): DetailRoute {
    const h = window.location.hash.replace(/^#\/?/, "");
    const m = h.match(/^nodes\/(.+)$/);
    if (m?.[1]) return { page: "detail", nodeId: decodeURIComponent(m[1]) };
    if (h === "nodes" || h === "metrics" || h === "topology")
        return { page: h };
    return { page: "topology" };
}

export function App() {
    const [route, setRoute] = useState<DetailRoute>(parseHash());

    useEffect(() => {
        const onHash = () => setRoute(parseHash());
        window.addEventListener("hashchange", onHash);
        return () => window.removeEventListener("hashchange", onHash);
    }, []);

    const embedded = embeddedParam() || isEmbedded();

    // mobile-readonly (spec §8.2): monitoring-only module; on narrow
    // viewports inside the shell surface the readonly banner (no write ops
    // exist to disable).
    const [narrow, setNarrow] = useState(
        typeof window !== "undefined"
            ? window.matchMedia("(max-width: 767px)").matches
            : false,
    );

    useEffect(() => {
        const mq = window.matchMedia("(max-width: 767px)");
        const on = () => setNarrow(mq.matches);
        mq.addEventListener("change", on);
        return () => mq.removeEventListener("change", on);
    }, []);

    return (
        <div className={embedded ? "h-full" : "flex h-full flex-col"}>
            {embedded && narrow ? (
                <ReadOnlyBanner text="只读模式 — 集群管理请用桌面端" />
            ) : null}
            {embedded && (
                <EmbeddedTabs
                    route={route.page === "detail" ? "nodes" : route.page}
                    onChange={setRoute}
                />
            )}
            <main className={`min-h-0 flex-1 ${embedded ? "" : "pb-16 pt-4"}`}>
                {route.page === "topology" && <TopologyPage />}
                {route.page === "nodes" && <NodesPage />}
                {route.page === "detail" && (
                    <NodeDetailPage nodeId={route.nodeId} />
                )}
                {route.page === "metrics" && <MetricsPage />}
            </main>
            {!embedded && (
                <BottomNav
                    route={route.page === "detail" ? "nodes" : route.page}
                    onChange={setRoute}
                />
            )}
        </div>
    );
}

// Page-level nav inside the shell iframe (see header comment).
function EmbeddedTabs({
    route,
    onChange,
}: {
    route: Route;
    onChange: (r: DetailRoute) => void;
}) {
    const items: Array<{ id: Route; label: string }> = [
        { id: "topology", label: "Topology" },
        { id: "nodes", label: "Nodes" },
        { id: "metrics", label: "Metrics" },
    ];
    return (
        <nav className="flex shrink-0 border-b border-border bg-bg-elevated">
            {items.map((it) => (
                <button
                    key={it.id}
                    type="button"
                    onClick={() => onChange({ page: it.id })}
                    className={`flex-1 py-2 text-center text-xs uppercase tracking-wide ${
                        route === it.id
                            ? "border-b-2 border-brand text-brand"
                            : "text-text-secondary"
                    }`}
                >
                    {it.label}
                </button>
            ))}
        </nav>
    );
}

function BottomNav({
    route,
    onChange,
}: {
    route: Route;
    onChange: (r: DetailRoute) => void;
}) {
    const items: Array<{ id: Route; label: string }> = [
        { id: "topology", label: "Topology" },
        { id: "nodes", label: "Nodes" },
        { id: "metrics", label: "Metrics" },
    ];
    return (
        <nav className="fixed inset-x-0 bottom-0 z-10 flex border-t border-border bg-bg-elevated">
            {items.map((it) => (
                <button
                    key={it.id}
                    type="button"
                    onClick={() => onChange({ page: it.id })}
                    className={`flex-1 py-3 text-center text-sm ${
                        route === it.id ? "text-brand" : "text-text-secondary"
                    }`}
                >
                    {it.label}
                </button>
            ))}
        </nav>
    );
}

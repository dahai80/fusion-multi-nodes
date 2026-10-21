import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// #81: dev server 11472 (architecture/port-registry.yaml). Proxies the
// master API surface (/api/*) to the cluster master on 11452 so standalone
// mode mirrors the embedded gateway /shell/fusion-multi-nodes/ layout
// (same-origin, no CORS). FUSION_DEV_BACKEND / FUSION_MULTINODE_WEB_PORT
// override for parity.
const backend = process.env.FUSION_DEV_BACKEND ?? "http://127.0.0.1:11452";
const port = Number(process.env.FUSION_MULTINODE_WEB_PORT ?? 11472);

const apiPaths = ["/api"];

export default defineConfig({
    plugins: [react()],
    // Embedded deployment serves dist through the gateway sub-path
    // /shell/fusion-multi-nodes/ — absolute /assets would 404 there.
    // Relative base + hash router (#/...) resolves assets against the
    // module root in both embedded and standalone modes.
    base: "./",
    server: {
        port,
        strictPort: true,
        proxy: Object.fromEntries(
            apiPaths.map((p) => [p, { target: backend, changeOrigin: true }]),
        ),
    },
    preview: {
        port,
        strictPort: true,
        proxy: Object.fromEntries(
            apiPaths.map((p) => [p, { target: backend, changeOrigin: true }]),
        ),
    },
    envPrefix: ["VITE_", "FUSION_"],
});

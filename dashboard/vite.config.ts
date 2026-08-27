import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dashboard talks to observer (WebSocket + /queues) and to order-service
// (POST /orders). Vite proxies those so the browser only ever hits :5173.
//
// Targets differ by where the dashboard runs:
//   - inside docker compose  -> service names (observer:8001, order-service:8000)
//   - running `npm run dev` on the host -> localhost
// Compose sets VITE_PROXY_* env vars; the fallback is the host case.
const observerTarget = process.env.VITE_PROXY_OBSERVER ?? "http://localhost:8001";
const ordersTarget = process.env.VITE_PROXY_ORDERS ?? "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  // The container mounts ./dashboard as a volume, and switching git branches
  // rewrites files in bulk. Vite's on-disk transform cache doesn't always
  // notice, and serves a stale (sometimes truncated) compile. Force a fresh
  // optimize on every start — trivial cost for a project this size, and it
  // makes `docker compose restart dashboard` a reliable fix.
  optimizeDeps: { force: true },
  server: {
    port: 5173,
    host: true,
    watch: { usePolling: true },
    proxy: {
      "/api/observer": {
        target: observerTarget,
        rewrite: (p) => p.replace(/^\/api\/observer/, ""),
        ws: true,
      },
      "/api/orders": {
        target: ordersTarget,
        rewrite: (p) => p.replace(/^\/api\/orders/, ""),
      },
    },
  },
});

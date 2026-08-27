import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Vite dev server proxies /api/* + /rest/v1/* + /assets/* to the FastAPI
// backend on port 8000 so the frontend "just works" in dev mode:
//   Terminal 1:  cd backend && python -m uvicorn api_ae:app --port 8000
//   Terminal 2:  cd frontend && npm run dev    (opens http://localhost:5173)
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api":     { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/rest/v1": { target: "http://127.0.0.1:8000", changeOrigin: true },
    },
  },
  build: {
    // Emit to backend-adjacent dir so uvicorn can serve the build output
    // directly via StaticFiles in production. Wire this up when we're
    // ready to cut over from the vanilla dashboard.
    outDir: "../dist",
    emptyOutDir: true,
    sourcemap: false,
  },
});

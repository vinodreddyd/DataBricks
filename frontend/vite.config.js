import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, frontend on :5173 proxies /api → FastAPI on :8000
// In prod, the build output goes into backend/static and FastAPI serves it.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:8000",
    },
  },
  build: {
    outDir: "../backend/static",
    emptyOutDir: true,
  },
});

import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: { target: "es2022", sourcemap: false, chunkSizeWarningLimit: 600 },
  test: { environment: "node", include: ["tests/**/*.test.ts"] },
});

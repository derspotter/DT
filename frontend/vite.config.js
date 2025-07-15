import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    port: 5173,
    host: '0.0.0.0', // Allow external access when running in Docker
    watch: {
      usePolling: true, // Important inside Docker containers
    },
    hmr: {
      clientPort: 5173 // Ensures HMR works through Docker
    }
  },
  build: {
    outDir: 'dist',
  }
})
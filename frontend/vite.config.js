import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    port: 3000, // Use the exposed port
    host: '0.0.0.0', // Allow external access when running in Docker
    watch: {
      usePolling: true, // Important inside Docker containers
    },
    hmr: {
      clientPort: 3000 // Tell browser client to use the exposed port
    },
    proxy: {
      // Proxy API requests to the backend container
      '/api': {
        target: 'http://backend:4000',
        changeOrigin: true, // Recommended for virtual hosts
        // No rewrite needed if backend routes already start with /api
      }
    }
  },
  build: {
    outDir: 'dist',
  }
})
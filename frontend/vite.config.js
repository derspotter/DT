import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// https://vite.dev/config/
const devProxyTarget = process.env.VITE_DEV_PROXY_TARGET || 'http://localhost:4000'
const hmrHost = process.env.VITE_HMR_HOST
const hmrProtocol = process.env.VITE_HMR_PROTOCOL
const hmrClientPort = process.env.VITE_HMR_CLIENT_PORT ? Number(process.env.VITE_HMR_CLIENT_PORT) : undefined

const hmr = hmrHost
  ? {
      host: hmrHost,
      protocol: hmrProtocol || 'ws',
      clientPort: hmrClientPort,
    }
  : {
      clientPort: 5175,
    }

export default defineConfig({
  plugins: [svelte()],
  server: {
    port: 5175,
    strictPort: true,
    host: '0.0.0.0',
    watch: {
      usePolling: true,
    },
    hmr,
    proxy: {
      '/api': {
        target: devProxyTarget,
        changeOrigin: true,
      },
    },
  },
})

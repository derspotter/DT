import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// https://vite.dev/config/
const devProxyTarget = process.env.VITE_DEV_PROXY_TARGET || 'http://localhost:4000'
const disableHmr = ['1', 'true', 'yes'].includes(String(process.env.VITE_DISABLE_HMR || '').trim().toLowerCase())
const hmrHost = process.env.VITE_HMR_HOST
const hmrProtocol = process.env.VITE_HMR_PROTOCOL
const hmrClientPort = process.env.VITE_HMR_CLIENT_PORT ? Number(process.env.VITE_HMR_CLIENT_PORT) : undefined
const allowedHosts = String(process.env.VITE_ALLOWED_HOSTS || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean)

// Vite's dev server treats any extension-less URL as a JS module request, so a
// probe for /Dockerfile reaches vite:import-analysis, fails to parse, and the
// 500 it returns embeds the file's full source. Deny the extension-less config
// files at the frontend root so those probes get a 403 instead. This is a
// stopgap for genkia.de still running `target: dev`; the real fix is serving the
// built `runner` stage in production. Repeats Vite's defaults because setting
// `deny` replaces them rather than extending them.
const fsDeny = [
  '.env',
  '.env.*',
  '*.{crt,pem}',
  '**/.git/**',
  'Dockerfile',
  'Dockerfile.*',
  '.dockerignore',
]

const hmr = disableHmr
  ? false
  : hmrHost
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
    allowedHosts,
    fs: {
      deny: fsDeny,
    },
    // No forced polling. The bundled chokidar 3 lets CHOKIDAR_USEPOLLING
    // override this option (node_modules/vite/dist/node/chunks/config.js:13887-13893):
    // the dev containers set it to 1 for their bind mount (docker-compose.yml:169,
    // docker-compose.dev.yml:30), so they keep polling; Playwright and CI leave
    // it unset, so the host uses native file events.
    watch: {
      usePolling: false,
    },
    hmr,
    proxy: {
      '/api': {
        target: devProxyTarget,
        changeOrigin: true,
        ws: true,
      },
    },
  },
})

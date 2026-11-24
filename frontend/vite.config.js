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

// chokidar 3's reading of CHOKIDAR_USEPOLLING: '0'/'false' off, '1'/'true' on,
// any other non-empty value on, unset or empty off (case-insensitive, untrimmed).
function pollingFromEnv(value) {
  if (value === undefined) return false
  const lower = value.toLowerCase()
  if (lower === 'false' || lower === '0') return false
  return lower.length > 0
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
    // No forced polling. CHOKIDAR_USEPOLLING turns it on: the dev containers set
    // it to 1 for their bind mount (docker compose), so they keep polling;
    // Playwright and CI leave it unset, so the host uses native file events.
    // The option itself carries the value (read with chokidar's own rule)
    // because chokidar chooses FSEvents on macOS from the option, before its
    // environment override applies.
    watch: {
      usePolling: pollingFromEnv(process.env.CHOKIDAR_USEPOLLING),
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

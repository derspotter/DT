import { defineConfig } from '@playwright/test'

const frontendPort = Number(process.env.E2E_FRONTEND_PORT || 5175)

export default defineConfig({
  testDir: './tests',
  timeout: 30_000,
  use: {
    baseURL: `http://localhost:${frontendPort}`,
    headless: true,
  },
  webServer: {
    command: `npm run dev -- --host 0.0.0.0 --port ${frontendPort}`,
    url: `http://localhost:${frontendPort}`,
    reuseExistingServer: true,
    // vite.config.js pins hmr.clientPort to the dev stack's 5175 unless
    // VITE_HMR_HOST is set (VITE_HMR_CLIENT_PORT alone is ignored); point the
    // test server's HMR client at the test server itself instead.
    env: { ...process.env, VITE_HMR_HOST: 'localhost', VITE_HMR_CLIENT_PORT: String(frontendPort) },
    timeout: 120_000,
  },
})

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
    timeout: 120_000,
  },
})

import type { Page } from '@playwright/test'

// Mocks the page's two WebSockets so a mocked spec never reaches a live server.
//
// - `/api/ws`, the app's log socket: a faked token would be rejected by a
//   live backend (close 4401 signs the page out).
// - Vite's HMR socket (`ws://<host>:<clientPort>/?token=…`, protocol
//   `vite-hmr`): vite.config.js pins `hmr.clientPort: 5175`, so a test server
//   on another port (E2E_FRONTEND_PORT=5190) points its HMR client at the dev
//   stack's :5175, which rejects the foreign token and closes the socket
//   before it opens. Vite's client then throws an unhandled
//   "WebSocket closed without opened." page error.
//
// Both mocks stay open and silent: no server connection, no close.
export async function mockLogSocket(page: Page) {
  await page.routeWebSocket('**/api/ws', () => {})
  await page.routeWebSocket(
    (url) => url.pathname === '/' && url.searchParams.has('token'),
    () => {},
  )
}

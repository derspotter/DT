import type { Page, Route, WebSocketRoute } from '@playwright/test'

export function fulfil(route: Route, body: unknown, status = 200) {
  return route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })
}

type CorpusEntry = { id: number; name: string; role: string; owner_username: string }

// The `/api/auth/me` payload. `corpusId` is `last_corpus_id` (pass `null` for
// a user without a corpus); `corpora` defaults to the one local corpus.
export function mePayload({
  id = 1,
  username = 'admin',
  admin = true,
  corpusId = 1,
  corpora = [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
}: {
  id?: number
  username?: string
  admin?: boolean
  corpusId?: number | null
  corpora?: CorpusEntry[]
} = {}) {
  return {
    user: { id, username, last_corpus_id: corpusId, is_admin: admin },
    corpora,
  }
}

// Mocks the page's two WebSockets so a mocked spec never reaches a live server.
//
// - `/api/ws`, the app's log socket: a faked token would be rejected by a
//   live backend (close 4401 signs the page out).
// - Vite's HMR socket (`ws://<host>:<clientPort>/?token=…`, protocol
//   `vite-hmr`): vite.config.js pins `hmr.clientPort: 5175`, so a test server
//   on another port (E2E_FRONTEND_PORT=5190) points its HMR client at the dev
//   stack's :5175, which rejects the foreign token and closes the socket
//   before it opens. Vite's client then throws an unhandled
//   "WebSocket closed without opened." page error. playwright.config.ts now
//   fixes the cause (VITE_HMR_CLIENT_PORT = the test server's port); this
//   mock stays as belt-and-braces for a reused server started without it.
//
// Both mocks stay open and silent: no server connection, no close.
// `onLogSocket` receives the log socket's route, so a spec can push frames
// with `ws.send(...)`.
export async function mockLogSocket(page: Page, onLogSocket: (ws: WebSocketRoute) => void = () => {}) {
  await page.routeWebSocket('**/api/ws', onLogSocket)
  await page.routeWebSocket(
    (url) => url.pathname === '/' && url.searchParams.has('token'),
    () => {},
  )
}

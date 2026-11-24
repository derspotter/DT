import { expect, type Page, type Route, type WebSocketRoute } from '@playwright/test'

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

// Signs in through the login form (any password; the spec's route answers).
export async function logIn(page: Page, username: string) {
  const form = page.getByTestId('login-form')
  await expect(form).toBeVisible()
  await form.getByLabel('Username').fill(username)
  await form.getByLabel('Password').fill('secret-password')
  await form.getByRole('button', { name: 'Sign in' }).click()
}

// Checkpoints for race tests: no fixed waits, no hook in the app.
//
// `trackFetches` wraps `window.fetch` before the app loads and counts, per
// request key (`pathname + search`), in `window.__dtFetch`:
// - `calls`: each `fetch` call, synchronously when the app makes it;
// - `reads`: each settled body read (`json`, `blob`, `text`, `arrayBuffer`),
//   counted just before the promise the app awaits settles.
// `afterBodyRead` waits for one more read than `previousReads` and then
// drains two `setTimeout(0)` rounds, so the app's continuation after that
// read, up to its next await on a request, has run.
//
// For causal checkpoints each call also gets a page-wide sequence number,
// sent to the route as the `x-dt-fetch-seq` request header (string or URL
// inputs only), and `events` keeps calls and reads in order.
// `firstCallAfterRead(readPath, callPath)` returns the sequence number of
// the first `callPath` call made after the latest `readPath` read: the
// request the app sent in its continuation after reading that body.
export const FETCH_SEQ_HEADER = 'x-dt-fetch-seq'
type FetchMatch = { path: string } | { source: string; flags: string }

function fetchMatch(match: string | RegExp): FetchMatch {
  if (typeof match === 'string') return { path: match }
  return { source: match.source, flags: match.flags }
}

export async function trackFetches(page: Page) {
  await page.addInitScript(() => {
    const w = window as any
    if (w.__dtFetch) return
    const calls: Record<string, number> = {}
    const reads: Record<string, number> = {}
    const events: { kind: 'call' | 'read'; key: string; seq: number }[] = []
    let lastSeq = 0
    const bump = (bag: Record<string, number>, key: string) => { bag[key] = (bag[key] || 0) + 1 }
    const pathOf = (key: string) => key.split('?')[0]
    w.__dtFetch = {
      calls,
      reads,
      events,
      firstCallAfterRead(readPath: string, callPath: string) {
        let last = -1
        events.forEach((event, i) => { if (event.kind === 'read' && pathOf(event.key) === readPath) last = i })
        if (last < 0) return null
        const hit = events.slice(last + 1).find((event) => event.kind === 'call' && pathOf(event.key) === callPath)
        return hit ? hit.seq : null
      },
      stats(spec: { path?: string; source?: string; flags?: string }) {
        const re = spec.source !== undefined ? new RegExp(spec.source, spec.flags) : null
        // lastIndex is reset per key, so `g` and `y` keep their meaning for
        // one key (`y`: anchored at offset 0) without carrying over.
        const hit = (key: string) => {
          if (!re) return key.split('?')[0] === spec.path
          re.lastIndex = 0
          return re.test(key)
        }
        const sum = (bag: Record<string, number>) =>
          Object.entries(bag).reduce((total, [key, n]) => (hit(key) ? total + n : total), 0)
        return { calls: sum(calls), reads: sum(reads) }
      },
    }
    const realFetch = window.fetch.bind(window)
    window.fetch = function (input: RequestInfo | URL, init?: RequestInit) {
      let key = ''
      try {
        const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
        const url = new URL(raw, window.location.href)
        key = url.pathname + url.search
      } catch {
        key = String(input)
      }
      const seq = ++lastSeq
      bump(calls, key)
      events.push({ kind: 'call', key, seq })
      let request: [RequestInfo | URL, RequestInit | undefined] = [input, init]
      if (typeof input === 'string' || input instanceof URL) {
        const headers = new Headers(init?.headers)
        headers.set('x-dt-fetch-seq', String(seq))
        request = [input, { ...init, headers }]
      }
      return realFetch(...request).then((response) => {
        const read = () => { bump(reads, key); events.push({ kind: 'read', key, seq }) }
        for (const method of ['json', 'blob', 'text', 'arrayBuffer'] as const) {
          const original = (response[method] as () => Promise<unknown>).bind(response)
          ;(response as any)[method] = () => original().then(
            (value) => { read(); return value },
            (error) => { read(); throw error },
          )
        }
        return response
      })
    }
  })
}

export async function fetchStats(page: Page, match: string | RegExp): Promise<{ calls: number; reads: number }> {
  return page.evaluate((spec) => (window as any).__dtFetch.stats(spec), fetchMatch(match))
}

export async function afterBodyRead(page: Page, match: string | RegExp, previousReads: number) {
  await page.waitForFunction(
    ({ spec, previous }) => (window as any).__dtFetch.stats(spec).reads > previous,
    { spec: fetchMatch(match), previous: previousReads },
  )
  await page.evaluate(() => new Promise((r) => setTimeout(() => setTimeout(r, 0), 0)))
}

// Waits until every request matching `match` made so far has had its body
// read (at least one), then drains as `afterBodyRead` does: none is in
// flight, so an app-side in-flight join cannot absorb the next request.
export async function settleReads(page: Page, match: string | RegExp) {
  await expect.poll(async () => {
    const stats = await fetchStats(page, match)
    return stats.calls > 0 && stats.reads === stats.calls
  }).toBe(true)
  await afterBodyRead(page, match, (await fetchStats(page, match)).reads - 1)
}

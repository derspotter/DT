import { expect, test, type Route } from '@playwright/test'

// Boot cost: the workspace data (seeds, corpus, counts) must be requested
// exactly once per page load, and must not queue behind the auth round trip —
// the backend scopes those requests to the token's corpus on its own.

function fulfil(route: Route, body: unknown) {
  return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })
}

function mockApi(route: Route, log: { path: string; at: number }[], authDelayMs: number, done: { auth: number }) {
  const path = new URL(route.request().url()).pathname
  log.push({ path, at: Date.now() })
  if (path === '/api/auth/me') {
    return new Promise<void>((resolve) => setTimeout(resolve, authDelayMs)).then(() => {
      done.auth = Date.now()
      return fulfil(route, {
        user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: false },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      })
    })
  }
  if (path === '/api/corpus') return fulfil(route, { items: [{ id: 7, work_id: 7, title: 'Boot item', authors: 'A. Author', year: 2024 }], total: 1, source: 'api', stage_totals: {} })
  if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
  if (path === '/api/openalex/quota') return fulfil(route, { available: false })
  if (path === '/api/seed/sources') return fulfil(route, { sources: [], total: 0, source: 'api' })
  if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
  if (path === '/api/ingest/stats') return fulfil(route, { stats: {} })
  return fulfil(route, {})
}

test('boot requests each workspace resource once and does not wait for the auth check', async ({ page }) => {
  const log: { path: string; at: number }[] = []
  const done = { auth: 0 }
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', (route) => mockApi(route, log, 250, done))
  await page.goto('/#/workspace')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  // The early corpus response (issued before the corpus id was known) must render, not be dropped as stale.
  await expect(page.locator('.corpus-select-row')).toHaveCount(1)
  // Let the post-bootstrap tab refresh (if any) fire before counting.
  await page.waitForTimeout(1200)

  const count = (p: string) => log.filter((entry) => entry.path === p).length
  expect(count('/api/auth/me'), 'auth check').toBe(1)
  expect(count('/api/seed/sources'), 'seed list').toBe(1)
  expect(count('/api/corpus'), 'corpus list').toBe(1)
  expect(count('/api/ingest/stats'), 'ingest stats').toBe(1)
  expect(count('/api/ingest/runs'), 'ingest runs').toBe(1)

  const firstSeeds = log.find((entry) => entry.path === '/api/seed/sources')!
  const firstCorpus = log.find((entry) => entry.path === '/api/corpus')!
  expect(done.auth, 'auth check answered').toBeGreaterThan(0)
  expect(firstSeeds.at, 'seed request issued before auth answered').toBeLessThan(done.auth)
  expect(firstCorpus.at, 'corpus request issued before auth answered').toBeLessThan(done.auth)
})

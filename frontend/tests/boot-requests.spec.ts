import { expect, test, type Route } from '@playwright/test'
import { fulfil, mePayload, mockLogSocket } from './helpers/mockApi'

// Boot cost: the workspace data (seeds, corpus, counts) must be requested
// exactly once per page load, and must not queue behind the auth round trip —
// the backend scopes those requests to the token's corpus on its own.

function mockApi(route: Route, log: { path: string; at: number }[], authDelayMs: number, done: { auth: number }) {
  const path = new URL(route.request().url()).pathname
  log.push({ path, at: Date.now() })
  if (path === '/api/auth/me') {
    return new Promise<void>((resolve) => setTimeout(resolve, authDelayMs)).then(() => {
      done.auth = Date.now()
      return fulfil(route, mePayload({ username: 'reader', admin: false }))
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
  await mockLogSocket(page)
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

// D15: the Graph and Scraper tabs load their component code on first open.
const componentRequested = (urls: string[], name: string) =>
  urls.some((url) => new URL(url).pathname.endsWith(`/src/components/${name}.svelte`))

async function bootWorkspace(page: import('@playwright/test').Page, urls: string[]) {
  const log: { path: string; at: number }[] = []
  page.on('request', (request) => urls.push(request.url()))
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', (route) => mockApi(route, log, 0, { auth: 0 }))
  await page.goto('/#/workspace')
  await expect.poll(() => log.some((entry) => entry.path === '/api/corpus')).toBe(true)
  await expect(page.locator('.corpus-select-row')).toHaveCount(1)
}

// First open of a lazy tab makes the dev server compile that component; on a
// cold CI runner this can take longer than the 5 s default.
const LAZY_TAB_TIMEOUT = 20_000

test('the graph and scraper code loads on first open only', async ({ page }) => {
  test.setTimeout(60_000)
  const urls: string[] = []
  await bootWorkspace(page, urls)
  expect(componentRequested(urls, 'ThreeGraph'), 'ThreeGraph at boot').toBe(false)
  expect(componentRequested(urls, 'ScraperLab'), 'ScraperLab at boot').toBe(false)

  await page.getByTestId('tab-graph').click()
  await expect(page.getByTestId('graph-3d-panel')).toBeVisible({ timeout: LAZY_TAB_TIMEOUT })
  expect(componentRequested(urls, 'ThreeGraph')).toBe(true)
  expect(componentRequested(urls, 'ScraperLab')).toBe(false)

  await page.getByTestId('tab-scraper').click()
  await expect(page.locator('.scraper-lab')).toBeVisible({ timeout: LAZY_TAB_TIMEOUT })
  expect(componentRequested(urls, 'ScraperLab')).toBe(true)
})

test('a failed tab import shows the error text and leaves the app usable', async ({ page }) => {
  const urls: string[] = []
  const consoleErrors: string[] = []
  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text())
  })
  await page.route('**/src/components/ThreeGraph.svelte*', (route) => route.abort())
  await bootWorkspace(page, urls)
  await page.getByTestId('tab-graph').click()
  await expect(page.getByText('Could not load this tab. Reload the page to try again.')).toBeVisible()
  // The import error itself reaches the console once, with the tab's name.
  await expect.poll(() => consoleErrors.filter((text) => text.includes('[tab-import] ThreeGraph')).length).toBe(1)
  await expect(page.getByTestId('graph-3d-panel')).toHaveCount(0)
  await page.getByTestId('tab-workspace').click()
  await expect(page.getByTestId('corpus-panel')).toBeVisible()
})

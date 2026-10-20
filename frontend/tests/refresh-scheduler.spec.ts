import { expect, test, type Page, type Route } from '@playwright/test'

// One adaptive refresh scheduler: 2 s while a search runs, 20 s idle, nothing
// while hidden, and a single trailing run per burst of worker log lines.

function fulfil(route: Route, body: unknown) {
  return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })
}

async function boot(page: Page, { running = false } = {}) {
  const counts = { corpus: 0 }
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') {
      return fulfil(route, {
        user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: false },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      })
    }
    if (path === '/api/corpus') {
      counts.corpus += 1
      return fulfil(route, { items: [{ id: 7, work_id: 7, title: 'Boot item', authors: 'A. Author', year: 2024 }], total: 1, source: 'api', stage_totals: {} })
    }
    if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
    if (path === '/api/openalex/quota') return fulfil(route, { available: false })
    if (path === '/api/seed/sources') {
      const sources = running
        ? [{ id: 1, title: 'Seed', name: 'Seed', run: { id: 5, status: 'running' }, status: 'running' }]
        : []
      return fulfil(route, { sources, total: sources.length, source: 'api' })
    }
    if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
    if (path === '/api/ingest/stats') return fulfil(route, { stats: {} })
    return fulfil(route, {})
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await page.waitForTimeout(1500)
  counts.corpus = 0
  return counts
}

async function setVisibility(page: Page, state: 'hidden' | 'visible') {
  await page.evaluate((value) => {
    Object.defineProperty(document, 'visibilityState', { value, configurable: true })
    document.dispatchEvent(new Event('visibilitychange'))
  }, state)
}

test('idle visible workspace polls at the idle cadence, not every 3 s', async ({ page }) => {
  const counts = await boot(page)
  await page.waitForTimeout(7000)
  expect(counts.corpus).toBeLessThanOrEqual(1)
})

test('a running search run polls every 2 s', async ({ page }) => {
  const counts = await boot(page, { running: true })
  await page.waitForTimeout(7000)
  expect(counts.corpus).toBeGreaterThanOrEqual(3)
})

test('a hidden tab does not poll, and becoming visible refreshes once', async ({ page }) => {
  const counts = await boot(page, { running: true })
  await setVisibility(page, 'hidden')
  counts.corpus = 0
  await page.waitForTimeout(5000)
  expect(counts.corpus).toBe(0)
  await setVisibility(page, 'visible')
  await page.waitForTimeout(1500)
  expect(counts.corpus).toBe(1)
})

test('a worker log burst triggers one trailing refresh, not one per line', async ({ page }) => {
  const counts = await boot(page)
  await page.evaluate(() => {
    const hook = (window as any).__dtRefresh
    for (let i = 0; i < 5; i += 1) setTimeout(() => hook.requestRefreshSoon(300), i * 20)
  })
  await page.waitForTimeout(2000)
  expect(counts.corpus).toBe(1)
})

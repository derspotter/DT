import { expect, test, type Page, type Route } from '@playwright/test'

// One adaptive refresh scheduler: 2 s while a search runs, 20 s idle, nothing
// while hidden, and a single trailing run per burst of worker log lines.

function fulfil(route: Route, body: unknown) {
  return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })
}

async function boot(
  page: Page,
  {
    running = false,
    hash = '#/workspace',
    corpusDelayMs = 0,
    admin = false,
    stats = {} as Record<string, number>,
    daemon = null as null | Record<string, unknown>,
  } = {},
) {
  const counts = { corpus: 0, pipelineStatus: 0, downloadStatus: 0, ingestRuns: 0 }
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') {
      return fulfil(route, {
        // `admin` as a username is an admin regardless of the flag (App.svelte userIsAdmin).
        user: { id: 1, username: admin ? 'admin' : 'reader', last_corpus_id: 1, is_admin: admin },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      })
    }
    if (path === '/api/corpus') {
      counts.corpus += 1
      if (corpusDelayMs) await new Promise((resolve) => setTimeout(resolve, corpusDelayMs))
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
    if (path === '/api/ingest/runs') {
      counts.ingestRuns += 1
      return fulfil(route, { runs: [], total: 0, source: 'api' })
    }
    if (path === '/api/ingest/stats') return fulfil(route, { stats })
    if (path === '/api/pipeline/daemon/status') {
      counts.pipelineStatus += 1
      return fulfil(route, daemon ? { daemon } : {})
    }
    if (path === '/api/downloads/worker/status') {
      counts.downloadStatus += 1
      return fulfil(route, {})
    }
    return fulfil(route, {})
  })
  await page.goto(`/${hash}`)
  if (hash === '#/workspace') await expect(page.getByTestId('seed-panel')).toBeVisible()
  await page.waitForTimeout(1500)
  counts.corpus = 0
  counts.pipelineStatus = 0
  counts.downloadStatus = 0
  counts.ingestRuns = 0
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
  // ...and polling did start: one cycle lands once the 20 s idle interval elapses.
  await page.waitForTimeout(14500)
  expect(counts.corpus).toBe(1)
})

test('switching from a tab without polling to the workspace starts polling', async ({ page }) => {
  test.setTimeout(60000)
  // #/graph is a real non-polling tab (#/corpus would normalize to the workspace).
  const counts = await boot(page, { hash: '#/graph' })
  await page.waitForTimeout(3000)
  expect(counts.corpus).toBe(0)
  await page.evaluate(() => { window.location.hash = '#/workspace' })
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await page.waitForTimeout(1500)
  counts.corpus = 0
  await page.waitForTimeout(21500)
  expect(counts.corpus).toBeGreaterThanOrEqual(1)
})

test('a non-admin watching enrichment polls every 2 s', async ({ page }) => {
  const counts = await boot(page, { stats: { enriching: 1, enriching_live: 1 } })
  await page.waitForTimeout(7000)
  expect(counts.corpus).toBeGreaterThanOrEqual(3)
})

test('abandoned in_progress claims (expired lease) do not keep a non-admin at 2 s', async ({ page }) => {
  const counts = await boot(page, { stats: { enriching: 4, downloading: 2, enriching_live: 0, downloading_live: 0 } })
  await page.waitForTimeout(7000)
  expect(counts.corpus).toBeLessThanOrEqual(1)
})

const idleDaemon = {
  workers: {
    enrich: { online: true, status: 'idle', last_seen_at: '2026-10-24T09:00:00Z', details: { state: 'polling' } },
    download: { online: true, status: 'idle', last_seen_at: '2026-10-24T09:00:00Z', details: { state: 'polling' } },
  },
}

test('an online but idle daemon does not put an admin on the 2 s cadence', async ({ page }) => {
  const counts = await boot(page, { admin: true, daemon: idleDaemon })
  await page.waitForTimeout(7000)
  expect(counts.corpus).toBeLessThanOrEqual(1)
})

test('a daemon with a batch in flight puts an admin on the 2 s cadence', async ({ page }) => {
  const busy = { workers: { ...idleDaemon.workers, enrich: { ...idleDaemon.workers.enrich, status: 'running' } } }
  const counts = await boot(page, { admin: true, daemon: busy })
  await page.waitForTimeout(7000)
  expect(counts.corpus).toBeGreaterThanOrEqual(3)
})

test('a daemon killed mid-batch (stale heartbeat still "running") does not count as active', async ({ page }) => {
  const dead = { workers: { ...idleDaemon.workers, enrich: { ...idleDaemon.workers.enrich, online: false, status: 'running' } } }
  const counts = await boot(page, { admin: true, daemon: dead })
  await page.waitForTimeout(7000)
  expect(counts.corpus).toBeLessThanOrEqual(1)
})

test('a log line that arrives while hidden is refreshed once the tab is visible again', async ({ page }) => {
  const counts = await boot(page)
  await setVisibility(page, 'hidden')
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(100))
  await page.waitForTimeout(2000)
  expect(counts.corpus).toBe(0)
  expect(counts.ingestRuns).toBe(0)
  await setVisibility(page, 'visible')
  await page.waitForTimeout(3000)
  // The dirty trailing cycle reloads the extracted-document list (ingest runs).
  expect(counts.ingestRuns).toBeGreaterThanOrEqual(1)
})

test('a log-triggered refresh on a non-polling tab discovers worker status (admin)', async ({ page }) => {
  const counts = await boot(page, { admin: true, hash: '#/logs' })
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(300))
  await page.waitForTimeout(1500)
  expect(counts.pipelineStatus).toBeGreaterThanOrEqual(1)
  expect(counts.downloadStatus).toBeGreaterThanOrEqual(1)
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

test('a log burst during an in-flight cycle still gets its trailing refresh', async ({ page }) => {
  const counts = await boot(page, { corpusDelayMs: 600 })
  await page.evaluate(() => {
    const hook = (window as any).__dtRefresh
    hook.requestRefreshSoon(100)
    // Lands while the first cycle (600 ms corpus response) is still in flight.
    setTimeout(() => hook.requestRefreshSoon(300), 300)
  })
  await page.waitForTimeout(4500)
  expect(counts.corpus).toBeGreaterThanOrEqual(2)
})

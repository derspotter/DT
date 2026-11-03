import { expect, test, type Page, type Route } from '@playwright/test'
import { mockLogSocket } from './helpers/mockApi'

// One tab→resources table with per-resource in-flight dedupe: every tab loads
// exactly what it renders, a resource in flight is joined rather than
// re-requested, and the corpus poll asks for one page.

const CORPUS_PAGE_SIZE = 120

function fulfil(route: Route, body: unknown) {
  return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })
}

function corpusRow(id: number) {
  return { id, work_id: id, title: `Work ${id}`, authors: 'A. Author', year: 2024, status: 'downloaded' }
}

type Options = {
  hash?: string
  admin?: boolean
  running?: boolean
  corpusDelayMs?: number
  seedsDelayMs?: number
  corpusTotal?: number
}

async function boot(page: Page, { hash = '#/workspace', admin = false, running = false, corpusDelayMs = 0, seedsDelayMs = 0, corpusTotal = 1 }: Options = {}) {
  const log: { path: string; search: URLSearchParams }[] = []
  const delays = { corpus: corpusDelayMs, seeds: seedsDelayMs }
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    const path = url.pathname
    log.push({ path, search: url.searchParams })
    if (path === '/api/auth/me') {
      return fulfil(route, {
        user: { id: 1, username: admin ? 'admin' : 'reader', last_corpus_id: 1, is_admin: admin },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      })
    }
    if (path === '/api/corpus') {
      if (delays.corpus) await new Promise((resolve) => setTimeout(resolve, delays.corpus))
      const limit = Number(url.searchParams.get('limit') || CORPUS_PAGE_SIZE)
      const offset = Number(url.searchParams.get('offset') || 0)
      const items = []
      for (let i = offset; i < Math.min(offset + limit, corpusTotal); i += 1) items.push(corpusRow(i + 1))
      return fulfil(route, { items, total: corpusTotal, source: 'api', stage_totals: { downloaded: corpusTotal } })
    }
    if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
    if (path === '/api/openalex/quota') return fulfil(route, { available: false })
    if (path === '/api/seed/sources') {
      if (delays.seeds) await new Promise((resolve) => setTimeout(resolve, delays.seeds))
      const sources = running
        ? [{ id: 1, title: 'Seed', name: 'Seed', run: { id: 5, status: 'running' }, status: 'running' }]
        : []
      return fulfil(route, { sources, total: sources.length, source: 'api' })
    }
    if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
    if (path === '/api/ingest/stats') {
      return fulfil(route, { stats: url.searchParams.get('global') === '1' ? { downloaded: 42 } : { downloaded: 1 } })
    }
    if (path === '/api/pipeline/daemon/status') {
      return fulfil(route, { daemon: { workers: {} }, backlog: url.searchParams.get('global') === '1' ? { downloading: 3 } : {} })
    }
    if (path === '/api/downloads') return fulfil(route, { items: [], source: 'api' })
    if (path === '/api/downloads/worker/status') return fulfil(route, {})
    if (path === '/api/kantropos/corpora') return fulfil(route, { corpora: [{ id: 'k1', name: 'Kantropos One' }] })
    if (path === '/api/kantropos/corpora/k1/browse') {
      return fulfil(route, { target: { id: 'k1', name: 'Kantropos One' }, summary: {}, assigned_corpora: [], current_items: [], pending_items: [] })
    }
    if (path === '/api/corpora/1/kantropos-assignment') {
      return fulfil(route, { assignment: null, corpora: [{ id: 'k1', name: 'Kantropos One' }], can_edit: true })
    }
    return fulfil(route, {})
  })
  await page.goto(`/${hash}`)
  if (hash === '#/workspace') await expect(page.getByTestId('seed-panel')).toBeVisible()
  // Let the boot loads (and any post-bootstrap tab refresh) settle.
  await page.waitForTimeout(1500)
  const count = (path: string, global: boolean | null = null) =>
    log.filter((entry) => entry.path === path && (global === null || (entry.search.get('global') === '1') === global)).length
  return { log, count, delays, reset: () => { log.length = 0 } }
}

test('switching to Korpus Management during a slow corpus poll still loads the browse', async ({ page }) => {
  const { count, delays, reset } = await boot(page, { admin: true })
  delays.corpus = 1500
  reset()
  // Start a cycle whose corpus poll stays in flight for 1.5 s...
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect.poll(() => count('/api/corpus')).toBe(1)
  // ...and open Korpus Management while it is still running.
  await page.evaluate(() => { window.location.hash = '#/upstream' })
  await expect.poll(() => count('/api/kantropos/corpora/k1/browse'), { timeout: 2000 }).toBeGreaterThanOrEqual(1)
  expect(count('/api/kantropos/corpora')).toBe(1)
})

test('an admin workspace cycle makes no downloads or download-worker-status requests', async ({ page }) => {
  const { count, reset } = await boot(page, { admin: true, running: true })
  reset()
  await page.waitForTimeout(5000)
  expect(count('/api/downloads')).toBe(0)
  expect(count('/api/downloads/worker/status')).toBe(0)
  expect(count('/api/pipeline/daemon/status')).toBeGreaterThanOrEqual(2)
  // The 2 s cycle does not re-read the OpenAlex budget.
  expect(count('/api/openalex/quota')).toBe(0)
})

test('the admin dashboard loads the global overview', async ({ page }) => {
  const { count } = await boot(page, { admin: true, hash: '#/dashboard' })
  const overview = page.locator('.card', { has: page.getByRole('heading', { name: 'Global overview' }) })
  await expect(overview).toBeVisible()
  await expect(overview.locator('.pipeline-stage', { hasText: 'Done' }).locator('strong')).toHaveText('42')
  expect(count('/api/ingest/stats', true)).toBe(1)
  expect(count('/api/pipeline/daemon/status', true)).toBe(1)
  expect(count('/api/downloads/worker/status')).toBe(0)
})

test('a resource in flight is not requested twice', async ({ page }) => {
  const { count, delays, reset } = await boot(page)
  delays.seeds = 1500
  reset()
  await page.evaluate(() => {
    window.dispatchEvent(new Event('focus'))
    setTimeout(() => document.dispatchEvent(new Event('visibilitychange')), 100)
    // Past the debounce, while the first seed request is still in flight.
    setTimeout(() => window.dispatchEvent(new Event('focus')), 600)
  })
  await page.waitForTimeout(2000)
  expect(count('/api/seed/sources')).toBe(1)
})

test('the corpus poll asks for one page and skips unchanged pages', async ({ page }) => {
  const { log, count, reset } = await boot(page, { running: true, corpusTotal: 300 })
  const panel = page.getByTestId('corpus-panel')
  await panel.getByRole('button', { name: 'Show more' }).click()
  await expect(panel).toContainText(`Loaded ${CORPUS_PAGE_SIZE * 2}/300`)
  reset()
  // Wait for one poll, then watch the table across the next one.
  await expect.poll(() => count('/api/corpus'), { timeout: 4000 }).toBeGreaterThanOrEqual(1)
  await page.waitForTimeout(300)
  await page.evaluate(() => {
    // The table itself: the panel's "Show more" button legitimately toggles
    // `disabled` while any corpus load is in flight.
    const target = document.querySelector('[data-testid="corpus-panel"] .corpus-table')!
    const w = window as any
    w.__corpusMutations = 0
    w.__corpusObserver = new MutationObserver((records) => { w.__corpusMutations += records.length })
    w.__corpusObserver.observe(target, { subtree: true, childList: true, characterData: true, attributes: true })
  })
  const before = count('/api/corpus')
  await expect.poll(() => count('/api/corpus'), { timeout: 4000 }).toBeGreaterThan(before)
  await page.waitForTimeout(300)
  expect(await page.evaluate(() => (window as any).__corpusMutations)).toBe(0)
  for (const entry of log.filter((e) => e.path === '/api/corpus')) {
    expect(entry.search.get('limit')).toBe(String(CORPUS_PAGE_SIZE))
    expect(entry.search.get('offset')).toBe('0')
  }
  // The appended rows survive the poll.
  await expect(panel).toContainText(`Loaded ${CORPUS_PAGE_SIZE * 2}/300`)
})

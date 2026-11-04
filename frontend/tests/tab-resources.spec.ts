import { expect, test, type Locator, type Page } from '@playwright/test'
import { fulfil, mockLogSocket } from './helpers/mockApi'

// One tab→resources table with per-resource in-flight dedupe: every tab loads
// exactly what it renders, a resource in flight is joined rather than
// re-requested, and the corpus poll asks for one page.

const CORPUS_PAGE_SIZE = 120
const STALE_CORPUS_NOTE = 'Refresh failed; showing earlier rows.'

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
  setup?: (state: any) => void
}

async function boot(page: Page, { setup = (_state: any) => {}, hash = '#/workspace', admin = false, running = false, corpusDelayMs = 0, seedsDelayMs = 0, corpusTotal = 1 }: Options = {}) {
  const log: { path: string; search: URLSearchParams }[] = []
  const delays = { corpus: corpusDelayMs, seeds: seedsDelayMs, me: 0 }
  // Mutable server state: the corpus rows (ids, in order) and the corpus /me reports.
  const state = {
    corpusIds: Array.from({ length: corpusTotal }, (_, i) => i + 1),
    lastCorpusId: 1,
    seedCandidates: 0,
    statsDownloaded: 1,
    // Answer the next multi-page reload from offset 0 with a 503.
    failPrefixReloads: 0,
    // Answer the next corpus request at this offset with a 503.
    failOffset: null as number | null,
    // Ids prepended, one each, right after a prefix-reload page at offset 0
    // (limit > one page) is answered: a leading insert between two pages.
    insertAfterPrefixStart: [] as number[],
    // Same trigger: the row at `from` moves to the very end, past the loaded
    // rows, so the reload's next page starts one row later than asked.
    moveAfterPrefixStart: [] as number[],
    // Answer corpus selects with this error instead of switching.
    selectError: null as string | null,
  }
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    const path = url.pathname
    log.push({ path, search: url.searchParams })
    if (path === '/api/auth/me') {
      if (delays.me) await new Promise((resolve) => setTimeout(resolve, delays.me))
      return fulfil(route, {
        user: { id: 1, username: admin ? 'admin' : 'reader', last_corpus_id: state.lastCorpusId, is_admin: admin },
        corpora: [
          { id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' },
          { id: 2, name: 'Second corpus', role: 'owner', owner_username: 'admin' },
        ],
      })
    }
    if (path === '/api/corpus') {
      if (delays.corpus) await new Promise((resolve) => setTimeout(resolve, delays.corpus))
      const limit = Number(url.searchParams.get('limit') || CORPUS_PAGE_SIZE)
      const offset = Number(url.searchParams.get('offset') || 0)
      if (state.failOffset !== null && offset === state.failOffset) {
        state.failOffset = null
        return route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ error: 'busy' }) })
      }
      if (state.failPrefixReloads > 0 && offset === 0 && limit > CORPUS_PAGE_SIZE) {
        state.failPrefixReloads -= 1
        return route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ error: 'busy' }) })
      }
      const items = state.corpusIds.slice(offset, offset + limit).map(corpusRow)
      const total = state.corpusIds.length
      await fulfil(route, { items, total, source: 'api', stage_totals: { downloaded: total } })
      if (offset === 0 && limit > CORPUS_PAGE_SIZE && state.insertAfterPrefixStart.length > 0) {
        state.corpusIds = [state.insertAfterPrefixStart.shift()!, ...state.corpusIds]
      }
      if (offset === 0 && limit > CORPUS_PAGE_SIZE && state.moveAfterPrefixStart.length > 0) {
        const [moved] = state.corpusIds.splice(state.moveAfterPrefixStart.shift()!, 1)
        state.corpusIds.push(moved)
      }
      return
    }
    const select = path.match(/^\/api\/corpora\/(\d+)\/select$/)
    if (select) {
      if (state.selectError) return fulfil(route, { error: state.selectError }, 403)
      state.lastCorpusId = Number(select[1])
      return fulfil(route, {})
    }
    if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
    if (path === '/api/openalex/quota') return fulfil(route, { available: false })
    if (path === '/api/seed/sources') {
      if (delays.seeds) await new Promise((resolve) => setTimeout(resolve, delays.seeds))
      const sources = running
        ? [{ id: 1, title: 'Seed', name: 'Seed', run: { id: 5, status: 'running' }, status: 'running' }]
        : state.seedCandidates
          ? [{ id: 1, source_type: 'search', source_key: '1', title: 'Seed', name: 'Seed', candidate_count: state.seedCandidates }]
          : []
      return fulfil(route, { sources, total: sources.length, source: 'api' })
    }
    if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
    if (path === '/api/ingest/stats') {
      return fulfil(route, { stats: url.searchParams.get('global') === '1' ? { downloaded: 42 } : { downloaded: state.statsDownloaded } })
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
    if (path === '/api/corpora/1/kantropos-assignment' || path === '/api/corpora/2/kantropos-assignment') {
      return fulfil(route, { assignment: null, corpora: [{ id: 'k1', name: 'Kantropos One' }], can_edit: true })
    }
    return fulfil(route, {})
  })
  setup(state)
  await page.goto(`/${hash}`)
  if (hash === '#/workspace') await expect(page.getByTestId('seed-panel')).toBeVisible()
  // Let the boot loads (and any post-bootstrap tab refresh) settle.
  await page.waitForTimeout(1500)
  const count = (path: string, global: boolean | null = null) =>
    log.filter((entry) => entry.path === path && (global === null || (entry.search.get('global') === '1') === global)).length
  return { log, count, delays, state, reset: () => { log.length = 0 } }
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

test('opening Korpus Management while a corpus switch is reloading still loads the browse', async ({ page }) => {
  const { count, delays, state, reset } = await boot(page, { admin: true })
  // The backend moved this user to corpus 2; the switch's reload is slow.
  state.lastCorpusId = 2
  delays.me = 300
  delays.corpus = 2000
  reset()
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  // The reconcile answered and its workspace reload is in flight...
  await expect.poll(() => count('/api/corpus')).toBeGreaterThanOrEqual(1)
  // ...when Korpus Management opens.
  await page.evaluate(() => { window.location.hash = '#/upstream' })
  await expect.poll(() => count('/api/kantropos/corpora/k1/browse'), { timeout: 4000 }).toBe(1)
  await expect.poll(() => count('/api/corpora/2/kantropos-assignment')).toBe(1)
  await page.waitForTimeout(2500)
  expect(count('/api/kantropos/corpora/k1/browse')).toBe(1)
})

test('the header counts load on a tab that does not render them itself', async ({ page }) => {
  await boot(page, {
    admin: true,
    hash: '#/downloads',
    corpusTotal: 5,
    setup: (state) => {
      state.seedCandidates = 9
      state.statsDownloaded = 4
    },
  })
  const header = page.locator('.header-stats')
  await expect(header).toContainText('9 items found')
  await expect(header).toContainText('5 items promoted')
  await expect(header).toContainText('4 items downloaded')
})

test('the graph tab requests none of the header reads', async ({ page }) => {
  const { count } = await boot(page, { hash: '#/graph' })
  // It does not render the counters it does not load.
  const header = page.locator('.header-stats')
  await expect(header).toContainText('backend')
  for (const label of ['items found', 'items promoted', 'items downloaded']) {
    await expect(header).not.toContainText(label)
  }
  expect(count('/api/ingest/stats')).toBe(0)
  expect(count('/api/corpus')).toBe(0)
  expect(count('/api/seed/sources')).toBe(0)
})

test('a row inserted at the top does not duplicate rows when more are loaded after the poll', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(String(error)))
  const { state } = await boot(page, { corpusTotal: 300 })
  const panel = page.getByTestId('corpus-panel')
  await panel.getByRole('button', { name: 'Show more' }).click()
  await expect(panel).toContainText(`Loaded ${CORPUS_PAGE_SIZE * 2}/300`)
  // A new work lands at the top of the sort order; the next poll sees it.
  state.corpusIds = [1000, ...state.corpusIds]
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect(panel).toContainText(/Loaded \d+\/301/)
  await panel.getByRole('button', { name: 'Show more' }).click()
  await expect(panel).toContainText('Loaded 301/301')
  const titles = await panel.locator('.corpus-table').evaluate((table) =>
    [...table.textContent!.matchAll(/Work (\d+)/g)].map((match) => match[1]),
  )
  expect(titles.length).toBe(301)
  expect(new Set(titles).size).toBe(301)
  expect(errors).toEqual([])
})

test('a failed prefix reload is retried by the next poll', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(String(error)))
  const { state, log } = await boot(page, { corpusTotal: 300 })
  const panel = page.getByTestId('corpus-panel')
  await panel.getByRole('button', { name: 'Show more' }).click()
  await expect(panel).toContainText(`Loaded ${CORPUS_PAGE_SIZE * 2}/300`)
  state.corpusIds = [1000, ...state.corpusIds]
  state.failPrefixReloads = 1
  const prefixReloads = () => log.filter((e) => e.path === '/api/corpus' && Number(e.search.get('limit')) > CORPUS_PAGE_SIZE).length
  await expect(panel).toContainText('Live corpus from API.')
  // First poll: the first page changed, its prefix reload answers 503. The
  // user is told, and the first page's counts still apply.
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect.poll(prefixReloads).toBe(1)
  await expect(panel).toContainText(STALE_CORPUS_NOTE)
  await expect(panel).not.toContainText('Live corpus from API.')
  await expect(panel).toContainText(`Loaded ${CORPUS_PAGE_SIZE * 2}/301`)
  await expect(page.locator('.header-stats')).toContainText('301 items promoted')
  // Second, identical poll: retries the reload, which now succeeds.
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect.poll(prefixReloads).toBe(2)
  await expect(panel).toContainText('Live corpus from API.')
  await expect(panel).not.toContainText(STALE_CORPUS_NOTE)
  await expect(panel).toContainText(/Loaded \d+\/301/)
  await panel.getByRole('button', { name: 'Show more' }).click()
  await expect(panel).toContainText('Loaded 301/301')
  const ids = await panel.locator('.corpus-table').evaluate((table) =>
    [...table.textContent!.matchAll(/Work (\d+)/g)].map((match) => match[1]),
  )
  expect(ids).toContain('1000')
  expect(ids.length).toBe(301)
  expect(new Set(ids).size).toBe(301)
  expect(errors).toEqual([])
})

// Clicks "Show more" until `rows` of `total` are loaded. A click while any
// corpus load (such as the idle poll) is in flight is ignored by the app,
// so each step retries its click.
async function showMoreUntil(panel: Locator, rows: number, total: number) {
  for (let loaded = CORPUS_PAGE_SIZE * 2; loaded <= rows; loaded += CORPUS_PAGE_SIZE) {
    await expect(async () => {
      if (!(await panel.textContent())?.includes(`Loaded ${loaded}/${total}`)) {
        await panel.getByRole('button', { name: 'Show more' }).click()
      }
      await expect(panel).toContainText(`Loaded ${loaded}/${total}`, { timeout: 1500 })
    }).toPass({ timeout: 15_000 })
  }
}

test('a changed first page reloads every loaded row in bounded pages', async ({ page }) => {
  test.setTimeout(90_000)
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(String(error)))
  const { state, log, reset } = await boot(page, { corpusTotal: 1400 })
  const panel = page.getByTestId('corpus-panel')
  await showMoreUntil(panel, 1320, 1400)
  const tableIds = () => panel.locator('.corpus-table').evaluate((table) =>
    [...table.textContent!.matchAll(/Work (\d+)/g)].map((match) => match[1]),
  )
  const corpusRequests = () => log
    .filter((e) => e.path === '/api/corpus')
    .map((e) => `${e.search.get('limit')}@${e.search.get('offset')}`)

  // A leading insert; the prefix reload's second page fails first.
  state.corpusIds = [5000, ...state.corpusIds]
  state.failOffset = 999
  reset()
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect.poll(corpusRequests).toEqual([`${CORPUS_PAGE_SIZE}@0`, '1000@0', '321@999'])
  await expect(panel).toContainText(STALE_CORPUS_NOTE)
  // Rows and signature untouched: the old rows stay, the next poll retries.
  await expect(panel).toContainText('Loaded 1320/1401')
  expect((await tableIds())[0]).toBe('1')

  reset()
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect.poll(corpusRequests).toEqual([`${CORPUS_PAGE_SIZE}@0`, '1000@0', '321@999'])
  await expect(panel).toContainText('Live corpus from API.')
  await expect(panel).toContainText('Loaded 1320/1401')
  const ids = await tableIds()
  expect(ids.length).toBe(1320)
  expect(new Set(ids).size).toBe(1320)
  expect(ids[0]).toBe('5000')
  expect(ids[1319]).toBe('1319')
  expect(errors).toEqual([])
})

test('a prefix reload whose pages disagree is retried once, then left alone', async ({ page }) => {
  test.setTimeout(90_000)
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(String(error)))
  const { state, log, reset } = await boot(page, { corpusTotal: 1400 })
  const panel = page.getByTestId('corpus-panel')
  await showMoreUntil(panel, 1320, 1400)
  const tableIds = () => panel.locator('.corpus-table').evaluate((table) =>
    [...table.textContent!.matchAll(/Work (\d+)/g)].map((match) => match[1]),
  )
  const corpusRequests = () => log
    .filter((e) => e.path === '/api/corpus')
    .map((e) => `${e.search.get('limit')}@${e.search.get('offset')}`)
  const prefixRequests = [`${CORPUS_PAGE_SIZE}@0`, '1000@0', '321@999', '1000@0', '321@999']

  // A row lands in front between the pages of the reload and again during
  // its retry: both attempts are inconsistent, so nothing changes.
  state.corpusIds = [5000, ...state.corpusIds]
  state.insertAfterPrefixStart = [6000, 7000]
  reset()
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect.poll(corpusRequests).toEqual(prefixRequests)
  await expect(panel).toContainText(STALE_CORPUS_NOTE)
  await expect(panel).not.toContainText('Live corpus from API.')
  // The rows stay; the counts are the first page's.
  await expect(panel).toContainText('Loaded 1320/1401')
  expect((await tableIds())[0]).toBe('1')
  expect(errors).toEqual([])

  // The signature stayed: the next poll reloads again. One insert between
  // its pages; the retry is consistent and replaces the rows.
  state.insertAfterPrefixStart = [8000]
  reset()
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect.poll(corpusRequests).toEqual(prefixRequests)
  await expect(panel).toContainText('Loaded 1320/1404')
  const ids = await tableIds()
  expect(ids.length).toBe(1320)
  expect(new Set(ids).size).toBe(1320)
  expect(ids.slice(0, 4)).toEqual(['8000', '7000', '6000', '5000'])
  expect(ids[1319]).toBe('1316')
  await expect(panel).toContainText('Live corpus from API.')
  expect(errors).toEqual([])
})

test('a row that leaves the loaded range mid-reload (no id repeats, total unchanged) is retried', async ({ page }) => {
  test.setTimeout(90_000)
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(String(error)))
  const { state, log, reset } = await boot(page, { corpusTotal: 1400 })
  const panel = page.getByTestId('corpus-panel')
  await showMoreUntil(panel, 1320, 1400)
  const corpusRequests = () => log
    .filter((e) => e.path === '/api/corpus')
    .map((e) => `${e.search.get('limit')}@${e.search.get('offset')}`)

  // Sorted by a mutable column: a row of the first reload page moves past the
  // loaded rows while the reload runs, so the next page's rows shift by one.
  state.corpusIds = [5000, ...state.corpusIds]
  state.moveAfterPrefixStart = [10]
  reset()
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect.poll(corpusRequests).toEqual([`${CORPUS_PAGE_SIZE}@0`, '1000@0', '321@999', '1000@0', '321@999'])
  await expect(panel).toContainText('Live corpus from API.')
  const ids = await panel.locator('.corpus-table').evaluate((table) =>
    [...table.textContent!.matchAll(/Work (\d+)/g)].map((match) => match[1]),
  )
  // The server's order: 5000, 1..1400 without the moved row 10. No gap.
  expect(ids.length).toBe(1320)
  expect(ids.slice(0, 11)).toEqual(['5000', '1', '2', '3', '4', '5', '6', '7', '8', '9', '11'])
  expect(ids[1319]).toBe('1320')
  expect(errors).toEqual([])
})

test('the corpus note is true before the first answer and after a failed load', async ({ page }) => {
  const { state } = await boot(page, { setup: (s) => { s.failOffset = 0 } })
  const panel = page.getByTestId('corpus-panel')
  await expect(panel).toContainText('Corpus not loaded.')
  await expect(panel).not.toContainText('Sample corpus data.')
  await expect(panel).not.toContainText('Live corpus from API.')
  expect(state.failOffset).toBeNull()
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect(panel).toContainText('Live corpus from API.')
  await expect(panel).not.toContainText('Corpus not loaded.')
})

test('a failed first-page poll does not claim a live corpus', async ({ page }) => {
  const { state } = await boot(page, { corpusTotal: 3 })
  const panel = page.getByTestId('corpus-panel')
  await expect(panel).toContainText('Live corpus from API.')
  state.failOffset = 0
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect(panel).toContainText(STALE_CORPUS_NOTE)
  await expect(panel).not.toContainText('Live corpus from API.')
  // The next poll finds the same first page: the rows are current after all.
  await page.evaluate(() => (window as any).__dtRefresh.requestRefreshSoon(10))
  await expect(panel).toContainText('Live corpus from API.')
  await expect(panel).not.toContainText(STALE_CORPUS_NOTE)
})

test('a failed corpus switch shows on every tab and the picker returns to the active corpus', async ({ page }) => {
  const { state } = await boot(page, { admin: true })
  state.selectError = 'No access to this corpus'
  const picker = page.locator('.header-corpus-picker select')
  const error = page.getByTestId('corpus-switch-error')
  await expect(picker).toHaveValue('1')

  await picker.selectOption('2')
  await expect(error).toHaveText('No access to this corpus')
  await expect(picker).toHaveValue('1')

  for (const tab of ['dashboard', 'downloads', 'graph', 'logs', 'admin']) {
    await page.evaluate((hash) => { window.location.hash = hash }, `#/${tab}`)
    await expect(page).toHaveURL(new RegExp(`#/${tab}$`))
    await expect(error).toHaveText('No access to this corpus')
  }

  // Once, on the tab that has its own corpus status line.
  await page.evaluate(() => { window.location.hash = '#/upstream' })
  await expect(page.locator('.upstream-management-form__status')).toHaveText('No access to this corpus')
  await expect(error).toHaveCount(0)

  // A switch that works clears it.
  state.selectError = null
  await page.evaluate(() => { window.location.hash = '#/workspace' })
  await picker.selectOption('2')
  await expect(picker).toHaveValue('2')
  await expect(error).toHaveCount(0)
})

test('a corpus switch keeps the search form choices', async ({ page }) => {
  await boot(page)
  const card = page.locator('.seed-intake-card--search')
  await card.getByRole('combobox', { name: 'Field' }).selectOption('title')
  await card.getByRole('textbox', { name: 'Author' }).fill('Elinor Ostrom')
  await card.getByRole('spinbutton', { name: 'From' }).fill('1990')
  await card.getByRole('spinbutton', { name: 'To' }).fill('2000')
  await card.getByRole('combobox', { name: 'Sort' }).selectOption('newest')
  await card.getByRole('textbox', { name: 'Query' }).fill('commons')

  await page.locator('.header-corpus-picker select').selectOption('2')
  await expect(page.locator('.header-corpus-picker select')).toHaveValue('2')
  // The query is per corpus, as on master.
  await expect(card.getByRole('textbox', { name: 'Query' })).toHaveValue('')
  await expect(card.getByRole('combobox', { name: 'Field' })).toHaveValue('title')
  await expect(card.getByRole('textbox', { name: 'Author' })).toHaveValue('Elinor Ostrom')
  await expect(card.getByRole('spinbutton', { name: 'From' })).toHaveValue('1990')
  await expect(card.getByRole('spinbutton', { name: 'To' })).toHaveValue('2000')
  await expect(card.getByRole('combobox', { name: 'Sort' })).toHaveValue('newest')
})

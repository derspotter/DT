import { expect, test, type Page, type Route } from '@playwright/test'
import { fulfil, mePayload, mockLogSocket } from './helpers/mockApi'

// A user's corpus reload (filter, sort, "Show more") is never dropped because
// the 2 s poll is in flight: filter and sort supersede it, "Show more" queues
// behind it. Appends drop rows already loaded, and a corpus switch remounts
// the Corpus table (selection and stage filter start over).

const corpusA = { id: 1, name: 'Corpus Alpha', role: 'owner', owner_username: 'reader' }
const corpusB = { id: 2, name: 'Corpus Beta', role: 'owner', owner_username: 'reader' }

type Params = { q: string; sort: string; offset: number; limit: number }
type Rows = (params: Params) => { ids: number[]; total: number }

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((r) => { resolve = r })
  return { promise, resolve }
}

type Hold = { match: (params: Params) => boolean; release: Promise<void>; seen: boolean }

type State = {
  corpusId: number
  requests: Params[]
  holds: Hold[]
  rows: Rows
  status: string
}

// The default corpus: 300 works with ids 1-300, paged by offset; `q` answers
// 5 rows with ids 1001-1005 titled after the query; any sort answers the ids
// in reverse.
const defaultRows: Rows = ({ q, sort, offset, limit }) => {
  if (q) return { ids: Array.from({ length: 5 }, (_, i) => 1001 + i), total: 5 }
  const all = Array.from({ length: 300 }, (_, i) => i + 1)
  if (sort) all.reverse()
  return { ids: all.slice(offset, offset + limit), total: 300 }
}

async function boot(page: Page, state: State) {
  await page.addInitScript(() => window.localStorage.setItem('rag_feeder_token', 'playwright-token'))
  await mockLogSocket(page)
  await page.route('**/api/**', async (route: Route) => {
    const url = new URL(route.request().url())
    const path = url.pathname
    if (path === '/api/auth/me') {
      return fulfil(route, mePayload({ username: 'reader', admin: false, corpusId: state.corpusId, corpora: [corpusA, corpusB] }))
    }
    if (path === '/api/corpora/2/select') {
      state.corpusId = 2
      return fulfil(route, { success: true })
    }
    if (path === '/api/corpus') {
      const params: Params = {
        q: url.searchParams.get('q') || '',
        sort: url.searchParams.get('sort') || '',
        offset: Number(url.searchParams.get('offset') || 0),
        limit: Number(url.searchParams.get('limit') || 0),
      }
      state.requests.push(params)
      // The answer is computed before any hold, as the server would.
      const { ids, total } = state.rows(params)
      const items = ids.map((id) => ({
        id,
        work_id: id,
        title: params.q ? `${params.q} paper ${id}` : `Paper ${id}`,
        authors: 'A. Author',
        year: 2024,
        status: state.status,
      }))
      const hold = state.holds.find((entry) => !entry.seen && entry.match(params))
      if (hold) {
        hold.seen = true
        await hold.release
      }
      return fulfil(route, { items, total, source: 'api', stage_totals: {} })
    }
    if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
    if (path === '/api/openalex/quota') return fulfil(route, { available: false })
    if (path === '/api/seed/sources') return fulfil(route, { sources: [], total: 0, source: 'api' })
    if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
    // A live worker lease keeps the poll at the 2 s cadence.
    if (path === '/api/ingest/stats') return fulfil(route, { stats: { enriching_live: 1 } })
    return fulfil(route, {})
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('corpus-panel')).toBeVisible()
}

function newState(overrides: Partial<State> = {}): State {
  return { corpusId: 1, requests: [], holds: [], rows: defaultRows, status: 'downloaded', ...overrides }
}

// Holds the next /api/corpus request matching `match` until released.
function hold(state: State, match: (params: Params) => boolean) {
  const gate = deferred()
  const entry: Hold = { match, release: gate.promise, seen: false }
  state.holds.push(entry)
  return { entry, release: gate.resolve }
}

const isPoll = (params: Params) => !params.q && !params.sort && params.offset === 0
const rows = (page: Page) => page.getByTestId('corpus-panel').locator('.corpus-select-row')
const filterInput = (page: Page) => page.getByLabel('Filter corpus items by title, author or publication')
const showMore = (page: Page) => page.getByTestId('corpus-panel').getByRole('button', { name: 'Show more' })

async function holdNextPoll(page: Page, state: State) {
  await expect(rows(page)).toHaveCount(120)
  const poll = hold(state, isPoll)
  await expect.poll(() => poll.entry.seen, { timeout: 8000 }).toBe(true)
  return poll
}

test('a filter typed during a slow poll reloads with the filter', async ({ page }) => {
  const state = newState()
  await boot(page, state)
  const poll = await holdNextPoll(page, state)
  await filterInput(page).fill('labour')
  await expect.poll(() => state.requests.some((params) => params.q === 'labour')).toBe(true)
  await expect(rows(page)).toHaveCount(5)
  poll.release()
  await page.waitForTimeout(500)
  await expect(rows(page)).toHaveCount(5)
  await expect(rows(page).first()).toContainText('labour paper 1001')
})

test('a sort click during a poll is applied', async ({ page }) => {
  const state = newState()
  await boot(page, state)
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '1')
  const poll = await holdNextPoll(page, state)
  await page.getByTestId('corpus-panel').locator('.table-sort', { hasText: 'Title' }).click()
  await expect.poll(() => state.requests.some((params) => params.sort === 'title:asc')).toBe(true)
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '300')
  poll.release()
  await page.waitForTimeout(500)
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '300')
})

test('Show more clicked during a poll runs after it', async ({ page }) => {
  const state = newState()
  await boot(page, state)
  const poll = await holdNextPoll(page, state)
  await expect(showMore(page)).toBeEnabled()
  await showMore(page).click()
  await page.waitForTimeout(300)
  expect(state.requests.filter((params) => params.offset === 120)).toHaveLength(0)
  poll.release()
  await expect.poll(() => state.requests.filter((params) => params.offset === 120).length).toBe(1)
  await expect(rows(page)).toHaveCount(240)
  await page.waitForTimeout(300)
  expect(state.requests.filter((params) => params.offset === 120)).toHaveLength(1)
})

test('a filter typed while Show more is in flight discards the append and its queue', async ({ page }) => {
  // q=x has 300 rows too, so a stale queued append would have more to load.
  const state = newState({
    rows: ({ q, offset, limit }) => ({
      ids: Array.from({ length: 300 }, (_, i) => (q ? 2001 : 1) + i).slice(offset, offset + limit),
      total: 300,
    }),
  })
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  const append = hold(state, (params) => params.offset === 120 && !params.q)
  await showMore(page).click()
  await expect.poll(() => append.entry.seen).toBe(true)
  await filterInput(page).fill('x')
  await expect.poll(() => state.requests.some((params) => params.q === 'x')).toBe(true)
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '2001')
  append.release()
  await page.waitForTimeout(800)
  await expect(rows(page)).toHaveCount(120)
  const ids = await rows(page).evaluateAll((els) => els.map((el) => Number(el.getAttribute('data-work-id'))))
  expect(ids.every((id) => id >= 2001)).toBe(true)
  expect(state.requests.some((params) => params.q === 'x' && params.offset === 120)).toBe(false)
})

test('Show more drops rows already loaded', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(error.message))
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text())
  })
  // A row inserted in front shifted the offsets: page 2 starts with id 120,
  // the last row of page 1.
  const state = newState({
    rows: ({ offset, limit }) => {
      const start = offset === 0 ? 1 : offset
      return { ids: Array.from({ length: limit }, (_, i) => start + i), total: 301 }
    },
  })
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  await showMore(page).click()
  await expect.poll(() => state.requests.some((params) => params.offset === 120)).toBe(true)
  await page.waitForTimeout(500)
  expect(errors.filter((text) => text.includes('each_key_duplicate'))).toEqual([])
  await expect(rows(page)).toHaveCount(239)
  const ids = await rows(page).evaluateAll((els) => els.map((el) => el.getAttribute('data-work-id')))
  expect(new Set(ids).size).toBe(ids.length)
  expect(ids.filter((id) => id === '120')).toHaveLength(1)
})

test('a corpus switch clears the Corpus selection and stage filter', async ({ page }) => {
  const state = newState()
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  const stage = page.getByTestId('corpus-panel').locator('.corpus-filter--stage select')
  await stage.selectOption('downloaded')
  const checkbox = (index: number) => rows(page).nth(index).locator('.ingest-select-cell input[type="checkbox"]')
  await checkbox(0).check()
  await checkbox(1).check()
  await expect(page.getByTestId('corpus-panel').locator('.corpus-bulk-bar')).toContainText('Selected: 2 /')

  await page.locator('.header-corpus-picker select').selectOption('2')
  await expect.poll(() => state.corpusId).toBe(2)
  await expect(page.locator('.header-corpus-picker select')).toHaveValue('2')
  await expect(rows(page).first()).toBeVisible()
  await expect(stage).toHaveValue('all')
  await expect(page.getByTestId('corpus-panel').locator('.corpus-bulk-bar')).toContainText('Selected: 0 /')
  await expect(rows(page).locator('.ingest-select-cell input:checked')).toHaveCount(0)
})

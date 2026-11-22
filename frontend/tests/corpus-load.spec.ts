import { expect, test, type Page, type Route } from '@playwright/test'
import { fulfil, mePayload, mockLogSocket } from './helpers/mockApi'

// A user's corpus reload (filter, sort, "Show more") is never dropped because
// the 2 s poll is in flight: filter and sort supersede it, "Show more" queues
// behind it. Appends drop rows already loaded, and a corpus switch remounts
// the Corpus table (selection and stage filter start over).

const corpusA = { id: 1, name: 'Corpus Alpha', role: 'owner', owner_username: 'reader' }
const corpusB = { id: 2, name: 'Corpus Beta', role: 'owner', owner_username: 'reader' }

// corpusId: the mock's current corpus when the request arrived.
type Params = { q: string; sort: string; offset: number; limit: number; stage: string; corpusId?: number }
// stageTotal defaults to total (no stage), stageTotals to {}.
type Answer = { ids: number[]; total: number; stageTotal?: number; stageTotals?: Record<string, number> }
type Rows = (params: Params) => Answer

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
  // Answers 500 to a matching /api/corpus request; `failed` counts them.
  fail: (params: Params) => boolean
  failed: number
  // Appended to every title: changing it changes the first page's signature.
  tag: string
  // Unlink requests (DELETE /api/corpus/works/:id, POST .../remove).
  unlinks: number[][]
  unlinkHold: Promise<void> | null
  onUnlink: (ids: number[]) => void
  // The bulk unlink's removed_work_ids, when not every requested id.
  removedIds: number[] | null
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
        stage: url.searchParams.get('stage') || '',
        corpusId: state.corpusId,
      }
      state.requests.push(params)
      // The answer is computed before any hold, as the server would.
      const { ids, total, stageTotal, stageTotals } = state.rows(params)
      const failing = state.fail(params)
      const items = ids.map((id) => ({
        id,
        work_id: id,
        title: `${params.q ? `${params.q} paper ${id}` : `Paper ${id}`}${state.tag}`,
        authors: 'A. Author',
        year: 2024,
        status: state.status,
        file_path: `/pdfs/${id}.pdf`,
        doi: `10.1000/${id}`,
        openalex_id: `W${id}`,
      }))
      const hold = state.holds.find((entry) => !entry.seen && entry.match(params))
      if (hold) {
        hold.seen = true
        await hold.release
      }
      if (failing) {
        state.failed += 1
        return fulfil(route, { error: 'boom' }, 500)
      }
      return fulfil(route, { items, total, stage_total: stageTotal ?? total, source: 'api', stage_totals: stageTotals ?? {} })
    }
    const unlink = path.match(/^\/api\/corpus\/works\/(\d+)$/)
    if ((unlink && route.request().method() === 'DELETE') || path === '/api/corpus/works/remove') {
      const ids = unlink ? [Number(unlink[1])] : (route.request().postDataJSON()?.workIds || []).map(Number)
      state.unlinks.push(ids)
      if (state.unlinkHold) await state.unlinkHold
      state.onUnlink(ids)
      return fulfil(route, unlink ? { success: true } : { success: true, removed_work_ids: state.removedIds ?? ids })
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
  return {
    corpusId: 1, requests: [], holds: [], rows: defaultRows, status: 'downloaded',
    fail: () => false, failed: 0, tag: '', unlinks: [], unlinkHold: null, onUnlink: () => {}, removedIds: null,
    ...overrides,
  }
}

// Holds the next /api/corpus request matching `match` until released.
function hold(state: State, match: (params: Params) => boolean) {
  const gate = deferred()
  const entry: Hold = { match, release: gate.promise, seen: false }
  state.holds.push(entry)
  return { entry, release: gate.resolve }
}

const isPoll = (params: Params) => !params.q && !params.sort && !params.stage && params.offset === 0
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
  // The switch resets the stage: the new corpus loads every stage.
  await expect.poll(() => state.requests.find((params) => params.corpusId === 2)?.stage).toBe('')
  await expect(page.locator('.header-corpus-picker select')).toHaveValue('2')
  await expect(rows(page).first()).toBeVisible()
  await expect(stage).toHaveValue('all')
  await expect(page.getByTestId('corpus-panel').locator('.corpus-bulk-bar')).toContainText('Selected: 0 /')
  await expect(rows(page).locator('.ingest-select-cell input:checked')).toHaveCount(0)
})

// --- Server-side stage filter (D13) ------------------------------------------

const panel = (page: Page) => page.getByTestId('corpus-panel')
const stageSelect = (page: Page) => panel(page).locator('.corpus-filter--stage select')
const header = (page: Page) => panel(page).locator('.workspace-panel-title p')
const lazyStatus = (page: Page) => panel(page).locator('.corpus-lazy-status')
const rowIds = (page: Page) => rows(page).evaluateAll((els) => els.map((el) => Number(el.getAttribute('data-work-id'))))
const range = (from: number, count: number) => Array.from({ length: count }, (_, i) => from + i)

// 300 works across all stages (ids 1-300); the 'downloaded' stage answers
// fx.downloaded (ids 501-650 unless given), 'failed_download' ids 901-902.
// q is ignored: every row matches. fx.stageTotal overrides the downloaded
// stage_total per request.
function stageFixture(downloaded = range(501, 150)) {
  const fx = { downloaded, stageTotal: null as null | ((params: Params) => number) }
  const rows: Rows = (params) => {
    const { stage, offset, limit } = params
    const stageTotals = { raw: 298 - fx.downloaded.length, downloaded: fx.downloaded.length, failed_download: 2 }
    if (stage === 'downloaded') {
      const stageTotal = fx.stageTotal ? fx.stageTotal(params) : fx.downloaded.length
      return { ids: fx.downloaded.slice(offset, offset + limit), total: 300, stageTotal, stageTotals }
    }
    if (stage === 'failed_download') return { ids: [901, 902].slice(offset, offset + limit), total: 300, stageTotal: 2, stageTotals }
    return { ids: range(1, 300).slice(offset, offset + limit), total: 300, stageTotals }
  }
  return { fx, rows }
}

async function loadDownloadedRows(page: Page, count: number) {
  await expect(rows(page)).toHaveCount(120)
  await stageSelect(page).selectOption('downloaded')
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '501')
  for (let n = 240; n <= count; n += 120) {
    await showMore(page).click()
    await expect(rows(page)).toHaveCount(n)
  }
}

test('the stage filter loads that stage from the server and pages within it', async ({ page }) => {
  const state = newState({ rows: stageFixture().rows })
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  await stageSelect(page).selectOption('downloaded')
  await expect.poll(() => state.requests.at(-1)).toMatchObject({ stage: 'downloaded', offset: 0 })
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '501')
  expect(await rowIds(page)).toEqual(range(501, 120))
  await showMore(page).click()
  await expect.poll(() => state.requests.some((params) => params.stage === 'downloaded' && params.offset === 120)).toBe(true)
  await expect(rows(page)).toHaveCount(150)
  expect(await rowIds(page)).toEqual(range(501, 150))
  await expect(showMore(page)).toHaveCount(0)
  await expect(lazyStatus(page)).toContainText('Showing 150 entries.')
  await expect(stageSelect(page).locator('option[value="all"]')).toHaveText('All stages (300)')
  expect(state.requests.filter((params) => params.offset > 0).every((params) => params.stage === 'downloaded')).toBe(true)
})

test('a poll keeps the stage and q and drops a row that left it from a loaded second page', async ({ page }) => {
  const { fx, rows: stageRows } = stageFixture()
  const state = newState({ rows: stageRows })
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  await filterInput(page).fill('paper')
  await expect.poll(() => state.requests.some((params) => params.q === 'paper')).toBe(true)
  await stageSelect(page).selectOption('downloaded')
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '501')
  await showMore(page).click()
  await expect(rows(page)).toHaveCount(150)
  const before = state.requests.length
  fx.downloaded = fx.downloaded.filter((id) => id !== 630)
  await expect(rows(page)).toHaveCount(149, { timeout: 8000 })
  expect(await rowIds(page)).not.toContain(630)
  const after = state.requests.slice(before)
  // The poll's first page, then the prefix reload of the 150 loaded rows.
  expect(after.some((params) => params.offset === 0 && params.limit === 150)).toBe(true)
  expect(after.every((params) => params.stage === 'downloaded' && params.q === 'paper')).toBe(true)
  await expect(header(page)).toContainText('Loaded 149/149.')
  // The quiet poll's commit updates the footer's count too.
  await expect(lazyStatus(page)).toContainText('Showing 149 entries.')
  await expect(showMore(page)).toHaveCount(0)
})

test('two prefix responses whose stage_total disagree are rejected', async ({ page }) => {
  const { fx, rows: stageRows } = stageFixture(range(501, 1100))
  const state = newState({ rows: stageRows })
  await boot(page, state)
  await loadDownloadedRows(page, 1080)
  await expect(header(page)).toContainText('Loaded 1080/1100.')
  const loaded = await rowIds(page)
  // The prefix reload reads offsets 0 (limit 1000) and 999 (limit 81).
  fx.stageTotal = (params) => (params.offset === 999 ? 1099 : 1100)
  state.tag = ' (v2)'
  await expect.poll(() => state.requests.filter((params) => params.offset === 999).length, { timeout: 10000 }).toBeGreaterThanOrEqual(2)
  await expect(header(page)).toContainText('Refresh failed; showing earlier rows.')
  await expect(header(page)).toContainText('Loaded 1080/1100.')
  expect(await rowIds(page)).toEqual(loaded)
  await expect(rows(page).first()).not.toContainText('(v2)')
})

test('counts move only with their rows', async ({ page }) => {
  const { fx, rows: stageRows } = stageFixture(range(501, 1100))
  const state = newState({ rows: stageRows })
  await boot(page, state)
  await loadDownloadedRows(page, 1080)
  await expect(header(page)).toContainText('Loaded 1080/1100.')
  const loaded = await rowIds(page)

  // (a) The poll's first page changed and counts 1101; the prefix reload's
  // second request fails once.
  fx.stageTotal = () => 1101
  state.tag = ' (v2)'
  let served = false
  state.fail = (params) => {
    if (served || params.offset !== 999) return false
    served = true
    return true
  }
  await expect.poll(() => state.failed, { timeout: 8000 }).toBe(1)
  // Keeps the next downloaded poll from retrying while the state is checked.
  const retry = hold(state, (params) => params.stage === 'downloaded' && params.offset === 0)
  await expect(header(page)).toContainText('Refresh failed; showing earlier rows.')
  await expect(header(page)).toContainText('Loaded 1080/1100.')
  expect(await rowIds(page)).toEqual(loaded)
  await expect(rows(page).first()).not.toContainText('(v2)')

  // (b) A stage change that fails, then a quiet poll for that stage.
  state.fail = (params) => params.stage === 'failed_download' && state.failed < 2
  await stageSelect(page).selectOption('failed_download')
  await expect.poll(() => state.failed).toBe(2)
  await expect(header(page)).toContainText('Loaded 1080/1100.')
  expect(await rowIds(page)).toEqual(loaded)
  const before = state.requests.length
  const poll = hold(state, (params) => params.stage === 'failed_download')
  await expect.poll(() => poll.entry.seen, { timeout: 8000 }).toBe(true)
  await expect(header(page)).toContainText('Loaded 1080/1100.')
  await expect(rows(page)).toHaveCount(1080)
  poll.release()
  await expect(rows(page)).toHaveCount(2)
  await expect(header(page)).toContainText('Loaded 2/2.')
  // Exactly the poll's first page: no prefix reload of the 1,080 old rows.
  const staged = state.requests.slice(before).filter((params) => params.stage === 'failed_download')
  expect(staged.map((params) => `${params.limit}@${params.offset}`)).toEqual(['120@0'])
  retry.release()
})

test('a removal answered after a corpus switch leaves the new corpus alone', async ({ page }) => {
  const members: Record<number, number[]> = { 1: [501, 502, 503, 504], 2: [501, 601, 602] }
  const state = newState()
  state.rows = ({ offset, limit }) => {
    const ids = members[state.corpusId]
    return { ids: ids.slice(offset, offset + limit), total: ids.length }
  }
  // The held unlink is corpus 1's.
  state.onUnlink = (ids) => { members[1] = members[1].filter((id) => !ids.includes(id)) }
  const unlink = deferred()
  state.unlinkHold = unlink.promise
  await boot(page, state)
  await expect(rows(page)).toHaveCount(4)
  await rows(page).first().locator('.corpus-remove').click()
  await expect.poll(() => state.unlinks.length).toBe(1)

  await page.locator('.header-corpus-picker select').selectOption('2')
  await expect.poll(() => state.corpusId).toBe(2)
  await expect(rows(page)).toHaveCount(3)
  await expect(header(page)).toContainText('Loaded 3/3.')
  // Park corpus 2's next poll, so any further /api/corpus request is the
  // removal's reconciliation.
  const poll = hold(state, (params) => params.corpusId === 2 && params.offset === 0)
  await expect.poll(() => poll.entry.seen, { timeout: 8000 }).toBe(true)
  const before = state.requests.length
  unlink.resolve()
  await page.waitForTimeout(600)
  expect(state.requests.length).toBe(before)
  await expect(rows(page)).toHaveCount(3)
  expect(await rowIds(page)).toContain(501)
  await expect(header(page)).toContainText('Loaded 3/3.')
  poll.release()
})

test('a quiet poll begun before a removal answers after its superseding reload and is discarded', async ({ page }) => {
  let members = range(1, 10)
  const state = newState({
    rows: ({ offset, limit }) => ({ ids: members.slice(offset, offset + limit), total: members.length }),
    onUnlink: (ids) => { members = members.filter((id) => !ids.includes(id)) },
  })
  await boot(page, state)
  await expect(rows(page)).toHaveCount(10)
  // A changed first page, so the held poll's answer would be applied.
  state.tag = ' (v2)'
  const poll = hold(state, isPoll)
  await expect.poll(() => poll.entry.seen, { timeout: 8000 }).toBe(true)
  const before = state.requests.length
  await rows(page).first().locator('.corpus-remove').click()
  await expect.poll(() => state.unlinks.length).toBe(1)
  await expect(rows(page)).toHaveCount(9)
  await expect.poll(() => state.requests.length).toBeGreaterThan(before)
  await expect(rows(page).first()).toContainText('(v2)')
  poll.release()
  await page.waitForTimeout(500)
  await expect(rows(page)).toHaveCount(9)
  expect(await rowIds(page)).not.toContain(1)
})

// Counts change only with a committed server answer: a removal takes its rows
// off the screen, and a failed follow-up leaves the last committed counts.
test('a removal whose follow-up refresh fails keeps the last committed counts', async ({ page }) => {
  const state = newState({ rows: stageFixture().rows })
  page.on('dialog', (dialog) => dialog.accept())
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  await stageSelect(page).selectOption('downloaded')
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '501')
  await expect(header(page)).toContainText('Loaded 120/150.')
  state.fail = () => true
  await rows(page).first().locator('.corpus-remove').click()
  await expect.poll(() => state.unlinks.length).toBe(1)
  await expect(rows(page)).toHaveCount(119)
  await expect.poll(() => state.failed).toBeGreaterThan(0)
  await expect(header(page)).toContainText('Refresh failed; showing earlier rows.')
  await expect(header(page)).toContainText('Loaded 119/150.')
  await expect(showMore(page)).toBeVisible()

  const failedBefore = state.failed
  await rows(page).nth(0).locator('.ingest-select-cell input').check()
  await rows(page).nth(1).locator('.ingest-select-cell input').check()
  await panel(page).getByRole('button', { name: 'Remove selected' }).click()
  await expect.poll(() => state.unlinks.length).toBe(2)
  expect(state.unlinks[1]).toEqual([502, 503])
  await expect(rows(page)).toHaveCount(117)
  await expect.poll(() => state.failed).toBeGreaterThan(failedBefore)
  await expect(header(page)).toContainText('Refresh failed; showing earlier rows.')
  await expect(header(page)).toContainText('Loaded 117/150.')
  await expect(showMore(page)).toBeVisible()
})

test('row actions wait for the rows of the selected stage', async ({ page }) => {
  const state = newState({ rows: stageFixture().rows })
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  await expect(header(page)).toContainText('Loaded 120/300.')
  // Expand row 1 (downloaded, with a file, DOI and OpenAlex id); select row 3.
  await rows(page).first().click()
  await rows(page).nth(2).locator('.ingest-select-cell input').check()
  const clearSelection = panel(page).getByRole('button', { name: 'Clear selection' })
  await expect(clearSelection).toBeEnabled()
  const pdf = panel(page).locator('.inline-detail-chip--download')
  const links = panel(page).locator('.corpus-inline-detail-row a.inline-detail-link')
  await expect(pdf).toBeEnabled()
  await expect(links).toHaveCount(2)
  await expect(links.first()).toHaveAttribute('href', /10\.1000\/1$/)

  const gate = hold(state, (params) => params.stage === 'downloaded')
  await stageSelect(page).selectOption('downloaded')
  await expect.poll(() => gate.entry.seen).toBe(true)
  const selectAllBox = panel(page).locator('.table-row.header input[type="checkbox"]')
  await expect(rows(page).first().locator('.corpus-remove')).toBeDisabled()
  await expect(rows(page).first().locator('.ingest-select-cell input')).toBeDisabled()
  await expect(selectAllBox).toBeDisabled()
  await expect(panel(page).locator('.corpus-bulk-bar').getByRole('button', { name: 'Select all' })).toBeDisabled()
  await expect(panel(page).getByRole('button', { name: 'Remove selected' })).toBeDisabled()
  await expect(showMore(page)).toBeDisabled()
  await expect(clearSelection).toBeDisabled()
  await expect(pdf).toBeDisabled()
  await expect(links.first()).not.toHaveAttribute('href', /.+/)
  await expect(links.nth(1)).not.toHaveAttribute('href', /.+/)
  await rows(page).nth(1).click()
  await expect(panel(page).locator('.corpus-inline-detail-row')).toHaveCount(1)
  await expect(rows(page).first()).toHaveClass(/active-row/)
  await expect(rows(page).nth(1)).not.toHaveClass(/active-row/)
  await expect(rows(page).nth(1)).not.toHaveClass(/row-checked/)
  await expect(header(page)).toContainText('Loaded 120/300.')

  gate.release()
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '501')
  await expect(rows(page).first().locator('.corpus-remove')).toBeEnabled()
  await expect(rows(page).first().locator('.ingest-select-cell input')).toBeEnabled()
  await expect(selectAllBox).toBeEnabled()
  await expect(showMore(page)).toBeEnabled()
  await expect(header(page)).toContainText('Loaded 120/150.')
  await expect(lazyStatus(page)).toContainText('Showing 120 of 150 entries.')
})

test('a failed stage load keeps the old rows inert until the user reverts', async ({ page }) => {
  const state = newState({ rows: stageFixture().rows, fail: (params) => params.stage === 'failed_download' })
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  await stageSelect(page).selectOption('failed_download')
  await expect.poll(() => state.failed).toBeGreaterThan(0)
  await expect(header(page)).toContainText('Refresh failed; showing earlier rows.')
  await expect(rows(page)).toHaveCount(120)
  await expect(rows(page).first()).toHaveAttribute('data-work-id', '1')
  await expect(rows(page).first().locator('.corpus-remove')).toBeDisabled()
  await expect(rows(page).first().locator('.ingest-select-cell input')).toBeDisabled()
  await expect(showMore(page)).toBeDisabled()

  // Back to the committed stage: the actions return before any new rows do.
  const gate = hold(state, (params) => !params.stage && params.offset === 0)
  await stageSelect(page).selectOption('all')
  await expect(rows(page).first().locator('.corpus-remove')).toBeEnabled()
  await expect(rows(page).first().locator('.ingest-select-cell input')).toBeEnabled()
  await expect(showMore(page)).toBeEnabled()
  expect(await rowIds(page)).toEqual(range(1, 120))
  gate.release()
})

for (const [bulk, followUpFails] of [[false, false], [true, false], [false, true], [true, true]]) {
  test(`a ${bulk ? 'bulk ' : ''}removal answered after a stage switch is counted once${followUpFails ? ', also when the follow-up fails' : ''}`, async ({ page }) => {
    const state = newState()
    // The unlink commits on arrival; its response is held.
    const removed = () => new Set(state.unlinks.flat())
    state.rows = ({ stage, offset, limit }) => {
      const gone = removed()
      const all = range(1, 300).filter((id) => !gone.has(id))
      const stageTotals = { raw: all.length - 150, downloaded: 150 }
      if (stage === 'downloaded') return { ids: range(501, 150).slice(offset, offset + limit), total: all.length, stageTotal: 150, stageTotals }
      return { ids: all.slice(offset, offset + limit), total: all.length, stageTotals }
    }
    const unlink = deferred()
    state.unlinkHold = unlink.promise
    page.on('dialog', (dialog) => dialog.accept())
    await boot(page, state)
    await expect(rows(page)).toHaveCount(120)
    const allLabel = stageSelect(page).locator('option[value="all"]')
    await expect(allLabel).toHaveText('All stages (300)')
    if (bulk) {
      await rows(page).nth(0).locator('.ingest-select-cell input').check()
      await rows(page).nth(1).locator('.ingest-select-cell input').check()
      await panel(page).getByRole('button', { name: 'Remove selected' }).click()
    } else {
      await rows(page).first().locator('.corpus-remove').click()
    }
    await expect.poll(() => state.unlinks.length).toBe(1)
    const expected = 300 - (bulk ? 2 : 1)
    // The stage load runs after the unlink committed: it already counts it.
    await stageSelect(page).selectOption('downloaded')
    await expect(rows(page).first()).toHaveAttribute('data-work-id', '501')
    await expect(allLabel).toHaveText(`All stages (${expected})`)
    const before = state.requests.length
    if (followUpFails) state.fail = () => true
    unlink.resolve()
    // The follow-up answers the same first page, and its counts still apply;
    // when it fails, the stage load's counts already hold the removal.
    await expect.poll(() => state.requests.length).toBeGreaterThan(before)
    if (followUpFails) await expect.poll(() => state.failed).toBeGreaterThan(0)
    await page.waitForTimeout(300)
    // Success: the server's counts. Failure: the last committed ones (the
    // stage load's, which already hold the removal) under the failed notice.
    await expect(allLabel).toHaveText(`All stages (${expected})`)
    await expect(page.getByTestId('workspace-sticky-bar')).toContainText(`3 Corpus (${expected})`)
    if (followUpFails) await expect(header(page)).toContainText('Refresh failed; showing earlier rows.')
    else await expect(header(page)).toContainText('Live corpus from API.')
  })
}

test('a stage request that fails before the first rows commit says so', async ({ page }) => {
  const state = newState({ rows: stageFixture().rows, fail: (params) => params.stage === 'failed_download' })
  // The boot's first page stays unanswered.
  const first = hold(state, (params) => !params.stage)
  await boot(page, state)
  await expect.poll(() => first.entry.seen).toBe(true)
  await expect(header(page)).toContainText('Corpus not loaded.')
  await stageSelect(page).selectOption('failed_download')
  await expect.poll(() => state.failed).toBeGreaterThan(0)
  await expect(header(page)).toContainText('Corpus not loaded. The last load failed.')
  first.release()
})

test('a bulk removal that removed nothing does not reload the loaded rows', async ({ page }) => {
  const state = newState({ removedIds: [] })
  page.on('dialog', (dialog) => dialog.accept())
  await boot(page, state)
  await expect(rows(page)).toHaveCount(120)
  await showMore(page).click()
  await expect(rows(page)).toHaveCount(240)
  await rows(page).nth(0).locator('.ingest-select-cell input').check()
  const before = state.requests.length
  await panel(page).getByRole('button', { name: 'Remove selected' }).click()
  await expect.poll(() => state.unlinks.length).toBe(1)
  await expect.poll(() => state.requests.length).toBeGreaterThan(before)
  await page.waitForTimeout(500)
  // The follow-up's first page is unchanged: no prefix reload of the 240 rows.
  expect(state.requests.slice(before).filter((params) => params.limit > 120)).toEqual([])
  await expect(rows(page)).toHaveCount(240)
  await expect(header(page)).toContainText('Live corpus from API.')
})

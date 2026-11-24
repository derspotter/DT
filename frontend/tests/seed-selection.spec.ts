import { expect, test, type Page } from '@playwright/test'
import { afterBodyRead, fetchStats, fulfil, logIn, mePayload, mockLogSocket, trackFetches } from './helpers/mockApi'

// Seed selection behaviour with the backend mocked: one search seed holding
// 70,000 items of which only the first page (3 rows) is loaded.

const corpusA = { id: 1, name: 'Corpus Alpha', role: 'owner', owner_username: 'reader' }
const corpusB = { id: 2, name: 'Corpus Beta', role: 'owner', owner_username: 'reader' }

const seedSource = {
  id: 'search:70',
  source_type: 'search',
  source_key: '70',
  seed_kind: 'search',
  label: 'Big search seed',
  candidate_count: 70000,
  state_counts: { in_corpus: 10, downloaded: 5 },
  removable: true,
  run: null,
}

const candidates = [0, 1, 2].map((n) => ({
  candidate_key: `c${n}`,
  title: `Economics paper ${n}`,
  authors: [{ name: 'A. Author' }],
  year: 2020,
  source: 'openalex',
  state: 'pending',
  in_corpus: false,
  refs_count: n,
  cited_by_count: n,
}))

type State = {
  corpusId: number
  promoteBody: any
  holdPromote: Promise<void> | null
  source?: typeof seedSource
  keywordConfig?: Record<string, unknown>
  candidates?: typeof candidates
  // Per-request gates for the 'readable' candidate's file (request n waits
  // for fileGates[n]); request 0 answers with this status (default 200).
  fileGates?: Promise<void>[]
  lateFileStatus?: number
  fileRequests?: number
  // Every request's path and Authorization header, in order.
  routeLog?: { path: string; auth: string }[]
}

const READABLE_FILE = '/api/seed/sources/search/70/candidates/readable/file'

async function boot(page: Page, state: State) {
  const source = state.source || seedSource
  await page.addInitScript(() => window.localStorage.setItem('rag_feeder_token', 'playwright-token'))
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    const auth = route.request().headers().authorization || ''
    state.routeLog?.push({ path, auth })
    if (path === '/api/auth/login') {
      return fulfil(route, { token: 'second-token', user: { id: 2, username: 'second', last_corpus_id: 1, is_admin: false } })
    }
    if (path === '/api/auth/me') {
      const second = auth === 'Bearer second-token'
      return fulfil(route, mePayload({ id: second ? 2 : 1, username: second ? 'second' : 'reader', admin: false, corpusId: state.corpusId, corpora: [corpusA, corpusB] }))
    }
    if (path === READABLE_FILE) {
      const n = state.fileRequests = (state.fileRequests || 0) + 1
      const gate = state.fileGates?.[n - 1]
      if (gate) await gate
      if (n === 1 && state.lateFileStatus === 500) return fulfil(route, { error: 'late-candidate-error-7f3' }, 500)
      return route.fulfill({
        status: 200,
        contentType: 'application/pdf',
        headers: { 'Content-Disposition': `attachment; filename="${n === 1 ? 'readable' : 'probe'}.pdf"` },
        body: Buffer.from('%PDF-1.4\n'),
      })
    }
    if (path === '/api/corpora/2/select') {
      state.corpusId = 2
      return fulfil(route, { success: true })
    }
    if (path === '/api/seed/sources') return fulfil(route, { sources: [source], total: 1, source: 'api' })
    if (path === '/api/seed/sources/search/70/candidates') {
      const rows = state.candidates || candidates
      return fulfil(route, { candidates: rows, total: source.candidate_count, offset: 0, limit: 200, source_summary: source })
    }
    if (path === '/api/seed/sources/search/70/promote') {
      state.promoteBody = route.request().postDataJSON()
      if (state.holdPromote) await state.holdPromote
      return fulfil(route, { success: true, message: 'Promoted.' })
    }
    if (path === '/api/corpus') return fulfil(route, { items: [], total: 0, source: 'api', stage_totals: {} })
    if (path === '/api/recursion-config') return fulfil(route, { keyword: state.keywordConfig || {} })
    if (path === '/api/openalex/quota') return fulfil(route, { available: false })
    if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
    if (path === '/api/ingest/stats') return fulfil(route, { stats: {} })
    return fulfil(route, {})
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
}

function collectPageErrors(page: Page) {
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(error.message))
  return errors
}

async function expandSeed(page: Page) {
  await page.getByText('Big search seed', { exact: true }).click()
  await expect(page.getByTestId('seed-candidate-row')).toHaveCount(3)
}

const rowCheckbox = (page: Page, index: number) =>
  page.getByTestId('seed-candidate-row').nth(index).locator('.ingest-select-cell input[type="checkbox"]')

test('toggling a row keeps the table mounted', async ({ page }) => {
  const errors = collectPageErrors(page)
  await boot(page, { corpusId: 1, promoteBody: null, holdPromote: null })
  await expandSeed(page)

  const row = page.getByTestId('seed-candidate-row').first()
  const handle = await row.elementHandle()
  await rowCheckbox(page, 0).click()
  await expect(rowCheckbox(page, 0)).toBeChecked()
  expect(await handle!.evaluate((el) => el.isConnected)).toBe(true)
  await expect(page.locator('.seed-source__body .table-toolbar')).toContainText('Selected: 1 / 3 selectable')

  await page.locator('.seed-source__body').getByRole('button', { name: 'Select all', exact: true }).click()
  // The selectable estimate: 70,000 minus 10 in the corpus and 5 downloaded.
  await expect(page.locator('.seed-source__body .table-toolbar')).toContainText('All 69,985 items selected')
  for (const index of [0, 1, 2]) await expect(rowCheckbox(page, index)).toBeChecked()

  // Deselecting one out of "all selected" drops into explicit mode.
  await rowCheckbox(page, 1).click()
  await expect(rowCheckbox(page, 1)).not.toBeChecked()
  await expect(rowCheckbox(page, 0)).toBeChecked()
  await expect(rowCheckbox(page, 2)).toBeChecked()
  await expect(page.locator('.seed-source__body .table-toolbar')).toContainText('Selected: 2 / 3 selectable')
  expect(await handle!.evaluate((el) => el.isConnected)).toBe(true)
  expect(errors).toEqual([])
})

test('expanding a paged seed keeps the server count', async ({ page }) => {
  let release!: () => void
  const state: State = { corpusId: 1, promoteBody: null, holdPromote: new Promise<void>((r) => { release = r }) }
  await boot(page, state)
  const summary = page.locator('.seed-source__summary')
  await expect(summary).toContainText('70000 items')
  await expandSeed(page)
  await expect(summary).toContainText('70000 items')
  await expect(summary).toContainText('To review: 69990')
  await expect(summary).toContainText('In corpus: 10')

  const promoteAll = page.getByRole('button', { name: 'Promote all promotable items', exact: true })
  await expect(promoteAll).toBeEnabled()
  await promoteAll.click()
  await expect(page.getByText('Promoting 69,985 candidate(s) into the corpus...')).toBeVisible()
  await expect.poll(() => state.promoteBody?.downloadBatchSize).toBe(69985)
  release()
})

test('a corpus switch clears selections', async ({ page }) => {
  const errors = collectPageErrors(page)
  const state: State = { corpusId: 1, promoteBody: null, holdPromote: null }
  await boot(page, state)
  await expandSeed(page)
  await rowCheckbox(page, 0).click()
  await expect(page.locator('.seed-source__body .table-toolbar')).toContainText('Selected: 1 / 3 selectable')

  await page.locator('.header-corpus-picker select').selectOption('2')
  await expect.poll(() => state.corpusId).toBe(2)
  await expect(page.locator('.header-corpus-picker select')).toHaveValue('2')
  await expect(page.locator('.seed-source.expanded')).toHaveCount(0)

  await expandSeed(page)
  await expect(page.locator('.seed-source__body .table-toolbar')).toContainText('Selected: 0 / 3 selectable')
  for (const index of [0, 1, 2]) await expect(rowCheckbox(page, index)).not.toBeChecked()
  expect(errors).toEqual([])
})

test('downstream expansion makes every item of a paged seed promotable', async ({ page }) => {
  let release!: () => void
  const state: State = {
    corpusId: 1,
    promoteBody: null,
    holdPromote: new Promise<void>((r) => { release = r }),
    // Without the override nothing would be left: 69,990 in corpus + 10 downloaded.
    source: { ...seedSource, state_counts: { in_corpus: 69990, downloaded: 10 } },
    keywordConfig: { includeDownstream: true, relatedDepthDownstream: 1 },
  }
  await boot(page, state)
  await expandSeed(page)
  const promoteAll = page.getByRole('button', { name: 'Promote all promotable items', exact: true })
  await expect(promoteAll).toBeEnabled()

  // Turning the override off leaves nothing promotable in the unloaded rest.
  const downstream = page.locator('.seed-expansion-pill', { hasText: 'Downstream' }).locator('input[type="checkbox"]')
  await downstream.uncheck()
  await expect(promoteAll).toBeDisabled()
  await downstream.check()
  await expect(promoteAll).toBeEnabled()

  await promoteAll.click()
  await expect(page.getByText('Promoting 70,000 candidate(s) into the corpus...')).toBeVisible()
  await expect.poll(() => state.promoteBody?.downloadBatchSize).toBe(70000)
  release()
})

test('select-all on a partly promoted paged seed reads checked and clears on the next click', async ({ page }) => {
  const errors = collectPageErrors(page)
  // 10 items, 3 already in the corpus, the first page of 5 (all pending) loaded:
  // 7 are selectable.
  const state: State = {
    corpusId: 1,
    promoteBody: null,
    holdPromote: null,
    source: { ...seedSource, candidate_count: 10, state_counts: { in_corpus: 3, downloaded: 0 } },
    candidates: [0, 1, 2, 3, 4].map((n) => ({ ...candidates[0], candidate_key: `p${n}`, title: `Partly paper ${n}` })),
  }
  await boot(page, state)
  await page.getByText('Big search seed', { exact: true }).click()
  await expect(page.getByTestId('seed-candidate-row')).toHaveCount(5)
  const selectAll = page.getByRole('checkbox', { name: 'Select all items in this seed' })
  const toolbar = page.locator('.seed-source__body .table-toolbar')

  await selectAll.click()
  await expect(selectAll).toBeChecked()
  await expect(page.locator('.seed-source__select')).toHaveAttribute('title', 'Clear all items in this seed')
  // All-selected counts the selectable rows (10 - 3 in the corpus), not the seed total.
  await expect(toolbar).toContainText('All 7 items selected')
  for (const index of [0, 1, 2, 3, 4]) await expect(rowCheckbox(page, index)).toBeChecked()

  await selectAll.click()
  await expect(selectAll).not.toBeChecked()
  await expect(toolbar).toContainText('Selected: 0 / 5 selectable')
  for (const index of [0, 1, 2, 3, 4]) await expect(rowCheckbox(page, index)).not.toBeChecked()
  expect(errors).toEqual([])
})

// D2: the list answers a match the caller cannot read as not downloaded (no
// work id, no file), so no Download button offers a file the route would 404.
test('a candidate shows a Download button only when the server marks its file readable', async ({ page }) => {
  await boot(page, {
    corpusId: 1,
    promoteBody: null,
    holdPromote: null,
    candidates: [
      { ...candidates[0], candidate_key: 'readable', title: 'Readable copy', state: 'downloaded_elsewhere', downloaded_work_id: 9, file_available: true } as any,
      { ...candidates[1], candidate_key: 'unreadable', title: 'Unreadable copy', state: 'pending', downloaded_work_id: null, file_available: false } as any,
      { ...candidates[2], candidate_key: 'missing', title: 'Missing copy', state: 'downloaded', downloaded_work_id: 8, file_available: false } as any,
    ],
  })
  await expandSeed(page)
  const row = (title: string) => page.getByTestId('seed-candidate-row').filter({ hasText: title })
  await expect(row('Readable copy').locator('.seed-download-link')).toHaveCount(1)
  await expect(row('Unreadable copy').locator('.seed-download-link')).toHaveCount(0)
  await expect(row('Missing copy').locator('.seed-download-link')).toHaveCount(0)
})

// FE-60b (spec D1): a candidate file that settles after a sign-out belongs to
// the session that ended: no file is saved and no status or error shows.
// The file route is built from the row's own source fields.
const readableCandidates = [
  { ...candidates[0], candidate_key: 'readable', source_type: 'search', source_key: '70', title: 'Readable copy', state: 'downloaded_elsewhere', downloaded_work_id: 9, file_available: true } as any,
  { ...candidates[1], candidate_key: 'unreadable', title: 'Unreadable copy', state: 'pending', downloaded_work_id: null, file_available: false } as any,
  { ...candidates[2], candidate_key: 'missing', title: 'Missing copy', state: 'downloaded', downloaded_work_id: 8, file_available: false } as any,
]

async function lateCandidateFile(page: Page, lateFileStatus: number) {
  let release!: () => void
  const state: State = {
    corpusId: 1,
    promoteBody: null,
    holdPromote: null,
    candidates: readableCandidates,
    fileGates: [new Promise<void>((r) => { release = r })],
    lateFileStatus,
    routeLog: [],
  }
  const names: string[] = []
  page.on('download', (download) => names.push(download.suggestedFilename()))
  await trackFetches(page)
  await boot(page, state)
  await expandSeed(page)
  const readableLink = page.getByTestId('seed-candidate-row').filter({ hasText: 'Readable copy' }).locator('.seed-download-link')
  await readableLink.click()
  await expect.poll(() => state.fileRequests).toBe(1)

  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'second')
  await expect(page.getByTestId('auth-user')).toHaveText('second')
  await expect.poll(() => state.routeLog!.some((e) => e.path === '/api/seed/sources' && e.auth === 'Bearer second-token')).toBe(true)

  const reads = (await fetchStats(page, READABLE_FILE)).reads
  release()
  await afterBodyRead(page, READABLE_FILE, reads)
  return { names, readableLink, state }
}

test('a candidate file that arrives after a sign-out is not saved', async ({ page }) => {
  const { names, readableLink } = await lateCandidateFile(page, 200)
  await expect(page.getByText('Downloaded readable.pdf.')).toHaveCount(0)

  await expandSeed(page)
  const download = page.waitForEvent('download')
  await readableLink.click()
  await download
  expect(names).toEqual(['probe.pdf'])
})

test('a candidate file failing after a sign-out shows nothing', async ({ page }) => {
  await lateCandidateFile(page, 500)
  await expect(page.getByText('late-candidate-error-7f3')).toHaveCount(0)
})

import { expect, test, type Page, type Route } from '@playwright/test'

// Seed selection behaviour with the backend mocked: one search seed holding
// 70,000 items of which only the first page (3 rows) is loaded.

function fulfil(route: Route, body: unknown, status = 200) {
  return route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })
}

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

type State = { corpusId: number, promoteBody: any, holdPromote: Promise<void> | null }

async function boot(page: Page, state: State) {
  await page.addInitScript(() => window.localStorage.setItem('rag_feeder_token', 'playwright-token'))
  await page.routeWebSocket('**/api/ws', () => {})
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') {
      return fulfil(route, {
        user: { id: 1, username: 'reader', last_corpus_id: state.corpusId, is_admin: false },
        corpora: [corpusA, corpusB],
      })
    }
    if (path === '/api/corpora/2/select') {
      state.corpusId = 2
      return fulfil(route, { success: true })
    }
    if (path === '/api/seed/sources') return fulfil(route, { sources: [seedSource], total: 1, source: 'api' })
    if (path === '/api/seed/sources/search/70/candidates') {
      return fulfil(route, { candidates, total: 70000, offset: 0, limit: 200, source_summary: seedSource })
    }
    if (path === '/api/seed/sources/search/70/promote') {
      state.promoteBody = route.request().postDataJSON()
      if (state.holdPromote) await state.holdPromote
      return fulfil(route, { success: true, message: 'Promoted.' })
    }
    if (path === '/api/corpus') return fulfil(route, { items: [], total: 0, source: 'api', stage_totals: {} })
    if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
    if (path === '/api/openalex/quota') return fulfil(route, { available: false })
    if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
    if (path === '/api/ingest/stats') return fulfil(route, { stats: {} })
    return fulfil(route, {})
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
}

// The mocked log socket never opens; the app closing it on reconnect raises
// this browser-level error, which is not a selection error.
function collectPageErrors(page: Page) {
  const errors: string[] = []
  page.on('pageerror', (error) => {
    if (error.message !== 'WebSocket closed without opened.') errors.push(error.message)
  })
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
  await expect(page.locator('.seed-source__body .table-toolbar')).toContainText('All 70,000 items selected')
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

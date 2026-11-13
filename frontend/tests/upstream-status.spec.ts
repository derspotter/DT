import { expect, test, type Page } from '@playwright/test'
import { fulfil, mePayload, mockLogSocket } from './helpers/mockApi'

// Korpus Management tells a failed load from an empty one: a failed target
// list or browse shows its error (with Retry) and keeps the last data, and the
// empty-state texts only appear for a target that actually loaded.

const NO_PENDING = 'No pending additions match the current filters.'
const NOT_IMPORTED = 'has not been imported'

type Answer = { status: number; error?: string }

function browsePayload() {
  return {
    target: { id: 'k1', name: 'Kantropos One' },
    summary: { pending_items_count: 1, pending_items_loaded: 1, has_imported_current: false },
    assigned_corpora: [],
    current_items: [],
    pending_items: [{ id: 11, title: 'Pending Work Alpha', year: 2024 }],
  }
}

async function boot(page: Page, {
  targets = [{ id: 'k1', name: 'Kantropos One' }],
  targetsAnswer = null as Answer | null,
  browseAnswers = [] as Answer[],
} = {}) {
  const counts = { browse: 0, targets: 0 }
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') return fulfil(route, mePayload({ admin: false, username: 'reader' }))
    if (path === '/api/kantropos/corpora') {
      counts.targets += 1
      if (targetsAnswer) return fulfil(route, { error: targetsAnswer.error }, targetsAnswer.status)
      return fulfil(route, { corpora: targets })
    }
    if (path === '/api/corpora/1/kantropos-assignment') {
      return fulfil(route, { assignment: null, corpora: targetsAnswer ? null : targets, can_edit: true })
    }
    if (path === '/api/kantropos/corpora/k1/browse') {
      counts.browse += 1
      const answer = browseAnswers.shift()
      if (answer) return fulfil(route, { error: answer.error }, answer.status)
      return fulfil(route, browsePayload())
    }
    if (path === '/api/corpus') return fulfil(route, { items: [], total: 0, source: 'api', stage_totals: {} })
    if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
    if (path === '/api/seed/sources') return fulfil(route, { sources: [], total: 0, source: 'api' })
    if (path === '/api/ingest/stats') return fulfil(route, { stats: {} })
    return fulfil(route, {})
  })
  await page.goto('/#/upstream')
  return counts
}

const statusLine = (page: Page) => page.locator('.upstream-browser__status')

test('no Kantropos targets shows "No upstream corpora are configured."', async ({ page }) => {
  await boot(page, { targets: [] })
  await expect(statusLine(page)).toHaveText('No upstream corpora are configured.')
  await expect(page.getByText(NO_PENDING)).toHaveCount(0)
  await expect(page.getByTestId('upstream-retry')).toHaveCount(0)
})

test('a failed target list shows the error, not "none configured"', async ({ page }) => {
  await boot(page, { targetsAnswer: { status: 500, error: 'down' } })
  await expect(statusLine(page)).toHaveText('Could not load the Kantropos corpora: down')
  await expect(statusLine(page)).toHaveClass(/error/)
  await expect(page.getByText('No upstream corpora are configured.')).toHaveCount(0)
  await expect(page.getByTestId('upstream-retry')).toBeVisible()
})

test('a failed first browse shows the error and Retry, and no empty-state texts', async ({ page }) => {
  const counts = await boot(page, { browseAnswers: [{ status: 502, error: 'Bad gateway' }] })
  await expect(statusLine(page)).toHaveText('Could not load the upstream corpus: Bad gateway.')
  await expect(statusLine(page)).toHaveClass(/error/)
  await expect(page.getByText(NO_PENDING)).toHaveCount(0)
  await expect(page.getByText(NOT_IMPORTED)).toHaveCount(0)

  await page.getByTestId('upstream-retry').click()
  await expect(page.getByText('Pending Work Alpha')).toBeVisible()
  await expect(page.getByText(NOT_IMPORTED)).toBeVisible()
  await expect(page.getByText(/Could not load the upstream corpus/)).toHaveCount(0)
  await expect(page.getByTestId('upstream-retry')).toHaveCount(0)
  expect(counts.browse).toBe(2)
})

test('a failed quiet refresh keeps the data and says it is stale', async ({ page }) => {
  const answers: Answer[] = []
  const counts = await boot(page, { browseAnswers: answers })
  await expect(page.getByText('Pending Work Alpha')).toBeVisible()
  const before = counts.browse
  answers.push({ status: 500, error: 'busy' })
  // Opening the tab again: its load is quiet.
  await page.evaluate(() => { window.location.hash = '#/workspace' })
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await page.evaluate(() => { window.location.hash = '#/upstream' })
  await expect.poll(() => counts.browse).toBeGreaterThan(before)
  await expect(statusLine(page)).toHaveText('Could not load the upstream corpus: busy. Showing the last loaded data.')
  await expect(page.getByText('Pending Work Alpha')).toBeVisible()
  await expect(page.getByTestId('upstream-retry')).toBeVisible()
})

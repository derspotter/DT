import { expect, test, type Page, type Route } from '@playwright/test'

// After a revocation the backend falls back to another corpus (or none) for
// every request. The live poll must follow /api/auth/me so the workspace never
// shows corpus A while the backend already serves B.

function fulfil(route: Route, body: unknown, status = 200) {
  return route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })
}

const corpusA = { id: 1, name: 'Corpus Alpha', role: 'owner', owner_username: 'reader' }
const corpusB = { id: 2, name: 'Corpus Beta', role: 'editor', owner_username: 'other' }

type Phase = 'A' | 'B' | 'none'

async function boot(page: Page, state: { phase: Phase }) {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.routeWebSocket('**/api/ws', () => {})
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname
    const { phase } = state
    if (path === '/api/auth/me') {
      const lastCorpusId = phase === 'A' ? 1 : phase === 'B' ? 2 : null
      const corpora = phase === 'A' ? [corpusA, corpusB] : phase === 'B' ? [corpusB] : []
      return fulfil(route, { user: { id: 1, username: 'reader', last_corpus_id: lastCorpusId, is_admin: false }, corpora })
    }
    if (path === '/api/corpora/1/select' || path === '/api/corpora/2/select') {
      throw new Error('reconcile must not POST select')
    }
    const scoped = ['/api/corpus', '/api/seed/sources', '/api/ingest/stats', '/api/ingest/runs']
    if (phase === 'none' && scoped.includes(path)) return fulfil(route, { error: 'Select a corpus first' }, 403)
    const label = phase === 'A' ? 'Alpha' : 'Beta'
    if (path === '/api/corpus') {
      return fulfil(route, { items: [{ id: 7, work_id: 7, title: `Item in ${label}`, authors: 'A. Author', year: 2024 }], total: 1, source: 'api', stage_totals: {} })
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
  await expect(page.getByTestId('seed-panel')).toBeVisible()
}

const picker = (page: Page) => page.locator('.header-corpus-picker select')

test('poll follows the backend corpus fallback from A to B', async ({ page }) => {
  const state = { phase: 'A' as Phase }
  await boot(page, state)
  await expect(picker(page)).toHaveValue('1')
  await expect(page.getByText('Item in Alpha')).toBeVisible()

  state.phase = 'B'
  await expect(picker(page)).toHaveValue('2', { timeout: 8000 })
  await expect(picker(page).locator('option:checked')).toContainText('Corpus Beta')
  await expect(page.getByText('Item in Beta')).toBeVisible()
  await expect(page.getByText('Item in Alpha')).toHaveCount(0)
})

test('poll clears corpus-scoped state when no corpus is left', async ({ page }) => {
  const state = { phase: 'A' as Phase }
  await boot(page, state)
  await expect(page.getByText('Item in Alpha')).toBeVisible()

  state.phase = 'none'
  await expect(picker(page)).toHaveValue('', { timeout: 8000 })
  await expect(picker(page).locator('option:checked')).toHaveText('Select a corpus')
  await expect(page.getByText('Item in Alpha')).toHaveCount(0)
  await expect(page.locator('.corpus-select-row')).toHaveCount(0)
})

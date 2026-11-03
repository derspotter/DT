import { expect, test, type Page, type Route, type WebSocketRoute } from '@playwright/test'
import { fulfil, mePayload, mockLogSocket } from './helpers/mockApi'

// After a revocation the backend falls back to another corpus (or none) for
// every request. The live poll must follow /api/auth/me so the workspace never
// shows corpus A while the backend already serves B.

const corpusA = { id: 1, name: 'Corpus Alpha', role: 'owner', owner_username: 'reader' }
const corpusB = { id: 2, name: 'Corpus Beta', role: 'editor', owner_username: 'other' }

type Phase = 'A' | 'B' | 'none'

type Hook = (path: string, route: Route) => Promise<boolean> | boolean

// `hook` sees every API request first; returning true means it handled it.
async function boot(
  page: Page,
  state: { phase: Phase },
  hook: Hook = () => false,
  onSocket: (ws: WebSocketRoute) => void = () => {},
) {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await mockLogSocket(page, onSocket)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (await hook(path, route)) return
    const { phase } = state
    if (path === '/api/auth/me') {
      const lastCorpusId = phase === 'A' ? 1 : phase === 'B' ? 2 : null
      const corpora = phase === 'A' ? [corpusA, corpusB] : phase === 'B' ? [corpusB] : []
      return fulfil(route, mePayload({ username: 'reader', admin: false, corpusId: lastCorpusId, corpora }))
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

function deferred<T = void>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((r) => { resolve = r })
  return { promise, resolve }
}

const scopedPaths = ['/api/corpus', '/api/seed/sources', '/api/ingest/stats', '/api/ingest/runs']

test('a slow seed list for corpus A does not repopulate the workspace after the corpus cleared', async ({ page }) => {
  const state = { phase: 'A' as Phase }
  let trap = false
  const seedHeld = deferred()
  const releaseSeed = deferred()
  let seedAnswered = false
  await boot(page, state, async (path, route) => {
    if (!trap) return false
    if (path === '/api/seed/sources' && state.phase === 'A') {
      // Captured under corpus A; the backend then drops the membership.
      state.phase = 'none'
      seedHeld.resolve()
      await releaseSeed.promise
      await fulfil(route, {
        sources: [{ id: 'kw:alpha', source_type: 'keyword', source_key: 'alpha', label: 'Seed in Alpha', candidate_count: 1 }],
        total: 1,
        source: 'api',
      })
      seedAnswered = true
      return true
    }
    if (path === '/api/ingest/stats') {
      // Answers after the seed request is held, i.e. with the 403 that starts
      // the reconciliation while the seed list is still pending.
      await seedHeld.promise
      return false
    }
    return false
  })
  await expect(page.getByText('Item in Alpha')).toBeVisible()

  trap = true
  await seedHeld.promise
  await expect(picker(page)).toHaveValue('', { timeout: 8000 })
  await expect(page.getByText('Item in Alpha')).toHaveCount(0)

  releaseSeed.resolve()
  await expect.poll(() => seedAnswered).toBeTruthy()
  await page.waitForTimeout(500)
  await expect(page.getByText('Seed in Alpha')).toHaveCount(0)
  await expect(picker(page)).toHaveValue('')
})

test('a pending reconciliation holds back the next refresh tick', async ({ page }) => {
  const state = { phase: 'A' as Phase }
  let forbidOnce = false
  let holdMe = false
  const meHeld = deferred()
  const releaseMe = deferred()
  let heldAt = 0
  const scopedAfterHold: string[] = []
  await boot(page, state, async (path, route) => {
    if (heldAt && scopedPaths.includes(path) && Date.now() - heldAt > 300) scopedAfterHold.push(path)
    if (path === '/api/ingest/stats' && forbidOnce) {
      forbidOnce = false
      holdMe = true
      await fulfil(route, { error: 'Select a corpus first' }, 403)
      return true
    }
    if (path === '/api/auth/me' && holdMe) {
      // The reconciliation the 403 started: hold it while the backend has
      // already fallen back to B.
      holdMe = false
      state.phase = 'B'
      heldAt = Date.now()
      meHeld.resolve()
      await releaseMe.promise
      return false
    }
    return false
  })
  await expect(page.getByText('Item in Alpha')).toBeVisible()

  forbidOnce = true
  await meHeld.promise
  // Longer than one 2 s refresh tick.
  await page.waitForTimeout(3500)
  expect(scopedAfterHold).toEqual([])
  await expect(picker(page)).toHaveValue('1')

  releaseMe.resolve()
  await expect(picker(page)).toHaveValue('2', { timeout: 8000 })
  await expect(page.getByText('Item in Beta')).toBeVisible()
  await expect(page.getByText('Item in Alpha')).toHaveCount(0)
})

test('an extraction completion for corpus A does not act on corpus B after the switch', async ({ page }) => {
  const state = { phase: 'A' as Phase }
  let socket: WebSocketRoute | null = null
  let holdNextSeed = false
  const seedHeld = deferred()
  const releaseSeed = deferred()
  const candidateRequests: string[] = []
  await boot(
    page,
    state,
    async (path, route) => {
      if (path.startsWith('/api/seed/sources/pdf/paperA/candidates')) candidateRequests.push(path)
      if (path === '/api/seed/sources' && holdNextSeed) {
        holdNextSeed = false
        // The backend drops corpus A while the completion is still awaiting.
        state.phase = 'B'
        seedHeld.resolve()
        await releaseSeed.promise
        await fulfil(route, { sources: [], total: 0, source: 'api' })
        return true
      }
      if (path === '/api/seed/sources' && state.phase === 'B') {
        // Corpus B happens to hold a seed for the same PDF name.
        await fulfil(route, {
          sources: [{ source_type: 'pdf', source_key: 'paperA', label: 'Seed in Beta', candidate_count: 1 }],
          total: 1,
          source: 'api',
        })
        return true
      }
      return false
    },
    (ws) => { socket = ws },
  )
  await expect(page.getByText('Item in Alpha')).toBeVisible()
  await expect.poll(() => socket !== null).toBeTruthy()

  const event = (extra: Record<string, unknown>) =>
    JSON.stringify({ kind: 'extraction', corpus_id: 1, base_name: 'paperA', filename: 'paperA.pdf', ...extra })
  socket!.send(event({ event: 'started' }))
  // The completion's seed reload for corpus A is held next.
  holdNextSeed = true
  socket!.send(event({ event: 'done', status: 'success' }))

  await seedHeld.promise
  await expect(picker(page)).toHaveValue('2', { timeout: 8000 })
  await expect(page.getByText('Seed in Beta')).toBeVisible()

  releaseSeed.resolve()
  await page.waitForTimeout(1500)
  // Without the epoch check the completion would focus (expand and load) B's seed.
  expect(candidateRequests).toEqual([])
  await expect(page.locator('.seed-intake-card--upload .tag.queued', { hasText: 'extracting' })).toHaveCount(0)
})

test('a corpus switch during the seed reload inside the completion focus does not expand B\'s seed', async ({ page }) => {
  const state = { phase: 'A' as Phase }
  let socket: WebSocketRoute | null = null
  // 0: idle; 1: the completion's seed reload is next; 2: the nested reload in focusSeedSource is next.
  let seedStage = 0
  const seedHeld = deferred()
  const releaseSeed = deferred()
  const candidateRequests: string[] = []
  await boot(
    page,
    state,
    async (path, route) => {
      if (path.startsWith('/api/seed/sources/pdf/paperA/candidates')) candidateRequests.push(path)
      if (path === '/api/seed/sources' && seedStage === 1) {
        // Corpus A's list does not show the seed yet, so focusSeedSource reloads.
        seedStage = 2
        await fulfil(route, { sources: [], total: 0, source: 'api' })
        return true
      }
      if (path === '/api/seed/sources' && seedStage === 2) {
        seedStage = 0
        state.phase = 'B'
        seedHeld.resolve()
        await releaseSeed.promise
        await fulfil(route, {
          sources: [{ source_type: 'pdf', source_key: 'paperA', label: 'Seed in Beta', candidate_count: 1 }],
          total: 1,
          source: 'api',
        })
        return true
      }
      if (path === '/api/seed/sources' && state.phase === 'B') {
        await fulfil(route, {
          sources: [{ source_type: 'pdf', source_key: 'paperA', label: 'Seed in Beta', candidate_count: 1 }],
          total: 1,
          source: 'api',
        })
        return true
      }
      return false
    },
    (ws) => { socket = ws },
  )
  await expect(page.getByText('Item in Alpha')).toBeVisible()
  await expect.poll(() => socket !== null).toBeTruthy()

  const event = (extra: Record<string, unknown>) =>
    JSON.stringify({ kind: 'extraction', corpus_id: 1, base_name: 'paperA', filename: 'paperA.pdf', ...extra })
  socket!.send(event({ event: 'started' }))
  // The completion's seed reload is next.
  seedStage = 1
  socket!.send(event({ event: 'done', status: 'success' }))

  await seedHeld.promise
  await expect(picker(page)).toHaveValue('2', { timeout: 8000 })
  await expect(page.getByText('Seed in Beta')).toBeVisible()

  releaseSeed.resolve()
  await page.waitForTimeout(1500)
  expect(candidateRequests).toEqual([])
  await expect(page.locator('.seed-source.expanded')).toHaveCount(0)
})

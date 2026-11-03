import { expect, test, type Page, type Route, type WebSocketRoute } from '@playwright/test'

// Guards the removal of the unreachable ingest/search tabs: their hashes land
// on the workspace, and a finished PDF extraction reports from the pdf seed's
// candidate count instead of fetching the bibliography entries the dead
// ingest tab used to render.

function fulfil(route: Route, body: unknown, status = 200) {
  return route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })
}

async function mockBackend(page: Page, counters: { bibliography: number }) {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname
    if (path.startsWith('/api/bibliography/')) {
      counters.bibliography += 1
      return fulfil(route, { items: [] })
    }
    if (path === '/api/auth/me') {
      return fulfil(route, {
        user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: true },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      })
    }
    if (path === '/api/seed/sources') {
      return fulfil(route, {
        sources: [{ source_type: 'pdf', source_key: 'paper', candidate_count: 3, state_counts: {} }],
        total: 1,
        source: 'api',
      })
    }
    if (path.startsWith('/api/extract-bibliography/') && route.request().method() === 'POST') {
      return fulfil(route, { state: 'started' })
    }
    if (path === '/api/corpus') return fulfil(route, { items: [], total: 0, source: 'api', stage_totals: {} })
    if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
    if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
    if (path === '/api/downloads') return fulfil(route, { items: [], total: 0, source: 'api' })
    if (path === '/api/ingest/stats') return fulfil(route, { stats: {} })
    return fulfil(route, {})
  })
}

test('#/ingest and #/search land on the workspace', async ({ page }) => {
  await page.routeWebSocket('**/api/ws', () => {})
  await mockBackend(page, { bibliography: 0 })
  // Each hash is a fresh deep link: a same-document hash change to an unknown
  // tab is left alone once signed in, so the second check reloads the page.
  for (const hash of ['#/ingest', '#/search']) {
    await page.goto('about:blank')
    await page.goto(`/${hash}`)
    await expect(page).toHaveURL(/#\/workspace$/)
    await expect(page.getByTestId('workspace-sticky-bar')).toBeVisible()
  }
  await expect(page.getByTestId('tab-ingest')).toHaveCount(0)
})

test('extraction completion reports the seed count without fetching bibliography entries', async ({ page }) => {
  let socket: WebSocketRoute | null = null
  await page.routeWebSocket('**/api/ws', (ws) => {
    socket = ws
  })
  const counters = { bibliography: 0 }
  await mockBackend(page, counters)

  await page.goto('/#/workspace')
  await expect(page.locator('input[type="file"]').first()).toBeAttached()
  await expect.poll(() => socket !== null).toBe(true)

  const frame = { kind: 'extraction', corpus_id: 1, base_name: 'paper', filename: 'paper.pdf' }
  socket!.send(JSON.stringify({ ...frame, event: 'started' }))
  await expect(page.getByRole('progressbar', { name: 'Extraction progress' })).toBeVisible()
  socket!.send(JSON.stringify({ ...frame, event: 'done', status: 'success' }))

  await expect(page.getByText('Extraction complete.')).toBeVisible()
  expect(counters.bibliography).toBe(0)
})

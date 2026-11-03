import { expect, test, type Page, type Route, type WebSocketRoute } from '@playwright/test'
import { fulfil, mePayload, mockLogSocket } from './helpers/mockApi'

// Guards the removal of the unreachable ingest/search tabs: their hashes land
// on the workspace, and a finished PDF extraction reports from the pdf seed's
// candidate count instead of fetching the bibliography entries the dead
// ingest tab used to render (GET /api/ingest/latest).

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((r) => { resolve = r })
  return { promise, resolve }
}

type Hook = (path: string, route: Route) => Promise<boolean> | boolean

async function mockBackend(page: Page, counters: { latest: number }, hook: Hook = () => false) {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    const method = route.request().method()
    if (await hook(path, route)) return
    if (path === '/api/ingest/latest') {
      counters.latest += 1
      return fulfil(route, { items: [] })
    }
    if (path === '/api/auth/me') {
      return fulfil(route, mePayload())
    }
    if (path === '/api/process_pdf' && method === 'POST') {
      return fulfil(route, { message: 'File uploaded successfully.', filename: 'paper.pdf' })
    }
    if (path === '/api/seed/sources') {
      return fulfil(route, {
        sources: [{ source_type: 'pdf', source_key: 'paper', candidate_count: 3, state_counts: {} }],
        total: 1,
        source: 'api',
      })
    }
    if (path.startsWith('/api/extract-bibliography/') && method === 'POST') {
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

async function openSocket(page: Page) {
  const handle: { socket: WebSocketRoute | null } = { socket: null }
  await mockLogSocket(page, (ws) => {
    handle.socket = ws
  })
  return handle
}

const frame = (base: string, extra: Record<string, unknown>) =>
  JSON.stringify({ kind: 'extraction', corpus_id: 1, base_name: base, filename: `${base}.pdf`, ...extra })

test('#/ingest and #/search land on the workspace', async ({ page }) => {
  await mockLogSocket(page)
  await mockBackend(page, { latest: 0 })
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
  const ws = await openSocket(page)
  const counters = { latest: 0 }
  await mockBackend(page, counters)

  await page.goto('/#/workspace')
  await expect.poll(() => ws.socket !== null).toBe(true)

  // Upload a PDF and press Extract, as a user would.
  const fileInput = page.locator('input[type="file"]').first()
  await expect(fileInput).toBeAttached()
  await fileInput.setInputFiles({ name: 'paper.pdf', mimeType: 'application/pdf', buffer: Buffer.from('%PDF-1.4\n') })
  const uploadRow = page.locator('.upload-item', { hasText: 'paper.pdf' })
  const extractButton = uploadRow.getByRole('button', { name: 'Extract' })
  await expect(extractButton).toBeEnabled()
  const extractRequest = page.waitForRequest(
    (req) => new URL(req.url()).pathname === '/api/extract-bibliography/paper.pdf' && req.method() === 'POST'
  )
  await extractButton.click()
  await extractRequest
  await expect(page.getByRole('progressbar', { name: 'Extraction progress' })).toBeVisible()

  ws.socket!.send(frame('paper', { event: 'done', status: 'success' }))

  await expect(page.getByText('Extraction complete.')).toBeVisible()
  // The run finishes after its 1200 ms grace period; no entries fetch happened.
  await expect(page.getByRole('progressbar', { name: 'Extraction progress' })).toHaveCount(0)
  expect(counters.latest).toBe(0)
})

test('a superseded extraction completion does not finish the next extraction', async ({ page }) => {
  const ws = await openSocket(page)
  let holdNextSeed = false
  const seedHeld = deferred()
  const releaseSeed = deferred()
  await mockBackend(page, { latest: 0 }, async (path, route) => {
    if (path === '/api/seed/sources' && holdNextSeed) {
      holdNextSeed = false
      seedHeld.resolve()
      await releaseSeed.promise
      await fulfil(route, {
        sources: [{ source_type: 'pdf', source_key: 'paperA', candidate_count: 3, state_counts: {} }],
        total: 1,
        source: 'api',
      })
      return true
    }
    return false
  })

  await page.goto('/#/workspace')
  await expect.poll(() => ws.socket !== null).toBe(true)
  const progress = page.getByRole('progressbar', { name: 'Extraction progress' })

  ws.socket!.send(frame('paperA', { event: 'started' }))
  await expect(progress).toBeVisible()
  // Run A's completion awaits its seed reload; run B starts meanwhile.
  holdNextSeed = true
  ws.socket!.send(frame('paperA', { event: 'done', status: 'success' }))
  await seedHeld.promise
  ws.socket!.send(frame('paperB', { event: 'started' }))
  await expect(page.getByText('Extraction queued...')).toBeVisible()

  releaseSeed.resolve()
  // Well past A's 1200 ms finish timer: B is still running and untouched by A.
  await page.waitForTimeout(2500)
  await expect(progress).toBeVisible()
  await expect(page.getByText('Extraction queued...')).toBeVisible()
  await expect(page.getByText('Extraction complete.')).toHaveCount(0)
})

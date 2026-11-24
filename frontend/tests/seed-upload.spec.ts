import { expect, test, type Route } from '@playwright/test'
import { afterBodyRead, fetchStats, fulfil, logIn, mockLogSocket, trackFetches } from './helpers/mockApi'

// Exercises the BibTeX/JSON seed upload flow end-to-end in the real frontend
// with the backend mocked (no auth/DB available in CI): file picker accepts a
// .bib, the upload posts to /api/process_pdf, the client detects the seed kind
// and calls /api/ingest/import-seed, and the result counts render on the item.

let processPdfCalls = 0
const IMPORTED = { added: 2, skipped: 1, corpus_linked: 2, total: 3, errors: [] }
let importResult: Record<string, unknown> = IMPORTED

async function mockApi(route: Route) {
  const url = new URL(route.request().url())
  const path = url.pathname
  const method = route.request().method()

  if (path === '/api/auth/me') {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        user: { id: 1, username: 'admin', last_corpus_id: 1, is_admin: true },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      }),
    })
  }

  if (path === '/api/process_pdf' && method === 'POST') {
    processPdfCalls += 1
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ message: 'File uploaded successfully.', filename: 'stored-seed.bib' }),
    })
  }

  if (path === '/api/ingest/import-seed' && method === 'POST') {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(importResult),
    })
  }

  // Permissive defaults so the workspace renders without real data.
  if (path === '/api/corpus') {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ items: [], total: 0, source: 'api', stage_totals: {} }) })
  }
  if (path === '/api/recursion-config') {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ keyword: {} }) })
  }
  const arrayKeyed: Record<string, string> = {
    '/api/seed/sources': 'sources',
    '/api/ingest/runs': 'runs',
    '/api/downloads': 'items',
  }
  if (arrayKeyed[path]) {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ [arrayKeyed[path]]: [], total: 0, source: 'api' }) })
  }
  if (path === '/api/ingest/stats') {
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ stats: {} }) })
  }

  return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' })
}

test('BibTeX seed upload imports into the corpus and reports counts', async ({ page }) => {
  processPdfCalls = 0
  importResult = IMPORTED
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', mockApi)

  await page.goto('/#/workspace')

  const fileInput = page.locator('input[type="file"]').first()
  await expect(fileInput).toBeAttached()

  const importRequest = page.waitForRequest(
    (req) => new URL(req.url()).pathname === '/api/ingest/import-seed' && req.method() === 'POST'
  )
  await fileInput.setInputFiles({
    name: 'references.bib',
    mimeType: 'application/x-bibtex',
    buffer: Buffer.from('@article{a, title={A Work}, author={Doe, Jane}, year={2020}}\n'),
  })
  const importReq = await importRequest

  // The client uploaded the file, detected the BibTeX seed kind, and imported it.
  expect(importReq.postDataJSON()).toMatchObject({ kind: 'bib', filename: 'stored-seed.bib' })

  // Result counts render on the upload item (proves the full round-trip).
  await expect(page.getByText(/Imported 2 works \(1 skipped, 2 added to corpus\)/)).toBeVisible()
  expect(processPdfCalls).toBe(1)
})

// FE-64 (spec D4): the upload list is saved best effort; a full
// localStorage neither fails the upload nor stops the import.
test('a full localStorage does not fail an upload', async ({ page }) => {
  processPdfCalls = 0
  importResult = IMPORTED
  const warnings: string[] = []
  page.on('console', (message) => {
    if (message.type() === 'warning') warnings.push(message.text())
  })
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
    const real = Storage.prototype.setItem
    Storage.prototype.setItem = function (k: string, v: string) {
      if (String(k).startsWith('rag-feeder:uploads:')) throw new DOMException('full', 'QuotaExceededError')
      return real.call(this, k, v)
    }
  })
  await page.route('**/api/**', mockApi)
  await page.goto('/#/workspace')

  const fileInput = page.locator('input[type="file"]').first()
  await expect(fileInput).toBeAttached()
  await fileInput.setInputFiles({
    name: 'references.bib',
    mimeType: 'application/x-bibtex',
    buffer: Buffer.from('@article{a, title={A Work}, author={Doe, Jane}, year={2020}}\n'),
  })

  const item = page.locator('.upload-item', { hasText: 'references.bib' })
  await expect(item).toContainText('Imported 2 works (1 skipped, 2 added to corpus).')
  await expect(item.locator('.status')).not.toHaveText('failed')
  expect(warnings.filter((text) => text.startsWith('[uploads] could not save the upload list:'))).toHaveLength(1)
})

async function uploadSeed(page: import('@playwright/test').Page) {
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await page.route('**/api/**', mockApi)
  await page.goto('/#/workspace')
  const fileInput = page.locator('input[type="file"]').first()
  await expect(fileInput).toBeAttached()
  await fileInput.setInputFiles({
    name: 'references.json',
    mimeType: 'application/json',
    buffer: Buffer.from('[]'),
  })
}

test('a stopped import shows the counts it saved', async ({ page }) => {
  importResult = {
    added: 380, skipped: 0, corpus_linked: 20, total: 1000, processed: 400,
    stopped: true, error: 'OperationalError: database is locked', errors: [],
  }
  await uploadSeed(page)
  const message = 'Import stopped after 400 of 1000 entries: OperationalError: database is locked (380 added, 20 linked before it).'
  const item = page.locator('.upload-item', { hasText: message })
  await expect(page.getByText(message, { exact: true })).toBeVisible()
  await expect(page.getByText(/Imported \d+ works/)).toHaveCount(0)
  await expect(item.locator('.status')).toHaveText('failed')
})

test('an import that saved nothing shows Import failed', async ({ page }) => {
  importResult = {
    added: 0, skipped: 0, corpus_linked: 0, total: 0, processed: 0,
    stopped: true, error: 'JSONDecodeError: Failed to parse json: Expecting value', errors: [],
  }
  await uploadSeed(page)
  const message = 'Import failed: JSONDecodeError: Failed to parse json: Expecting value'
  const item = page.locator('.upload-item', { hasText: message })
  await expect(page.getByText(message, { exact: true })).toBeVisible()
  await expect(page.getByText(/Imported \d+ works/)).toHaveCount(0)
  await expect(item.locator('.status')).toHaveText('failed')
})

// A seed upload whose file answer settles after a sign-out and a new
// sign-in does not import the old account's file under the new token, and
// the rest of that selection is not uploaded under it either (review, out
// of scope round 1).
test('a seed upload answered after an account change does not import into the next account', async ({ page }) => {
  importResult = IMPORTED
  let release!: () => void
  const uploadGate = new Promise<void>((r) => { release = r })
  let uploadRequests = 0
  const uploadHeld: (() => void)[] = []
  let importGateOn = true
  const importHeld: (() => void)[] = []
  const answeredSeeds: string[] = []
  await mockLogSocket(page)
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'playwright-token')
    }
  })
  await trackFetches(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') {
      return fulfil(route, { token: 'second-token', user: { id: 2, username: 'second', last_corpus_id: 1, is_admin: false } })
    }
    if (path === '/api/auth/me') {
      const second = auth === 'Bearer second-token'
      return fulfil(route, {
        user: second ? { id: 2, username: 'second', last_corpus_id: 1, is_admin: false } : { id: 1, username: 'admin', last_corpus_id: 1, is_admin: true },
        corpora: [{ id: 1, name: 'Local research corpus', role: 'owner', owner_username: 'admin' }],
      })
    }
    if (path === '/api/process_pdf') {
      uploadRequests += 1
      if (uploadRequests === 1) await uploadGate
      else await new Promise<void>((resolve) => { uploadHeld.push(resolve) })
    }
    if (path === '/api/ingest/import-seed' && importGateOn) {
      await new Promise<void>((resolve) => { importHeld.push(resolve) })
    }
    await mockApi(route)
    if (path === '/api/seed/sources') answeredSeeds.push(auth)
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('auth-user')).toHaveText('admin')

  await page.locator('input[type="file"]').first().setInputFiles([
    {
      name: 'references.bib',
      mimeType: 'application/x-bibtex',
      buffer: Buffer.from('@article{a, title={A Work}, author={Doe, Jane}, year={2020}}\n'),
    },
    { name: 'more.json', mimeType: 'application/json', buffer: Buffer.from('[]') },
  ])
  await expect.poll(() => uploadRequests).toBe(1)

  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'second')
  await expect(page.getByTestId('auth-user')).toHaveText('second')
  await expect.poll(() => answeredSeeds.includes('Bearer second-token')).toBe(true)
  const importCalls = (await fetchStats(page, '/api/ingest/import-seed')).calls
  const upload = await fetchStats(page, '/api/process_pdf')

  release()
  await afterBodyRead(page, '/api/process_pdf', upload.reads)
  expect((await fetchStats(page, '/api/ingest/import-seed')).calls).toBe(importCalls)
  expect((await fetchStats(page, '/api/process_pdf')).calls).toBe(upload.calls)

  importGateOn = false
  for (const resume of importHeld.splice(0)) resume()
  for (const resume of uploadHeld.splice(0)) resume()
})

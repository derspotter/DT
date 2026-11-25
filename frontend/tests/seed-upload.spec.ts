import { expect, test, type Route } from '@playwright/test'
import { FETCH_SEQ_HEADER, afterBodyRead, fetchStats, fulfil, logIn, mePayload, mockLogSocket, settleReads, trackFetches } from './helpers/mockApi'

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

// FE-65: a corpus switch while a seed upload is in flight stops the
// selection. The upload that settles after the switch does not import into
// the corpus now selected, and the rest of the selection is not uploaded.
test('a seed upload answered after a corpus switch does not import into the next corpus', async ({ page }) => {
  importResult = IMPORTED
  let release!: () => void
  const uploadGate = new Promise<void>((r) => { release = r })
  let uploadRequests = 0
  let corpusId = 1
  let seedsAfterSwitch = 0
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await trackFetches(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') {
      return fulfil(route, mePayload({
        corpusId,
        corpora: [1, 2].map((id) => ({ id, name: `Research corpus ${id}`, role: 'owner', owner_username: 'admin' })),
      }))
    }
    if (path === '/api/corpora/2/select') {
      corpusId = 2
      return fulfil(route, { selected: 2 })
    }
    if (path === '/api/process_pdf') {
      uploadRequests += 1
      if (uploadRequests === 1) await uploadGate
    }
    await mockApi(route)
    if (path === '/api/seed/sources' && corpusId === 2) seedsAfterSwitch += 1
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

  await page.locator('.header-corpus-picker select').selectOption('2')
  await expect.poll(() => seedsAfterSwitch).toBeGreaterThan(0)
  const importCalls = (await fetchStats(page, '/api/ingest/import-seed')).calls
  const upload = await fetchStats(page, '/api/process_pdf')
  expect(importCalls).toBe(0)
  expect(upload.calls).toBe(1)

  release()
  await afterBodyRead(page, '/api/process_pdf', upload.reads)
  expect((await fetchStats(page, '/api/ingest/import-seed')).calls).toBe(importCalls)
  expect((await fetchStats(page, '/api/process_pdf')).calls).toBe(upload.calls)
  expect(uploadRequests).toBe(1)
})

// An import that already started in corpus 1 reports its result on its row,
// also after corpus 1 → 2 → 1 while it ran (the row is back on screen).
test('an import that settles after switching away and back shows its result', async ({ page }) => {
  importResult = IMPORTED
  let release!: () => void
  const importGate = new Promise<void>((r) => { release = r })
  let corpusId = 1
  await mockLogSocket(page)
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await trackFetches(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') {
      return fulfil(route, mePayload({
        corpusId,
        corpora: [1, 2].map((id) => ({ id, name: `Research corpus ${id}`, role: 'owner', owner_username: 'admin' })),
      }))
    }
    const select = path.match(/^\/api\/corpora\/(\d+)\/select$/)
    if (select) {
      corpusId = Number(select[1])
      return fulfil(route, { selected: corpusId })
    }
    if (path === '/api/ingest/import-seed') await importGate
    await mockApi(route)
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('auth-user')).toHaveText('admin')

  await page.locator('input[type="file"]').first().setInputFiles([{
    name: 'references.bib',
    mimeType: 'application/x-bibtex',
    buffer: Buffer.from('@article{a, title={A Work}, author={Doe, Jane}, year={2020}}\n'),
  }])
  await expect.poll(async () => (await fetchStats(page, '/api/ingest/import-seed')).calls).toBe(1)

  const picker = page.locator('.header-corpus-picker select')
  for (const target of ['2', '1']) {
    const me = (await fetchStats(page, '/api/auth/me')).reads
    await picker.selectOption(target)
    await afterBodyRead(page, '/api/auth/me', me)
  }
  await expect(page.getByText('references.bib')).toBeVisible()

  const imports = (await fetchStats(page, '/api/ingest/import-seed')).reads
  release()
  await afterBodyRead(page, '/api/ingest/import-seed', imports)
  await expect(page.getByText(/Imported 2 works \(1 skipped, 2 added to corpus\)/)).toBeVisible()
  await expect(page.getByText('Importing seed…')).toBeHidden()
})

// FE-65b, FE-68 (phase 9 D4): the import and the extraction carry the corpus
// they started in; a late extraction reply after a committed switch changes
// nothing; saved upload rows belong to one user and one corpus.
const EXTRACT = /^\/api\/extract-bibliography\//
const PDF = { name: 'paper.pdf', mimeType: 'application/pdf', buffer: Buffer.from('%PDF-1.4\n%%EOF\n') }
const BIB = {
  name: 'references.bib',
  mimeType: 'application/x-bibtex',
  buffer: Buffer.from('@article{a, title={A Work}, author={Doe, Jane}, year={2020}}\n'),
}
const USERS: Record<string, { id: number; username: string }> = {
  'alice-token': { id: 11, username: 'alice' },
  'bob-token': { id: 12, username: 'bob' },
}

type Held = { release: () => void; seq: number }
type ExtractStep = { gate: Promise<void>; status: number; body: unknown }

async function bootCorpora(page: import('@playwright/test').Page) {
  const state = {
    corpusId: 1,
    uploadGate: null as Promise<void> | null,
    importBodies: [] as unknown[],
    extractBodies: [] as unknown[],
    // Per extraction request, in order; once empty, answered 202 at once.
    extractSteps: [] as ExtractStep[],
    meGateOn: false,
    heldMe: [] as Held[],
  }
  await mockLogSocket(page)
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'alice-token')
    }
  })
  await trackFetches(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const token = (request.headers().authorization || '').replace(/^Bearer /, '')
    if (path === '/api/auth/login') {
      const username = String(request.postDataJSON()?.username || '')
      const [nextToken, user] = Object.entries(USERS).find(([, u]) => u.username === username)!
      return fulfil(route, { token: nextToken, user: { ...user, last_corpus_id: state.corpusId, is_admin: false } })
    }
    if (path === '/api/auth/me') {
      const seq = Number(request.headers()[FETCH_SEQ_HEADER] || 0)
      if (state.meGateOn) await new Promise<void>((release) => { state.heldMe.push({ release, seq }) })
      const user = USERS[token] || USERS['alice-token']
      return fulfil(route, mePayload({
        id: user.id,
        username: user.username,
        admin: false,
        corpusId: state.corpusId,
        corpora: [1, 2].map((id) => ({ id, name: `Research corpus ${id}`, role: 'owner', owner_username: user.username })),
      }))
    }
    const select = path.match(/^\/api\/corpora\/(\d+)\/select$/)
    if (select) {
      state.corpusId = Number(select[1])
      return fulfil(route, { selected: state.corpusId })
    }
    if (path === '/api/process_pdf') {
      if (state.uploadGate) await state.uploadGate
      const name = (request.postDataBuffer()?.toString('latin1').match(/filename="([^"]+)"/) || [])[1] || 'upload'
      return fulfil(route, { message: 'File uploaded successfully.', filename: `stored-${name}` })
    }
    if (path === '/api/ingest/import-seed') {
      state.importBodies.push(request.postDataJSON())
    }
    if (EXTRACT.test(path)) {
      state.extractBodies.push(request.postDataJSON())
      const step = state.extractSteps.shift()
      if (step) {
        await step.gate
        return fulfil(route, step.body, step.status)
      }
      return fulfil(route, { state: 'started', queued: false, queue_position: 0 }, 202)
    }
    return mockApi(route)
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('auth-user')).toHaveText('alice')
  return state
}

type BootedCorpora = Awaited<ReturnType<typeof bootCorpora>>

const picker = (page: import('@playwright/test').Page) => page.locator('.header-corpus-picker select')
const uploadRow = (page: import('@playwright/test').Page, name: string) => page.locator('.upload-item', { hasText: name })

// Saves the switch to `target` and holds its /me (the first /me the app
// sent after reading the select's answer) at the gate: not committed yet.
async function saveSwitch(page: import('@playwright/test').Page, state: BootedCorpora, target: string) {
  state.meGateOn = true
  const selectPath = `/api/corpora/${target}/select`
  const selectReads = (await fetchStats(page, selectPath)).reads
  await picker(page).selectOption(target)
  await afterBodyRead(page, selectPath, selectReads)
  const seq = await page.evaluate((path) => (window as any).__dtFetch.firstCallAfterRead(path, '/api/auth/me'), selectPath)
  expect(seq).not.toBeNull()
  await expect.poll(() => state.heldMe.some((held) => held.seq === seq)).toBe(true)
  return seq as number
}

// Releases the switch's own /me and checkpoints on its read: the workspace
// reset runs synchronously after it. Then lifts the gate for any other /me.
async function commitSwitch(page: import('@playwright/test').Page, state: BootedCorpora, seq: number) {
  const meReads = (await fetchStats(page, '/api/auth/me')).reads
  const own = state.heldMe.find((held) => held.seq === seq)!
  state.heldMe.splice(state.heldMe.indexOf(own), 1)
  own.release()
  await afterBodyRead(page, '/api/auth/me', meReads)
  state.meGateOn = false
  for (const held of state.heldMe.splice(0)) held.release()
}

async function switchAndCommit(page: import('@playwright/test').Page, state: BootedCorpora, target: string) {
  await commitSwitch(page, state, await saveSwitch(page, state, target))
}

async function uploadPdfRow(page: import('@playwright/test').Page, file = PDF) {
  await page.locator('input[type="file"]').first().setInputFiles(file)
  await expect(uploadRow(page, file.name)).toContainText('Uploaded')
}

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((r) => { resolve = r })
  return { promise, resolve }
}

test('an import started before a saved switch keeps its corpus', async ({ page }) => {
  const upload = deferred()
  const state = await bootCorpora(page)
  state.uploadGate = upload.promise
  await page.locator('input[type="file"]').first().setInputFiles(BIB)
  await expect.poll(async () => (await fetchStats(page, '/api/process_pdf')).calls).toBe(1)
  const seq = await saveSwitch(page, state, '2')
  upload.resolve()
  await expect.poll(() => state.importBodies.length).toBe(1)
  expect(state.importBodies[0]).toMatchObject({ filename: 'stored-references.bib', kind: 'bib', corpusId: 1 })
  await commitSwitch(page, state, seq)
})

test('an extraction clicked before the commit keeps its corpus', async ({ page }) => {
  const state = await bootCorpora(page)
  await uploadPdfRow(page)
  const seq = await saveSwitch(page, state, '2')
  await uploadRow(page, 'paper.pdf').getByRole('button', { name: 'Extract' }).click()
  await expect.poll(() => state.extractBodies.length).toBe(1)
  expect(state.extractBodies[0]).toEqual({ corpusId: 1 })
  await commitSwitch(page, state, seq)
})

test('a late extraction reply is dropped after the commit', async ({ page }) => {
  const reply = deferred()
  const state = await bootCorpora(page)
  await uploadPdfRow(page)
  state.extractSteps.push({ gate: reply.promise, status: 202, body: { state: 'started', queued: false, queue_position: 0 } })
  const seq = await saveSwitch(page, state, '2')
  await uploadRow(page, 'paper.pdf').getByRole('button', { name: 'Extract' }).click()
  await expect.poll(() => state.extractBodies.length).toBe(1)
  await commitSwitch(page, state, seq)

  const eReads = (await fetchStats(page, EXTRACT)).reads
  reply.resolve()
  await afterBodyRead(page, EXTRACT, eReads)
  await expect(page.locator('.extract-progress')).toHaveCount(0)

  // The outcome went into corpus 1's saved rows: back there the row says the
  // extraction started instead of sitting idle as 'Uploaded'.
  await switchAndCommit(page, state, '1')
  await expect(uploadRow(page, 'paper.pdf')).toContainText('Extraction started')
  await expect(uploadRow(page, 'paper.pdf')).not.toContainText('Uploaded')
})

test('a late extraction failure does not clear the new corpus progress', async ({ page }) => {
  const reply = deferred()
  const state = await bootCorpora(page)
  await uploadPdfRow(page)
  state.extractSteps.push({ gate: reply.promise, status: 500, body: { error: 'Failed to start bibliography extraction' } })
  const seq = await saveSwitch(page, state, '2')
  await uploadRow(page, 'paper.pdf').getByRole('button', { name: 'Extract' }).click()
  await expect.poll(() => state.extractBodies.length).toBe(1)
  await commitSwitch(page, state, seq)

  const second = { ...PDF, name: 'second.pdf' }
  await uploadPdfRow(page, second)
  await uploadRow(page, 'second.pdf').getByRole('button', { name: 'Extract' }).click()
  await expect(page.locator('.extract-progress')).toBeVisible()

  const eReads = (await fetchStats(page, EXTRACT)).reads
  reply.resolve()
  await afterBodyRead(page, EXTRACT, eReads)
  await expect(page.locator('.extract-progress')).toBeVisible()
  await expect(uploadRow(page, 'paper.pdf')).toHaveCount(0)

  // Back in corpus 1 the row shows the failure, not an idle 'Uploaded'.
  await switchAndCommit(page, state, '1')
  await expect(uploadRow(page, 'paper.pdf')).toContainText('Failed to start bibliography extraction')
  await expect(uploadRow(page, 'paper.pdf').locator('.status')).toHaveText('failed')
})

test("a saved upload row is not shown to the next user of the browser", async ({ page }) => {
  await bootCorpora(page)
  await uploadPdfRow(page)
  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  await settleReads(page, '/api/seed/sources')
  await expect(uploadRow(page, 'paper.pdf')).toHaveCount(0)
})

test('a row saved under the legacy corpus-only key is ignored', async ({ page }) => {
  await bootCorpora(page)
  await page.evaluate(() => {
    window.localStorage.setItem('rag-feeder:uploads:1', JSON.stringify([
      { id: 'legacy', name: 'legacy.pdf', size: 10, status: 'uploaded', backendFilename: 'stored-legacy.pdf', message: 'Uploaded' },
    ]))
  })
  await page.reload()
  await expect(page.getByTestId('auth-user')).toHaveText('alice')
  await settleReads(page, '/api/seed/sources')
  await expect(uploadRow(page, 'legacy.pdf')).toHaveCount(0)
})

test('a restored row does not show a transient submission message', async ({ page }) => {
  const reply = deferred()
  const state = await bootCorpora(page)
  await uploadPdfRow(page)
  state.extractSteps.push({ gate: reply.promise, status: 202, body: { state: 'started', queued: false, queue_position: 0 } })
  await uploadRow(page, 'paper.pdf').getByRole('button', { name: 'Extract' }).click()
  await expect(uploadRow(page, 'paper.pdf')).toContainText('Submitting extraction request...')
  await switchAndCommit(page, state, '2')
  await switchAndCommit(page, state, '1')
  await expect(uploadRow(page, 'paper.pdf')).toBeVisible()
  await expect(uploadRow(page, 'paper.pdf')).not.toContainText('Submitting extraction request...')
  reply.resolve()
})

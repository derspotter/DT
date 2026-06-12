import { expect, test, type Route } from '@playwright/test'

// Exercises the BibTeX/JSON seed upload flow end-to-end in the real frontend
// with the backend mocked (no auth/DB available in CI): file picker accepts a
// .bib, the upload posts to /api/process_pdf, the client detects the seed kind
// and calls /api/ingest/import-seed, and the result counts render on the item.

let processPdfCalls = 0

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
      body: JSON.stringify({ added: 2, skipped: 1, corpus_linked: 2, total: 3, errors: [] }),
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
